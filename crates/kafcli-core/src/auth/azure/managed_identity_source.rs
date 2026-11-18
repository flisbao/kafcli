use super::azure_environment::{AzureEnvironment, read_non_blank_variable};
use super::default_credential_chain::{CredentialLink, LinkFailure};
use super::token_http_client::{TOKEN_REQUEST_TIMEOUT, TokenHttpClient};
use super::token_response::parse_token_response;
use crate::auth::token::access_token::AccessToken;
use crate::auth::token::token_source::TokenSource;
use crate::error::CoreResult;
use crate::registry::path_segment::encode_path_segment;
use secrecy::{ExposeSecret, SecretString};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Base address of the Instance Metadata Service, reachable only from inside an Azure VM or pod.
pub const DEFAULT_IMDS_BASE_URL: &str = "http://169.254.169.254";

/// Principal shown for a managed identity without a `client_id`.
pub const SYSTEM_ASSIGNED_PRINCIPAL: &str = "system-assigned managed identity";

const SOURCE_NAME: &str = "azure-managed-identity";

/// The managed identity takes a resource, not a scope: the `/.default` suffix is dropped.
pub(crate) fn resource_for_scope(scope: &str) -> String {
    scope.strip_suffix("/.default").unwrap_or(scope).to_owned()
}

const IMDS_TOKEN_PATH: &str = "/metadata/identity/oauth2/token";
const IMDS_API_VERSION: &str = "2018-02-01";
const APP_SERVICE_API_VERSION: &str = "2019-08-01";
const IDENTITY_ENDPOINT_VARIABLE: &str = "IDENTITY_ENDPOINT";
const IDENTITY_HEADER_VARIABLE: &str = "IDENTITY_HEADER";
const APP_SERVICE_HEADER_NAME: &str = "X-IDENTITY-HEADER";

/// Fetches tokens for the managed identity of the host. The App Service endpoint is used when
/// `IDENTITY_ENDPOINT` and `IDENTITY_HEADER` are set (App Service, Container Apps); otherwise IMDS.
pub struct ManagedIdentityTokenSource {
    client_id: Option<String>,
    resource: String,
    environment: Arc<dyn AzureEnvironment>,
    imds_base_url: String,
    probe_timeout: Option<Duration>,
}

impl ManagedIdentityTokenSource {
    /// `resource` is the scope without its `/.default` suffix. `probe_timeout` replaces the default
    /// token request timeout when set.
    pub(crate) fn with_endpoints(
        client_id: Option<String>,
        resource: String,
        environment: Arc<dyn AzureEnvironment>,
        imds_base_url: String,
        probe_timeout: Option<Duration>,
    ) -> Self {
        Self {
            client_id: client_id.filter(|value| !value.trim().is_empty()),
            resource,
            environment,
            imds_base_url,
            probe_timeout,
        }
    }

    fn query(&self, api_version: &str) -> String {
        let mut query = format!(
            "api-version={api_version}&resource={}",
            encode_path_segment(&self.resource)
        );
        if let Some(client_id) = &self.client_id {
            query.push_str("&client_id=");
            query.push_str(&encode_path_segment(client_id));
        }
        query
    }

    fn app_service_endpoint(&self) -> Option<(String, SecretString)> {
        let endpoint =
            read_non_blank_variable(self.environment.as_ref(), IDENTITY_ENDPOINT_VARIABLE)?;
        let header = read_non_blank_variable(self.environment.as_ref(), IDENTITY_HEADER_VARIABLE)?;
        Some((endpoint, SecretString::from(header)))
    }
}

impl ManagedIdentityTokenSource {
    /// IMDS that cannot be reached, or answers 400 or 404 (no identity on this host), is
    /// unavailable. Any other answer is an error. The App Service endpoint has no such case.
    fn fetch_token_or_classify(&self) -> Result<AccessToken, LinkFailure> {
        let http = TokenHttpClient::new(self.probe_timeout.unwrap_or(TOKEN_REQUEST_TIMEOUT))
            .map_err(LinkFailure::Error)?;
        let now = SystemTime::now();
        match self.app_service_endpoint() {
            Some((endpoint, header)) => {
                let url = format!("{endpoint}?{}", self.query(APP_SERVICE_API_VERSION));
                let response = http
                    .get(
                        &url,
                        &[(APP_SERVICE_HEADER_NAME, header.expose_secret())],
                        &[&header],
                    )
                    .map_err(LinkFailure::Error)?;
                parse_token_response(response.status, &response.body, now, &[&header])
                    .map_err(LinkFailure::Error)
            }
            None => {
                let url = format!(
                    "{}{IMDS_TOKEN_PATH}?{}",
                    self.imds_base_url.trim_end_matches('/'),
                    self.query(IMDS_API_VERSION)
                );
                let response = http
                    .get(&url, &[("Metadata", "true")], &[])
                    .map_err(LinkFailure::unavailable_from)?;
                match parse_token_response(response.status, &response.body, now, &[]) {
                    Err(error) if matches!(response.status, 400 | 404) => {
                        Err(LinkFailure::unavailable_from(error))
                    }
                    other => other.map_err(LinkFailure::Error),
                }
            }
        }
    }
}

impl CredentialLink for ManagedIdentityTokenSource {
    fn fetch_link_token(&self) -> Result<AccessToken, LinkFailure> {
        self.fetch_token_or_classify()
    }

    fn source_name(&self) -> &'static str {
        SOURCE_NAME
    }
}

impl TokenSource for ManagedIdentityTokenSource {
    fn fetch_access_token(&self) -> CoreResult<AccessToken> {
        self.fetch_token_or_classify()
            .map_err(LinkFailure::into_error)
    }

    fn describe_source(&self) -> &'static str {
        SOURCE_NAME
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::azure::azure_environment::FakeAzureEnvironment;
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
    use secrecy::ExposeSecret;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const RESOURCE: &str = "https://ns.servicebus.windows.net";
    const IMDS_PATH: &str = "/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fns.servicebus.windows.net";
    const APP_SERVICE_PATH: &str =
        "/msi?api-version=2019-08-01&resource=https%3A%2F%2Fns.servicebus.windows.net";

    fn route(method: &str, path: &str, body: &str) -> FakeRoute {
        FakeRoute {
            method: method.to_string(),
            path_and_query: path.to_string(),
            status: 200,
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    fn imds_source(
        client_id: Option<&str>,
        imds_base_url: String,
        variables: &[(&str, &str)],
    ) -> ManagedIdentityTokenSource {
        ManagedIdentityTokenSource::with_endpoints(
            client_id.map(str::to_string),
            RESOURCE.to_string(),
            Arc::new(FakeAzureEnvironment::new(variables)),
            imds_base_url,
            None,
        )
    }

    #[test]
    fn imds_request_has_metadata_header_and_resource() {
        let server = FakeRegistryServer::start(vec![route(
            "GET",
            IMDS_PATH,
            r#"{"access_token":"imds-token","expires_on":"2000000000"}"#,
        )])
        .unwrap();
        let source = imds_source(None, server.url(), &[]);

        let token = source.fetch_access_token().unwrap();

        assert_eq!(token.value.expose_secret(), "imds-token");
        let requests = server.recorded_requests();
        assert_eq!(requests[0].path_and_query, IMDS_PATH);
        assert!(
            requests[0]
                .headers
                .iter()
                .any(|(name, value)| name.eq_ignore_ascii_case("metadata") && value == "true")
        );
        assert_eq!(source.describe_source(), "azure-managed-identity");
    }

    #[test]
    fn app_service_endpoint_wins_when_set() {
        let server = FakeRegistryServer::start(vec![
            route(
                "GET",
                APP_SERVICE_PATH,
                r#"{"access_token":"app-service-token","expires_in":3600}"#,
            ),
            route(
                "GET",
                IMDS_PATH,
                r#"{"access_token":"imds-token","expires_in":3600}"#,
            ),
        ])
        .unwrap();
        let endpoint = format!("{}/msi", server.url());
        let source = imds_source(
            None,
            "http://127.0.0.1:1".to_string(),
            &[("IDENTITY_ENDPOINT", &endpoint), ("IDENTITY_HEADER", "h")],
        );

        let token = source.fetch_access_token().unwrap();

        assert_eq!(token.value.expose_secret(), "app-service-token");
        let requests = server.recorded_requests();
        assert_eq!(requests[0].path_and_query, APP_SERVICE_PATH);
        assert!(
            requests[0].headers.iter().any(|(name, value)| name
                .eq_ignore_ascii_case("x-identity-header")
                && value == "h")
        );
    }

    #[test]
    fn user_assigned_client_id_is_sent() {
        let path = format!("{IMDS_PATH}&client_id=uami-1");
        let server = FakeRegistryServer::start(vec![route(
            "GET",
            &path,
            r#"{"access_token":"t","expires_in":3600}"#,
        )])
        .unwrap();
        let source = imds_source(Some("uami-1"), server.url(), &[]);

        source.fetch_access_token().unwrap();

        assert_eq!(server.recorded_requests()[0].path_and_query, path);
    }

    #[test]
    fn accepts_string_expires_on_and_prefers_expires_in_from_imds() {
        let server = FakeRegistryServer::start(vec![route(
            "GET",
            IMDS_PATH,
            r#"{"access_token":"t","expires_on":"2000000000","expires_in":"86399"}"#,
        )])
        .unwrap();
        let source = imds_source(None, server.url(), &[]);
        let before = SystemTime::now();

        let token = source.fetch_access_token().unwrap();

        let expected_from_expires_in = before + Duration::from_secs(86_399);
        let from_expires_on = UNIX_EPOCH + Duration::from_secs(2_000_000_000);
        let slack = Duration::from_secs(60);
        assert!(token.expires_at >= expected_from_expires_in - slack);
        assert!(token.expires_at <= expected_from_expires_in + slack);
        assert_ne!(token.expires_at, from_expires_on);
    }
}
