use super::entra_token_client::request_entra_token;
use super::token_http_client::TokenHttpClient;
use crate::auth::token::access_token::AccessToken;
use crate::auth::token::token_source::TokenSource;
use crate::error::CoreResult;
use secrecy::{ExposeSecret, SecretString};

/// Fetches tokens for an Entra ID service principal with its client secret (client credentials grant).
pub struct ServicePrincipalSecretTokenSource {
    http: TokenHttpClient,
    authority_host: String,
    tenant_id: String,
    client_id: String,
    client_secret: SecretString,
    scope: String,
}

impl ServicePrincipalSecretTokenSource {
    pub fn new(
        http: TokenHttpClient,
        authority_host: String,
        tenant_id: String,
        client_id: String,
        client_secret: SecretString,
        scope: String,
    ) -> Self {
        Self {
            http,
            authority_host,
            tenant_id,
            client_id,
            client_secret,
            scope,
        }
    }
}

impl TokenSource for ServicePrincipalSecretTokenSource {
    fn fetch_access_token(&self) -> CoreResult<AccessToken> {
        let form = [
            ("grant_type", "client_credentials"),
            ("client_id", self.client_id.as_str()),
            ("client_secret", self.client_secret.expose_secret()),
            ("scope", self.scope.as_str()),
        ];
        request_entra_token(
            &self.http,
            &self.authority_host,
            &self.tenant_id,
            &form,
            &[&self.client_secret],
        )
    }

    fn describe_source(&self) -> &'static str {
        "azure-sp-secret"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::azure::token_http_client::TOKEN_REQUEST_TIMEOUT;
    use crate::error::CoreError;
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};

    const SCOPE: &str = "https://ns.servicebus.windows.net/.default";

    fn token_route(status: u16, body: &str) -> FakeRoute {
        FakeRoute {
            method: "POST".to_string(),
            path_and_query: "/tenant-1/oauth2/v2.0/token".to_string(),
            status,
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    fn source_for(
        authority_host: String,
        client_secret: &str,
    ) -> ServicePrincipalSecretTokenSource {
        ServicePrincipalSecretTokenSource::new(
            TokenHttpClient::new(TOKEN_REQUEST_TIMEOUT).unwrap(),
            authority_host,
            "tenant-1".to_string(),
            "app".to_string(),
            SecretString::from(client_secret),
            SCOPE.to_string(),
        )
    }

    #[test]
    fn posts_client_credentials_form() {
        let server = FakeRegistryServer::start(vec![token_route(
            200,
            r#"{"access_token":"token-from-route","expires_in":3600}"#,
        )])
        .unwrap();
        let source = source_for(server.url(), "client-secret-value");

        let token = source.fetch_access_token().unwrap();

        assert_eq!(token.value.expose_secret(), "token-from-route");
        let requests = server.recorded_requests();
        let body = &requests[0].body;
        assert!(
            body.contains("grant_type=client_credentials"),
            "body: {body}"
        );
        assert!(body.contains("client_id=app"), "body: {body}");
        assert!(
            body.contains("scope=https%3A%2F%2Fns.servicebus.windows.net%2F.default"),
            "body: {body}"
        );
        assert!(body.contains("client_secret="), "body: {body}");
        assert_eq!(source.describe_source(), "azure-sp-secret");
    }

    #[test]
    fn entra_error_never_leaks_secret() {
        let server = FakeRegistryServer::start(vec![token_route(
            401,
            r#"{"error":"invalid_client","error_description":"AADSTS7000215: the secret client-secret-value is wrong"}"#,
        )])
        .unwrap();
        let source = source_for(server.url(), "client-secret-value");

        let error = source.fetch_access_token().unwrap_err();

        let message = format!("{error}");
        assert!(message.contains("azure token request failed (HTTP 401): invalid_client"));
        assert!(
            !message.contains("client-secret-value"),
            "message: {message}"
        );
        assert!(matches!(error, CoreError::Authentication(_)));
    }
}
