use super::azure_cli_source::AzureCliTokenSource;
use super::azure_environment::{AzureEnvironment, read_non_blank_variable};
use super::azure_token_source_factory::{
    checked_authority_host, resolve_workload_identity_settings,
};
use super::client_assertion::ClientCertificate;
use super::managed_identity_source::{ManagedIdentityTokenSource, resource_for_scope};
use super::service_principal_certificate_source::ServicePrincipalCertificateTokenSource;
use super::service_principal_secret_source::ServicePrincipalSecretTokenSource;
use super::token_http_client::{TOKEN_REQUEST_TIMEOUT, TokenHttpClient};
use super::workload_identity_source::WorkloadIdentityTokenSource;
use crate::auth::token::access_token::AccessToken;
use crate::auth::token::token_source::TokenSource;
use crate::config::azure_credential_model::{AzureDefaultConfig, AzureWorkloadIdentityConfig};
use crate::error::{CoreError, CoreResult};
use secrecy::SecretString;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

/// Probe timeout of the managed identity link. A host without IMDS must not wait for the full
/// token request timeout before the chain moves on.
const MANAGED_IDENTITY_PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const CHAIN_SOURCE_NAME: &str = "azure-default";

/// Why a link of the chain did not produce a token.
pub(crate) enum LinkFailure {
    /// The link cannot be used on this host, so the chain tries the next link.
    Unavailable(String),
    /// The link is usable but failed, so the chain stops with this error.
    Error(CoreError),
}

impl LinkFailure {
    /// Treats an error as "unavailable", keeping its message as the reason.
    pub(crate) fn unavailable_from(error: CoreError) -> Self {
        Self::Unavailable(message_of(error))
    }

    /// The error shown when the link is used alone or was chosen by the chain.
    pub(crate) fn into_error(self) -> CoreError {
        match self {
            Self::Unavailable(reason) => CoreError::Authentication(reason),
            Self::Error(error) => error,
        }
    }
}

fn message_of(error: CoreError) -> String {
    match error {
        CoreError::Authentication(message) | CoreError::AuthenticationTransient(message) => message,
        other => other.to_string(),
    }
}

/// One link of the chain. Unavailability is reported apart from errors.
pub(crate) trait CredentialLink: Send + Sync {
    fn fetch_link_token(&self) -> Result<AccessToken, LinkFailure>;

    /// Short name of the token type, such as `azure-sp-secret`.
    fn source_name(&self) -> &'static str;
}

/// Link over a plain token source. Every failure is an error, never "unavailable".
struct SourceLink {
    source: Box<dyn TokenSource>,
}

impl CredentialLink for SourceLink {
    fn fetch_link_token(&self) -> Result<AccessToken, LinkFailure> {
        self.source.fetch_access_token().map_err(LinkFailure::Error)
    }

    fn source_name(&self) -> &'static str {
        self.source.describe_source()
    }
}

/// Link whose settings are missing from the environment.
struct UnavailableLink {
    reason: String,
}

impl CredentialLink for UnavailableLink {
    fn fetch_link_token(&self) -> Result<AccessToken, LinkFailure> {
        Err(LinkFailure::Unavailable(self.reason.clone()))
    }

    fn source_name(&self) -> &'static str {
        CHAIN_SOURCE_NAME
    }
}

fn unavailable(reason: &str) -> Box<dyn CredentialLink> {
    Box::new(UnavailableLink {
        reason: reason.to_owned(),
    })
}

enum EnvironmentCredential {
    ClientSecret(SecretString),
    Certificate(String),
}

/// `AZURE_CLIENT_SECRET` wins over `AZURE_CLIENT_CERTIFICATE_PATH`, as in the Azure SDK. The secret
/// is wrapped as soon as it is read.
fn read_environment_credential(
    environment: &dyn AzureEnvironment,
) -> Option<EnvironmentCredential> {
    if let Some(secret) = read_non_blank_variable(environment, "AZURE_CLIENT_SECRET") {
        return Some(EnvironmentCredential::ClientSecret(SecretString::from(
            secret,
        )));
    }
    read_non_blank_variable(environment, "AZURE_CLIENT_CERTIFICATE_PATH")
        .map(EnvironmentCredential::Certificate)
}

/// The context value wins; otherwise the variable, when it is not blank.
fn configured_or_variable(
    configured: Option<&str>,
    environment: &dyn AzureEnvironment,
    name: &str,
) -> Option<String> {
    configured
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| read_non_blank_variable(environment, name))
}

/// Service principal from `AZURE_CLIENT_SECRET` or `AZURE_CLIENT_CERTIFICATE_PATH`. The secret is
/// read here and kept only inside the source.
fn environment_link(
    config: &AzureDefaultConfig,
    scope: &str,
    environment: &dyn AzureEnvironment,
    authority_host: Option<&str>,
) -> CoreResult<Box<dyn CredentialLink>> {
    let Some(credential) = read_environment_credential(environment) else {
        return Ok(unavailable(
            "AZURE_CLIENT_SECRET or AZURE_CLIENT_CERTIFICATE_PATH is not set",
        ));
    };
    let tenant_id =
        configured_or_variable(config.tenant_id.as_deref(), environment, "AZURE_TENANT_ID");
    let client_id =
        configured_or_variable(config.client_id.as_deref(), environment, "AZURE_CLIENT_ID");
    let (Some(tenant_id), Some(client_id)) = (tenant_id, client_id) else {
        return Ok(unavailable(
            "AZURE_TENANT_ID and AZURE_CLIENT_ID are not set",
        ));
    };
    let authority = checked_authority_host(authority_host, environment)?;
    let http = TokenHttpClient::new(TOKEN_REQUEST_TIMEOUT)?;
    let source: Box<dyn TokenSource> = match credential {
        EnvironmentCredential::ClientSecret(secret) => {
            Box::new(ServicePrincipalSecretTokenSource::new(
                http,
                authority,
                tenant_id,
                client_id,
                secret,
                scope.to_owned(),
            ))
        }
        EnvironmentCredential::Certificate(path) => {
            let certificate = ClientCertificate::load_from_pem_file(Path::new(&path))?;
            Box::new(ServicePrincipalCertificateTokenSource::new(
                http,
                authority,
                tenant_id,
                client_id,
                certificate,
                scope.to_owned(),
            ))
        }
    };
    Ok(Box::new(SourceLink { source }))
}

/// AKS workload identity, only when `AZURE_FEDERATED_TOKEN_FILE` is set.
fn workload_link(
    config: &AzureDefaultConfig,
    scope: &str,
    environment: &dyn AzureEnvironment,
    authority_host: Option<&str>,
) -> CoreResult<Box<dyn CredentialLink>> {
    if read_non_blank_variable(environment, "AZURE_FEDERATED_TOKEN_FILE").is_none() {
        return Ok(unavailable("AZURE_FEDERATED_TOKEN_FILE is not set"));
    }
    let identity = AzureWorkloadIdentityConfig {
        tenant_id: config.tenant_id.clone(),
        client_id: config.client_id.clone(),
        token_file: None,
        scope: None,
        authority_host: None,
    };
    let settings = match resolve_workload_identity_settings(&identity, environment) {
        Ok(settings) => settings,
        Err(error) => return Ok(unavailable(&message_of(error))),
    };
    let authority = checked_authority_host(authority_host, environment)?;
    let http = TokenHttpClient::new(TOKEN_REQUEST_TIMEOUT)?;
    let source = WorkloadIdentityTokenSource::new(
        http,
        authority,
        settings.tenant_id,
        settings.client_id,
        settings.token_file,
        scope.to_owned(),
    );
    Ok(Box::new(SourceLink {
        source: Box::new(source),
    }))
}

/// IMDS with a short probe timeout. The user-assigned identity is `client_id`, else `AZURE_CLIENT_ID`.
fn managed_identity_link(
    config: &AzureDefaultConfig,
    scope: &str,
    environment: Arc<dyn AzureEnvironment>,
    imds_base_url: String,
) -> Box<dyn CredentialLink> {
    let client_id = configured_or_variable(
        config.client_id.as_deref(),
        environment.as_ref(),
        "AZURE_CLIENT_ID",
    );
    Box::new(ManagedIdentityTokenSource::with_endpoints(
        client_id,
        resource_for_scope(scope),
        environment,
        imds_base_url,
        Some(MANAGED_IDENTITY_PROBE_TIMEOUT),
    ))
}

/// Tries the links in order: environment, workload identity, managed identity and the Azure CLI.
/// The first link that yields a token is chosen, and from then on its errors are returned as they
/// are. Before a choice, an error stops the chain; only an unavailable link is skipped.
pub(crate) struct DefaultCredentialChain {
    links: Vec<(&'static str, Box<dyn CredentialLink>)>,
    chosen: Mutex<Option<usize>>,
}

impl DefaultCredentialChain {
    /// `authority_host` replaces the environment authority; the factory passes `None`. Tests use it
    /// to reach a loopback fake Entra, which the environment rule does not allow.
    pub(crate) fn new(
        config: &AzureDefaultConfig,
        scope: &str,
        environment: Arc<dyn AzureEnvironment>,
        authority_host: Option<&str>,
        imds_base_url: String,
        azure_cli: AzureCliTokenSource,
    ) -> CoreResult<Self> {
        let links: Vec<(&'static str, Box<dyn CredentialLink>)> = vec![
            (
                "environment",
                environment_link(config, scope, environment.as_ref(), authority_host)?,
            ),
            (
                "workload identity",
                workload_link(config, scope, environment.as_ref(), authority_host)?,
            ),
            (
                "managed identity",
                managed_identity_link(config, scope, Arc::clone(&environment), imds_base_url),
            ),
            ("azure cli", Box::new(azure_cli)),
        ];
        Ok(Self {
            links,
            chosen: Mutex::new(None),
        })
    }
}

impl TokenSource for DefaultCredentialChain {
    fn fetch_access_token(&self) -> CoreResult<AccessToken> {
        let mut chosen = lock_chosen(&self.chosen);
        if let Some((_, link)) = (*chosen).and_then(|index| self.links.get(index)) {
            return link.fetch_link_token().map_err(LinkFailure::into_error);
        }
        let mut reasons = Vec::new();
        for (index, (label, link)) in self.links.iter().enumerate() {
            match link.fetch_link_token() {
                Ok(token) => {
                    *chosen = Some(index);
                    return Ok(token);
                }
                Err(LinkFailure::Unavailable(reason)) => reasons.push(format!("{label}: {reason}")),
                Err(LinkFailure::Error(error)) => return Err(error),
            }
        }
        Err(CoreError::Authentication(format!(
            "azure-default: no credential worked: {}",
            reasons.join("; ")
        )))
    }

    fn describe_source(&self) -> &'static str {
        let chosen = lock_chosen(&self.chosen);
        (*chosen)
            .and_then(|index| self.links.get(index))
            .map_or(CHAIN_SOURCE_NAME, |(_, link)| link.source_name())
    }
}

fn lock_chosen(chosen: &Mutex<Option<usize>>) -> MutexGuard<'_, Option<usize>> {
    chosen.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::azure::azure_environment::FakeAzureEnvironment;
    use crate::config::azure_credential_model::AzureDefaultConfig;
    use crate::registry::fake_registry_server::{
        FakeRegistryServer, FakeRoute, GeneratedFakeRoute,
    };
    use secrecy::ExposeSecret;
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    const SCOPE: &str = "https://ns.servicebus.windows.net/.default";
    const TOKEN_PATH: &str = "/tenant-1/oauth2/v2.0/token";
    const IMDS_PATH: &str = "/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fns.servicebus.windows.net";

    fn token_route(status: u16, body: &str) -> FakeRoute {
        FakeRoute {
            method: "POST".to_string(),
            path_and_query: TOKEN_PATH.to_string(),
            status,
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    fn closed_port_url() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{address}")
    }

    fn missing_az() -> AzureCliTokenSource {
        AzureCliTokenSource::with_program(
            PathBuf::from("/nonexistent/az"),
            None,
            SCOPE.to_string(),
            Duration::from_secs(30),
        )
    }

    fn build_chain(
        variables: &[(&str, &str)],
        authority_host: Option<String>,
        imds_base_url: String,
    ) -> DefaultCredentialChain {
        DefaultCredentialChain::new(
            &AzureDefaultConfig {
                tenant_id: None,
                client_id: None,
                scope: None,
            },
            SCOPE,
            Arc::new(FakeAzureEnvironment::new(variables)),
            authority_host.as_deref(),
            imds_base_url,
            missing_az(),
        )
        .unwrap()
    }

    fn secret_variables() -> Vec<(&'static str, &'static str)> {
        vec![
            ("AZURE_TENANT_ID", "tenant-1"),
            ("AZURE_CLIENT_ID", "app"),
            ("AZURE_CLIENT_SECRET", "client-secret-value"),
        ]
    }

    #[test]
    fn environment_secret_is_used_first() {
        let entra = FakeRegistryServer::start(vec![token_route(
            200,
            r#"{"access_token":"env-token","expires_in":3600}"#,
        )])
        .unwrap();
        let imds = FakeRegistryServer::start(Vec::new()).unwrap();
        let chain = build_chain(&secret_variables(), Some(entra.url()), imds.url());

        let token = chain.fetch_access_token().unwrap();

        assert_eq!(token.value.expose_secret(), "env-token");
        assert_eq!(entra.recorded_requests().len(), 1);
        assert!(imds.recorded_requests().is_empty());
        assert_eq!(chain.describe_source(), "azure-sp-secret");
    }

    #[test]
    fn workload_identity_is_used_when_token_file_is_set() {
        let entra = FakeRegistryServer::start(vec![token_route(
            200,
            r#"{"access_token":"aks-token","expires_in":3600}"#,
        )])
        .unwrap();
        let mut token_file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut token_file, b"federated").unwrap();
        let token_path = token_file.path().display().to_string();
        let chain = build_chain(
            &[
                ("AZURE_TENANT_ID", "tenant-1"),
                ("AZURE_CLIENT_ID", "app"),
                ("AZURE_FEDERATED_TOKEN_FILE", &token_path),
            ],
            Some(entra.url()),
            closed_port_url(),
        );

        let token = chain.fetch_access_token().unwrap();

        assert_eq!(token.value.expose_secret(), "aks-token");
        assert_eq!(chain.describe_source(), "azure-workload-identity");
    }

    #[test]
    fn off_azure_chain_fails_fast_with_every_reason() {
        let started = Instant::now();
        let chain = build_chain(&[], None, closed_port_url());

        let error = chain.fetch_access_token().unwrap_err();

        assert!(started.elapsed() < Duration::from_secs(5));
        let message = format!("{error}");
        assert!(
            message.starts_with(
                "authentication error: azure-default: no credential worked: environment: "
            ),
            "message: {message}"
        );
        for label in [
            "; workload identity: ",
            "; managed identity: ",
            "; azure cli: ",
        ] {
            assert!(message.contains(label), "message: {message}");
        }
    }

    #[test]
    fn chosen_link_errors_are_not_skipped() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let route = GeneratedFakeRoute::new(
            "POST",
            TOKEN_PATH,
            Arc::new(move || {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    (200, r#"{"access_token":"t","expires_in":3600}"#.to_string())
                } else {
                    (
                        401,
                        r#"{"error":"invalid_client","error_description":"bad secret"}"#
                            .to_string(),
                    )
                }
            }),
        );
        let entra = FakeRegistryServer::start_with_generated(Vec::new(), vec![route]).unwrap();
        let imds = FakeRegistryServer::start(Vec::new()).unwrap();
        let chain = build_chain(&secret_variables(), Some(entra.url()), imds.url());

        chain.fetch_access_token().unwrap();
        let error = chain.fetch_access_token().unwrap_err();

        assert!(
            format!("{error}").contains("azure token request failed (HTTP 401): invalid_client"),
            "error: {error}"
        );
        assert_eq!(entra.recorded_requests().len(), 2);
        assert!(imds.recorded_requests().is_empty());
    }

    #[test]
    fn entra_rejection_before_a_choice_stops_the_chain() {
        let entra = FakeRegistryServer::start(vec![token_route(
            401,
            r#"{"error":"invalid_client","error_description":"bad secret"}"#,
        )])
        .unwrap();
        let imds = FakeRegistryServer::start(Vec::new()).unwrap();
        let chain = build_chain(&secret_variables(), Some(entra.url()), imds.url());

        let error = chain.fetch_access_token().unwrap_err();

        assert!(
            format!("{error}").contains("azure token request failed (HTTP 401)"),
            "error: {error}"
        );
        assert!(imds.recorded_requests().is_empty());
    }

    #[test]
    fn imds_not_found_or_bad_request_is_unavailable() {
        for (status, body) in [(400, ""), (404, "<html>no identity</html>")] {
            let route = GeneratedFakeRoute::new(
                "GET",
                IMDS_PATH,
                Arc::new(move || (status, body.to_string())),
            );
            let imds = FakeRegistryServer::start_with_generated(Vec::new(), vec![route]).unwrap();
            let chain = build_chain(&[], None, imds.url());

            let error = chain.fetch_access_token().unwrap_err();

            let message = format!("{error}");
            assert!(
                message.contains(&format!(
                    "managed identity: azure token request failed (HTTP {status})"
                )),
                "status {status}: {message}"
            );
            assert!(
                message.contains("no credential worked"),
                "status {status}: {message}"
            );
        }
    }

    #[test]
    fn imds_server_error_is_not_unavailable() {
        let imds = FakeRegistryServer::start(vec![FakeRoute {
            method: "GET".to_string(),
            path_and_query: IMDS_PATH.to_string(),
            status: 500,
            headers: Vec::new(),
            body: r#"{"error":"server_error"}"#.to_string(),
        }])
        .unwrap();
        let chain = build_chain(&[], None, imds.url());

        let error = chain.fetch_access_token().unwrap_err();

        let message = format!("{error}");
        assert!(message.contains("(HTTP 500)"), "message: {message}");
        assert!(
            !message.contains("no credential worked"),
            "message: {message}"
        );
    }

    // The server must hold the connection without answering, which needs a sleep. The repo's
    // clippy.toml allows sleep only in tests, with a local allow (see ADR 0004).
    #[test]
    #[allow(clippy::disallowed_methods)]
    fn imds_without_answer_is_unavailable_after_probe_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                std::thread::sleep(Duration::from_secs(5));
                drop(stream);
            }
        });
        let chain = build_chain(&[], None, format!("http://{address}"));

        let started = Instant::now();
        let error = chain.fetch_access_token().unwrap_err();

        assert!(started.elapsed() < Duration::from_secs(4));
        let message = format!("{error}");
        assert!(
            message.contains("managed identity: azure token request failed: timed out"),
            "message: {message}"
        );
    }
}
