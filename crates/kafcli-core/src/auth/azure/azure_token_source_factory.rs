use super::azure_cli_source::AzureCliTokenSource;
use super::azure_environment::{AzureEnvironment, read_non_blank_variable, resolve_authority_host};
use super::client_assertion::ClientCertificate;
use super::default_credential_chain::DefaultCredentialChain;
use super::managed_identity_source::{
    DEFAULT_IMDS_BASE_URL, ManagedIdentityTokenSource, resource_for_scope,
};
use super::service_principal_certificate_source::ServicePrincipalCertificateTokenSource;
use super::service_principal_secret_source::ServicePrincipalSecretTokenSource;
use super::token_http_client::{TOKEN_REQUEST_TIMEOUT, TokenHttpClient};
use super::workload_identity_source::WorkloadIdentityTokenSource;
use crate::auth::token::cached_token_source::CachedTokenSource;
use crate::auth::token::token_source::TokenSource;
use crate::config::azure_credential_model::{AzureCredentialConfig, AzureWorkloadIdentityConfig};
use crate::config::validation::{validate_authority_host, validate_environment_authority_host};
use crate::error::{CoreError, CoreResult};
use crate::secret::resolver::SecretResolver;
use std::path::PathBuf;
use std::sync::Arc;

/// Settings of a workload identity once the context and the AKS environment are merged.
pub(crate) struct WorkloadIdentitySettings {
    pub(crate) tenant_id: String,
    pub(crate) client_id: String,
    pub(crate) token_file: PathBuf,
}

/// Context fields win; missing ones come from `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` and
/// `AZURE_FEDERATED_TOKEN_FILE`. Any field still missing fails before any request.
pub(crate) fn resolve_workload_identity_settings(
    config: &AzureWorkloadIdentityConfig,
    environment: &dyn AzureEnvironment,
) -> CoreResult<WorkloadIdentitySettings> {
    let tenant_id = non_blank_text(config.tenant_id.as_deref())
        .or_else(|| read_non_blank_variable(environment, "AZURE_TENANT_ID"));
    let client_id = non_blank_text(config.client_id.as_deref())
        .or_else(|| read_non_blank_variable(environment, "AZURE_CLIENT_ID"));
    let token_file = config.token_file.clone().or_else(|| {
        read_non_blank_variable(environment, "AZURE_FEDERATED_TOKEN_FILE").map(PathBuf::from)
    });
    match (tenant_id, client_id, token_file) {
        (Some(tenant_id), Some(client_id), Some(token_file)) => Ok(WorkloadIdentitySettings {
            tenant_id,
            client_id,
            token_file,
        }),
        _ => Err(CoreError::Authentication(
            "azure-workload-identity needs tenant_id, client_id and token_file (or AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_FEDERATED_TOKEN_FILE)"
                .to_owned(),
        )),
    }
}

fn non_blank_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

/// Resolves the authority and checks it before any request leaves the process. A context setting
/// may use loopback HTTP (validated as for contexts); the environment value must be HTTPS.
pub(crate) fn checked_authority_host(
    configured: Option<&str>,
    environment: &dyn AzureEnvironment,
) -> CoreResult<String> {
    let configured = configured.map(str::trim).filter(|value| !value.is_empty());
    let host = resolve_authority_host(configured, environment);
    match configured {
        Some(_) => validate_authority_host(configured, false)?,
        None => validate_environment_authority_host(&host)?,
    }
    Ok(host)
}

/// Builds the cached token source of one Azure credential. The secret is resolved here, once,
/// and kept only inside the source. The certificate file is read here too, so an unreadable file
/// fails before any connection.
pub fn create_azure_token_source(
    credential: &AzureCredentialConfig,
    scope: &str,
    resolver: &dyn SecretResolver,
    environment: Arc<dyn AzureEnvironment>,
) -> CoreResult<Arc<dyn TokenSource>> {
    match credential {
        AzureCredentialConfig::ServicePrincipalSecret(config) => {
            let client_secret = resolver.resolve_secret(&config.secret_ref)?;
            let authority_host =
                checked_authority_host(config.authority_host.as_deref(), environment.as_ref())?;
            let http = TokenHttpClient::new(TOKEN_REQUEST_TIMEOUT)?;
            let source = ServicePrincipalSecretTokenSource::new(
                http,
                authority_host,
                config.tenant_id.clone(),
                config.client_id.clone(),
                client_secret,
                scope.to_owned(),
            );
            Ok(Arc::new(CachedTokenSource::new(Box::new(source))))
        }
        AzureCredentialConfig::ServicePrincipalCertificate(config) => {
            let authority_host =
                checked_authority_host(config.authority_host.as_deref(), environment.as_ref())?;
            let certificate = ClientCertificate::load_from_pem_file(&config.certificate_location)?;
            let http = TokenHttpClient::new(TOKEN_REQUEST_TIMEOUT)?;
            let source = ServicePrincipalCertificateTokenSource::new(
                http,
                authority_host,
                config.tenant_id.clone(),
                config.client_id.clone(),
                certificate,
                scope.to_owned(),
            );
            Ok(Arc::new(CachedTokenSource::new(Box::new(source))))
        }
        AzureCredentialConfig::WorkloadIdentity(config) => {
            let settings = resolve_workload_identity_settings(config, environment.as_ref())?;
            let authority_host =
                checked_authority_host(config.authority_host.as_deref(), environment.as_ref())?;
            let http = TokenHttpClient::new(TOKEN_REQUEST_TIMEOUT)?;
            let source = WorkloadIdentityTokenSource::new(
                http,
                authority_host,
                settings.tenant_id,
                settings.client_id,
                settings.token_file,
                scope.to_owned(),
            );
            Ok(Arc::new(CachedTokenSource::new(Box::new(source))))
        }
        AzureCredentialConfig::ManagedIdentity(config) => {
            let source = ManagedIdentityTokenSource::with_endpoints(
                config.client_id.clone(),
                resource_for_scope(scope),
                environment,
                DEFAULT_IMDS_BASE_URL.to_owned(),
                None,
            );
            Ok(Arc::new(CachedTokenSource::new(Box::new(source))))
        }
        AzureCredentialConfig::AzureCli(config) => {
            let source = AzureCliTokenSource::new(config.tenant_id.clone(), scope.to_owned());
            Ok(Arc::new(CachedTokenSource::new(Box::new(source))))
        }
        AzureCredentialConfig::Default(config) => {
            let chain = DefaultCredentialChain::new(
                config,
                scope,
                &environment,
                None,
                DEFAULT_IMDS_BASE_URL.to_owned(),
                AzureCliTokenSource::new(config.tenant_id.clone(), scope.to_owned()),
            )?;
            Ok(Arc::new(CachedTokenSource::new(Box::new(chain))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::azure_credential_model::{
        AzureManagedIdentityConfig, AzureServicePrincipalCertificateConfig,
        AzureServicePrincipalSecretConfig,
    };
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use std::path::PathBuf;

    const SCOPE: &str = "https://ns.servicebus.windows.net/.default";

    fn certificate_credential(certificate_location: PathBuf) -> AzureCredentialConfig {
        AzureCredentialConfig::ServicePrincipalCertificate(AzureServicePrincipalCertificateConfig {
            tenant_id: "tenant-1".to_string(),
            client_id: "app".to_string(),
            certificate_location,
            scope: None,
            authority_host: None,
        })
    }

    #[test]
    fn unreadable_certificate_fails_before_any_request() {
        let missing = PathBuf::from("/nonexistent/kafcli-test/app-cert.pem");
        let environment = Arc::new(FixedEnvironment(None));

        let error = match create_azure_token_source(
            &certificate_credential(missing),
            "https://ns.servicebus.windows.net/.default",
            &FixedSecretResolver::returning("unused"),
            environment,
        ) {
            Err(error) => error,
            Ok(_) => panic!("expected the missing certificate to be refused"),
        };

        assert!(format!("{error}").contains("could not read certificate file"));
    }

    struct FixedEnvironment(Option<String>);

    impl AzureEnvironment for FixedEnvironment {
        fn read_variable(&self, _name: &str) -> Option<String> {
            self.0.clone()
        }
    }

    fn credential(authority_host: Option<&str>) -> AzureCredentialConfig {
        AzureCredentialConfig::ServicePrincipalSecret(AzureServicePrincipalSecretConfig {
            tenant_id: "tenant-1".to_string(),
            client_id: "app".to_string(),
            secret_ref: "env:PRD_SP_SECRET".parse().unwrap(),
            scope: None,
            authority_host: authority_host.map(str::to_string),
        })
    }

    fn create_with(
        authority_host: Option<&str>,
        environment_value: Option<&str>,
    ) -> CoreResult<Arc<dyn TokenSource>> {
        create_azure_token_source(
            &credential(authority_host),
            "https://ns.servicebus.windows.net/.default",
            &FixedSecretResolver::returning("client-secret-value"),
            Arc::new(FixedEnvironment(environment_value.map(str::to_string))),
        )
    }

    #[test]
    fn environment_http_authority_is_refused_before_any_request() {
        let error = match create_with(None, Some("http://example.com")) {
            Err(error) => error,
            Ok(_) => panic!("expected the http authority to be refused"),
        };

        assert!(format!("{error}").contains("AZURE_AUTHORITY_HOST must be an https:// URL"));
    }

    #[test]
    fn whitespace_environment_authority_falls_back_to_public_cloud() {
        assert!(create_with(None, Some("  ")).is_ok());
    }

    #[test]
    fn configured_loopback_http_authority_is_accepted() {
        assert!(create_with(Some("http://127.0.0.1:1"), Some("http://example.com")).is_ok());
    }

    #[test]
    fn configured_remote_http_authority_is_refused() {
        assert!(create_with(Some("http://example.com"), None).is_err());
    }

    fn workload_credential(authority_host: Option<&str>) -> AzureCredentialConfig {
        AzureCredentialConfig::WorkloadIdentity(AzureWorkloadIdentityConfig {
            tenant_id: None,
            client_id: None,
            token_file: None,
            scope: None,
            authority_host: authority_host.map(str::to_string),
        })
    }

    #[test]
    fn workload_identity_reads_environment_defaults() {
        use crate::auth::azure::azure_environment::FakeAzureEnvironment;
        use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
        use std::io::Write;

        let server = FakeRegistryServer::start(vec![FakeRoute {
            method: "POST".to_string(),
            path_and_query: "/tenant-1/oauth2/v2.0/token".to_string(),
            status: 200,
            headers: Vec::new(),
            body: r#"{"access_token":"t","expires_in":3600}"#.to_string(),
        }])
        .unwrap();
        let mut token_file = tempfile::NamedTempFile::new().unwrap();
        token_file.write_all(b"aks-token").unwrap();
        let token_path = token_file.path().display().to_string();
        let environment = FakeAzureEnvironment::new(&[
            ("AZURE_TENANT_ID", "tenant-1"),
            ("AZURE_CLIENT_ID", "app-1"),
            ("AZURE_FEDERATED_TOKEN_FILE", &token_path),
        ]);
        let source = create_azure_token_source(
            &workload_credential(Some(&server.url())),
            SCOPE,
            &FixedSecretResolver::returning("unused"),
            Arc::new(environment),
        )
        .unwrap();

        source.fetch_access_token().unwrap();

        let body = &server.recorded_requests()[0].body;
        assert!(body.contains("client_id=app-1"), "body: {body}");
        assert!(body.ends_with("client_assertion=aks-token"), "body: {body}");
    }

    #[test]
    fn missing_workload_identity_settings_lists_variables() {
        use crate::auth::azure::azure_environment::FakeAzureEnvironment;

        let error = match create_azure_token_source(
            &workload_credential(None),
            SCOPE,
            &FixedSecretResolver::returning("unused"),
            Arc::new(FakeAzureEnvironment::new(&[])),
        ) {
            Err(error) => error,
            Ok(_) => panic!("expected the missing settings to be refused"),
        };

        assert_eq!(
            format!("{error}"),
            "authentication error: azure-workload-identity needs tenant_id, client_id and token_file (or AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_FEDERATED_TOKEN_FILE)"
        );
    }

    #[test]
    fn managed_identity_resource_drops_default_suffix() {
        use crate::auth::azure::azure_environment::FakeAzureEnvironment;
        use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};

        let server = FakeRegistryServer::start(vec![FakeRoute {
            method: "GET".to_string(),
            path_and_query:
                "/msi?api-version=2019-08-01&resource=https%3A%2F%2Fns.servicebus.windows.net"
                    .to_string(),
            status: 200,
            headers: Vec::new(),
            body: r#"{"access_token":"t","expires_in":3600}"#.to_string(),
        }])
        .unwrap();
        let endpoint = format!("{}/msi", server.url());
        let environment = FakeAzureEnvironment::new(&[
            ("IDENTITY_ENDPOINT", &endpoint),
            ("IDENTITY_HEADER", "h"),
        ]);
        let source = create_azure_token_source(
            &AzureCredentialConfig::ManagedIdentity(AzureManagedIdentityConfig {
                client_id: None,
                scope: None,
            }),
            SCOPE,
            &FixedSecretResolver::returning("unused"),
            Arc::new(environment),
        )
        .unwrap();

        source.fetch_access_token().unwrap();

        assert_eq!(server.recorded_requests().len(), 1);
    }
}
