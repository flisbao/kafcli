use super::azure_environment::{AzureEnvironment, resolve_authority_host};
use super::service_principal_secret_source::ServicePrincipalSecretTokenSource;
use super::token_http_client::{TOKEN_REQUEST_TIMEOUT, TokenHttpClient};
use crate::auth::token::cached_token_source::CachedTokenSource;
use crate::auth::token::token_source::TokenSource;
use crate::config::azure_credential_model::AzureCredentialConfig;
use crate::config::validation::{validate_authority_host, validate_environment_authority_host};
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;
use std::sync::Arc;

/// Builds the cached token source of one Azure credential. The secret is resolved here, once,
/// and kept only inside the source.
/// Resolves the authority and checks it before any request leaves the process. A context setting
/// may use loopback HTTP (validated as for contexts); the environment value must be HTTPS.
fn checked_authority_host(
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::azure_credential_model::AzureServicePrincipalSecretConfig;
    use crate::secret::fixed_resolver::FixedSecretResolver;

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
}
