use super::azure_environment::{AzureEnvironment, resolve_authority_host};
use super::service_principal_secret_source::ServicePrincipalSecretTokenSource;
use super::token_http_client::{TOKEN_REQUEST_TIMEOUT, TokenHttpClient};
use crate::auth::token::cached_token_source::CachedTokenSource;
use crate::auth::token::token_source::TokenSource;
use crate::config::azure_credential_model::AzureCredentialConfig;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;
use std::sync::Arc;

/// Builds the cached token source of one Azure credential. The secret is resolved here, once,
/// and kept only inside the source.
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
                resolve_authority_host(config.authority_host.as_deref(), environment.as_ref());
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
