use super::client_properties::ClientProperties;
use super::token::token_source::TokenSource;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;
use std::sync::Arc;

/// Builds the librdkafka properties that one authentication mode requires.
pub trait AuthProvider {
    fn build_client_properties(
        &self,
        resolver: &dyn SecretResolver,
    ) -> CoreResult<ClientProperties>;

    /// Displayable name of the configured principal (never the secret).
    fn describe_principal(&self) -> Option<String> {
        None
    }

    /// Token source for OAUTHBEARER. Modes without a bearer token have none.
    fn create_token_source(
        &self,
        brokers: &[String],
        resolver: &dyn SecretResolver,
    ) -> CoreResult<Option<Arc<dyn TokenSource>>> {
        let _ = (brokers, resolver);
        Ok(None)
    }
}
