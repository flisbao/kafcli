use super::client_properties::ClientProperties;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;

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
}
