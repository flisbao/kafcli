use super::client_properties::ClientProperties;
use super::provider::AuthProvider;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;

/// No authentication and no encryption.
#[derive(Debug, Default)]
pub struct PlaintextAuthProvider;

impl AuthProvider for PlaintextAuthProvider {
    fn build_client_properties(
        &self,
        _resolver: &dyn SecretResolver,
    ) -> CoreResult<ClientProperties> {
        let mut properties = ClientProperties::default();
        properties.set_plain("security.protocol", "PLAINTEXT");
        Ok(properties)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::fixed_resolver::FixedSecretResolver;

    #[test]
    fn configures_plaintext_protocol() {
        let properties = PlaintextAuthProvider
            .build_client_properties(&FixedSecretResolver::returning("s3cret"))
            .unwrap();
        assert_eq!(
            properties.plain_value("security.protocol"),
            Some("PLAINTEXT")
        );
        assert_eq!(
            properties.plain_value("ssl.endpoint.identification.algorithm"),
            None
        );
        assert_eq!(PlaintextAuthProvider.describe_principal(), None);
    }
}
