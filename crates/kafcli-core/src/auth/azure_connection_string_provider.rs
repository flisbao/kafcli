use super::client_properties::ClientProperties;
use super::provider::AuthProvider;
use super::tls_settings::apply_tls_settings;
use crate::config::model::AzureConnectionStringConfig;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;

/// Literal username Azure Event Hubs expects for connection-string authentication.
const CONNECTION_STRING_USERNAME: &str = "$ConnectionString";

/// Azure Event Hubs connection string sent as SASL/PLAIN credentials.
#[derive(Debug)]
pub struct AzureConnectionStringAuthProvider {
    config: AzureConnectionStringConfig,
}

impl AzureConnectionStringAuthProvider {
    pub fn new(config: AzureConnectionStringConfig) -> Self {
        Self { config }
    }
}

impl AuthProvider for AzureConnectionStringAuthProvider {
    fn build_client_properties(
        &self,
        resolver: &dyn SecretResolver,
    ) -> CoreResult<ClientProperties> {
        let mut properties = ClientProperties::default();
        apply_tls_settings(&mut properties, "SASL_SSL", None);
        properties.set_plain("sasl.mechanism", "PLAIN");
        properties.set_plain("sasl.username", CONNECTION_STRING_USERNAME);
        let connection_string = resolver.resolve_secret(&self.config.secret_ref)?;
        properties.set_secret("sasl.password", connection_string);
        Ok(properties)
    }

    fn describe_principal(&self) -> Option<String> {
        Some(CONNECTION_STRING_USERNAME.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::fixed_resolver::FixedSecretResolver;

    #[test]
    fn uses_connection_string_literal_as_username() {
        let provider = AzureConnectionStringAuthProvider::new(AzureConnectionStringConfig {
            secret_ref: "env:AZURE_CONNECTION_STRING".parse().unwrap(),
        });
        let properties = provider
            .build_client_properties(&FixedSecretResolver::returning("s3cret"))
            .unwrap();
        assert_eq!(
            properties.plain_value("sasl.username"),
            Some("$ConnectionString")
        );
        assert_eq!(
            properties.plain_value("security.protocol"),
            Some("SASL_SSL")
        );
        assert_eq!(properties.plain_value("sasl.mechanism"), Some("PLAIN"));
        assert!(properties.is_secret("sasl.password"));
        assert_eq!(
            provider.describe_principal(),
            Some("$ConnectionString".to_string())
        );
    }
}
