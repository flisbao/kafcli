use super::client_properties::ClientProperties;
use super::provider::AuthProvider;
use super::tls_settings::apply_tls_settings;
use crate::config::model::SaslCredentialsConfig;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;

/// SASL/PLAIN with a username and a secret password.
#[derive(Debug)]
pub struct SaslPlainAuthProvider {
    config: SaslCredentialsConfig,
}

impl SaslPlainAuthProvider {
    pub fn new(config: SaslCredentialsConfig) -> Self {
        Self { config }
    }
}

impl AuthProvider for SaslPlainAuthProvider {
    fn build_client_properties(
        &self,
        resolver: &dyn SecretResolver,
    ) -> CoreResult<ClientProperties> {
        let mut properties = ClientProperties::default();
        let security_protocol = if self.config.use_tls {
            "SASL_SSL"
        } else {
            "SASL_PLAINTEXT"
        };
        apply_tls_settings(
            &mut properties,
            security_protocol,
            self.config.ca_location.as_deref(),
        );
        properties.set_plain("sasl.mechanism", "PLAIN");
        properties.set_plain("sasl.username", self.config.username.as_str());
        let password = resolver.resolve_secret(&self.config.secret_ref)?;
        properties.set_secret("sasl.password", password);
        Ok(properties)
    }

    fn describe_principal(&self) -> Option<String> {
        Some(self.config.username.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::fixed_resolver::FixedSecretResolver;

    #[test]
    fn configures_plain_mechanism_with_username() {
        let provider = SaslPlainAuthProvider::new(SaslCredentialsConfig {
            username: "app".into(),
            secret_ref: "env:KAFKA_PASSWORD".parse().unwrap(),
            ca_location: None,
            use_tls: true,
        });
        let properties = provider
            .build_client_properties(&FixedSecretResolver::returning("s3cret"))
            .unwrap();
        assert_eq!(
            properties.plain_value("security.protocol"),
            Some("SASL_SSL")
        );
        assert_eq!(properties.plain_value("sasl.mechanism"), Some("PLAIN"));
        assert_eq!(properties.plain_value("sasl.username"), Some("app"));
        assert!(properties.is_secret("sasl.password"));
        assert_eq!(provider.describe_principal(), Some("app".to_string()));
    }
}
