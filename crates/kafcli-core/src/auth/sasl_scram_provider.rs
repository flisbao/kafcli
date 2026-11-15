use super::client_properties::ClientProperties;
use super::provider::AuthProvider;
use super::tls_settings::apply_tls_settings;
use crate::config::model::SaslCredentialsConfig;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;

/// SCRAM hash function used by SASL/SCRAM.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScramMechanism {
    Sha256,
    Sha512,
}

impl ScramMechanism {
    /// Name of the mechanism as librdkafka expects it in `sasl.mechanism`.
    pub fn sasl_mechanism_name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }
}

/// SASL/SCRAM with a username and a secret password.
#[derive(Debug)]
pub struct SaslScramAuthProvider {
    mechanism: ScramMechanism,
    config: SaslCredentialsConfig,
}

impl SaslScramAuthProvider {
    pub fn new(mechanism: ScramMechanism, config: SaslCredentialsConfig) -> Self {
        Self { mechanism, config }
    }
}

impl AuthProvider for SaslScramAuthProvider {
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
        properties.set_plain("sasl.mechanism", self.mechanism.sasl_mechanism_name());
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

    fn scram_config(use_tls: bool) -> SaslCredentialsConfig {
        SaslCredentialsConfig {
            username: "app".into(),
            secret_ref: "env:KAFKA_PASSWORD".parse().unwrap(),
            ca_location: None,
            use_tls,
        }
    }

    #[test]
    fn configures_scram_sha_512_over_tls() {
        let provider = SaslScramAuthProvider::new(ScramMechanism::Sha512, scram_config(true));
        let properties = provider
            .build_client_properties(&FixedSecretResolver::returning("s3cret"))
            .unwrap();
        assert_eq!(
            properties.plain_value("security.protocol"),
            Some("SASL_SSL")
        );
        assert_eq!(
            properties.plain_value("sasl.mechanism"),
            Some("SCRAM-SHA-512")
        );
        assert_eq!(properties.plain_value("sasl.username"), Some("app"));
        assert!(properties.is_secret("sasl.password"));
        assert_eq!(
            properties.plain_value("ssl.endpoint.identification.algorithm"),
            Some("https")
        );
    }

    #[test]
    fn uses_sasl_plaintext_when_tls_disabled() {
        let provider = SaslScramAuthProvider::new(ScramMechanism::Sha256, scram_config(false));
        let properties = provider
            .build_client_properties(&FixedSecretResolver::returning("s3cret"))
            .unwrap();
        assert_eq!(
            properties.plain_value("security.protocol"),
            Some("SASL_PLAINTEXT")
        );
        assert_eq!(
            properties.plain_value("sasl.mechanism"),
            Some("SCRAM-SHA-256")
        );
        assert_eq!(
            properties.plain_value("ssl.endpoint.identification.algorithm"),
            None
        );
        assert_eq!(properties.plain_value("ssl.ca.location"), None);
    }

    #[test]
    fn describes_principal_as_username() {
        let provider = SaslScramAuthProvider::new(ScramMechanism::Sha256, scram_config(true));
        assert_eq!(provider.describe_principal(), Some("app".to_string()));
    }
}
