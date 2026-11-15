use super::client_properties::ClientProperties;
use super::provider::AuthProvider;
use super::tls_settings::apply_tls_settings;
use crate::config::model::MutualTlsAuthConfig;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;

/// TLS with a client certificate and key.
#[derive(Debug)]
pub struct MutualTlsAuthProvider {
    config: MutualTlsAuthConfig,
}

impl MutualTlsAuthProvider {
    pub fn new(config: MutualTlsAuthConfig) -> Self {
        Self { config }
    }
}

impl AuthProvider for MutualTlsAuthProvider {
    fn build_client_properties(
        &self,
        resolver: &dyn SecretResolver,
    ) -> CoreResult<ClientProperties> {
        let mut properties = ClientProperties::default();
        apply_tls_settings(&mut properties, "SSL", self.config.ca_location.as_deref());
        properties.set_plain(
            "ssl.certificate.location",
            self.config.certificate_location.to_string_lossy(),
        );
        properties.set_plain(
            "ssl.key.location",
            self.config.key_location.to_string_lossy(),
        );
        if let Some(reference) = &self.config.key_password_ref {
            let password = resolver.resolve_secret(reference)?;
            properties.set_secret("ssl.key.password", password);
        }
        Ok(properties)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use crate::secret::reference::SecretReference;
    use std::path::PathBuf;

    fn mutual_tls_config(key_password_ref: Option<SecretReference>) -> MutualTlsAuthConfig {
        MutualTlsAuthConfig {
            ca_location: Some(PathBuf::from("/etc/kafka/ca.pem")),
            certificate_location: PathBuf::from("/etc/kafka/client.pem"),
            key_location: PathBuf::from("/etc/kafka/client.key"),
            key_password_ref,
        }
    }

    #[test]
    fn configures_client_certificate_and_key() {
        let provider = MutualTlsAuthProvider::new(mutual_tls_config(None));
        let properties = provider
            .build_client_properties(&FixedSecretResolver::returning("s3cret"))
            .unwrap();
        assert_eq!(properties.plain_value("security.protocol"), Some("SSL"));
        assert_eq!(
            properties.plain_value("ssl.certificate.location"),
            Some("/etc/kafka/client.pem")
        );
        assert_eq!(
            properties.plain_value("ssl.key.location"),
            Some("/etc/kafka/client.key")
        );
        assert_eq!(
            properties.plain_value("ssl.ca.location"),
            Some("/etc/kafka/ca.pem")
        );
        assert!(!properties.is_secret("ssl.key.password"));
    }

    #[test]
    fn resolves_key_password_as_secret() {
        let reference: SecretReference = "env:KEY_PASSWORD".parse().unwrap();
        let provider = MutualTlsAuthProvider::new(mutual_tls_config(Some(reference)));
        let properties = provider
            .build_client_properties(&FixedSecretResolver::returning("s3cret"))
            .unwrap();
        assert!(properties.is_secret("ssl.key.password"));
    }
}
