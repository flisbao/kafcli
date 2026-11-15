use super::client_properties::ClientProperties;
use super::provider::AuthProvider;
use super::tls_settings::apply_tls_settings;
use crate::config::model::TlsAuthConfig;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;

/// Server-authenticated TLS.
#[derive(Debug)]
pub struct TlsAuthProvider {
    config: TlsAuthConfig,
}

impl TlsAuthProvider {
    pub fn new(config: TlsAuthConfig) -> Self {
        Self { config }
    }
}

impl AuthProvider for TlsAuthProvider {
    fn build_client_properties(
        &self,
        _resolver: &dyn SecretResolver,
    ) -> CoreResult<ClientProperties> {
        let mut properties = ClientProperties::default();
        apply_tls_settings(&mut properties, "SSL", self.config.ca_location.as_deref());
        Ok(properties)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use std::path::PathBuf;

    #[test]
    fn sets_ca_location_and_hostname_verification() {
        let provider = TlsAuthProvider::new(TlsAuthConfig {
            ca_location: Some(PathBuf::from("/etc/kafka/ca.pem")),
        });
        let properties = provider
            .build_client_properties(&FixedSecretResolver::returning("s3cret"))
            .unwrap();
        assert_eq!(properties.plain_value("security.protocol"), Some("SSL"));
        assert_eq!(
            properties.plain_value("ssl.ca.location"),
            Some("/etc/kafka/ca.pem")
        );
        assert_eq!(
            properties.plain_value("ssl.endpoint.identification.algorithm"),
            Some("https")
        );
    }

    #[test]
    fn omits_ca_location_when_not_configured() {
        let provider = TlsAuthProvider::new(TlsAuthConfig { ca_location: None });
        let properties = provider
            .build_client_properties(&FixedSecretResolver::returning("s3cret"))
            .unwrap();
        assert_eq!(properties.plain_value("ssl.ca.location"), None);
    }
}
