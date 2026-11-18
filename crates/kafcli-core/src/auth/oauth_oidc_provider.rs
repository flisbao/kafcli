use super::client_properties::ClientProperties;
use super::oauth_bearer_settings::apply_oauth_bearer_settings;
use super::provider::AuthProvider;
use crate::config::model::OAuthOidcAuthConfig;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;

/// OAUTHBEARER with `sasl.oauthbearer.method=oidc`. librdkafka fetches the token from the token
/// endpoint and refreshes it on its own, so kafcli serves no token source.
#[derive(Debug)]
pub struct OAuthOidcAuthProvider {
    config: OAuthOidcAuthConfig,
}

impl OAuthOidcAuthProvider {
    pub fn new(config: OAuthOidcAuthConfig) -> Self {
        Self { config }
    }
}

impl AuthProvider for OAuthOidcAuthProvider {
    fn build_client_properties(
        &self,
        resolver: &dyn SecretResolver,
    ) -> CoreResult<ClientProperties> {
        let mut properties = ClientProperties::default();
        apply_oauth_bearer_settings(
            &mut properties,
            self.config.use_tls,
            self.config.ca_location.as_deref(),
        );
        properties.set_plain("sasl.oauthbearer.method", "oidc");
        properties.set_plain("sasl.oauthbearer.client.id", self.config.client_id.as_str());
        let client_secret = resolver.resolve_secret(&self.config.secret_ref)?;
        properties.set_secret("sasl.oauthbearer.client.secret", client_secret);
        properties.set_plain(
            "sasl.oauthbearer.token.endpoint.url",
            self.config.token_endpoint_url.as_str(),
        );
        if let Some(scope) = &self.config.scope {
            properties.set_plain("sasl.oauthbearer.scope", scope.as_str());
        }
        // librdkafka verifies the token endpoint with https.ca.location, not ssl.ca.location.
        if let Some(location) = &self.config.ca_location {
            properties.set_plain("https.ca.location", location.to_string_lossy());
        }
        Ok(properties)
    }

    fn describe_principal(&self) -> Option<String> {
        Some(self.config.client_id.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use crate::secret::reference::SecretReference;

    fn oidc_config(token_endpoint_url: &str, scope: Option<&str>) -> OAuthOidcAuthConfig {
        OAuthOidcAuthConfig {
            token_endpoint_url: token_endpoint_url.to_string(),
            client_id: "kafcli-client".to_string(),
            secret_ref: "env:KAFCLI_IT_OIDC_SECRET"
                .parse::<SecretReference>()
                .unwrap(),
            scope: scope.map(str::to_string),
            ca_location: None,
            use_tls: true,
        }
    }

    #[test]
    fn oidc_sets_native_refresh_properties() {
        let provider =
            OAuthOidcAuthProvider::new(oidc_config("https://idp.example/token", Some("kafka")));

        let properties = provider
            .build_client_properties(&FixedSecretResolver::returning("client-secret-value"))
            .unwrap();

        assert_eq!(
            properties.plain_value("security.protocol"),
            Some("SASL_SSL")
        );
        assert_eq!(
            properties.plain_value("sasl.mechanism"),
            Some("OAUTHBEARER")
        );
        assert_eq!(
            properties.plain_value("sasl.oauthbearer.method"),
            Some("oidc")
        );
        assert_eq!(
            properties.plain_value("sasl.oauthbearer.client.id"),
            Some("kafcli-client")
        );
        assert!(properties.is_secret("sasl.oauthbearer.client.secret"));
        assert_eq!(
            properties.plain_value("sasl.oauthbearer.token.endpoint.url"),
            Some("https://idp.example/token")
        );
        assert_eq!(
            properties.plain_value("sasl.oauthbearer.scope"),
            Some("kafka")
        );
    }

    #[test]
    fn oidc_sets_token_endpoint_ca_when_configured() {
        let mut config = oidc_config("https://idp.example/token", None);
        config.ca_location = Some(std::path::PathBuf::from("/etc/kafcli/idp-ca.pem"));
        let provider = OAuthOidcAuthProvider::new(config);

        let properties = provider
            .build_client_properties(&FixedSecretResolver::returning("client-secret-value"))
            .unwrap();

        assert_eq!(
            properties.plain_value("https.ca.location"),
            Some("/etc/kafcli/idp-ca.pem")
        );
        assert_eq!(
            properties.plain_value("ssl.ca.location"),
            Some("/etc/kafcli/idp-ca.pem")
        );
    }

    #[test]
    fn oidc_omits_token_endpoint_ca_when_not_configured() {
        let properties = OAuthOidcAuthProvider::new(oidc_config("https://idp.example/token", None))
            .build_client_properties(&FixedSecretResolver::returning("client-secret-value"))
            .unwrap();

        assert_eq!(properties.plain_value("https.ca.location"), None);
    }

    #[test]
    fn oidc_omits_scope_when_not_configured() {
        let provider = OAuthOidcAuthProvider::new(oidc_config("https://idp.example/token", None));

        let properties = provider
            .build_client_properties(&FixedSecretResolver::returning("client-secret-value"))
            .unwrap();

        assert_eq!(properties.plain_value("sasl.oauthbearer.scope"), None);
        assert!(!properties.is_secret("sasl.oauthbearer.scope"));
    }

    #[test]
    fn oidc_principal_is_client_id() {
        let provider = OAuthOidcAuthProvider::new(oidc_config("https://idp.example/token", None));

        assert_eq!(
            provider.describe_principal().as_deref(),
            Some("kafcli-client")
        );
    }
}
