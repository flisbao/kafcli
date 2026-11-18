use super::azure::azure_oauth_provider::AzureOAuthAuthProvider;
use super::azure_connection_string_provider::AzureConnectionStringAuthProvider;
use super::bearer_provider::BearerAuthProvider;
use super::mutual_tls_provider::MutualTlsAuthProvider;
use super::oauth_oidc_provider::OAuthOidcAuthProvider;
use super::plaintext_provider::PlaintextAuthProvider;
use super::provider::AuthProvider;
use super::sasl_plain_provider::SaslPlainAuthProvider;
use super::sasl_scram_provider::{SaslScramAuthProvider, ScramMechanism};
use super::tls_provider::TlsAuthProvider;
use crate::config::azure_credential_model::AzureCredentialConfig;
use crate::config::model::AuthConfig;

/// Creates the provider for a context's authentication mode.
/// Exhaustive on purpose: a new `AuthConfig` variant must get a provider here.
pub fn create_auth_provider(auth: &AuthConfig) -> Box<dyn AuthProvider> {
    match auth {
        AuthConfig::Plaintext => Box::new(PlaintextAuthProvider),
        AuthConfig::Tls(config) => Box::new(TlsAuthProvider::new(config.clone())),
        AuthConfig::MutualTls(config) => Box::new(MutualTlsAuthProvider::new(config.clone())),
        AuthConfig::SaslPlain(config) => Box::new(SaslPlainAuthProvider::new(config.clone())),
        AuthConfig::SaslScram256(config) => Box::new(SaslScramAuthProvider::new(
            ScramMechanism::Sha256,
            config.clone(),
        )),
        AuthConfig::SaslScram512(config) => Box::new(SaslScramAuthProvider::new(
            ScramMechanism::Sha512,
            config.clone(),
        )),
        AuthConfig::AzureConnectionString(config) => {
            Box::new(AzureConnectionStringAuthProvider::new(config.clone()))
        }
        AuthConfig::Bearer(config) => Box::new(BearerAuthProvider::new(config.clone())),
        AuthConfig::OAuthOidc(config) => Box::new(OAuthOidcAuthProvider::new(config.clone())),
        AuthConfig::AzureServicePrincipalSecret(config) => Box::new(AzureOAuthAuthProvider::new(
            AzureCredentialConfig::ServicePrincipalSecret(config.clone()),
        )),
        AuthConfig::AzureServicePrincipalCertificate(config) => {
            Box::new(AzureOAuthAuthProvider::new(
                AzureCredentialConfig::ServicePrincipalCertificate(config.clone()),
            ))
        }
        AuthConfig::AzureWorkloadIdentity(config) => Box::new(AzureOAuthAuthProvider::new(
            AzureCredentialConfig::WorkloadIdentity(config.clone()),
        )),
        AuthConfig::AzureManagedIdentity(config) => Box::new(AzureOAuthAuthProvider::new(
            AzureCredentialConfig::ManagedIdentity(config.clone()),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{
        AzureConnectionStringConfig, BearerAuthConfig, MutualTlsAuthConfig, OAuthOidcAuthConfig,
        SaslCredentialsConfig, TlsAuthConfig,
    };
    use crate::error::{CoreError, CoreResult};
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use crate::secret::reference::SecretReference;
    use crate::secret::resolver::SecretResolver;
    use secrecy::SecretString;
    use std::path::PathBuf;

    struct FailingSecretResolver;

    impl SecretResolver for FailingSecretResolver {
        fn resolve_secret(&self, _reference: &SecretReference) -> CoreResult<SecretString> {
            Err(CoreError::SecretResolution("resolver refused".to_string()))
        }
    }

    fn sasl_credentials() -> SaslCredentialsConfig {
        SaslCredentialsConfig {
            username: "app".into(),
            secret_ref: "env:KAFKA_PASSWORD".parse().unwrap(),
            ca_location: None,
            use_tls: true,
        }
    }

    #[test]
    fn creates_provider_matching_each_auth_type() {
        let cases = [
            (AuthConfig::Plaintext, "PLAINTEXT"),
            (AuthConfig::Tls(TlsAuthConfig { ca_location: None }), "SSL"),
            (
                AuthConfig::MutualTls(MutualTlsAuthConfig {
                    ca_location: None,
                    certificate_location: PathBuf::from("/c.pem"),
                    key_location: PathBuf::from("/k.pem"),
                    key_password_ref: None,
                }),
                "SSL",
            ),
            (AuthConfig::SaslPlain(sasl_credentials()), "SASL_SSL"),
            (AuthConfig::SaslScram256(sasl_credentials()), "SASL_SSL"),
            (AuthConfig::SaslScram512(sasl_credentials()), "SASL_SSL"),
            (
                AuthConfig::Bearer(BearerAuthConfig {
                    token_ref: "env:KAFCLI_IT_BEARER_TOKEN".parse().unwrap(),
                    ca_location: None,
                    use_tls: true,
                }),
                "SASL_SSL",
            ),
            (
                AuthConfig::OAuthOidc(OAuthOidcAuthConfig {
                    token_endpoint_url: "https://idp.example/token".into(),
                    client_id: "kafcli".into(),
                    secret_ref: "env:KAFCLI_IT_OIDC_SECRET".parse().unwrap(),
                    scope: None,
                    ca_location: None,
                    use_tls: true,
                }),
                "SASL_SSL",
            ),
            (
                AuthConfig::AzureConnectionString(AzureConnectionStringConfig {
                    secret_ref: "env:AZURE".parse().unwrap(),
                }),
                "SASL_SSL",
            ),
        ];
        for (auth, expected_protocol) in cases {
            let provider = create_auth_provider(&auth);
            let properties = provider
                .build_client_properties(&FixedSecretResolver::returning("s3cret"))
                .unwrap();
            assert_eq!(
                properties.plain_value("security.protocol"),
                Some(expected_protocol),
                "auth type {}",
                auth.type_name()
            );
        }
    }

    #[test]
    fn propagates_secret_resolution_failure() {
        let provider = create_auth_provider(&AuthConfig::SaslPlain(sasl_credentials()));
        let error = provider
            .build_client_properties(&FailingSecretResolver)
            .unwrap_err();
        assert!(matches!(error, CoreError::SecretResolution(_)));
    }
}
