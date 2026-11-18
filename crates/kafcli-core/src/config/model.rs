use crate::config::schema_registry_model::SchemaRegistryConfig;
use crate::secret::reference::SecretReference;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Whole kafcli configuration file: the current context and every named context.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct Configuration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_context: Option<String>,
    #[serde(default)]
    pub contexts: BTreeMap<String, ConnectionContext>,
}

/// A named Kafka cluster connection.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionContext {
    pub brokers: Vec<String>,
    pub auth: AuthConfig,
    #[serde(default)]
    pub protected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_registry: Option<SchemaRegistryConfig>,
}

/// Authentication mode of a context, selected by the `type` field.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum AuthConfig {
    #[serde(rename = "none")]
    Plaintext,
    #[serde(rename = "tls")]
    Tls(TlsAuthConfig),
    #[serde(rename = "mtls")]
    MutualTls(MutualTlsAuthConfig),
    #[serde(rename = "sasl-plain")]
    SaslPlain(SaslCredentialsConfig),
    #[serde(rename = "sasl-scram-256")]
    SaslScram256(SaslCredentialsConfig),
    #[serde(rename = "sasl-scram-512")]
    SaslScram512(SaslCredentialsConfig),
    #[serde(rename = "azure-connection-string")]
    AzureConnectionString(AzureConnectionStringConfig),
    #[serde(rename = "bearer")]
    Bearer(BearerAuthConfig),
    #[serde(rename = "oauth-oidc")]
    OAuthOidc(OAuthOidcAuthConfig),
}

impl AuthConfig {
    /// Returns the value of the `type` field that selects this authentication mode.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Plaintext => "none",
            Self::Tls(_) => "tls",
            Self::MutualTls(_) => "mtls",
            Self::SaslPlain(_) => "sasl-plain",
            Self::SaslScram256(_) => "sasl-scram-256",
            Self::SaslScram512(_) => "sasl-scram-512",
            Self::AzureConnectionString(_) => "azure-connection-string",
            Self::Bearer(_) => "bearer",
            Self::OAuthOidc(_) => "oauth-oidc",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TlsAuthConfig {
    pub ca_location: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutualTlsAuthConfig {
    pub ca_location: Option<PathBuf>,
    pub certificate_location: PathBuf,
    pub key_location: PathBuf,
    pub key_password_ref: Option<SecretReference>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SaslCredentialsConfig {
    pub username: String,
    pub secret_ref: SecretReference,
    pub ca_location: Option<PathBuf>,
    /// `false` selects SASL_PLAINTEXT, intended for local development only.
    #[serde(default = "default_use_tls")]
    pub use_tls: bool,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AzureConnectionStringConfig {
    pub secret_ref: SecretReference,
}

/// OAUTHBEARER with a bearer token read from `token_ref`.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BearerAuthConfig {
    pub token_ref: SecretReference,
    pub ca_location: Option<PathBuf>,
    /// `false` selects SASL_PLAINTEXT, intended for local development only.
    #[serde(default = "default_use_tls")]
    pub use_tls: bool,
}

/// OAUTHBEARER with a token that librdkafka fetches from an OpenID Connect token endpoint
/// (client credentials grant) and refreshes on its own.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthOidcAuthConfig {
    pub token_endpoint_url: String,
    pub client_id: String,
    /// Client secret. Only the reference is stored, never the value.
    pub secret_ref: SecretReference,
    pub scope: Option<String>,
    pub ca_location: Option<PathBuf>,
    /// `false` selects SASL_PLAINTEXT, intended for local development only.
    #[serde(default = "default_use_tls")]
    pub use_tls: bool,
}

fn default_use_tls() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_name_matches_serialized_tag() {
        let context_auth = AuthConfig::SaslScram512(SaslCredentialsConfig {
            username: "app".into(),
            secret_ref: "env:KAFKA_PASSWORD".parse().unwrap(),
            ca_location: None,
            use_tls: true,
        });
        assert_eq!(context_auth.type_name(), "sasl-scram-512");
        assert_eq!(AuthConfig::Plaintext.type_name(), "none");
    }

    #[test]
    fn bearer_round_trips_through_json() {
        let json =
            r#"{"type":"bearer","token_ref":"env:KAFCLI_IT_BEARER_TOKEN","ca_location":null}"#;

        let auth: AuthConfig = serde_json::from_str(json).unwrap();

        assert_eq!(auth.type_name(), "bearer");
        assert!(matches!(&auth, AuthConfig::Bearer(config) if config.use_tls));
        let written = serde_json::to_string(&auth).unwrap();
        let reread: AuthConfig = serde_json::from_str(&written).unwrap();
        assert_eq!(reread, auth);
    }

    #[test]
    fn bearer_rejects_unknown_field() {
        let json = r#"{"type":"bearer","token_ref":"env:TOKEN","surprise":true}"#;

        assert!(serde_json::from_str::<AuthConfig>(json).is_err());
    }
}
