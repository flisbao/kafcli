use crate::secret::reference::SecretReference;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Whole kafcli configuration file: the default profile and every named profile.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct Configuration {
    pub default_profile: Option<String>,
    #[serde(default)]
    pub profiles: BTreeMap<String, Profile>,
}

/// A named Kafka cluster connection.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub brokers: Vec<String>,
    pub auth: AuthConfig,
    #[serde(default)]
    pub protected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_registry: Option<toml::Table>,
}

/// Authentication mode of a profile, selected by the `type` field.
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

fn default_use_tls() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_name_matches_serialized_tag() {
        let profile_auth = AuthConfig::SaslScram512(SaslCredentialsConfig {
            username: "app".into(),
            secret_ref: "env:KAFKA_PASSWORD".parse().unwrap(),
            ca_location: None,
            use_tls: true,
        });
        assert_eq!(profile_auth.type_name(), "sasl-scram-512");
        assert_eq!(AuthConfig::Plaintext.type_name(), "none");
    }
}
