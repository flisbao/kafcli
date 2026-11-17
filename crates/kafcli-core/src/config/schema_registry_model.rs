use crate::secret::reference::SecretReference;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Schema Registry of a context, selected by the `type` field.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum SchemaRegistryConfig {
    #[serde(rename = "confluent")]
    Confluent(ConfluentRegistryConfig),
    #[serde(rename = "azure")]
    Azure(AzureRegistryConfig),
}

impl SchemaRegistryConfig {
    /// Returns the value of the `type` field that selects this registry.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Confluent(_) => "confluent",
            Self::Azure(_) => "azure",
        }
    }

    pub fn url(&self) -> &str {
        match self {
            Self::Confluent(config) => &config.url,
            Self::Azure(config) => &config.url,
        }
    }

    pub fn auth(&self) -> &RegistryAuthConfig {
        match self {
            Self::Confluent(config) => &config.auth,
            Self::Azure(config) => &config.auth,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConfluentRegistryConfig {
    pub url: String,
    #[serde(default)]
    pub auth: RegistryAuthConfig,
    pub ca_location: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AzureRegistryConfig {
    pub url: String,
    pub group: String,
    #[serde(default)]
    pub auth: RegistryAuthConfig,
}

/// Authentication of a Schema Registry, selected by the `type` field. Defaults to anonymous access.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum RegistryAuthConfig {
    #[default]
    #[serde(rename = "none")]
    Anonymous,
    #[serde(rename = "basic")]
    Basic(RegistryBasicAuthConfig),
    #[serde(rename = "bearer")]
    Bearer(RegistryBearerAuthConfig),
    #[serde(rename = "mtls")]
    MutualTls(RegistryMutualTlsConfig),
}

impl RegistryAuthConfig {
    /// Returns the value of the `type` field that selects this authentication mode.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Anonymous => "none",
            Self::Basic(_) => "basic",
            Self::Bearer(_) => "bearer",
            Self::MutualTls(_) => "mtls",
        }
    }

    /// Secret references this authentication mode reads, in the order they are declared.
    pub fn secret_references(&self) -> Vec<&SecretReference> {
        match self {
            Self::Anonymous | Self::MutualTls(_) => Vec::new(),
            Self::Basic(config) => vec![&config.secret_ref],
            Self::Bearer(config) => vec![&config.token_ref],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryBasicAuthConfig {
    pub username: String,
    pub secret_ref: SecretReference,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryBearerAuthConfig {
    pub token_ref: SecretReference,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryMutualTlsConfig {
    pub certificate_location: PathBuf,
    pub key_location: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_confluent_registry_with_basic_auth() {
        let config: SchemaRegistryConfig = serde_json::from_str(
            r#"{
                "type": "confluent",
                "url": "https://sr.example:8081",
                "auth": {"type": "basic", "username": "app", "secret_ref": "env:SR_PASSWORD"},
                "ca_location": "/etc/ca.pem"
            }"#,
        )
        .unwrap();

        assert_eq!(
            config,
            SchemaRegistryConfig::Confluent(ConfluentRegistryConfig {
                url: "https://sr.example:8081".into(),
                auth: RegistryAuthConfig::Basic(RegistryBasicAuthConfig {
                    username: "app".into(),
                    secret_ref: "env:SR_PASSWORD".parse().unwrap(),
                }),
                ca_location: Some("/etc/ca.pem".into()),
            })
        );
        assert_eq!(config.type_name(), "confluent");
    }

    #[test]
    fn deserializes_azure_registry_from_spec_example() {
        let config: SchemaRegistryConfig = serde_json::from_str(
            r#"{
                "type": "azure",
                "url": "https://prd-ns.servicebus.windows.net",
                "group": "pedidos",
                "auth": {"type": "bearer", "token_ref": "env:SR_TOKEN"}
            }"#,
        )
        .unwrap();

        assert_eq!(
            config,
            SchemaRegistryConfig::Azure(AzureRegistryConfig {
                url: "https://prd-ns.servicebus.windows.net".into(),
                group: "pedidos".into(),
                auth: RegistryAuthConfig::Bearer(RegistryBearerAuthConfig {
                    token_ref: "env:SR_TOKEN".parse().unwrap(),
                }),
            })
        );
        assert_eq!(config.type_name(), "azure");
        assert_eq!(config.auth().type_name(), "bearer");
    }

    #[test]
    fn auth_defaults_to_none() {
        let config: SchemaRegistryConfig =
            serde_json::from_str(r#"{"type": "confluent", "url": "http://sr:8081"}"#).unwrap();

        assert_eq!(config.auth(), &RegistryAuthConfig::Anonymous);
        assert_eq!(config.auth().type_name(), "none");
    }

    #[test]
    fn rejects_unknown_registry_field() {
        let error = serde_json::from_str::<SchemaRegistryConfig>(
            r#"{"type": "azure", "url": "https://ns", "group": "g", "region": "x"}"#,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("region"), "error: {error}");
    }
}
