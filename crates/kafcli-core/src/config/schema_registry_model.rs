use crate::config::azure_credential_model::AzureCredentialConfig;
use crate::config::model::AuthConfig;
use crate::secret::reference::SecretReference;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
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
    /// Entra ID token for the Azure Schema Registry. Only valid on an Azure registry.
    #[serde(rename = "azure")]
    Azure(RegistryAzureAuthConfig),
}

impl RegistryAuthConfig {
    /// Returns the value of the `type` field that selects this authentication mode.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Anonymous => "none",
            Self::Basic(_) => "basic",
            Self::Bearer(_) => "bearer",
            Self::MutualTls(_) => "mtls",
            Self::Azure(_) => "azure",
        }
    }

    /// Secret references this authentication mode reads, in the order they are declared.
    pub fn secret_references(&self) -> Vec<&SecretReference> {
        match self {
            Self::Anonymous | Self::MutualTls(_) => Vec::new(),
            Self::Basic(config) => vec![&config.secret_ref],
            Self::Bearer(config) => vec![&config.token_ref],
            Self::Azure(config) => config
                .credential
                .as_ref()
                .map_or_else(Vec::new, AzureCredentialConfig::secret_references),
        }
    }
}

/// Entra ID authentication of the Azure Schema Registry. Without its own credential the registry
/// uses the Azure credential of the context's broker auth.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryAzureAuthConfig {
    /// Own `azure-*` credential, written as the same object the broker auth uses. Only the JSON
    /// configuration can set it; `--registry-auth-type azure` leaves it out.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "registry_credential_document"
    )]
    pub credential: Option<AzureCredentialConfig>,
}

/// Reads and writes the registry's own credential in the same shape as the broker auth
/// (`{"type": "azure-sp-secret", ...}`), reusing the `AuthConfig` variants that hold it.
mod registry_credential_document {
    use super::*;
    use serde::de::Error as _;
    use serde::ser::Error as _;

    pub fn serialize<S: Serializer>(
        credential: &Option<AzureCredentialConfig>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let Some(credential) = credential else {
            return serializer.serialize_none();
        };
        let document = match credential {
            AzureCredentialConfig::ServicePrincipalSecret(config) => {
                AuthConfig::AzureServicePrincipalSecret(config.clone())
            }
            AzureCredentialConfig::ServicePrincipalCertificate(config) => {
                AuthConfig::AzureServicePrincipalCertificate(config.clone())
            }
            AzureCredentialConfig::WorkloadIdentity(config) => {
                AuthConfig::AzureWorkloadIdentity(config.clone())
            }
            AzureCredentialConfig::ManagedIdentity(config) => {
                AuthConfig::AzureManagedIdentity(config.clone())
            }
            AzureCredentialConfig::AzureCli(config) => AuthConfig::AzureCli(config.clone()),
            AzureCredentialConfig::Default(config) => AuthConfig::AzureDefault(config.clone()),
        };
        document
            .serialize(serializer)
            .map_err(|error| S::Error::custom(error.to_string()))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<AzureCredentialConfig>, D::Error> {
        let Some(document) = Option::<AuthConfig>::deserialize(deserializer)? else {
            return Ok(None);
        };
        document.azure_credential().map(Some).ok_or_else(|| {
            D::Error::custom("schema registry credential must be one of the azure-* types")
        })
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
    fn azure_auth_without_credential_inherits_broker_credential() {
        let config: SchemaRegistryConfig = serde_json::from_str(
            r#"{
                "type": "azure",
                "url": "https://prd-ns.servicebus.windows.net",
                "group": "pedidos",
                "auth": {"type": "azure"}
            }"#,
        )
        .unwrap();

        assert_eq!(config.auth().type_name(), "azure");
        assert!(config.auth().secret_references().is_empty());
        assert_eq!(
            config.auth(),
            &RegistryAuthConfig::Azure(RegistryAzureAuthConfig { credential: None })
        );
    }

    #[test]
    fn azure_auth_with_own_credential_lists_its_secret_reference() {
        let config: SchemaRegistryConfig = serde_json::from_str(
            r#"{
                "type": "azure",
                "url": "https://prd-ns.servicebus.windows.net",
                "group": "pedidos",
                "auth": {
                    "type": "azure",
                    "credential": {
                        "type": "azure-sp-secret",
                        "tenant_id": "tenant-1",
                        "client_id": "app-1",
                        "secret_ref": "env:SR_SP_SECRET"
                    }
                }
            }"#,
        )
        .unwrap();

        let references: Vec<String> = config
            .auth()
            .secret_references()
            .into_iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(references, vec!["env:SR_SP_SECRET".to_string()]);
    }

    #[test]
    fn azure_auth_credential_round_trips_through_json() {
        let json = r#"{"type":"azure","credential":{"type":"azure-cli","tenant_id":"tenant-1"}}"#;
        let auth: RegistryAuthConfig = serde_json::from_str(json).unwrap();

        let written = serde_json::to_string(&auth).unwrap();

        assert_eq!(written, json);
        assert_eq!(
            serde_json::to_string(&RegistryAuthConfig::Azure(RegistryAzureAuthConfig {
                credential: None
            }))
            .unwrap(),
            r#"{"type":"azure"}"#
        );
    }

    #[test]
    fn azure_auth_refuses_credential_that_is_not_azure() {
        let error = serde_json::from_str::<RegistryAuthConfig>(
            r#"{"type":"azure","credential":{"type":"bearer","token_ref":"env:T"}}"#,
        )
        .unwrap_err()
        .to_string();

        assert!(
            error.contains("credential must be one of the azure-* types"),
            "error: {error}"
        );
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
