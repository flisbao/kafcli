use crate::secret::reference::SecretReference;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Azure credential of a context. Serialized through the `AuthConfig` variants, one per credential.
#[derive(Clone, Debug, PartialEq)]
pub enum AzureCredentialConfig {
    ServicePrincipalSecret(AzureServicePrincipalSecretConfig),
    ServicePrincipalCertificate(AzureServicePrincipalCertificateConfig),
    WorkloadIdentity(AzureWorkloadIdentityConfig),
    ManagedIdentity(AzureManagedIdentityConfig),
}

impl AzureCredentialConfig {
    /// Returns the value of the `type` field that selects this credential.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::ServicePrincipalSecret(_) => "azure-sp-secret",
            Self::ServicePrincipalCertificate(_) => "azure-sp-cert",
            Self::WorkloadIdentity(_) => "azure-workload-identity",
            Self::ManagedIdentity(_) => "azure-managed-identity",
        }
    }

    /// Scope configured by the user. `None` lets the scope be derived from the brokers.
    pub fn scope(&self) -> Option<&str> {
        match self {
            Self::ServicePrincipalSecret(config) => config.scope.as_deref(),
            Self::ServicePrincipalCertificate(config) => config.scope.as_deref(),
            Self::WorkloadIdentity(config) => config.scope.as_deref(),
            Self::ManagedIdentity(config) => config.scope.as_deref(),
        }
    }

    /// Every secret reference of the credential, for `context show`.
    pub fn secret_references(&self) -> Vec<&SecretReference> {
        match self {
            Self::ServicePrincipalSecret(config) => vec![&config.secret_ref],
            Self::ServicePrincipalCertificate(_)
            | Self::WorkloadIdentity(_)
            | Self::ManagedIdentity(_) => Vec::new(),
        }
    }
}

/// Entra ID service principal that authenticates with a client secret (client credentials grant).
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AzureServicePrincipalSecretConfig {
    pub tenant_id: String,
    pub client_id: String,
    /// Client secret. Only the reference is stored, never the value.
    pub secret_ref: SecretReference,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// Entra ID authority, `https://`. Defaults to the public cloud or `AZURE_AUTHORITY_HOST`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_host: Option<String>,
}

/// Entra ID service principal that authenticates with a certificate: a signed RS256 client assertion.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AzureServicePrincipalCertificateConfig {
    pub tenant_id: String,
    pub client_id: String,
    /// PEM file with the application certificate and its unencrypted RSA private key.
    pub certificate_location: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// Entra ID authority, `https://`. Defaults to the public cloud or `AZURE_AUTHORITY_HOST`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_host: Option<String>,
}

/// Workload identity of a pod (AKS). Missing fields come from `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`
/// and `AZURE_FEDERATED_TOKEN_FILE`, which the AKS webhook injects.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AzureWorkloadIdentityConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// File with the projected service account token. It is read again on every token fetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// Entra ID authority, `https://`. Defaults to the public cloud or `AZURE_AUTHORITY_HOST`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_host: Option<String>,
}

/// Managed identity of the host: the user-assigned identity `client_id`, or the system-assigned one
/// when `client_id` is absent. Tokens come from the App Service endpoint or from IMDS.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AzureManagedIdentityConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

#[cfg(test)]
mod tests {
    use crate::config::model::AuthConfig;
    use serde_json::{Value, json};

    fn prd_eh_auth_json() -> Value {
        json!({
            "type": "azure-sp-secret",
            "tenant_id": "00000000-0000-0000-0000-000000000000",
            "client_id": "11111111-1111-1111-1111-111111111111",
            "secret_ref": "env:PRD_SP_SECRET"
        })
    }

    #[test]
    fn azure_sp_secret_json_matches_spec_example() {
        let original = prd_eh_auth_json();

        let auth: AuthConfig = serde_json::from_value(original.clone()).unwrap();

        assert_eq!(auth.type_name(), "azure-sp-secret");
        assert_eq!(serde_json::to_value(&auth).unwrap(), original);
        let reread: AuthConfig =
            serde_json::from_value(serde_json::to_value(&auth).unwrap()).unwrap();
        assert_eq!(reread, auth);
    }

    #[test]
    fn auth_config_rebuilds_the_azure_credential() {
        let auth: AuthConfig = serde_json::from_value(prd_eh_auth_json()).unwrap();

        let credential = auth.azure_credential().unwrap();

        assert_eq!(credential.type_name(), "azure-sp-secret");
        assert_eq!(credential.scope(), None);
        assert_eq!(credential.secret_references().len(), 1);
        assert!(AuthConfig::Plaintext.azure_credential().is_none());
    }

    #[test]
    fn azure_sp_secret_rejects_unknown_field() {
        let mut json = prd_eh_auth_json();
        json["surprise"] = json!(true);

        assert!(serde_json::from_value::<AuthConfig>(json).is_err());
    }
}
