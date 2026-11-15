use crate::error::{CoreError, CoreResult};
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

const EXPECTED_FORMS: &str = "expected env:VAR, file:/path or keyring:service/account";

/// Where a secret lives. Parsed from `env:VAR`, `file:/path` or `keyring:service/account`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(try_from = "String", into = "String")]
pub enum SecretReference {
    Environment { variable: String },
    File { path: PathBuf },
    Keyring { service: String, account: String },
}

impl FromStr for SecretReference {
    type Err = CoreError;

    fn from_str(text: &str) -> CoreResult<Self> {
        let (scheme, body) = text.split_once(':').ok_or_else(invalid_form)?;
        match scheme {
            "env" => parse_environment_body(body),
            "file" => parse_file_body(body),
            "keyring" => parse_keyring_body(body),
            _ => Err(invalid_form()),
        }
    }
}

impl fmt::Display for SecretReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Environment { variable } => write!(formatter, "env:{variable}"),
            Self::File { path } => write!(formatter, "file:{}", path.display()),
            Self::Keyring { service, account } => write!(formatter, "keyring:{service}/{account}"),
        }
    }
}

impl TryFrom<String> for SecretReference {
    type Error = CoreError;

    fn try_from(text: String) -> CoreResult<Self> {
        text.parse()
    }
}

impl From<SecretReference> for String {
    fn from(reference: SecretReference) -> Self {
        reference.to_string()
    }
}

fn invalid_form() -> CoreError {
    CoreError::InvalidSecretReference(EXPECTED_FORMS.to_string())
}

fn parse_environment_body(body: &str) -> CoreResult<SecretReference> {
    if body.is_empty() {
        return Err(CoreError::InvalidSecretReference(
            "environment variable name after 'env:' must not be empty".to_string(),
        ));
    }
    Ok(SecretReference::Environment {
        variable: body.to_string(),
    })
}

fn parse_file_body(body: &str) -> CoreResult<SecretReference> {
    if body.is_empty() {
        return Err(CoreError::InvalidSecretReference(
            "file path after 'file:' must not be empty".to_string(),
        ));
    }
    Ok(SecretReference::File {
        path: PathBuf::from(body),
    })
}

fn parse_keyring_body(body: &str) -> CoreResult<SecretReference> {
    let (service, account) = body.split_once('/').ok_or_else(|| {
        CoreError::InvalidSecretReference(
            "keyring reference must be 'keyring:service/account'".to_string(),
        )
    })?;
    if service.is_empty() || account.is_empty() {
        return Err(CoreError::InvalidSecretReference(
            "keyring service and account must both be non-empty".to_string(),
        ));
    }
    Ok(SecretReference::Keyring {
        service: service.to_string(),
        account: account.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[test]
    fn parses_environment_reference() {
        assert_eq!(
            "env:KAFKA_PASSWORD".parse::<SecretReference>().unwrap(),
            SecretReference::Environment {
                variable: "KAFKA_PASSWORD".into()
            }
        );
    }
    #[test]
    fn parses_file_reference() {
        assert_eq!(
            "file:/run/secrets/kafka"
                .parse::<SecretReference>()
                .unwrap(),
            SecretReference::File {
                path: "/run/secrets/kafka".into()
            }
        );
    }
    #[test]
    fn parses_keyring_reference() {
        assert_eq!(
            "keyring:kafcli/prd-sp".parse::<SecretReference>().unwrap(),
            SecretReference::Keyring {
                service: "kafcli".into(),
                account: "prd-sp".into()
            }
        );
    }
    #[test]
    fn displays_reference_in_parseable_form() {
        assert_eq!(
            "keyring:kafcli/prd-sp"
                .parse::<SecretReference>()
                .unwrap()
                .to_string(),
            "keyring:kafcli/prd-sp"
        );
    }
    #[test]
    fn rejects_unknown_scheme_without_echoing_input() {
        let error = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=TOPSECRET"
            .parse::<SecretReference>()
            .unwrap_err()
            .to_string();
        assert!(!error.contains("TOPSECRET"));
        assert!(error.contains("env:") && error.contains("file:") && error.contains("keyring:"));
    }
    #[test]
    fn rejects_keyring_reference_without_account() {
        assert!("keyring:kafcli".parse::<SecretReference>().is_err());
    }
    #[test]
    fn deserializes_from_reference_string_and_rejects_bad_input_without_echo() {
        use serde::de::IntoDeserializer;
        use serde::de::value::{Error as ValueError, StringDeserializer};

        let parsed: SecretReference = SecretReference::deserialize(
            StringDeserializer::<ValueError>::new("file:/run/k".into()),
        )
        .unwrap();
        assert_eq!(
            parsed,
            SecretReference::File {
                path: "/run/k".into()
            }
        );

        let deserializer: StringDeserializer<ValueError> =
            "bad:TOPSECRET".to_string().into_deserializer();
        let error = SecretReference::deserialize(deserializer)
            .unwrap_err()
            .to_string();
        assert!(!error.contains("TOPSECRET"));
    }
    #[test]
    fn rejects_empty_variable_name() {
        assert!("env:".parse::<SecretReference>().is_err());
    }
}
