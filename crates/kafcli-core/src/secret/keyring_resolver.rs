use super::reference::SecretReference;
use super::resolver::SecretResolver;
use crate::error::{CoreError, CoreResult};
use secrecy::SecretString;

/// Resolves `keyring:service/account` references through the OS credential store.
pub struct KeyringSecretResolver;

impl SecretResolver for KeyringSecretResolver {
    fn resolve_secret(&self, reference: &SecretReference) -> CoreResult<SecretString> {
        let SecretReference::Keyring { service, account } = reference else {
            return Err(CoreError::SecretResolution(
                "keyring resolver only handles keyring: references".to_string(),
            ));
        };
        let password = keyring::Entry::new(service, account).and_then(|entry| entry.get_password());
        map_keyring_result(service, account, password)
    }
}

/// Turns the keyring outcome into a resolution result.
///
/// Backend error text is never copied into the message: it can echo keyring attributes.
fn map_keyring_result(
    service: &str,
    account: &str,
    password: keyring::Result<String>,
) -> CoreResult<SecretString> {
    match password {
        Ok(value) if !value.is_empty() => Ok(SecretString::from(value)),
        Ok(_) => Err(CoreError::SecretResolution(format!(
            "keyring entry {service}/{account} is empty"
        ))),
        Err(keyring::Error::NoEntry) => Err(CoreError::SecretResolution(format!(
            "no keyring entry for {service}/{account}"
        ))),
        Err(_) => Err(CoreError::SecretResolution(format!(
            "keyring unavailable for {service}/{account}; use env: or file: instead"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    #[test]
    fn maps_missing_entry_to_no_entry_message() {
        let error = map_keyring_result("kafcli", "prd", Err(keyring::Error::NoEntry))
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "secret could not be resolved: no keyring entry for kafcli/prd"
        );
    }

    #[test]
    fn maps_other_failures_without_backend_text() {
        let backend_failure = keyring::Error::Invalid(
            "attribute".to_string(),
            "BACKEND-DETAIL-THAT-MUST-NOT-LEAK".to_string(),
        );

        let error = map_keyring_result("kafcli", "prd", Err(backend_failure))
            .unwrap_err()
            .to_string();

        assert!(error.contains("keyring unavailable for kafcli/prd"));
        assert!(error.contains("use env: or file:"));
        assert!(!error.contains("BACKEND-DETAIL"));
    }

    #[test]
    fn maps_stored_value_to_secret() {
        let secret = map_keyring_result("kafcli", "prd", Ok("value".to_string())).unwrap();

        assert_eq!(secret.expose_secret(), "value");
    }
}
