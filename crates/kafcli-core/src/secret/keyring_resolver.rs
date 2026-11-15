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
        match password {
            Ok(value) if !value.is_empty() => Ok(SecretString::from(value)),
            Ok(_) => Err(CoreError::SecretResolution(format!(
                "keyring entry {service}/{account} is empty"
            ))),
            Err(_) => Err(CoreError::SecretResolution(format!(
                "keyring unavailable for {service}/{account}; use env: or file: instead"
            ))),
        }
    }
}
