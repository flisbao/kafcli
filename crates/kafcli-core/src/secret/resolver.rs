use super::reference::SecretReference;
use crate::error::CoreResult;
use secrecy::SecretString;

/// Turns a secret reference into the secret value it points to.
pub trait SecretResolver {
    fn resolve_secret(&self, reference: &SecretReference) -> CoreResult<SecretString>;
}
