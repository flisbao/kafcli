use super::reference::SecretReference;
use super::resolver::SecretResolver;
use crate::error::CoreResult;
use secrecy::SecretString;

/// Test double that returns the same secret for every reference.
pub struct FixedSecretResolver {
    value: String,
}

impl FixedSecretResolver {
    pub fn returning(value: &str) -> Self {
        Self {
            value: value.to_string(),
        }
    }
}

impl SecretResolver for FixedSecretResolver {
    fn resolve_secret(&self, _reference: &SecretReference) -> CoreResult<SecretString> {
        Ok(SecretString::from(self.value.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::resolver::SecretResolver;
    use secrecy::ExposeSecret;

    #[test]
    fn returns_configured_value_for_any_reference() {
        let resolver = FixedSecretResolver::returning("fixed");
        let secret = resolver
            .resolve_secret(&"env:ANY".parse().unwrap())
            .unwrap();
        assert_eq!(secret.expose_secret(), "fixed");
    }
}
