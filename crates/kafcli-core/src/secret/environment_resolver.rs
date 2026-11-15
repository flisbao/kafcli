use super::reference::SecretReference;
use super::resolver::SecretResolver;
use crate::error::{CoreError, CoreResult};
use secrecy::SecretString;

type VariableLookup = Box<dyn Fn(&str) -> Option<String>>;

/// Resolves `env:VAR` references through an injectable variable lookup.
pub struct EnvironmentSecretResolver {
    lookup: VariableLookup,
}

impl EnvironmentSecretResolver {
    pub fn from_process_environment() -> Self {
        Self::with_lookup(|name| std::env::var(name).ok())
    }

    pub fn with_lookup(lookup: impl Fn(&str) -> Option<String> + 'static) -> Self {
        Self {
            lookup: Box::new(lookup),
        }
    }
}

impl SecretResolver for EnvironmentSecretResolver {
    fn resolve_secret(&self, reference: &SecretReference) -> CoreResult<SecretString> {
        let SecretReference::Environment { variable } = reference else {
            return Err(CoreError::SecretResolution(
                "environment resolver only handles env: references".to_string(),
            ));
        };
        match (self.lookup)(variable) {
            Some(value) if !value.is_empty() => Ok(SecretString::from(value)),
            Some(_) => Err(CoreError::SecretResolution(format!(
                "environment variable '{variable}' is empty"
            ))),
            None => Err(CoreError::SecretResolution(format!(
                "environment variable '{variable}' is not set"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::reference::SecretReference;
    use crate::secret::resolver::SecretResolver;
    use secrecy::ExposeSecret;

    #[test]
    fn resolves_variable_value() {
        let resolver = EnvironmentSecretResolver::with_lookup(|name| {
            (name == "KAFKA_PASSWORD").then(|| "s3cret".to_string())
        });
        let secret = resolver
            .resolve_secret(&"env:KAFKA_PASSWORD".parse().unwrap())
            .unwrap();
        assert_eq!(secret.expose_secret(), "s3cret");
    }
    #[test]
    fn reports_missing_variable_by_name() {
        let resolver = EnvironmentSecretResolver::with_lookup(|_| None);
        let error = resolver
            .resolve_secret(&"env:KAFKA_PASSWORD".parse().unwrap())
            .unwrap_err()
            .to_string();
        assert!(error.contains("KAFKA_PASSWORD"));
    }
    #[test]
    fn rejects_empty_variable_value() {
        let resolver = EnvironmentSecretResolver::with_lookup(|_| Some(String::new()));
        assert!(
            resolver
                .resolve_secret(&"env:KAFKA_PASSWORD".parse().unwrap())
                .is_err()
        );
    }
    #[test]
    fn rejects_non_environment_reference() {
        let resolver = EnvironmentSecretResolver::with_lookup(|_| Some("x".into()));
        let reference = SecretReference::File {
            path: "/tmp/none".into(),
        };
        assert!(resolver.resolve_secret(&reference).is_err());
    }
}
