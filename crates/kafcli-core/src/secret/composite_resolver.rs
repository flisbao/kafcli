use super::environment_resolver::EnvironmentSecretResolver;
use super::file_resolver::FileSecretResolver;
#[cfg(feature = "keyring")]
use super::keyring_resolver::KeyringSecretResolver;
use super::reference::SecretReference;
use super::resolver::SecretResolver;
use crate::error::{CoreError, CoreResult};
use secrecy::SecretString;

/// Routes each reference to the resolver for its scheme.
pub struct CompositeSecretResolver {
    environment: EnvironmentSecretResolver,
    file: FileSecretResolver,
    keyring: Option<Box<dyn SecretResolver>>,
}

impl CompositeSecretResolver {
    pub fn new(
        environment: EnvironmentSecretResolver,
        file: FileSecretResolver,
        keyring: Option<Box<dyn SecretResolver>>,
    ) -> Self {
        Self {
            environment,
            file,
            keyring,
        }
    }

    /// Process environment and files always; keyring only when the `keyring` feature is enabled.
    pub fn with_default_sources() -> Self {
        Self::new(
            EnvironmentSecretResolver::from_process_environment(),
            FileSecretResolver,
            default_keyring_resolver(),
        )
    }
}

impl SecretResolver for CompositeSecretResolver {
    fn resolve_secret(&self, reference: &SecretReference) -> CoreResult<SecretString> {
        match reference {
            SecretReference::Environment { .. } => self.environment.resolve_secret(reference),
            SecretReference::File { .. } => self.file.resolve_secret(reference),
            SecretReference::Keyring { .. } => match &self.keyring {
                Some(keyring) => keyring.resolve_secret(reference),
                None => Err(CoreError::SecretResolution(
                    "keyring is not available in this build; use env: or file: instead".to_string(),
                )),
            },
        }
    }
}

#[cfg(feature = "keyring")]
fn default_keyring_resolver() -> Option<Box<dyn SecretResolver>> {
    Some(Box::new(KeyringSecretResolver))
}

#[cfg(not(feature = "keyring"))]
fn default_keyring_resolver() -> Option<Box<dyn SecretResolver>> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use crate::secret::reference::SecretReference;
    use crate::secret::resolver::SecretResolver;
    use secrecy::ExposeSecret;
    use std::io::Write;

    #[test]
    fn dispatches_to_source_matching_reference() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"from-file\n").unwrap();
        let resolver = CompositeSecretResolver::new(
            EnvironmentSecretResolver::with_lookup(|_| Some("from-env".into())),
            FileSecretResolver,
            Some(Box::new(FixedSecretResolver::returning("from-keyring"))),
        );

        let from_env = resolver
            .resolve_secret(&"env:ANY".parse().unwrap())
            .unwrap();
        let from_file = resolver
            .resolve_secret(&SecretReference::File {
                path: file.path().to_path_buf(),
            })
            .unwrap();
        let from_keyring = resolver
            .resolve_secret(&"keyring:kafcli/acc".parse().unwrap())
            .unwrap();

        assert_eq!(from_env.expose_secret(), "from-env");
        assert_eq!(from_file.expose_secret(), "from-file");
        assert_eq!(from_keyring.expose_secret(), "from-keyring");
    }
    #[test]
    fn suggests_alternatives_when_keyring_is_unavailable() {
        let resolver = CompositeSecretResolver::new(
            EnvironmentSecretResolver::with_lookup(|_| None),
            FileSecretResolver,
            None,
        );
        let error = resolver
            .resolve_secret(&"keyring:kafcli/acc".parse().unwrap())
            .unwrap_err()
            .to_string();
        assert!(error.contains("env:") && error.contains("file:"));
    }
}
