/// Public Entra ID authority, used when neither the context nor the environment names one.
pub const DEFAULT_AUTHORITY_HOST: &str = "https://login.microsoftonline.com";

const AUTHORITY_HOST_VARIABLE: &str = "AZURE_AUTHORITY_HOST";

/// Read access to the process environment, replaced by a fake in tests.
pub trait AzureEnvironment: Send + Sync {
    fn read_variable(&self, name: &str) -> Option<String>;
}

pub struct ProcessAzureEnvironment;

impl AzureEnvironment for ProcessAzureEnvironment {
    fn read_variable(&self, name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|value| !value.is_empty())
    }
}

/// Picks the Entra ID authority: the context setting, then `AZURE_AUTHORITY_HOST`, then the public
/// cloud. The result never ends with `/`.
pub fn resolve_authority_host(
    configured: Option<&str>,
    environment: &dyn AzureEnvironment,
) -> String {
    let chosen = configured
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| environment.read_variable(AUTHORITY_HOST_VARIABLE))
        .unwrap_or_else(|| DEFAULT_AUTHORITY_HOST.to_owned());
    chosen.trim_end_matches('/').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct FakeEnvironment {
        variables: HashMap<String, String>,
    }

    impl AzureEnvironment for FakeEnvironment {
        fn read_variable(&self, name: &str) -> Option<String> {
            self.variables.get(name).cloned()
        }
    }

    fn environment_with_authority(value: Option<&str>) -> FakeEnvironment {
        let mut variables = HashMap::new();
        if let Some(value) = value {
            variables.insert("AZURE_AUTHORITY_HOST".to_owned(), value.to_owned());
        }
        FakeEnvironment { variables }
    }

    #[test]
    fn configured_authority_wins_over_environment() {
        let environment = environment_with_authority(Some("https://env.example/"));

        let host = resolve_authority_host(Some("https://config.example/"), &environment);

        assert_eq!(host, "https://config.example");
    }

    #[test]
    fn environment_applies_when_config_is_absent() {
        let environment = environment_with_authority(Some("https://env.example/"));

        assert_eq!(
            resolve_authority_host(None, &environment),
            "https://env.example"
        );
    }

    #[test]
    fn public_cloud_is_the_default() {
        let environment = environment_with_authority(None);

        assert_eq!(
            resolve_authority_host(None, &environment),
            DEFAULT_AUTHORITY_HOST
        );
    }
}
