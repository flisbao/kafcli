use crate::config::model::Configuration;
use crate::error::{CoreError, CoreResult};
use std::fs;
use std::io::Write;
use std::path::Path;
use tempfile::NamedTempFile;

/// Writes `configuration` to `config_path` atomically (temporary file in the same directory, then rename).
///
/// Missing parent directories are created.
pub fn save_configuration(config_path: &Path, configuration: &Configuration) -> CoreResult<()> {
    let parent_directory = match config_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    fs::create_dir_all(parent_directory)?;
    let serialized = toml::to_string_pretty(configuration).map_err(|_| {
        CoreError::Configuration("configuration could not be serialized to TOML".to_string())
    })?;
    let mut temporary_file = NamedTempFile::new_in(parent_directory)?;
    temporary_file.write_all(serialized.as_bytes())?;
    temporary_file.as_file().sync_all()?;
    temporary_file
        .persist(config_path)
        .map_err(|persist_error| CoreError::Io(persist_error.error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::loader::load_configuration;
    use crate::config::model::{AuthConfig, ConnectionContext, SaslCredentialsConfig};
    use crate::secret::reference::SecretReference;
    use std::collections::BTreeMap;

    fn sample_configuration() -> Configuration {
        let secured_context = ConnectionContext {
            brokers: vec!["prd.example:9093".to_string()],
            auth: AuthConfig::SaslScram512(SaslCredentialsConfig {
                username: "app".to_string(),
                secret_ref: "keyring:kafcli/prd".parse::<SecretReference>().unwrap(),
                ca_location: None,
                use_tls: true,
            }),
            protected: true,
            schema_registry: None,
        };
        let local_context = ConnectionContext {
            brokers: vec!["localhost:9092".to_string()],
            auth: AuthConfig::Plaintext,
            protected: false,
            schema_registry: None,
        };
        Configuration {
            current_context: Some("local".to_string()),
            contexts: BTreeMap::from([
                ("local".to_string(), local_context),
                ("prd".to_string(), secured_context),
            ]),
        }
    }

    #[test]
    fn saved_configuration_loads_back_identically() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.toml");
        let configuration = sample_configuration();

        save_configuration(&config_path, &configuration).unwrap();

        assert_eq!(load_configuration(&config_path).unwrap(), configuration);
    }

    #[test]
    fn creates_missing_parent_directories() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory
            .path()
            .join("nested")
            .join("kafcli")
            .join("config.toml");

        save_configuration(&config_path, &sample_configuration()).unwrap();

        assert!(config_path.is_file());
    }

    #[test]
    fn leaves_no_temporary_file_behind() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.toml");

        save_configuration(&config_path, &sample_configuration()).unwrap();

        let entries: Vec<String> = std::fs::read_dir(directory.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, vec!["config.toml".to_string()]);
    }
}
