use crate::error::{CoreError, CoreResult};
use std::path::PathBuf;

const APPLICATION_DIRECTORY: &str = "kafcli";
const CONFIGURATION_FILE_NAME: &str = "config.json";

/// Default configuration file: `<config dir>/kafcli/config.json`.
pub fn default_config_path() -> CoreResult<PathBuf> {
    dirs::config_dir()
        .map(|directory| {
            directory
                .join(APPLICATION_DIRECTORY)
                .join(CONFIGURATION_FILE_NAME)
        })
        .ok_or_else(|| {
            CoreError::Configuration("could not determine the configuration directory".to_string())
        })
}

/// Directory for kafcli state: `<state dir or local data dir>/kafcli`.
pub fn default_state_directory() -> CoreResult<PathBuf> {
    dirs::state_dir()
        .or_else(dirs::data_local_dir)
        .map(|directory| directory.join(APPLICATION_DIRECTORY))
        .ok_or_else(|| {
            CoreError::Configuration("could not determine the state directory".to_string())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_path_ends_with_kafcli_config_file() {
        let path = default_config_path().unwrap();
        assert!(path.ends_with("kafcli/config.json"));
    }

    #[test]
    fn state_directory_ends_with_kafcli() {
        let path = default_state_directory().unwrap();
        assert!(path.ends_with("kafcli"));
    }
}
