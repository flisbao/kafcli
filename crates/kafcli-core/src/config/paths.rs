use crate::error::{CoreError, CoreResult};
use std::ffi::OsString;
use std::path::PathBuf;

const APPLICATION_DIRECTORY: &str = "kafcli";
const CONFIGURATION_FILE_NAME: &str = "config.json";
/// Overrides the state directory when set to a non-empty path; the audit log is written inside it.
pub const STATE_DIRECTORY_ENVIRONMENT_VARIABLE: &str = "KAFCLI_STATE_DIR";

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

/// Directory for kafcli state: `KAFCLI_STATE_DIR` when it is set and non-empty, otherwise
/// `<state dir or local data dir>/kafcli`.
pub fn default_state_directory() -> CoreResult<PathBuf> {
    resolve_state_directory(
        std::env::var_os(STATE_DIRECTORY_ENVIRONMENT_VARIABLE),
        dirs::state_dir().or_else(dirs::data_local_dir),
    )
    .ok_or_else(|| CoreError::Configuration("could not determine the state directory".to_string()))
}

/// The override wins as given; an empty override counts as absent.
fn resolve_state_directory(
    override_value: Option<OsString>,
    platform_directory: Option<PathBuf>,
) -> Option<PathBuf> {
    match override_value.filter(|value| !value.is_empty()) {
        Some(value) => Some(PathBuf::from(value)),
        None => platform_directory.map(|directory| directory.join(APPLICATION_DIRECTORY)),
    }
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
        // Uses the pure resolver with no override, so the result does not depend on KAFCLI_STATE_DIR.
        let path =
            resolve_state_directory(None, dirs::state_dir().or_else(dirs::data_local_dir)).unwrap();
        assert!(path.ends_with("kafcli"));
    }

    #[test]
    fn state_directory_override_is_used_as_given() {
        let resolved = resolve_state_directory(
            Some(OsString::from("/var/lib/kafcli-test")),
            Some(PathBuf::from("/home/someone/.local/state")),
        );

        assert_eq!(resolved, Some(PathBuf::from("/var/lib/kafcli-test")));
    }

    #[test]
    fn empty_state_directory_override_falls_back_to_platform_directory() {
        let resolved = resolve_state_directory(
            Some(OsString::new()),
            Some(PathBuf::from("/home/someone/.local/state")),
        );

        assert_eq!(
            resolved,
            Some(PathBuf::from("/home/someone/.local/state/kafcli"))
        );
    }

    #[test]
    fn missing_override_without_platform_directory_is_none() {
        assert_eq!(resolve_state_directory(None, None), None);
    }
}
