use crate::cli::arguments::GlobalArguments;
use kafcli_core::config::paths::default_config_path;
use std::path::PathBuf;

/// Everything a command needs from the command line: where the configuration lives and the global flags.
pub struct ExecutionContext {
    pub config_path: PathBuf,
    pub global: GlobalArguments,
}

impl ExecutionContext {
    /// Uses `--config` (or `KAFCLI_CONFIG`) when given, otherwise the default configuration path.
    pub fn from_global_arguments(global: GlobalArguments) -> anyhow::Result<Self> {
        let config_path = match &global.config {
            Some(path) => path.clone(),
            None => default_config_path()?,
        };
        tracing::debug!(config_path = %config_path.display(), "resolved configuration path");
        Ok(Self {
            config_path,
            global,
        })
    }
}
