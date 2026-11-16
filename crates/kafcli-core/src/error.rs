use thiserror::Error;

/// Errors produced by the kafcli core library.
///
/// Messages describe where a problem is, never the value that caused it,
/// so secrets cannot leak through error output.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("context '{0}' not found")]
    ContextNotFound(String),
    #[error("no context selected: pass --context or run 'kafcli context use <name>'")]
    NoContextSelected,
    #[error("invalid secret reference: {0}")]
    InvalidSecretReference(String),
    #[error("secret could not be resolved: {0}")]
    SecretResolution(String),
    #[error("context validation failed: {0}")]
    ContextValidation(String),
    #[error("kafka error: {0}")]
    Kafka(String),
    #[error("operation aborted: {0}")]
    OperationAborted(String),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

pub type CoreResult<T> = Result<T, CoreError>;
