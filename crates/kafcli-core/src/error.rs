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
    #[error("authentication error: {0}")]
    Authentication(String),
    /// Authentication failure caused by the transport, such as a timeout or a refused connection.
    /// Same text as `Authentication`; the registry client treats it as transient.
    #[error("authentication error: {0}")]
    AuthenticationTransient(String),
    #[error("kafka error: {0}")]
    Kafka(String),
    #[error("topic '{0}' not found in cluster metadata")]
    TopicNotFound(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("value does not match schema at '{pointer}': {reason}")]
    SchemaValidation { pointer: String, reason: String },
    #[error("schema registry error: {0}")]
    SchemaRegistry(String),
    /// Registry failure that may pass on its own: transport error, timeout or HTTP 5xx. Same text as
    /// `SchemaRegistry`; the schema cache only forgets it after a while.
    #[error("schema registry error: {0}")]
    SchemaRegistryTransient(String),
    #[error("template error: {0}")]
    Template(String),
    #[error("operation aborted: {0}")]
    OperationAborted(String),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

impl CoreError {
    /// Whether the failure may pass on its own, so it should not be remembered for the whole run.
    pub fn is_transient_registry_failure(&self) -> bool {
        matches!(self, Self::SchemaRegistryTransient(_))
    }
}

pub type CoreResult<T> = Result<T, CoreError>;
