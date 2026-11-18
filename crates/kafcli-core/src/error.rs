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
    /// The broker does not offer an operation kafcli needs.
    #[error("{0}")]
    UnsupportedOperation(String),
    #[error("consumer group '{0}' not found")]
    GroupNotFound(String),
    /// A limit the user set was crossed; the command already wrote its report.
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),
    /// `groups audit` found at least one ERROR finding; the report was already written.
    #[error("groups audit found {0} ERROR finding(s)")]
    AuditFailed(usize),
    /// A destructive operation needs a group without members.
    #[error(
        "consumer group '{group}' has {members} active member(s); stop its consumers or pass --wait-empty <DURATION>"
    )]
    GroupNotEmpty { group: String, members: usize },
    /// `--wait-empty` ended while the group still had members.
    #[error(
        "consumer group '{group}' still has {members} active member(s) after waiting {waited_seconds}s"
    )]
    GroupStillActive {
        group: String,
        members: usize,
        waited_seconds: u64,
    },
    /// The broker could not tell whether the group is empty; the text says why and what to do.
    #[error("{0}")]
    GroupStateUnknown(String),
    /// The broker refused an offset commit because the group has members. `detail` is empty or
    /// starts with `; ` and says where the offsets before the change were saved.
    #[error(
        "the broker refused the offset commit: consumer group '{group}' has active members{detail}"
    )]
    CommitRefusedActiveGroup { group: String, detail: String },
    /// `--max-lag` passed for the groups read, but some groups could not be read at all.
    #[error("--max-lag could not be checked for {0} group(s) that could not be read")]
    IncompleteLimitCheck(usize),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

impl CoreError {
    /// The same error with `; <context>` appended to its text. Variants that carry text keep their
    /// variant, so exit codes and matching do not change; the others become `Kafka` with the full
    /// text, which is what a failed write reports anyway.
    pub fn with_context(self, context: &str) -> Self {
        let suffix = format!("; {context}");
        match self {
            Self::Kafka(text) => Self::Kafka(text + &suffix),
            Self::InvalidInput(text) => Self::InvalidInput(text + &suffix),
            Self::UnsupportedOperation(text) => Self::UnsupportedOperation(text + &suffix),
            Self::Authentication(text) => Self::Authentication(text + &suffix),
            Self::AuthenticationTransient(text) => Self::AuthenticationTransient(text + &suffix),
            Self::OperationAborted(text) => Self::OperationAborted(text + &suffix),
            Self::GroupStateUnknown(text) => Self::GroupStateUnknown(text + &suffix),
            Self::CommitRefusedActiveGroup { group, detail } => Self::CommitRefusedActiveGroup {
                group,
                detail: detail + &suffix,
            },
            Self::Io(error) => Self::Io(std::io::Error::new(
                error.kind(),
                format!("{error}{suffix}"),
            )),
            other => Self::Kafka(format!("{other}{suffix}")),
        }
    }

    /// Whether the failure may pass on its own, so it should not be remembered for the whole run.
    pub fn is_transient_registry_failure(&self) -> bool {
        matches!(self, Self::SchemaRegistryTransient(_))
    }
}

pub type CoreResult<T> = Result<T, CoreError>;
