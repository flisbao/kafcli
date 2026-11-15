use super::operation_level::OperationLevel;

/// Describes an operation about to run and the flags the user passed for it.
#[derive(Debug, Clone)]
pub struct GuardRequest {
    pub level: OperationLevel,
    /// Human-readable summary, e.g. "reset offsets of group pedidos-service".
    pub description: String,
    /// Value the user must type to confirm a destructive operation on a protected profile.
    pub confirmation_target: String,
    /// `--yes`: skips the y/N prompt for writes on a protected profile.
    pub assume_yes: bool,
    /// `--execute`: a destructive operation only runs when this is set.
    pub execute: bool,
}
