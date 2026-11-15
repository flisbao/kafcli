use crate::error::CoreResult;

/// Asks the operator for confirmation. The binary provides the terminal implementation.
pub trait ConfirmationPrompter {
    fn is_interactive(&self) -> bool;
    fn confirm_yes_no(&self, message: &str) -> CoreResult<bool>;
    /// Returns `true` when the typed answer equals `expected_value`.
    fn confirm_typed_value(&self, message: &str, expected_value: &str) -> CoreResult<bool>;
}
