use super::confirmation_prompter::ConfirmationPrompter;
use super::guard_request::GuardRequest;
use super::operation_level::OperationLevel;
use crate::error::{CoreError, CoreResult};

/// What the caller should do after the guard accepts a request.
#[derive(Debug, PartialEq, Eq)]
pub enum GuardOutcome {
    Proceed,
    DryRun,
}

/// Decides whether an operation may run, asking the operator when the policy requires it.
///
/// Returns `DryRun` for destructive operations without `--execute`, and an
/// `OperationAborted` error when confirmation is missing or declined.
pub fn enforce_guard(
    request: &GuardRequest,
    profile_protected: bool,
    prompter: &dyn ConfirmationPrompter,
) -> CoreResult<GuardOutcome> {
    match request.level {
        OperationLevel::Read => Ok(GuardOutcome::Proceed),
        OperationLevel::Write => enforce_write_guard(request, profile_protected, prompter),
        OperationLevel::Destructive => {
            enforce_destructive_guard(request, profile_protected, prompter)
        }
    }
}

fn enforce_write_guard(
    request: &GuardRequest,
    profile_protected: bool,
    prompter: &dyn ConfirmationPrompter,
) -> CoreResult<GuardOutcome> {
    if !profile_protected || request.assume_yes {
        return Ok(GuardOutcome::Proceed);
    }
    if !prompter.is_interactive() {
        return Err(build_aborted_error(
            "confirmation required on protected profile: pass --yes",
        ));
    }
    let message = format!("{} on protected profile? [y/N]", request.description);
    if prompter.confirm_yes_no(&message)? {
        Ok(GuardOutcome::Proceed)
    } else {
        Err(build_aborted_error("cancelled by user"))
    }
}

fn enforce_destructive_guard(
    request: &GuardRequest,
    profile_protected: bool,
    prompter: &dyn ConfirmationPrompter,
) -> CoreResult<GuardOutcome> {
    if !request.execute {
        return Ok(GuardOutcome::DryRun);
    }
    if !profile_protected {
        return Ok(GuardOutcome::Proceed);
    }
    if !prompter.is_interactive() {
        return Err(build_aborted_error(
            "typed confirmation required on protected profile; run interactively",
        ));
    }
    let message = format!(
        "Type '{}' to confirm: {}",
        request.confirmation_target, request.description
    );
    if prompter.confirm_typed_value(&message, &request.confirmation_target)? {
        Ok(GuardOutcome::Proceed)
    } else {
        Err(build_aborted_error("typed value did not match"))
    }
}

fn build_aborted_error(reason: &str) -> CoreError {
    CoreError::OperationAborted(reason.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{CoreError, CoreResult};
    use std::cell::{Cell, RefCell};

    /// Prompter with scripted answers that counts prompts and records the last typed prompt.
    struct ScriptedPrompter {
        interactive: bool,
        yes_no_answer: bool,
        typed_answer: bool,
        calls: Cell<u32>,
        typed_message: RefCell<String>,
        typed_expected_value: RefCell<String>,
    }

    impl ScriptedPrompter {
        fn interactive_answering(yes_no_answer: bool, typed_answer: bool) -> Self {
            Self {
                interactive: true,
                yes_no_answer,
                typed_answer,
                calls: Cell::new(0),
                typed_message: RefCell::new(String::new()),
                typed_expected_value: RefCell::new(String::new()),
            }
        }

        fn build_non_interactive_prompter() -> Self {
            Self {
                interactive: false,
                ..Self::interactive_answering(false, false)
            }
        }

        fn prompt_count(&self) -> u32 {
            self.calls.get()
        }
    }

    impl ConfirmationPrompter for ScriptedPrompter {
        fn is_interactive(&self) -> bool {
            self.interactive
        }

        fn confirm_yes_no(&self, _message: &str) -> CoreResult<bool> {
            self.calls.set(self.calls.get() + 1);
            Ok(self.yes_no_answer)
        }

        fn confirm_typed_value(&self, message: &str, expected_value: &str) -> CoreResult<bool> {
            self.calls.set(self.calls.get() + 1);
            *self.typed_message.borrow_mut() = message.to_string();
            *self.typed_expected_value.borrow_mut() = expected_value.to_string();
            Ok(self.typed_answer)
        }
    }

    fn request(level: OperationLevel, assume_yes: bool, execute: bool) -> GuardRequest {
        GuardRequest {
            level,
            description: "reset offsets of group pedidos-service".to_string(),
            confirmation_target: "pedidos-service".to_string(),
            assume_yes,
            execute,
        }
    }

    fn aborted_message(result: CoreResult<GuardOutcome>) -> String {
        match result {
            Err(CoreError::OperationAborted(message)) => message,
            other => panic!("expected OperationAborted, got {other:?}"),
        }
    }

    #[test]
    fn read_always_proceeds() {
        let prompter = ScriptedPrompter::interactive_answering(false, false);
        let outcome = enforce_guard(
            &request(OperationLevel::Read, false, false),
            true,
            &prompter,
        );
        assert_eq!(outcome.unwrap(), GuardOutcome::Proceed);
        assert_eq!(prompter.prompt_count(), 0);
    }

    #[test]
    fn write_on_unprotected_proceeds_without_prompt() {
        let prompter = ScriptedPrompter::interactive_answering(false, false);
        let outcome = enforce_guard(
            &request(OperationLevel::Write, false, false),
            false,
            &prompter,
        );
        assert_eq!(outcome.unwrap(), GuardOutcome::Proceed);
        assert_eq!(prompter.prompt_count(), 0);
    }

    #[test]
    fn write_on_protected_proceeds_with_assume_yes() {
        let prompter = ScriptedPrompter::interactive_answering(false, false);
        let outcome = enforce_guard(
            &request(OperationLevel::Write, true, false),
            true,
            &prompter,
        );
        assert_eq!(outcome.unwrap(), GuardOutcome::Proceed);
        assert_eq!(prompter.prompt_count(), 0);
    }

    #[test]
    fn write_on_protected_aborts_without_terminal() {
        let prompter = ScriptedPrompter::build_non_interactive_prompter();
        let result = enforce_guard(
            &request(OperationLevel::Write, false, false),
            true,
            &prompter,
        );
        assert_eq!(
            aborted_message(result),
            "confirmation required on protected profile: pass --yes"
        );
        assert_eq!(prompter.prompt_count(), 0);
    }

    #[test]
    fn write_on_protected_aborts_when_user_declines() {
        let prompter = ScriptedPrompter::interactive_answering(false, false);
        let result = enforce_guard(
            &request(OperationLevel::Write, false, false),
            true,
            &prompter,
        );
        assert_eq!(aborted_message(result), "cancelled by user");
        assert_eq!(prompter.prompt_count(), 1);
    }

    #[test]
    fn destructive_without_execute_is_dry_run() {
        let prompter = ScriptedPrompter::interactive_answering(false, false);
        let outcome = enforce_guard(
            &request(OperationLevel::Destructive, false, false),
            true,
            &prompter,
        );
        assert_eq!(outcome.unwrap(), GuardOutcome::DryRun);
        assert_eq!(prompter.prompt_count(), 0);
    }

    #[test]
    fn destructive_on_unprotected_proceeds_with_execute() {
        let prompter = ScriptedPrompter::interactive_answering(false, false);
        let outcome = enforce_guard(
            &request(OperationLevel::Destructive, false, true),
            false,
            &prompter,
        );
        assert_eq!(outcome.unwrap(), GuardOutcome::Proceed);
        assert_eq!(prompter.prompt_count(), 0);
    }

    #[test]
    fn destructive_on_protected_ignores_assume_yes_without_terminal() {
        let prompter = ScriptedPrompter::build_non_interactive_prompter();
        let result = enforce_guard(
            &request(OperationLevel::Destructive, true, true),
            true,
            &prompter,
        );
        assert_eq!(
            aborted_message(result),
            "typed confirmation required on protected profile; run interactively"
        );
        assert_eq!(prompter.prompt_count(), 0);
    }

    #[test]
    fn destructive_on_protected_aborts_on_mismatch() {
        let prompter = ScriptedPrompter::interactive_answering(false, false);
        let result = enforce_guard(
            &request(OperationLevel::Destructive, false, true),
            true,
            &prompter,
        );
        assert_eq!(aborted_message(result), "typed value did not match");
        assert_eq!(prompter.prompt_count(), 1);
    }

    #[test]
    fn destructive_on_protected_proceeds_on_match() {
        let prompter = ScriptedPrompter::interactive_answering(false, true);
        let outcome = enforce_guard(
            &request(OperationLevel::Destructive, false, true),
            true,
            &prompter,
        );
        assert_eq!(outcome.unwrap(), GuardOutcome::Proceed);
        assert_eq!(prompter.prompt_count(), 1);
        assert_eq!(
            *prompter.typed_expected_value.borrow(),
            "pedidos-service".to_string()
        );
        assert_eq!(
            *prompter.typed_message.borrow(),
            "Type 'pedidos-service' to confirm: reset offsets of group pedidos-service"
        );
    }

    #[test]
    fn destructive_on_protected_prompts_even_with_assume_yes() {
        let prompter = ScriptedPrompter::interactive_answering(false, true);
        let outcome = enforce_guard(
            &request(OperationLevel::Destructive, true, true),
            true,
            &prompter,
        );
        assert_eq!(outcome.unwrap(), GuardOutcome::Proceed);
        assert_eq!(prompter.prompt_count(), 1);
    }
}
