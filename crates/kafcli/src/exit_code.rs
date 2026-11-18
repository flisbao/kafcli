use kafcli_core::error::CoreError;
use std::process::ExitCode;

/// Process exit codes. `LimitExceeded` means a limit the user set, such as `--max-lag`, was crossed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum KafcliExitCode {
    Success = 0,
    Failure = 1,
    LimitExceeded = 2,
    Aborted = 3,
}

/// Maps an error to the process exit code: a crossed limit is 2, an aborted operation is 3,
/// anything else is 1.
pub fn resolve_exit_code(error: &anyhow::Error) -> ExitCode {
    ExitCode::from(classify_exit_code(error) as u8)
}

fn classify_exit_code(error: &anyhow::Error) -> KafcliExitCode {
    match error
        .chain()
        .find_map(|cause| cause.downcast_ref::<CoreError>())
    {
        Some(CoreError::LimitExceeded(_)) => KafcliExitCode::LimitExceeded,
        Some(CoreError::OperationAborted(_)) => KafcliExitCode::Aborted,
        _ => KafcliExitCode::Failure,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aborted_operation_maps_to_exit_code_three() {
        let error = anyhow::Error::new(CoreError::OperationAborted("typed value mismatch".into()));

        assert_eq!(classify_exit_code(&error), KafcliExitCode::Aborted);
    }

    #[test]
    fn finds_core_error_under_added_context() {
        let error = anyhow::Error::new(CoreError::OperationAborted("no".into()))
            .context("could not delete topic");

        assert_eq!(classify_exit_code(&error), KafcliExitCode::Aborted);
    }

    #[test]
    fn limit_exceeded_maps_to_exit_code_two() {
        let error = anyhow::Error::new(CoreError::LimitExceeded("group 'g' lag 5".into()));

        assert_eq!(classify_exit_code(&error), KafcliExitCode::LimitExceeded);
    }

    #[test]
    fn audit_failed_maps_to_exit_code_one() {
        let error = anyhow::Error::new(CoreError::AuditFailed(2));

        assert_eq!(classify_exit_code(&error), KafcliExitCode::Failure);
    }

    #[test]
    fn other_errors_map_to_exit_code_one() {
        let error = anyhow::anyhow!("something else failed");

        assert_eq!(classify_exit_code(&error), KafcliExitCode::Failure);
    }
}
