use kafcli_core::error::CoreError;
use std::process::ExitCode;

/// Process exit codes. `LimitExceeded` is reserved for the bulk-operation limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
#[allow(
    dead_code,
    reason = "LimitExceeded is produced by the bulk-operation limits added in a later plan"
)]
pub enum KafcliExitCode {
    Success = 0,
    Failure = 1,
    LimitExceeded = 2,
    Aborted = 3,
}

/// Maps an error to the process exit code: an aborted operation is 3, anything else is 1.
pub fn exit_code_for_error(error: &anyhow::Error) -> ExitCode {
    ExitCode::from(kafcli_exit_code_for_error(error) as u8)
}

fn kafcli_exit_code_for_error(error: &anyhow::Error) -> KafcliExitCode {
    match error
        .chain()
        .find_map(|cause| cause.downcast_ref::<CoreError>())
    {
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

        assert_eq!(kafcli_exit_code_for_error(&error), KafcliExitCode::Aborted);
    }

    #[test]
    fn finds_core_error_under_added_context() {
        let error = anyhow::Error::new(CoreError::OperationAborted("no".into()))
            .context("could not delete topic");

        assert_eq!(kafcli_exit_code_for_error(&error), KafcliExitCode::Aborted);
    }

    #[test]
    fn other_errors_map_to_exit_code_one() {
        let error = anyhow::anyhow!("something else failed");

        assert_eq!(kafcli_exit_code_for_error(&error), KafcliExitCode::Failure);
    }
}
