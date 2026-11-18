use crate::cli::offsets_arguments::RestoreOffsetsArguments;
use crate::commands::cluster_reader_connection::connect_cluster_reader;
use crate::commands::offsets::set_offsets::{ResetRun, apply_offset_plan};
use crate::execution_context::ExecutionContext;
use crate::output::renderer::RenderedOutput;
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::selection::select_context;
use kafcli_core::error::CoreError;
use kafcli_core::guard::confirmation_prompter::ConfirmationPrompter;
use kafcli_core::ops::offsets::offset_backup::read_offset_backup;
use kafcli_core::ops::offsets::offset_reset_plan::plan_offset_restore;
use kafcli_core::secret::resolver::SecretResolver;

/// Command name written to the audit log and to the backup the restore itself takes.
const COMMAND_NAME: &str = "offsets restore";

/// Moves the group of a backup file back to the offsets it holds, through the same destructive
/// flow as `offsets set`, so the restore writes its own backup first.
pub fn restore_offsets(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    prompter: &dyn ConfirmationPrompter,
    arguments: &RestoreOffsetsArguments,
) -> anyhow::Result<RenderedOutput> {
    let backup = read_offset_backup(&arguments.file)?;
    let configuration = load_configuration(&execution.config_path)?;
    let selected = select_context(&configuration, execution.global.context.as_deref())?;
    if backup.context != selected.name {
        return Err(CoreError::InvalidInput(format!(
            "backup was taken on context '{}', not on '{}'",
            backup.context, selected.name
        ))
        .into());
    }
    let context_name = selected.name.clone();
    let context_protected = selected.context.protected;
    let reader = connect_cluster_reader(
        execution,
        &configuration,
        resolver,
        arguments.timeout_seconds,
    )?;
    let plan = plan_offset_restore(&reader, &backup)?;
    let file_name = arguments
        .file
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    apply_offset_plan(
        execution,
        &reader,
        prompter,
        plan,
        &ResetRun {
            command_name: COMMAND_NAME,
            context_name: &context_name,
            context_protected,
            execute: arguments.execute,
            wait_empty: arguments.wait_empty,
            group_stopped: arguments.group_stopped,
            timeout_seconds: arguments.timeout_seconds,
            audit_target: format!("group={} file={file_name}", backup.group),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::arguments::GlobalArguments;
    use crate::cli::offsets_arguments::RestoreOffsetsArguments;
    use crate::output::output_format::OutputFormat;
    use crate::prompt::terminal_prompter::TerminalPrompter;
    use kafcli_core::config::model::{AuthConfig, Configuration, ConnectionContext};
    use kafcli_core::config::writer::save_configuration;
    use kafcli_core::error::CoreError;
    use kafcli_core::ops::offsets::offset_backup::{
        BackupPartition, OffsetBackup, write_offset_backup,
    };
    use kafcli_core::secret::fixed_resolver::FixedSecretResolver;
    use std::collections::BTreeMap;

    #[test]
    fn backup_from_other_context_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.json");
        let configuration = Configuration {
            current_context: Some("dev".to_owned()),
            contexts: BTreeMap::from([(
                "dev".to_owned(),
                ConnectionContext {
                    brokers: vec!["127.0.0.1:1".to_owned()],
                    auth: AuthConfig::Plaintext,
                    protected: false,
                    schema_registry: None,
                },
            )]),
            groups_audit: None,
        };
        save_configuration(&config_path, &configuration).unwrap();
        let backup = OffsetBackup {
            kafcli_offsets_backup: 1,
            created_at: "2026-11-18T11:06:40.123Z".to_owned(),
            context: "prod".to_owned(),
            group: "g".to_owned(),
            command: "offsets set".to_owned(),
            partitions: vec![BackupPartition {
                topic: "t".to_owned(),
                partition: 0,
                committed_offset: Some(3),
                log_start_offset: 0,
                log_end_offset: 9,
                lag: Some(6),
            }],
        };
        let file = write_offset_backup(directory.path(), &backup).unwrap();
        let execution = ExecutionContext {
            config_path,
            global: GlobalArguments {
                context: None,
                output: OutputFormat::Table,
                verbosity: 0,
                config: None,
                brokers: None,
                insecure: false,
            },
        };
        let arguments = RestoreOffsetsArguments {
            file,
            execute: true,
            wait_empty: None,
            group_stopped: false,
            timeout_seconds: 30,
        };

        let Err(error) = restore_offsets(
            &execution,
            &FixedSecretResolver::returning("unused"),
            &TerminalPrompter,
            &arguments,
        ) else {
            panic!("restore from another context was accepted");
        };

        let core = error.downcast_ref::<CoreError>();
        assert!(
            matches!(core, Some(CoreError::InvalidInput(text)) if text == "backup was taken on context 'prod', not on 'dev'"),
            "{error:?}"
        );
    }
}
