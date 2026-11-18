use crate::cli::offsets_arguments::SetOffsetsArguments;
use crate::commands::cluster_reader_connection::connect_cluster_reader;
use crate::commands::destructive_support::{WarningAuditWriter, resolve_backup_directory};
use crate::execution_context::ExecutionContext;
use crate::output::collection_warnings::write_collection_warnings;
use crate::output::offset_reset_views::OffsetResetView;
use crate::output::renderer::{RenderedOutput, render_output};
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::selection::select_context;
use kafcli_core::guard::confirmation_prompter::ConfirmationPrompter;
use kafcli_core::ops::offsets::kafka_offset_committer::KafkaOffsetCommitter;
use kafcli_core::ops::offsets::offset_reset_execution::{
    DestructiveCollaborators, DestructiveRunSettings, execute_offset_reset,
};
use kafcli_core::ops::offsets::offset_reset_plan::{OffsetResetRequest, plan_offset_reset};
use kafcli_core::secret::resolver::SecretResolver;
use std::time::{Duration, Instant};

/// Command name written to the audit log and to the backup.
const COMMAND_NAME: &str = "offsets set";

/// Plans an offset reset of `--group` on `--topic` and, with `--execute`, applies it.
///
/// Destructive: dry-run by default; with `--execute` the group must be stopped, a protected
/// context asks for the group name, the current offsets are backed up, and only then the new ones
/// are committed and read back.
pub fn set_offsets(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    prompter: &dyn ConfirmationPrompter,
    arguments: &SetOffsetsArguments,
) -> anyhow::Result<RenderedOutput> {
    let configuration = load_configuration(&execution.config_path)?;
    let selected = select_context(&configuration, execution.global.context.as_deref())?;
    let context_name = selected.name.clone();
    let context_protected = selected.context.protected;
    let backup_directory = resolve_backup_directory()?;
    let reader = connect_cluster_reader(
        execution,
        &configuration,
        resolver,
        arguments.timeout_seconds,
    )?;
    let request = OffsetResetRequest {
        group: arguments.group.clone(),
        topic: arguments.topic.clone(),
        partitions: (!arguments.partitions.is_empty()).then(|| arguments.partitions.clone()),
        target: arguments.target(),
    };
    let plan = plan_offset_reset(&reader, &request)?;
    let committer = KafkaOffsetCommitter::new(
        reader.connection().clone(),
        reader.family(),
        Duration::from_secs(arguments.timeout_seconds),
    );
    let now = Instant::now;
    let collaborators = DestructiveCollaborators {
        reader: &reader,
        committer: &committer,
        prompter,
        audit: &WarningAuditWriter,
        now: &now,
    };
    let settings = DestructiveRunSettings {
        context_name: &context_name,
        context_protected,
        execute: arguments.execute,
        wait_empty: arguments.wait_empty,
        group_stopped: arguments.group_stopped,
        backup_directory: &backup_directory,
        audit_target: format!("group={} topic={}", arguments.group, arguments.topic),
    };
    let outcome = execute_offset_reset(&collaborators, plan, COMMAND_NAME, &settings)?;
    let output_format = execution.global.output;
    write_collection_warnings(&outcome.warnings, output_format);
    let view = OffsetResetView::from_outcome(outcome);
    Ok(RenderedOutput::complete(render_output(
        &view,
        output_format,
    )?))
}
