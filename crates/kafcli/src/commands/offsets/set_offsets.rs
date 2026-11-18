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
use kafcli_core::ops::groups::kafka_cluster_reader::KafkaClusterReader;
use kafcli_core::ops::offsets::kafka_offset_committer::KafkaOffsetCommitter;
use kafcli_core::ops::offsets::offset_reset_execution::{
    DestructiveCollaborators, DestructiveRunSettings, execute_offset_reset,
};
use kafcli_core::ops::offsets::offset_reset_plan::{
    OffsetResetPlan, OffsetResetRequest, plan_offset_reset,
};
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
            audit_target: format!("group={} topic={}", arguments.group, arguments.topic),
        },
    )
}

/// The flags and context a reset runs with, shared by `offsets set` and `offsets restore`.
pub struct ResetRun<'a> {
    pub command_name: &'a str,
    pub context_name: &'a str,
    pub context_protected: bool,
    pub execute: bool,
    pub wait_empty: Option<Duration>,
    pub group_stopped: bool,
    pub timeout_seconds: u64,
    pub audit_target: String,
}

/// Runs `plan` through the destructive flow and renders the result.
pub fn apply_offset_plan(
    execution: &ExecutionContext,
    reader: &KafkaClusterReader,
    prompter: &dyn ConfirmationPrompter,
    plan: OffsetResetPlan,
    run: &ResetRun,
) -> anyhow::Result<RenderedOutput> {
    let backup_directory = resolve_backup_directory()?;
    let committer = KafkaOffsetCommitter::new(
        reader.connection().clone(),
        reader.family(),
        Duration::from_secs(run.timeout_seconds),
    );
    let now = Instant::now;
    let collaborators = DestructiveCollaborators {
        reader,
        committer: &committer,
        prompter,
        audit: &WarningAuditWriter,
        now: &now,
    };
    let settings = DestructiveRunSettings {
        context_name: run.context_name,
        context_protected: run.context_protected,
        execute: run.execute,
        wait_empty: run.wait_empty,
        group_stopped: run.group_stopped,
        backup_directory: &backup_directory,
        audit_target: run.audit_target.clone(),
    };
    let outcome = execute_offset_reset(&collaborators, plan, run.command_name, &settings)?;
    let output_format = execution.global.output;
    write_collection_warnings(&outcome.warnings, output_format);
    let view = OffsetResetView::from_outcome(outcome);
    Ok(RenderedOutput::complete(render_output(
        &view,
        output_format,
    )?))
}
