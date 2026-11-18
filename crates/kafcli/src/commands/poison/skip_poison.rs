use crate::cli::poison_arguments::SkipPoisonArguments;
use crate::commands::cluster_reader_connection::connect_cluster_reader;
use crate::commands::destructive_support::{WarningAuditWriter, resolve_backup_directory};
use crate::execution_context::ExecutionContext;
use crate::output::collection_warnings::write_collection_warnings;
use crate::output::poison_views::PoisonSkipView;
use crate::output::renderer::{RenderedOutput, render_output};
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::overrides::{ContextOverrides, apply_context_overrides};
use kafcli_core::config::selection::select_context;
use kafcli_core::connection::client_config_builder::ConnectionSettings;
use kafcli_core::guard::confirmation_prompter::ConfirmationPrompter;
use kafcli_core::ops::offsets::kafka_offset_committer::KafkaOffsetCommitter;
use kafcli_core::ops::offsets::offset_reset_execution::{
    DestructiveCollaborators, DestructiveRunSettings,
};
use kafcli_core::ops::poison::dead_letter_publisher::KafkaDeadLetterPublisher;
use kafcli_core::ops::poison::kafka_message_reader::KafkaPartitionMessageReader;
use kafcli_core::ops::poison::poison_skip::{
    PoisonSkipRequest, execute_poison_skip, validate_poison_skip_request,
};
use kafcli_core::secret::resolver::SecretResolver;
use std::time::{Duration, Instant};

/// Copies the message at `--offset` to `--dlt` and commits `--offset + 1` for `--group`.
///
/// Destructive: dry-run by default; with `--execute` it runs the same group checks, guard and
/// backup as `offsets set` before the copy and the commit.
pub fn skip_poison(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    prompter: &dyn ConfirmationPrompter,
    arguments: &SkipPoisonArguments,
) -> anyhow::Result<RenderedOutput> {
    let request = PoisonSkipRequest {
        group: arguments.group.clone(),
        topic: arguments.topic.clone(),
        partition: arguments.partition,
        offset: arguments.offset,
        dead_letter_topic: arguments.dlt.clone(),
        reason: arguments.reason.clone(),
    };
    validate_poison_skip_request(&request)?;
    let configuration = load_configuration(&execution.config_path)?;
    let selected = select_context(&configuration, execution.global.context.as_deref())?;
    let context_name = selected.name.clone();
    let context_protected = selected.context.protected;
    let backup_directory = resolve_backup_directory()?;
    let timeout = Duration::from_secs(arguments.timeout_seconds);
    let reader = connect_cluster_reader(
        execution,
        &configuration,
        resolver,
        arguments.timeout_seconds,
    )?;
    let connection_context = apply_context_overrides(
        selected.context,
        &ContextOverrides {
            brokers: execution.global.brokers.clone(),
        },
    );
    let publisher = KafkaDeadLetterPublisher::new(
        connection_context,
        resolver,
        ConnectionSettings {
            insecure: execution.global.insecure,
        },
        timeout,
    );
    let messages = KafkaPartitionMessageReader::new(reader.connection().clone(), timeout);
    let committer =
        KafkaOffsetCommitter::new(reader.connection().clone(), reader.family(), timeout);
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
        audit_target: format!(
            "group={} topic={} partition={} offset={} dlt={}",
            arguments.group, arguments.topic, arguments.partition, arguments.offset, arguments.dlt
        ),
    };
    let outcome = execute_poison_skip(&collaborators, &messages, &publisher, &request, &settings)?;
    let output_format = execution.global.output;
    write_collection_warnings(&outcome.warnings, output_format);
    Ok(RenderedOutput::complete(render_output(
        &PoisonSkipView::from_outcome(outcome),
        output_format,
    )?))
}
