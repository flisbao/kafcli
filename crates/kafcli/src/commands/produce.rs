use crate::cli::produce_arguments::{ProduceArguments, ProduceFormat, ProduceInputArgument};
use crate::commands::registry_connection::{connect_schema_registry, encoder_needs_registry};
use crate::execution_context::ExecutionContext;
use crate::output::produce_dry_run_view::DryRunView;
use crate::output::produce_report_view::ProduceReportView;
use crate::output::production_warning::warn_if_production;
use crate::output::renderer::{RenderedOutput, render_output};
use crate::output::terminal_text::sanitize_for_terminal;
use anyhow::bail;
use kafcli_core::audit::audit_record::{AuditOutcome, AuditRecord};
use kafcli_core::audit::audit_writer::{AuditWriter, JsonLinesAuditWriter};
use kafcli_core::codec::codec_selection::{SchemaSource, build_value_encoder};
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::overrides::{ContextOverrides, apply_context_overrides};
use kafcli_core::config::paths::default_state_directory;
use kafcli_core::config::selection::select_context;
use kafcli_core::connection::client_config_builder::ConnectionSettings;
use kafcli_core::guard::confirmation_prompter::ConfirmationPrompter;
use kafcli_core::guard::guard_enforcer::enforce_guard;
use kafcli_core::guard::guard_request::GuardRequest;
use kafcli_core::guard::operation_level::OperationLevel;
use kafcli_core::ops::produce::message_producer::{
    ProduceOptions, prepare_messages, produce_messages, stream_messages,
};
use kafcli_core::ops::produce::produce_input::{
    InputMode, InputSource, ProduceInput, read_produce_inputs,
};
use kafcli_core::secret::resolver::SecretResolver;
use kafcli_core::template::ProcessEnvironment;
use serde_json::{Value, json};
use std::io::{self, Write};
use std::time::Duration;
use time::OffsetDateTime;

/// Command name written to the audit log.
const AUDIT_COMMAND_NAME: &str = "produce";

/// Produces the input messages to `--topic` of the selected context.
///
/// Order: select the context and warn if it is protected; build the encoder, which reads the schema
/// from its file or from the registry (reads only, never a registration); read and prepare every
/// message (input and template errors stop here, before any connection); stop after rendering when
/// `--dry-run`; ask the guard for a write; produce; audit; render. A run with failed deliveries
/// returns its report together with the error that makes the process exit 1.
pub fn produce(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    prompter: &dyn ConfirmationPrompter,
    arguments: &ProduceArguments,
) -> anyhow::Result<RenderedOutput> {
    let configuration = load_configuration(&execution.config_path)?;
    let selected = select_context(&configuration, execution.global.context.as_deref())?;
    warn_if_production(&selected.name, &selected.context);
    let protected = selected.context.protected;
    let context_name = selected.name;
    let target = format!("topic={}", arguments.topic);

    let selection = arguments.encoder_selection();
    warn_if_values_unvalidated(arguments);
    let timeout = Duration::from_secs(arguments.timeout_seconds);
    let registry = if encoder_needs_registry(&selection) {
        Some(connect_schema_registry(
            &context_name,
            &selected.context,
            resolver,
            timeout,
        )?)
    } else {
        None
    };
    let selected_encoder = build_value_encoder(&selection, registry.as_deref())?;

    let inputs = read_inputs(arguments)?;
    if inputs.is_empty() {
        bail!("no messages to produce");
    }
    let options = build_options(arguments);
    let now = OffsetDateTime::now_utc();
    let encoder = selected_encoder.encoder.as_ref();

    if arguments.dry_run {
        let messages = prepare_messages(&inputs, &options, encoder, &ProcessEnvironment, now)?;
        write_audit_record(&context_name, &target, AuditOutcome::DryRun, None);
        let view = DryRunView::from_messages(&messages, selected_encoder.schema_id.clone());
        return Ok(RenderedOutput::complete(render_output(
            &view,
            execution.global.output,
        )?));
    }

    // Template and encoding errors of every input show up here, before anything is sent. The full
    // run is then prepared one message at a time, so memory does not grow with --count.
    let first_round = ProduceOptions {
        count: 1,
        ..options.clone()
    };
    prepare_messages(&inputs, &first_round, encoder, &ProcessEnvironment, now)?;
    let mut messages = stream_messages(&inputs, &options, encoder, &ProcessEnvironment, now)?;
    let message_count = options
        .count
        .saturating_mul(u64::try_from(inputs.len()).unwrap_or(u64::MAX));

    let request = GuardRequest {
        level: OperationLevel::Write,
        description: format!(
            "produce {} messages to topic {}",
            message_count, arguments.topic
        ),
        confirmation_target: arguments.topic.clone(),
        assume_yes: arguments.yes,
        execute: false,
    };
    if let Err(error) = enforce_guard(&request, protected, prompter) {
        write_audit_record(&context_name, &target, AuditOutcome::Aborted, None);
        return Err(error.into());
    }

    let overrides = ContextOverrides {
        brokers: execution.global.brokers.clone(),
    };
    let connection_context = apply_context_overrides(selected.context, &overrides);
    let settings = ConnectionSettings {
        insecure: execution.global.insecure,
    };
    let report = match produce_messages(
        &connection_context,
        resolver,
        &settings,
        &options,
        &mut messages,
    ) {
        Ok(report) => report,
        Err(error) => {
            write_audit_record(&context_name, &target, AuditOutcome::Failed, None);
            return Err(error.into());
        }
    };

    let failed = report.failed;
    let outcome = if failed > 0 {
        AuditOutcome::Failed
    } else {
        AuditOutcome::Executed
    };
    let after = json!({"sent": report.sent, "failed": failed});
    write_audit_record(&context_name, &target, outcome, Some(after));

    let trailing_error =
        (failed > 0).then(|| anyhow::anyhow!("produce finished with {failed} failed deliveries"));
    let text = render_output(&ProduceReportView(report), execution.global.output)?;
    Ok(RenderedOutput {
        text: Some(text),
        trailing_error,
    })
}

/// Reads the input text from its source and splits it into produce units.
///
/// Standard input is locked only for this read, so the guard prompt can read answers afterwards.
fn read_inputs(arguments: &ProduceArguments) -> anyhow::Result<Vec<ProduceInput>> {
    let mode = match arguments.input {
        ProduceInputArgument::Json => InputMode::Json,
        ProduceInputArgument::Envelope => InputMode::Envelope,
    };
    let source = arguments.input_source();
    let inputs = if source == InputSource::Stdin {
        let mut stdin = io::stdin().lock();
        read_produce_inputs(&source, mode, &mut stdin)?
    } else {
        read_produce_inputs(&source, mode, &mut io::empty())?
    };
    Ok(inputs)
}

fn build_options(arguments: &ProduceArguments) -> ProduceOptions {
    ProduceOptions {
        topic: arguments.topic.clone(),
        key_template: arguments.key.clone(),
        header_templates: arguments.headers.clone(),
        partition: arguments.partition,
        count: arguments.count,
        rate_per_second: arguments.rate,
        render_templates: !arguments.no_template,
        timeout: Duration::from_secs(arguments.timeout_seconds),
    }
}

/// `--no-validate` with a JSON schema source sends the values unchecked; say so on stderr.
fn warn_if_values_unvalidated(arguments: &ProduceArguments) {
    let schema_given = arguments.schema_source.to_schema_source() != SchemaSource::Absent;
    if arguments.no_validate && arguments.format == ProduceFormat::Json && schema_given {
        let _ = writeln!(
            io::stderr().lock(),
            "warning: --no-validate: values are sent without schema validation"
        );
    }
}

/// Appends one audit record. A failure to write it is a warning, never a reason to fail the command.
fn write_audit_record(
    context_name: &str,
    target: &str,
    outcome: AuditOutcome,
    after: Option<Value>,
) {
    let mut record = AuditRecord::new(context_name, AUDIT_COMMAND_NAME, target, outcome);
    record.after = after;
    let written = default_state_directory().and_then(|directory| {
        JsonLinesAuditWriter::create_in_directory(&directory).append_record(&record)
    });
    if let Err(error) = written {
        let _ = writeln!(
            io::stderr().lock(),
            "{}",
            sanitize_for_terminal(&format!("warning: audit log not written: {error}"))
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::produce_arguments::ProduceWire;
    use crate::cli::schema_source_arguments::SchemaSourceArguments;

    #[test]
    fn options_carry_template_switch_and_timeout() {
        let arguments = ProduceArguments {
            topic: "orders".to_string(),
            format: ProduceFormat::Json,
            wire: ProduceWire::Raw,
            schema_source: SchemaSourceArguments {
                schema_file: None,
                schema_subject: None,
                schema_version: None,
                schema_id: None,
            },
            no_validate: false,
            value: Some("{}".to_string()),
            file: None,
            stdin: false,
            input: ProduceInputArgument::Json,
            key: Some("k".to_string()),
            headers: vec![("trace".to_string(), "t".to_string())],
            partition: Some(2),
            count: 3,
            rate: Some(10),
            no_template: true,
            dry_run: false,
            yes: false,
            timeout_seconds: 7,
        };

        let options = build_options(&arguments);

        assert!(!options.render_templates);
        assert_eq!(options.timeout, Duration::from_secs(7));
        assert_eq!(options.count, 3);
        assert_eq!(options.rate_per_second, Some(10));
        assert_eq!(options.key_template.as_deref(), Some("k"));
    }
}
