use crate::cli::schema_arguments::RegisterSchemaArguments;
use crate::commands::registry_connection::connect_schema_registry;
use crate::commands::schema::read_checked_schema_text;
use crate::execution_context::ExecutionContext;
use crate::output::production_warning::warn_if_production;
use crate::output::renderer::{RenderedOutput, render_output};
use crate::output::schema_views::RegisteredSchemaView;
use crate::output::terminal_text::sanitize_for_terminal;
use kafcli_core::audit::audit_record::{AuditOutcome, AuditRecord};
use kafcli_core::audit::audit_writer::{AuditWriter, JsonLinesAuditWriter};
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::paths::default_state_directory;
use kafcli_core::config::selection::select_context;
use kafcli_core::guard::confirmation_prompter::ConfirmationPrompter;
use kafcli_core::guard::guard_enforcer::enforce_guard;
use kafcli_core::guard::guard_request::GuardRequest;
use kafcli_core::guard::operation_level::OperationLevel;
use kafcli_core::registry::registered_schema::RegisteredSchema;
use kafcli_core::secret::resolver::SecretResolver;
use serde_json::{Value, json};
use std::io::{self, Write};
use std::time::Duration;

/// Command name written to the audit log.
const AUDIT_COMMAND_NAME: &str = "schema register";

/// Registers `--schema-file` under `--subject` in the registry of the selected context.
///
/// Order: select the context and warn if it is protected; read and check the file locally (nothing
/// is sent before this); ask the guard for a write; connect and register; audit; render. The audit
/// record holds the subject, the schema id and the version, never the schema text.
pub fn register_schema(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    prompter: &dyn ConfirmationPrompter,
    arguments: &RegisterSchemaArguments,
) -> anyhow::Result<RenderedOutput> {
    let configuration = load_configuration(&execution.config_path)?;
    let selected = select_context(&configuration, execution.global.context.as_deref())?;
    warn_if_production(&selected.name, &selected.context);
    let protected = selected.context.protected;
    let context_name = selected.name;
    let target = format!("subject={}", arguments.subject);
    let kind = arguments.schema_type.to_schema_kind();
    let text = read_checked_schema_text(&arguments.schema_file, kind)?;

    let request = GuardRequest {
        level: OperationLevel::Write,
        description: format!("register schema for subject {}", arguments.subject),
        confirmation_target: arguments.subject.clone(),
        assume_yes: arguments.yes,
        execute: false,
    };
    if let Err(error) = enforce_guard(&request, protected, prompter) {
        write_audit_record(&context_name, &target, AuditOutcome::Aborted, None);
        return Err(error.into());
    }

    let timeout = Duration::from_secs(arguments.timeout_seconds);
    let registered = match register_through_registry(
        &context_name,
        &selected.context,
        resolver,
        timeout,
        arguments,
        &text,
    ) {
        Ok(registered) => registered,
        Err(error) => {
            write_audit_record(&context_name, &target, AuditOutcome::Failed, None);
            return Err(error);
        }
    };

    let after = json!({"schema_id": registered.id, "version": registered.version});
    write_audit_record(&context_name, &target, AuditOutcome::Executed, Some(after));
    let view = RegisteredSchemaView::from_registered(&registered);
    Ok(RenderedOutput::complete(render_output(
        &view,
        execution.global.output,
    )?))
}

/// Builds the registry client of the context and registers the schema through it.
fn register_through_registry(
    context_name: &str,
    context: &kafcli_core::config::model::ConnectionContext,
    resolver: &dyn SecretResolver,
    timeout: Duration,
    arguments: &RegisterSchemaArguments,
    text: &str,
) -> anyhow::Result<RegisteredSchema> {
    let registry = connect_schema_registry(context_name, context, resolver, timeout)?;
    let kind = arguments.schema_type.to_schema_kind();
    Ok(registry.register_schema(&arguments.subject, kind, text)?)
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
