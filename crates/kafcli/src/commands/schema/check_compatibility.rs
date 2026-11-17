use crate::cli::schema_arguments::CheckCompatibilityArguments;
use crate::commands::registry_connection::connect_schema_registry;
use crate::commands::schema::read_checked_schema_text;
use crate::execution_context::ExecutionContext;
use crate::output::production_warning::warn_if_production;
use crate::output::renderer::{RenderedOutput, render_output};
use crate::output::schema_views::CompatibilityView;
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::selection::select_context;
use kafcli_core::secret::resolver::SecretResolver;
use std::time::Duration;

/// Checks `--schema-file` against a version of `--subject`. Reads only: nothing is registered, the
/// guard does not apply and nothing is written to the audit log.
///
/// An incompatible schema prints the report and then fails, so the process exits with code 1.
pub fn check_compatibility(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    arguments: &CheckCompatibilityArguments,
) -> anyhow::Result<RenderedOutput> {
    let configuration = load_configuration(&execution.config_path)?;
    let selected = select_context(&configuration, execution.global.context.as_deref())?;
    warn_if_production(&selected.name, &selected.context);
    let kind = arguments.schema_type.to_schema_kind();
    let text = read_checked_schema_text(&arguments.schema_file, kind)?;
    let timeout = Duration::from_secs(arguments.timeout_seconds);
    let registry = connect_schema_registry(&selected.name, &selected.context, resolver, timeout)?;

    let version = arguments.selected_version();
    let report = registry.check_compatibility(&arguments.subject, version, kind, &text)?;
    let compatible = report.compatible;
    let view = CompatibilityView {
        subject: arguments.subject.clone(),
        version: version.to_string(),
        compatible,
        messages: report.messages,
    };
    let text = render_output(&view, execution.global.output)?;
    let trailing_error = (!compatible).then(|| {
        anyhow::anyhow!(
            "schema is not compatible with subject '{}' version {version}",
            arguments.subject
        )
    });
    Ok(RenderedOutput {
        text: Some(text),
        trailing_error,
    })
}
