use crate::cli::schema_arguments::GetSchemaArguments;
use crate::commands::registry_connection::connect_schema_registry;
use crate::execution_context::ExecutionContext;
use crate::output::production_warning::warn_if_production;
use crate::output::renderer::{RenderedOutput, render_output};
use crate::output::schema_views::SchemaDetailView;
use kafcli_core::codec::codec_selection::{WireFormat, schema_id_from_text};
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::selection::select_context;
use kafcli_core::registry::registered_schema::SchemaVersionSelector;
use kafcli_core::secret::resolver::SecretResolver;
use std::time::Duration;

/// Reads one schema by id, or by subject and version, from the registry of the selected context.
/// Reads only: the guard does not apply and nothing is written to the audit log.
pub fn get_schema(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    arguments: &GetSchemaArguments,
) -> anyhow::Result<RenderedOutput> {
    let configuration = load_configuration(&execution.config_path)?;
    let selected = select_context(&configuration, execution.global.context.as_deref())?;
    warn_if_production(&selected.name, &selected.context);
    let timeout = Duration::from_secs(arguments.timeout_seconds);
    let registry = connect_schema_registry(&selected.name, &selected.context, resolver, timeout)?;

    let schema = if let Some(id) = &arguments.id {
        let schema_id = schema_id_from_text(id, WireFormat::Raw, registry.registry_type())?;
        registry.fetch_schema_by_id(&schema_id)?
    } else {
        let subject = arguments.subject.as_deref().unwrap_or_default();
        let version = arguments.version.unwrap_or(SchemaVersionSelector::Latest);
        registry.fetch_schema_by_subject(subject, version)?
    };
    let view = SchemaDetailView::from_registered(schema);
    Ok(RenderedOutput::complete(render_output(
        &view,
        execution.global.output,
    )?))
}
