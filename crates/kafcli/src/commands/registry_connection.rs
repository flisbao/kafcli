use kafcli_core::codec::codec_selection::{
    DecoderSelection, EncoderSelection, ValueFormat, WireFormat, selection_needs_registry,
};
use kafcli_core::config::model::ConnectionContext;
use kafcli_core::config::validation::validate_schema_registry;
use kafcli_core::registry::registry_factory::build_schema_registry;
use kafcli_core::registry::schema_registry::SchemaRegistry;
use kafcli_core::secret::resolver::SecretResolver;
use std::time::Duration;

/// Whether `produce` needs a registry client. `--no-validate` with `--format json --wire raw` never
/// reads the schema, so the context may have no registry at all.
pub fn encoder_needs_registry(selection: &EncoderSelection) -> bool {
    let schema_unused = !selection.validate
        && selection.format == ValueFormat::Json
        && selection.wire == WireFormat::Raw;
    !schema_unused && selection_needs_registry(selection.format, selection.wire, &selection.schema)
}

/// Whether `consume` needs a registry client. `--format json --wire raw` never reads a registry.
pub fn decoder_needs_registry(selection: &DecoderSelection) -> bool {
    let raw_json = selection.format == ValueFormat::Json && selection.wire == WireFormat::Raw;
    !raw_json && selection_needs_registry(selection.format, selection.wire, &selection.schema)
}

/// Builds the registry client of the context, resolving its secrets. No request is sent here.
pub fn connect_schema_registry(
    context_name: &str,
    context: &ConnectionContext,
    resolver: &dyn SecretResolver,
    timeout: Duration,
) -> anyhow::Result<Box<dyn SchemaRegistry>> {
    let Some(config) = context.schema_registry.as_ref() else {
        anyhow::bail!(
            "context '{context_name}' has no schema_registry; add one with 'kafcli context add ... --registry-type'"
        );
    };
    // Checked here, not only by `context add`: a hand-edited context must not reach the registry.
    validate_schema_registry(config, context.protected)?;
    Ok(build_schema_registry(config, resolver, timeout)?)
}
