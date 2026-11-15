use crate::output::output_format::OutputFormat;
use serde::Serialize;

/// Version of the JSON envelope; bump it when the shape of `data` changes incompatibly.
pub const OUTPUT_SCHEMA_VERSION: u32 = 1;

/// A result that can be shown as a human-readable table.
pub trait TableRenderable {
    fn render_table(&self) -> String;
}

#[derive(Serialize)]
struct JsonEnvelope<'a, T: Serialize> {
    kafcli_output: u32,
    data: &'a T,
}

/// Renders a command result in the requested format.
pub fn render_output<T: Serialize + TableRenderable>(
    value: &T,
    format: OutputFormat,
) -> anyhow::Result<String> {
    match format {
        OutputFormat::Table => Ok(value.render_table()),
        OutputFormat::Json => Ok(serde_json::to_string_pretty(&JsonEnvelope {
            kafcli_output: OUTPUT_SCHEMA_VERSION,
            data: value,
        })?),
    }
}
