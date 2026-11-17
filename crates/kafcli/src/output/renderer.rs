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

/// What a command leaves for `main`: the text for stdout, and an error to report after that text.
///
/// A run can produce its full report and still fail (some deliveries were not acknowledged), so the
/// report must reach stdout before the error reaches stderr.
pub struct RenderedOutput {
    /// Text for stdout. `None` when the command already wrote its stdout while it ran.
    pub text: Option<String>,
    pub trailing_error: Option<anyhow::Error>,
}

impl RenderedOutput {
    pub fn complete(text: String) -> Self {
        Self {
            text: Some(text),
            trailing_error: None,
        }
    }

    /// For a command that streamed its stdout itself and has nothing left to print.
    pub fn streamed() -> Self {
        Self {
            text: None,
            trailing_error: None,
        }
    }
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
