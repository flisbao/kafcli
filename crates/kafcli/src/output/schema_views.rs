use crate::output::context_views::build_table;
use crate::output::renderer::TableRenderable;
use crate::output::terminal_text::sanitize_for_terminal;
use comfy_table::Cell;
use kafcli_core::registry::registered_schema::{RegisteredSchema, SchemaId, SchemaKind};
use serde::Serialize;

/// Columns of the key and value tables of the schema commands.
const KEY_VALUE_HEADERS: &[&str] = &["FIELD", "VALUE"];
/// Indentation of the schema text in the table view.
const SCHEMA_INDENT: &str = "  ";

/// The schema registered by `schema register`. The schema text is not repeated.
#[derive(Serialize)]
pub struct RegisteredSchemaView {
    pub subject: Option<String>,
    pub schema_id: SchemaId,
    pub version: Option<u32>,
    #[serde(rename = "type")]
    pub schema_type: SchemaKind,
}

impl RegisteredSchemaView {
    pub fn from_registered(schema: &RegisteredSchema) -> Self {
        Self {
            subject: schema.subject.clone(),
            schema_id: schema.id.clone(),
            version: schema.version,
            schema_type: schema.kind,
        }
    }
}

impl TableRenderable for RegisteredSchemaView {
    fn render_table(&self) -> String {
        let mut table = build_table(KEY_VALUE_HEADERS);
        table.add_row(vec![
            Cell::new("subject"),
            Cell::new(optional_text(self.subject.as_deref())),
        ]);
        table.add_row(vec![
            Cell::new("schema_id"),
            Cell::new(sanitize_for_terminal(&self.schema_id.to_string())),
        ]);
        table.add_row(vec![
            Cell::new("version"),
            Cell::new(optional_number(self.version)),
        ]);
        table.add_row(vec![Cell::new("type"), Cell::new(self.schema_type.name())]);
        table.to_string()
    }
}

/// A schema read by `schema get`. `schema` is the registry text, as the registry sent it.
#[derive(Serialize)]
pub struct SchemaDetailView {
    pub subject: Option<String>,
    pub schema_id: SchemaId,
    pub version: Option<u32>,
    #[serde(rename = "type")]
    pub schema_type: SchemaKind,
    pub schema: String,
}

impl SchemaDetailView {
    pub fn from_registered(schema: RegisteredSchema) -> Self {
        Self {
            subject: schema.subject,
            schema_id: schema.id,
            version: schema.version,
            schema_type: schema.kind,
            schema: schema.text,
        }
    }
}

impl TableRenderable for SchemaDetailView {
    fn render_table(&self) -> String {
        let mut table = build_table(KEY_VALUE_HEADERS);
        table.add_row(vec![
            Cell::new("subject"),
            Cell::new(optional_text(self.subject.as_deref())),
        ]);
        table.add_row(vec![
            Cell::new("schema_id"),
            Cell::new(sanitize_for_terminal(&self.schema_id.to_string())),
        ]);
        table.add_row(vec![
            Cell::new("version"),
            Cell::new(optional_number(self.version)),
        ]);
        table.add_row(vec![Cell::new("type"), Cell::new(self.schema_type.name())]);
        table.add_row(vec![
            Cell::new("schema"),
            Cell::new(render_schema_text(&self.schema)),
        ]);
        table.to_string()
    }
}

/// The report of `schema check-compat`.
#[derive(Serialize)]
pub struct CompatibilityView {
    pub subject: String,
    pub version: String,
    pub compatible: bool,
    pub messages: Vec<String>,
}

impl TableRenderable for CompatibilityView {
    fn render_table(&self) -> String {
        let mut table = build_table(KEY_VALUE_HEADERS);
        table.add_row(vec![
            Cell::new("subject"),
            Cell::new(sanitize_for_terminal(&self.subject)),
        ]);
        table.add_row(vec![
            Cell::new("version"),
            Cell::new(sanitize_for_terminal(&self.version)),
        ]);
        table.add_row(vec![
            Cell::new("compatible"),
            Cell::new(if self.compatible { "yes" } else { "no" }),
        ]);
        let messages = if self.messages.is_empty() {
            "-".to_string()
        } else {
            self.messages
                .iter()
                .map(|message| sanitize_for_terminal(message))
                .collect::<Vec<_>>()
                .join("\n")
        };
        table.add_row(vec![Cell::new("messages"), Cell::new(messages)]);
        table.to_string()
    }
}

fn optional_text(text: Option<&str>) -> String {
    text.map_or("-".to_string(), sanitize_for_terminal)
}

fn optional_number(number: Option<u32>) -> String {
    number.map_or("-".to_string(), |number| number.to_string())
}

/// Pretty-prints the schema when it is JSON, then replaces control characters line by line, so the
/// registry text cannot move the cursor or rewrite earlier lines. Every line is indented.
pub fn render_schema_text(text: &str) -> String {
    let shown = match serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|value| serde_json::to_string_pretty(&value).ok())
    {
        Some(pretty) => pretty,
        None => text.to_string(),
    };
    shown
        .split('\n')
        .map(|line| format!("{SCHEMA_INDENT}{}", sanitize_for_terminal(line)))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_text_is_pretty_printed_and_indented() {
        let rendered = render_schema_text(r#"{"type":"string"}"#);

        assert_eq!(rendered, "  {\n    \"type\": \"string\"\n  }");
    }

    #[test]
    fn schema_text_that_is_not_json_keeps_its_lines_neutralized() {
        let rendered = render_schema_text("line one\u{1b}[2J\nline two");

        assert_eq!(rendered, "  line one?[2J\n  line two");
    }
}
