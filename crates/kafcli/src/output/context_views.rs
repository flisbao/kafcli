use crate::output::renderer::TableRenderable;
use crate::output::terminal_text::sanitize_for_terminal;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Cell, Table};
use kafcli_core::config::model::ConnectionContext;
use serde::Serialize;

/// One context as listed by `context list`, `context add` and `context use`.
#[derive(Serialize)]
pub struct ContextSummary {
    pub name: String,
    pub brokers: Vec<String>,
    pub auth_type: String,
    pub protected: bool,
    pub is_current: bool,
}

impl ContextSummary {
    pub fn from_context(name: &str, context: &ConnectionContext, is_current: bool) -> Self {
        Self {
            name: name.to_string(),
            brokers: context.brokers.clone(),
            auth_type: context.auth.type_name().to_string(),
            protected: context.protected,
            is_current,
        }
    }
}

#[derive(Serialize)]
pub struct ContextListView {
    pub contexts: Vec<ContextSummary>,
}

/// One context in detail, as shown by `context show`. Secret references are listed, never resolved.
#[derive(Serialize)]
pub struct ContextDetailView {
    pub name: String,
    pub brokers: Vec<String>,
    pub auth_type: String,
    pub principal: Option<String>,
    pub secret_references: Vec<String>,
    pub protected: bool,
    pub has_schema_registry: bool,
}

/// The name of the current context, as printed by `context current`.
#[derive(Serialize)]
pub struct CurrentContextView {
    pub name: String,
}

impl TableRenderable for ContextSummary {
    fn render_table(&self) -> String {
        build_summary_table([self]).to_string()
    }
}

impl TableRenderable for ContextListView {
    fn render_table(&self) -> String {
        if self.contexts.is_empty() {
            return "no contexts configured".to_string();
        }
        build_summary_table(&self.contexts).to_string()
    }
}

impl TableRenderable for ContextDetailView {
    fn render_table(&self) -> String {
        let mut table = build_table(&["FIELD", "VALUE"]);
        table.add_row(vec![
            Cell::new("name"),
            Cell::new(sanitize_for_terminal(&self.name)),
        ]);
        table.add_row(vec![
            Cell::new("brokers"),
            Cell::new(sanitize_for_terminal(&self.brokers.join(", "))),
        ]);
        table.add_row(vec![
            Cell::new("auth_type"),
            Cell::new(sanitize_for_terminal(&self.auth_type)),
        ]);
        table.add_row(vec![
            Cell::new("principal"),
            Cell::new(
                self.principal
                    .as_deref()
                    .map_or("-".to_string(), sanitize_for_terminal),
            ),
        ]);
        table.add_row(vec![
            Cell::new("secret_references"),
            Cell::new(sanitize_for_terminal(&join_or_dash(
                &self.secret_references,
            ))),
        ]);
        table.add_row(vec![Cell::new("protected"), Cell::new(self.protected)]);
        table.add_row(vec![
            Cell::new("schema_registry"),
            Cell::new(self.has_schema_registry),
        ]);
        table.to_string()
    }
}

impl TableRenderable for CurrentContextView {
    fn render_table(&self) -> String {
        sanitize_for_terminal(&self.name)
    }
}

fn build_summary_table<'a>(summaries: impl IntoIterator<Item = &'a ContextSummary>) -> Table {
    let mut table = build_table(&["CURRENT", "NAME", "BROKERS", "AUTH", "ENVIRONMENT"]);
    for summary in summaries {
        let environment = if summary.protected { "PRODUCTION" } else { "-" };
        table.add_row(vec![
            Cell::new(if summary.is_current { "*" } else { "" }),
            Cell::new(sanitize_for_terminal(&summary.name)),
            Cell::new(sanitize_for_terminal(&summary.brokers.join(", "))),
            Cell::new(sanitize_for_terminal(&summary.auth_type)),
            Cell::new(environment),
        ]);
    }
    table
}

fn build_table(headers: &[&str]) -> Table {
    let mut table = Table::new();
    *table.style_mut() = UTF8_FULL_CONDENSED;
    table.set_header(headers.iter().map(Cell::new).collect::<Vec<_>>());
    table
}

fn join_or_dash(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_list_neutralizes_terminal_control_characters() {
        let view = ContextListView {
            contexts: vec![ContextSummary {
                name: "\x1b[2Jdev".to_string(),
                brokers: vec!["broker\u{9b}31m:9092".to_string()],
                auth_type: "none".to_string(),
                protected: false,
                is_current: true,
            }],
        };

        let rendered = view.render_table();

        assert!(!rendered.chars().any(|character| character == '\x1b'));
        assert!(!rendered.contains('\u{9b}'));
        assert!(rendered.contains("?[2Jdev"));
    }

    #[test]
    fn current_context_view_renders_bare_sanitized_name() {
        let view = CurrentContextView {
            name: "\x1b[2Jdev".to_string(),
        };

        assert_eq!(view.render_table(), "?[2Jdev");
    }
}
