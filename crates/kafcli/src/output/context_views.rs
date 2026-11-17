use crate::output::renderer::TableRenderable;
use crate::output::terminal_text::sanitize_for_terminal;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Cell, Table};
use kafcli_core::config::model::ConnectionContext;
use kafcli_core::config::schema_registry_model::SchemaRegistryConfig;
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
    pub schema_registry: Option<RegistryDetailView>,
}

/// The Schema Registry of a context, as shown by `context show`. Secret references are listed, never resolved.
#[derive(Serialize)]
pub struct RegistryDetailView {
    pub registry_type: String,
    pub url: String,
    pub group: Option<String>,
    pub auth_type: String,
    pub secret_references: Vec<String>,
}

impl RegistryDetailView {
    pub fn from_config(config: &SchemaRegistryConfig) -> Self {
        let group = match config {
            SchemaRegistryConfig::Azure(azure) => Some(azure.group.clone()),
            SchemaRegistryConfig::Confluent(_) => None,
        };
        Self {
            registry_type: config.type_name().to_string(),
            url: redact_url_userinfo(config.url()),
            group,
            auth_type: config.auth().type_name().to_string(),
            secret_references: config
                .auth()
                .secret_references()
                .into_iter()
                .map(ToString::to_string)
                .collect(),
        }
    }
}

/// Hides the `user:password@` part of the URL authority, so a hand-edited URL is never echoed.
fn redact_url_userinfo(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, remainder) = rest.split_at(authority_end);
    match authority.rsplit_once('@') {
        Some((_, host)) => format!("{scheme}://***@{host}{remainder}"),
        None => url.to_string(),
    }
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
        for (field, value) in schema_registry_rows(self.schema_registry.as_ref()) {
            table.add_row(vec![
                Cell::new(field),
                Cell::new(sanitize_for_terminal(&value)),
            ]);
        }
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

pub fn build_table(headers: &[&str]) -> Table {
    let mut table = Table::new();
    *table.style_mut() = UTF8_FULL_CONDENSED;
    table.set_header(headers.iter().map(Cell::new).collect::<Vec<_>>());
    table
}

fn schema_registry_rows(registry: Option<&RegistryDetailView>) -> Vec<(&'static str, String)> {
    let Some(registry) = registry else {
        return vec![("schema_registry", "-".to_string())];
    };
    vec![
        (
            "schema_registry.registry_type",
            registry.registry_type.clone(),
        ),
        ("schema_registry.url", registry.url.clone()),
        (
            "schema_registry.group",
            registry.group.clone().unwrap_or_else(|| "-".to_string()),
        ),
        ("schema_registry.auth_type", registry.auth_type.clone()),
        (
            "schema_registry.secret_references",
            join_or_dash(&registry.secret_references),
        ),
    ]
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
    use kafcli_core::config::schema_registry_model::{ConfluentRegistryConfig, RegistryAuthConfig};

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

    fn confluent_url(url: &str) -> RegistryDetailView {
        RegistryDetailView::from_config(&SchemaRegistryConfig::Confluent(ConfluentRegistryConfig {
            url: url.to_string(),
            auth: RegistryAuthConfig::Anonymous,
            ca_location: None,
        }))
    }

    #[test]
    fn hides_userinfo_of_registry_url() {
        let view = confluent_url("https://user:pass@sr.example:8081/base");

        assert_eq!(view.url, "https://***@sr.example:8081/base");
    }

    #[test]
    fn keeps_registry_url_without_userinfo() {
        let view = confluent_url("https://sr.example:8081/base");

        assert_eq!(view.url, "https://sr.example:8081/base");
    }

    #[test]
    fn keeps_at_sign_outside_the_authority() {
        let view = confluent_url("https://sr.example:8081/topics/a@b");

        assert_eq!(view.url, "https://sr.example:8081/topics/a@b");
    }
}
