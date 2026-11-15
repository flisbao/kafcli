use crate::output::renderer::TableRenderable;
use crate::output::terminal_text::sanitize_for_terminal;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Cell, Table};
use kafcli_core::config::model::Profile;
use serde::Serialize;

/// One profile as listed by `profile list`, `profile add` and `profile use`.
#[derive(Serialize)]
pub struct ProfileSummary {
    pub name: String,
    pub brokers: Vec<String>,
    pub auth_type: String,
    pub protected: bool,
    pub is_default: bool,
}

impl ProfileSummary {
    pub fn from_profile(name: &str, profile: &Profile, is_default: bool) -> Self {
        Self {
            name: name.to_string(),
            brokers: profile.brokers.clone(),
            auth_type: profile.auth.type_name().to_string(),
            protected: profile.protected,
            is_default,
        }
    }
}

#[derive(Serialize)]
pub struct ProfileListView {
    pub profiles: Vec<ProfileSummary>,
}

/// One profile in detail, as shown by `profile show`. Secret references are listed, never resolved.
#[derive(Serialize)]
pub struct ProfileDetailView {
    pub name: String,
    pub brokers: Vec<String>,
    pub auth_type: String,
    pub principal: Option<String>,
    pub secret_references: Vec<String>,
    pub protected: bool,
    pub has_schema_registry: bool,
}

impl TableRenderable for ProfileSummary {
    fn render_table(&self) -> String {
        build_summary_table([self]).to_string()
    }
}

impl TableRenderable for ProfileListView {
    fn render_table(&self) -> String {
        if self.profiles.is_empty() {
            return "no profiles configured".to_string();
        }
        build_summary_table(&self.profiles).to_string()
    }
}

impl TableRenderable for ProfileDetailView {
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

fn build_summary_table<'a>(summaries: impl IntoIterator<Item = &'a ProfileSummary>) -> Table {
    let mut table = build_table(&["NAME", "BROKERS", "AUTH", "PROTECTED", "DEFAULT"]);
    for summary in summaries {
        table.add_row(vec![
            Cell::new(sanitize_for_terminal(&summary.name)),
            Cell::new(sanitize_for_terminal(&summary.brokers.join(", "))),
            Cell::new(sanitize_for_terminal(&summary.auth_type)),
            Cell::new(summary.protected),
            Cell::new(summary.is_default),
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
    fn profile_list_neutralizes_terminal_control_characters() {
        let view = ProfileListView {
            profiles: vec![ProfileSummary {
                name: "\x1b[2Jdev".to_string(),
                brokers: vec!["broker\u{9b}31m:9092".to_string()],
                auth_type: "none".to_string(),
                protected: false,
                is_default: true,
            }],
        };

        let rendered = view.render_table();

        assert!(!rendered.chars().any(|character| character == '\x1b'));
        assert!(!rendered.contains('\u{9b}'));
        assert!(rendered.contains("?[2Jdev"));
    }
}
