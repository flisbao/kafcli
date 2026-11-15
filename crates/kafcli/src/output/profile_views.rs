use crate::output::renderer::TableRenderable;
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
        summary_table([self]).to_string()
    }
}

impl TableRenderable for ProfileListView {
    fn render_table(&self) -> String {
        if self.profiles.is_empty() {
            return "no profiles configured".to_string();
        }
        summary_table(&self.profiles).to_string()
    }
}

impl TableRenderable for ProfileDetailView {
    fn render_table(&self) -> String {
        let mut table = new_table(&["FIELD", "VALUE"]);
        table.add_row(vec![Cell::new("name"), Cell::new(&self.name)]);
        table.add_row(vec![
            Cell::new("brokers"),
            Cell::new(self.brokers.join(", ")),
        ]);
        table.add_row(vec![Cell::new("auth_type"), Cell::new(&self.auth_type)]);
        table.add_row(vec![
            Cell::new("principal"),
            Cell::new(self.principal.as_deref().unwrap_or("-")),
        ]);
        table.add_row(vec![
            Cell::new("secret_references"),
            Cell::new(joined_or_dash(&self.secret_references)),
        ]);
        table.add_row(vec![Cell::new("protected"), Cell::new(self.protected)]);
        table.add_row(vec![
            Cell::new("schema_registry"),
            Cell::new(self.has_schema_registry),
        ]);
        table.to_string()
    }
}

fn summary_table<'a>(summaries: impl IntoIterator<Item = &'a ProfileSummary>) -> Table {
    let mut table = new_table(&["NAME", "BROKERS", "AUTH", "PROTECTED", "DEFAULT"]);
    for summary in summaries {
        table.add_row(vec![
            Cell::new(&summary.name),
            Cell::new(summary.brokers.join(", ")),
            Cell::new(&summary.auth_type),
            Cell::new(summary.protected),
            Cell::new(summary.is_default),
        ]);
    }
    table
}

fn new_table(headers: &[&str]) -> Table {
    let mut table = Table::new();
    *table.style_mut() = UTF8_FULL_CONDENSED;
    table.set_header(headers.iter().map(Cell::new).collect::<Vec<_>>());
    table
}

fn joined_or_dash(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_string()
    } else {
        values.join(", ")
    }
}
