use crate::output::renderer::TableRenderable;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Cell, Table};
use kafcli_core::connection::connection_probe::ConnectionReport;

/// Topic names shown in the table; JSON output always carries the full list.
const MAXIMUM_LISTED_TOPICS: usize = 50;

impl TableRenderable for ConnectionReport {
    fn render_table(&self) -> String {
        let mut table = Table::new();
        *table.style_mut() = UTF8_FULL_CONDENSED;
        table.set_header(vec![Cell::new("FIELD"), Cell::new("VALUE")]);
        table.add_row(vec![Cell::new("profile"), Cell::new(&self.profile_name)]);
        table.add_row(vec![Cell::new("auth_type"), Cell::new(&self.auth_type)]);
        table.add_row(vec![
            Cell::new("principal"),
            Cell::new(self.principal.as_deref().unwrap_or("-")),
        ]);
        table.add_row(vec![Cell::new("brokers"), Cell::new(self.broker_count)]);
        table.add_row(vec![
            Cell::new("topics"),
            Cell::new(self.visible_topics.len()),
        ]);

        let mut rendered = table.to_string();
        for topic in self.visible_topics.iter().take(MAXIMUM_LISTED_TOPICS) {
            rendered.push('\n');
            rendered.push_str(topic);
        }
        let hidden_topics = self
            .visible_topics
            .len()
            .saturating_sub(MAXIMUM_LISTED_TOPICS);
        if hidden_topics > 0 {
            rendered.push_str(&format!("\n... and {hidden_topics} more"));
        }
        rendered
    }
}
