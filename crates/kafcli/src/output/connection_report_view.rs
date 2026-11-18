use crate::output::renderer::TableRenderable;
use crate::output::terminal_text::sanitize_for_terminal;
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
        table.add_row(vec![
            Cell::new("context"),
            Cell::new(sanitize_for_terminal(&self.context_name)),
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
            Cell::new("authenticated_principal"),
            Cell::new(
                self.authenticated_principal
                    .as_deref()
                    .map_or("-".to_string(), sanitize_for_terminal),
            ),
        ]);
        table.add_row(vec![Cell::new("brokers"), Cell::new(self.broker_count)]);
        table.add_row(vec![
            Cell::new("topics"),
            Cell::new(self.visible_topics.len()),
        ]);

        let mut rendered = table.to_string();
        for topic in self.visible_topics.iter().take(MAXIMUM_LISTED_TOPICS) {
            rendered.push('\n');
            rendered.push_str(&sanitize_for_terminal(topic));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn report_with_topics(topic_count: usize, principal: Option<&str>) -> ConnectionReport {
        ConnectionReport {
            context_name: "dev".to_string(),
            auth_type: "none".to_string(),
            principal: principal.map(str::to_string),
            authenticated_principal: None,
            broker_count: 1,
            visible_topics: (1..=topic_count)
                .map(|number| format!("topic-{number:02}"))
                .collect(),
        }
    }

    fn collect_listed_topic_lines(rendered: &str) -> Vec<&str> {
        rendered
            .lines()
            .filter(|line| line.starts_with("topic-"))
            .collect()
    }

    #[test]
    fn zero_topics_list_no_names_and_no_overflow_line() {
        let rendered = report_with_topics(0, Some("kafcli")).render_table();

        assert!(collect_listed_topic_lines(&rendered).is_empty());
        assert!(!rendered.contains("... and"));
    }

    #[test]
    fn fifty_topics_are_all_listed_without_overflow_line() {
        let rendered = report_with_topics(50, Some("kafcli")).render_table();

        assert_eq!(collect_listed_topic_lines(&rendered).len(), 50);
        assert!(rendered.contains("topic-50"));
        assert!(!rendered.contains("... and"));
    }

    #[test]
    fn fifty_one_topics_list_fifty_and_report_one_more() {
        let rendered = report_with_topics(51, Some("kafcli")).render_table();

        assert_eq!(collect_listed_topic_lines(&rendered).len(), 50);
        assert!(!rendered.contains("topic-51"));
        assert!(rendered.contains("... and 1 more"));
    }

    #[test]
    fn fifty_two_topics_list_fifty_and_report_two_more() {
        let rendered = report_with_topics(52, Some("kafcli")).render_table();

        assert_eq!(collect_listed_topic_lines(&rendered).len(), 50);
        assert!(!rendered.contains("topic-52"));
        assert!(rendered.contains("... and 2 more"));
    }

    #[test]
    fn missing_principal_renders_dash() {
        let rendered = report_with_topics(0, None).render_table();

        assert!(
            rendered
                .lines()
                .any(|line| line.contains("principal") && line.contains("┆ -")),
            "rendered: {rendered}"
        );
    }

    #[test]
    fn authenticated_principal_row_shows_token_principal() {
        let mut report = report_with_topics(0, Some("kafcli"));
        report.authenticated_principal = Some("app-123".to_string());

        let rendered = report.render_table();

        assert!(
            rendered
                .lines()
                .any(|line| line.contains("authenticated_principal") && line.contains("app-123")),
            "rendered: {rendered}"
        );
    }

    #[test]
    fn missing_authenticated_principal_renders_dash() {
        let rendered = report_with_topics(0, Some("kafcli")).render_table();

        assert!(
            rendered
                .lines()
                .any(|line| line.contains("authenticated_principal") && line.contains("┆ -")),
            "rendered: {rendered}"
        );
    }

    #[test]
    fn terminal_control_characters_in_names_are_neutralized() {
        let mut report = report_with_topics(1, Some("\x1b[31mevil\x1b[0m"));
        report.visible_topics = vec!["bad\u{9b}2Jtopic".to_string()];

        let rendered = report.render_table();

        assert!(!rendered.chars().any(|character| character == '\x1b'));
        assert!(!rendered.contains('\u{9b}'));
        assert!(rendered.contains("bad?2Jtopic"));
    }
}
