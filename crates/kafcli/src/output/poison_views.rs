use crate::output::offset_reset_views::{describe_group_activity, final_line};
use crate::output::renderer::TableRenderable;
use crate::output::terminal_text::sanitize_for_terminal;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Cell, Table};
use kafcli_core::ops::poison::poison_finder::{PoisonReport, PoisonStatus};
use kafcli_core::ops::poison::poison_skip::PoisonSkipOutcome;
use serde::Serialize;

/// Longest decode error shown in the table, in characters; JSON keeps the whole text.
const MAX_DECODE_ERROR_CHARACTERS: usize = 120;
const NULL_CELL: &str = "-";

/// The `poison find` result. The JSON is the report itself.
#[derive(Debug, Serialize)]
pub struct PoisonView {
    #[serde(flatten)]
    pub report: PoisonReport,
}

impl TableRenderable for PoisonView {
    fn render_table(&self) -> String {
        let heading = format!(
            "GROUP {} TOPIC {}",
            sanitize_for_terminal(&self.report.group),
            sanitize_for_terminal(&self.report.topic)
        );
        if self.report.candidates.is_empty() {
            return format!("{heading}\nno partition with lag");
        }
        let mut table = Table::new();
        *table.style_mut() = UTF8_FULL_CONDENSED;
        table.set_header(vec![
            Cell::new("PARTITION"),
            Cell::new("COMMITTED"),
            Cell::new("LAG"),
            Cell::new("STATUS"),
            Cell::new("OFFSET"),
            Cell::new("VALUE-SIZE"),
            Cell::new("DECODE-ERROR"),
        ]);
        for candidate in &self.report.candidates {
            table.add_row(vec![
                Cell::new(candidate.partition),
                Cell::new(candidate.committed_offset),
                Cell::new(candidate.lag),
                Cell::new(status_text(&candidate.status)),
                Cell::new(format_optional(candidate.message_offset)),
                Cell::new(
                    candidate
                        .value_size
                        .map_or(NULL_CELL.to_owned(), |size| size.to_string()),
                ),
                Cell::new(
                    candidate
                        .decode_error
                        .as_deref()
                        .map_or(NULL_CELL.to_owned(), shorten_decode_error),
                ),
            ]);
        }
        format!("{heading}\n{table}")
    }
}

/// The `poison skip` result: what is copied where, and the commit that moves past it.
#[derive(Debug, Serialize)]
pub struct PoisonSkipView {
    #[serde(flatten)]
    pub outcome: PoisonSkipOutcome,
}

impl PoisonSkipView {
    pub fn from_outcome(outcome: PoisonSkipOutcome) -> Self {
        Self { outcome }
    }
}

impl TableRenderable for PoisonSkipView {
    fn render_table(&self) -> String {
        let outcome = &self.outcome;
        let mut lines = vec![format!(
            "SOURCE {}/{}@{}  DLT {}  COMMIT {} -> {}",
            sanitize_for_terminal(&outcome.topic),
            outcome.partition,
            outcome.offset,
            sanitize_for_terminal(&outcome.dead_letter_topic),
            outcome.offset,
            outcome.offset.saturating_add(1)
        )];
        if let Some(delivery) = &outcome.dead_letter_delivery {
            lines.push(format!(
                "copied to partition {} offset {}",
                delivery.partition, delivery.offset
            ));
        }
        lines.push(format!(
            "headers added: {}",
            outcome.headers_added.join(", ")
        ));
        lines.push(format!(
            "group state: {}",
            describe_group_activity(&outcome.group_activity)
        ));
        lines.push(final_line(
            outcome.dry_run,
            outcome.committed_offset.is_some(),
            outcome.backup_file.as_ref(),
        ));
        lines.join("\n")
    }
}

fn status_text(status: &PoisonStatus) -> &'static str {
    match status {
        PoisonStatus::Undecodable => "undecodable",
        PoisonStatus::Decodable => "decodable",
        PoisonStatus::BelowLogStart => "below-log-start",
        PoisonStatus::NoMessage => "no-message",
    }
}

fn shorten_decode_error(error: &str) -> String {
    let sanitized = sanitize_for_terminal(error);
    if sanitized.chars().count() <= MAX_DECODE_ERROR_CHARACTERS {
        return sanitized;
    }
    let mut shortened: String = sanitized
        .chars()
        .take(MAX_DECODE_ERROR_CHARACTERS - 3)
        .collect();
    shortened.push_str("...");
    shortened
}

fn format_optional(value: Option<i64>) -> String {
    value.map_or(NULL_CELL.to_owned(), |value| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafcli_core::ops::offsets::group_activity::GroupActivity;
    use kafcli_core::ops::poison::poison_finder::{PoisonCandidate, PoisonReport, PoisonStatus};
    use kafcli_core::ops::poison::poison_skip::PoisonSkipOutcome;

    fn report(decode_error: Option<String>) -> PoisonReport {
        PoisonReport {
            group: "g".to_owned(),
            topic: "t".to_owned(),
            candidates: vec![PoisonCandidate {
                partition: 0,
                committed_offset: 5,
                log_start_offset: 0,
                log_end_offset: 10,
                lag: 5,
                status: PoisonStatus::Undecodable,
                message_offset: Some(5),
                key_size: Some(1),
                value_size: Some(3),
                decode_error,
                envelope: None,
            }],
            warnings: Vec::new(),
        }
    }

    #[test]
    fn decode_error_is_sanitized_and_truncated() {
        let error = format!("\x1b[31mbad{}", "x".repeat(300));
        let view = PoisonView {
            report: report(Some(error)),
        };

        let table = view.render_table();

        assert!(!table.contains('\x1b'), "{table}");
        assert!(table.contains("?[31mbad"), "{table}");
        let longest_run = table
            .split(|character: char| character != 'x')
            .map(str::len)
            .max()
            .unwrap_or_default();
        assert!(longest_run < 120, "{longest_run}");
        assert!(table.contains("undecodable"), "{table}");
    }

    fn skip_outcome(dry_run: bool) -> PoisonSkipOutcome {
        PoisonSkipOutcome {
            group: "g".to_owned(),
            topic: "t".to_owned(),
            partition: 0,
            offset: 5,
            dead_letter_topic: "t.DLT".to_owned(),
            dry_run,
            group_activity: GroupActivity::Empty,
            headers_added: vec!["kafcli-original-topic".to_owned()],
            dead_letter_delivery: None,
            committed_offset: (!dry_run).then_some(6),
            backup_file: (!dry_run).then(|| std::path::PathBuf::from("/b.json")),
            warnings: Vec::new(),
        }
    }

    #[test]
    fn skip_view_shows_source_dlt_and_commit() {
        let table = PoisonSkipView::from_outcome(skip_outcome(true)).render_table();

        assert!(
            table.starts_with("SOURCE t/0@5  DLT t.DLT  COMMIT 5 -> 6"),
            "{table}"
        );
        assert!(
            table.contains("headers added: kafcli-original-topic"),
            "{table}"
        );
        assert!(table.contains("group state: empty"), "{table}");
        assert!(
            table.ends_with("DRY RUN: nothing was committed; re-run with --execute to apply"),
            "{table}"
        );
        let executed = PoisonSkipView::from_outcome(skip_outcome(false)).render_table();
        assert!(
            executed.ends_with("committed; backup: /b.json"),
            "{executed}"
        );
    }

    #[test]
    fn json_is_the_report() {
        let view = PoisonView {
            report: report(None),
        };

        let json = serde_json::to_value(&view).unwrap();

        assert_eq!(json["group"], "g");
        assert_eq!(json["candidates"][0]["status"], "undecodable");
        assert_eq!(json["candidates"][0]["message_offset"], 5);
    }
}
