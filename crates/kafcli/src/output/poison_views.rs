use crate::output::renderer::TableRenderable;
use crate::output::terminal_text::sanitize_for_terminal;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Cell, Table};
use kafcli_core::ops::poison::poison_finder::{PoisonReport, PoisonStatus};
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
    use kafcli_core::ops::poison::poison_finder::{PoisonCandidate, PoisonReport, PoisonStatus};

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
