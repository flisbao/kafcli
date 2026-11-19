use crate::output::renderer::TableRenderable;
use crate::output::terminal_text::sanitize_for_terminal;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Cell, Table};
use kafcli_core::ops::produce::message_producer::ProduceReport;
use serde::Serialize;

/// Deliveries listed in the table; JSON output always carries the kept list.
const MAXIMUM_LISTED_DELIVERIES: usize = 20;

/// The produce result. JSON carries every field of the report unchanged.
#[derive(Serialize)]
#[serde(transparent)]
pub struct ProduceReportView(pub ProduceReport);

impl TableRenderable for ProduceReportView {
    fn render_table(&self) -> String {
        let report = &self.0;
        let mut summary = Table::new();
        *summary.style_mut() = UTF8_FULL_CONDENSED;
        summary.set_header(vec![
            Cell::new("TOPIC"),
            Cell::new("SENT"),
            Cell::new("FAILED"),
        ]);
        summary.add_row(vec![
            Cell::new(sanitize_for_terminal(&report.topic)),
            Cell::new(report.sent),
            Cell::new(report.failed),
        ]);
        let mut rendered = summary.to_string();

        if let Some(error) = &report.first_error {
            rendered.push_str(&format!("\nfirst error: {}", sanitize_for_terminal(error)));
        }

        if !report.deliveries.is_empty() {
            let mut deliveries = Table::new();
            *deliveries.style_mut() = UTF8_FULL_CONDENSED;
            deliveries.set_header(vec![
                Cell::new("PARTITION"),
                Cell::new("OFFSET"),
                Cell::new("TIMESTAMP"),
            ]);
            for delivery in report.deliveries.iter().take(MAXIMUM_LISTED_DELIVERIES) {
                deliveries.add_row(vec![
                    Cell::new(delivery.partition),
                    Cell::new(delivery.offset),
                    Cell::new(
                        delivery
                            .timestamp
                            .as_deref()
                            .map_or("-".to_string(), sanitize_for_terminal),
                    ),
                ]);
            }
            rendered.push('\n');
            rendered.push_str(&deliveries.to_string());

            let hidden = report
                .deliveries
                .len()
                .saturating_sub(MAXIMUM_LISTED_DELIVERIES);
            if hidden > 0 {
                rendered.push_str(&format!("\n... and {hidden} more deliveries"));
            }
        }

        if report.deliveries_truncated {
            rendered.push_str(&format!(
                "\ndeliveries list truncated to the first {} kept in the report",
                report.deliveries.len()
            ));
        }
        rendered
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafcli_core::ops::produce::message_producer::DeliveryReport;

    fn report_with(first_error: Option<&str>, delivery_count: usize) -> ProduceReport {
        ProduceReport {
            topic: "orders\x1b[2J".to_string(),
            sent: delivery_count as u64,
            failed: 1,
            deliveries: (0..delivery_count)
                .map(|index| DeliveryReport {
                    partition: 0,
                    offset: i64::try_from(index).unwrap(),
                    timestamp: Some("2026-11-17T00:00:00.000Z".to_string()),
                })
                .collect(),
            deliveries_truncated: false,
            first_error: first_error.map(str::to_string),
        }
    }

    #[test]
    fn table_neutralizes_topic_and_broker_error_text() {
        let rendered = ProduceReportView(report_with(Some("\x1b[31mboom"), 0)).render_table();

        assert!(!rendered.contains('\x1b'), "rendered: {rendered}");
        assert!(
            rendered.contains("first error: ?[31mboom"),
            "rendered: {rendered}"
        );
        assert!(rendered.contains("orders?[2J"), "rendered: {rendered}");
    }

    #[test]
    fn table_lists_at_most_twenty_deliveries() {
        let rendered = ProduceReportView(report_with(None, 25)).render_table();

        assert!(
            rendered.contains("... and 5 more deliveries"),
            "rendered: {rendered}"
        );
        let offset_rows = rendered
            .lines()
            .filter(|line| line.contains("2026-11-17T00:00:00.000Z"))
            .count();
        assert_eq!(offset_rows, 20);
    }

    #[test]
    fn json_keeps_every_report_field() {
        let value = serde_json::to_value(ProduceReportView(report_with(None, 1))).unwrap();

        assert_eq!(value["sent"], 1);
        assert_eq!(value["failed"], 1);
        assert_eq!(value["deliveries"][0]["partition"], 0);
        assert!(value.get("first_error").is_some());
    }
}
