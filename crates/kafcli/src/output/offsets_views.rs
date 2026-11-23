use crate::output::renderer::TableRenderable;
use crate::output::terminal_text::sanitize_for_terminal;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Cell, Table};
use kafcli_core::ops::groups::group_lag::GroupLag;
use serde::Serialize;

/// Offsets of one partition in `kafcli offsets show`. This shape is the contract the offset backup
/// of `offsets set` reads (ADR 0006), so fields are only ever added.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct PartitionOffsetsView {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: Option<i64>,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
    pub lag: Option<i64>,
}

/// The `kafcli offsets show` result for one group.
#[derive(Debug, Serialize)]
pub struct OffsetsView {
    pub group: String,
    pub state: String,
    pub partitions: Vec<PartitionOffsetsView>,
    pub warnings: Vec<String>,
}

impl OffsetsView {
    pub fn from_group_lag(group: GroupLag, warnings: Vec<String>) -> Self {
        let partitions = group
            .topics
            .into_iter()
            .flat_map(|topic| topic.partitions)
            .map(|partition| PartitionOffsetsView {
                topic: partition.topic,
                partition: partition.partition,
                committed_offset: partition.committed_offset,
                log_start_offset: partition.log_start_offset,
                log_end_offset: partition.log_end_offset,
                lag: partition.lag,
            })
            .collect();
        Self {
            group: group.group,
            state: group.state,
            partitions,
            warnings,
        }
    }
}

impl TableRenderable for OffsetsView {
    fn render_table(&self) -> String {
        let heading = format!(
            "GROUP {} ({})",
            sanitize_for_terminal(&self.group),
            sanitize_for_terminal(&self.state)
        );
        if self.partitions.is_empty() {
            return format!("{heading}\nno committed offsets");
        }
        let mut table = Table::new();
        *table.style_mut() = UTF8_FULL_CONDENSED;
        table.set_header(vec![
            Cell::new("TOPIC"),
            Cell::new("PARTITION"),
            Cell::new("COMMITTED"),
            Cell::new("LOG-START"),
            Cell::new("LOG-END"),
            Cell::new("LAG"),
        ]);
        for partition in &self.partitions {
            table.add_row(vec![
                Cell::new(sanitize_for_terminal(&partition.topic)),
                Cell::new(partition.partition),
                Cell::new(format_optional(partition.committed_offset)),
                Cell::new(partition.log_start_offset),
                Cell::new(partition.log_end_offset),
                Cell::new(format_optional(partition.lag)),
            ]);
        }
        format!("{heading}\n{table}")
    }
}

fn format_optional(value: Option<i64>) -> String {
    value.map_or("-".to_owned(), |value| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafcli_core::ops::groups::group_lag::{PartitionLag, TopicLag};

    fn group_lag() -> GroupLag {
        GroupLag {
            group: "pedidos-service".to_owned(),
            state: "Empty".to_owned(),
            has_active_members: false,
            member_count: 0,
            total_lag: 7,
            topics: vec![TopicLag {
                topic: "pedidos".to_owned(),
                total_lag: 7,
                partitions: vec![
                    PartitionLag {
                        topic: "pedidos".to_owned(),
                        partition: 0,
                        committed_offset: Some(13),
                        log_start_offset: 5,
                        log_end_offset: 20,
                        lag: Some(7),
                        member: None,
                    },
                    PartitionLag {
                        topic: "pedidos".to_owned(),
                        partition: 1,
                        committed_offset: None,
                        log_start_offset: 0,
                        log_end_offset: 9,
                        lag: None,
                        member: None,
                    },
                ],
            }],
        }
    }

    #[test]
    fn offsets_view_includes_log_start() {
        let view = OffsetsView::from_group_lag(group_lag(), vec!["w".to_owned()]);

        let json = serde_json::to_value(&view).unwrap();

        assert_eq!(json["group"], "pedidos-service");
        assert_eq!(json["state"], "Empty");
        assert_eq!(
            json["partitions"][0],
            serde_json::json!({
                "topic": "pedidos",
                "partition": 0,
                "committed_offset": 13,
                "log_start_offset": 5,
                "log_end_offset": 20,
                "lag": 7
            })
        );
        assert_eq!(json["warnings"][0], "w");
        let table = view.render_table();
        for column in [
            "TOPIC",
            "PARTITION",
            "COMMITTED",
            "LOG-START",
            "LOG-END",
            "LAG",
        ] {
            assert!(table.contains(column), "missing {column}: {table}");
        }
    }

    #[test]
    fn null_committed_renders_dash() {
        let view = OffsetsView::from_group_lag(group_lag(), Vec::new());

        let json = serde_json::to_value(&view).unwrap();
        let table = view.render_table();

        assert!(json["partitions"][1]["committed_offset"].is_null());
        assert!(json["partitions"][1]["lag"].is_null());
        let row = table
            .lines()
            .find(|line| line.contains(" 9 "))
            .unwrap_or_else(|| panic!("no row for partition 1: {table}"));
        assert_eq!(row.matches(" - ").count(), 2, "{row}");
    }
}
