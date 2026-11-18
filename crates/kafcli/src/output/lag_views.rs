use crate::output::renderer::TableRenderable;
use crate::output::terminal_text::sanitize_for_terminal;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Cell, Table};
use kafcli_core::ops::groups::group_lag::{GroupLag, PartitionLag};
use serde::Serialize;

/// The `kafcli lag` result: groups in display order and the warnings of the collection.
#[derive(Debug, Serialize)]
pub struct LagView {
    pub groups: Vec<GroupLag>,
    pub warnings: Vec<String>,
}

const NULL_CELL: &str = "-";

impl TableRenderable for LagView {
    /// One table per group with the columns of `kafka-consumer-groups.sh --describe`, each followed
    /// by its `TOTAL` line.
    fn render_table(&self) -> String {
        if self.groups.is_empty() {
            return "no consumer groups found".to_owned();
        }
        let mut sections = Vec::with_capacity(self.groups.len());
        for group in &self.groups {
            let mut section = String::new();
            let partitions: Vec<&PartitionLag> = group
                .topics
                .iter()
                .flat_map(|topic| topic.partitions.iter())
                .collect();
            if !partitions.is_empty() {
                section.push_str(&render_group_table(&group.group, &partitions));
                section.push('\n');
            }
            section.push_str(&format!(
                "TOTAL {} {}",
                sanitize_for_terminal(&group.group),
                group.total_lag
            ));
            if !group.has_active_members {
                section.push_str(" (no active members)");
            }
            sections.push(section);
        }
        sections.join("\n\n")
    }
}

fn render_group_table(group: &str, partitions: &[&PartitionLag]) -> String {
    let mut table = Table::new();
    *table.style_mut() = UTF8_FULL_CONDENSED;
    table.set_header(vec![
        Cell::new("GROUP"),
        Cell::new("TOPIC"),
        Cell::new("PARTITION"),
        Cell::new("CURRENT-OFFSET"),
        Cell::new("LOG-END-OFFSET"),
        Cell::new("LAG"),
        Cell::new("CONSUMER-ID"),
        Cell::new("HOST"),
        Cell::new("CLIENT-ID"),
    ]);
    let group = sanitize_for_terminal(group);
    for partition in partitions {
        let member = partition.member.as_ref();
        table.add_row(vec![
            Cell::new(&group),
            Cell::new(sanitize_for_terminal(&partition.topic)),
            Cell::new(partition.partition),
            Cell::new(format_optional(partition.committed_offset)),
            Cell::new(partition.log_end_offset),
            Cell::new(format_optional(partition.lag)),
            Cell::new(member.map_or(NULL_CELL.to_owned(), |member| {
                sanitize_for_terminal(&member.member_id)
            })),
            Cell::new(member.map_or(NULL_CELL.to_owned(), |member| {
                sanitize_for_terminal(&member.host)
            })),
            Cell::new(member.map_or(NULL_CELL.to_owned(), |member| {
                sanitize_for_terminal(&member.client_id)
            })),
        ]);
    }
    table.to_string()
}

fn format_optional(value: Option<i64>) -> String {
    value.map_or(NULL_CELL.to_owned(), |value| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafcli_core::ops::groups::group_lag::{MemberRef, TopicLag};

    fn partition(
        partition: i32,
        committed: Option<i64>,
        member: Option<MemberRef>,
    ) -> PartitionLag {
        PartitionLag {
            topic: "pedidos".to_owned(),
            partition,
            committed_offset: committed,
            log_start_offset: 0,
            log_end_offset: 100,
            lag: committed.map(|offset| 100 - offset),
            member,
        }
    }

    fn view_with(has_active_members: bool, partitions: Vec<PartitionLag>) -> LagView {
        let total_lag = partitions
            .iter()
            .filter_map(|partition| partition.lag)
            .sum();
        LagView {
            groups: vec![GroupLag {
                group: "pedidos-service".to_owned(),
                state: "Stable".to_owned(),
                has_active_members,
                total_lag,
                topics: vec![TopicLag {
                    topic: "pedidos".to_owned(),
                    total_lag,
                    partitions,
                }],
            }],
            warnings: Vec::new(),
        }
    }

    #[test]
    fn lag_table_renders_dash_for_null() {
        let member = MemberRef {
            member_id: "consumer-1-abc".to_owned(),
            client_id: "consumer-1".to_owned(),
            host: "/10.0.0.7".to_owned(),
        };
        let view = view_with(
            true,
            vec![
                partition(0, Some(90), Some(member)),
                partition(1, None, None),
            ],
        );

        let table = view.render_table();

        for column in [
            "GROUP",
            "TOPIC",
            "PARTITION",
            "CURRENT-OFFSET",
            "LOG-END-OFFSET",
            "LAG",
            "CONSUMER-ID",
            "HOST",
            "CLIENT-ID",
        ] {
            assert!(table.contains(column), "missing {column}: {table}");
        }
        let second_row = table
            .lines()
            .find(|line| line.contains("pedidos") && line.contains(" 1 "))
            .unwrap_or_else(|| panic!("no row for partition 1: {table}"));
        assert!(second_row.matches(" - ").count() >= 5, "{second_row}");
        assert!(table.contains("TOTAL pedidos-service 10"), "{table}");
        assert!(!table.contains("(no active members)"), "{table}");
    }

    #[test]
    fn group_without_members_is_marked() {
        let view = view_with(false, vec![partition(0, Some(40), None)]);

        let table = view.render_table();

        assert!(
            table.contains("TOTAL pedidos-service 60 (no active members)"),
            "{table}"
        );
    }

    #[test]
    fn broker_text_is_sanitized() {
        let member = MemberRef {
            member_id: "m\x1b[31m".to_owned(),
            client_id: "c".to_owned(),
            host: "h".to_owned(),
        };
        let view = view_with(true, vec![partition(0, Some(90), Some(member))]);

        assert!(!view.render_table().contains('\x1b'));
    }
}
