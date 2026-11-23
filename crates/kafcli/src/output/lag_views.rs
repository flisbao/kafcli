use crate::cli::lag_arguments::LagDetail;
use crate::output::output_format::OutputFormat;
use crate::output::renderer::{TableRenderable, render_json_line};
use crate::output::terminal_text::sanitize_for_terminal;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Cell, Table};
use kafcli_core::ops::groups::group_lag::{GroupLag, PartitionLag, TopicLag, UnreadableGroup};
use kafcli_core::ops::groups::lag_trend::{GroupLagChange, LagTrend};
use serde::Serialize;

/// The `kafcli lag` result: groups in display order and the warnings of the collection.
/// `changes` is set only under `--watch`, empty on the first sample.
#[derive(Debug, Serialize)]
pub struct LagView {
    pub groups: Vec<GroupLag>,
    pub warnings: Vec<String>,
    /// Groups left out because their lag could not be read; their warnings say why.
    pub unreadable_groups: Vec<UnreadableGroup>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changes: Option<Vec<GroupLagChange>>,
    /// Level of the table; the JSON always carries every level.
    #[serde(skip)]
    pub detail: LagDetail,
}

/// One `--watch` frame. JSON is one NDJSON line; a table clears the screen first only on a terminal,
/// where it also carries the warnings, and ends with an empty line otherwise so piped frames stay
/// apart.
pub fn render_watch_frame(
    view: &LagView,
    format: OutputFormat,
    is_terminal: bool,
) -> anyhow::Result<String> {
    match format {
        OutputFormat::Json => render_json_line(view),
        OutputFormat::Table if is_terminal => {
            // Warnings written to stderr before the frame would be cleared with the screen.
            let mut frame = format!("{CLEAR_SCREEN}{}", view.render_table());
            for warning in &view.warnings {
                frame.push_str(&format!("\nwarning: {}", sanitize_for_terminal(warning)));
            }
            Ok(frame)
        }
        OutputFormat::Table => Ok(format!("{}\n", view.render_table())),
    }
}

/// Clears the terminal and moves the cursor home.
const CLEAR_SCREEN: &str = "\x1b[2J\x1b[H";

const NULL_CELL: &str = "-";

impl TableRenderable for LagView {
    /// The table of `detail`, followed by the `--watch` changes when there are any.
    fn render_table(&self) -> String {
        if self.groups.is_empty() {
            return "no consumer groups found".to_owned();
        }
        let mut rendered = match self.detail {
            LagDetail::Group => render_groups_table(&self.groups),
            LagDetail::Topic => render_topics_table(&self.groups),
            LagDetail::Partition => render_partition_sections(&self.groups),
        };
        if let Some(changes) = self.changes.as_ref().filter(|changes| !changes.is_empty()) {
            rendered.push_str("\n\n");
            rendered.push_str(&render_changes_table(changes));
        }
        rendered
    }
}

/// One line per group.
fn render_groups_table(groups: &[GroupLag]) -> String {
    let mut table = Table::new();
    *table.style_mut() = UTF8_FULL_CONDENSED;
    table.set_header(vec![
        Cell::new("GROUP"),
        Cell::new("STATE"),
        Cell::new("TOPICS"),
        Cell::new("PARTITIONS"),
        Cell::new("LAG"),
        Cell::new("MEMBERS"),
    ]);
    for group in groups {
        let partitions: usize = group
            .topics
            .iter()
            .map(|topic| topic.partitions.len())
            .sum();
        table.add_row(vec![
            Cell::new(sanitize_for_terminal(&group.group)),
            Cell::new(sanitize_for_terminal(&group.state)),
            Cell::new(group.topics.len()),
            Cell::new(partitions),
            Cell::new(group.total_lag),
            Cell::new(group.member_count),
        ]);
    }
    table.to_string()
}

/// One line per group and topic, topics by lag (highest first) then name. A group with no topic
/// keeps one line, so `--top` still shows every group it kept.
fn render_topics_table(groups: &[GroupLag]) -> String {
    let mut table = Table::new();
    *table.style_mut() = UTF8_FULL_CONDENSED;
    table.set_header(vec![
        Cell::new("GROUP"),
        Cell::new("TOPIC"),
        Cell::new("PARTITIONS"),
        Cell::new("LAG"),
        Cell::new("MEMBERS"),
    ]);
    for group in groups {
        let name = sanitize_for_terminal(&group.group);
        if group.topics.is_empty() {
            table.add_row(vec![
                Cell::new(&name),
                Cell::new(NULL_CELL),
                Cell::new(0),
                Cell::new(0),
                Cell::new(group.member_count),
            ]);
            continue;
        }
        let mut topics: Vec<&TopicLag> = group.topics.iter().collect();
        topics.sort_by(|left, right| {
            right
                .total_lag
                .cmp(&left.total_lag)
                .then_with(|| left.topic.cmp(&right.topic))
        });
        for topic in topics {
            table.add_row(vec![
                Cell::new(&name),
                Cell::new(sanitize_for_terminal(&topic.topic)),
                Cell::new(topic.partitions.len()),
                Cell::new(topic.total_lag),
                Cell::new(group.member_count),
            ]);
        }
    }
    table.to_string()
}

/// One table per group with the columns of `kafka-consumer-groups.sh --describe` (`MEMBER` in place
/// of `CONSUMER-ID`), each followed by its `TOTAL` line.
fn render_partition_sections(groups: &[GroupLag]) -> String {
    let mut sections = Vec::with_capacity(groups.len());
    for group in groups {
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

fn render_changes_table(changes: &[GroupLagChange]) -> String {
    let mut table = Table::new();
    *table.style_mut() = UTF8_FULL_CONDENSED;
    table.set_header(vec![
        Cell::new("GROUP"),
        Cell::new("CONSUMED/S"),
        Cell::new("LAG-DELTA"),
        Cell::new("TREND"),
    ]);
    for change in changes {
        table.add_row(vec![
            Cell::new(sanitize_for_terminal(&change.group)),
            Cell::new(
                change
                    .consumed_per_second
                    .map_or(NULL_CELL.to_owned(), |rate| format!("{rate:.1}")),
            ),
            Cell::new(format!("{:+}", change.lag_delta)),
            Cell::new(describe_trend(change.trend)),
        ]);
    }
    table.to_string()
}

fn describe_trend(trend: LagTrend) -> &'static str {
    match trend {
        LagTrend::Rising => "rising",
        LagTrend::Falling => "falling",
        LagTrend::Steady => "steady",
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
        Cell::new("MEMBER"),
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
                short_member_id(&member.member_id, &member.client_id)
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

/// Characters of the member id kept in the `MEMBER` column.
const SHORT_MEMBER_ID_LENGTH: usize = 8;

/// The part of a member id that tells instances apart, cut to `SHORT_MEMBER_ID_LENGTH` characters.
///
/// Kafka builds member ids as `<client-id>-<uuid>`, and Azure Event Hubs adds a
/// `<namespace>:c:<group>:I:` prefix, so the text after the last `<client-id>-` is the unique part.
/// Instances often share a client id, so the client id alone does not identify them.
fn short_member_id(member_id: &str, client_id: &str) -> String {
    let unique = if client_id.is_empty() {
        member_id
    } else {
        member_id
            .rsplit_once(&format!("{client_id}-"))
            .map_or(member_id, |(_, suffix)| suffix)
    };
    let short: String = unique.chars().take(SHORT_MEMBER_ID_LENGTH).collect();
    sanitize_for_terminal(&short)
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
                member_count: usize::from(has_active_members),
                total_lag,
                topics: vec![TopicLag {
                    topic: "pedidos".to_owned(),
                    total_lag,
                    partitions,
                }],
            }],
            warnings: Vec::new(),
            unreadable_groups: Vec::new(),
            changes: None,
            detail: LagDetail::Partition,
        }
    }

    fn watched_view() -> LagView {
        let mut view = view_with(true, vec![partition(0, Some(90), None)]);
        view.changes = Some(vec![GroupLagChange {
            group: "pedidos-service".to_owned(),
            consumed_per_second: Some(12.5),
            lag_delta: -3,
            trend: kafcli_core::ops::groups::lag_trend::LagTrend::Falling,
        }]);
        view
    }

    #[test]
    fn watch_without_tty_has_no_ansi_clear() {
        let frame = render_watch_frame(&watched_view(), OutputFormat::Table, false).unwrap();

        assert!(!frame.contains("\x1b["), "{frame:?}");
        assert!(frame.contains("TOTAL pedidos-service 10"), "{frame}");
        assert!(frame.contains("falling"), "{frame}");
        assert!(frame.ends_with('\n'), "{frame:?}");
    }

    #[test]
    fn watch_on_tty_clears_screen_first() {
        let frame = render_watch_frame(&watched_view(), OutputFormat::Table, true).unwrap();

        assert!(frame.starts_with("\x1b[2J\x1b[H"), "{frame:?}");
    }

    #[test]
    fn watch_on_tty_keeps_warnings_inside_the_frame() {
        let mut view = watched_view();
        view.warnings = vec!["the group list may be incomplete\x1b[2J".to_owned()];

        let frame = render_watch_frame(&view, OutputFormat::Table, true).unwrap();

        let after_clear = frame.trim_start_matches("\x1b[2J\x1b[H");
        assert!(
            after_clear.contains("warning: the group list may be incomplete"),
            "{frame:?}"
        );
        assert!(!after_clear.contains('\x1b'), "{frame:?}");
    }

    #[test]
    fn watch_json_frame_is_one_line() {
        let frame = render_watch_frame(&watched_view(), OutputFormat::Json, true).unwrap();

        assert!(!frame.contains('\n'), "{frame}");
        let value: serde_json::Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(value["kafcli_output"], 1);
        assert_eq!(value["data"]["changes"][0]["trend"], "falling");
        assert_eq!(value["data"]["changes"][0]["consumed_per_second"], 12.5);
    }

    #[test]
    fn view_without_watch_has_no_changes_field() {
        let view = view_with(true, vec![partition(0, Some(90), None)]);

        let json = serde_json::to_value(&view).unwrap();

        assert!(json.get("changes").is_none(), "{json}");
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
            "MEMBER",
            "HOST",
            "CLIENT-ID",
        ] {
            assert!(table.contains(column), "missing {column}: {table}");
        }
        assert!(!table.contains("CONSUMER-ID"), "{table}");
        assert!(!table.contains("consumer-1-abc"), "{table}");
        let first_row = table
            .lines()
            .find(|line| line.contains("pedidos") && line.contains(" 0 "))
            .unwrap_or_else(|| panic!("no row for partition 0: {table}"));
        assert!(first_row.contains(" abc "), "{first_row}");
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

    #[test]
    fn short_member_id_keeps_uuid_prefix_of_event_hubs_id() {
        let id = "evh-x.servicebus.windows.net:c:hpg-group-billing:I:src-bridge-consumer-0-8807c20668ac4abb9afa5eb4f21c714e";
        assert_eq!(short_member_id(id, "src-bridge-consumer-0"), "8807c206");
    }

    #[test]
    fn short_member_id_handles_apache_kafka_format() {
        assert_eq!(
            short_member_id("consumer-1-0deea70f-ae67-4847", "consumer-1"),
            "0deea70f"
        );
    }

    #[test]
    fn short_member_id_without_client_id_uses_whole_id() {
        assert_eq!(short_member_id("abcdefghijkl", ""), "abcdefgh");
        assert_eq!(short_member_id("m-1", "other"), "m-1");
    }

    #[test]
    fn short_member_id_truncates_by_char() {
        assert_eq!(short_member_id("ççççççççççç", ""), "çççççççç");
    }

    fn topic_lag(topic: &str, lags: &[i64]) -> TopicLag {
        TopicLag {
            topic: topic.to_owned(),
            total_lag: lags.iter().sum(),
            partitions: (0..)
                .zip(lags)
                .map(|(partition, lag)| PartitionLag {
                    topic: topic.to_owned(),
                    partition,
                    committed_offset: Some(100 - lag),
                    log_start_offset: 0,
                    log_end_offset: 100,
                    lag: Some(*lag),
                    member: None,
                })
                .collect(),
        }
    }

    fn group_lag(name: &str, state: &str, member_count: usize, topics: Vec<TopicLag>) -> GroupLag {
        GroupLag {
            group: name.to_owned(),
            state: state.to_owned(),
            has_active_members: member_count > 0,
            member_count,
            total_lag: topics.iter().map(|topic| topic.total_lag).sum(),
            topics,
        }
    }

    fn two_group_view(detail: LagDetail) -> LagView {
        LagView {
            groups: vec![
                group_lag(
                    "a",
                    "Stable",
                    3,
                    vec![topic_lag("y", &[0]), topic_lag("x", &[2, 3])],
                ),
                group_lag("b", "Empty", 0, Vec::new()),
            ],
            warnings: Vec::new(),
            unreadable_groups: Vec::new(),
            changes: None,
            detail,
        }
    }

    /// The trimmed cells of the first table row whose first cell is `first`.
    fn row_cells(table: &str, first: &str) -> Vec<String> {
        table
            .lines()
            .map(|line| {
                line.trim_matches(|c: char| c == '│' || c.is_whitespace())
                    .split('┆')
                    .map(|cell| cell.trim().to_owned())
                    .collect::<Vec<_>>()
            })
            .find(|cells| cells.first().is_some_and(|cell| cell == first))
            .unwrap_or_else(|| panic!("no row starting with {first}: {table}"))
    }

    fn rows_starting_with(table: &str, first: &str) -> Vec<Vec<String>> {
        table
            .lines()
            .map(|line| {
                line.trim_matches(|c: char| c == '│' || c.is_whitespace())
                    .split('┆')
                    .map(|cell| cell.trim().to_owned())
                    .collect::<Vec<_>>()
            })
            .filter(|cells| cells.first().is_some_and(|cell| cell == first))
            .collect()
    }

    #[test]
    fn group_detail_has_one_row_per_group() {
        let table = two_group_view(LagDetail::Group).render_table();

        assert_eq!(
            row_cells(&table, "GROUP"),
            ["GROUP", "STATE", "TOPICS", "PARTITIONS", "LAG", "MEMBERS"]
        );
        assert_eq!(row_cells(&table, "a"), ["a", "Stable", "2", "3", "5", "3"]);
        assert_eq!(row_cells(&table, "b"), ["b", "Empty", "0", "0", "0", "0"]);
        assert!(!table.contains("TOTAL"), "{table}");
        assert!(!table.contains("PARTITION "), "{table}");
    }

    #[test]
    fn topic_detail_orders_topics_by_lag() {
        let table = two_group_view(LagDetail::Topic).render_table();

        assert_eq!(
            row_cells(&table, "GROUP"),
            ["GROUP", "TOPIC", "PARTITIONS", "LAG", "MEMBERS"]
        );
        assert_eq!(
            rows_starting_with(&table, "a"),
            [["a", "x", "2", "5", "3"], ["a", "y", "1", "0", "3"]]
        );
        assert_eq!(rows_starting_with(&table, "b"), [["b", "-", "0", "0", "0"]]);
    }

    #[test]
    fn topic_detail_ties_order_by_name() {
        let mut view = two_group_view(LagDetail::Topic);
        view.groups[0].topics = vec![topic_lag("q", &[4]), topic_lag("p", &[4])];

        let table = view.render_table();

        let topics: Vec<String> = rows_starting_with(&table, "a")
            .into_iter()
            .map(|cells| cells[1].clone())
            .collect();
        assert_eq!(topics, ["p", "q"]);
    }

    #[test]
    fn partition_detail_keeps_total_lines() {
        let table = two_group_view(LagDetail::Partition).render_table();

        assert!(table.contains("TOTAL a 5"), "{table}");
        assert!(table.contains("TOTAL b 0 (no active members)"), "{table}");
    }

    #[test]
    fn watch_frame_appends_changes_at_group_detail() {
        let mut view = two_group_view(LagDetail::Group);
        view.changes = Some(vec![GroupLagChange {
            group: "a".to_owned(),
            consumed_per_second: Some(1.0),
            lag_delta: 2,
            trend: LagTrend::Rising,
        }]);

        let frame = render_watch_frame(&view, OutputFormat::Table, false).unwrap();

        assert!(frame.contains("MEMBERS"), "{frame}");
        assert!(frame.contains("CONSUMED/S"), "{frame}");
    }

    #[test]
    fn json_ignores_detail() {
        let json = serde_json::to_value(two_group_view(LagDetail::Topic)).unwrap();

        assert!(json.get("detail").is_none(), "{json}");
    }
}
