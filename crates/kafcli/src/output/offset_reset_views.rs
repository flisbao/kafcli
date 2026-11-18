use crate::output::renderer::TableRenderable;
use crate::output::terminal_text::sanitize_for_terminal;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Cell, Table};
use kafcli_core::ops::offsets::group_activity::GroupActivity;
use kafcli_core::ops::offsets::offset_reset_execution::OffsetResetOutcome;
use kafcli_core::ops::offsets::offset_reset_plan::PlannedOffsetChange;
use serde::Serialize;
use std::path::PathBuf;

const DRY_RUN_LINE: &str = "DRY RUN: nothing was committed; re-run with --execute to apply";
const NULL_CELL: &str = "-";

/// The result of `offsets set` and `offsets restore`: the before -> after table and what happened.
#[derive(Debug, Serialize)]
pub struct OffsetResetView {
    pub group: String,
    pub dry_run: bool,
    pub committed: bool,
    pub group_activity: GroupActivity,
    pub changes: Vec<PlannedOffsetChange>,
    pub total_skipped: i64,
    pub total_reprocessed: i64,
    pub backup_file: Option<PathBuf>,
    pub warnings: Vec<String>,
}

impl OffsetResetView {
    pub fn from_outcome(outcome: OffsetResetOutcome) -> Self {
        Self {
            group: outcome.plan.group,
            dry_run: outcome.dry_run,
            committed: outcome.committed,
            group_activity: outcome.group_activity,
            changes: outcome.plan.changes,
            total_skipped: outcome.plan.total_skipped,
            total_reprocessed: outcome.plan.total_reprocessed,
            backup_file: outcome.backup_file,
            warnings: outcome.warnings,
        }
    }
}

impl TableRenderable for OffsetResetView {
    fn render_table(&self) -> String {
        let mut table = Table::new();
        *table.style_mut() = UTF8_FULL_CONDENSED;
        table.set_header(vec![
            Cell::new("TOPIC"),
            Cell::new("PARTITION"),
            Cell::new("CURRENT"),
            Cell::new("NEW"),
            Cell::new("LOG-START"),
            Cell::new("LOG-END"),
            Cell::new("SKIPPED"),
            Cell::new("REPROCESSED"),
        ]);
        for change in &self.changes {
            let new_offset = if change.clamped {
                format!("{}*", change.new_offset)
            } else {
                change.new_offset.to_string()
            };
            table.add_row(vec![
                Cell::new(sanitize_for_terminal(&change.topic)),
                Cell::new(change.partition),
                Cell::new(format_optional(change.current_offset)),
                Cell::new(new_offset),
                Cell::new(change.log_start_offset),
                Cell::new(change.log_end_offset),
                Cell::new(format_optional(change.messages_skipped)),
                Cell::new(format_optional(change.messages_reprocessed)),
            ]);
        }
        let mut lines = vec![
            format!("GROUP {}", sanitize_for_terminal(&self.group)),
            table.to_string(),
            format!(
                "group state: {}",
                describe_group_activity(&self.group_activity)
            ),
            format!(
                "total: {} skipped, {} reprocessed",
                self.total_skipped, self.total_reprocessed
            ),
        ];
        lines.push(final_line(
            self.dry_run,
            self.committed,
            self.backup_file.as_ref(),
        ));
        lines.join("\n")
    }
}

/// The last line of a destructive result: dry-run hint, commit with its backup, or nothing done.
pub fn final_line(dry_run: bool, committed: bool, backup_file: Option<&PathBuf>) -> String {
    match (dry_run, committed, backup_file) {
        (true, _, _) => DRY_RUN_LINE.to_owned(),
        (false, true, Some(path)) => format!(
            "committed; backup: {}",
            sanitize_for_terminal(&path.display().to_string())
        ),
        (false, true, None) => "committed".to_owned(),
        (false, false, _) => "nothing was committed".to_owned(),
    }
}

pub fn describe_group_activity(activity: &GroupActivity) -> String {
    match activity {
        GroupActivity::Empty => "empty".to_owned(),
        GroupActivity::Active { members } => format!("active ({members} members)"),
        GroupActivity::NotDescribed => "not described".to_owned(),
        GroupActivity::DescribeFailed { .. } => "describe failed".to_owned(),
    }
}

fn format_optional(value: Option<i64>) -> String {
    value.map_or(NULL_CELL.to_owned(), |value| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafcli_core::ops::offsets::group_activity::GroupActivity;
    use kafcli_core::ops::offsets::offset_reset_plan::{OffsetResetPlan, PlannedOffsetChange};
    use std::path::PathBuf;

    fn change(
        partition: i32,
        current: Option<i64>,
        new: i64,
        clamped: bool,
    ) -> PlannedOffsetChange {
        PlannedOffsetChange {
            topic: "pedidos".to_owned(),
            partition,
            current_offset: current,
            new_offset: new,
            log_start_offset: 0,
            log_end_offset: 20,
            clamped,
            messages_skipped: current.map(|current| (new - current).max(0)),
            messages_reprocessed: current.map(|current| (current - new).max(0)),
        }
    }

    fn outcome(dry_run: bool, changes: Vec<PlannedOffsetChange>) -> OffsetResetOutcome {
        OffsetResetOutcome {
            plan: OffsetResetPlan {
                group: "pedidos-service".to_owned(),
                total_skipped: 7,
                total_reprocessed: 0,
                changes,
                warnings: Vec::new(),
            },
            dry_run,
            group_activity: GroupActivity::Empty,
            backup_file: (!dry_run).then(|| PathBuf::from("/state/offsets-backup/g.json")),
            committed: !dry_run,
            warnings: vec!["w".to_owned()],
        }
    }

    #[test]
    fn dry_run_table_ends_with_hint() {
        let view =
            OffsetResetView::from_outcome(outcome(true, vec![change(0, Some(13), 20, false)]));

        let table = view.render_table();

        assert!(table.contains("TOPIC"), "{table}");
        assert!(table.contains("REPROCESSED"), "{table}");
        assert!(table.contains("group state: empty"), "{table}");
        assert!(table.contains("total: 7 skipped, 0 reprocessed"), "{table}");
        assert!(
            table.ends_with("DRY RUN: nothing was committed; re-run with --execute to apply"),
            "{table}"
        );
    }

    #[test]
    fn committed_table_ends_with_backup() {
        let view =
            OffsetResetView::from_outcome(outcome(false, vec![change(0, Some(13), 20, false)]));

        let table = view.render_table();

        assert!(
            table.ends_with("committed; backup: /state/offsets-backup/g.json"),
            "{table}"
        );
    }

    #[test]
    fn clamped_offset_is_marked() {
        let view =
            OffsetResetView::from_outcome(outcome(true, vec![change(0, Some(13), 20, true)]));

        assert!(
            view.render_table().contains("20*"),
            "{}",
            view.render_table()
        );
    }

    #[test]
    fn null_current_renders_dash() {
        let view = OffsetResetView::from_outcome(outcome(true, vec![change(1, None, 20, false)]));

        let table = view.render_table();

        let row = table
            .lines()
            .find(|line| line.contains("pedidos") && !line.starts_with("GROUP"))
            .unwrap();
        assert!(row.contains(" - "), "{row}");
    }

    #[test]
    fn active_group_state_counts_members() {
        let mut outcome = outcome(true, vec![change(0, Some(13), 20, false)]);
        outcome.group_activity = GroupActivity::Active { members: 2 };

        let table = OffsetResetView::from_outcome(outcome).render_table();

        assert!(table.contains("group state: active (2 members)"), "{table}");
    }

    #[test]
    fn json_has_the_contract_fields() {
        let view =
            OffsetResetView::from_outcome(outcome(false, vec![change(0, Some(13), 20, false)]));

        let json = serde_json::to_value(&view).unwrap();

        assert_eq!(json["group"], "pedidos-service");
        assert_eq!(json["dry_run"], false);
        assert_eq!(json["committed"], true);
        assert_eq!(json["group_activity"]["status"], "empty");
        assert_eq!(json["changes"][0]["new_offset"], 20);
        assert_eq!(json["total_skipped"], 7);
        assert_eq!(json["total_reprocessed"], 0);
        assert_eq!(json["backup_file"], "/state/offsets-backup/g.json");
        assert_eq!(json["warnings"], serde_json::json!(["w"]));
    }
}
