//! The before -> after table of an offset reset, computed from what the cluster reports.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::error::{CoreError, CoreResult};
use crate::ops::groups::cluster_offsets_reader::ClusterOffsetsReader;
use crate::ops::groups::group_snapshot::PartitionWatermarks;
use crate::ops::offsets::offset_backup::OffsetBackup;
use crate::ops::offsets::offset_target::{OffsetTarget, ResolvedOffset, resolve_target_offset};

/// One partition of a reset plan. The message counts are `None` when the group has no commit there.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PlannedOffsetChange {
    pub topic: String,
    pub partition: i32,
    pub current_offset: Option<i64>,
    pub new_offset: i64,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
    pub clamped: bool,
    pub messages_skipped: Option<i64>,
    pub messages_reprocessed: Option<i64>,
}

/// Every partition a reset touches, ordered by topic and partition, with the totals.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct OffsetResetPlan {
    pub group: String,
    pub changes: Vec<PlannedOffsetChange>,
    pub total_skipped: i64,
    pub total_reprocessed: i64,
    pub warnings: Vec<String>,
}

impl OffsetResetPlan {
    /// The offsets to commit: only the partitions whose offset changes.
    pub fn offsets_to_commit(&self) -> BTreeMap<(String, i32), i64> {
        self.changes
            .iter()
            .filter(|change| change.current_offset != Some(change.new_offset))
            .map(|change| ((change.topic.clone(), change.partition), change.new_offset))
            .collect()
    }
}

/// What `offsets set` asks for. `partitions: None` means every partition of the topic.
pub struct OffsetResetRequest {
    pub group: String,
    pub topic: String,
    pub partitions: Option<Vec<i32>>,
    pub target: OffsetTarget,
}

/// Plans a reset of `request.group` on one topic without changing anything.
pub fn plan_offset_reset(
    reader: &dyn ClusterOffsetsReader,
    request: &OffsetResetRequest,
) -> CoreResult<OffsetResetPlan> {
    let topic = request.topic.as_str();
    let existing = read_topic_partitions(reader, topic)?;
    let partitions = match &request.partitions {
        None => existing,
        Some(requested) => {
            let mut selected = requested.clone();
            selected.sort_unstable();
            selected.dedup();
            for partition in &selected {
                if !existing.contains(partition) {
                    return Err(missing_partition_error(topic, *partition));
                }
            }
            selected
        }
    };
    let offsets_for_time = match request.target {
        OffsetTarget::Datetime(instant) => {
            reader.find_offsets_for_time(topic, &partitions, unix_millis(instant))?
        }
        _ => BTreeMap::new(),
    };
    let committed = reader.fetch_committed_offsets(
        &request.group,
        &BTreeMap::from([(topic.to_owned(), partitions.clone())]),
    )?;
    let mut builder = PlanBuilder::new(&request.group);
    for partition in partitions {
        let current = committed
            .get(&(topic.to_owned(), partition))
            .copied()
            .flatten();
        let watermarks = reader.fetch_watermarks(topic, partition)?;
        let offset_for_time = offsets_for_time.get(&partition).copied().flatten();
        let resolved = resolve_target_offset(request.target, current, watermarks, offset_for_time)
            .map_err(|reason| {
                CoreError::InvalidInput(format!("partition {topic}/{partition} {reason}"))
            })?;
        builder.push(topic, partition, current, watermarks, resolved);
    }
    Ok(builder.finish())
}

/// Plans moving the group of `backup` back to the offsets the backup holds.
///
/// Each backed-up offset goes through the same limit as `offsets set`. A partition that had no
/// commit in the backup stays as it is, with a warning: kafcli cannot delete a committed offset.
pub fn plan_offset_restore(
    reader: &dyn ClusterOffsetsReader,
    backup: &OffsetBackup,
) -> CoreResult<OffsetResetPlan> {
    let mut wanted: BTreeMap<&str, BTreeMap<i32, Option<i64>>> = BTreeMap::new();
    for partition in &backup.partitions {
        wanted
            .entry(partition.topic.as_str())
            .or_default()
            .insert(partition.partition, partition.committed_offset);
    }
    let mut builder = PlanBuilder::new(&backup.group);
    for (topic, partitions) in wanted {
        let existing = read_topic_partitions(reader, topic)?;
        if let Some(missing) = partitions
            .keys()
            .find(|partition| !existing.contains(partition))
        {
            return Err(missing_partition_error(topic, *missing));
        }
        let restored: Vec<i32> = partitions
            .iter()
            .filter(|(_, offset)| offset.is_some())
            .map(|(partition, _)| *partition)
            .collect();
        let committed = if restored.is_empty() {
            BTreeMap::new()
        } else {
            reader.fetch_committed_offsets(
                &backup.group,
                &BTreeMap::from([(topic.to_owned(), restored)]),
            )?
        };
        for (partition, backed_up) in partitions {
            let Some(backed_up) = backed_up else {
                builder.warn(format!(
                    "{topic}/{partition} had no committed offset in the backup; its current offset is left unchanged"
                ));
                continue;
            };
            let current = committed
                .get(&(topic.to_owned(), partition))
                .copied()
                .flatten();
            let watermarks = reader.fetch_watermarks(topic, partition)?;
            let resolved =
                resolve_target_offset(OffsetTarget::Offset(backed_up), current, watermarks, None)
                    .map_err(|reason| {
                    CoreError::InvalidInput(format!("partition {topic}/{partition} {reason}"))
                })?;
            builder.push(topic, partition, current, watermarks, resolved);
        }
    }
    Ok(builder.finish())
}

/// The sorted partitions of `topic`; `TopicNotFound` when the cluster does not have it.
pub(crate) fn read_topic_partitions(
    reader: &dyn ClusterOffsetsReader,
    topic: &str,
) -> CoreResult<Vec<i32>> {
    let mut partitions = reader
        .list_topic_partitions(Some(topic))?
        .remove(topic)
        .ok_or_else(|| CoreError::TopicNotFound(topic.to_owned()))?;
    partitions.sort_unstable();
    partitions.dedup();
    Ok(partitions)
}

pub(crate) fn missing_partition_error(topic: &str, partition: i32) -> CoreError {
    CoreError::InvalidInput(format!(
        "partition {partition} does not exist in topic '{topic}'"
    ))
}

fn unix_millis(instant: time::OffsetDateTime) -> i64 {
    i64::try_from(instant.unix_timestamp_nanos() / 1_000_000).unwrap_or(i64::MAX)
}

/// Collects planned changes with their warnings and totals.
pub(crate) struct PlanBuilder {
    group: String,
    changes: Vec<PlannedOffsetChange>,
    warnings: Vec<String>,
}

impl PlanBuilder {
    pub(crate) fn new(group: &str) -> Self {
        Self {
            group: group.to_owned(),
            changes: Vec::new(),
            warnings: Vec::new(),
        }
    }

    pub(crate) fn warn(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    pub(crate) fn push(
        &mut self,
        topic: &str,
        partition: i32,
        current: Option<i64>,
        watermarks: PartitionWatermarks,
        resolved: ResolvedOffset,
    ) {
        if resolved.clamped {
            self.warnings.push(format!(
                "target offset of {topic}/{partition} was limited to {} (log start {}, log end {})",
                resolved.offset, watermarks.low, watermarks.high
            ));
        }
        let new_offset = resolved.offset;
        self.changes.push(PlannedOffsetChange {
            topic: topic.to_owned(),
            partition,
            current_offset: current,
            new_offset,
            log_start_offset: watermarks.low,
            log_end_offset: watermarks.high,
            clamped: resolved.clamped,
            messages_skipped: current.map(|current| new_offset.saturating_sub(current).max(0)),
            messages_reprocessed: current.map(|current| current.saturating_sub(new_offset).max(0)),
        });
    }

    pub(crate) fn finish(mut self) -> OffsetResetPlan {
        self.changes.sort_by(|left, right| {
            (left.topic.as_str(), left.partition).cmp(&(right.topic.as_str(), right.partition))
        });
        let total_skipped = self
            .changes
            .iter()
            .filter_map(|change| change.messages_skipped)
            .fold(0_i64, i64::saturating_add);
        let total_reprocessed = self
            .changes
            .iter()
            .filter_map(|change| change.messages_reprocessed)
            .fold(0_i64, i64::saturating_add);
        OffsetResetPlan {
            group: self.group,
            changes: self.changes,
            total_skipped,
            total_reprocessed,
            warnings: self.warnings,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::broker_capabilities::BrokerFamily;
    use crate::ops::groups::fake_cluster_reader::FakeClusterReader;
    use crate::ops::offsets::offset_backup::BackupPartition;
    use time::OffsetDateTime;

    fn reader_with_two_partitions() -> FakeClusterReader {
        let mut reader = FakeClusterReader::new(BrokerFamily::GenericKafka);
        reader.topics.insert("t".to_owned(), vec![1, 0]);
        for partition in [0, 1] {
            reader.watermarks.insert(
                ("t".to_owned(), partition),
                PartitionWatermarks { low: 0, high: 20 },
            );
        }
        reader
    }

    fn commit(reader: &mut FakeClusterReader, partition: i32, offset: i64) {
        reader
            .committed
            .insert(("g".to_owned(), "t".to_owned(), partition), offset);
    }

    fn request(target: OffsetTarget, partitions: Option<Vec<i32>>) -> OffsetResetRequest {
        OffsetResetRequest {
            group: "g".to_owned(),
            topic: "t".to_owned(),
            partitions,
            target,
        }
    }

    #[test]
    fn plan_counts_skipped_and_reprocessed() {
        let mut reader = reader_with_two_partitions();
        commit(&mut reader, 0, 10);
        commit(&mut reader, 1, 15);

        let plan = plan_offset_reset(&reader, &request(OffsetTarget::Latest, None)).unwrap();

        let partitions: Vec<i32> = plan.changes.iter().map(|change| change.partition).collect();
        let skipped: Vec<Option<i64>> = plan
            .changes
            .iter()
            .map(|change| change.messages_skipped)
            .collect();
        let reprocessed: Vec<Option<i64>> = plan
            .changes
            .iter()
            .map(|change| change.messages_reprocessed)
            .collect();
        assert_eq!(partitions, vec![0, 1]);
        assert_eq!(skipped, vec![Some(10), Some(5)]);
        assert_eq!(reprocessed, vec![Some(0), Some(0)]);
        assert_eq!(plan.total_skipped, 15);
        assert_eq!(plan.total_reprocessed, 0);
        assert_eq!(plan.group, "g");
    }

    #[test]
    fn plan_counts_reprocessed_when_moving_back() {
        let mut reader = reader_with_two_partitions();
        commit(&mut reader, 0, 10);

        let plan =
            plan_offset_reset(&reader, &request(OffsetTarget::Earliest, Some(vec![0]))).unwrap();

        assert_eq!(plan.changes.len(), 1);
        assert_eq!(plan.changes[0].messages_reprocessed, Some(10));
        assert_eq!(plan.total_reprocessed, 10);
    }

    #[test]
    fn plan_without_commit_has_null_counts() {
        let reader = reader_with_two_partitions();

        let plan =
            plan_offset_reset(&reader, &request(OffsetTarget::Offset(3), Some(vec![1]))).unwrap();

        assert_eq!(plan.changes[0].current_offset, None);
        assert_eq!(plan.changes[0].new_offset, 3);
        assert_eq!(plan.changes[0].messages_skipped, None);
        assert_eq!(plan.changes[0].messages_reprocessed, None);
        assert_eq!(plan.total_skipped, 0);
        assert_eq!(
            plan.offsets_to_commit(),
            BTreeMap::from([(("t".to_owned(), 1), 3)])
        );
    }

    #[test]
    fn unknown_partition_is_rejected() {
        let reader = reader_with_two_partitions();

        let result = plan_offset_reset(&reader, &request(OffsetTarget::Latest, Some(vec![7])));

        assert!(
            matches!(&result, Err(CoreError::InvalidInput(text)) if text == "partition 7 does not exist in topic 't'"),
            "{result:?}"
        );
    }

    #[test]
    fn unchanged_partitions_are_not_committed() {
        let mut reader = reader_with_two_partitions();
        commit(&mut reader, 0, 10);

        let plan =
            plan_offset_reset(&reader, &request(OffsetTarget::Offset(10), Some(vec![0]))).unwrap();

        assert_eq!(plan.changes.len(), 1);
        assert!(plan.offsets_to_commit().is_empty());
    }

    #[test]
    fn clamped_target_adds_warning() {
        let mut reader = reader_with_two_partitions();
        commit(&mut reader, 0, 10);

        let plan =
            plan_offset_reset(&reader, &request(OffsetTarget::Offset(99), Some(vec![0]))).unwrap();

        assert!(plan.changes[0].clamped);
        assert_eq!(
            plan.warnings,
            vec!["target offset of t/0 was limited to 20 (log start 0, log end 20)".to_owned()]
        );
    }

    #[test]
    fn shift_by_without_commit_names_the_partition() {
        let mut reader = reader_with_two_partitions();
        commit(&mut reader, 0, 10);

        let result = plan_offset_reset(&reader, &request(OffsetTarget::ShiftBy(-2), None));

        assert!(
            matches!(&result, Err(CoreError::InvalidInput(text)) if text == "partition t/1 has no committed offset; --shift-by needs one"),
            "{result:?}"
        );
    }

    #[test]
    fn datetime_uses_offsets_for_time_once() {
        let mut reader = reader_with_two_partitions();
        commit(&mut reader, 0, 2);
        commit(&mut reader, 1, 2);
        let instant = OffsetDateTime::from_unix_timestamp(1_795_000_000).unwrap();
        let millis = i64::try_from(instant.unix_timestamp_nanos() / 1_000_000).unwrap();
        reader
            .times
            .insert(("t".to_owned(), 0), vec![(millis - 1, 4), (millis, 5)]);

        let plan =
            plan_offset_reset(&reader, &request(OffsetTarget::Datetime(instant), None)).unwrap();

        let new_offsets: Vec<i64> = plan
            .changes
            .iter()
            .map(|change| change.new_offset)
            .collect();
        assert_eq!(new_offsets, vec![5, 20]);
        assert_eq!(
            reader.offsets_for_time_requests(),
            vec![("t".to_owned(), vec![0, 1], millis)]
        );
    }

    #[test]
    fn missing_topic_is_topic_not_found() {
        let reader = reader_with_two_partitions();
        let mut missing = request(OffsetTarget::Latest, None);
        missing.topic = "nope".to_owned();

        let result = plan_offset_reset(&reader, &missing);

        assert!(
            matches!(result, Err(CoreError::TopicNotFound(_))),
            "{result:?}"
        );
    }

    fn restore_backup(partitions: Vec<(&str, i32, Option<i64>)>) -> OffsetBackup {
        OffsetBackup {
            kafcli_offsets_backup: 1,
            created_at: "2026-11-18T11:06:40.123Z".to_owned(),
            context: "dev".to_owned(),
            group: "g".to_owned(),
            command: "offsets set".to_owned(),
            partitions: partitions
                .into_iter()
                .map(|(topic, partition, committed_offset)| BackupPartition {
                    topic: topic.to_owned(),
                    partition,
                    committed_offset,
                    log_start_offset: 0,
                    log_end_offset: 20,
                    lag: committed_offset.map(|offset| 20 - offset),
                })
                .collect(),
        }
    }

    #[test]
    fn restore_targets_backup_offsets() {
        let mut reader = reader_with_two_partitions();
        commit(&mut reader, 0, 20);
        commit(&mut reader, 1, 20);

        let plan = plan_offset_restore(
            &reader,
            &restore_backup(vec![("t", 0, Some(4)), ("t", 1, Some(8))]),
        )
        .unwrap();

        assert_eq!(plan.group, "g");
        let moves: Vec<(i32, Option<i64>, i64)> = plan
            .changes
            .iter()
            .map(|change| (change.partition, change.current_offset, change.new_offset))
            .collect();
        assert_eq!(moves, vec![(0, Some(20), 4), (1, Some(20), 8)]);
        assert_eq!(plan.total_reprocessed, 28);
    }

    #[test]
    fn restore_clamps_offset_below_new_log_start() {
        let mut reader = reader_with_two_partitions();
        reader.watermarks.insert(
            ("t".to_owned(), 0),
            PartitionWatermarks { low: 5, high: 20 },
        );
        commit(&mut reader, 0, 20);

        let plan = plan_offset_restore(&reader, &restore_backup(vec![("t", 0, Some(3))])).unwrap();

        assert_eq!(plan.changes[0].new_offset, 5);
        assert!(plan.changes[0].clamped);
        assert_eq!(
            plan.warnings,
            vec!["target offset of t/0 was limited to 5 (log start 5, log end 20)".to_owned()]
        );
    }

    #[test]
    fn restore_skips_partitions_without_commit() {
        let mut reader = reader_with_two_partitions();
        commit(&mut reader, 0, 20);
        commit(&mut reader, 1, 20);

        let plan = plan_offset_restore(
            &reader,
            &restore_backup(vec![("t", 0, Some(4)), ("t", 1, None)]),
        )
        .unwrap();

        assert_eq!(plan.changes.len(), 1);
        assert_eq!(plan.changes[0].partition, 0);
        assert_eq!(
            plan.warnings,
            vec![
                "t/1 had no committed offset in the backup; its current offset is left unchanged"
                    .to_owned()
            ]
        );
    }

    #[test]
    fn restore_of_deleted_topic_fails() {
        let reader = reader_with_two_partitions();

        let result = plan_offset_restore(&reader, &restore_backup(vec![("gone", 0, Some(4))]));

        assert!(
            matches!(result, Err(CoreError::TopicNotFound(_))),
            "{result:?}"
        );
    }

    #[test]
    fn restore_of_deleted_partition_fails() {
        let reader = reader_with_two_partitions();

        let result = plan_offset_restore(&reader, &restore_backup(vec![("t", 9, Some(4))]));

        assert!(
            matches!(&result, Err(CoreError::InvalidInput(text)) if text == "partition 9 does not exist in topic 't'"),
            "{result:?}"
        );
    }
}
