//! Change of consumer lag between two samples, for `lag --watch`.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Serialize;

use crate::ops::groups::group_lag::{GroupLag, GroupLagReport};

/// Direction of the total lag of a group between two samples.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LagTrend {
    Rising,
    Falling,
    Steady,
}

/// How a group moved between two samples. `consumed_per_second` is `None` when no partition had a
/// commit in both samples or no time passed.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct GroupLagChange {
    pub group: String,
    pub consumed_per_second: Option<f64>,
    pub lag_delta: i64,
    pub trend: LagTrend,
}

/// Compares the groups present in both samples, in the order of `current`.
pub fn compare_lag_samples(
    previous: &GroupLagReport,
    current: &GroupLagReport,
    elapsed: Duration,
) -> Vec<GroupLagChange> {
    let previous_groups: BTreeMap<&str, &GroupLag> = previous
        .groups
        .iter()
        .map(|group| (group.group.as_str(), group))
        .collect();
    current
        .groups
        .iter()
        .filter_map(|group| {
            let before = previous_groups.get(group.group.as_str())?;
            let lag_delta = group.total_lag - before.total_lag;
            Some(GroupLagChange {
                group: group.group.clone(),
                consumed_per_second: measure_consumption_rate(before, group, elapsed),
                lag_delta,
                trend: match lag_delta {
                    delta if delta > 0 => LagTrend::Rising,
                    delta if delta < 0 => LagTrend::Falling,
                    _ => LagTrend::Steady,
                },
            })
        })
        .collect()
}

/// Committed offsets gained per second, over the partitions with a commit in both samples.
fn measure_consumption_rate(
    previous: &GroupLag,
    current: &GroupLag,
    elapsed: Duration,
) -> Option<f64> {
    let seconds = elapsed.as_secs_f64();
    if seconds <= 0.0 {
        return None;
    }
    let previous_commits = map_committed_offsets(previous);
    let mut consumed: i64 = 0;
    let mut has_common_partition = false;
    for (key, offset) in map_committed_offsets(current) {
        if let Some(before) = previous_commits.get(&key) {
            consumed += offset - before;
            has_common_partition = true;
        }
    }
    has_common_partition.then(|| consumed as f64 / seconds)
}

fn map_committed_offsets(group: &GroupLag) -> BTreeMap<(&str, i32), i64> {
    group
        .topics
        .iter()
        .flat_map(|topic| topic.partitions.iter())
        .filter_map(|partition| {
            partition
                .committed_offset
                .map(|offset| ((partition.topic.as_str(), partition.partition), offset))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::groups::group_lag::{PartitionLag, TopicLag};

    fn partition(partition: i32, committed: Option<i64>, lag: Option<i64>) -> PartitionLag {
        PartitionLag {
            topic: "pedidos".to_owned(),
            partition,
            committed_offset: committed,
            log_start_offset: 0,
            log_end_offset: 1000,
            lag,
            member: None,
        }
    }

    fn sample(total_lag: i64, partitions: Vec<PartitionLag>) -> GroupLagReport {
        GroupLagReport {
            groups: vec![GroupLag {
                group: "g".to_owned(),
                state: "Stable".to_owned(),
                has_active_members: true,
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
    fn computes_rate_and_rising_trend() {
        let previous = sample(5, vec![partition(0, Some(100), Some(5))]);
        let current = sample(8, vec![partition(0, Some(200), Some(8))]);

        let changes = compare_lag_samples(&previous, &current, Duration::from_secs(10));

        assert_eq!(
            changes,
            vec![GroupLagChange {
                group: "g".to_owned(),
                consumed_per_second: Some(10.0),
                lag_delta: 3,
                trend: LagTrend::Rising,
            }]
        );
    }

    #[test]
    fn partition_missing_in_one_sample_is_ignored() {
        let previous = sample(
            10,
            vec![partition(0, Some(100), Some(10)), partition(1, None, None)],
        );
        let current = sample(
            4,
            vec![
                partition(0, Some(150), Some(4)),
                partition(1, Some(900), Some(0)),
                partition(2, Some(50), Some(0)),
            ],
        );

        let changes = compare_lag_samples(&previous, &current, Duration::from_secs(5));

        assert_eq!(changes[0].consumed_per_second, Some(10.0));
        assert_eq!(changes[0].trend, LagTrend::Falling);
        assert_eq!(changes[0].lag_delta, -6);
    }

    #[test]
    fn steady_when_lag_unchanged() {
        let previous = sample(7, vec![partition(0, Some(100), Some(7))]);
        let current = sample(7, vec![partition(0, Some(100), Some(7))]);

        let changes = compare_lag_samples(&previous, &current, Duration::from_secs(1));

        assert_eq!(changes[0].trend, LagTrend::Steady);
        assert_eq!(changes[0].consumed_per_second, Some(0.0));
    }

    #[test]
    fn no_rate_without_common_commits() {
        let previous = sample(7, vec![partition(0, None, None)]);
        let current = sample(7, vec![partition(0, Some(100), Some(7))]);

        let changes = compare_lag_samples(&previous, &current, Duration::from_secs(1));

        assert_eq!(changes[0].consumed_per_second, None);
    }
}
