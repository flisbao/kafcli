//! Consumer lag per partition, topic and group, computed from what the cluster reports.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::thread;

use serde::Serialize;

use crate::capability::broker_operation::incomplete_group_list_warning;
use crate::error::{CoreError, CoreResult};
use crate::ops::groups::cluster_offsets_reader::ClusterOffsetsReader;
use crate::ops::groups::group_snapshot::{GroupDescription, PartitionWatermarks};

/// State shown for a group that `DescribeGroups` did not report but that has committed offsets.
const UNKNOWN_GROUP_STATE: &str = "Unknown";

/// Which groups and topics to read. `group: None` means every group.
#[derive(Clone, Debug, Default)]
pub struct LagRequest {
    pub group: Option<String>,
    pub topic: Option<String>,
}

/// The group member a partition is assigned to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct MemberRef {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
}

/// Lag of one partition. `committed_offset` and `lag` are `None` when the group has no commit there.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: Option<i64>,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
    pub lag: Option<i64>,
    pub member: Option<MemberRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TopicLag {
    pub topic: String,
    pub total_lag: i64,
    pub partitions: Vec<PartitionLag>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct GroupLag {
    pub group: String,
    pub state: String,
    pub has_active_members: bool,
    pub total_lag: i64,
    pub topics: Vec<TopicLag>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct GroupLagReport {
    pub groups: Vec<GroupLag>,
    pub warnings: Vec<String>,
    /// Groups left out of `--all-groups` because their lag could not be read.
    pub unreadable_groups: Vec<UnreadableGroup>,
}

/// A group whose lag could not be read, with the reason.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct UnreadableGroup {
    pub group: String,
    pub error: String,
}

/// Reads the groups of `request` one at a time and computes their lag.
///
/// The partitions read for a group are the ones of `--topic` when given; otherwise the topics its
/// members are assigned or subscribed to; otherwise, for a group without members or with a member
/// whose metadata was not decoded, every topic of the cluster. Of those, only partitions with a committed offset are kept, plus the partitions
/// assigned to an active member, which are always kept.
pub fn collect_group_lag(
    reader: &dyn ClusterOffsetsReader,
    request: &LagRequest,
) -> CoreResult<GroupLagReport> {
    let mut warnings = Vec::new();
    let mut descriptions = reader.describe_groups(request.group.as_deref())?;
    let mut requested_group_missing = false;
    if let Some(name) = &request.group {
        descriptions.retain(|description| description.name == *name);
        if descriptions.is_empty() {
            requested_group_missing = true;
            descriptions.push(describe_unknown_group(name));
        }
    }
    if (descriptions.is_empty() || requested_group_missing)
        && let Some(warning) = incomplete_group_list_warning(reader.broker_family())
    {
        warnings.push(warning.to_owned());
    }
    descriptions.sort_by(|left, right| left.name.cmp(&right.name));

    let mut topics = TopicPartitionSource::new(reader, request.topic.as_deref());
    let watermarks = WatermarkCache::new();
    let mut candidates_per_group = Vec::with_capacity(descriptions.len());
    for description in &descriptions {
        candidates_per_group.push(topics.select_candidate_partitions(description)?);
    }
    let results = compute_lag_of_groups(reader, &watermarks, &descriptions, &candidates_per_group);

    let mut groups = Vec::with_capacity(descriptions.len());
    let mut unreadable_groups = Vec::new();
    for (description, result) in descriptions.iter().zip(results) {
        let group_lag = match result {
            Ok(group_lag) => group_lag,
            // With --group the error is the answer; across every group it costs only that group.
            Err(error) if request.group.is_none() => {
                warnings.push(format!(
                    "could not read the lag of group '{}': {error}",
                    description.name
                ));
                unreadable_groups.push(UnreadableGroup {
                    group: description.name.clone(),
                    error: error.to_string(),
                });
                continue;
            }
            Err(error) => return Err(error),
        };
        if requested_group_missing && group_lag.topics.is_empty() {
            return Err(CoreError::GroupNotFound(description.name.clone()));
        }
        groups.push(group_lag);
    }
    Ok(GroupLagReport {
        groups,
        warnings,
        unreadable_groups,
    })
}

fn describe_unknown_group(name: &str) -> GroupDescription {
    GroupDescription {
        name: name.to_owned(),
        state: UNKNOWN_GROUP_STATE.to_owned(),
        protocol_type: String::new(),
        members: Vec::new(),
    }
}

fn is_internal_topic(topic: &str) -> bool {
    topic.starts_with("__")
}

/// Topic metadata read at most once per call, shared by every group of the request.
struct TopicPartitionSource<'a> {
    reader: &'a dyn ClusterOffsetsReader,
    topic: Option<&'a str>,
    cached: Option<BTreeMap<String, Vec<i32>>>,
}

impl<'a> TopicPartitionSource<'a> {
    fn new(reader: &'a dyn ClusterOffsetsReader, topic: Option<&'a str>) -> Self {
        Self {
            reader,
            topic,
            cached: None,
        }
    }

    fn load(&mut self) -> CoreResult<&BTreeMap<String, Vec<i32>>> {
        if self.cached.is_none() {
            let mut partitions = self.reader.list_topic_partitions(self.topic)?;
            partitions.retain(|topic, _| !is_internal_topic(topic));
            for topic_partitions in partitions.values_mut() {
                topic_partitions.sort_unstable();
                topic_partitions.dedup();
            }
            self.cached = Some(partitions);
        }
        Ok(self.cached.get_or_insert_with(BTreeMap::new))
    }

    fn select_candidate_partitions(
        &mut self,
        description: &GroupDescription,
    ) -> CoreResult<BTreeMap<String, Vec<i32>>> {
        let restricted_to_topic = self.topic.is_some();
        let all_partitions = self.load()?;
        // A member whose metadata was not decoded tells nothing about its topics, so its group is
        // read like an empty one: every topic, kept where it has a commit.
        let has_undecoded_member = description
            .members
            .iter()
            .any(|member| member.subscription.is_none() && member.assignment.is_none());
        if restricted_to_topic || description.members.is_empty() || has_undecoded_member {
            return Ok(all_partitions.clone());
        }
        let member_topics: BTreeSet<&str> = description
            .members
            .iter()
            .flat_map(|member| {
                let assigned = member
                    .assignment
                    .iter()
                    .flat_map(|assignment| assignment.partitions.keys());
                let subscribed = member
                    .subscription
                    .iter()
                    .flat_map(|subscription| subscription.topics.iter());
                assigned.chain(subscribed).map(String::as_str)
            })
            .collect();
        Ok(all_partitions
            .iter()
            .filter(|(topic, _)| member_topics.contains(topic.as_str()))
            .map(|(topic, partitions)| (topic.clone(), partitions.clone()))
            .collect())
    }
}

/// Most groups read at the same time. Each one opens its own client, so this also caps sockets.
const LAG_WORKERS: usize = 8;

/// Reads the lag of every group on up to `LAG_WORKERS` threads. Results keep the order of
/// `descriptions`. A single group is read on the calling thread.
fn compute_lag_of_groups(
    reader: &dyn ClusterOffsetsReader,
    watermarks: &WatermarkCache,
    descriptions: &[GroupDescription],
    candidates_per_group: &[BTreeMap<String, Vec<i32>>],
) -> Vec<CoreResult<GroupLag>> {
    let compute = |index: usize| {
        compute_group_lag(
            reader,
            watermarks,
            &descriptions[index],
            &candidates_per_group[index],
        )
    };
    let workers = LAG_WORKERS.min(descriptions.len());
    if workers <= 1 {
        return (0..descriptions.len()).map(compute).collect();
    }
    let next_index = AtomicUsize::new(0);
    let mut slots: Vec<Option<CoreResult<GroupLag>>> =
        (0..descriptions.len()).map(|_| None).collect();
    let finished: Vec<Vec<(usize, CoreResult<GroupLag>)>> = thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                scope.spawn(|| {
                    let mut done = Vec::new();
                    loop {
                        let index = next_index.fetch_add(1, Ordering::Relaxed);
                        if index >= descriptions.len() {
                            return done;
                        }
                        done.push((index, compute(index)));
                    }
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap_or_default())
            .collect()
    });
    for (index, result) in finished.into_iter().flatten() {
        slots[index] = Some(result);
    }
    slots
        .into_iter()
        .zip(descriptions)
        .map(|(slot, description)| {
            slot.unwrap_or_else(|| {
                Err(CoreError::Kafka(format!(
                    "the worker reading group '{}' stopped unexpectedly",
                    description.name
                )))
            })
        })
        .collect()
}

/// Watermarks of one lag sample, shared by every group of the sample. Each partition is fetched
/// once: concurrent workers wait for the first fetch, and a failure is kept for the sample too,
/// so a partition without a leader costs one timeout instead of one per group.
struct WatermarkCache {
    known: Mutex<BTreeMap<(String, i32), Arc<WatermarkCell>>>,
}

/// One partition's fetch: empty until the first worker stores the answer or the error text.
type WatermarkCell = OnceLock<Result<PartitionWatermarks, String>>;

impl WatermarkCache {
    fn new() -> Self {
        Self {
            known: Mutex::new(BTreeMap::new()),
        }
    }

    fn get(
        &self,
        reader: &dyn ClusterOffsetsReader,
        topic: &str,
        partition: i32,
    ) -> CoreResult<PartitionWatermarks> {
        let cell = {
            // The map only caches broker answers, so a poisoned lock still holds valid entries.
            let mut known = self.known.lock().unwrap_or_else(PoisonError::into_inner);
            Arc::clone(known.entry((topic.to_owned(), partition)).or_default())
        };
        let mut first_error = None;
        let stored = cell.get_or_init(|| {
            reader.fetch_watermarks(topic, partition).map_err(|error| {
                let text = error.to_string();
                first_error = Some(error);
                text
            })
        });
        if let Some(error) = first_error {
            return Err(error);
        }
        stored.as_ref().copied().map_err(|text| {
            CoreError::Kafka(format!(
                "watermarks of {topic}/{partition} could not be read in this sample ({text})"
            ))
        })
    }
}

fn compute_group_lag(
    reader: &dyn ClusterOffsetsReader,
    watermarks_cache: &WatermarkCache,
    description: &GroupDescription,
    candidates: &BTreeMap<String, Vec<i32>>,
) -> CoreResult<GroupLag> {
    let committed = if candidates.is_empty() {
        BTreeMap::new()
    } else {
        reader.fetch_committed_offsets(&description.name, candidates)?
    };
    let assigned_members = map_assigned_members(description);
    let mut topics = Vec::new();
    for (topic, partitions) in candidates {
        let mut partition_lags = Vec::new();
        for partition in partitions {
            let key = (topic.clone(), *partition);
            let committed_offset = committed.get(&key).copied().flatten();
            let member = assigned_members.get(&key).cloned();
            if committed_offset.is_none() && member.is_none() {
                continue;
            }
            let watermarks = watermarks_cache.get(reader, topic, *partition)?;
            partition_lags.push(PartitionLag {
                topic: topic.clone(),
                partition: *partition,
                committed_offset,
                log_start_offset: watermarks.low,
                log_end_offset: watermarks.high,
                lag: committed_offset.map(|offset| (watermarks.high - offset).max(0)),
                member,
            });
        }
        if partition_lags.is_empty() {
            continue;
        }
        let total_lag = partition_lags.iter().filter_map(|lag| lag.lag).sum();
        topics.push(TopicLag {
            topic: topic.clone(),
            total_lag,
            partitions: partition_lags,
        });
    }
    Ok(GroupLag {
        group: description.name.clone(),
        state: description.state.clone(),
        has_active_members: !description.members.is_empty(),
        total_lag: topics.iter().map(|topic| topic.total_lag).sum(),
        topics,
    })
}

fn map_assigned_members(description: &GroupDescription) -> BTreeMap<(String, i32), MemberRef> {
    let mut assigned = BTreeMap::new();
    for member in &description.members {
        let Some(assignment) = &member.assignment else {
            continue;
        };
        for (topic, partitions) in &assignment.partitions {
            for partition in partitions {
                assigned.insert(
                    (topic.clone(), *partition),
                    MemberRef {
                        member_id: member.member_id.clone(),
                        client_id: member.client_id.clone(),
                        host: member.host.clone(),
                    },
                );
            }
        }
    }
    assigned
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::broker_capabilities::BrokerFamily;
    use crate::ops::groups::consumer_protocol::{MemberAssignment, MemberSubscription};
    use crate::ops::groups::fake_cluster_reader::FakeClusterReader;
    use crate::ops::groups::group_snapshot::{GroupDescription, GroupMember, PartitionWatermarks};
    use std::collections::BTreeMap;

    const INCOMPLETE_WARNING: &str = "the group list may be incomplete on Azure Event Hubs (ListGroups is unverified, see ADR 0001)";

    fn group(name: &str, members: Vec<GroupMember>) -> GroupDescription {
        GroupDescription {
            name: name.to_owned(),
            state: if members.is_empty() {
                "Empty"
            } else {
                "Stable"
            }
            .to_owned(),
            protocol_type: "consumer".to_owned(),
            members,
        }
    }

    fn member(id: &str, topic: &str, partitions: &[i32]) -> GroupMember {
        GroupMember {
            member_id: id.to_owned(),
            client_id: format!("{id}-client"),
            host: "/10.0.0.7".to_owned(),
            subscription: Some(MemberSubscription {
                topics: vec![topic.to_owned()],
            }),
            assignment: Some(MemberAssignment {
                partitions: BTreeMap::from([(topic.to_owned(), partitions.to_vec())]),
            }),
        }
    }

    fn reader_with_topic(topic: &str, highs: &[i64]) -> FakeClusterReader {
        let mut reader = FakeClusterReader::new(BrokerFamily::GenericKafka);
        reader.topics.insert(
            topic.to_owned(),
            (0..i32::try_from(highs.len()).unwrap()).collect(),
        );
        for (partition, high) in (0..).zip(highs) {
            reader.watermarks.insert(
                (topic.to_owned(), partition),
                PartitionWatermarks {
                    low: 0,
                    high: *high,
                },
            );
        }
        reader
    }

    fn commit(
        reader: &mut FakeClusterReader,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) {
        reader
            .committed
            .insert((group.to_owned(), topic.to_owned(), partition), offset);
    }

    fn request_group(name: &str) -> LagRequest {
        LagRequest {
            group: Some(name.to_owned()),
            topic: None,
        }
    }

    #[test]
    fn parallel_lag_keeps_alphabetical_group_order() {
        let mut reader = reader_with_topic("t", &[100]);
        for index in (0..20).rev() {
            let name = format!("g{index:02}");
            reader.groups.push(group(&name, Vec::new()));
            commit(&mut reader, &name, "t", 0, index);
        }

        let report = collect_group_lag(&reader, &LagRequest::default()).unwrap();

        let names: Vec<String> = report.groups.iter().map(|lag| lag.group.clone()).collect();
        let expected: Vec<String> = (0..20).map(|index| format!("g{index:02}")).collect();
        assert_eq!(names, expected);
    }

    #[test]
    fn parallel_lag_reports_failing_group_as_unreadable() {
        let mut reader = reader_with_topic("t", &[100]);
        for name in ["g0", "g1", "g2"] {
            reader.groups.push(group(name, Vec::new()));
            commit(&mut reader, name, "t", 0, 10);
        }
        reader
            .failing_committed_partitions
            .insert(("g1".to_owned(), "t".to_owned(), 0));

        let report = collect_group_lag(&reader, &LagRequest::default()).unwrap();

        let names: Vec<&str> = report.groups.iter().map(|lag| lag.group.as_str()).collect();
        assert_eq!(names, vec!["g0", "g2"]);
        assert_eq!(report.unreadable_groups[0].group, "g1");
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.starts_with("could not read the lag of group 'g1': "))
        );
    }

    #[test]
    fn groups_are_read_on_several_threads() {
        let mut reader = reader_with_topic("t", &[100]);
        reader.committed_rendezvous = 2;
        for index in 0..16 {
            let name = format!("g{index:02}");
            reader.groups.push(group(&name, Vec::new()));
            commit(&mut reader, &name, "t", 0, 1);
        }

        collect_group_lag(&reader, &LagRequest::default()).unwrap();

        assert!(reader.committed_threads().len() > 1);
    }

    #[test]
    fn single_group_does_not_start_workers() {
        let mut reader = reader_with_topic("t", &[100]);
        reader.groups.push(group("g1", Vec::new()));
        commit(&mut reader, "g1", "t", 0, 1);

        collect_group_lag(&reader, &request_group("g1")).unwrap();

        assert_eq!(
            reader.committed_threads(),
            std::collections::HashSet::from([std::thread::current().id()])
        );
        assert_eq!(reader.committed_offset_requests().len(), 1);
    }

    #[test]
    fn single_group_request_returns_its_error() {
        let mut reader = reader_with_topic("t", &[100]);
        reader.groups.push(group("g1", Vec::new()));
        commit(&mut reader, "g1", "t", 0, 10);
        reader
            .failing_committed_partitions
            .insert(("g1".to_owned(), "t".to_owned(), 0));

        assert!(collect_group_lag(&reader, &request_group("g1")).is_err());
    }

    #[test]
    fn concurrent_groups_share_one_watermark_fetch() {
        let mut reader = reader_with_topic("t", &[100]);
        reader.watermark_rendezvous = 2;
        for index in 0..16 {
            let name = format!("g{index:02}");
            reader.groups.push(group(&name, Vec::new()));
            commit(&mut reader, &name, "t", 0, 1);
        }

        let report = collect_group_lag(&reader, &LagRequest::default()).unwrap();

        assert_eq!(report.groups.len(), 16);
        assert_eq!(reader.watermark_requests(), 1);
    }

    #[test]
    fn failed_watermarks_are_read_once_per_sample() {
        let mut reader = reader_with_topic("t", &[100]);
        reader.failing_watermarks.insert(("t".to_owned(), 0));
        for name in ["g0", "g1", "g2"] {
            reader.groups.push(group(name, Vec::new()));
            commit(&mut reader, name, "t", 0, 1);
        }

        let report = collect_group_lag(&reader, &LagRequest::default()).unwrap();

        assert_eq!(report.unreadable_groups.len(), 3);
        assert_eq!(reader.watermark_requests(), 1);
        assert!(
            report
                .unreadable_groups
                .iter()
                .all(|unreadable| unreadable.error.contains("LeaderNotAvailable")),
            "{:?}",
            report.unreadable_groups
        );
    }

    #[test]
    fn watermarks_are_fetched_once_per_partition_across_groups() {
        let mut reader = reader_with_topic("t", &[100, 100]);
        for name in ["g1", "g2"] {
            reader.groups.push(group(name, Vec::new()));
            commit(&mut reader, name, "t", 0, 10);
            commit(&mut reader, name, "t", 1, 20);
        }

        let report = collect_group_lag(&reader, &LagRequest::default()).unwrap();

        assert_eq!(report.groups.len(), 2);
        assert_eq!(reader.watermark_requests(), 2);
    }

    #[test]
    fn undecodable_members_scan_every_topic_for_commits() {
        let mut reader = reader_with_topic("pedidos", &[100]);
        let mut undecoded = member("m1", "pedidos", &[0]);
        undecoded.subscription = None;
        undecoded.assignment = None;
        reader.groups.push(group("g", vec![undecoded]));
        commit(&mut reader, "g", "pedidos", 0, 40);

        let report = collect_group_lag(&reader, &request_group("g")).unwrap();

        assert_eq!(report.groups[0].total_lag, 60);
        assert!(report.groups[0].has_active_members);
    }

    #[test]
    fn non_consumer_protocol_group_reports_lag_from_commits() {
        let mut reader = reader_with_topic("pedidos", &[100]);
        let mut connect_member = member("m1", "pedidos", &[0]);
        connect_member.subscription = None;
        connect_member.assignment = None;
        let mut description = group("connect-cluster", vec![connect_member]);
        description.protocol_type = "connect".to_owned();
        reader.groups.push(description);
        commit(&mut reader, "connect-cluster", "pedidos", 0, 90);

        let report = collect_group_lag(&reader, &LagRequest::default()).unwrap();

        assert_eq!(report.groups[0].total_lag, 10);
    }

    #[test]
    fn computes_lag_per_partition_and_totals() {
        let mut reader = reader_with_topic("pedidos", &[100, 50]);
        reader
            .groups
            .push(group("g", vec![member("m1", "pedidos", &[0, 1])]));
        commit(&mut reader, "g", "pedidos", 0, 90);
        commit(&mut reader, "g", "pedidos", 1, 50);

        let report = collect_group_lag(&reader, &request_group("g")).unwrap();

        let group_lag = &report.groups[0];
        assert_eq!(group_lag.total_lag, 10);
        assert!(group_lag.has_active_members);
        let partitions = &group_lag.topics[0].partitions;
        assert_eq!(partitions[0].lag, Some(10));
        assert_eq!(partitions[0].committed_offset, Some(90));
        assert_eq!(partitions[0].log_end_offset, 100);
        assert_eq!(partitions[1].lag, Some(0));
        assert_eq!(group_lag.topics[0].total_lag, 10);
        assert_eq!(
            partitions[0]
                .member
                .as_ref()
                .map(|member| member.member_id.as_str()),
            Some("m1")
        );
    }

    #[test]
    fn partition_without_commit_is_null_and_not_counted() {
        let mut reader = reader_with_topic("pedidos", &[100, 40]);
        reader
            .groups
            .push(group("g", vec![member("m1", "pedidos", &[0, 1])]));
        commit(&mut reader, "g", "pedidos", 0, 70);

        let report = collect_group_lag(&reader, &request_group("g")).unwrap();

        let partitions = &report.groups[0].topics[0].partitions;
        assert_eq!(partitions.len(), 2);
        assert_eq!(partitions[1].committed_offset, None);
        assert_eq!(partitions[1].lag, None);
        assert_eq!(report.groups[0].total_lag, 30);
    }

    #[test]
    fn empty_group_finds_topics_from_committed_offsets() {
        let mut reader = reader_with_topic("pedidos", &[10]);
        reader.topics.insert("pagamentos".to_owned(), vec![0]);
        reader.watermarks.insert(
            ("pagamentos".to_owned(), 0),
            PartitionWatermarks { low: 0, high: 5 },
        );
        reader.groups.push(group("g", Vec::new()));
        commit(&mut reader, "g", "pedidos", 0, 4);

        let report = collect_group_lag(&reader, &request_group("g")).unwrap();

        let group_lag = &report.groups[0];
        let topics: Vec<&str> = group_lag
            .topics
            .iter()
            .map(|topic| topic.topic.as_str())
            .collect();
        assert_eq!(topics, vec!["pedidos"]);
        assert!(!group_lag.has_active_members);
        assert_eq!(group_lag.total_lag, 6);
    }

    #[test]
    fn committed_above_high_watermark_clamps_to_zero() {
        let mut reader = reader_with_topic("pedidos", &[10]);
        reader.groups.push(group("g", Vec::new()));
        commit(&mut reader, "g", "pedidos", 0, 15);

        let report = collect_group_lag(&reader, &request_group("g")).unwrap();

        assert_eq!(report.groups[0].topics[0].partitions[0].lag, Some(0));
        assert_eq!(report.groups[0].total_lag, 0);
    }

    #[test]
    fn unknown_group_without_commits_is_group_not_found() {
        let reader = reader_with_topic("pedidos", &[10]);

        let result = collect_group_lag(&reader, &request_group("nope"));

        assert!(
            matches!(&result, Err(CoreError::GroupNotFound(name)) if name == "nope"),
            "{result:?}"
        );
    }

    #[test]
    fn unknown_group_with_commits_is_reported_without_members() {
        let mut reader = reader_with_topic("pedidos", &[10]);
        commit(&mut reader, "g", "pedidos", 0, 3);

        let report = collect_group_lag(&reader, &request_group("g")).unwrap();

        assert_eq!(report.groups[0].state, "Unknown");
        assert!(!report.groups[0].has_active_members);
        assert_eq!(report.groups[0].total_lag, 7);
    }

    #[test]
    fn topic_flag_limits_partitions_to_that_topic() {
        let mut reader = reader_with_topic("pedidos", &[10]);
        reader.topics.insert("pagamentos".to_owned(), vec![0]);
        reader.watermarks.insert(
            ("pagamentos".to_owned(), 0),
            PartitionWatermarks { low: 0, high: 5 },
        );
        reader.groups.push(group("g", Vec::new()));
        commit(&mut reader, "g", "pedidos", 0, 4);
        commit(&mut reader, "g", "pagamentos", 0, 1);

        let request = LagRequest {
            group: Some("g".to_owned()),
            topic: Some("pagamentos".to_owned()),
        };
        let report = collect_group_lag(&reader, &request).unwrap();

        let topics: Vec<&str> = report.groups[0]
            .topics
            .iter()
            .map(|topic| topic.topic.as_str())
            .collect();
        assert_eq!(topics, vec!["pagamentos"]);
    }

    #[test]
    fn internal_topics_are_skipped() {
        let mut reader = reader_with_topic("pedidos", &[10]);
        reader
            .topics
            .insert("__consumer_offsets".to_owned(), vec![0, 1]);
        reader.groups.push(group("g", Vec::new()));
        commit(&mut reader, "g", "pedidos", 0, 4);

        collect_group_lag(&reader, &request_group("g")).unwrap();

        for (_, partitions) in reader.committed_offset_requests() {
            assert!(
                !partitions.contains_key("__consumer_offsets"),
                "{partitions:?}"
            );
        }
    }

    #[test]
    fn event_hubs_empty_group_list_adds_warning() {
        let reader = FakeClusterReader::new(BrokerFamily::AzureEventHubs);

        let report = collect_group_lag(&reader, &LagRequest::default()).unwrap();

        assert!(report.groups.is_empty());
        assert_eq!(report.warnings, vec![INCOMPLETE_WARNING.to_owned()]);
    }

    #[test]
    fn generic_kafka_empty_group_list_has_no_warning() {
        let reader = FakeClusterReader::new(BrokerFamily::GenericKafka);

        let report = collect_group_lag(&reader, &LagRequest::default()).unwrap();

        assert!(report.warnings.is_empty());
    }

    #[test]
    fn reads_offsets_once_per_group() {
        let mut reader = reader_with_topic("pedidos", &[10]);
        for name in ["a", "b", "c"] {
            reader.groups.push(group(name, Vec::new()));
            commit(&mut reader, name, "pedidos", 0, 1);
        }

        let report = collect_group_lag(&reader, &LagRequest::default()).unwrap();

        let mut requested: Vec<String> = reader
            .committed_offset_requests()
            .into_iter()
            .map(|(group, _)| group)
            .collect();
        requested.sort();
        assert_eq!(requested, vec!["a", "b", "c"]);
        assert_eq!(report.groups.len(), 3);
    }

    #[test]
    fn partition_without_leader_skips_only_its_group_in_all_groups() {
        let mut reader = reader_with_topic("pedidos", &[10]);
        reader.topics.insert("pagamentos".to_owned(), vec![0]);
        reader.watermarks.insert(
            ("pagamentos".to_owned(), 0),
            PartitionWatermarks { low: 0, high: 5 },
        );
        reader.groups.push(group("g1", Vec::new()));
        reader.groups.push(group("g2", Vec::new()));
        commit(&mut reader, "g1", "pedidos", 0, 1);
        commit(&mut reader, "g2", "pagamentos", 0, 1);
        reader.failing_watermarks.insert(("pedidos".to_owned(), 0));

        let report = collect_group_lag(&reader, &LagRequest::default()).unwrap();

        let names: Vec<&str> = report
            .groups
            .iter()
            .map(|group| group.group.as_str())
            .collect();
        assert_eq!(names, vec!["g2"]);
        assert_eq!(report.unreadable_groups.len(), 1);
        assert_eq!(report.unreadable_groups[0].group, "g1");
        assert!(
            report.unreadable_groups[0]
                .error
                .contains("LeaderNotAvailable"),
            "{:?}",
            report.unreadable_groups
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|warning| warning.starts_with("could not read the lag of group 'g1': ")),
            "{:?}",
            report.warnings
        );
    }

    #[test]
    fn requested_group_error_is_still_returned() {
        let mut reader = reader_with_topic("pedidos", &[10]);
        reader.groups.push(group("g1", Vec::new()));
        commit(&mut reader, "g1", "pedidos", 0, 1);
        reader.failing_watermarks.insert(("pedidos".to_owned(), 0));

        let result = collect_group_lag(&reader, &request_group("g1"));

        assert!(
            matches!(&result, Err(error) if error.to_string().contains("LeaderNotAvailable")),
            "{result:?}"
        );
    }

    #[test]
    fn offset_fetch_partition_error_is_not_read_as_missing_commit() {
        let mut reader = reader_with_topic("pedidos", &[10]);
        reader.groups.push(group("g", Vec::new()));
        commit(&mut reader, "g", "pedidos", 0, 1);
        reader
            .failing_committed_partitions
            .insert(("g".to_owned(), "pedidos".to_owned(), 0));

        let result = collect_group_lag(&reader, &request_group("g"));

        assert!(
            matches!(&result, Err(error) if error.to_string().contains("TopicAuthorizationFailed")),
            "{result:?}"
        );
    }
}
