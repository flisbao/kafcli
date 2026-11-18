//! In-memory `ClusterOffsetsReader` for tests.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

use crate::capability::broker_capabilities::BrokerFamily;
use crate::error::{CoreError, CoreResult};
use crate::ops::groups::cluster_offsets_reader::ClusterOffsetsReader;
use crate::ops::groups::group_snapshot::{GroupDescription, PartitionWatermarks};

/// One `fetch_committed_offsets` call: the group and the partitions it asked for.
pub type CommittedOffsetRequest = (String, BTreeMap<String, Vec<i32>>);

/// Serves fixed data and records every `fetch_committed_offsets` call.
///
/// `topics` is returned as given, internal topics included, so tests can check that callers skip
/// them.
pub struct FakeClusterReader {
    pub family: BrokerFamily,
    pub groups: Vec<GroupDescription>,
    pub topics: BTreeMap<String, Vec<i32>>,
    /// Committed offsets keyed by `(group, topic, partition)`.
    pub committed: BTreeMap<(String, String, i32), i64>,
    pub watermarks: BTreeMap<(String, i32), PartitionWatermarks>,
    /// `(topic, partition)` whose watermarks fail with `LeaderNotAvailable`.
    pub failing_watermarks: BTreeSet<(String, i32)>,
    /// `(group, topic, partition)` whose committed offset fails with `TopicAuthorizationFailed`.
    pub failing_committed_partitions: BTreeSet<(String, String, i32)>,
    /// `(timestamp_millis, offset)` pairs per `(topic, partition)`, in offset order.
    pub times: BTreeMap<(String, i32), Vec<(i64, i64)>>,
    /// Answers for successive `describe_groups` calls; the last one repeats. Empty means `groups`.
    pub group_snapshots: RefCell<VecDeque<Vec<GroupDescription>>>,
    /// When set, `describe_groups` fails with `CoreError::Kafka` of this text.
    pub describe_failure: Option<String>,
    /// Commits made through a linked fake committer; read before `committed`.
    pub committed_overrides: RefCell<BTreeMap<(String, String, i32), i64>>,
    clock: Cell<Instant>,
    waits: RefCell<Vec<Duration>>,
    committed_offset_requests: RefCell<Vec<CommittedOffsetRequest>>,
}

impl FakeClusterReader {
    pub fn new(family: BrokerFamily) -> Self {
        Self {
            family,
            groups: Vec::new(),
            topics: BTreeMap::new(),
            committed: BTreeMap::new(),
            watermarks: BTreeMap::new(),
            failing_watermarks: BTreeSet::new(),
            failing_committed_partitions: BTreeSet::new(),
            times: BTreeMap::new(),
            group_snapshots: RefCell::new(VecDeque::new()),
            describe_failure: None,
            committed_overrides: RefCell::new(BTreeMap::new()),
            clock: Cell::new(Instant::now()),
            waits: RefCell::new(Vec::new()),
            committed_offset_requests: RefCell::new(Vec::new()),
        }
    }

    /// The fake clock, moved only by `wait_serving_client_events`.
    pub fn now(&self) -> Instant {
        self.clock.get()
    }

    /// Every interval passed to `wait_serving_client_events`, in order.
    pub fn waits(&self) -> Vec<Duration> {
        self.waits.borrow().clone()
    }

    fn current_groups(&self) -> Vec<GroupDescription> {
        let mut snapshots = self.group_snapshots.borrow_mut();
        match snapshots.len() {
            0 => self.groups.clone(),
            1 => snapshots.front().cloned().unwrap_or_default(),
            _ => snapshots.pop_front().unwrap_or_default(),
        }
    }

    /// The `(group, partitions)` arguments of every `fetch_committed_offsets` call, in order.
    pub fn committed_offset_requests(&self) -> Vec<CommittedOffsetRequest> {
        self.committed_offset_requests.borrow().clone()
    }
}

impl ClusterOffsetsReader for FakeClusterReader {
    fn describe_groups(&self, group: Option<&str>) -> CoreResult<Vec<GroupDescription>> {
        if let Some(failure) = &self.describe_failure {
            return Err(CoreError::Kafka(failure.clone()));
        }
        Ok(self
            .current_groups()
            .iter()
            .filter(|description| group.is_none_or(|name| description.name == name))
            .cloned()
            .collect())
    }

    fn list_topic_partitions(&self, topic: Option<&str>) -> CoreResult<BTreeMap<String, Vec<i32>>> {
        match topic {
            None => Ok(self.topics.clone()),
            Some(name) => match self.topics.get(name) {
                Some(partitions) => Ok(BTreeMap::from([(name.to_owned(), partitions.clone())])),
                None => Err(CoreError::TopicNotFound(name.to_owned())),
            },
        }
    }

    fn fetch_committed_offsets(
        &self,
        group: &str,
        partitions: &BTreeMap<String, Vec<i32>>,
    ) -> CoreResult<BTreeMap<(String, i32), Option<i64>>> {
        self.committed_offset_requests
            .borrow_mut()
            .push((group.to_owned(), partitions.clone()));
        let mut offsets = BTreeMap::new();
        for (topic, topic_partitions) in partitions {
            for partition in topic_partitions {
                if self.failing_committed_partitions.contains(&(
                    group.to_owned(),
                    topic.clone(),
                    *partition,
                )) {
                    return Err(CoreError::Kafka("TopicAuthorizationFailed".to_owned()));
                }
                let key = (group.to_owned(), topic.clone(), *partition);
                let committed = self
                    .committed_overrides
                    .borrow()
                    .get(&key)
                    .copied()
                    .or_else(|| self.committed.get(&key).copied());
                offsets.insert((topic.clone(), *partition), committed);
            }
        }
        Ok(offsets)
    }

    fn fetch_watermarks(&self, topic: &str, partition: i32) -> CoreResult<PartitionWatermarks> {
        if self
            .failing_watermarks
            .contains(&(topic.to_owned(), partition))
        {
            return Err(CoreError::Kafka("LeaderNotAvailable".to_owned()));
        }
        self.watermarks
            .get(&(topic.to_owned(), partition))
            .copied()
            .ok_or_else(|| CoreError::Kafka(format!("no watermarks for {topic}/{partition}")))
    }

    fn find_offsets_for_time(
        &self,
        topic: &str,
        partitions: &[i32],
        timestamp_millis: i64,
    ) -> CoreResult<BTreeMap<i32, Option<i64>>> {
        Ok(partitions
            .iter()
            .map(|partition| {
                let offset = self
                    .times
                    .get(&(topic.to_owned(), *partition))
                    .and_then(|pairs| {
                        pairs
                            .iter()
                            .find(|(timestamp, _)| *timestamp >= timestamp_millis)
                            .map(|(_, offset)| *offset)
                    });
                (*partition, offset)
            })
            .collect())
    }

    fn wait_serving_client_events(&self, interval: Duration) {
        self.clock.set(self.clock.get() + interval);
        self.waits.borrow_mut().push(interval);
    }

    fn broker_family(&self) -> BrokerFamily {
        self.family
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn group(name: &str, state: &str) -> GroupDescription {
        GroupDescription {
            name: name.to_owned(),
            state: state.to_owned(),
            protocol_type: "consumer".to_owned(),
            members: Vec::new(),
        }
    }

    #[test]
    fn fake_finds_first_offset_at_or_after_time() {
        let mut reader = FakeClusterReader::new(BrokerFamily::GenericKafka);
        reader.times.insert(
            ("pedidos".to_owned(), 0),
            vec![(1_000, 0), (2_000, 1), (3_000, 2)],
        );

        let found = reader
            .find_offsets_for_time("pedidos", &[0, 1], 1_500)
            .unwrap();
        let after_last = reader
            .find_offsets_for_time("pedidos", &[0], 9_000)
            .unwrap();

        assert_eq!(found, BTreeMap::from([(0, Some(1)), (1, None)]));
        assert_eq!(after_last, BTreeMap::from([(0, None)]));
    }

    #[test]
    fn fake_replays_group_snapshots_and_keeps_last() {
        let reader = FakeClusterReader::new(BrokerFamily::GenericKafka);
        reader
            .group_snapshots
            .borrow_mut()
            .extend([vec![group("g", "Stable")], vec![group("g", "Empty")]]);

        let states: Vec<String> = (0..3)
            .map(|_| reader.describe_groups(Some("g")).unwrap()[0].state.clone())
            .collect();

        assert_eq!(states, vec!["Stable", "Empty", "Empty"]);
    }

    #[test]
    fn fake_describe_failure_is_returned() {
        let mut reader = FakeClusterReader::new(BrokerFamily::GenericKafka);
        reader.describe_failure = Some("CoordinatorNotAvailable".to_owned());

        let result = reader.describe_groups(Some("g"));

        assert!(
            matches!(&result, Err(CoreError::Kafka(text)) if text == "CoordinatorNotAvailable"),
            "{result:?}"
        );
    }

    #[test]
    fn fake_wait_advances_the_clock_and_records_it() {
        let reader = FakeClusterReader::new(BrokerFamily::GenericKafka);
        let start = reader.now();

        reader.wait_serving_client_events(Duration::from_secs(2));

        assert_eq!(reader.now() - start, Duration::from_secs(2));
        assert_eq!(reader.waits(), vec![Duration::from_secs(2)]);
    }

    #[test]
    fn committed_overrides_win_over_committed() {
        let mut reader = FakeClusterReader::new(BrokerFamily::GenericKafka);
        reader
            .committed
            .insert(("g".to_owned(), "pedidos".to_owned(), 0), 3);
        reader
            .committed_overrides
            .borrow_mut()
            .insert(("g".to_owned(), "pedidos".to_owned(), 0), 9);

        let offsets = reader
            .fetch_committed_offsets("g", &BTreeMap::from([("pedidos".to_owned(), vec![0])]))
            .unwrap();

        assert_eq!(offsets.get(&("pedidos".to_owned(), 0)), Some(&Some(9)));
    }
}
