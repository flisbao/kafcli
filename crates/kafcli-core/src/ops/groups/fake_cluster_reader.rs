//! In-memory `ClusterOffsetsReader` for tests.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

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
            committed_offset_requests: RefCell::new(Vec::new()),
        }
    }

    /// The `(group, partitions)` arguments of every `fetch_committed_offsets` call, in order.
    pub fn committed_offset_requests(&self) -> Vec<CommittedOffsetRequest> {
        self.committed_offset_requests.borrow().clone()
    }
}

impl ClusterOffsetsReader for FakeClusterReader {
    fn describe_groups(&self, group: Option<&str>) -> CoreResult<Vec<GroupDescription>> {
        Ok(self
            .groups
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
                let committed = self
                    .committed
                    .get(&(group.to_owned(), topic.clone(), *partition))
                    .copied();
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

    fn broker_family(&self) -> BrokerFamily {
        self.family
    }
}
