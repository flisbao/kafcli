//! The read-only cluster access that the lag and audit operations need.

use std::collections::BTreeMap;

use crate::capability::broker_capabilities::BrokerFamily;
use crate::error::CoreResult;
use crate::ops::groups::group_snapshot::{GroupDescription, PartitionWatermarks};

/// Reads groups, topics and offsets from a cluster without changing anything.
pub trait ClusterOffsetsReader {
    /// Describes one group, or every group when `group` is `None`. Groups that do not exist are
    /// left out.
    fn describe_groups(&self, group: Option<&str>) -> CoreResult<Vec<GroupDescription>>;

    /// Partitions of one topic, or of every topic except the internal ones (names starting with
    /// `__`) when `topic` is `None`.
    fn list_topic_partitions(&self, topic: Option<&str>) -> CoreResult<BTreeMap<String, Vec<i32>>>;

    /// Committed offset of each partition for `group`; `None` when the group committed nothing there.
    fn fetch_committed_offsets(
        &self,
        group: &str,
        partitions: &BTreeMap<String, Vec<i32>>,
    ) -> CoreResult<BTreeMap<(String, i32), Option<i64>>>;

    fn fetch_watermarks(&self, topic: &str, partition: i32) -> CoreResult<PartitionWatermarks>;

    fn broker_family(&self) -> BrokerFamily;
}
