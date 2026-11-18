//! Read-only operations on consumer groups: protocol decoding, offsets and lag.

pub mod cluster_offsets_reader;
pub mod consumer_protocol;
#[cfg(any(test, feature = "test-support"))]
pub mod fake_cluster_reader;
pub mod group_lag;
pub mod group_snapshot;
pub mod kafka_cluster_reader;
