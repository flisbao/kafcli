//! Messages a consumer group is stuck on: find them, and skip them by copying to a dead-letter topic.

#[cfg(any(test, feature = "test-support"))]
pub mod fake_message_reader;
pub mod kafka_message_reader;
pub mod partition_message_reader;
pub mod poison_finder;
