//! `PartitionMessageReader` backed by a librdkafka consumer that assigns, reads and never commits.

use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::{ClientConfig, Message};

use crate::connection::kafka_connection::KafkaConnection;
use crate::error::{CoreError, CoreResult};
use crate::message::record_header::RecordHeader;
use crate::ops::groups::kafka_cluster_reader::build_read_only_config;
use crate::ops::poison::partition_message_reader::{FetchedRecord, PartitionMessageReader};

/// Longest single poll, so the deadline is checked often.
const POLL_SLICE: Duration = Duration::from_millis(500);

/// Reads with a throwaway group id that never joins: `assign`, never `subscribe`; never `commit`.
pub struct KafkaPartitionMessageReader {
    connection: KafkaConnection,
    timeout: Duration,
}

impl KafkaPartitionMessageReader {
    pub fn new(connection: KafkaConnection, timeout: Duration) -> Self {
        Self {
            connection,
            timeout,
        }
    }
}

impl PartitionMessageReader for KafkaPartitionMessageReader {
    fn read_first_messages(
        &self,
        topic: &str,
        start_offsets: &BTreeMap<i32, i64>,
    ) -> CoreResult<BTreeMap<i32, FetchedRecord>> {
        if start_offsets.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut config = build_read_only_config(&self.connection);
        config.set("group.id", throwaway_group_id());
        // An offset outside the log is an error, never a silent jump to another message.
        config.set("auto.offset.reset", "error");
        limit_prefetch(&mut config);
        let consumer = self.connection.create_consumer(config)?;
        let mut assignment = TopicPartitionList::new();
        for (partition, start) in start_offsets {
            assignment
                .add_partition_offset(topic, *partition, Offset::Offset(*start))
                .map_err(map_read_error)?;
        }
        consumer.assign(&assignment).map_err(map_read_error)?;

        let mut found = BTreeMap::new();
        let deadline = Instant::now() + self.timeout;
        while found.len() < start_offsets.len() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let Some(received) = consumer.poll(remaining.min(POLL_SLICE)) else {
                continue;
            };
            let message = received.map_err(map_read_error)?;
            let partition = message.partition();
            if found.contains_key(&partition) || !start_offsets.contains_key(&partition) {
                continue;
            }
            found.insert(partition, fetched_record(&message));
            let mut done = TopicPartitionList::new();
            done.add_partition(topic, partition);
            consumer.pause(&done).map_err(map_read_error)?;
        }
        drop(consumer);
        Ok(found)
    }
}

/// Only the first message of each partition is read, so the consumer prefetches little: 1 MiB
/// queued in total instead of librdkafka's 64 MiB, and one message per partition.
fn limit_prefetch(config: &mut ClientConfig) {
    config.set("queued.max.messages.kbytes", "1024");
    config.set("queued.min.messages", "1");
}

fn throwaway_group_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or_default();
    format!("kafcli-poison-{millis}")
}

fn fetched_record(message: &BorrowedMessage<'_>) -> FetchedRecord {
    FetchedRecord {
        topic: message.topic().to_owned(),
        partition: message.partition(),
        offset: message.offset(),
        timestamp_millis: message.timestamp().to_millis(),
        key: message.key().map(<[u8]>::to_vec),
        value: message.payload().map(<[u8]>::to_vec),
        headers: message
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .map(|header| RecordHeader {
                        name: header.key.to_owned(),
                        value: header.value.map(<[u8]>::to_vec),
                    })
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn map_read_error(error: rdkafka::error::KafkaError) -> CoreError {
    CoreError::Kafka(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poison_reader_limits_prefetch() {
        let mut config = ClientConfig::new();

        limit_prefetch(&mut config);

        assert_eq!(config.get("queued.max.messages.kbytes"), Some("1024"));
        assert_eq!(config.get("queued.min.messages"), Some("1"));
    }
}
