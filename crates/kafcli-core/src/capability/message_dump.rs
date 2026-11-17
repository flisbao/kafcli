use crate::config::model::ConnectionContext;
use crate::connection::client_config_builder::{ConnectionSettings, build_client_config};
use crate::error::{CoreError, CoreResult};
use crate::secret::resolver::SecretResolver;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde::Serialize;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Bytes of each value kept in a dump, as hex.
const VALUE_PREFIX_BYTES: usize = 64;
const POLL_SLICE: Duration = Duration::from_millis(100);
const DUMP_GROUP_PREFIX: &str = "kafcli-probe-dump-";

/// One header of a dumped message. `value_utf8` is `None` when the bytes are not UTF-8 or the
/// header carries no value; `value_hex` is empty in the second case.
#[derive(Clone, Debug, Serialize)]
pub struct HeaderDump {
    pub name: String,
    pub value_utf8: Option<String>,
    pub value_hex: String,
}

/// Metadata and the first bytes of one message read by `dump_topic_messages`.
#[derive(Clone, Debug, Serialize)]
pub struct MessageDump {
    pub partition: i32,
    pub offset: i64,
    pub timestamp_millis: Option<i64>,
    pub key_length: Option<usize>,
    pub value_length: Option<usize>,
    pub value_prefix_hex: String,
    pub headers: Vec<HeaderDump>,
}

/// Reads up to `maximum_messages` messages from every partition of `topic`, starting at the
/// beginning of each partition.
///
/// The consumer only assigns partitions and never subscribes or commits. It stops when it has
/// read `maximum_messages` or when `timeout` passes without a new message. The result is sorted
/// by partition and then by offset.
pub fn dump_topic_messages(
    context: &ConnectionContext,
    resolver: &dyn SecretResolver,
    settings: &ConnectionSettings,
    topic: &str,
    maximum_messages: usize,
    timeout: Duration,
) -> CoreResult<Vec<MessageDump>> {
    let base_config = build_client_config(context, resolver, settings)?;
    let group_id = format!("{DUMP_GROUP_PREFIX}{}", to_unix_millis(SystemTime::now()));
    let consumer = create_dump_consumer(base_config, &group_id)?;

    let partitions = fetch_partition_identifiers(&consumer, topic, timeout)?;
    let mut assignment = TopicPartitionList::new();
    for partition in partitions {
        assignment
            .add_partition_offset(topic, partition, Offset::Beginning)
            .map_err(map_kafka_error)?;
    }
    consumer.assign(&assignment).map_err(map_kafka_error)?;

    let mut messages = Vec::new();
    let mut last_message_at = Instant::now();
    while messages.len() < maximum_messages && last_message_at.elapsed() < timeout {
        let Some(received) = consumer.poll(POLL_SLICE) else {
            continue;
        };
        messages.push(describe_message(&received.map_err(map_kafka_error)?));
        last_message_at = Instant::now();
    }

    messages.sort_by_key(|message| (message.partition, message.offset));
    Ok(messages)
}

/// Converts one header into its text (when UTF-8) and hex forms.
pub fn describe_header(name: &str, value: Option<&[u8]>) -> HeaderDump {
    HeaderDump {
        name: name.to_string(),
        value_utf8: value
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .map(str::to_string),
        value_hex: value.map(encode_hex).unwrap_or_default(),
    }
}

/// Hex of the first `VALUE_PREFIX_BYTES` bytes of a value; empty when there is no value.
pub fn encode_prefix_hex(value: Option<&[u8]>) -> String {
    value
        .map(|bytes| {
            let prefix: Vec<u8> = bytes.iter().copied().take(VALUE_PREFIX_BYTES).collect();
            encode_hex(&prefix)
        })
        .unwrap_or_default()
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn create_dump_consumer(base_config: ClientConfig, group_id: &str) -> CoreResult<BaseConsumer> {
    let mut config = base_config;
    // Missing topics must never be created by a metadata fetch or a consumer.
    config.set("allow.auto.create.topics", "false");
    config.set("enable.auto.commit", "false");
    // librdkafka rejects `assign` without a group id; the group is never committed to.
    config.set("group.id", group_id);
    config.create().map_err(map_kafka_error)
}

pub(crate) fn fetch_partition_identifiers<C: ConsumerContext>(
    consumer: &BaseConsumer<C>,
    topic: &str,
    timeout: Duration,
) -> CoreResult<Vec<i32>> {
    let metadata = consumer
        .fetch_metadata(Some(topic), timeout)
        .map_err(map_kafka_error)?;
    let topic_metadata = metadata
        .topics()
        .iter()
        .find(|candidate| candidate.name() == topic && candidate.error().is_none())
        .ok_or_else(|| CoreError::TopicNotFound(topic.to_string()))?;
    Ok(topic_metadata
        .partitions()
        .iter()
        .map(|partition| partition.id())
        .collect())
}

fn describe_message(message: &BorrowedMessage<'_>) -> MessageDump {
    let headers = message
        .headers()
        .map(|headers| {
            headers
                .iter()
                .map(|header| describe_header(header.key, header.value))
                .collect()
        })
        .unwrap_or_default();
    MessageDump {
        partition: message.partition(),
        offset: message.offset(),
        timestamp_millis: message.timestamp().to_millis(),
        key_length: message.key().map(<[u8]>::len),
        value_length: message.payload().map(<[u8]>::len),
        value_prefix_hex: encode_prefix_hex(message.payload()),
        headers,
    }
}

fn to_unix_millis(instant: SystemTime) -> i64 {
    instant
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}

fn map_kafka_error(error: rdkafka::error::KafkaError) -> CoreError {
    CoreError::Kafka(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_header_keeps_text() {
        let header = describe_header("content-type", Some(b"avro/binary+abc"));

        assert_eq!(header.name, "content-type");
        assert_eq!(header.value_utf8.as_deref(), Some("avro/binary+abc"));
        assert_eq!(header.value_hex, "6176726f2f62696e6172792b616263");
    }

    #[test]
    fn binary_header_has_no_utf8() {
        let header = describe_header("blob", Some(&[0xff, 0x00]));

        assert_eq!(header.value_utf8, None);
        assert_eq!(header.value_hex, "ff00");
    }

    #[test]
    fn missing_header_value_is_empty() {
        let header = describe_header("flag", None);

        assert_eq!(header.value_utf8, None);
        assert_eq!(header.value_hex, "");
    }

    #[test]
    fn prefix_stops_at_64_bytes() {
        let value = [0x01_u8; 100];

        let prefix = encode_prefix_hex(Some(&value));

        assert_eq!(prefix.len(), 128);
        assert_eq!(prefix, "01".repeat(64));
    }

    #[test]
    fn prefix_of_missing_value_is_empty() {
        assert_eq!(encode_prefix_hex(None), "");
    }
}
