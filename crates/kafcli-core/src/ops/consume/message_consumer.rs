//! Reads messages from a topic and passes each one to an `EnvelopeSink`.
//!
//! Without a group the consumer only assigns partitions and never commits. With a group it
//! subscribes, stores each offset once the message was delivered or filtered out, and lets the
//! group commit the stored offsets. Without `--follow` each partition is read up to
//! the high watermark observed when the run started.

use crate::capability::message_dump::fetch_partition_identifiers;
use crate::codec::value_codec::ValueDecoder;
use crate::config::model::ConnectionContext;
use crate::connection::client_config_builder::ConnectionSettings;
use crate::connection::kafka_connection::{KafkaConnection, build_kafka_connection};
use crate::error::{CoreError, CoreResult};
use crate::message::message_envelope::{EnvelopeValue, MessageEnvelope};
use crate::message::record_header::RecordHeader;
use crate::ops::consume::consume_client_context::{ConsumeClientContext, ConsumeConsumer};
use crate::ops::consume::message_filter::{HeaderFilter, matches_header_filters, redact_fields};
use crate::ops::consume::start_position::StartPosition;
use crate::secret::resolver::SecretResolver;
use rdkafka::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;

/// Longest single `poll`, so the stop conditions are checked at least this often.
const POLL_SLICE: Duration = Duration::from_millis(500);
const CONSUME_GROUP_PREFIX: &str = "kafcli-consume-";
const WITHHELD_REASON: &str =
    "value withheld: --redact cannot be applied to a truncated or undecodable value";

/// What to read and how. `max_bytes = 0` means no limit on the value size kept or decoded.
#[derive(Debug, Clone, PartialEq)]
pub struct ConsumeOptions {
    pub topic: String,
    pub partitions: Option<Vec<i32>>,
    pub start: StartPosition,
    pub count: Option<u64>,
    pub follow: bool,
    pub group: Option<String>,
    pub max_bytes: usize,
    /// Object keys whose values are replaced by `"[redacted]"` in decoded JSON values, at any depth.
    /// Keys and headers are not redacted. A truncated or undecodable value cannot be redacted, so with
    /// any redact name it is withheld instead of emitted.
    pub redact: Vec<String>,
    pub filters: Vec<HeaderFilter>,
    pub timeout: Duration,
}

/// Receives each envelope that passes the filters, one at a time.
pub trait EnvelopeSink {
    fn accept_envelope(&mut self, envelope: &MessageEnvelope) -> io::Result<()>;
}

/// Counts of one consume run. `group_id` is the group the consumer used: the generated
/// `kafcli-consume-<unix millis>` without `--group`, or the group given with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeSummary {
    pub delivered: u64,
    pub skipped_by_filter: u64,
    pub group_id: String,
}

/// Reads `options.topic` and sends each matching message to `sink`.
///
/// Stops when `count` messages were delivered, when every partition reached its target (without
/// `--follow`), or when the sink reports a broken pipe. Without `--follow` it fails when no
/// message or partition end is seen for `timeout`.
pub fn consume_messages(
    context: &ConnectionContext,
    resolver: &dyn SecretResolver,
    settings: &ConnectionSettings,
    options: &ConsumeOptions,
    decoder: &dyn ValueDecoder,
    sink: &mut dyn EnvelopeSink,
) -> CoreResult<ConsumeSummary> {
    validate_options(options)?;
    let connection = build_kafka_connection(context, resolver, settings)?;
    let mut run = ConsumeRun::new(options, decoder, sink);
    match &options.group {
        Some(group) => {
            let consumer = create_subscribe_consumer(&connection, group, options.start)?;
            fetch_partition_identifiers(&consumer, &options.topic, options.timeout)?;
            consumer
                .subscribe(&[options.topic.as_str()])
                .map_err(map_kafka_error)?;
            run.read_until_done(&consumer, true)?;
            Ok(run.summary(group.clone()))
        }
        None => {
            let group_id = format!(
                "{CONSUME_GROUP_PREFIX}{}",
                to_unix_millis(SystemTime::now())
            );
            let consumer = create_assign_consumer(&connection, &group_id)?;
            run.assign_partitions(&consumer)?;
            run.read_until_done(&consumer, false)?;
            Ok(run.summary(group_id))
        }
    }
}

fn validate_options(options: &ConsumeOptions) -> CoreResult<()> {
    if options.group.is_none() {
        return Ok(());
    }
    if matches!(
        options.start,
        StartPosition::Offset(_) | StartPosition::Time(_)
    ) {
        return Err(CoreError::InvalidInput(
            "--from offset:/time: cannot be combined with --group".to_string(),
        ));
    }
    if options.partitions.is_some() {
        return Err(CoreError::InvalidInput(
            "--partition cannot be combined with --group".to_string(),
        ));
    }
    Ok(())
}

/// Whether the loop keeps polling after handling a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flow {
    Continue,
    Stop,
}

/// State of one run: the sink, the partitions still short of their target and the counters.
struct ConsumeRun<'a> {
    options: &'a ConsumeOptions,
    decoder: &'a dyn ValueDecoder,
    sink: &'a mut dyn EnvelopeSink,
    /// Partition -> high watermark to read up to (not used with `--follow`).
    pending: BTreeMap<i32, i64>,
    /// Partitions whose target was already decided, whether or not they still have messages to read.
    registered: BTreeSet<i32>,
    /// True once the consumer holds at least one partition.
    assignment_ready: bool,
    delivered: u64,
    skipped_by_filter: u64,
    last_progress: Instant,
}

impl<'a> ConsumeRun<'a> {
    fn new(
        options: &'a ConsumeOptions,
        decoder: &'a dyn ValueDecoder,
        sink: &'a mut dyn EnvelopeSink,
    ) -> Self {
        Self {
            options,
            decoder,
            sink,
            pending: BTreeMap::new(),
            registered: BTreeSet::new(),
            assignment_ready: false,
            delivered: 0,
            skipped_by_filter: 0,
            last_progress: Instant::now(),
        }
    }

    fn summary(&self, group_id: String) -> ConsumeSummary {
        ConsumeSummary {
            delivered: self.delivered,
            skipped_by_filter: self.skipped_by_filter,
            group_id,
        }
    }

    /// Assigns the selected partitions at the start position and records each partition's target.
    fn assign_partitions(&mut self, consumer: &ConsumeConsumer) -> CoreResult<()> {
        let topic = self.options.topic.as_str();
        let timeout = self.options.timeout;
        let partitions = select_partitions(consumer, self.options)?;
        let mut watermarks = BTreeMap::new();
        for partition in &partitions {
            let watermark = consumer
                .fetch_watermarks(topic, *partition, timeout)
                .map_err(map_kafka_error)?;
            watermarks.insert(*partition, watermark);
        }
        let time_offsets = match self.options.start {
            StartPosition::Time(instant) => {
                resolve_time_offsets(consumer, topic, &partitions, instant, timeout)?
            }
            _ => BTreeMap::new(),
        };

        let mut assignment = TopicPartitionList::new();
        for (partition, (low, high)) in watermarks {
            let (start, next) = match self.options.start {
                StartPosition::Earliest => (Offset::Beginning, low),
                StartPosition::Latest => (Offset::End, high),
                StartPosition::Offset(offset) => clamp_explicit_offset(offset, low, high),
                StartPosition::Time(_) => match time_offsets.get(&partition) {
                    Some(&offset) => (Offset::Offset(offset), offset),
                    None => (Offset::End, high),
                },
            };
            assignment
                .add_partition_offset(topic, partition, start)
                .map_err(map_kafka_error)?;
            if next < high {
                self.pending.insert(partition, high);
            }
            self.registered.insert(partition);
        }
        consumer.assign(&assignment).map_err(map_kafka_error)?;
        self.assignment_ready = true;
        Ok(())
    }

    /// Polls until the stop condition holds or, without `--follow`, the timeout passes with no progress.
    fn read_until_done(&mut self, consumer: &ConsumeConsumer, subscribed: bool) -> CoreResult<()> {
        loop {
            if self.count_reached() {
                return Ok(());
            }
            if !self.options.follow {
                if subscribed {
                    self.register_assignment(consumer)?;
                }
                if self.assignment_ready && self.pending.is_empty() {
                    return Ok(());
                }
                if self.last_progress.elapsed() >= self.options.timeout {
                    return Err(self.stall_error(subscribed));
                }
            }
            let Some(received) = consumer.poll(POLL_SLICE) else {
                continue;
            };
            match received {
                Ok(message) => {
                    self.last_progress = Instant::now();
                    if self.handle_message(consumer, &message)? == Flow::Stop {
                        return Ok(());
                    }
                }
                Err(KafkaError::PartitionEOF(partition)) => {
                    self.last_progress = Instant::now();
                    if !self.options.follow {
                        self.pending.remove(&partition);
                    }
                }
                Err(error) => return Err(map_kafka_error(error)),
            }
        }
    }

    fn count_reached(&self) -> bool {
        self.options
            .count
            .is_some_and(|count| self.delivered >= count)
    }

    fn stall_error(&self, subscribed: bool) -> CoreError {
        let topic = self.options.topic.as_str();
        let seconds = self.options.timeout.as_secs();
        if subscribed && !self.assignment_ready {
            CoreError::Kafka(format!(
                "no partition assigned within {seconds} seconds while reading topic '{topic}'"
            ))
        } else {
            CoreError::Kafka(format!(
                "no progress for {seconds} seconds while reading topic '{topic}'"
            ))
        }
    }

    /// Gives a target to every partition of the current group assignment that does not have one yet.
    fn register_assignment(&mut self, consumer: &ConsumeConsumer) -> CoreResult<()> {
        let assignment = consumer.assignment().map_err(map_kafka_error)?;
        if assignment.count() == 0 {
            return Ok(());
        }
        self.assignment_ready = true;
        for element in assignment.elements() {
            if !self.registered.contains(&element.partition()) {
                self.register_partition(consumer, element.partition())?;
            }
        }
        Ok(())
    }

    fn register_partition(&mut self, consumer: &ConsumeConsumer, partition: i32) -> CoreResult<()> {
        self.registered.insert(partition);
        let (low, high) = consumer
            .fetch_watermarks(&self.options.topic, partition, self.options.timeout)
            .map_err(map_kafka_error)?;
        if low < high {
            self.pending.insert(partition, high);
        }
        Ok(())
    }

    fn handle_message(
        &mut self,
        consumer: &ConsumeConsumer,
        message: &BorrowedMessage<'_>,
    ) -> CoreResult<Flow> {
        if !self.options.follow
            && !self.is_before_target(consumer, message.partition(), message.offset())?
        {
            // Past the target: this run does not process the message, so the group must not store it.
            return Ok(Flow::Continue);
        }
        let headers = read_headers(message);
        if !matches_header_filters(&self.options.filters, &headers) {
            self.skipped_by_filter += 1;
            self.store_group_position(consumer, message)?;
            return Ok(Flow::Continue);
        }
        let envelope = self.build_envelope(message, &headers);
        match self.sink.accept_envelope(&envelope) {
            Ok(()) => {
                self.delivered += 1;
                self.store_group_position(consumer, message)?;
                Ok(Flow::Continue)
            }
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(Flow::Stop),
            Err(error) => Err(CoreError::Io(error)),
        }
    }

    /// Stores the offset of a message this run has processed, so the group commits up to it.
    /// Assignment runs have no group to commit for and never store.
    fn store_group_position(
        &self,
        consumer: &ConsumeConsumer,
        message: &BorrowedMessage<'_>,
    ) -> CoreResult<()> {
        if self.options.group.is_none() {
            return Ok(());
        }
        consumer
            .store_offset_from_message(message)
            .map_err(map_kafka_error)
    }

    /// Returns whether the message is below its partition's target. Reaching the target drops the partition.
    fn is_before_target(
        &mut self,
        consumer: &ConsumeConsumer,
        partition: i32,
        offset: i64,
    ) -> CoreResult<bool> {
        if !self.registered.contains(&partition) {
            self.register_partition(consumer, partition)?;
        }
        let Some(&target) = self.pending.get(&partition) else {
            return Ok(false);
        };
        if offset + 1 >= target {
            self.pending.remove(&partition);
        }
        Ok(offset < target)
    }

    fn build_envelope(
        &self,
        message: &BorrowedMessage<'_>,
        headers: &[RecordHeader],
    ) -> MessageEnvelope {
        // Kafka reports "no timestamp" as NotAvailable or as a negative value; both become null.
        let timestamp_millis = message
            .timestamp()
            .to_millis()
            .filter(|millis| *millis >= 0);
        let value = self.build_value(message.payload(), headers);
        MessageEnvelope::from_record(
            message.topic(),
            message.partition(),
            message.offset(),
            timestamp_millis,
            message.key(),
            value,
            headers,
        )
    }

    fn build_value(&self, payload: Option<&[u8]>, headers: &[RecordHeader]) -> EnvelopeValue {
        build_envelope_value(
            payload,
            headers,
            self.decoder,
            self.options.max_bytes,
            &self.options.redact,
        )
    }
}

/// Chooses what the envelope keeps of a value. With redaction on, a truncated or undecodable value
/// is withheld, because redaction cannot reach its content.
fn build_envelope_value(
    payload: Option<&[u8]>,
    headers: &[RecordHeader],
    decoder: &dyn ValueDecoder,
    max_bytes: usize,
    redact: &[String],
) -> EnvelopeValue {
    let Some(bytes) = payload else {
        return EnvelopeValue::Absent;
    };
    let redacting = !redact.is_empty();
    let withheld = || EnvelopeValue::Withheld {
        reason: WITHHELD_REASON.to_string(),
    };
    if max_bytes > 0 && bytes.len() > max_bytes {
        if redacting {
            return withheld();
        }
        return EnvelopeValue::Truncated {
            prefix: bytes.iter().take(max_bytes).copied().collect(),
        };
    }
    match decoder.decode_value(bytes, headers) {
        Ok(serde_json::Value::Null) => EnvelopeValue::JsonNull {
            bytes: bytes.to_vec(),
        },
        Ok(mut value) => {
            redact_fields(&mut value, redact);
            EnvelopeValue::Decoded(value)
        }
        Err(_) if redacting => withheld(),
        Err(error) => EnvelopeValue::Undecodable {
            bytes: bytes.to_vec(),
            error,
        },
    }
}

/// Clamps an explicit `--from offset:N` to the partition's watermarks. Returns the offset to assign
/// and the position the partition reads from.
fn clamp_explicit_offset(requested: i64, low: i64, high: i64) -> (Offset, i64) {
    if requested < low {
        (Offset::Offset(low), low)
    } else if requested > high {
        (Offset::End, high)
    } else {
        (Offset::Offset(requested), requested)
    }
}

fn create_assign_consumer(
    connection: &KafkaConnection,
    group_id: &str,
) -> CoreResult<ConsumeConsumer> {
    let mut config = apply_common_settings(connection.client_config().clone(), group_id);
    // Assignment never commits: the group id exists only because librdkafka requires one.
    config.set("enable.auto.commit", "false");
    config.set("enable.auto.offset.store", "false");
    create_consume_consumer(connection, config)
}

fn create_subscribe_consumer(
    connection: &KafkaConnection,
    group_id: &str,
    start: StartPosition,
) -> CoreResult<ConsumeConsumer> {
    let config = build_subscribe_config(connection.client_config().clone(), group_id, start);
    create_consume_consumer(connection, config)
}

/// Creates a consumer in `config` and waits for its first token when there is a token source.
fn create_consume_consumer(
    connection: &KafkaConnection,
    config: ClientConfig,
) -> CoreResult<ConsumeConsumer> {
    let consumer: ConsumeConsumer = config
        .create_with_context(ConsumeClientContext::new(connection.refresher().clone()))
        .map_err(map_kafka_error)?;
    connection.wait_for_initial_token(|timeout| {
        consumer.poll(timeout);
    })?;
    Ok(consumer)
}

fn build_subscribe_config(
    base_config: ClientConfig,
    group_id: &str,
    start: StartPosition,
) -> ClientConfig {
    let mut config = apply_common_settings(base_config, group_id);
    config.set("enable.auto.commit", "true");
    // Offsets are stored by hand after a message is delivered, so auto commit never passes an unread one.
    config.set("enable.auto.offset.store", "false");
    let reset = match start {
        StartPosition::Latest => "latest",
        _ => "earliest",
    };
    config.set("auto.offset.reset", reset);
    config
}

fn apply_common_settings(base_config: ClientConfig, group_id: &str) -> ClientConfig {
    let mut config = base_config;
    // Missing topics must never be created by a metadata fetch or a consumer.
    config.set("allow.auto.create.topics", "false");
    config.set("group.id", group_id);
    // Reaching the end of a partition is reported as an event, which ends partitions without messages.
    config.set("enable.partition.eof", "true");
    config
}

/// Resolves the partitions to read: all of them, or the requested ones after checking they exist.
fn select_partitions(consumer: &ConsumeConsumer, options: &ConsumeOptions) -> CoreResult<Vec<i32>> {
    let available = fetch_partition_identifiers(consumer, &options.topic, options.timeout)?;
    let Some(requested) = &options.partitions else {
        return Ok(available);
    };
    let mut selected = BTreeSet::new();
    for partition in requested {
        if !available.contains(partition) {
            return Err(CoreError::InvalidInput(format!(
                "partition {partition} not found in topic '{}'",
                options.topic
            )));
        }
        selected.insert(*partition);
    }
    Ok(selected.into_iter().collect())
}

/// Finds, for each partition, the first offset at or after `instant`. Partitions without one are absent.
fn resolve_time_offsets(
    consumer: &ConsumeConsumer,
    topic: &str,
    partitions: &[i32],
    instant: OffsetDateTime,
    timeout: Duration,
) -> CoreResult<BTreeMap<i32, i64>> {
    let millis = i64::try_from(instant.unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| CoreError::InvalidInput("start time is out of range".to_string()))?;
    let mut query = TopicPartitionList::new();
    for partition in partitions {
        query
            .add_partition_offset(topic, *partition, Offset::Offset(millis))
            .map_err(map_kafka_error)?;
    }
    let found = consumer
        .offsets_for_times(query, timeout)
        .map_err(map_kafka_error)?;
    Ok(found
        .elements()
        .iter()
        .filter_map(|element| match element.offset() {
            Offset::Offset(offset) => Some((element.partition(), offset)),
            _ => None,
        })
        .collect())
}

/// Copies the headers with their original bytes; a header without a value keeps `None`.
fn read_headers(message: &BorrowedMessage<'_>) -> Vec<RecordHeader> {
    message
        .headers()
        .map(|headers| {
            headers
                .iter()
                .map(|header| RecordHeader {
                    name: header.key.to_string(),
                    value: header.value.map(<[u8]>::to_vec),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn to_unix_millis(instant: SystemTime) -> i64 {
    instant
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}

fn map_kafka_error(error: KafkaError) -> CoreError {
    CoreError::Kafka(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::raw_json_codec::RawJsonCodec;

    fn options(
        start: StartPosition,
        group: Option<&str>,
        partitions: Option<Vec<i32>>,
    ) -> ConsumeOptions {
        ConsumeOptions {
            topic: "orders".to_string(),
            partitions,
            start,
            count: None,
            follow: false,
            group: group.map(str::to_string),
            max_bytes: 0,
            redact: Vec::new(),
            filters: Vec::new(),
            timeout: Duration::from_secs(1),
        }
    }

    #[test]
    fn offset_start_cannot_combine_with_group() {
        let error =
            validate_options(&options(StartPosition::Offset(3), Some("g"), None)).unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid input: --from offset:/time: cannot be combined with --group"
        );
    }

    #[test]
    fn time_start_cannot_combine_with_group() {
        let instant = OffsetDateTime::UNIX_EPOCH;
        let error =
            validate_options(&options(StartPosition::Time(instant), Some("g"), None)).unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid input: --from offset:/time: cannot be combined with --group"
        );
    }

    #[test]
    fn partitions_cannot_combine_with_group() {
        let error = validate_options(&options(StartPosition::Earliest, Some("g"), Some(vec![0])))
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid input: --partition cannot be combined with --group"
        );
    }

    #[test]
    fn group_free_options_are_accepted() {
        assert!(validate_options(&options(StartPosition::Offset(3), None, Some(vec![0]))).is_ok());
    }

    #[test]
    fn explicit_offset_below_low_watermark_starts_at_low() {
        assert_eq!(clamp_explicit_offset(2, 5, 9), (Offset::Offset(5), 5));
    }

    #[test]
    fn explicit_offset_inside_watermarks_is_kept() {
        assert_eq!(clamp_explicit_offset(7, 5, 9), (Offset::Offset(7), 7));
    }

    #[test]
    fn explicit_offset_equal_high_watermark_is_kept() {
        assert_eq!(clamp_explicit_offset(9, 5, 9), (Offset::Offset(9), 9));
    }

    #[test]
    fn explicit_offset_above_high_watermark_starts_at_end() {
        assert_eq!(clamp_explicit_offset(12, 5, 9), (Offset::End, 9));
    }

    #[test]
    fn undecodable_value_is_withheld_when_redacting() {
        let value = build_envelope_value(
            Some(b"not json"),
            &[],
            &RawJsonCodec,
            0,
            &["pw".to_string()],
        );

        assert_eq!(
            value,
            EnvelopeValue::Withheld {
                reason: WITHHELD_REASON.to_string()
            }
        );
    }

    #[test]
    fn undecodable_value_is_kept_as_bytes_without_redaction() {
        let value = build_envelope_value(Some(b"not json"), &[], &RawJsonCodec, 0, &[]);

        assert!(matches!(value, EnvelopeValue::Undecodable { .. }));
    }

    #[test]
    fn truncated_value_is_withheld_when_redacting() {
        let value = build_envelope_value(
            Some(br#"{"a":1}"#),
            &[],
            &RawJsonCodec,
            4,
            &["pw".to_string()],
        );

        assert_eq!(
            value,
            EnvelopeValue::Withheld {
                reason: WITHHELD_REASON.to_string()
            }
        );
    }

    #[test]
    fn truncated_value_is_kept_as_prefix_without_redaction() {
        let value = build_envelope_value(Some(br#"{"a":1}"#), &[], &RawJsonCodec, 4, &[]);

        assert_eq!(
            value,
            EnvelopeValue::Truncated {
                prefix: br#"{"a""#.to_vec()
            }
        );
    }

    #[test]
    fn decoded_value_is_redacted_when_redacting() {
        let value = build_envelope_value(
            Some(br#"{"pw":"x"}"#),
            &[],
            &RawJsonCodec,
            0,
            &["pw".to_string()],
        );

        assert_eq!(
            value,
            EnvelopeValue::Decoded(serde_json::json!({"pw": "[redacted]"}))
        );
    }

    #[test]
    fn group_run_commits_only_offsets_stored_by_hand() {
        let config = build_subscribe_config(ClientConfig::new(), "g", StartPosition::Earliest);

        assert_eq!(config.get("enable.auto.commit"), Some("true"));
        assert_eq!(config.get("enable.auto.offset.store"), Some("false"));
        assert_eq!(config.get("allow.auto.create.topics"), Some("false"));
    }

    #[test]
    fn json_null_payload_is_kept_as_its_bytes() {
        let value = build_envelope_value(Some(b"null"), &[], &RawJsonCodec, 0, &[]);

        assert_eq!(
            value,
            EnvelopeValue::JsonNull {
                bytes: b"null".to_vec()
            }
        );
    }

    #[test]
    fn unix_millis_of_epoch_is_zero() {
        assert_eq!(to_unix_millis(UNIX_EPOCH), 0);
    }
}
