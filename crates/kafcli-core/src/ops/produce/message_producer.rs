use crate::capability::broker_capabilities::{detect_broker_family, resolve_producer_tuning};
use crate::codec::value_codec::ValueEncoder;
use crate::config::model::ConnectionContext;
use crate::connection::client_config_builder::{ConnectionSettings, build_client_config};
use crate::error::{CoreError, CoreResult};
use crate::message::binary_text::decode_base64;
use crate::message::message_envelope::MessageEnvelope;
use crate::message::record_header::RecordHeader;
use crate::message::timestamp_text::format_rfc3339_millis;
use crate::ops::produce::produce_input::ProduceInput;
use crate::ops::produce::rate_limiter::RateLimiter;
use crate::secret::resolver::SecretResolver;
use crate::template::{
    EnvironmentLookup, TemplateState, render_text_template, render_value_templates,
};
use rdkafka::ClientConfig;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{DeliveryResult, Header, Message, OwnedHeaders, Timestamp};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer, ProducerContext};
use serde::Serialize;
use serde_json::Value;
use std::borrow::Cow;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{Duration, Instant};
use time::OffsetDateTime;

/// Most messages one run may prepare, counting every repetition.
const MAXIMUM_MESSAGES: u64 = 10_000_000;
const QUEUE_BUFFERING_MAX_MESSAGES: &str = "100000";
const QUEUE_FULL_POLL: Duration = Duration::from_millis(100);
const MAXIMUM_REPORTED_DELIVERIES: usize = 1_000;
/// Smallest message timeout given to librdkafka, so a short `--timeout` still allows retries.
const MINIMUM_MESSAGE_TIMEOUT: Duration = Duration::from_secs(10);
/// librdkafka reads `message.timeout.ms` as a C `int`.
const MAXIMUM_LIBRDKAFKA_TIMEOUT_MILLIS: u64 = i32::MAX as u64;
/// Largest total of key, value and header bytes prepared for one run.
pub const MAX_PREPARED_BYTES: usize = 1024 * 1024 * 1024;

/// What to produce and how: target topic, templates, count, pacing and the timeout for each network step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceOptions {
    pub topic: String,
    pub key_template: Option<String>,
    pub header_templates: Vec<(String, String)>,
    pub partition: Option<i32>,
    pub count: u64,
    pub rate_per_second: Option<u32>,
    pub render_templates: bool,
    pub timeout: Duration,
}

/// A message ready for the wire. `value: None` is a tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedMessage {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<RecordHeader>,
    pub partition: Option<i32>,
}

/// Where one acknowledged message landed. `timestamp` is `None` when the broker gave no usable time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeliveryReport {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<String>,
}

/// Outcome of one produce run. `failed` counts every message not acknowledged by the broker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProduceReport {
    pub topic: String,
    pub sent: u64,
    pub failed: u64,
    pub deliveries: Vec<DeliveryReport>,
    pub deliveries_truncated: bool,
    pub first_error: Option<String>,
}

/// Expands the inputs into the messages to send: the whole input list is repeated `count` times.
///
/// Each message is rendered with the current sequence, encoded, and only then does the sequence
/// advance. Envelope inputs are not template-rendered, except that `--key` and `--header` apply to them.
pub fn prepare_messages(
    inputs: &[ProduceInput],
    options: &ProduceOptions,
    encoder: &dyn ValueEncoder,
    environment: &dyn EnvironmentLookup,
    now: OffsetDateTime,
) -> CoreResult<Vec<PreparedMessage>> {
    prepare_messages_with_byte_limit(
        inputs,
        options,
        encoder,
        environment,
        now,
        MAX_PREPARED_BYTES,
    )
}

/// `prepare_messages` with the byte cap passed in, so tests can use a small limit.
fn prepare_messages_with_byte_limit(
    inputs: &[ProduceInput],
    options: &ProduceOptions,
    encoder: &dyn ValueEncoder,
    environment: &dyn EnvironmentLookup,
    now: OffsetDateTime,
    byte_limit: usize,
) -> CoreResult<Vec<PreparedMessage>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }
    let input_count = u64::try_from(inputs.len()).unwrap_or(u64::MAX);
    let total = options.count.saturating_mul(input_count);
    if total > MAXIMUM_MESSAGES {
        return Err(CoreError::InvalidInput(format!(
            "too many messages: {total} (limit {MAXIMUM_MESSAGES})"
        )));
    }

    let mut preparer = MessagePreparer {
        options,
        encoder,
        environment,
        now,
        state: TemplateState::new(),
    };
    let mut prepared = Vec::new();
    let mut prepared_bytes: usize = 0;
    for _ in 0..options.count {
        for input in inputs {
            let message = preparer.prepare(input)?;
            prepared_bytes = prepared_bytes.saturating_add(prepared_message_bytes(&message));
            if prepared_bytes > byte_limit {
                return Err(CoreError::InvalidInput(
                    "prepared messages exceed 1 GiB".to_string(),
                ));
            }
            prepared.push(message);
            preparer.state.advance();
        }
    }
    Ok(prepared)
}

/// Bytes a prepared message holds: key, value, and header names and values.
fn prepared_message_bytes(message: &PreparedMessage) -> usize {
    let header_bytes: usize = message
        .headers
        .iter()
        .map(|header| {
            header
                .name
                .len()
                .saturating_add(header.value.as_ref().map_or(0, Vec::len))
        })
        .fold(0, usize::saturating_add);
    message
        .key
        .as_ref()
        .map_or(0, Vec::len)
        .saturating_add(message.value.as_ref().map_or(0, Vec::len))
        .saturating_add(header_bytes)
}

/// Sends the prepared messages to `options.topic` and waits for the broker's acknowledgements.
///
/// The topic is checked in cluster metadata before anything is sent. A full local queue is retried
/// after polling; messages not acknowledged by the final flush count as failed.
pub fn produce_messages(
    context: &ConnectionContext,
    resolver: &dyn SecretResolver,
    settings: &ConnectionSettings,
    options: &ProduceOptions,
    messages: Vec<PreparedMessage>,
) -> CoreResult<ProduceReport> {
    let tuning = resolve_producer_tuning(detect_broker_family(context));
    let base_config = build_client_config(context, resolver, settings)?;
    ensure_topic_exists(&base_config, &options.topic, options.timeout)?;

    let (sender, receiver) = mpsc::channel();
    let producer: BaseProducer<ProduceDeliveryContext> =
        build_producer_config(&base_config, tuning.compression_disabled, options.timeout)
            .create_with_context(ProduceDeliveryContext { deliveries: sender })
            .map_err(map_kafka_error)?;

    let mut tally = DeliveryTally::default();
    let mut progress = ProgressClock::new(Instant::now(), options.timeout);
    let mut limiter = options.rate_per_second.map(RateLimiter::new);
    for message in &messages {
        if let Some(limiter) = limiter.as_mut() {
            wait_for_slot(&producer, limiter);
        }
        let queued = enqueue_message(
            &producer,
            &options.topic,
            message,
            &mut tally,
            &receiver,
            &mut progress,
        );
        if !queued {
            // Remaining messages are left unsent and count as failed.
            break;
        }
        producer.poll(Duration::ZERO);
        if tally.collect(&receiver) > 0 {
            progress.mark(Instant::now());
        }
    }

    if let Err(error) = producer.flush(options.timeout) {
        tally.note_error(&error.to_string());
    }
    tally.collect(&receiver);
    let total = u64::try_from(messages.len()).unwrap_or(u64::MAX);
    Ok(tally.into_report(options.topic.clone(), total))
}

/// Turns inputs into messages for one run, holding the sequence state for it.
struct MessagePreparer<'a> {
    options: &'a ProduceOptions,
    encoder: &'a dyn ValueEncoder,
    environment: &'a dyn EnvironmentLookup,
    now: OffsetDateTime,
    state: TemplateState,
}

impl MessagePreparer<'_> {
    fn prepare(&self, input: &ProduceInput) -> CoreResult<PreparedMessage> {
        match input {
            ProduceInput::Payload { value, .. } => self.prepare_payload(value),
            ProduceInput::Envelope { envelope, line } => self.prepare_envelope(envelope, *line),
        }
    }

    fn prepare_payload(&self, value: &Value) -> CoreResult<PreparedMessage> {
        let payload: Cow<'_, Value> = if self.options.render_templates {
            Cow::Owned(render_value_templates(
                value,
                &self.state,
                self.environment,
                self.now,
            )?)
        } else {
            Cow::Borrowed(value)
        };
        let fields: &Value = &payload;
        let encoded = self.encoder.encode_value(fields)?;
        let key = self.render_key(Some(fields))?;
        let mut headers = self.render_headers(Some(fields))?;
        headers.extend(encoded.headers);
        Ok(PreparedMessage {
            key,
            value: Some(encoded.bytes),
            headers,
            partition: self.options.partition,
        })
    }

    fn prepare_envelope(
        &self,
        envelope: &MessageEnvelope,
        line: usize,
    ) -> CoreResult<PreparedMessage> {
        let resolved = self.resolve_envelope_value(envelope, line)?;
        let fields = resolved.fields;
        let key = if self.options.key_template.is_some() {
            self.render_key(fields)?
        } else {
            envelope.key_bytes()?
        };
        let mut headers = envelope.record_headers()?;
        headers.extend(self.render_headers(fields)?);
        headers.extend(resolved.encoder_headers);
        Ok(PreparedMessage {
            key,
            value: resolved.bytes,
            headers,
            partition: envelope.partition.or(self.options.partition),
        })
    }

    /// Decides the bytes to send for an envelope and the JSON that `{{field:...}}` reads from.
    ///
    /// `value_base64` wins over `value` and is sent verbatim, so it has no JSON to read fields from.
    fn resolve_envelope_value<'e>(
        &self,
        envelope: &'e MessageEnvelope,
        line: usize,
    ) -> CoreResult<ResolvedEnvelopeValue<'e>> {
        if let Some(encoded) = &envelope.value_base64 {
            let bytes = decode_base64(encoded).map_err(|_| {
                CoreError::InvalidInput(format!(
                    "envelope line {line}: value_base64 is not valid base64"
                ))
            })?;
            return Ok(ResolvedEnvelopeValue {
                bytes: Some(bytes),
                encoder_headers: Vec::new(),
                fields: None,
            });
        }
        match &envelope.value {
            Some(value) => {
                let encoded = self.encoder.encode_value(value)?;
                Ok(ResolvedEnvelopeValue {
                    bytes: Some(encoded.bytes),
                    encoder_headers: encoded.headers,
                    fields: Some(value),
                })
            }
            None => Ok(ResolvedEnvelopeValue {
                bytes: None,
                encoder_headers: Vec::new(),
                fields: None,
            }),
        }
    }

    fn render_key(&self, fields: Option<&Value>) -> CoreResult<Option<Vec<u8>>> {
        self.options
            .key_template
            .as_deref()
            .map(|template| self.render_text(template, fields))
            .transpose()
    }

    fn render_headers(&self, fields: Option<&Value>) -> CoreResult<Vec<RecordHeader>> {
        self.options
            .header_templates
            .iter()
            .map(|(name, template)| -> CoreResult<RecordHeader> {
                Ok(RecordHeader {
                    name: name.clone(),
                    value: Some(self.render_text(template, fields)?),
                })
            })
            .collect()
    }

    /// Renders a key or header value when templates are on; otherwise the text is taken literally.
    fn render_text(&self, text: &str, fields: Option<&Value>) -> CoreResult<Vec<u8>> {
        if !self.options.render_templates {
            return Ok(text.as_bytes().to_vec());
        }
        render_text_template(text, &self.state, self.environment, self.now, fields)
            .map(String::into_bytes)
    }
}

/// What an envelope contributes to the wire message before its templates are applied.
struct ResolvedEnvelopeValue<'e> {
    bytes: Option<Vec<u8>>,
    encoder_headers: Vec<RecordHeader>,
    fields: Option<&'e Value>,
}

/// Forwards each delivery report to the producer loop, which reads them after polling.
struct ProduceDeliveryContext {
    deliveries: Sender<Result<DeliveryReport, String>>,
}

impl ClientContext for ProduceDeliveryContext {}

impl ProducerContext for ProduceDeliveryContext {
    type DeliveryOpaque = ();

    fn delivery(&self, delivery_result: &DeliveryResult<'_>, _delivery_opaque: ()) {
        let outcome = match delivery_result {
            Ok(message) => Ok(DeliveryReport {
                partition: message.partition(),
                offset: message.offset(),
                timestamp: format_delivery_timestamp(message.timestamp()),
            }),
            Err((error, _message)) => Err(error.to_string()),
        };
        // The receiver lives until `produce_messages` returns, so a failed send is harmless.
        let _ = self.deliveries.send(outcome);
    }
}

/// Collects delivery outcomes while the run is in progress and builds the final report.
#[derive(Default)]
struct DeliveryTally {
    sent: u64,
    deliveries: Vec<DeliveryReport>,
    deliveries_truncated: bool,
    first_error: Option<String>,
}

impl DeliveryTally {
    /// Records every outcome waiting in the channel and returns how many were acknowledged.
    fn collect(&mut self, receiver: &Receiver<Result<DeliveryReport, String>>) -> usize {
        self.record_outcomes(receiver.try_iter())
    }

    /// Records each outcome and returns how many were acknowledged deliveries.
    ///
    /// Failed deliveries do not count, so a dead broker's error reports cannot look like progress.
    fn record_outcomes(
        &mut self,
        outcomes: impl IntoIterator<Item = Result<DeliveryReport, String>>,
    ) -> usize {
        let mut acknowledged = 0;
        for outcome in outcomes {
            match outcome {
                Ok(report) => {
                    acknowledged += 1;
                    self.record_delivery(report);
                }
                Err(message) => self.note_error(&message),
            }
        }
        acknowledged
    }

    fn record_delivery(&mut self, report: DeliveryReport) {
        self.sent = self.sent.saturating_add(1);
        if self.deliveries.len() < MAXIMUM_REPORTED_DELIVERIES {
            self.deliveries.push(report);
        } else {
            self.deliveries_truncated = true;
        }
    }

    fn note_error(&mut self, message: &str) {
        if self.first_error.is_none() {
            self.first_error = Some(neutralize_text(message));
        }
    }

    fn into_report(self, topic: String, total: u64) -> ProduceReport {
        ProduceReport {
            topic,
            sent: self.sent,
            failed: total.saturating_sub(self.sent),
            deliveries: self.deliveries,
            deliveries_truncated: self.deliveries_truncated,
            first_error: self.first_error,
        }
    }
}

fn ensure_topic_exists(
    base_config: &ClientConfig,
    topic: &str,
    timeout: Duration,
) -> CoreResult<()> {
    let mut config = base_config.clone();
    config.set("allow.auto.create.topics", "false");
    let consumer: BaseConsumer = config.create().map_err(map_kafka_error)?;
    let metadata = consumer
        .fetch_metadata(Some(topic), timeout)
        .map_err(map_kafka_error)?;
    let found = metadata
        .topics()
        .iter()
        .any(|candidate| candidate.name() == topic && candidate.error().is_none());
    if found {
        Ok(())
    } else {
        Err(CoreError::TopicNotFound(topic.to_string()))
    }
}

fn build_producer_config(
    base_config: &ClientConfig,
    compression_disabled: bool,
    timeout: Duration,
) -> ClientConfig {
    let mut config = base_config.clone();
    // Missing topics must never be created by a producer.
    config.set("allow.auto.create.topics", "false");
    config.set("enable.idempotence", "true");
    config.set("acks", "all");
    config.set("queue.buffering.max.messages", QUEUE_BUFFERING_MAX_MESSAGES);
    config.set(
        "message.timeout.ms",
        message_timeout_millis(timeout).to_string(),
    );
    if compression_disabled {
        config.set("compression.type", "none");
    }
    config
}

/// Delivery timeout for librdkafka: the run timeout, but never below the floor and never above `i32::MAX` ms.
fn message_timeout_millis(timeout: Duration) -> u64 {
    let millis =
        u64::try_from(timeout.max(MINIMUM_MESSAGE_TIMEOUT).as_millis()).unwrap_or(u64::MAX);
    millis.min(MAXIMUM_LIBRDKAFKA_TIMEOUT_MILLIS)
}

/// Tracks the last moment the run made progress: a send was queued or a delivery arrived.
struct ProgressClock {
    last_progress: Instant,
    timeout: Duration,
}

impl ProgressClock {
    fn new(now: Instant, timeout: Duration) -> Self {
        Self {
            last_progress: now,
            timeout,
        }
    }

    fn mark(&mut self, now: Instant) {
        self.last_progress = now;
    }

    /// Time without progress when it exceeds the timeout, otherwise `None`.
    fn stalled_for(&self, now: Instant) -> Option<Duration> {
        stalled_duration(self.last_progress, now, self.timeout)
    }
}

/// Returns how long the run has gone without progress, but only when that exceeds `timeout`.
fn stalled_duration(last_progress: Instant, now: Instant, timeout: Duration) -> Option<Duration> {
    let idle = now.saturating_duration_since(last_progress);
    (idle > timeout).then_some(idle)
}

/// Explains a stall in whole seconds for the report.
fn stall_message(stalled_for: Duration) -> String {
    format!(
        "queue stayed full for {} seconds without progress",
        stalled_for.as_secs()
    )
}

/// Waits for the limiter's next slot by polling the producer, so delivery reports keep flowing.
fn wait_for_slot(producer: &BaseProducer<ProduceDeliveryContext>, limiter: &mut RateLimiter) {
    let now = Instant::now();
    let slot_opens_at = now + limiter.wait_for_next_slot(now);
    loop {
        let current = Instant::now();
        if current >= slot_opens_at {
            return;
        }
        producer.poll(slot_opens_at.saturating_duration_since(current));
    }
}

/// Queues one message. A full local queue is retried with the same record after polling.
///
/// Returns `false` when the queue stayed full for longer than the run timeout without any progress;
/// the caller then stops enqueuing.
fn enqueue_message(
    producer: &BaseProducer<ProduceDeliveryContext>,
    topic: &str,
    message: &PreparedMessage,
    tally: &mut DeliveryTally,
    receiver: &Receiver<Result<DeliveryReport, String>>,
    progress: &mut ProgressClock,
) -> bool {
    let mut record = build_record(topic, message);
    loop {
        match producer.send(record) {
            Ok(()) => {
                progress.mark(Instant::now());
                return true;
            }
            Err((error, returned))
                if error.rdkafka_error_code() == Some(RDKafkaErrorCode::QueueFull) =>
            {
                if let Some(stalled_for) = progress.stalled_for(Instant::now()) {
                    tally.note_error(&stall_message(stalled_for));
                    return false;
                }
                producer.poll(QUEUE_FULL_POLL);
                if tally.collect(receiver) > 0 {
                    progress.mark(Instant::now());
                }
                record = returned;
            }
            Err((error, _returned)) => {
                tally.note_error(&error.to_string());
                return true;
            }
        }
    }
}

fn build_record<'a>(topic: &'a str, message: &'a PreparedMessage) -> BaseRecord<'a, [u8], [u8]> {
    let mut record: BaseRecord<'a, [u8], [u8]> = BaseRecord::to(topic);
    if let Some(key) = message.key.as_deref() {
        record = record.key(key);
    }
    if let Some(value) = message.value.as_deref() {
        record = record.payload(value);
    }
    if let Some(partition) = message.partition {
        record = record.partition(partition);
    }
    record.headers(build_headers(&message.headers))
}

fn build_headers(headers: &[RecordHeader]) -> OwnedHeaders {
    headers.iter().fold(
        OwnedHeaders::new_with_capacity(headers.len()),
        |owned, header| {
            owned.insert(Header {
                key: header.name.as_str(),
                value: header.value.as_deref(),
            })
        },
    )
}

/// Formats a broker timestamp; `None` when the broker gave none or it is before the epoch.
fn format_delivery_timestamp(timestamp: Timestamp) -> Option<String> {
    let millis = timestamp.to_millis()?;
    if millis < 0 {
        return None;
    }
    let instant = OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000).ok()?;
    format_rfc3339_millis(instant)
}

/// Replaces control characters so a broker or client message cannot disturb the terminal.
fn neutralize_text(text: &str) -> String {
    text.chars()
        .map(|character| {
            if character.is_control() {
                char::REPLACEMENT_CHARACTER
            } else {
                character
            }
        })
        .collect()
}

fn map_kafka_error(error: KafkaError) -> CoreError {
    CoreError::Kafka(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::raw_json_codec::RawJsonCodec;
    use crate::message::message_envelope::parse_envelope_line;
    use crate::template::ProcessEnvironment;
    use serde_json::json;

    fn options(count: u64) -> ProduceOptions {
        ProduceOptions {
            topic: "orders".to_string(),
            key_template: None,
            header_templates: Vec::new(),
            partition: None,
            count,
            rate_per_second: None,
            render_templates: true,
            timeout: Duration::from_secs(5),
        }
    }

    fn payload(value: Value, line: usize) -> ProduceInput {
        ProduceInput::Payload { value, line }
    }

    fn envelope(line_text: &str, line: usize) -> ProduceInput {
        ProduceInput::Envelope {
            envelope: parse_envelope_line(line_text).expect("test envelope parses"),
            line,
        }
    }

    fn prepare(
        inputs: &[ProduceInput],
        options: &ProduceOptions,
    ) -> CoreResult<Vec<PreparedMessage>> {
        prepare_messages(
            inputs,
            options,
            &RawJsonCodec,
            &ProcessEnvironment,
            OffsetDateTime::UNIX_EPOCH,
        )
    }

    fn values(messages: &[PreparedMessage]) -> Vec<Option<String>> {
        messages
            .iter()
            .map(|message| {
                message
                    .value
                    .as_ref()
                    .map(|bytes| String::from_utf8_lossy(bytes).to_string())
            })
            .collect()
    }

    #[test]
    fn count_repeats_whole_input_list() -> CoreResult<()> {
        let inputs = vec![payload(json!({"n": 1}), 1), payload(json!({"n": 2}), 2)];

        let messages = prepare(&inputs, &options(2))?;

        assert_eq!(
            values(&messages),
            vec![
                Some("{\"n\":1}".to_string()),
                Some("{\"n\":2}".to_string()),
                Some("{\"n\":1}".to_string()),
                Some("{\"n\":2}".to_string()),
            ]
        );
        Ok(())
    }

    #[test]
    fn sequence_advances_per_message() -> CoreResult<()> {
        let inputs = vec![payload(json!({"seq": "{{seq}}"}), 1)];

        let messages = prepare(&inputs, &options(3))?;

        assert_eq!(
            values(&messages),
            vec![
                Some("{\"seq\":1}".to_string()),
                Some("{\"seq\":2}".to_string()),
                Some("{\"seq\":3}".to_string()),
            ]
        );
        Ok(())
    }

    #[test]
    fn envelope_value_base64_is_sent_verbatim() -> CoreResult<()> {
        let inputs = vec![
            envelope(r#"{"kafcli_envelope":1,"value_base64":"/wA="}"#, 1),
            envelope(
                r#"{"kafcli_envelope":1,"value":{"a":"{{seq}}"},"value_base64":"/wA="}"#,
                2,
            ),
        ];

        let messages = prepare(&inputs, &options(1))?;

        assert_eq!(messages[0].value, Some(vec![0xff, 0x00]));
        assert_eq!(messages[1].value, Some(vec![0xff, 0x00]));
        assert!(messages.iter().all(|message| message.headers.is_empty()));
        Ok(())
    }

    #[test]
    fn envelope_json_null_with_base64_sends_null_bytes_not_tombstone() -> CoreResult<()> {
        let inputs = vec![envelope(
            r#"{"kafcli_envelope":1,"key":"k","value":null,"value_base64":"bnVsbA=="}"#,
            1,
        )];

        let messages = prepare(&inputs, &options(1))?;

        assert_eq!(messages[0].value, Some(b"null".to_vec()));
        Ok(())
    }

    #[test]
    fn envelope_without_value_is_tombstone() -> CoreResult<()> {
        let inputs = vec![envelope(r#"{"kafcli_envelope":1,"key":"k"}"#, 1)];

        let messages = prepare(&inputs, &options(1))?;

        assert_eq!(messages[0].value, None);
        assert_eq!(messages[0].key, Some(b"k".to_vec()));
        Ok(())
    }

    #[test]
    fn envelope_partition_overrides_option() -> CoreResult<()> {
        let inputs = vec![
            envelope(r#"{"kafcli_envelope":1,"partition":2,"value":1}"#, 1),
            envelope(r#"{"kafcli_envelope":1,"value":1}"#, 2),
        ];
        let mut produce_options = options(1);
        produce_options.partition = Some(5);

        let messages = prepare(&inputs, &produce_options)?;

        assert_eq!(messages[0].partition, Some(2));
        assert_eq!(messages[1].partition, Some(5));
        Ok(())
    }

    #[test]
    fn message_limit_is_enforced() {
        let inputs = vec![payload(json!({}), 1)];

        let message = match prepare(&inputs, &options(10_000_001)) {
            Err(error) => error.to_string(),
            Ok(messages) => format!("no error, {} messages", messages.len()),
        };

        assert_eq!(
            message,
            "invalid input: too many messages: 10000001 (limit 10000000)"
        );
    }

    #[test]
    fn key_and_header_templates_render_from_payload() -> CoreResult<()> {
        let inputs = vec![payload(json!({"id": "abc"}), 1)];
        let mut produce_options = options(1);
        produce_options.key_template = Some("{{field:id}}".to_string());
        produce_options.header_templates = vec![("origin".to_string(), "run-{{seq}}".to_string())];

        let messages = prepare(&inputs, &produce_options)?;

        assert_eq!(messages[0].key, Some(b"abc".to_vec()));
        assert_eq!(
            messages[0].headers,
            vec![RecordHeader {
                name: "origin".to_string(),
                value: Some(b"run-1".to_vec()),
            }]
        );
        Ok(())
    }

    #[test]
    fn templates_off_sends_text_literally() -> CoreResult<()> {
        let inputs = vec![payload(json!({"id": "{{seq}}"}), 1)];
        let mut produce_options = options(1);
        produce_options.render_templates = false;
        produce_options.key_template = Some("{{seq}}".to_string());

        let messages = prepare(&inputs, &produce_options)?;

        assert_eq!(messages[0].key, Some(b"{{seq}}".to_vec()));
        assert_eq!(
            values(&messages),
            vec![Some("{\"id\":\"{{seq}}\"}".to_string())]
        );
        Ok(())
    }

    #[test]
    fn message_timeout_has_floor_and_follows_run_timeout() {
        let short = build_producer_config(&ClientConfig::new(), false, Duration::from_secs(2));
        let long = build_producer_config(&ClientConfig::new(), false, Duration::from_secs(30));

        assert_eq!(short.get("message.timeout.ms"), Some("10000"));
        assert_eq!(long.get("message.timeout.ms"), Some("30000"));
    }

    #[test]
    fn message_timeout_is_capped_at_librdkafka_int_range() {
        assert_eq!(
            message_timeout_millis(Duration::from_secs(u64::MAX / 2)),
            MAXIMUM_LIBRDKAFKA_TIMEOUT_MILLIS
        );
    }

    #[test]
    fn stall_guard_fires_only_after_timeout() {
        let start = Instant::now();
        let timeout = Duration::from_secs(2);

        assert_eq!(stalled_duration(start, start + timeout, timeout), None);
        let stalled = stalled_duration(start, start + timeout + Duration::from_millis(1), timeout);
        assert_eq!(stalled.map(|duration| duration.as_secs()), Some(2));
    }

    #[test]
    fn progress_resets_the_stall_clock() {
        let start = Instant::now();
        let mut clock = ProgressClock::new(start, Duration::from_secs(2));

        clock.mark(start + Duration::from_secs(5));

        assert_eq!(clock.stalled_for(start + Duration::from_secs(6)), None);
        assert!(clock.stalled_for(start + Duration::from_secs(8)).is_some());
    }

    #[test]
    fn stall_message_names_whole_seconds() {
        assert_eq!(
            stall_message(Duration::from_millis(3_900)),
            "queue stayed full for 3 seconds without progress"
        );
    }

    #[test]
    fn prepared_bytes_limit_is_enforced() -> CoreResult<()> {
        let inputs = vec![payload(json!({"n": 1}), 1)];
        // Each message is `{"n":1}`, 7 bytes, so two messages need 14 bytes.
        let prepare_with_limit = |limit: usize| {
            prepare_messages_with_byte_limit(
                &inputs,
                &options(2),
                &RawJsonCodec,
                &ProcessEnvironment,
                OffsetDateTime::UNIX_EPOCH,
                limit,
            )
        };

        assert_eq!(prepare_with_limit(14)?.len(), 2);
        let message = match prepare_with_limit(13) {
            Err(error) => error.to_string(),
            Ok(messages) => format!("no error, {} messages", messages.len()),
        };
        assert_eq!(message, "invalid input: prepared messages exceed 1 GiB");
        Ok(())
    }

    #[test]
    fn failed_deliveries_do_not_count_as_progress() {
        let report = DeliveryReport {
            partition: 0,
            offset: 0,
            timestamp: None,
        };
        let mut tally = DeliveryTally::default();

        let failures_only = tally.record_outcomes(vec![
            Err("Message timed out".to_string()),
            Err("Message timed out".to_string()),
        ]);
        assert_eq!(failures_only, 0);
        assert_eq!(tally.sent, 0);

        let mixed = tally.record_outcomes(vec![Err("Message timed out".to_string()), Ok(report)]);
        assert_eq!(mixed, 1);
        assert_eq!(tally.sent, 1);
    }

    #[test]
    fn delivery_timestamp_is_absent_for_negative_millis() {
        assert_eq!(format_delivery_timestamp(Timestamp::NotAvailable), None);
        assert_eq!(format_delivery_timestamp(Timestamp::CreateTime(-1)), None);
        assert_eq!(
            format_delivery_timestamp(Timestamp::CreateTime(1_794_875_047_253)),
            Some("2026-11-17T00:24:07.253Z".to_string())
        );
    }

    #[test]
    fn neutralize_replaces_control_characters() {
        assert_eq!(neutralize_text("a\u{1b}[2Jb\nc"), "a\u{FFFD}[2Jb\u{FFFD}c");
    }
}
