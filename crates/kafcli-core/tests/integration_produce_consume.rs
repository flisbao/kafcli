#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Produce and consume tests against the Redpanda plaintext broker of `tests/fixtures/compose.yaml`.
//!
//! Run with: `docker compose -f tests/fixtures/compose.yaml up -d --wait redpanda-plaintext` and then
//! `cargo test -p kafcli-core --test integration_produce_consume -- --ignored`

use kafcli_core::codec::raw_json_codec::RawJsonCodec;
use kafcli_core::config::model::{AuthConfig, ConnectionContext};
use kafcli_core::connection::client_config_builder::ConnectionSettings;
use kafcli_core::connection::kafka_connection::build_kafka_connection;
use kafcli_core::error::{CoreError, CoreResult};
use kafcli_core::message::binary_text::decode_base64;
use kafcli_core::message::message_envelope::MessageEnvelope;
use kafcli_core::message::record_header::RecordHeader;
use kafcli_core::ops::consume::message_consumer::{
    ConsumeOptions, ConsumeSummary, EnvelopeSink, consume_messages,
};
use kafcli_core::ops::consume::message_filter::HeaderFilter;
use kafcli_core::ops::consume::start_position::StartPosition;
use kafcli_core::ops::produce::message_producer::{
    PreparedMessage, ProduceOptions, ProduceReport, prepare_messages, produce_messages,
};
use kafcli_core::ops::produce::produce_input::{InputMode, InputSource, read_produce_inputs};
use kafcli_core::secret::fixed_resolver::FixedSecretResolver;
use kafcli_core::template::ProcessEnvironment;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::collections::BTreeMap;
use std::io::{self, Cursor};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;

const PLAINTEXT_BROKER: &str = "localhost:19092";
const PRODUCE_TIMEOUT: Duration = Duration::from_secs(15);
const CONSUME_TIMEOUT: Duration = Duration::from_secs(10);

fn compose_file() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/compose.yaml")
}

fn run_rpk(arguments: &[&str]) -> String {
    let output = Command::new("docker")
        .args(["compose", "-f"])
        .arg(compose_file())
        .args(["exec", "-T", "redpanda-plaintext", "rpk"])
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "rpk {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn unique_name(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{prefix}-{millis}")
}

/// Deletes its topic when dropped, so a failing assertion does not leave the topic behind.
struct TopicGuard {
    name: String,
}

impl TopicGuard {
    fn create(name: &str) -> Self {
        run_rpk(&["topic", "create", name, "-p", "2"]);
        Self {
            name: name.to_string(),
        }
    }
}

impl Drop for TopicGuard {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["compose", "-f"])
            .arg(compose_file())
            .args(["exec", "-T", "redpanda-plaintext", "rpk", "topic", "delete"])
            .arg(&self.name)
            .output();
    }
}

fn plaintext_context() -> ConnectionContext {
    ConnectionContext {
        brokers: vec![PLAINTEXT_BROKER.to_string()],
        auth: AuthConfig::Plaintext,
        protected: false,
        schema_registry: None,
    }
}

fn produce_options(topic: &str) -> ProduceOptions {
    ProduceOptions {
        topic: topic.to_string(),
        key_template: None,
        header_templates: Vec::new(),
        partition: None,
        count: 1,
        rate_per_second: None,
        render_templates: true,
        timeout: PRODUCE_TIMEOUT,
    }
}

/// Reads three JSON documents from NDJSON text, prepares them and produces them to `topic`.
fn produce_three_messages(topic: &str) -> CoreResult<ProduceReport> {
    let text = r#"{"id":"{{seq}}","kind":"a"}
{"id":"{{seq}}","kind":"b"}
{"id":"{{seq}}","kind":"c"}"#;
    let inputs = read_produce_inputs(
        &InputSource::Value(text.to_string()),
        InputMode::Json,
        &mut Cursor::new(Vec::new()),
    )?;
    let options = produce_options(topic);
    let messages = prepare_messages(
        &inputs,
        &options,
        &RawJsonCodec,
        &ProcessEnvironment,
        OffsetDateTime::UNIX_EPOCH,
    )?;
    produce_messages(
        &plaintext_context(),
        &FixedSecretResolver::returning("unused"),
        &ConnectionSettings { insecure: false },
        &options,
        messages,
    )
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn produces_messages_and_reports_offsets() {
    let topic = unique_name("kafcli-produce-it");
    let _guard = TopicGuard::create(&topic);

    let report = produce_three_messages(&topic).unwrap();

    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    assert_eq!(report.sent, 3);
    assert_eq!(report.failed, 0);
    assert_eq!(report.deliveries.len(), 3);
    assert_eq!(report.first_error, None);
    let mut offsets_by_partition: BTreeMap<i32, Vec<i64>> = BTreeMap::new();
    for delivery in &report.deliveries {
        offsets_by_partition
            .entry(delivery.partition)
            .or_default()
            .push(delivery.offset);
    }
    for (partition, offsets) in &offsets_by_partition {
        let mut sorted = offsets.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            &sorted, offsets,
            "offsets not increasing in partition {partition}"
        );
    }
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn missing_topic_fails_before_sending() {
    let topic = unique_name("kafcli-produce-missing");

    match produce_three_messages(&topic) {
        Err(CoreError::TopicNotFound(name)) => assert_eq!(name, topic),
        other => panic!("expected TopicNotFound, got {other:?}"),
    }

    let listing = run_rpk(&["topic", "list"]);
    assert!(!listing.contains(&topic), "topic was created: {listing}");
}

fn consume_options(topic: &str, start: StartPosition) -> ConsumeOptions {
    ConsumeOptions {
        topic: topic.to_string(),
        partitions: None,
        start,
        count: None,
        follow: false,
        group: None,
        max_bytes: 0,
        redact: Vec::new(),
        filters: Vec::new(),
        timeout: CONSUME_TIMEOUT,
    }
}

/// Keeps every envelope it receives, in arrival order.
#[derive(Default)]
struct CollectingSink {
    envelopes: Vec<MessageEnvelope>,
}

impl EnvelopeSink for CollectingSink {
    fn accept_envelope(&mut self, envelope: &MessageEnvelope) -> io::Result<()> {
        self.envelopes.push(envelope.clone());
        Ok(())
    }
}

fn consume_into_sink(
    options: &ConsumeOptions,
) -> CoreResult<(ConsumeSummary, Vec<MessageEnvelope>)> {
    let mut sink = CollectingSink::default();
    let summary = consume_messages(
        &plaintext_context(),
        &FixedSecretResolver::returning("unused"),
        &ConnectionSettings { insecure: false },
        options,
        &RawJsonCodec,
        &mut sink,
    )?;
    Ok((summary, sink.envelopes))
}

fn prepared(partition: i32, value: &[u8]) -> PreparedMessage {
    PreparedMessage {
        key: Some(b"key".to_vec()),
        value: Some(value.to_vec()),
        headers: Vec::new(),
        partition: Some(partition),
    }
}

fn produce_prepared(topic: &str, messages: Vec<PreparedMessage>) -> CoreResult<ProduceReport> {
    produce_messages(
        &plaintext_context(),
        &FixedSecretResolver::returning("unused"),
        &ConnectionSettings { insecure: false },
        &produce_options(topic),
        messages,
    )
}

fn json_value(index: usize) -> Vec<u8> {
    format!(r#"{{"n":{index}}}"#).into_bytes()
}

/// Produces `values` to one partition with the given create time, in milliseconds since the epoch.
fn produce_with_timestamp(topic: &str, partition: i32, timestamp_millis: i64, values: &[Vec<u8>]) {
    let connection = build_kafka_connection(
        &plaintext_context(),
        &FixedSecretResolver::returning("unused"),
        &ConnectionSettings { insecure: false },
    )
    .unwrap();
    let producer: BaseProducer = connection.client_config().clone().create().unwrap();
    for value in values {
        producer
            .send(
                BaseRecord::<(), [u8]>::to(topic)
                    .partition(partition)
                    .payload(value.as_slice())
                    .timestamp(timestamp_millis),
            )
            .map_err(|(error, _)| error)
            .unwrap();
    }
    producer.flush(PRODUCE_TIMEOUT).unwrap();
}

fn offsets_of(envelopes: &[MessageEnvelope]) -> Vec<i64> {
    envelopes
        .iter()
        .filter_map(|envelope| envelope.offset)
        .collect()
}

/// Committed offsets of `group_id` for `partitions` of `topic`, using a fresh consumer in that group.
fn committed_offsets_of(group_id: &str, topic: &str, partitions: &[i32]) -> Vec<Offset> {
    let connection = build_kafka_connection(
        &plaintext_context(),
        &FixedSecretResolver::returning("unused"),
        &ConnectionSettings { insecure: false },
    )
    .unwrap();
    let mut config = connection.client_config().clone();
    config.set("group.id", group_id);
    config.set("enable.auto.commit", "false");
    let consumer: BaseConsumer = config.create().unwrap();
    let mut query = TopicPartitionList::new();
    for partition in partitions {
        query.add_partition(topic, *partition);
    }
    let committed = consumer.committed_offsets(query, PRODUCE_TIMEOUT).unwrap();
    committed
        .elements()
        .iter()
        .map(|element| element.offset())
        .collect()
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn reads_until_initial_high_watermark_and_stops() {
    let topic = unique_name("kafcli-consume-it");
    let _guard = TopicGuard::create(&topic);
    produce_prepared(
        &topic,
        vec![
            prepared(0, &json_value(0)),
            prepared(0, &json_value(1)),
            prepared(0, &json_value(2)),
            prepared(1, &json_value(3)),
            prepared(1, &json_value(4)),
            prepared(1, &json_value(5)),
        ],
    )
    .unwrap();

    let (summary, envelopes) =
        consume_into_sink(&consume_options(&topic, StartPosition::Earliest)).unwrap();

    assert_eq!(summary.delivered, 6);
    assert_eq!(summary.skipped_by_filter, 0);
    assert_eq!(envelopes.len(), 6);
    assert!(envelopes.iter().all(|envelope| envelope.value.is_some()));
    assert!(summary.group_id.starts_with("kafcli-consume-"));
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn count_stops_early() {
    let topic = unique_name("kafcli-consume-count");
    let _guard = TopicGuard::create(&topic);
    produce_prepared(
        &topic,
        (0..5)
            .map(|index| prepared(0, &json_value(index)))
            .collect(),
    )
    .unwrap();
    let mut options = consume_options(&topic, StartPosition::Earliest);
    options.count = Some(2);

    let (summary, envelopes) = consume_into_sink(&options).unwrap();

    assert_eq!(summary.delivered, 2);
    assert_eq!(offsets_of(&envelopes), vec![0, 1]);
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn offset_start_skips_earlier_messages() {
    let topic = unique_name("kafcli-consume-offset");
    let _guard = TopicGuard::create(&topic);
    produce_prepared(
        &topic,
        (0..4)
            .map(|index| prepared(0, &json_value(index)))
            .collect(),
    )
    .unwrap();
    let mut options = consume_options(&topic, StartPosition::Offset(2));
    options.partitions = Some(vec![0]);

    let (summary, envelopes) = consume_into_sink(&options).unwrap();

    assert_eq!(summary.delivered, 2);
    assert_eq!(offsets_of(&envelopes), vec![2, 3]);
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn time_start_uses_offsets_for_times() {
    let topic = unique_name("kafcli-consume-time");
    let _guard = TopicGuard::create(&topic);
    let now = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    // Explicit create times keep the two batches apart without waiting on the wall clock.
    produce_with_timestamp(&topic, 0, now - 10_000, &[json_value(0), json_value(1)]);
    produce_with_timestamp(&topic, 0, now - 1_000, &[json_value(2), json_value(3)]);
    produce_with_timestamp(&topic, 1, now - 10_000, &[json_value(4)]);
    let start =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(now - 5_000) * 1_000_000).unwrap();
    let mut options = consume_options(&topic, StartPosition::Time(start));
    options.partitions = Some(vec![0, 1]);

    let (summary, envelopes) = consume_into_sink(&options).unwrap();

    // Partition 1 has nothing at or after the start time, so it begins at its end and adds nothing.
    assert_eq!(summary.delivered, 2);
    assert_eq!(offsets_of(&envelopes), vec![2, 3]);
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn oversized_value_is_truncated_not_decoded() {
    let topic = unique_name("kafcli-consume-truncate");
    let _guard = TopicGuard::create(&topic);
    let payload = format!(r#"{{"data":"{}"}}"#, "x".repeat(100)).into_bytes();
    produce_prepared(&topic, vec![prepared(0, &payload)]).unwrap();
    let mut options = consume_options(&topic, StartPosition::Earliest);
    options.max_bytes = 16;

    let (_, envelopes) = consume_into_sink(&options).unwrap();

    assert_eq!(envelopes.len(), 1);
    let envelope = &envelopes[0];
    assert!(envelope.value_truncated);
    assert_eq!(envelope.value, None);
    assert_eq!(envelope.decode_error, None);
    let kept = decode_base64(envelope.value_base64.as_deref().unwrap()).unwrap();
    assert_eq!(kept, payload[..16].to_vec());
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn non_json_value_becomes_decode_error() {
    let topic = unique_name("kafcli-consume-decode");
    let _guard = TopicGuard::create(&topic);
    produce_prepared(&topic, vec![prepared(0, b"not json")]).unwrap();

    let (_, envelopes) =
        consume_into_sink(&consume_options(&topic, StartPosition::Earliest)).unwrap();

    assert_eq!(envelopes.len(), 1);
    let envelope = &envelopes[0];
    assert_eq!(envelope.value, None);
    assert!(envelope.decode_error.is_some());
    let kept = decode_base64(envelope.value_base64.as_deref().unwrap()).unwrap();
    assert_eq!(kept, b"not json".to_vec());
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn consume_without_group_commits_nothing() {
    let topic = unique_name("kafcli-consume-nocommit");
    let _guard = TopicGuard::create(&topic);
    produce_prepared(
        &topic,
        vec![prepared(0, &json_value(0)), prepared(1, &json_value(1))],
    )
    .unwrap();

    let (summary, envelopes) =
        consume_into_sink(&consume_options(&topic, StartPosition::Earliest)).unwrap();

    assert_eq!(envelopes.len(), 2);
    let committed = committed_offsets_of(&summary.group_id, &topic, &[0, 1]);
    assert_eq!(committed, vec![Offset::Invalid, Offset::Invalid]);
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn broken_pipe_sink_stops_cleanly() {
    struct BrokenAfterFirst {
        accepted: usize,
    }
    impl EnvelopeSink for BrokenAfterFirst {
        fn accept_envelope(&mut self, _envelope: &MessageEnvelope) -> io::Result<()> {
            if self.accepted == 0 {
                self.accepted += 1;
                return Ok(());
            }
            Err(io::Error::from(io::ErrorKind::BrokenPipe))
        }
    }
    let topic = unique_name("kafcli-consume-pipe");
    let _guard = TopicGuard::create(&topic);
    produce_prepared(
        &topic,
        (0..3)
            .map(|index| prepared(0, &json_value(index)))
            .collect(),
    )
    .unwrap();
    let mut sink = BrokenAfterFirst { accepted: 0 };

    let summary = consume_messages(
        &plaintext_context(),
        &FixedSecretResolver::returning("unused"),
        &ConnectionSettings { insecure: false },
        &consume_options(&topic, StartPosition::Earliest),
        &RawJsonCodec,
        &mut sink,
    )
    .unwrap();

    assert_eq!(summary.delivered, 1);
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn group_mode_reads_until_watermark_and_stops() {
    let topic = unique_name("kafcli-consume-group");
    let _guard = TopicGuard::create(&topic);
    produce_prepared(
        &topic,
        vec![
            prepared(0, &json_value(0)),
            prepared(0, &json_value(1)),
            prepared(0, &json_value(2)),
            prepared(1, &json_value(3)),
            prepared(1, &json_value(4)),
        ],
    )
    .unwrap();
    let group = unique_name("kafcli-consume-it-group");
    let mut options = consume_options(&topic, StartPosition::Earliest);
    options.group = Some(group.clone());

    let (summary, envelopes) = consume_into_sink(&options).unwrap();

    assert_eq!(summary.delivered, 5);
    assert_eq!(summary.group_id, group);
    assert_eq!(envelopes.len(), 5);
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn group_second_run_delivers_what_the_first_left() {
    let topic = unique_name("kafcli-consume-resume");
    let _guard = TopicGuard::create(&topic);
    produce_prepared(
        &topic,
        vec![
            prepared(0, &json_value(0)),
            prepared(0, &json_value(1)),
            prepared(0, &json_value(2)),
            prepared(1, &json_value(3)),
            prepared(1, &json_value(4)),
            prepared(1, &json_value(5)),
        ],
    )
    .unwrap();
    let group = unique_name("kafcli-consume-resume-group");
    let mut first = consume_options(&topic, StartPosition::Earliest);
    first.group = Some(group.clone());
    first.count = Some(4);
    let (first_summary, first_envelopes) = consume_into_sink(&first).unwrap();

    let mut second = consume_options(&topic, StartPosition::Earliest);
    second.group = Some(group.clone());
    let (second_summary, second_envelopes) = consume_into_sink(&second).unwrap();

    assert_eq!(first_summary.delivered, 4);
    assert_eq!(second_summary.delivered, 2);
    // Together the two runs deliver each of the six messages exactly once.
    let mut positions: Vec<(i32, i64)> = first_envelopes
        .iter()
        .chain(second_envelopes.iter())
        .map(|envelope| (envelope.partition.unwrap(), envelope.offset.unwrap()))
        .collect();
    positions.sort_unstable();
    assert_eq!(
        positions,
        vec![(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2)]
    );
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn header_filter_skips_non_matching_messages() {
    let topic = unique_name("kafcli-consume-filter");
    let _guard = TopicGuard::create(&topic);
    let with_kind = |kind: &[u8], index: usize| PreparedMessage {
        key: None,
        value: Some(json_value(index)),
        headers: vec![RecordHeader {
            name: "kind".to_string(),
            value: Some(kind.to_vec()),
        }],
        partition: Some(0),
    };
    produce_prepared(
        &topic,
        vec![with_kind(b"a", 0), with_kind(b"b", 1), with_kind(b"a", 2)],
    )
    .unwrap();
    let mut options = consume_options(&topic, StartPosition::Earliest);
    options.filters = vec![HeaderFilter {
        name: "kind".to_string(),
        value: "a".to_string(),
    }];

    let (summary, envelopes) = consume_into_sink(&options).unwrap();

    assert_eq!(summary.delivered, 2);
    assert_eq!(summary.skipped_by_filter, 1);
    assert_eq!(offsets_of(&envelopes), vec![0, 2]);
}
