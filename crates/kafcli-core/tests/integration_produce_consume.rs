#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Produce tests against the Redpanda plaintext broker of `tests/fixtures/compose.yaml`.
//!
//! Run with: `docker compose -f tests/fixtures/compose.yaml up -d --wait redpanda-plaintext` and then
//! `cargo test -p kafcli-core --test integration_produce_consume -- --ignored`

use kafcli_core::codec::raw_json_codec::RawJsonCodec;
use kafcli_core::config::model::{AuthConfig, ConnectionContext};
use kafcli_core::connection::client_config_builder::ConnectionSettings;
use kafcli_core::error::{CoreError, CoreResult};
use kafcli_core::ops::produce::message_producer::{
    ProduceOptions, ProduceReport, prepare_messages, produce_messages,
};
use kafcli_core::ops::produce::produce_input::{InputMode, InputSource, read_produce_inputs};
use kafcli_core::secret::fixed_resolver::FixedSecretResolver;
use kafcli_core::template::ProcessEnvironment;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;

const PLAINTEXT_BROKER: &str = "localhost:19092";
const PRODUCE_TIMEOUT: Duration = Duration::from_secs(15);

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
