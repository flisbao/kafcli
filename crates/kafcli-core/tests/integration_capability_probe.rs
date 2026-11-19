#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests report failures by panicking"
)]

//! Capability probe tests against the Redpanda plaintext broker of `tests/fixtures/compose.yaml`.
//!
//! Run with: `docker compose -f tests/fixtures/compose.yaml up -d --wait redpanda-plaintext` and then
//! `cargo test -p kafcli-core --test integration_capability_probe -- --ignored`

use kafcli_core::capability::capability_probe::{ProbeOptions, probe_capabilities};
use kafcli_core::capability::capability_report::{
    CapabilityName, CapabilityOutcome, CapabilityReport,
};
use kafcli_core::capability::message_dump::{MessageDump, dump_topic_messages};
use kafcli_core::config::model::{AuthConfig, ConnectionContext};
use kafcli_core::connection::client_config_builder::ConnectionSettings;
use kafcli_core::error::{CoreError, CoreResult};
use kafcli_core::secret::fixed_resolver::FixedSecretResolver;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const PLAINTEXT_BROKER: &str = "localhost:19092";
const PROBE_TIMEOUT: Duration = Duration::from_secs(15);
const EXPECTED_ORDER: [CapabilityName; 10] = [
    CapabilityName::ClusterMetadata,
    CapabilityName::IdempotentProduce,
    CapabilityName::HeadersRoundTrip,
    CapabilityName::ListOffsets,
    CapabilityName::OffsetsForTimes,
    CapabilityName::CommitOffsetsOutsideGroup,
    CapabilityName::FetchCommittedOffsets,
    CapabilityName::ListGroups,
    CapabilityName::DescribeGroup,
    CapabilityName::CommitRejectedWhileGroupActive,
];

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

fn probe(topic: &str) -> CoreResult<CapabilityReport> {
    probe_capabilities(
        "plain",
        &plaintext_context(),
        &FixedSecretResolver::returning("unused"),
        &ConnectionSettings { insecure: false },
        &ProbeOptions {
            topic: topic.to_string(),
            timeout: PROBE_TIMEOUT,
        },
    )
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn every_capability_is_supported_on_redpanda() {
    let topic = unique_name("kafcli-probe-it");
    let _guard = TopicGuard::create(&topic);

    let report = probe(&topic).unwrap();

    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    let names: Vec<CapabilityName> = report.checks.iter().map(|check| check.name).collect();
    assert_eq!(names, EXPECTED_ORDER.to_vec());
    for check in &report.checks {
        assert!(
            matches!(check.outcome, CapabilityOutcome::Supported { .. }),
            "not supported: {check:?}"
        );
    }
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn missing_topic_fails_before_producing() {
    let topic = unique_name("kafcli-probe-missing");

    match probe(&topic) {
        Err(CoreError::TopicNotFound(name)) => assert_eq!(name, topic),
        other => panic!("expected TopicNotFound, got {other:?}"),
    }

    let listing = run_rpk(&["topic", "list"]);
    assert!(!listing.contains(&topic), "topic was created: {listing}");
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn dump_reads_probe_messages_with_headers() {
    let topic = unique_name("kafcli-probe-dump");
    let _guard = TopicGuard::create(&topic);
    let report = probe(&topic).unwrap();

    let messages = dump_topic_messages(
        &plaintext_context(),
        &FixedSecretResolver::returning("unused"),
        &ConnectionSettings { insecure: false },
        &topic,
        10,
        Duration::from_secs(5),
    )
    .unwrap();

    println!("{}", serde_json::to_string_pretty(&messages).unwrap());
    let probe_messages: Vec<&MessageDump> = messages
        .iter()
        .filter(|message| message.partition == 0)
        .filter(|message| {
            message.headers.iter().any(|header| {
                header.name == "kafcli-probe-run"
                    && header.value_utf8.as_deref() == Some(report.run_identifier.as_str())
            })
        })
        .collect();
    assert!(
        probe_messages.len() >= 3,
        "expected at least 3 probe messages on partition 0, got {messages:?}"
    );
}
