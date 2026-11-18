#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Read operations (`lag`, `offsets show`, `groups audit`) through the real binary, compared with
//! the Kafka tools of the `kafka-plaintext` broker of `tests/fixtures/compose.yaml`.
//!
//! Run with: `docker compose -f tests/fixtures/compose.yaml up -d --wait kafka-plaintext` and then
//! `cargo test -p kafcli --test integration_read_operations_cli -- --ignored --test-threads=1`

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

const PLAINTEXT_BROKER: &str = "127.0.0.1:59092";
const SERVICE: &str = "kafka-plaintext";
const TOPIC_PARTITIONS: usize = 3;
const PRODUCED_MESSAGES: usize = 30;
const CONSUMED_MESSAGES: usize = 12;

fn compose_file() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/compose.yaml")
}

fn kafka_tool(script: &str) -> ProcessCommand {
    let mut command = ProcessCommand::new("docker");
    command
        .args(["compose", "-f"])
        .arg(compose_file())
        .args(["exec", "-T", SERVICE])
        .arg(format!("/opt/kafka/bin/{script}"))
        .args(["--bootstrap-server", "localhost:9092"]);
    command
}

fn run_kafka_tool(script: &str, arguments: &[&str]) -> String {
    let output = kafka_tool(script).args(arguments).output().unwrap();
    assert!(
        output.status.success(),
        "{script} {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

/// Deletes its topic when dropped, so a failing assertion does not leave the topic behind.
struct TopicGuard {
    name: String,
}

impl TopicGuard {
    fn create(name: &str, partitions: usize) -> Self {
        let partitions = partitions.to_string();
        run_kafka_tool(
            "kafka-topics.sh",
            &["--create", "--topic", name, "--partitions", &partitions],
        );
        Self {
            name: name.to_owned(),
        }
    }
}

impl Drop for TopicGuard {
    fn drop(&mut self) {
        let _ = kafka_tool("kafka-topics.sh")
            .args(["--delete", "--topic", &self.name])
            .output();
    }
}

fn build_kafcli_command(directory: &Path) -> Command {
    let mut command = Command::cargo_bin("kafcli").unwrap();
    command
        .env_remove("KAFCLI_CONTEXT")
        .env_remove("KAFCLI_CURRENT_CONTEXT")
        .env("KAFCLI_CONFIG", directory.join("config.json"))
        .env("KAFCLI_STATE_DIR", directory.join("state"));
    command
}

fn add_plaintext_context(directory: &Path) {
    build_kafcli_command(directory)
        .args([
            "context",
            "add",
            "plain",
            "--brokers",
            PLAINTEXT_BROKER,
            "--auth-type",
            "none",
            "--use",
        ])
        .assert()
        .success();
}

/// Produces `PRODUCED_MESSAGES` messages whose keys cycle over the partitions.
fn produce_keyed_messages(topic: &str) {
    let mut child = kafka_tool("kafka-console-producer.sh")
        .args([
            "--topic",
            topic,
            "--property",
            "parse.key=true",
            "--property",
            "key.separator=:",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    {
        let stdin = child.stdin.as_mut().unwrap();
        for index in 0..PRODUCED_MESSAGES {
            writeln!(stdin, "key-{}:{{\"id\":{index}}}", index % TOPIC_PARTITIONS).unwrap();
        }
    }
    drop(child.stdin.take());
    assert!(child.wait().unwrap().success());
}

/// Reads `CONSUMED_MESSAGES` messages with `group`, one per poll, and exits; closing the consumer
/// commits exactly the messages it read.
fn consume_with_group(topic: &str, group: &str) {
    let count = CONSUMED_MESSAGES.to_string();
    run_kafka_tool(
        "kafka-console-consumer.sh",
        &[
            "--topic",
            topic,
            "--group",
            group,
            "--from-beginning",
            "--max-messages",
            &count,
            "--timeout-ms",
            "30000",
            "--consumer-property",
            "max.poll.records=1",
        ],
    );
}

/// Creates a topic, fills it and leaves `group` stopped with committed offsets.
fn prepare_stopped_group() -> (TopicGuard, String) {
    let topic = TopicGuard::create(&unique_name("lag"), TOPIC_PARTITIONS);
    produce_keyed_messages(&topic.name);
    let group = unique_name("lag-group");
    consume_with_group(&topic.name, &group);
    (topic, group)
}

/// `(topic, partition) -> (CURRENT-OFFSET, LOG-END-OFFSET, LAG)` from `kafka-consumer-groups.sh`.
fn describe_with_kafka_tool(group: &str) -> BTreeMap<(String, i64), (String, String, String)> {
    let output = run_kafka_tool(
        "kafka-consumer-groups.sh",
        &["--describe", "--group", group],
    );
    output
        .lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>())
        .filter(|columns| columns.len() >= 6 && columns[0] == group)
        .map(|columns| {
            (
                (columns[1].to_owned(), columns[2].parse().unwrap()),
                (
                    columns[3].to_owned(),
                    columns[4].to_owned(),
                    columns[5].to_owned(),
                ),
            )
        })
        .collect()
}

fn lag_json(directory: &Path, arguments: &[&str]) -> Value {
    let output = build_kafcli_command(directory)
        .args(["lag", "--output", "json"])
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "lag failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn render_cell(value: &Value) -> String {
    match value {
        Value::Null => "-".to_owned(),
        other => other.to_string(),
    }
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn lag_matches_kafka_consumer_groups_describe() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    let (topic, group) = prepare_stopped_group();

    let report = lag_json(
        directory.path(),
        &["--group", &group, "--topic", &topic.name],
    );
    let expected = describe_with_kafka_tool(&group);

    let partitions = report["data"]["groups"][0]["topics"][0]["partitions"]
        .as_array()
        .unwrap();
    let actual: BTreeMap<(String, i64), (String, String, String)> = partitions
        .iter()
        .map(|partition| {
            (
                (
                    partition["topic"].as_str().unwrap().to_owned(),
                    partition["partition"].as_i64().unwrap(),
                ),
                (
                    render_cell(&partition["committed_offset"]),
                    render_cell(&partition["log_end_offset"]),
                    render_cell(&partition["lag"]),
                ),
            )
        })
        .collect();
    assert!(
        !expected.is_empty(),
        "kafka-consumer-groups.sh listed nothing"
    );
    assert_eq!(actual, expected);
    let total: i64 = partitions
        .iter()
        .filter_map(|partition| partition["lag"].as_i64())
        .sum();
    assert_eq!(
        total,
        (PRODUCED_MESSAGES - CONSUMED_MESSAGES) as i64 - uncommitted_backlog(&expected)
    );
    assert_eq!(
        report["data"]["groups"][0]["total_lag"].as_i64().unwrap(),
        total
    );
}

/// Messages of partitions the group never committed on, which `kafka-consumer-groups.sh` does not
/// list for a group without members.
fn uncommitted_backlog(listed: &BTreeMap<(String, i64), (String, String, String)>) -> i64 {
    let listed_messages: i64 = listed
        .values()
        .map(|(_, log_end, _)| log_end.parse::<i64>().unwrap())
        .sum();
    PRODUCED_MESSAGES as i64 - listed_messages
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn lag_of_stopped_group_without_topic_flag() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    let (topic, group) = prepare_stopped_group();

    let report = lag_json(directory.path(), &["--group", &group]);

    let group_report = &report["data"]["groups"][0];
    assert_eq!(group_report["has_active_members"], Value::Bool(false));
    let topics: Vec<&str> = group_report["topics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|topic| topic["topic"].as_str().unwrap())
        .collect();
    assert_eq!(topics, vec![topic.name.as_str()]);
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn max_lag_exceeded_exits_two_with_report_on_stdout() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    let (_topic, group) = prepare_stopped_group();

    let output = build_kafcli_command(directory.path())
        .args([
            "lag",
            "--group",
            &group,
            "--max-lag",
            "1",
            "--output",
            "json",
        ])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(2));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        report["data"]["groups"][0]["group"].as_str(),
        Some(group.as_str())
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("limit exceeded"));
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn unknown_group_is_reported_as_not_found() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());

    build_kafcli_command(directory.path())
        .args(["lag", "--group", "nope"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("consumer group 'nope' not found"));
}

const AUDIT_SERVICE_PATTERN: &str = r"^(?P<service>[a-z]+)-consumer-\d+$";
const MEMBER_WAIT_SECONDS: u64 = 30;

/// A `kafka-console-consumer.sh` that stays in `group` until dropped. Dropping it kills the process
/// inside the container, because killing `docker compose exec` alone leaves it running there.
struct BackgroundConsumer {
    child: std::process::Child,
    client_id: String,
}

impl BackgroundConsumer {
    fn start(topic: &str, group: &str, client_id: &str) -> Self {
        let client_property = format!("client.id={client_id}");
        let child = kafka_tool("kafka-console-consumer.sh")
            .args([
                "--topic",
                topic,
                "--group",
                group,
                "--timeout-ms",
                "120000",
                "--consumer-property",
                &client_property,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Self {
            child,
            client_id: client_id.to_owned(),
        }
    }
}

impl Drop for BackgroundConsumer {
    fn drop(&mut self) {
        let pattern = format!("client.id={}", self.client_id);
        let _ = ProcessCommand::new("docker")
            .args(["compose", "-f"])
            .arg(compose_file())
            .args(["exec", "-T", SERVICE, "pkill", "-f", &pattern])
            .output();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Waits until `lag` shows `expected` distinct members in `group`.
#[allow(clippy::disallowed_methods)]
fn wait_for_members(directory: &Path, topic: &str, group: &str, expected: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(MEMBER_WAIT_SECONDS);
    loop {
        let output = build_kafcli_command(directory)
            .args([
                "lag", "--group", group, "--topic", topic, "--output", "json",
            ])
            .output()
            .unwrap();
        if output.status.success() {
            let report: Value = serde_json::from_slice(&output.stdout).unwrap();
            let members: std::collections::BTreeSet<String> = report["data"]["groups"][0]["topics"]
                .as_array()
                .into_iter()
                .flatten()
                .flat_map(|topic| topic["partitions"].as_array().into_iter().flatten())
                .filter_map(|partition| partition["member"]["member_id"].as_str())
                .map(str::to_owned)
                .collect();
            if members.len() == expected {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "group {group} did not reach {expected} members"
        );
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn audit_json(directory: &Path, group: &str) -> (Option<i32>, Value) {
    let output = build_kafcli_command(directory)
        .args([
            "groups",
            "audit",
            "--group",
            group,
            "--service-pattern",
            AUDIT_SERVICE_PATTERN,
            "--output",
            "json",
        ])
        .output()
        .unwrap();
    let report = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "audit stdout is not JSON ({error}): {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output.status.code(), report)
}

fn has_finding(report: &Value, rule: &str, severity: &str) -> bool {
    report["data"]["findings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|finding| finding["rule"] == rule && finding["severity"] == severity)
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn audit_detects_r1_with_two_services_in_one_group() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    let topic = TopicGuard::create(&unique_name("audit"), 2);
    let group = unique_name("audit-group");
    let _pedidos = BackgroundConsumer::start(&topic.name, &group, "pedidos-consumer-1");
    let _estoque = BackgroundConsumer::start(&topic.name, &group, "estoque-consumer-1");
    wait_for_members(directory.path(), &topic.name, &group, 2);

    let (code, report) = audit_json(directory.path(), &group);

    assert_eq!(code, Some(1), "{report}");
    assert!(has_finding(&report, "R1", "ERROR"), "{report}");
    assert_eq!(report["data"]["groups_checked"], 1);
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn audit_of_clean_group_exits_zero() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    let topic = TopicGuard::create(&unique_name("audit"), 2);
    let group = unique_name("audit-group");
    let _pedidos = BackgroundConsumer::start(&topic.name, &group, "pedidos-consumer-1");
    wait_for_members(directory.path(), &topic.name, &group, 1);

    let (code, report) = audit_json(directory.path(), &group);

    assert_eq!(code, Some(0), "{report}");
    assert!(!has_finding(&report, "R1", "ERROR"), "{report}");
    assert!(!has_finding(&report, "R2", "ERROR"), "{report}");
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn audit_warns_r3_for_stopped_group_with_lag() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    let (_topic, group) = prepare_stopped_group();

    let (code, report) = audit_json(directory.path(), &group);

    assert_eq!(code, Some(0), "{report}");
    assert!(has_finding(&report, "R3", "WARNING"), "{report}");
}
