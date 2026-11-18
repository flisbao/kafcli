#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Destructive operations (`offsets set`, `offsets restore`, `poison skip`) through the real
//! binary, checked with the Kafka tools of the `kafka-plaintext` broker of
//! `tests/fixtures/compose.yaml`. Offsets, backups and the audit log go only to the temporary
//! directory of `KAFCLI_STATE_DIR`.
//!
//! Run with: `docker compose -f tests/fixtures/compose.yaml up -d --wait kafka-plaintext` and then
//! `cargo test -p kafcli --test integration_destructive_operations_cli -- --ignored --test-threads=1`

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    let topic = TopicGuard::create(&unique_name("reset"), TOPIC_PARTITIONS);
    produce_keyed_messages(&topic.name);
    let group = unique_name("reset-group");
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

fn render_cell(value: &Value) -> String {
    match value {
        Value::Null => "-".to_owned(),
        other => other.to_string(),
    }
}

/// Committed offset of each partition, as `kafka-consumer-groups.sh` reports it.
fn committed_offsets(group: &str) -> BTreeMap<i64, String> {
    describe_with_kafka_tool(group)
        .into_iter()
        .map(|((_, partition), (current, _, _))| (partition, current))
        .collect()
}

/// Starts a console consumer of `group` on an idle topic, in the background. It leaves the group
/// by itself after `timeout_ms` without messages, and it never touches the offsets of the
/// topic under test.
fn start_background_member(idle_topic: &str, group: &str, timeout_ms: u64) {
    let status = ProcessCommand::new("docker")
        .args(["compose", "-f"])
        .arg(compose_file())
        .args([
            "exec",
            "-d",
            SERVICE,
            "/opt/kafka/bin/kafka-console-consumer.sh",
        ])
        .args(["--bootstrap-server", "localhost:9092"])
        .args(["--topic", idle_topic, "--group", group])
        .args(["--timeout-ms", &timeout_ms.to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}

fn group_has_members(group: &str) -> bool {
    let output = kafka_tool("kafka-consumer-groups.sh")
        .args(["--describe", "--group", group, "--members"])
        .output()
        .unwrap();
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .any(|line| line.split_whitespace().next() == Some(group))
}

/// Polls the Kafka tool until `group` lists a member. The background consumer gives no signal to
/// wait on, so this test helper sleeps between polls.
#[allow(clippy::disallowed_methods)]
fn wait_for_member(group: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if group_has_members(group) {
            return;
        }
        thread::sleep(Duration::from_millis(500));
    }
    panic!("group {group} got no member within 15 s");
}

fn state_directory(directory: &Path) -> PathBuf {
    directory.join("state")
}

fn backup_files(directory: &Path) -> Vec<PathBuf> {
    match std::fs::read_dir(state_directory(directory).join("offsets-backup")) {
        Ok(entries) => entries.map(|entry| entry.unwrap().path()).collect(),
        Err(_) => Vec::new(),
    }
}

fn audit_outcomes(directory: &Path) -> Vec<String> {
    std::fs::read_to_string(state_directory(directory).join("audit.log"))
        .unwrap_or_default()
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).unwrap()["outcome"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

fn set_offsets(directory: &Path, group: &str, topic: &str, arguments: &[&str]) -> Command {
    let mut command = build_kafcli_command(directory);
    command
        .args(["offsets", "set", "--group", group, "--topic", topic])
        .args(arguments);
    command
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn offsets_set_dry_run_changes_nothing() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    let (topic, group) = prepare_stopped_group();
    let before = committed_offsets(&group);

    set_offsets(directory.path(), &group, &topic.name, &["--to-earliest"])
        .assert()
        .success()
        .stdout(predicate::str::contains("DRY RUN: nothing was committed"));

    assert_eq!(committed_offsets(&group), before);
    assert!(
        !state_directory(directory.path())
            .join("offsets-backup")
            .exists()
    );
    assert_eq!(audit_outcomes(directory.path()), vec!["dry-run"]);
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn offsets_set_execute_writes_backup_and_moves_group() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    let (topic, group) = prepare_stopped_group();
    let before = committed_offsets(&group);

    set_offsets(
        directory.path(),
        &group,
        &topic.name,
        &["--to-latest", "--execute"],
    )
    .assert()
    .success()
    .stdout(predicate::str::contains("committed; backup: "));

    let described = describe_with_kafka_tool(&group);
    assert_eq!(described.len(), TOPIC_PARTITIONS);
    for (current, end, lag) in described.values() {
        assert_eq!(lag, "0", "{described:?}");
        assert_eq!(current, end);
    }
    let backups = backup_files(directory.path());
    assert_eq!(backups.len(), 1);
    let backup: Value =
        serde_json::from_str(&std::fs::read_to_string(&backups[0]).unwrap()).unwrap();
    let backed_up: BTreeMap<i64, String> = backup["partitions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|partition| {
            (
                partition["partition"].as_i64().unwrap(),
                render_cell(&partition["committed_offset"]),
            )
        })
        .collect();
    assert_eq!(backed_up, before);
    assert_eq!(audit_outcomes(directory.path()), vec!["executed"]);
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn offsets_set_refuses_active_group() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    let (topic, group) = prepare_stopped_group();
    let idle = TopicGuard::create(&unique_name("idle"), 1);
    let before = committed_offsets(&group);
    start_background_member(&idle.name, &group, 20_000);
    wait_for_member(&group);

    set_offsets(
        directory.path(),
        &group,
        &topic.name,
        &["--to-latest", "--execute"],
    )
    .assert()
    .code(1)
    .stderr(predicate::str::contains("active member(s)"));

    assert_eq!(committed_offsets(&group), before);
    assert!(backup_files(directory.path()).is_empty());
    assert_eq!(audit_outcomes(directory.path()), vec!["aborted"]);
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn offsets_set_wait_empty_proceeds_after_consumer_leaves() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    let (topic, group) = prepare_stopped_group();
    let idle = TopicGuard::create(&unique_name("idle"), 1);
    start_background_member(&idle.name, &group, 3_000);
    wait_for_member(&group);

    set_offsets(
        directory.path(),
        &group,
        &topic.name,
        &["--to-latest", "--execute", "--wait-empty", "60s"],
    )
    .assert()
    .success();

    for (_, _, lag) in describe_with_kafka_tool(&group).values() {
        assert_eq!(lag, "0");
    }
}
