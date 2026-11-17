#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Produce and consume through the real binary, against the Redpanda plaintext broker of
//! `tests/fixtures/compose.yaml`.
//!
//! Run with: `docker compose -f tests/fixtures/compose.yaml up -d --wait` and then
//! `cargo test -p kafcli --test integration_produce_consume_cli -- --ignored --test-threads=1`

use assert_cmd::Command;
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

const PLAINTEXT_BROKER: &str = "127.0.0.1:19092";
/// Messages in the topic that the closed-pipe test reads from.
const PIPE_TEST_MESSAGES: usize = 200;
/// Padding per message; 200 messages of about 1 KiB exceed the 64 KiB pipe buffer.
const PIPE_TEST_PADDING_BYTES: usize = 1000;
/// `not json` in base64: a value that is neither JSON nor kept as text.
const POISON_VALUE_BASE64: &str = "bm90IGpzb24=";

fn compose_file() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/compose.yaml")
}

fn run_rpk(arguments: &[&str]) -> String {
    let output = ProcessCommand::new("docker")
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
        let _ = ProcessCommand::new("docker")
            .args(["compose", "-f"])
            .arg(compose_file())
            .args(["exec", "-T", "redpanda-plaintext", "rpk", "topic", "delete"])
            .arg(&self.name)
            .output();
    }
}

/// Command with the configuration and state directory inside `directory`.
fn build_kafcli_command(directory: &Path) -> Command {
    let mut command = Command::cargo_bin("kafcli").unwrap();
    command
        .env_remove("KAFCLI_CONTEXT")
        .env_remove("KAFCLI_CURRENT_CONTEXT")
        .env("KAFCLI_CONFIG", directory.join("config.json"))
        .env("KAFCLI_STATE_DIR", directory.join("state"));
    command
}

/// Adds a plaintext context to the broker of the fixtures and makes it the current one.
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

/// Produces one JSON message with a key and two headers.
fn produce_keyed_message(directory: &Path, topic: &str, index: usize) {
    let value = format!(r#"{{"id":{index}}}"#);
    let key = format!("pedido-{index}");
    let sequence = format!("seq={index}");
    build_kafcli_command(directory)
        .args([
            "produce",
            "--topic",
            topic,
            "--value",
            &value,
            "--key",
            &key,
            "--header",
            "origem=kafcli",
            "--header",
            &sequence,
        ])
        .assert()
        .success();
}

/// Produces the envelope lines in `input` through standard input.
fn produce_envelopes(directory: &Path, topic: &str, input: &[u8]) {
    build_kafcli_command(directory)
        .args([
            "produce", "--topic", topic, "--stdin", "--input", "envelope",
        ])
        .write_stdin(input.to_vec())
        .assert()
        .success();
}

/// Consumes `topic` as NDJSON envelopes and returns the stdout bytes. Nothing may appear on stderr.
fn consume_envelopes(directory: &Path, topic: &str) -> Vec<u8> {
    let output = build_kafcli_command(directory)
        .args(["--output", "json", "consume", "--topic", topic])
        .assert()
        .success()
        .get_output()
        .clone();
    assert!(
        output.stderr.is_empty(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn parse_envelopes(bytes: &[u8]) -> Vec<Value> {
    String::from_utf8(bytes.to_vec())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// Key, headers and value of each envelope, sorted by key. Positions (topic, partition, offset) are left out.
fn key_headers_values(envelopes: &[Value]) -> Vec<(String, Value, Value)> {
    let mut rows: Vec<(String, Value, Value)> = envelopes
        .iter()
        .map(|envelope| {
            (
                envelope["key"].as_str().unwrap().to_string(),
                envelope["headers"].clone(),
                envelope["value"].clone(),
            )
        })
        .collect();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn envelope_round_trip_preserves_key_headers_and_value() {
    let source = unique_name("kafcli-cli-source");
    let target = unique_name("kafcli-cli-target");
    let _source_guard = TopicGuard::create(&source);
    let _target_guard = TopicGuard::create(&target);
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    for index in 1..=3 {
        produce_keyed_message(directory.path(), &source, index);
    }

    let consumed_source = consume_envelopes(directory.path(), &source);
    produce_envelopes(directory.path(), &target, &consumed_source);
    let consumed_target = consume_envelopes(directory.path(), &target);

    let source_rows = key_headers_values(&parse_envelopes(&consumed_source));
    assert_eq!(source_rows.len(), 3);
    assert!(
        source_rows
            .iter()
            .all(|(_, headers, _)| headers.as_array().map(Vec::len) == Some(2)),
        "{source_rows:?}"
    );
    assert_eq!(
        key_headers_values(&parse_envelopes(&consumed_target)),
        source_rows
    );
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn poison_value_round_trips_as_bytes() {
    let source = unique_name("kafcli-cli-poison-source");
    let target = unique_name("kafcli-cli-poison-target");
    let _source_guard = TopicGuard::create(&source);
    let _target_guard = TopicGuard::create(&target);
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    let envelope_line =
        format!(r#"{{"kafcli_envelope":1,"key":"poison","value_base64":"{POISON_VALUE_BASE64}"}}"#);
    produce_envelopes(directory.path(), &source, envelope_line.as_bytes());

    let consumed_source = consume_envelopes(directory.path(), &source);
    produce_envelopes(directory.path(), &target, &consumed_source);
    let consumed_target = parse_envelopes(&consume_envelopes(directory.path(), &target));

    assert_eq!(consumed_target.len(), 1);
    assert_eq!(consumed_target[0]["key"], "poison");
    assert_eq!(consumed_target[0]["value_base64"], POISON_VALUE_BASE64);
    assert!(consumed_target[0]["decode_error"].is_string());
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn head_closing_pipe_exits_zero() {
    let topic = unique_name("kafcli-cli-pipe");
    let _guard = TopicGuard::create(&topic);
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    let padding = "x".repeat(PIPE_TEST_PADDING_BYTES);
    let lines: String = (0..PIPE_TEST_MESSAGES)
        .map(|index| format!("{{\"n\":{index},\"pad\":\"{padding}\"}}\n"))
        .collect();
    let input = directory.path().join("input.ndjson");
    std::fs::write(&input, lines).unwrap();
    build_kafcli_command(directory.path())
        .args(["produce", "--topic", &topic, "--file"])
        .arg(&input)
        .assert()
        .success();

    let mut child = ProcessCommand::new(env!("CARGO_BIN_EXE_kafcli"))
        .env_remove("KAFCLI_CONTEXT")
        .env_remove("KAFCLI_CURRENT_CONTEXT")
        .env("KAFCLI_CONFIG", directory.path().join("config.json"))
        .env("KAFCLI_STATE_DIR", directory.path().join("state"))
        .args(["--output", "json", "consume", "--topic", &topic])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut first_line = String::new();
    reader.read_line(&mut first_line).unwrap();
    assert!(first_line.starts_with('{'), "first line: {first_line}");
    // Closing the read end while the process still has messages to write makes its next write fail.
    drop(reader);

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "status: {:?}", output.status);
    assert!(
        output.stderr.is_empty(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn table_output_neutralizes_control_characters() {
    let topic = unique_name("kafcli-cli-table");
    let _guard = TopicGuard::create(&topic);
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path());
    produce_envelopes(
        directory.path(),
        &topic,
        br#"{"kafcli_envelope":1,"key":"\u001b[31mred","value":{"n":1}}"#,
    );

    let output = build_kafcli_command(directory.path())
        .args(["consume", "--topic", &topic])
        .assert()
        .success()
        .get_output()
        .clone();

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.contains('\u{1b}'), "stdout: {stdout:?}");
    assert!(stdout.contains("?[31mred"), "stdout: {stdout:?}");
    assert!(stdout.contains("{\"n\":1}"), "stdout: {stdout:?}");
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "consumed 1 messages\n"
    );
}
