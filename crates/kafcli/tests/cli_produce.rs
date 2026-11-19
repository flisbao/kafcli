#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests report failures by panicking"
)]

use assert_cmd::Command;
use assert_cmd::assert::Assert;
use predicates::prelude::*;
use serde_json::Value;
use std::path::{Path, PathBuf};

/// Command with the configuration and state directory inside `directory`, so no test touches the
/// user's real configuration or audit log.
fn build_kafcli_command(directory: &Path) -> Command {
    let mut command = Command::cargo_bin("kafcli").unwrap();
    command
        .env_remove("KAFCLI_CONTEXT")
        .env_remove("KAFCLI_CURRENT_CONTEXT")
        .env("KAFCLI_CONFIG", directory.join("config.json"))
        .env("KAFCLI_STATE_DIR", directory.join("state"));
    command
}

fn state_directory(directory: &Path) -> PathBuf {
    directory.join("state")
}

fn read_audit_log(directory: &Path) -> String {
    std::fs::read_to_string(state_directory(directory).join("audit.log")).unwrap_or_default()
}

fn parse_stdout_json(assertion: &Assert) -> Value {
    serde_json::from_slice(&assertion.get_output().stdout).unwrap()
}

fn add_context(directory: &Path, name: &str, extra_arguments: &[&str]) {
    build_kafcli_command(directory)
        .args(["context", "add", name, "--brokers", "127.0.0.1:1"])
        .args(extra_arguments)
        .assert()
        .success();
}

fn write_input_file(directory: &Path, content: &str) -> PathBuf {
    let path = directory.join("input.ndjson");
    std::fs::write(&path, content).unwrap();
    path
}

#[test]
fn dry_run_does_not_connect_and_shows_hex() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    let started = std::time::Instant::now();
    build_kafcli_command(directory.path())
        .args(["produce", "--topic", "orders", "--value", r#"{"id":1}"#])
        .arg("--dry-run")
        .assert()
        .success()
        .stdout(predicate::str::contains("7b226964223a317d"))
        .stdout(predicate::str::contains("8"));

    assert!(started.elapsed() < std::time::Duration::from_secs(10));
    assert!(read_audit_log(directory.path()).contains("\"outcome\":\"dry-run\""));
}

#[test]
fn dry_run_json_lists_messages() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);
    let input = write_input_file(directory.path(), "{\"id\":1}\n{\"id\":2}\n");

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "produce", "--topic", "orders"])
        .arg("--file")
        .arg(&input)
        .arg("--dry-run")
        .assert()
        .success();

    let data = parse_stdout_json(&assertion)["data"].clone();
    assert_eq!(data["dry_run"], true);
    assert_eq!(data["messages_truncated"], false);
    assert_eq!(data["messages"].as_array().map(Vec::len), Some(2));
    assert_eq!(data["messages"][0]["value_bytes"], 8);
}

#[test]
fn protected_context_without_tty_and_yes_aborts_with_exit_3() {
    let directory = tempfile::tempdir().unwrap();
    add_context(
        directory.path(),
        "prd",
        &["--auth-type", "tls", "--protected", "--use"],
    );

    build_kafcli_command(directory.path())
        .args([
            "produce",
            "--topic",
            "orders",
            "--value",
            r#"{"secret":"abc123"}"#,
        ])
        .assert()
        .code(3)
        .stderr(predicate::str::contains(
            "confirmation required on protected context: pass --yes",
        ));

    let audit = read_audit_log(directory.path());
    assert!(audit.contains("\"outcome\":\"aborted\""), "audit: {audit}");
    assert!(audit.contains("\"command\":\"produce\""), "audit: {audit}");
    assert!(audit.contains("topic=orders"), "audit: {audit}");
    assert!(
        !audit.contains("abc123"),
        "audit leaked the payload: {audit}"
    );
}

#[test]
fn protected_dry_run_needs_no_confirmation() {
    let directory = tempfile::tempdir().unwrap();
    add_context(
        directory.path(),
        "prd",
        &["--auth-type", "tls", "--protected", "--use"],
    );

    build_kafcli_command(directory.path())
        .args(["produce", "--topic", "orders", "--value", "{}", "--dry-run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("dry run"))
        .stderr(predicate::str::contains(
            "WARNING: context 'prd' is PRODUCTION",
        ))
        .stderr(predicate::str::contains("confirmation required").not());
}

#[test]
fn unknown_template_fails_before_connecting() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    let started = std::time::Instant::now();
    build_kafcli_command(directory.path())
        .args([
            "produce",
            "--topic",
            "orders",
            "--value",
            r#"{"id":"{{nope}}"}"#,
        ])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("unknown template '{{nope}}'"));

    assert!(started.elapsed() < std::time::Duration::from_secs(10));
}

#[test]
fn missing_env_template_does_not_print_other_env_values() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    let assertion = build_kafcli_command(directory.path())
        .env("KAFCLI_TEST_PRESENT_VARIABLE", "s3cret-value")
        .args(["produce", "--topic", "orders", "--value"])
        .arg(r#"{"token":"{{env:KAFCLI_TEST_MISSING_VARIABLE}}"}"#)
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "environment variable 'KAFCLI_TEST_MISSING_VARIABLE' is not set",
        ));

    let combined = [
        assertion.get_output().stdout.as_slice(),
        assertion.get_output().stderr.as_slice(),
    ]
    .concat();
    assert!(!String::from_utf8_lossy(&combined).contains("s3cret-value"));
}

#[test]
fn empty_input_is_an_error_before_connecting() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);
    let input = write_input_file(directory.path(), "\n   \n");

    build_kafcli_command(directory.path())
        .args(["produce", "--topic", "orders", "--file"])
        .arg(&input)
        .assert()
        .code(1)
        .stderr(predicate::str::contains("no messages to produce"));
}

#[test]
fn value_file_and_stdin_are_mutually_exclusive() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    build_kafcli_command(directory.path())
        .args(["produce", "--topic", "orders", "--value", "{}", "--stdin"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn an_input_source_is_required() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["produce", "--topic", "orders"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("required"));
}

#[test]
fn header_without_equals_is_usage_error() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    build_kafcli_command(directory.path())
        .args(["produce", "--topic", "orders", "--value", "{}"])
        .args(["--header", "trace"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("expected NAME=VALUE"));
}

#[test]
fn stdin_input_is_read_for_dry_run() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    let assertion = build_kafcli_command(directory.path())
        .args([
            "--output", "json", "produce", "--topic", "orders", "--stdin",
        ])
        .arg("--dry-run")
        .write_stdin("{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n")
        .assert()
        .success();

    assert_eq!(
        parse_stdout_json(&assertion)["data"]["messages"]
            .as_array()
            .map(Vec::len),
        Some(3)
    );
}

#[test]
fn unreachable_broker_fails_and_audits_failed() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    let started = std::time::Instant::now();
    build_kafcli_command(directory.path())
        .args([
            "produce",
            "--topic",
            "orders",
            "--value",
            "{}",
            "--timeout-seconds",
            "2",
        ])
        .assert()
        .code(1)
        .stderr(predicate::str::starts_with("error:"));

    assert!(started.elapsed() < std::time::Duration::from_secs(20));
    assert!(read_audit_log(directory.path()).contains("\"outcome\":\"failed\""));
}

#[test]
fn audit_directory_is_taken_from_state_dir_variable() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    build_kafcli_command(directory.path())
        .args(["produce", "--topic", "orders", "--value", "{}", "--dry-run"])
        .assert()
        .success();

    assert!(
        state_directory(directory.path())
            .join("audit.log")
            .is_file()
    );
}

#[test]
fn help_lists_examples_and_the_no_template_rule() {
    Command::cargo_bin("kafcli")
        .unwrap()
        .args(["produce", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Examples:"))
        .stdout(predicate::str::contains("--no-template"))
        .stdout(predicate::str::contains("literal `{{`"))
        .stdout(predicate::str::contains("--input envelope"));
}
