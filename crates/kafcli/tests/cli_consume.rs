#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests report failures by panicking"
)]

use assert_cmd::Command;
use predicates::prelude::*;
use std::path::Path;

/// Command with the configuration and state directory inside `directory`, so no test touches the
/// user's real configuration.
fn build_kafcli_command(directory: &Path) -> Command {
    let mut command = Command::cargo_bin("kafcli").unwrap();
    command
        .env_remove("KAFCLI_CONTEXT")
        .env_remove("KAFCLI_CURRENT_CONTEXT")
        .env("KAFCLI_CONFIG", directory.join("config.json"))
        .env("KAFCLI_STATE_DIR", directory.join("state"));
    command
}

fn add_context(directory: &Path, name: &str, extra_arguments: &[&str]) {
    build_kafcli_command(directory)
        .args(["context", "add", name, "--brokers", "127.0.0.1:1"])
        .args(extra_arguments)
        .assert()
        .success();
}

#[test]
fn group_conflicts_with_partition() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args([
            "consume",
            "--topic",
            "orders",
            "--group",
            "billing",
            "--partition",
            "0",
        ])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn group_with_offset_start_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    let started = std::time::Instant::now();
    build_kafcli_command(directory.path())
        .args([
            "consume", "--topic", "orders", "--group", "billing", "--from", "offset:3",
        ])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "--from offset:/time: cannot be combined with --group",
        ));

    assert!(started.elapsed() < std::time::Duration::from_secs(10));
}

#[test]
fn invalid_filter_is_rejected() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["consume", "--topic", "orders", "--filter", "origem=kafcli"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("expected header:NAME=VALUE"));
}

#[test]
fn invalid_start_position_is_rejected() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["consume", "--topic", "orders", "--from", "yesterday"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("invalid start position"));
}

#[test]
fn help_states_the_redaction_rules_and_examples() {
    Command::cargo_bin("kafcli")
        .unwrap()
        .args(["consume", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Examples:"))
        .stdout(predicate::str::contains(
            "--redact covers decoded JSON values only; keys and headers are not redacted.",
        ))
        .stdout(predicate::str::contains(
            "With --redact, oversized or non-JSON values are withheld",
        ))
        .stdout(predicate::str::contains("--format json --wire raw"))
        .stdout(predicate::str::contains(
            "kafcli consume --topic a --output json | kafcli produce --topic b --stdin --input envelope",
        ));
}
