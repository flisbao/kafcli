#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use assert_cmd::Command;
use assert_cmd::assert::Assert;
use predicates::prelude::*;
use serde_json::{Value, json};
use std::path::Path;

fn build_kafcli_command(config_directory: &Path) -> Command {
    let mut command = Command::cargo_bin("kafcli").unwrap();
    command
        .env_remove("KAFCLI_CONFIG")
        .env_remove("KAFCLI_CONTEXT")
        .env_remove("KAFCLI_CURRENT_CONTEXT")
        .arg("--config")
        .arg(config_directory.join("config.json"));
    command
}

fn parse_stdout_json(assertion: &Assert) -> Value {
    serde_json::from_slice(&assertion.get_output().stdout).unwrap()
}

fn add_plaintext_context(config_directory: &Path, name: &str, extra_arguments: &[&str]) {
    let mut command = build_kafcli_command(config_directory);
    command
        .args(["context", "add", name, "--brokers", "localhost:9092"])
        .args(["--auth-type", "none"])
        .args(extra_arguments)
        .assert()
        .success();
}

#[test]
fn list_on_first_use_shows_no_contexts() {
    let directory = tempfile::tempdir().unwrap();

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "context", "list"])
        .assert()
        .success();

    assert_eq!(parse_stdout_json(&assertion)["data"]["contexts"], json!([]));
}

#[test]
fn add_then_list_shows_context() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path(), "dev", &["--use"]);

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "context", "list"])
        .assert()
        .success();

    assert_eq!(
        parse_stdout_json(&assertion)["data"]["contexts"][0],
        json!({
            "name": "dev",
            "brokers": ["localhost:9092"],
            "auth_type": "none",
            "protected": false,
            "is_current": true
        })
    );
}

#[test]
fn json_output_carries_schema_version() {
    let directory = tempfile::tempdir().unwrap();

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "context", "list"])
        .assert()
        .success();

    assert_eq!(parse_stdout_json(&assertion)["kafcli_output"], 1);
}

#[test]
fn add_refuses_to_overwrite_without_force() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path(), "dev", &[]);

    build_kafcli_command(directory.path())
        .args(["context", "add", "dev", "--brokers", "other:9092"])
        .args(["--auth-type", "none"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "context 'dev' already exists; pass --force to replace it",
        ));
}

#[test]
fn add_requires_credentials_for_sasl() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["context", "add", "prd", "--brokers", "prd:9093"])
        .args([
            "--auth-type",
            "sasl-scram-512",
            "--secret-ref",
            "env:KAFKA_PASSWORD",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--username"));
}

#[test]
fn add_rejects_plaintext_on_protected_context() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["context", "add", "prd", "--brokers", "prd:9092"])
        .args(["--auth-type", "none", "--protected"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "not allowed on a protected context",
        ));
}

#[test]
fn show_never_prints_resolved_secret() {
    let directory = tempfile::tempdir().unwrap();
    build_kafcli_command(directory.path())
        .args(["context", "add", "prd", "--brokers", "prd:9093"])
        .args(["--auth-type", "sasl-scram-512", "--username", "app"])
        .args(["--secret-ref", "env:KAFKA_PASSWORD", "--protected"])
        .assert()
        .success();

    let text_assertion = build_kafcli_command(directory.path())
        .env("KAFKA_PASSWORD", "s3cret-value")
        .args(["context", "show", "prd", "-vv"])
        .assert()
        .success()
        .stdout(predicate::str::contains("env:KAFKA_PASSWORD"));
    let text_combined = [
        text_assertion.get_output().stdout.as_slice(),
        text_assertion.get_output().stderr.as_slice(),
    ]
    .concat();
    assert!(!String::from_utf8_lossy(&text_combined).contains("s3cret-value"));

    let json_assertion = build_kafcli_command(directory.path())
        .env("KAFKA_PASSWORD", "s3cret-value")
        .args(["--output", "json", "context", "show", "prd"])
        .assert()
        .success()
        .stdout(predicate::str::contains("env:KAFKA_PASSWORD"));
    let json_combined = [
        json_assertion.get_output().stdout.as_slice(),
        json_assertion.get_output().stderr.as_slice(),
    ]
    .concat();
    assert!(!String::from_utf8_lossy(&json_combined).contains("s3cret-value"));
}

#[test]
fn use_sets_current_context() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path(), "dev", &[]);
    add_plaintext_context(directory.path(), "local", &[]);

    build_kafcli_command(directory.path())
        .args(["context", "use", "local"])
        .assert()
        .success();

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "context", "list"])
        .assert()
        .success();
    let contexts = parse_stdout_json(&assertion)["data"]["contexts"].clone();
    assert_eq!(contexts[0]["name"], "dev");
    assert_eq!(contexts[0]["is_current"], false);
    assert_eq!(contexts[1]["name"], "local");
    assert_eq!(contexts[1]["is_current"], true);
}

#[test]
fn current_prints_current_context() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path(), "dev", &["--use"]);

    build_kafcli_command(directory.path())
        .args(["context", "current"])
        .assert()
        .success()
        .stdout("dev\n");
}

#[test]
fn current_without_selection_fails() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["context", "current"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("no context selected"));
}

#[test]
fn context_flag_overrides_current_context() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path(), "dev", &["--use"]);
    add_plaintext_context(directory.path(), "prd", &[]);

    let assertion = build_kafcli_command(directory.path())
        .args(["--context", "prd", "--output", "json", "context", "show"])
        .assert()
        .success();

    assert_eq!(parse_stdout_json(&assertion)["data"]["name"], "prd");
}

#[test]
fn environment_variable_selects_context() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path(), "dev", &["--use"]);
    add_plaintext_context(directory.path(), "prd", &[]);

    let from_environment = build_kafcli_command(directory.path())
        .env("KAFCLI_CONTEXT", "prd")
        .args(["--output", "json", "context", "show"])
        .assert()
        .success();
    assert_eq!(parse_stdout_json(&from_environment)["data"]["name"], "prd");

    let flag_wins = build_kafcli_command(directory.path())
        .env("KAFCLI_CONTEXT", "prd")
        .args(["--context", "dev", "--output", "json", "context", "show"])
        .assert()
        .success();
    assert_eq!(parse_stdout_json(&flag_wins)["data"]["name"], "dev");
}

#[test]
fn environment_variable_naming_missing_context_fails() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .env("KAFCLI_CONTEXT", "ghost")
        .args(["context", "show"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("context 'ghost' not found"));
}

#[test]
fn unknown_context_exits_with_failure() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["context", "show", "nope"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("context 'nope' not found"));
}

#[test]
fn profile_subcommand_is_gone() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["profile", "list"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("unrecognized subcommand"));
}

#[test]
fn list_marks_current_and_production() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path(), "dev", &["--use"]);
    build_kafcli_command(directory.path())
        .args(["context", "add", "prd", "--brokers", "prd:9092"])
        .args(["--auth-type", "tls", "--protected"])
        .assert()
        .success();

    let assertion = build_kafcli_command(directory.path())
        .args(["context", "list"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assertion.get_output().stdout).to_string();
    let dev_line = stdout
        .lines()
        .find(|line| line.contains("dev"))
        .unwrap_or_default();
    let prd_line = stdout
        .lines()
        .find(|line| line.contains("prd"))
        .unwrap_or_default();
    assert!(dev_line.contains('*'), "stdout: {stdout}");
    assert!(prd_line.contains("PRODUCTION"), "stdout: {stdout}");
}

#[test]
fn every_context_subcommand_help_has_examples() {
    for subcommand in ["list", "show", "add", "use", "current", "test"] {
        Command::cargo_bin("kafcli")
            .unwrap()
            .args(["context", subcommand, "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Examples:"));
    }
}

#[test]
fn test_against_unreachable_broker_fails_fast() {
    let directory = tempfile::tempdir().unwrap();
    build_kafcli_command(directory.path())
        .args(["context", "add", "down", "--brokers", "127.0.0.1:1"])
        .args(["--auth-type", "none", "--use"])
        .assert()
        .success();

    let started = std::time::Instant::now();
    let assertion = build_kafcli_command(directory.path())
        .args(["context", "test", "--timeout-seconds", "2"])
        .assert()
        .code(1);

    assert!(started.elapsed() < std::time::Duration::from_secs(10));
    let stderr = String::from_utf8_lossy(&assertion.get_output().stderr).to_string();
    assert!(stderr.starts_with("error: kafka error"), "stderr: {stderr}");
}

#[test]
fn add_does_not_persist_environment_defaults() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("config.json");

    build_kafcli_command(directory.path())
        .env("KAFCLI_CURRENT_CONTEXT", "prd")
        .args(["context", "add", "dev", "--brokers", "localhost:9092"])
        .args(["--auth-type", "none"])
        .assert()
        .success();

    let written = std::fs::read_to_string(config_path).unwrap();
    assert!(!written.contains("current_context"), "written: {written}");
}

#[test]
fn use_does_not_persist_environment_defaults() {
    let directory = tempfile::tempdir().unwrap();
    let config_path = directory.path().join("config.json");
    add_plaintext_context(directory.path(), "dev", &[]);
    add_plaintext_context(directory.path(), "local", &[]);

    build_kafcli_command(directory.path())
        .env("KAFCLI_CURRENT_CONTEXT", "prd")
        .args(["context", "use", "local"])
        .assert()
        .success();

    let written = std::fs::read_to_string(config_path).unwrap();
    assert!(
        written.contains("\"current_context\": \"local\""),
        "written: {written}"
    );
    assert!(!written.contains("prd"), "written: {written}");
}

#[test]
fn unknown_subcommand_is_usage_failure_with_exit_code_one() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["context", "nonsense"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("unrecognized subcommand"));
}

#[test]
fn usage_error_keeps_clap_line_breaks_on_stderr() {
    let directory = tempfile::tempdir().unwrap();

    let assertion = build_kafcli_command(directory.path())
        .args(["context", "nonsense"])
        .assert()
        .code(1);

    let stderr = String::from_utf8_lossy(&assertion.get_output().stderr).into_owned();
    assert!(stderr.lines().count() > 1, "stderr: {stderr}");
    assert!(!stderr.contains("??"), "stderr: {stderr}");
}

#[test]
fn help_exits_zero_and_prints_to_stdout() {
    Command::cargo_bin("kafcli")
        .unwrap()
        .args(["context", "add", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--auth-type"));
}

#[test]
fn test_rejects_zero_timeout_seconds() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["context", "test", "--timeout-seconds", "0"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("not in 1..=3600"));
}

#[test]
fn add_rejects_empty_broker_entries() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["context", "add", "dev", "--brokers", "a:9092,,b:9092"])
        .args(["--auth-type", "none"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "context has an empty broker address",
        ));
}
