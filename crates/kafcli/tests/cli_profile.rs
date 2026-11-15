#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use assert_cmd::Command;
use assert_cmd::assert::Assert;
use predicates::prelude::*;
use serde_json::{Value, json};
use std::path::Path;

fn kafcli(config_directory: &Path) -> Command {
    let mut command = Command::cargo_bin("kafcli").unwrap();
    command
        .env_remove("KAFCLI_CONFIG")
        .env_remove("KAFCLI_DEFAULT_PROFILE")
        .arg("--config")
        .arg(config_directory.join("config.toml"));
    command
}

fn stdout_json(assertion: &Assert) -> Value {
    serde_json::from_slice(&assertion.get_output().stdout).unwrap()
}

fn add_plaintext_profile(config_directory: &Path, name: &str, extra_arguments: &[&str]) {
    let mut command = kafcli(config_directory);
    command
        .args(["profile", "add", name, "--brokers", "localhost:9092"])
        .args(["--auth-type", "none"])
        .args(extra_arguments)
        .assert()
        .success();
}

#[test]
fn list_on_first_use_shows_no_profiles() {
    let directory = tempfile::tempdir().unwrap();

    let assertion = kafcli(directory.path())
        .args(["--output", "json", "profile", "list"])
        .assert()
        .success();

    assert_eq!(stdout_json(&assertion)["data"]["profiles"], json!([]));
}

#[test]
fn add_then_list_shows_profile() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_profile(directory.path(), "dev", &["--set-default"]);

    let assertion = kafcli(directory.path())
        .args(["--output", "json", "profile", "list"])
        .assert()
        .success();

    assert_eq!(
        stdout_json(&assertion)["data"]["profiles"][0],
        json!({
            "name": "dev",
            "brokers": ["localhost:9092"],
            "auth_type": "none",
            "protected": false,
            "is_default": true
        })
    );
}

#[test]
fn json_output_carries_schema_version() {
    let directory = tempfile::tempdir().unwrap();

    let assertion = kafcli(directory.path())
        .args(["--output", "json", "profile", "list"])
        .assert()
        .success();

    assert_eq!(stdout_json(&assertion)["kafcli_output"], 1);
}

#[test]
fn add_refuses_to_overwrite_without_force() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_profile(directory.path(), "dev", &[]);

    kafcli(directory.path())
        .args(["profile", "add", "dev", "--brokers", "other:9092"])
        .args(["--auth-type", "none"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("already exists"));
}

#[test]
fn add_requires_credentials_for_sasl() {
    let directory = tempfile::tempdir().unwrap();

    kafcli(directory.path())
        .args(["profile", "add", "prd", "--brokers", "prd:9093"])
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
fn add_rejects_plaintext_on_protected_profile() {
    let directory = tempfile::tempdir().unwrap();

    kafcli(directory.path())
        .args(["profile", "add", "prd", "--brokers", "prd:9092"])
        .args(["--auth-type", "none", "--protected"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "not allowed on a protected profile",
        ));
}

#[test]
fn show_never_prints_resolved_secret() {
    let directory = tempfile::tempdir().unwrap();
    kafcli(directory.path())
        .args(["profile", "add", "prd", "--brokers", "prd:9093"])
        .args(["--auth-type", "sasl-scram-512", "--username", "app"])
        .args(["--secret-ref", "env:KAFKA_PASSWORD", "--protected"])
        .assert()
        .success();

    let text_assertion = kafcli(directory.path())
        .env("KAFKA_PASSWORD", "s3cret-value")
        .args(["profile", "show", "prd", "-vv"])
        .assert()
        .success()
        .stdout(predicate::str::contains("env:KAFKA_PASSWORD"));
    let text_combined = [
        text_assertion.get_output().stdout.as_slice(),
        text_assertion.get_output().stderr.as_slice(),
    ]
    .concat();
    assert!(!String::from_utf8_lossy(&text_combined).contains("s3cret-value"));

    let json_assertion = kafcli(directory.path())
        .env("KAFKA_PASSWORD", "s3cret-value")
        .args(["--output", "json", "profile", "show", "prd"])
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
fn use_sets_default_profile() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_profile(directory.path(), "dev", &[]);
    add_plaintext_profile(directory.path(), "local", &[]);

    kafcli(directory.path())
        .args(["profile", "use", "local"])
        .assert()
        .success();

    let assertion = kafcli(directory.path())
        .args(["--output", "json", "profile", "list"])
        .assert()
        .success();
    let profiles = stdout_json(&assertion)["data"]["profiles"].clone();
    assert_eq!(profiles[0]["name"], "dev");
    assert_eq!(profiles[0]["is_default"], false);
    assert_eq!(profiles[1]["name"], "local");
    assert_eq!(profiles[1]["is_default"], true);
}

#[test]
fn unknown_profile_exits_with_failure() {
    let directory = tempfile::tempdir().unwrap();

    kafcli(directory.path())
        .args(["profile", "show", "nope"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("profile 'nope' not found"));
}

#[test]
fn every_profile_subcommand_help_has_examples() {
    for subcommand in ["list", "show", "add", "use", "test"] {
        Command::cargo_bin("kafcli")
            .unwrap()
            .args(["profile", subcommand, "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Examples:"));
    }
}
