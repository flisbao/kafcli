#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests report failures by panicking"
)]

//! End-to-end connection tests against the Redpanda brokers of `tests/fixtures/compose.yaml`.
//!
//! Run with: `docker compose -f tests/fixtures/compose.yaml up -d --wait` and then
//! `KAFCLI_IT_SCRAM_PASSWORD=kafcli-it-scram-secret cargo test -p kafcli --test integration_connection -- --ignored`

use assert_cmd::Command;
use assert_cmd::assert::Assert;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const PLAINTEXT_BROKER: &str = "localhost:19092";
const SCRAM_BROKER: &str = "localhost:29092";
const MTLS_BROKER: &str = "localhost:39092";
const SCRAM_USERNAME: &str = "kafcli";
const SCRAM_PASSWORD: &str = "kafcli-it-scram-secret";
const SCRAM_PASSWORD_VARIABLE: &str = "KAFCLI_IT_SCRAM_PASSWORD";

fn locate_certificate_directory() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/certificates")
}

fn locate_certificate_file(file_name: &str) -> PathBuf {
    locate_certificate_directory().join(file_name)
}

fn build_kafcli_command(config_directory: &Path) -> Command {
    let mut command = Command::cargo_bin("kafcli").unwrap();
    command
        .env_remove("KAFCLI_CONFIG")
        .env_remove("KAFCLI_CONTEXT")
        .env_remove("KAFCLI_CURRENT_CONTEXT")
        .env("XDG_STATE_HOME", config_directory.join("state"))
        .arg("--config")
        .arg(config_directory.join("config.json"));
    command
}

fn parse_stdout_json(assertion: &Assert) -> Value {
    serde_json::from_slice(&assertion.get_output().stdout).unwrap()
}

fn add_context(config_directory: &Path, name: &str, arguments: &[&str]) {
    build_kafcli_command(config_directory)
        .args(["context", "add", name])
        .args(arguments)
        .assert()
        .success();
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn connects_with_plaintext() {
    let directory = tempfile::tempdir().unwrap();
    add_context(
        directory.path(),
        "plain",
        &["--brokers", PLAINTEXT_BROKER, "--auth-type", "none"],
    );

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "context", "test", "plain"])
        .assert()
        .success();

    assert!(parse_stdout_json(&assertion)["data"]["broker_count"].as_u64() >= Some(1));
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn connects_with_scram_and_reports_principal() {
    let directory = tempfile::tempdir().unwrap();
    add_context(
        directory.path(),
        "scram",
        &[
            "--brokers",
            SCRAM_BROKER,
            "--auth-type",
            "sasl-scram-256",
            "--username",
            SCRAM_USERNAME,
            "--secret-ref",
            "env:KAFCLI_IT_SCRAM_PASSWORD",
            "--no-tls",
        ],
    );

    let assertion = build_kafcli_command(directory.path())
        .env(SCRAM_PASSWORD_VARIABLE, SCRAM_PASSWORD)
        .args(["--output", "json", "context", "test", "scram"])
        .assert()
        .success();

    assert_eq!(
        parse_stdout_json(&assertion)["data"]["principal"],
        SCRAM_USERNAME
    );
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn rejects_wrong_scram_password() {
    let directory = tempfile::tempdir().unwrap();
    add_context(
        directory.path(),
        "scram",
        &[
            "--brokers",
            SCRAM_BROKER,
            "--auth-type",
            "sasl-scram-256",
            "--username",
            SCRAM_USERNAME,
            "--secret-ref",
            "env:KAFCLI_IT_SCRAM_PASSWORD",
            "--no-tls",
        ],
    );

    // Positive control: the same context succeeds with the right password, so a later failure
    // is caused by the password and not by fixtures that are down or on another port.
    build_kafcli_command(directory.path())
        .env(SCRAM_PASSWORD_VARIABLE, SCRAM_PASSWORD)
        .args(["context", "test", "scram", "--timeout-seconds", "5"])
        .assert()
        .success();

    let started = Instant::now();
    let assertion = build_kafcli_command(directory.path())
        .env(SCRAM_PASSWORD_VARIABLE, "wrong-password")
        .args(["context", "test", "scram", "--timeout-seconds", "5"])
        .assert()
        .code(1);

    assert!(started.elapsed() < Duration::from_secs(30));
    let stderr = String::from_utf8_lossy(&assertion.get_output().stderr).to_string();
    assert!(stderr.starts_with("error: kafka error"), "stderr: {stderr}");
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn connects_with_mutual_tls() {
    let directory = tempfile::tempdir().unwrap();
    let ca_location = locate_certificate_file("ca.pem");
    let client_certificate = locate_certificate_file("client.pem");
    let client_key = locate_certificate_file("client.key");
    add_context(
        directory.path(),
        "mtls",
        &[
            "--brokers",
            MTLS_BROKER,
            "--auth-type",
            "mtls",
            "--ca-location",
            ca_location.to_str().unwrap(),
            "--certificate-location",
            client_certificate.to_str().unwrap(),
            "--key-location",
            client_key.to_str().unwrap(),
        ],
    );

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "context", "test", "mtls"])
        .assert()
        .success();

    assert!(parse_stdout_json(&assertion)["data"]["broker_count"].as_u64() >= Some(1));
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn never_leaks_secret_in_any_output() {
    let directory = tempfile::tempdir().unwrap();
    add_context(
        directory.path(),
        "scram",
        &[
            "--brokers",
            SCRAM_BROKER,
            "--auth-type",
            "sasl-scram-256",
            "--username",
            SCRAM_USERNAME,
            "--secret-ref",
            "env:KAFCLI_IT_SCRAM_PASSWORD",
            "--no-tls",
        ],
    );

    // Output is checked for leaks before the exit code, so a leak is reported even when the run fails.
    let assertion = build_kafcli_command(directory.path())
        .env(SCRAM_PASSWORD_VARIABLE, SCRAM_PASSWORD)
        .args(["-vv", "--output", "json", "context", "test", "scram"])
        .assert();

    let mut captured = assertion.get_output().stdout.clone();
    captured.extend_from_slice(&assertion.get_output().stderr);
    // `context test` writes no audit log today, so this state-dir scan is a forward guard that
    // starts to matter once an audit writer exists.
    captured.extend_from_slice(&read_all_files(&directory.path().join("state")));
    assert!(!String::from_utf8_lossy(&captured).contains(SCRAM_PASSWORD));

    assertion.success();
}

/// Concatenates every file below `directory` (the audit log, if any); a missing directory is empty.
fn read_all_files(directory: &Path) -> Vec<u8> {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .flat_map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                read_all_files(&path)
            } else {
                std::fs::read(&path).unwrap_or_default()
            }
        })
        .collect()
}
