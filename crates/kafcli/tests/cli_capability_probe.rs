#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests report failures by panicking"
)]

// `CARGO_BIN_EXE_*` is not set for examples, so the tests locate `capability_probe` next to the
// test binary. `cargo test -p kafcli` builds the examples before it runs the integration tests.

use assert_cmd::Command;
use predicates::prelude::*;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const PROTECTED_CONTEXT: &str = "prd";
const UNREACHABLE_BROKER: &str = "127.0.0.1:1";

fn probe_binary() -> PathBuf {
    let test_binary = std::env::current_exe().unwrap();
    // target/<profile>/deps/<test> -> target/<profile>/examples/capability_probe
    let profile_directory = test_binary.parent().unwrap().parent().unwrap();
    profile_directory.join("examples").join("capability_probe")
}

fn write_protected_configuration(directory: &Path) -> PathBuf {
    let configuration = serde_json::json!({
        "current_context": PROTECTED_CONTEXT,
        "contexts": {
            PROTECTED_CONTEXT: {
                "brokers": [UNREACHABLE_BROKER],
                "auth": {"type": "tls"},
                "protected": true
            }
        }
    });
    let path = directory.join("config.json");
    std::fs::write(&path, configuration.to_string()).unwrap();
    path
}

fn probe_command(configuration_path: &Path) -> Command {
    let mut command = Command::new(probe_binary());
    command
        .env_remove("KAFCLI_CONFIG")
        .env_remove("KAFCLI_CONTEXT")
        .arg("--config")
        .arg(configuration_path);
    command
}

#[test]
fn probe_refuses_protected_context() {
    let directory = tempfile::tempdir().unwrap();
    let configuration_path = write_protected_configuration(directory.path());
    let started = Instant::now();

    probe_command(&configuration_path)
        .args(["--context", PROTECTED_CONTEXT, "--topic", "orders"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains(
            "error: capability probe refuses protected context 'prd'",
        ));

    assert!(
        started.elapsed() < Duration::from_secs(5),
        "refusal must not wait for a connection attempt"
    );
}

#[test]
fn dump_is_allowed_on_protected_context() {
    let directory = tempfile::tempdir().unwrap();
    let configuration_path = write_protected_configuration(directory.path());

    probe_command(&configuration_path)
        .args([
            "--context",
            PROTECTED_CONTEXT,
            "--topic",
            "orders",
            "--dump",
            "1",
            "--timeout-seconds",
            "1",
        ])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("refuses").not());
}

#[test]
fn missing_topic_flag_is_usage_error() {
    let directory = tempfile::tempdir().unwrap();
    let configuration_path = write_protected_configuration(directory.path());

    probe_command(&configuration_path)
        .args(["--context", PROTECTED_CONTEXT])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("--topic"));
}
