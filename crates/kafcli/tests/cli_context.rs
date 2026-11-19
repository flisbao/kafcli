#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests report failures by panicking"
)]

use assert_cmd::Command;
use assert_cmd::assert::Assert;
use kafcli_core::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
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

#[test]
fn use_on_protected_context_warns_and_keeps_json_stdout_clean() {
    let directory = tempfile::tempdir().unwrap();
    build_kafcli_command(directory.path())
        .args(["context", "add", "prd", "--brokers", "prd:9093"])
        .args(["--auth-type", "tls", "--protected"])
        .assert()
        .success();

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "context", "use", "prd"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "WARNING: context 'prd' is PRODUCTION",
        ));

    let stderr = String::from_utf8_lossy(&assertion.get_output().stderr).into_owned();
    assert!(!stderr.contains('\x1b'), "stderr: {stderr}");
    assert!(serde_json::from_slice::<Value>(&assertion.get_output().stdout).is_ok());
}

#[test]
fn test_on_protected_context_warns_before_connecting() {
    let directory = tempfile::tempdir().unwrap();
    build_kafcli_command(directory.path())
        .args(["context", "add", "prd", "--brokers", "127.0.0.1:1"])
        .args(["--auth-type", "tls", "--protected"])
        .assert()
        .success();

    let assertion = build_kafcli_command(directory.path())
        .args(["context", "test", "prd", "--timeout-seconds", "2"])
        .assert()
        .code(1);

    let stderr = String::from_utf8_lossy(&assertion.get_output().stderr).into_owned();
    let warning_position = stderr.find("WARNING: context 'prd' is PRODUCTION");
    let error_position = stderr.find("error:");
    assert!(warning_position.is_some(), "stderr: {stderr}");
    assert!(error_position.is_some(), "stderr: {stderr}");
    assert!(warning_position < error_position, "stderr: {stderr}");
}

#[test]
fn unprotected_context_prints_no_warning() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path(), "dev", &[]);

    build_kafcli_command(directory.path())
        .args(["context", "use", "dev"])
        .assert()
        .success()
        .stderr(predicate::str::contains("WARNING").not());
}

#[test]
fn add_with_use_on_protected_context_warns() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["context", "add", "prd", "--brokers", "prd:9093"])
        .args(["--auth-type", "tls", "--protected", "--use"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "WARNING: context 'prd' is PRODUCTION",
        ));

    let without_use = tempfile::tempdir().unwrap();
    build_kafcli_command(without_use.path())
        .args(["context", "add", "prd", "--brokers", "prd:9093"])
        .args(["--auth-type", "tls", "--protected"])
        .assert()
        .success()
        .stderr(predicate::str::contains("WARNING").not());
}

#[test]
fn add_context_with_confluent_registry_round_trips_in_show() {
    let directory = tempfile::tempdir().unwrap();
    build_kafcli_command(directory.path())
        .args(["context", "add", "dev", "--brokers", "localhost:9092"])
        .args(["--auth-type", "none"])
        .args(["--registry-type", "confluent"])
        .args(["--registry-url", "https://sr.example:8081"])
        .args([
            "--registry-auth-type",
            "basic",
            "--registry-username",
            "app",
        ])
        .args(["--registry-secret-ref", "env:SR_PASSWORD"])
        .assert()
        .success();

    let assertion = build_kafcli_command(directory.path())
        .env("SR_PASSWORD", "sr-s3cret-value")
        .args(["--output", "json", "context", "show", "dev"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assertion.get_output().stdout).into_owned();
    assert!(!stdout.contains("sr-s3cret-value"), "stdout: {stdout}");
    let json = parse_stdout_json(&assertion);
    let registry = &json["data"]["schema_registry"];
    assert_eq!(registry["registry_type"], "confluent");
    assert_eq!(registry["url"], "https://sr.example:8081");
    assert_eq!(registry["auth_type"], "basic");
    assert_eq!(registry["secret_references"], json!(["env:SR_PASSWORD"]));
}

#[test]
fn registry_flags_require_registry_type() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["context", "add", "dev", "--brokers", "localhost:9092"])
        .args(["--auth-type", "none"])
        .args(["--registry-url", "https://sr.example:8081"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("--registry-type"));
}

#[test]
fn basic_registry_auth_needs_username_and_secret() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["context", "add", "dev", "--brokers", "localhost:9092"])
        .args(["--auth-type", "none"])
        .args(["--registry-type", "confluent"])
        .args(["--registry-url", "https://sr.example:8081"])
        .args(["--registry-auth-type", "basic"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "--registry-auth-type basic needs --registry-username and --registry-secret-ref",
        ));
}

#[test]
fn list_marks_context_selected_by_environment_variable() {
    let directory = tempfile::tempdir().unwrap();
    add_plaintext_context(directory.path(), "dev", &["--use"]);
    add_plaintext_context(directory.path(), "prd", &[]);

    let assertion = build_kafcli_command(directory.path())
        .env("KAFCLI_CONTEXT", "prd")
        .args(["--output", "json", "context", "list"])
        .assert()
        .success();

    let contexts = parse_stdout_json(&assertion)["data"]["contexts"].clone();
    let is_current = |name: &str| {
        contexts
            .as_array()
            .and_then(|items| items.iter().find(|item| item["name"] == name))
            .map(|item| item["is_current"].clone())
    };
    assert_eq!(is_current("prd"), Some(json!(true)));
    assert_eq!(is_current("dev"), Some(json!(false)));
}

#[test]
fn add_bearer_context_requires_token_ref() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["context", "add", "dev-oauth", "--brokers", "kafka:9093"])
        .args(["--auth-type", "bearer"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "--auth-type bearer requires --token-ref",
        ));
}

#[test]
fn add_bearer_context_shows_token_reference() {
    let directory = tempfile::tempdir().unwrap();
    build_kafcli_command(directory.path())
        .args(["context", "add", "dev-oauth", "--brokers", "kafka:9093"])
        .args(["--auth-type", "bearer", "--token-ref", "env:KAFKA_TOKEN"])
        .assert()
        .success();

    let assertion = build_kafcli_command(directory.path())
        .env("KAFKA_TOKEN", "s3cret-token-value")
        .args(["--output", "json", "context", "show", "dev-oauth"])
        .assert()
        .success();

    let shown = parse_stdout_json(&assertion);
    assert_eq!(
        shown["data"]["secret_references"],
        json!(["env:KAFKA_TOKEN"])
    );
    let stdout = String::from_utf8_lossy(&assertion.get_output().stdout).to_string();
    assert!(!stdout.contains("s3cret-token-value"));
}

#[test]
fn add_oidc_context_requires_all_flags() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["context", "add", "dev-oidc", "--brokers", "kafka:9093"])
        .args(["--auth-type", "oauth-oidc"])
        .args(["--token-endpoint-url", "https://idp.example/token"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "--auth-type oauth-oidc requires --token-endpoint-url, --client-id and --secret-ref",
        ));
}

#[test]
fn oidc_show_lists_secret_reference_only() {
    let directory = tempfile::tempdir().unwrap();
    build_kafcli_command(directory.path())
        .args(["context", "add", "dev-oidc", "--brokers", "kafka:9093"])
        .args(["--auth-type", "oauth-oidc"])
        .args(["--token-endpoint-url", "https://idp.example/token"])
        .args(["--client-id", "kafcli", "--scope", "kafka"])
        .args(["--secret-ref", "env:KAFCLI_TEST_OIDC_SECRET"])
        .assert()
        .success();

    let assertion = build_kafcli_command(directory.path())
        .env("KAFCLI_TEST_OIDC_SECRET", "s3cret-client-value")
        .args(["--output", "json", "context", "show", "dev-oidc"])
        .assert()
        .success();

    let shown = parse_stdout_json(&assertion);
    assert_eq!(shown["data"]["auth_type"], json!("oauth-oidc"));
    assert_eq!(shown["data"]["principal"], json!("kafcli"));
    assert_eq!(
        shown["data"]["secret_references"],
        json!(["env:KAFCLI_TEST_OIDC_SECRET"])
    );
    let stdout = String::from_utf8_lossy(&assertion.get_output().stdout).to_string();
    assert!(!stdout.contains("s3cret-client-value"));
}

#[test]
fn add_azure_sp_secret_context_requires_flags() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args([
            "context",
            "add",
            "prd-eh",
            "--brokers",
            "prd-ns.servicebus.windows.net:9093",
        ])
        .args([
            "--auth-type",
            "azure-sp-secret",
            "--client-id",
            "app",
            "--secret-ref",
            "env:PRD_SP_SECRET",
        ])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "--auth-type azure-sp-secret requires --tenant-id, --client-id and --secret-ref",
        ));
}

#[test]
fn azure_sp_secret_show_lists_reference() {
    let directory = tempfile::tempdir().unwrap();
    build_kafcli_command(directory.path())
        .args([
            "context",
            "add",
            "prd-eh",
            "--brokers",
            "prd-ns.servicebus.windows.net:9093",
        ])
        .args([
            "--auth-type",
            "azure-sp-secret",
            "--tenant-id",
            "tenant-1",
            "--client-id",
            "app",
            "--secret-ref",
            "env:PRD_SP_SECRET",
            "--protected",
        ])
        .assert()
        .success();

    let assertion = build_kafcli_command(directory.path())
        .env("PRD_SP_SECRET", "s3cret-value")
        .args(["--output", "json", "context", "show", "prd-eh"])
        .assert()
        .success();

    let shown = parse_stdout_json(&assertion);
    assert_eq!(shown["data"]["auth_type"], json!("azure-sp-secret"));
    assert_eq!(shown["data"]["principal"], json!("app"));
    assert_eq!(
        shown["data"]["secret_references"],
        json!(["env:PRD_SP_SECRET"])
    );
    assert!(!String::from_utf8_lossy(&assertion.get_output().stdout).contains("s3cret-value"));
}

#[test]
fn add_azure_sp_cert_requires_certificate_location() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args([
            "context",
            "add",
            "prd-cert",
            "--brokers",
            "prd-ns.servicebus.windows.net:9093",
        ])
        .args([
            "--auth-type",
            "azure-sp-cert",
            "--tenant-id",
            "tenant-1",
            "--client-id",
            "app",
        ])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "--auth-type azure-sp-cert requires --tenant-id, --client-id and --certificate-location",
        ));
}

#[test]
fn add_workload_identity_context_without_flags() {
    let directory = tempfile::tempdir().unwrap();
    build_kafcli_command(directory.path())
        .args([
            "context",
            "add",
            "aks-eh",
            "--brokers",
            "prd-ns.servicebus.windows.net:9093",
            "--auth-type",
            "azure-workload-identity",
        ])
        .assert()
        .success();

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "context", "show", "aks-eh"])
        .assert()
        .success();

    let shown = parse_stdout_json(&assertion);
    assert_eq!(shown["data"]["auth_type"], json!("azure-workload-identity"));
    assert_eq!(shown["data"]["principal"], Value::Null);
}

#[test]
fn add_azure_default_context_without_flags() {
    let directory = tempfile::tempdir().unwrap();
    build_kafcli_command(directory.path())
        .args([
            "context",
            "add",
            "sdk-eh",
            "--brokers",
            "prd-ns.servicebus.windows.net:9093",
            "--auth-type",
            "azure-default",
        ])
        .assert()
        .success();

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "context", "show", "sdk-eh"])
        .assert()
        .success();

    let shown = parse_stdout_json(&assertion);
    assert_eq!(shown["data"]["auth_type"], json!("azure-default"));
    assert_eq!(shown["data"]["principal"], Value::Null);
}

#[test]
fn add_managed_identity_context_without_flags_shows_system_assigned() {
    let directory = tempfile::tempdir().unwrap();
    build_kafcli_command(directory.path())
        .args([
            "context",
            "add",
            "vm-eh",
            "--brokers",
            "prd-ns.servicebus.windows.net:9093",
            "--auth-type",
            "azure-managed-identity",
        ])
        .assert()
        .success();

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "context", "show", "vm-eh"])
        .assert()
        .success();

    let shown = parse_stdout_json(&assertion);
    assert_eq!(shown["data"]["auth_type"], json!("azure-managed-identity"));
    assert_eq!(
        shown["data"]["principal"],
        json!("system-assigned managed identity")
    );
}

const ENTRA_TENANT: &str = "tenant-1";
const SERVICE_PRINCIPAL_SECRET_VARIABLE: &str = "KAFCLI_CONTEXT_TEST_SP_SECRET";

/// Unsecured JWT (`alg: none`) whose `appid` claim is `app-123`, the principal `context test` reports.
const ENTRA_ACCESS_TOKEN: &str =
    "eyJhbGciOiJub25lIn0.eyJhcHBpZCI6ImFwcC0xMjMiLCJzdWIiOiJpZ25vcmVkIn0.";

#[test]
fn context_test_reports_token_principal() {
    let directory = tempfile::tempdir().unwrap();
    let entra = FakeRegistryServer::start(vec![FakeRoute {
        method: "POST".to_string(),
        path_and_query: format!("/{ENTRA_TENANT}/oauth2/v2.0/token"),
        status: 200,
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body: json!({"access_token": ENTRA_ACCESS_TOKEN, "expires_in": 3600}).to_string(),
    }])
    .unwrap();
    build_kafcli_command(directory.path())
        .env(SERVICE_PRINCIPAL_SECRET_VARIABLE, "sp-client-secret")
        .args(["context", "add", "entra", "--brokers", "127.0.0.1:1"])
        .args([
            "--auth-type",
            "azure-sp-secret",
            "--tenant-id",
            ENTRA_TENANT,
        ])
        .args([
            "--client-id",
            "app-123",
            "--scope",
            "https://eventhubs.example/.default",
        ])
        .arg("--use")
        .args([
            "--secret-ref",
            &format!("env:{SERVICE_PRINCIPAL_SECRET_VARIABLE}"),
        ])
        .args(["--authority-host", &entra.url()])
        .assert()
        .success();

    // The token is issued before the broker is contacted, so the failure comes from Kafka.
    let assertion = build_kafcli_command(directory.path())
        .env(SERVICE_PRINCIPAL_SECRET_VARIABLE, "sp-client-secret")
        .args(["context", "test", "--timeout-seconds", "2"])
        .assert()
        .code(1);

    let stderr = String::from_utf8_lossy(&assertion.get_output().stderr).to_string();
    assert!(stderr.contains("error: kafka error"), "stderr: {stderr}");
    assert!(
        !stderr.contains(ENTRA_ACCESS_TOKEN),
        "stderr leaks the token: {stderr}"
    );
    assert!(
        entra
            .recorded_requests()
            .iter()
            .any(|request| request.path_and_query == format!("/{ENTRA_TENANT}/oauth2/v2.0/token")),
        "no token request reached the fake Entra endpoint"
    );
}
