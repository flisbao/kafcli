#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Schema flags of `produce` and `consume` without a broker. The contexts point at a closed port and
//! the schema registry is a `FakeRegistryServer` on localhost.

use assert_cmd::Command;
use kafcli_core::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
use predicates::prelude::*;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

const AVRO_SCHEMA: &str =
    r#"{"type":"record","name":"Pedido","fields":[{"name":"id","type":"long"}]}"#;
const AZURE_SCHEMA_ID: &str = "0f1e2d3c4b5a69788796a5b4c3d2e1f0";
const AZURE_LIST_PATH: &str =
    "/$schemaGroups/pedidos/schemas/pedidos-value/versions?api-version=2023-07-01";
const AZURE_VERSION_PATH: &str =
    "/$schemaGroups/pedidos/schemas/pedidos-value/versions/1?api-version=2023-07-01";
const CONFLUENT_LATEST_PATH: &str = "/subjects/pedidos-value/versions/latest";
const CLOSED_BROKER: &str = "127.0.0.1:1";
const TOKEN_VARIABLE: &str = "KAFCLI_CLI_SCHEMAS_TOKEN";

/// Command with the configuration and state directory inside `directory`, so no test touches the
/// user's real configuration or audit log.
fn build_kafcli_command(directory: &Path) -> Command {
    let mut command = Command::cargo_bin("kafcli").unwrap();
    command
        .env_remove("KAFCLI_CONTEXT")
        .env_remove("KAFCLI_CURRENT_CONTEXT")
        .env("KAFCLI_CONFIG", directory.join("config.json"))
        .env("KAFCLI_STATE_DIR", directory.join("state"))
        .env(TOKEN_VARIABLE, "unused-test-token");
    command
}

fn add_context(directory: &Path, name: &str, extra_arguments: &[&str]) {
    build_kafcli_command(directory)
        .args(["context", "add", name, "--brokers", CLOSED_BROKER])
        .args(extra_arguments)
        .assert()
        .success();
}

fn add_confluent_context(directory: &Path, registry_url: &str) {
    add_context(
        directory,
        "dev",
        &[
            "--auth-type",
            "none",
            "--use",
            "--registry-type",
            "confluent",
            "--registry-url",
            registry_url,
        ],
    );
}

/// Azure contexts need a bearer token reference; the tests set the variable on every command.
fn add_azure_context(directory: &Path, registry_url: &str) {
    add_context(
        directory,
        "dev",
        &[
            "--auth-type",
            "none",
            "--use",
            "--registry-type",
            "azure",
            "--registry-url",
            registry_url,
            "--registry-group",
            "pedidos",
            "--registry-auth-type",
            "bearer",
            "--registry-secret-ref",
            &format!("env:{TOKEN_VARIABLE}"),
        ],
    );
}

fn write_schema_file(directory: &Path, content: &str) -> PathBuf {
    let path = directory.join("pedido.avsc");
    std::fs::write(&path, content).unwrap();
    path
}

fn route(
    method: &str,
    path: &str,
    status: u16,
    headers: &[(&str, &str)],
    body: String,
) -> FakeRoute {
    FakeRoute {
        method: method.to_string(),
        path_and_query: path.to_string(),
        status,
        headers: headers
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect(),
        body,
    }
}

/// Confluent registry that answers the latest version of `pedidos-value` with schema id 7.
fn confluent_registry() -> FakeRegistryServer {
    let body = json!({
        "subject": "pedidos-value",
        "id": 7,
        "version": 1,
        "schema": AVRO_SCHEMA,
    })
    .to_string();
    FakeRegistryServer::start(vec![route("GET", CONFLUENT_LATEST_PATH, 200, &[], body)]).unwrap()
}

/// Azure registry that answers the latest version of `pedidos-value` with the fixed schema id.
fn azure_registry() -> FakeRegistryServer {
    FakeRegistryServer::start(vec![
        route(
            "GET",
            AZURE_LIST_PATH,
            200,
            &[("Content-Type", "application/json")],
            r#"{"Value":[1]}"#.to_string(),
        ),
        route(
            "GET",
            AZURE_VERSION_PATH,
            200,
            &[
                ("Content-Type", "application/json; serialization=Avro"),
                ("Schema-Id", AZURE_SCHEMA_ID),
                ("Schema-Name", "pedidos-value"),
            ],
            AVRO_SCHEMA.to_string(),
        ),
    ])
    .unwrap()
}

fn parse_stdout_json(output: &[u8]) -> Value {
    serde_json::from_slice(output).unwrap()
}

#[test]
fn avro_azure_dry_run_shows_schema_id_and_content_type() {
    let directory = tempfile::tempdir().unwrap();
    let registry = azure_registry();
    add_azure_context(directory.path(), &registry.url());

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "produce", "--topic", "pedidos"])
        .args(["--format", "avro", "--wire", "azure"])
        .args([
            "--schema-subject",
            "pedidos-value",
            "--value",
            r#"{"id":1}"#,
        ])
        .arg("--dry-run")
        .assert()
        .success();

    let data = parse_stdout_json(&assertion.get_output().stdout)["data"].clone();
    // Avro `long` 1 is zigzag-encoded to one byte.
    assert_eq!(data["messages"][0]["value_bytes"], 1);
    let headers = data["messages"][0]["headers"].to_string();
    assert!(headers.contains("content-type"), "headers: {headers}");
    assert!(
        headers.contains(&format!("avro/binary+{AZURE_SCHEMA_ID}")),
        "headers: {headers}"
    );
    assert_eq!(data["schema_id"], AZURE_SCHEMA_ID);
}

#[test]
fn confluent_dry_run_shows_framed_hex() {
    let directory = tempfile::tempdir().unwrap();
    let registry = confluent_registry();
    add_confluent_context(directory.path(), &registry.url());

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "produce", "--topic", "pedidos"])
        .args(["--format", "avro", "--wire", "confluent"])
        .args([
            "--schema-subject",
            "pedidos-value",
            "--value",
            r#"{"id":1}"#,
        ])
        .arg("--dry-run")
        .assert()
        .success();

    let data = parse_stdout_json(&assertion.get_output().stdout)["data"].clone();
    let hex = data["messages"][0]["value_hex"].as_str().unwrap();
    assert!(hex.starts_with("0000000007"), "hex: {hex}");
    assert_eq!(data["schema_id"], "7");
}

#[test]
fn invalid_avro_value_fails_before_connecting() {
    let directory = tempfile::tempdir().unwrap();
    let schema = write_schema_file(directory.path(), AVRO_SCHEMA);
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    build_kafcli_command(directory.path())
        .args([
            "produce", "--topic", "pedidos", "--format", "avro", "--wire", "raw",
        ])
        .arg("--schema-file")
        .arg(&schema)
        .args(["--value", r#"{"id":"CANARY-9911"}"#])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "value does not match schema at '/id'",
        ))
        .stderr(predicate::str::contains("CANARY-9911").not());
}

#[test]
fn json_with_azure_wire_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let registry = azure_registry();
    add_azure_context(directory.path(), &registry.url());

    build_kafcli_command(directory.path())
        .args([
            "produce", "--topic", "pedidos", "--format", "json", "--wire", "azure",
        ])
        .args([
            "--schema-subject",
            "pedidos-value",
            "--value",
            "{}",
            "--dry-run",
        ])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "--format json with --wire azure is not supported",
        ));
}

#[test]
fn no_validate_with_avro_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let schema = write_schema_file(directory.path(), AVRO_SCHEMA);
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    build_kafcli_command(directory.path())
        .args([
            "produce", "--topic", "pedidos", "--format", "avro", "--wire", "raw",
        ])
        .arg("--schema-file")
        .arg(&schema)
        .args(["--no-validate", "--value", "{}", "--dry-run"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "--no-validate only applies to --format json",
        ));
}

#[test]
fn schema_version_requires_subject() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    build_kafcli_command(directory.path())
        .args([
            "produce",
            "--topic",
            "pedidos",
            "--format",
            "avro",
            "--wire",
            "confluent",
        ])
        .args(["--schema-version", "2", "--value", "{}", "--dry-run"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("--schema-subject"));
}

#[test]
fn schema_file_and_subject_conflict() {
    let directory = tempfile::tempdir().unwrap();
    let schema = write_schema_file(directory.path(), AVRO_SCHEMA);
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    build_kafcli_command(directory.path())
        .args([
            "produce", "--topic", "pedidos", "--format", "avro", "--wire", "raw",
        ])
        .arg("--schema-file")
        .arg(&schema)
        .args([
            "--schema-subject",
            "pedidos-value",
            "--value",
            "{}",
            "--dry-run",
        ])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn wire_confluent_without_registry_names_context() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    build_kafcli_command(directory.path())
        .args(["produce", "--topic", "pedidos", "--format", "avro", "--wire", "confluent"])
        .args(["--schema-id", "7", "--value", "{}", "--dry-run"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "context 'dev' has no schema_registry; add one with 'kafcli context add ... --registry-type'",
        ));
}

#[test]
fn json_raw_with_no_validate_works_without_registry() {
    let directory = tempfile::tempdir().unwrap();
    add_context(directory.path(), "dev", &["--auth-type", "none", "--use"]);

    build_kafcli_command(directory.path())
        .args([
            "produce", "--topic", "pedidos", "--format", "json", "--wire", "raw",
        ])
        .args(["--schema-subject", "pedidos-value", "--no-validate"])
        .args(["--value", r#"{"id":1}"#, "--dry-run"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "warning: --no-validate: values are sent without schema validation",
        ));
}

#[test]
fn consume_rejects_schema_flags_on_confluent() {
    let directory = tempfile::tempdir().unwrap();
    let registry = confluent_registry();
    add_confluent_context(directory.path(), &registry.url());

    build_kafcli_command(directory.path())
        .args([
            "consume",
            "--topic",
            "pedidos",
            "--format",
            "avro",
            "--wire",
            "confluent",
        ])
        .args(["--schema-subject", "pedidos-value", "--count", "1"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "--wire confluent reads the schema id from each message",
        ));
}

#[test]
fn schema_registry_requests_are_only_gets() {
    let directory = tempfile::tempdir().unwrap();
    let registry = confluent_registry();
    add_confluent_context(directory.path(), &registry.url());

    build_kafcli_command(directory.path())
        .args([
            "produce",
            "--topic",
            "pedidos",
            "--format",
            "avro",
            "--wire",
            "confluent",
        ])
        .args([
            "--schema-subject",
            "pedidos-value",
            "--value",
            r#"{"id":1}"#,
        ])
        .args(["--timeout-seconds", "2"])
        .assert()
        .failure();

    let requests = registry.recorded_requests();
    assert!(!requests.is_empty());
    assert!(
        requests.iter().all(|request| request.method == "GET"),
        "requests: {requests:?}"
    );
}

#[test]
fn produce_never_sends_put_or_post_to_registry() {
    let directory = tempfile::tempdir().unwrap();
    let registry = confluent_registry();
    add_confluent_context(directory.path(), &registry.url());

    build_kafcli_command(directory.path())
        .args([
            "produce",
            "--topic",
            "pedidos",
            "--format",
            "avro",
            "--wire",
            "confluent",
        ])
        .args([
            "--schema-subject",
            "pedidos-value",
            "--value",
            r#"{"id":1}"#,
        ])
        .arg("--dry-run")
        .assert()
        .success();

    let requests = registry.recorded_requests();
    assert!(!requests.is_empty());
    assert!(
        requests.iter().all(|request| request.method == "GET"),
        "requests: {requests:?}"
    );
}

#[test]
fn registry_401_never_prints_token() {
    const TOKEN: &str = "kafcli-cli-schemas-token-7c1e";
    let directory = tempfile::tempdir().unwrap();
    let registry = FakeRegistryServer::start(vec![route(
        "GET",
        CONFLUENT_LATEST_PATH,
        401,
        &[],
        r#"{"error_code":40101,"message":"unauthorized"}"#.to_string(),
    )])
    .unwrap();
    add_context(
        directory.path(),
        "dev",
        &[
            "--auth-type",
            "none",
            "--use",
            "--registry-type",
            "confluent",
            "--registry-url",
            &registry.url(),
            "--registry-auth-type",
            "bearer",
            "--registry-secret-ref",
            &format!("env:{TOKEN_VARIABLE}"),
        ],
    );

    for extra in [&["-vv"][..], &["--output", "json"][..]] {
        let assertion = build_kafcli_command(directory.path())
            .env(TOKEN_VARIABLE, TOKEN)
            .args(extra)
            .args([
                "produce",
                "--topic",
                "pedidos",
                "--format",
                "avro",
                "--wire",
                "confluent",
            ])
            .args([
                "--schema-subject",
                "pedidos-value",
                "--value",
                r#"{"id":1}"#,
            ])
            .arg("--dry-run")
            .assert()
            .code(1);
        let output = assertion.get_output();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stdout.contains(TOKEN), "stdout leaks token");
        assert!(!stderr.contains(TOKEN), "stderr leaks token");
    }

    let requests = registry.recorded_requests();
    assert!(
        requests
            .iter()
            .any(|request| request.headers.iter().any(|(name, value)| {
                name.eq_ignore_ascii_case("authorization") && value.contains(TOKEN)
            })),
        "the token was never sent: {requests:?}"
    );
}
