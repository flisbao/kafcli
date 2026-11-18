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

// ---- kafcli schema register, get and check-compat (no broker) ----

const SUBJECT: &str = "pedidos-value";
const COMPATIBILITY_PATH: &str =
    "/compatibility/subjects/pedidos-value/versions/latest?verbose=true";
const CONFLUENT_REGISTER_PATH: &str = "/subjects/pedidos-value/versions";
const AZURE_PUT_PATH: &str = "/$schemaGroups/pedidos/schemas/pedidos-value?api-version=2023-07-01";
const AVRO_WITH_DEFAULT: &str = r#"{"type":"record","name":"Pedido","fields":[{"name":"id","type":"long"},{"name":"nota","type":"string","default":""}]}"#;
const AVRO_TYPE_CHANGED: &str =
    r#"{"type":"record","name":"Pedido","fields":[{"name":"id","type":"string"}]}"#;

fn register_arguments(schema_file: &Path) -> Vec<String> {
    vec![
        "schema".to_string(),
        "register".to_string(),
        "--subject".to_string(),
        SUBJECT.to_string(),
        "--schema-file".to_string(),
        schema_file.display().to_string(),
    ]
}

/// A protected context without a registry: protected contexts refuse plain http registries, and the
/// guard runs before the registry is contacted, so no registry is needed to test the abort.
fn add_protected_context(directory: &Path) {
    add_context(
        directory,
        "prd",
        &["--auth-type", "tls", "--protected", "--use"],
    );
}

#[test]
fn register_validates_schema_locally_before_any_request() {
    let directory = tempfile::tempdir().unwrap();
    let registry = FakeRegistryServer::start(vec![]).unwrap();
    add_confluent_context(directory.path(), &registry.url());
    let broken = write_schema_file(directory.path(), r#"{"type":"record","name":"Broken""#);

    build_kafcli_command(directory.path())
        .args(register_arguments(&broken))
        .assert()
        .code(1)
        .stderr(predicate::str::contains("is not a valid Avro schema"));

    assert!(registry.recorded_requests().is_empty());
}

#[test]
fn register_on_protected_without_tty_and_yes_aborts_with_exit_3() {
    let directory = tempfile::tempdir().unwrap();
    add_protected_context(directory.path());
    let schema = write_schema_file(directory.path(), AVRO_SCHEMA);

    build_kafcli_command(directory.path())
        .args(register_arguments(&schema))
        .assert()
        .code(3)
        .stderr(predicate::str::contains(
            "confirmation required on protected context: pass --yes",
        ));

    let audit = std::fs::read_to_string(directory.path().join("state").join("audit.log")).unwrap();
    assert!(audit.contains("\"outcome\":\"aborted\""), "audit: {audit}");
    assert!(
        audit.contains("\"command\":\"schema register\""),
        "audit: {audit}"
    );
    assert!(audit.contains("subject=pedidos-value"), "audit: {audit}");
}

#[test]
fn register_audit_has_no_schema_text() {
    const MARKER: &str = "CANARY_FIELD_7731";
    let directory = tempfile::tempdir().unwrap();
    let registry = FakeRegistryServer::start(vec![route(
        "POST",
        CONFLUENT_REGISTER_PATH,
        200,
        &[],
        r#"{"id":42}"#.to_string(),
    )])
    .unwrap();
    add_confluent_context(directory.path(), &registry.url());
    let schema = write_schema_file(
        directory.path(),
        &format!(
            r#"{{"type":"record","name":"Pedido","fields":[{{"name":"{MARKER}","type":"long"}}]}}"#
        ),
    );

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json"])
        .args(register_arguments(&schema))
        .assert()
        .success();

    let data = parse_stdout_json(&assertion.get_output().stdout)["data"].clone();
    assert_eq!(data["subject"], SUBJECT);
    assert_eq!(data["schema_id"], "42");
    assert_eq!(data["type"], "avro");
    let audit = std::fs::read_to_string(directory.path().join("state").join("audit.log")).unwrap();
    assert!(audit.contains("\"outcome\":\"executed\""), "audit: {audit}");
    assert!(audit.contains("\"schema_id\":\"42\""), "audit: {audit}");
    assert!(audit.contains("subject=pedidos-value"), "audit: {audit}");
    assert!(!audit.contains(MARKER), "audit leaked schema text: {audit}");
}

#[test]
fn azure_register_sends_put_and_reports_id() {
    let directory = tempfile::tempdir().unwrap();
    let registry = FakeRegistryServer::start(vec![route(
        "PUT",
        AZURE_PUT_PATH,
        200,
        &[("Schema-Id", AZURE_SCHEMA_ID), ("Schema-Version", "1")],
        "{}".to_string(),
    )])
    .unwrap();
    add_azure_context(directory.path(), &registry.url());
    let schema = write_schema_file(directory.path(), AVRO_SCHEMA);

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json"])
        .args(register_arguments(&schema))
        .assert()
        .success();

    let data = parse_stdout_json(&assertion.get_output().stdout)["data"].clone();
    assert_eq!(data["schema_id"], AZURE_SCHEMA_ID);
    assert_eq!(data["version"], 1);
    let requests = registry.recorded_requests();
    assert_eq!(requests.len(), 1, "requests: {requests:?}");
    assert_eq!(requests[0].method, "PUT");
}

#[test]
fn azure_check_compat_is_unsupported() {
    let directory = tempfile::tempdir().unwrap();
    let registry = FakeRegistryServer::start(vec![]).unwrap();
    add_azure_context(directory.path(), &registry.url());
    let schema = write_schema_file(directory.path(), AVRO_SCHEMA);

    build_kafcli_command(directory.path())
        .args([
            "schema",
            "check-compat",
            "--subject",
            SUBJECT,
            "--schema-file",
        ])
        .arg(&schema)
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "check-compat is not supported by the Azure Schema Registry",
        ));

    assert!(registry.recorded_requests().is_empty());
}

#[test]
fn azure_register_json_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let registry = FakeRegistryServer::start(vec![]).unwrap();
    add_azure_context(directory.path(), &registry.url());
    let schema = write_schema_file(directory.path(), r#"{"type":"object"}"#);

    build_kafcli_command(directory.path())
        .args(["schema", "register", "--subject", SUBJECT, "--type", "json"])
        .arg("--schema-file")
        .arg(&schema)
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "JSON Schema on the Azure Schema Registry is not supported by kafcli",
        ));

    assert!(registry.recorded_requests().is_empty());
}

#[test]
fn get_requires_subject_or_id() {
    let directory = tempfile::tempdir().unwrap();
    add_confluent_context(directory.path(), "http://127.0.0.1:1");

    build_kafcli_command(directory.path())
        .args(["schema", "get"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("--subject").and(predicate::str::contains("--id")));

    build_kafcli_command(directory.path())
        .args(["schema", "get", "--subject", SUBJECT, "--id", "7"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn get_json_output_has_kafcli_output_envelope() {
    let directory = tempfile::tempdir().unwrap();
    let registry = FakeRegistryServer::start(vec![route(
        "GET",
        "/schemas/ids/7",
        200,
        &[],
        json!({"schema": AVRO_SCHEMA}).to_string(),
    )])
    .unwrap();
    add_confluent_context(directory.path(), &registry.url());

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "schema", "get", "--id", "7"])
        .assert()
        .success();

    let envelope = parse_stdout_json(&assertion.get_output().stdout);
    assert_eq!(envelope["kafcli_output"], 1);
    let data = &envelope["data"];
    assert_eq!(data["schema_id"], "7");
    assert_eq!(data["type"], "avro");
    assert!(data["subject"].is_null(), "data: {data}");
    assert_eq!(data["schema"], AVRO_SCHEMA);
}

#[test]
fn get_table_neutralizes_control_characters() {
    let directory = tempfile::tempdir().unwrap();
    let registry = FakeRegistryServer::start(vec![route(
        "GET",
        "/schemas/ids/7",
        200,
        &[],
        json!({"schema": "not json \u{1b}[2J"}).to_string(),
    )])
    .unwrap();
    add_confluent_context(directory.path(), &registry.url());

    build_kafcli_command(directory.path())
        .args(["schema", "get", "--id", "7"])
        .assert()
        .success()
        .stdout(predicate::str::contains("not json ?[2J"))
        .stdout(predicate::str::contains('\u{1b}').not());
}

#[test]
fn check_compat_incompatible_prints_view_and_exits_1() {
    let directory = tempfile::tempdir().unwrap();
    let registry = FakeRegistryServer::start(vec![route(
        "POST",
        COMPATIBILITY_PATH,
        200,
        &[],
        r#"{"is_compatible":false,"messages":["type changed"]}"#.to_string(),
    )])
    .unwrap();
    add_confluent_context(directory.path(), &registry.url());
    let schema = write_schema_file(directory.path(), AVRO_TYPE_CHANGED);

    let assertion = build_kafcli_command(directory.path())
        .args([
            "--output",
            "json",
            "schema",
            "check-compat",
            "--subject",
            SUBJECT,
        ])
        .arg("--schema-file")
        .arg(&schema)
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "schema is not compatible with subject 'pedidos-value' version latest",
        ));

    let data = parse_stdout_json(&assertion.get_output().stdout)["data"].clone();
    assert_eq!(data["compatible"], false);
    assert_eq!(data["version"], "latest");
    assert_eq!(data["messages"][0], "type changed");
}

#[test]
fn check_compat_compatible_exits_0() {
    let directory = tempfile::tempdir().unwrap();
    let registry = FakeRegistryServer::start(vec![route(
        "POST",
        COMPATIBILITY_PATH,
        200,
        &[],
        r#"{"is_compatible":true}"#.to_string(),
    )])
    .unwrap();
    add_confluent_context(directory.path(), &registry.url());
    let schema = write_schema_file(directory.path(), AVRO_WITH_DEFAULT);

    let assertion = build_kafcli_command(directory.path())
        .args([
            "--output",
            "json",
            "schema",
            "check-compat",
            "--subject",
            SUBJECT,
        ])
        .arg("--schema-file")
        .arg(&schema)
        .assert()
        .success();

    let data = parse_stdout_json(&assertion.get_output().stdout)["data"].clone();
    assert_eq!(data["compatible"], true);
}

#[test]
fn help_lists_examples() {
    for subcommand in ["register", "get", "check-compat"] {
        build_kafcli_command(tempfile::tempdir().unwrap().path())
            .args(["schema", subcommand, "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Examples:"))
            .stdout(predicate::str::contains(format!(
                "kafcli schema {subcommand}"
            )));
    }
}

/// Writes a configuration by hand with a protected context whose registry is `registry_url`.
/// `context add` would refuse these settings, so the file is the only way to reach the registry rules.
fn write_protected_registry_config(directory: &Path, registry_url: &str, registry_auth: Value) {
    let config = json!({
        "current_context": "prd",
        "contexts": {
            "prd": {
                "brokers": [CLOSED_BROKER],
                "auth": {"type": "tls", "ca_location": null},
                "protected": true,
                "schema_registry": {
                    "type": "confluent",
                    "url": registry_url,
                    "auth": registry_auth
                }
            }
        }
    });
    std::fs::write(directory.join("config.json"), config.to_string()).unwrap();
}

const HTTP_REFUSED: &str = "schema registry over http:// is not allowed on a protected context";
const ANONYMOUS_REFUSED: &str =
    "schema registry auth type 'none' is not allowed on a protected context";

#[test]
fn schema_get_refuses_http_registry_on_protected_context() {
    let directory = tempfile::tempdir().unwrap();
    let registry = FakeRegistryServer::start(vec![]).unwrap();
    write_protected_registry_config(
        directory.path(),
        &registry.url(),
        json!({"type": "basic", "username": "app", "secret_ref": "env:SR_PASSWORD"}),
    );

    build_kafcli_command(directory.path())
        .args(["schema", "get", "--subject", SUBJECT])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(HTTP_REFUSED));
    assert!(registry.recorded_requests().is_empty());
}

#[test]
fn schema_get_refuses_anonymous_registry_on_protected_https() {
    let directory = tempfile::tempdir().unwrap();
    // The fake speaks plain HTTP, so any request that reached it would be recorded even if it failed.
    let registry = FakeRegistryServer::start(vec![]).unwrap();
    let https_url = registry.url().replacen("http://", "https://", 1);
    write_protected_registry_config(directory.path(), &https_url, json!({"type": "none"}));

    build_kafcli_command(directory.path())
        .args(["schema", "get", "--subject", SUBJECT])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(ANONYMOUS_REFUSED));
    assert!(registry.recorded_requests().is_empty());
}

#[test]
fn produce_dry_run_refuses_http_registry_on_protected_context() {
    let directory = tempfile::tempdir().unwrap();
    let registry = FakeRegistryServer::start(vec![]).unwrap();
    write_protected_registry_config(
        directory.path(),
        &registry.url(),
        json!({"type": "basic", "username": "app", "secret_ref": "env:SR_PASSWORD"}),
    );

    build_kafcli_command(directory.path())
        .args(["produce", "--topic", "pedidos"])
        .args(["--format", "avro", "--wire", "confluent"])
        .args(["--schema-subject", SUBJECT, "--value", r#"{"id":1}"#])
        .arg("--dry-run")
        .assert()
        .code(1)
        .stderr(predicate::str::contains(HTTP_REFUSED));
    assert!(registry.recorded_requests().is_empty());
}

#[test]
fn produce_dry_run_refuses_anonymous_registry_on_protected_https() {
    let directory = tempfile::tempdir().unwrap();
    let registry = FakeRegistryServer::start(vec![]).unwrap();
    let https_url = registry.url().replacen("http://", "https://", 1);
    write_protected_registry_config(directory.path(), &https_url, json!({"type": "none"}));

    build_kafcli_command(directory.path())
        .args(["produce", "--topic", "pedidos"])
        .args(["--format", "avro", "--wire", "confluent"])
        .args(["--schema-subject", SUBJECT, "--value", r#"{"id":1}"#])
        .arg("--dry-run")
        .assert()
        .code(1)
        .stderr(predicate::str::contains(ANONYMOUS_REFUSED));
    assert!(registry.recorded_requests().is_empty());
}

const SERVICE_PRINCIPAL_SECRET_VARIABLE: &str = "KAFCLI_CLI_SCHEMAS_SP_SECRET";
const ENTRA_TENANT: &str = "tenant-1";
const ENTRA_TOKEN: &str = "entra-token-from-fake";

/// One fake server plays both Entra ID (token route) and the Azure Schema Registry.
fn entra_and_azure_registry() -> FakeRegistryServer {
    let token_body = json!({"access_token": ENTRA_TOKEN, "expires_in": 3600}).to_string();
    FakeRegistryServer::start(vec![
        route(
            "POST",
            &format!("/{ENTRA_TENANT}/oauth2/v2.0/token"),
            200,
            &[("Content-Type", "application/json")],
            token_body,
        ),
        route(
            "GET",
            &format!("/$schemaGroups/$schemas/{AZURE_SCHEMA_ID}?api-version=2023-07-01"),
            200,
            &[("Content-Type", "application/json; serialization=Avro")],
            AVRO_SCHEMA.to_string(),
        ),
    ])
    .unwrap()
}

#[test]
fn schema_get_with_azure_auth_uses_entra_token() {
    let directory = tempfile::tempdir().unwrap();
    let server = entra_and_azure_registry();
    add_context(
        directory.path(),
        "dev",
        &[
            "--auth-type",
            "azure-sp-secret",
            "--tenant-id",
            ENTRA_TENANT,
            "--client-id",
            "app-1",
            "--secret-ref",
            &format!("env:{SERVICE_PRINCIPAL_SECRET_VARIABLE}"),
            "--authority-host",
            &server.url(),
            "--use",
            "--registry-type",
            "azure",
            "--registry-url",
            &server.url(),
            "--registry-group",
            "pedidos",
            "--registry-auth-type",
            "azure",
        ],
    );

    build_kafcli_command(directory.path())
        .env(SERVICE_PRINCIPAL_SECRET_VARIABLE, "sp-client-secret")
        .args(["schema", "get", "--id", AZURE_SCHEMA_ID])
        .assert()
        .success();

    let requests = server.recorded_requests();
    let registry_request = requests
        .iter()
        .find(|request| request.path_and_query.starts_with("/$schemaGroups/"))
        .expect("registry request");
    let authorization = registry_request
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
        .map(|(_, value)| value.as_str());
    assert_eq!(
        authorization,
        Some(format!("Bearer {ENTRA_TOKEN}").as_str())
    );
}
