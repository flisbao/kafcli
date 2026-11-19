#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests report failures by panicking"
)]

//! Avro and JSON Schema through the real binary, against the Redpanda plaintext broker and Schema
//! Registry of `tests/fixtures/compose.yaml`. The Azure cases use a local fake registry instead.
//!
//! Run with: `docker compose -f tests/fixtures/compose.yaml up -d --wait` and then
//! `cargo test -p kafcli --test integration_schemas_cli -- --ignored --test-threads=1`

use assert_cmd::Command;
use kafcli_core::config::model::{AuthConfig, ConnectionContext};
use kafcli_core::config::schema_registry_model::{
    ConfluentRegistryConfig, RegistryAuthConfig, SchemaRegistryConfig,
};
use kafcli_core::connection::client_config_builder::ConnectionSettings;
use kafcli_core::message::binary_text::decode_base64;
use kafcli_core::ops::produce::message_producer::{
    PreparedMessage, ProduceOptions, produce_messages,
};
use kafcli_core::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
use kafcli_core::registry::registered_schema::{SchemaId, SchemaKind};
use kafcli_core::registry::registry_factory::build_schema_registry;
use kafcli_core::secret::environment_resolver::EnvironmentSecretResolver;
use predicates::prelude::*;
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PLAINTEXT_BROKER: &str = "127.0.0.1:19092";
const REGISTRY_HOST: &str = "127.0.0.1:18081";
const REGISTRY_URL: &str = "http://127.0.0.1:18081";
const TOKEN_VARIABLE: &str = "KAFCLI_IT_REGISTRY_TOKEN";
const AVRO_RECORD: &str =
    r#"{"type":"record","name":"Pedido","fields":[{"name":"id","type":"long"}]}"#;
const JSON_SCHEMA: &str =
    r#"{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"]}"#;
/// Recursive record: each node points to the next one through a union with `null`.
const NESTED_RECORD: &str = r#"{"type":"record","name":"Node","fields":[{"name":"value","type":"long"},{"name":"next","type":["null","Node"]}]}"#;
/// Deeper than the decoder's 64-level limit.
const NESTING_LEVELS: usize = 100;
const AZURE_GROUP: &str = "pedidos";
const AZURE_SUBJECT: &str = "pedidos-value";
const AZURE_SCHEMA_ID: &str = "azure-it-schema-1";
const AZURE_API_VERSION: &str = "?api-version=2023-07-01";
const MISSING_CONTENT_TYPE: &str = "value has no content-type header with avro/binary+<schema id>; the legacy 36-byte prefix format is not supported";

fn compose_file() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/compose.yaml")
}

fn run_rpk(arguments: &[&str]) {
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
}

fn unique_name(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    format!("{prefix}-{millis}")
}

/// Deletes its topic when dropped, so a failing assertion does not leave the topic behind.
struct TopicGuard {
    name: String,
}

impl TopicGuard {
    fn create(name: &str) -> Self {
        run_rpk(&["topic", "create", name, "-p", "1"]);
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
        .env("KAFCLI_STATE_DIR", directory.join("state"))
        .env(TOKEN_VARIABLE, "it-registry-token");
    command
}

/// Plaintext context to the fixture broker, with the given registry flags, made current.
fn add_context(directory: &Path, registry_arguments: &[&str]) {
    build_kafcli_command(directory)
        .args([
            "context",
            "add",
            "it",
            "--brokers",
            PLAINTEXT_BROKER,
            "--auth-type",
            "none",
            "--use",
        ])
        .args(registry_arguments)
        .assert()
        .success();
}

fn add_confluent_context(directory: &Path) {
    add_context(
        directory,
        &[
            "--registry-type",
            "confluent",
            "--registry-url",
            REGISTRY_URL,
        ],
    );
}

fn add_azure_context(directory: &Path, url: &str) {
    add_context(
        directory,
        &[
            "--registry-type",
            "azure",
            "--registry-url",
            url,
            "--registry-group",
            AZURE_GROUP,
            "--registry-auth-type",
            "bearer",
            "--registry-secret-ref",
            &format!("env:{TOKEN_VARIABLE}"),
        ],
    );
}

/// Sends `GET path` to the fixture registry and returns the status and body.
fn registry_get(path: &str) -> std::io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(REGISTRY_HOST)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let status = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    Ok((status, body))
}

/// Waits until the fixture registry answers `GET /subjects` with 200, for up to 30 seconds.
/// The 500 ms pause between attempts is the readiness poll interval required by the test plan; it is
/// the only sleep in the test suite and has no client timeout to wait on.
#[allow(clippy::disallowed_methods, reason = "test-only sleep (ADR 0004)")]
fn wait_for_registry() {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if matches!(registry_get("/subjects"), Ok((200, _))) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "schema registry at {REGISTRY_URL} did not answer 200 within 30 seconds"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Registers a schema with the core Confluent client, never with `produce`. Returns the registry id.
fn register_confluent_schema(subject: &str, kind: SchemaKind, text: &str) -> u32 {
    wait_for_registry();
    let context = ConnectionContext {
        brokers: vec!["localhost:9092".to_string()],
        auth: AuthConfig::Plaintext,
        protected: false,
        schema_registry: Some(SchemaRegistryConfig::Confluent(ConfluentRegistryConfig {
            url: REGISTRY_URL.to_string(),
            auth: RegistryAuthConfig::Anonymous,
            ca_location: None,
        })),
    };
    let registry = build_schema_registry(
        &context,
        &EnvironmentSecretResolver::from_process_environment(),
        Duration::from_secs(10),
    )
    .unwrap();
    let registered = registry.register_schema(subject, kind, text).unwrap();
    match registered.id {
        SchemaId::Confluent(number) => number,
        other => panic!("unexpected id {other:?}"),
    }
}

/// Writes NDJSON with one JSON document per value.
fn write_values_file(directory: &Path, values: &[Value]) -> PathBuf {
    let path = directory.join("values.ndjson");
    let content: String = values.iter().map(|value| format!("{value}\n")).collect();
    std::fs::write(&path, content).unwrap();
    path
}

fn write_schema_file(directory: &Path, name: &str, text: &str) -> PathBuf {
    let path = directory.join(name);
    std::fs::write(&path, text).unwrap();
    path
}

fn parse_envelopes(bytes: &[u8]) -> Vec<Value> {
    String::from_utf8_lossy(bytes)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn consume_json(directory: &Path, topic: &str, arguments: &[&str]) -> Vec<Value> {
    let assertion = build_kafcli_command(directory)
        .args(["--output", "json", "consume", "--topic", topic])
        .args(arguments)
        .assert()
        .success();
    parse_envelopes(&assertion.get_output().stdout)
}

fn produce_values(directory: &Path, topic: &str, arguments: &[&str], values: &[Value]) {
    let file = write_values_file(directory, values);
    build_kafcli_command(directory)
        .args(["produce", "--topic", topic])
        .args(arguments)
        .arg("--file")
        .arg(&file)
        .assert()
        .success();
}

/// Key and custom header that every replay test sends, so a replay that drops them fails.
const REPLAY_KEY: &str = "order-it-7";
const REPLAY_HEADER: &str = "trace-id=trace-it-7";

/// Produces one message that carries `REPLAY_KEY` and `REPLAY_HEADER`.
fn produce_keyed_message(directory: &Path, topic: &str, arguments: &[&str], value: &Value) {
    build_kafcli_command(directory)
        .args(["produce", "--topic", topic])
        .args(arguments)
        .args(["--key", REPLAY_KEY, "--header", REPLAY_HEADER])
        .args(["--value", &value.to_string()])
        .assert()
        .success();
}

fn assert_key_and_trace_header(envelope: &Value) {
    assert_eq!(envelope["key"], REPLAY_KEY);
    assert_eq!(
        header_values(envelope, "trace-id"),
        vec!["trace-it-7".to_string()]
    );
}

fn header_values(envelope: &Value, name: &str) -> Vec<String> {
    envelope["headers"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|header| header["name"] == name)
        .map(|header| header["value"].as_str().unwrap_or_default().to_string())
        .collect()
}

#[test]
#[ignore]
fn confluent_avro_round_trip() {
    let directory = tempfile::tempdir().unwrap();
    let subject = unique_name("pedidos-confluent");
    let topic = unique_name("it-confluent-avro");
    let id = register_confluent_schema(&subject, SchemaKind::Avro, AVRO_RECORD);
    let _topic = TopicGuard::create(&topic);
    add_confluent_context(directory.path());
    let values = [json!({"id": 1}), json!({"id": 2})];

    produce_values(
        directory.path(),
        &topic,
        &[
            "--format",
            "avro",
            "--wire",
            "confluent",
            "--schema-subject",
            &subject,
        ],
        &values,
    );

    let decoded = consume_json(
        directory.path(),
        &topic,
        &["--format", "avro", "--wire", "confluent", "--count", "2"],
    );
    let decoded_values: Vec<Value> = decoded
        .iter()
        .map(|envelope| envelope["value"].clone())
        .collect();
    assert_eq!(decoded_values, values.to_vec());

    let raw = consume_json(
        directory.path(),
        &topic,
        &["--format", "json", "--wire", "raw", "--count", "1"],
    );
    let bytes = decode_base64(raw[0]["value_base64"].as_str().unwrap()).unwrap();
    let mut prefix = vec![0u8];
    prefix.extend(id.to_be_bytes());
    assert_eq!(&bytes[..5], prefix.as_slice());
}

#[test]
#[ignore]
fn confluent_json_schema_rejects_invalid_value() {
    let directory = tempfile::tempdir().unwrap();
    let subject = unique_name("pedidos-json");
    let topic = unique_name("it-confluent-json");
    register_confluent_schema(&subject, SchemaKind::Json, JSON_SCHEMA);
    let _topic = TopicGuard::create(&topic);
    add_confluent_context(directory.path());
    let file = write_values_file(directory.path(), &[json!({"id": "CANARY-5521"})]);

    build_kafcli_command(directory.path())
        .args(["produce", "--topic", &topic])
        .args([
            "--format",
            "json",
            "--wire",
            "confluent",
            "--schema-subject",
            &subject,
        ])
        .arg("--file")
        .arg(&file)
        .assert()
        .code(1)
        .stderr(predicate::str::contains("CANARY-5521").not());

    let received = consume_json(
        directory.path(),
        &topic,
        &["--format", "json", "--wire", "raw"],
    );
    assert!(received.is_empty(), "received: {received:?}");
}

#[test]
#[ignore]
fn raw_avro_with_schema_file_round_trip() {
    let directory = tempfile::tempdir().unwrap();
    let topic = unique_name("it-raw-avro");
    let schema = write_schema_file(directory.path(), "pedido.avsc", AVRO_RECORD);
    let _topic = TopicGuard::create(&topic);
    add_context(directory.path(), &[]);
    let values = [json!({"id": 10}), json!({"id": 11})];
    let schema_path = schema.to_string_lossy().to_string();
    let schema_arguments = [
        "--format",
        "avro",
        "--wire",
        "raw",
        "--schema-file",
        schema_path.as_str(),
    ];

    produce_values(directory.path(), &topic, &schema_arguments, &values);

    let mut consume_arguments = schema_arguments.to_vec();
    consume_arguments.extend(["--count", "2"]);
    let decoded = consume_json(directory.path(), &topic, &consume_arguments);
    let decoded_values: Vec<Value> = decoded
        .iter()
        .map(|envelope| envelope["value"].clone())
        .collect();
    assert_eq!(decoded_values, values.to_vec());
}

/// Fake Azure registry with the subject `pedidos-value` at version 1 and the fixed schema id.
fn azure_registry() -> FakeRegistryServer {
    let schema_headers = vec![
        (
            "Content-Type".to_string(),
            "application/json; serialization=Avro".to_string(),
        ),
        ("Schema-Id".to_string(), AZURE_SCHEMA_ID.to_string()),
        ("Schema-Name".to_string(), AZURE_SUBJECT.to_string()),
    ];
    let route = |path: String, body: &str| FakeRoute {
        method: "GET".to_string(),
        path_and_query: path,
        status: 200,
        headers: schema_headers.clone(),
        body: body.to_string(),
    };
    FakeRegistryServer::start(vec![
        FakeRoute {
            method: "GET".to_string(),
            path_and_query: format!(
                "/$schemaGroups/{AZURE_GROUP}/schemas/{AZURE_SUBJECT}/versions{AZURE_API_VERSION}"
            ),
            status: 200,
            headers: vec![("Content-Type".to_string(), "application/json".to_string())],
            body: r#"{"Value":[1]}"#.to_string(),
        },
        route(
            format!(
                "/$schemaGroups/{AZURE_GROUP}/schemas/{AZURE_SUBJECT}/versions/1{AZURE_API_VERSION}"
            ),
            AVRO_RECORD,
        ),
        route(
            format!("/$schemaGroups/$schemas/{AZURE_SCHEMA_ID}{AZURE_API_VERSION}"),
            AVRO_RECORD,
        ),
    ])
    .unwrap()
}

#[test]
#[ignore]
fn azure_avro_round_trip_with_fake_registry() {
    let directory = tempfile::tempdir().unwrap();
    let topic = unique_name("it-azure-avro");
    let registry = azure_registry();
    let _topic = TopicGuard::create(&topic);
    add_azure_context(directory.path(), &registry.url());
    let values = [json!({"id": 21}), json!({"id": 22})];
    let azure_arguments = [
        "--format",
        "avro",
        "--wire",
        "azure",
        "--schema-subject",
        AZURE_SUBJECT,
    ];

    produce_values(directory.path(), &topic, &azure_arguments, &values);

    let decoded = consume_json(
        directory.path(),
        &topic,
        &["--format", "avro", "--wire", "azure", "--count", "2"],
    );
    let decoded_values: Vec<Value> = decoded
        .iter()
        .map(|envelope| envelope["value"].clone())
        .collect();
    assert_eq!(decoded_values, values.to_vec());
    let content_type = format!("avro/binary+{AZURE_SCHEMA_ID}");
    for envelope in &decoded {
        assert_eq!(
            header_values(envelope, "content-type"),
            vec![content_type.clone()]
        );
    }
}

#[test]
#[ignore]
fn azure_envelope_replay_keeps_single_content_type() {
    let directory = tempfile::tempdir().unwrap();
    let source = unique_name("it-azure-source");
    let target = unique_name("it-azure-target");
    let registry = azure_registry();
    let _source = TopicGuard::create(&source);
    let _target = TopicGuard::create(&target);
    add_azure_context(directory.path(), &registry.url());
    produce_keyed_message(
        directory.path(),
        &source,
        &[
            "--format",
            "avro",
            "--wire",
            "azure",
            "--schema-subject",
            AZURE_SUBJECT,
        ],
        &json!({"id": 31}),
    );
    let envelopes = consume_json(
        directory.path(),
        &source,
        &["--format", "avro", "--wire", "azure", "--count", "1"],
    );
    let replay: String = envelopes
        .iter()
        .map(|envelope| format!("{envelope}\n"))
        .collect();

    build_kafcli_command(directory.path())
        .args(["produce", "--topic", &target])
        .args([
            "--format",
            "avro",
            "--wire",
            "azure",
            "--schema-subject",
            AZURE_SUBJECT,
        ])
        .args(["--input", "envelope", "--stdin"])
        .write_stdin(replay.into_bytes())
        .assert()
        .success();

    let replayed = consume_json(
        directory.path(),
        &target,
        &["--format", "avro", "--wire", "azure", "--count", "1"],
    );
    assert_eq!(replayed[0]["value"], json!({"id": 31}));
    assert_key_and_trace_header(&replayed[0]);
    assert_eq!(
        header_values(&replayed[0], "content-type"),
        vec![format!("avro/binary+{AZURE_SCHEMA_ID}")]
    );
}

#[test]
#[ignore]
fn confluent_avro_envelope_replay_keeps_key_and_headers() {
    let directory = tempfile::tempdir().unwrap();
    let subject = unique_name("pedidos-replay");
    let source = unique_name("it-confluent-replay-source");
    let target = unique_name("it-confluent-replay-target");
    register_confluent_schema(&subject, SchemaKind::Avro, AVRO_RECORD);
    let _source = TopicGuard::create(&source);
    let _target = TopicGuard::create(&target);
    add_confluent_context(directory.path());
    let confluent_arguments = [
        "--format",
        "avro",
        "--wire",
        "confluent",
        "--schema-subject",
        &subject,
    ];
    produce_keyed_message(
        directory.path(),
        &source,
        &confluent_arguments,
        &json!({"id": 41}),
    );
    let envelopes = consume_json(
        directory.path(),
        &source,
        &["--format", "avro", "--wire", "confluent", "--count", "1"],
    );
    assert_key_and_trace_header(&envelopes[0]);
    let replay: String = envelopes
        .iter()
        .map(|envelope| format!("{envelope}\n"))
        .collect();

    build_kafcli_command(directory.path())
        .args(["produce", "--topic", &target])
        .args(confluent_arguments)
        .args(["--input", "envelope", "--stdin"])
        .write_stdin(replay.into_bytes())
        .assert()
        .success();

    let replayed = consume_json(
        directory.path(),
        &target,
        &["--format", "avro", "--wire", "confluent", "--count", "1"],
    );
    assert_eq!(replayed[0]["value"], json!({"id": 41}));
    assert_key_and_trace_header(&replayed[0]);
}

#[test]
#[ignore]
fn azure_message_without_content_type_is_decode_error() {
    let directory = tempfile::tempdir().unwrap();
    let topic = unique_name("it-azure-plain");
    let registry = azure_registry();
    let _topic = TopicGuard::create(&topic);
    add_azure_context(directory.path(), &registry.url());
    produce_values(directory.path(), &topic, &[], &[json!({"id": 41})]);

    let envelopes = consume_json(
        directory.path(),
        &topic,
        &["--format", "avro", "--wire", "azure", "--count", "1"],
    );
    assert_eq!(envelopes[0]["decode_error"], MISSING_CONTENT_TYPE);
    assert_eq!(envelopes[0]["value"], Value::Null);
}

#[test]
#[ignore]
fn produce_with_missing_subject_fails_and_registers_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let subject = unique_name("pedidos-missing");
    let topic = unique_name("it-missing-subject");
    wait_for_registry();
    let _topic = TopicGuard::create(&topic);
    add_confluent_context(directory.path());

    build_kafcli_command(directory.path())
        .args(["produce", "--topic", &topic])
        .args([
            "--format",
            "avro",
            "--wire",
            "confluent",
            "--schema-subject",
            &subject,
        ])
        .args(["--value", r#"{"id":1}"#])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(format!(
            "subject '{subject}' not found"
        )));

    let (status, body) = registry_get("/subjects").unwrap();
    assert_eq!(status, 200);
    assert!(!body.contains(&subject), "subjects: {body}");
}

/// Avro binary of `NESTED_RECORD` with `levels` nodes followed by `null`: each node is its zigzag
/// `value`, then the union index 1 (`Node`), and the chain ends with the union index 0 (`null`).
fn nested_node_bytes(levels: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for level in 0..levels {
        push_zigzag_long(&mut bytes, i64::try_from(level).unwrap());
        bytes.push(0x02);
    }
    push_zigzag_long(&mut bytes, i64::try_from(levels).unwrap());
    bytes.push(0x00);
    bytes
}

/// Appends an Avro `long` as zigzag-encoded base-128 varint bytes.
fn push_zigzag_long(bytes: &mut Vec<u8>, value: i64) {
    let mut encoded = ((value << 1) ^ (value >> 63)).cast_unsigned();
    loop {
        let low = (encoded & 0x7f) as u8;
        encoded >>= 7;
        if encoded == 0 {
            bytes.push(low);
            return;
        }
        bytes.push(low | 0x80);
    }
}

#[test]
#[ignore]
fn consume_deeply_nested_avro_does_not_crash() {
    let directory = tempfile::tempdir().unwrap();
    let topic = unique_name("it-deep-avro");
    let schema = write_schema_file(directory.path(), "node.avsc", NESTED_RECORD);
    let schema_path = schema.to_string_lossy().to_string();
    let _topic = TopicGuard::create(&topic);
    add_context(directory.path(), &[]);
    // produce refuses JSON nested beyond 64 levels, so the Avro bytes go out through the core producer.
    let context = ConnectionContext {
        brokers: vec![PLAINTEXT_BROKER.to_string()],
        auth: AuthConfig::Plaintext,
        protected: false,
        schema_registry: None,
    };
    let options = ProduceOptions {
        topic: topic.clone(),
        key_template: None,
        header_templates: Vec::new(),
        partition: None,
        count: 1,
        rate_per_second: None,
        render_templates: false,
        timeout: Duration::from_secs(30),
    };
    let message = PreparedMessage {
        key: None,
        value: Some(nested_node_bytes(NESTING_LEVELS)),
        headers: Vec::new(),
        partition: None,
    };
    let report = produce_messages(
        &context,
        &EnvironmentSecretResolver::from_process_environment(),
        &ConnectionSettings { insecure: false },
        &options,
        &mut std::iter::once(Ok(message)),
    )
    .unwrap();
    assert_eq!(report.failed, 0);

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "consume", "--topic", &topic])
        .args([
            "--format",
            "avro",
            "--wire",
            "raw",
            "--schema-file",
            &schema_path,
        ])
        .args(["--max-bytes", "0", "--count", "1"])
        .assert()
        .code(0);
    let envelopes = parse_envelopes(&assertion.get_output().stdout);
    let decode_error = envelopes[0]["decode_error"].as_str().unwrap_or_default();
    assert!(
        decode_error.contains("value nests deeper than 64 levels")
            || decode_error.contains("value is not valid Avro for the schema"),
        "decode_error: {decode_error}"
    );
}

/// Adds a field with a default: a backward-compatible change under the registry's default rule.
const AVRO_WITH_DEFAULT_FIELD: &str = r#"{"type":"record","name":"Pedido","fields":[{"name":"id","type":"long"},{"name":"nota","type":"string","default":""}]}"#;
/// Changes the type of `id`: not readable with the old schema, so not compatible.
const AVRO_WITH_TYPE_CHANGE: &str =
    r#"{"type":"record","name":"Pedido","fields":[{"name":"id","type":"string"}]}"#;

#[test]
#[ignore]
fn register_get_by_subject_and_id() {
    wait_for_registry();
    let directory = tempfile::tempdir().unwrap();
    let subject = unique_name("pedidos-register");
    add_confluent_context(directory.path());
    let schema_file = write_schema_file(directory.path(), "pedido.avsc", AVRO_RECORD);

    let registered = build_kafcli_command(directory.path())
        .args(["--output", "json", "schema", "register", "--subject"])
        .arg(&subject)
        .arg("--schema-file")
        .arg(&schema_file)
        .assert()
        .success();
    let data =
        serde_json::from_slice::<Value>(&registered.get_output().stdout).unwrap()["data"].clone();
    let id = data["schema_id"].as_str().unwrap().to_string();
    assert_eq!(data["subject"], subject.as_str());
    assert_eq!(data["type"], "avro");

    let by_subject = build_kafcli_command(directory.path())
        .args(["--output", "json", "schema", "get", "--subject"])
        .arg(&subject)
        .assert()
        .success();
    let subject_data =
        serde_json::from_slice::<Value>(&by_subject.get_output().stdout).unwrap()["data"].clone();
    assert_eq!(subject_data["schema"], AVRO_RECORD);
    assert_eq!(subject_data["schema_id"], id.as_str());
    assert_eq!(subject_data["version"], 1);

    let by_id = build_kafcli_command(directory.path())
        .args(["--output", "json", "schema", "get", "--id", &id])
        .assert()
        .success();
    let id_data =
        serde_json::from_slice::<Value>(&by_id.get_output().stdout).unwrap()["data"].clone();
    assert_eq!(id_data["schema"], AVRO_RECORD);
    assert_eq!(id_data["schema_id"], id.as_str());
}

#[test]
#[ignore]
fn check_compat_accepts_added_field_with_default() {
    let directory = tempfile::tempdir().unwrap();
    let subject = unique_name("pedidos-compat-ok");
    register_confluent_schema(&subject, SchemaKind::Avro, AVRO_RECORD);
    add_confluent_context(directory.path());
    let candidate = write_schema_file(directory.path(), "v2.avsc", AVRO_WITH_DEFAULT_FIELD);

    let assertion = build_kafcli_command(directory.path())
        .args(["--output", "json", "schema", "check-compat", "--subject"])
        .arg(&subject)
        .arg("--schema-file")
        .arg(&candidate)
        .assert()
        .success();

    let data =
        serde_json::from_slice::<Value>(&assertion.get_output().stdout).unwrap()["data"].clone();
    assert_eq!(data["compatible"], true);
}

#[test]
#[ignore]
fn check_compat_rejects_type_change_with_exit_1() {
    let directory = tempfile::tempdir().unwrap();
    let subject = unique_name("pedidos-compat-bad");
    register_confluent_schema(&subject, SchemaKind::Avro, AVRO_RECORD);
    add_confluent_context(directory.path());
    let candidate = write_schema_file(directory.path(), "v2.avsc", AVRO_WITH_TYPE_CHANGE);

    build_kafcli_command(directory.path())
        .args(["schema", "check-compat", "--subject"])
        .arg(&subject)
        .arg("--schema-file")
        .arg(&candidate)
        .assert()
        .code(1)
        .stderr(predicate::str::contains("is not compatible with subject"));
}
