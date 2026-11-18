#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Bearer (OAUTHBEARER) authentication against the `kafka-oauth` fixture of
//! `tests/fixtures/compose.yaml`. The fixture accepts unsecured JWTs (`alg: none`).
//!
//! Run with: `docker compose -f tests/fixtures/compose.yaml up -d --wait kafka-oauth` and then
//! `cargo test -p kafcli --test integration_oauth_cli bearer -- --ignored --test-threads=1`

use assert_cmd::Command;
use kafcli_core::registry::fake_registry_server::{FakeRegistryServer, GeneratedFakeRoute};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const OAUTH_BROKER: &str = "127.0.0.1:49092";
const FIXTURE_SERVICE: &str = "kafka-oauth";
/// Plaintext listener inside the fixture container.
const FIXTURE_BOOTSTRAP: &str = "localhost:9092";
const TOKEN_VARIABLE: &str = "KAFCLI_IT_BEARER_TOKEN";
const OIDC_SECRET_VARIABLE: &str = "KAFCLI_IT_OIDC_SECRET";
const OIDC_CLIENT_SECRET: &str = "kafcli-it-client-secret";

fn compose_file() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/compose.yaml")
}

fn run_fixture_kafka_topics(arguments: &[&str]) {
    let output = ProcessCommand::new("docker")
        .args(["compose", "-f"])
        .arg(compose_file())
        .args(["exec", "-T", FIXTURE_SERVICE])
        .args([
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            FIXTURE_BOOTSTRAP,
        ])
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "kafka-topics {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn unique_name(prefix: &str) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{prefix}-{millis}")
}

/// Deletes its topic when dropped, so a failing assertion does not leave the topic behind.
struct TopicGuard {
    name: String,
}

impl TopicGuard {
    fn create(name: &str) -> Self {
        run_fixture_kafka_topics(&["--create", "--topic", name, "--partitions", "1"]);
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
            .args(["exec", "-T", FIXTURE_SERVICE])
            .args([
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                FIXTURE_BOOTSTRAP,
            ])
            .args(["--delete", "--topic", &self.name])
            .output();
    }
}

fn seconds_since_epoch() -> i64 {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    i64::try_from(seconds).unwrap()
}

/// Base64url without padding, the encoding JWT segments use.
fn base64_url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut encoded = String::new();
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        let group = (u32::from(first) << 16) | (u32::from(second) << 8) | u32::from(third);
        for index in 0..=chunk.len() {
            let shift = 18 - 6 * index as u32;
            encoded.push(char::from(ALPHABET[((group >> shift) & 0x3f) as usize]));
        }
    }
    encoded
}

/// JWT with `alg: none`, an empty signature and the given subject and expiry.
fn unsecured_jwt(subject: &str, expires_at_seconds: i64) -> String {
    let header = base64_url(br#"{"alg":"none"}"#);
    let claims = format!(r#"{{"sub":"{subject}","exp":{expires_at_seconds}}}"#);
    format!("{header}.{}.", base64_url(claims.as_bytes()))
}

/// Command with the configuration and state directory inside `directory`, and the bearer token
/// in the environment variable that the context reads.
fn build_kafcli_command(directory: &Path, token: &str) -> Command {
    let mut command = Command::cargo_bin("kafcli").unwrap();
    command
        .env_remove("KAFCLI_CONTEXT")
        .env_remove("KAFCLI_CURRENT_CONTEXT")
        .env("KAFCLI_CONFIG", directory.join("config.json"))
        .env("KAFCLI_STATE_DIR", directory.join("state"))
        .env(TOKEN_VARIABLE, token);
    command
}

fn add_bearer_context(directory: &Path, name: &str, token: &str) {
    build_kafcli_command(directory, token)
        .args([
            "context",
            "add",
            name,
            "--brokers",
            OAUTH_BROKER,
            "--auth-type",
            "bearer",
            "--token-ref",
            "env:KAFCLI_IT_BEARER_TOKEN",
            "--no-tls",
            "--use",
        ])
        .assert()
        .success();
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn bearer_produce_and_consume_round_trip() {
    let topic = TopicGuard::create(&unique_name("kafcli-bearer"));
    let directory = tempfile::tempdir().unwrap();
    let token = unsecured_jwt("kafcli-it", seconds_since_epoch() + 3600);
    add_bearer_context(directory.path(), "it-bearer", &token);

    build_kafcli_command(directory.path(), &token)
        .args(["produce", "--topic", &topic.name, "--value", r#"{"n":1}"#])
        .assert()
        .success();

    let assertion = build_kafcli_command(directory.path(), &token)
        .args([
            "consume",
            "--topic",
            &topic.name,
            "--count",
            "1",
            "--output",
            "json",
        ])
        .assert()
        .success();
    let line = String::from_utf8_lossy(&assertion.get_output().stdout).to_string();
    let envelope: Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
    assert_eq!(envelope["value"], serde_json::json!({"n": 1}));
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn expired_bearer_token_fails_fast_with_clear_error() {
    let directory = tempfile::tempdir().unwrap();
    let token = unsecured_jwt("kafcli-it", seconds_since_epoch() - 60);
    add_bearer_context(directory.path(), "it-expired", &token);

    let started = Instant::now();
    let assertion = build_kafcli_command(directory.path(), &token)
        .args(["context", "test", "--timeout-seconds", "20"])
        .assert()
        .code(1);

    assert!(started.elapsed() < Duration::from_secs(10));
    let stderr = String::from_utf8_lossy(&assertion.get_output().stderr).to_string();
    assert!(
        stderr.contains("bearer token expired at"),
        "stderr: {stderr}"
    );
    assert!(!stderr.contains(&token), "stderr leaks the token");
}

/// Body of a token response whose JWT expires `lifetime_seconds` from now.
fn oidc_token_response(lifetime_seconds: i64) -> String {
    json!({
        "access_token": unsecured_jwt("kafcli-it", seconds_since_epoch() + lifetime_seconds),
        "token_type": "bearer",
        "expires_in": lifetime_seconds,
    })
    .to_string()
}

/// Fake token endpoint that answers every `POST /token` with a fresh token of `lifetime_seconds`.
fn start_fake_token_endpoint(lifetime_seconds: i64) -> FakeRegistryServer {
    let route = GeneratedFakeRoute::new(
        "POST",
        "/token",
        Arc::new(move || (200, oidc_token_response(lifetime_seconds))),
    )
    .with_header("Content-Type", "application/json");
    FakeRegistryServer::start_with_generated(Vec::new(), vec![route]).unwrap()
}

/// Command with the configuration directory of `directory` and the OIDC client secret set.
fn build_oidc_command(directory: &Path) -> Command {
    let mut command = build_kafcli_command(directory, "unused-token");
    command.env(OIDC_SECRET_VARIABLE, OIDC_CLIENT_SECRET);
    command
}

fn add_oidc_context(directory: &Path, name: &str, token_endpoint: &str) {
    build_oidc_command(directory)
        .args([
            "context",
            "add",
            name,
            "--brokers",
            OAUTH_BROKER,
            "--auth-type",
            "oauth-oidc",
            "--token-endpoint-url",
            token_endpoint,
            "--client-id",
            "it",
            "--secret-ref",
            "env:KAFCLI_IT_OIDC_SECRET",
            "--no-tls",
            "--use",
        ])
        .assert()
        .success();
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn oidc_round_trip_with_fake_token_endpoint() {
    let topic = TopicGuard::create(&unique_name("kafcli-oidc"));
    let token_endpoint = start_fake_token_endpoint(3600);
    let endpoint = format!("{}/token", token_endpoint.url());
    let directory = tempfile::tempdir().unwrap();
    add_oidc_context(directory.path(), "it-oidc", &endpoint);

    build_oidc_command(directory.path())
        .args(["produce", "--topic", &topic.name, "--value", r#"{"n":7}"#])
        .assert()
        .success();

    let assertion = build_oidc_command(directory.path())
        .args([
            "consume",
            "--topic",
            &topic.name,
            "--count",
            "1",
            "--output",
            "json",
        ])
        .assert()
        .success();
    let line = String::from_utf8_lossy(&assertion.get_output().stdout).to_string();
    let envelope: Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
    assert_eq!(envelope["value"], json!({"n": 7}));

    let token_requests: Vec<_> = token_endpoint
        .recorded_requests()
        .into_iter()
        .filter(|request| request.path_and_query == "/token")
        .collect();
    assert!(
        !token_requests.is_empty(),
        "no request reached the token endpoint"
    );
    assert!(
        token_requests
            .iter()
            .all(|request| request.body.contains("grant_type=client_credentials")),
        "token request without client_credentials grant"
    );
}

#[test]
#[ignore = "requires docker compose fixtures"]
fn oidc_refreshes_token_during_long_consume() {
    let topic = TopicGuard::create(&unique_name("kafcli-oidc-refresh"));
    // Tokens live 15 s and the broker re-authenticates every 10 s (connections.max.reauth.ms),
    // so the consumer only survives the 25 s wait if librdkafka fetches new tokens.
    let token_endpoint = start_fake_token_endpoint(15);
    let endpoint = format!("{}/token", token_endpoint.url());
    let directory = tempfile::tempdir().unwrap();
    add_oidc_context(directory.path(), "it-oidc-refresh", &endpoint);

    let consumer = ProcessCommand::new(assert_cmd::cargo::cargo_bin("kafcli"))
        .env("KAFCLI_CONFIG", directory.path().join("config.json"))
        .env("KAFCLI_STATE_DIR", directory.path().join("state"))
        .env_remove("KAFCLI_CONTEXT")
        .env_remove("KAFCLI_CURRENT_CONTEXT")
        .env(OIDC_SECRET_VARIABLE, OIDC_CLIENT_SECRET)
        .args([
            "consume",
            "--topic",
            &topic.name,
            "--count",
            "1",
            "--follow",
            "--timeout-seconds",
            "40",
            "--output",
            "json",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // Waits 25 s without sleeping: the sender lives until the timeout, so the receive times out.
    let (wait_guard, wait_signal) = std::sync::mpsc::channel::<()>();
    let _ = wait_signal.recv_timeout(Duration::from_secs(25));
    drop(wait_guard);
    build_oidc_command(directory.path())
        .args(["produce", "--topic", &topic.name, "--value", r#"{"n":9}"#])
        .assert()
        .success();

    let output = consumer.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "consume failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let line = String::from_utf8_lossy(&output.stdout).to_string();
    let envelope: Value = serde_json::from_str(line.lines().next().unwrap()).unwrap();
    assert_eq!(envelope["value"], json!({"n": 9}));

    let token_requests = token_endpoint
        .recorded_requests()
        .into_iter()
        .filter(|request| request.path_and_query == "/token")
        .count();
    // One token for the consumer, one for the producer, and at least one refresh by the consumer.
    assert!(
        token_requests >= 3,
        "expected a refresh, got {token_requests} token requests"
    );
}
