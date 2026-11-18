#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! No OAuth or Azure secret reaches stdout, stderr or the audit log. Each marker is a literal
//! string that a leak would print: the client secret, the Entra access token (its signature
//! segment carries the marker, so the token text itself is searched) and the bearer token.

use assert_cmd::Command;
use kafcli_core::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
use serde_json::json;
use std::path::Path;

const SP_SECRET_VARIABLE: &str = "KAFCLI_LEAK_SP_SECRET";
const BEARER_VARIABLE: &str = "KAFCLI_LEAK_BEARER";
const SP_SECRET: &str = "leak-sp-secret-value";
const ACCESS_TOKEN_MARKER: &str = "leak-access-token-value";
const BEARER_MARKER: &str = "leak-bearer-marker";
/// Entra access token whose signature segment holds `ACCESS_TOKEN_MARKER`; `appid` is `app-123`.
const ACCESS_TOKEN: &str = "eyJhbGciOiJub25lIn0.eyJhcHBpZCI6ImFwcC0xMjMifQ.leak-access-token-value";
/// Bearer JWT that expires in 2100, with `BEARER_MARKER` as its signature segment.
const BEARER_TOKEN: &str =
    "eyJhbGciOiJub25lIn0.eyJzdWIiOiJrYWZjbGktbGVhayIsImV4cCI6NDEwMjQ0NDgwMH0.leak-bearer-marker";
const BROKER: &str = "127.0.0.1:1";
const TENANT: &str = "tenant-1";

/// Command with the configuration and state directory inside `directory`.
fn build_kafcli_command(directory: &Path) -> Command {
    let mut command = Command::cargo_bin("kafcli").unwrap();
    command
        .env_remove("KAFCLI_CONTEXT")
        .env_remove("KAFCLI_CURRENT_CONTEXT")
        .env("KAFCLI_CONFIG", directory.join("config.json"))
        .env("KAFCLI_STATE_DIR", directory.join("state"))
        .env(SP_SECRET_VARIABLE, SP_SECRET)
        .env(BEARER_VARIABLE, BEARER_TOKEN);
    command
}

fn add_context(directory: &Path, name: &str, extra_arguments: &[&str]) {
    build_kafcli_command(directory)
        .args(["context", "add", name, "--brokers", BROKER, "--use"])
        .args(extra_arguments)
        .assert()
        .success();
}

fn entra_route(status: u16, body: String) -> FakeRoute {
    FakeRoute {
        method: "POST".to_string(),
        path_and_query: format!("/{TENANT}/oauth2/v2.0/token"),
        status,
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body,
    }
}

/// Entra endpoint that issues a token. The client secret is known to the test.
fn entra_issuing_token() -> FakeRegistryServer {
    let body = json!({"access_token": ACCESS_TOKEN, "expires_in": 3600}).to_string();
    FakeRegistryServer::start(vec![entra_route(200, body)]).unwrap()
}

/// Entra endpoint that refuses the request and echoes the client secret in its description,
/// which the CLI must redact before printing the error.
fn entra_refusing_and_echoing_secret() -> FakeRegistryServer {
    let body = json!({
        "error": "invalid_client",
        "error_description": format!("AADSTS7000215: invalid client secret {SP_SECRET} for app-123"),
    })
    .to_string();
    FakeRegistryServer::start(vec![entra_route(401, body)]).unwrap()
}

/// OpenID Connect token endpoint for `oauth-oidc`, answering every request with the token.
fn oidc_token_endpoint() -> FakeRegistryServer {
    FakeRegistryServer::start(vec![FakeRoute {
        method: "POST".to_string(),
        path_and_query: "/token".to_string(),
        status: 200,
        headers: vec![("Content-Type".to_string(), "application/json".to_string())],
        body: json!({"access_token": ACCESS_TOKEN, "token_type": "bearer", "expires_in": 3600})
            .to_string(),
    }])
    .unwrap()
}

fn sp_arguments(authority_host: &str) -> Vec<String> {
    [
        "--auth-type",
        "azure-sp-secret",
        "--tenant-id",
        TENANT,
        "--client-id",
        "app-123",
        "--scope",
        "https://eventhubs.example/.default",
        "--secret-ref",
        "env:KAFCLI_LEAK_SP_SECRET",
        "--authority-host",
    ]
    .iter()
    .map(|argument| argument.to_string())
    .chain(std::iter::once(authority_host.to_string()))
    .collect()
}

/// Everything one run prints: stdout, stderr and the audit log when it exists.
fn collect_output(directory: &Path, arguments: &[&str]) -> String {
    let output = build_kafcli_command(directory)
        .args(arguments)
        .output()
        .unwrap();
    let mut text = String::from_utf8_lossy(&output.stdout).to_string();
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    if let Ok(audit) = std::fs::read_to_string(directory.join("state").join("audit.log")) {
        text.push_str(&audit);
    }
    text
}

/// Runs the three commands the brief names against one context and returns what they printed.
fn collect_all_outputs(directory: &Path, name: &str) -> Vec<String> {
    vec![
        collect_output(
            directory,
            &["-vv", "context", "test", "--timeout-seconds", "2"],
        ),
        collect_output(
            directory,
            &[
                "-vv",
                "--output",
                "json",
                "context",
                "test",
                "--timeout-seconds",
                "2",
            ],
        ),
        collect_output(directory, &["context", "show", name, "--output", "json"]),
    ]
}

#[test]
fn oauth_modes_never_print_secrets() {
    let secrets = [
        SP_SECRET,
        ACCESS_TOKEN_MARKER,
        BEARER_MARKER,
        ACCESS_TOKEN,
        BEARER_TOKEN,
    ];

    let bearer_directory = tempfile::tempdir().unwrap();
    add_context(
        bearer_directory.path(),
        "bearer",
        &[
            "--auth-type",
            "bearer",
            "--token-ref",
            "env:KAFCLI_LEAK_BEARER",
            "--no-tls",
        ],
    );

    let issuing = entra_issuing_token();
    let issuing_directory = tempfile::tempdir().unwrap();
    let issuing_arguments = sp_arguments(&issuing.url());
    add_context(
        issuing_directory.path(),
        "sp-issued",
        &issuing_arguments
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    );

    let refusing = entra_refusing_and_echoing_secret();
    let refusing_directory = tempfile::tempdir().unwrap();
    let refusing_arguments = sp_arguments(&refusing.url());
    add_context(
        refusing_directory.path(),
        "sp-refused",
        &refusing_arguments
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    );

    let oidc = oidc_token_endpoint();
    let oidc_directory = tempfile::tempdir().unwrap();
    let token_endpoint = format!("{}/token", oidc.url());
    add_context(
        oidc_directory.path(),
        "oidc",
        &[
            "--auth-type",
            "oauth-oidc",
            "--token-endpoint-url",
            &token_endpoint,
            "--client-id",
            "app-123",
            "--secret-ref",
            "env:KAFCLI_LEAK_SP_SECRET",
            "--no-tls",
        ],
    );

    let scenarios = [
        ("bearer", bearer_directory.path(), "bearer"),
        (
            "azure-sp-secret, token issued",
            issuing_directory.path(),
            "sp-issued",
        ),
        (
            "azure-sp-secret, refused",
            refusing_directory.path(),
            "sp-refused",
        ),
        ("oauth-oidc", oidc_directory.path(), "oidc"),
    ];
    for (label, directory, context_name) in scenarios {
        let outputs = collect_all_outputs(directory, context_name);
        for output in &outputs {
            for secret in secrets {
                assert!(
                    !output.contains(secret),
                    "scenario '{label}' printed a secret: {output}"
                );
            }
        }
        if label == "azure-sp-secret, refused" {
            assert!(
                outputs[0].contains("HTTP 401"),
                "the refusal never reached the error path: {}",
                outputs[0]
            );
        }
    }

    // The scenarios only prove something if the secrets were in use: each endpoint was called.
    assert!(!issuing.recorded_requests().is_empty());
    assert!(!refusing.recorded_requests().is_empty());
    assert!(!oidc.recorded_requests().is_empty());
}
