#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! `kafcli lag` argument handling, without a broker.

use assert_cmd::Command;
use predicates::prelude::*;
use std::path::Path;

fn build_kafcli_command(directory: &Path) -> Command {
    let mut command = Command::cargo_bin("kafcli").unwrap();
    command
        .env_remove("KAFCLI_CONTEXT")
        .env_remove("KAFCLI_CURRENT_CONTEXT")
        .env("KAFCLI_CONFIG", directory.join("config.json"))
        .env("KAFCLI_STATE_DIR", directory.join("state"));
    command
}

#[test]
fn lag_requires_group_or_all_groups() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["lag"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("--group").and(predicate::str::contains("--all-groups")));
}

#[test]
fn lag_rejects_group_with_all_groups() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["lag", "--group", "g", "--all-groups"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn lag_rejects_top_out_of_range() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["lag", "--all-groups", "--top", "0"])
        .assert()
        .code(1);
}

#[test]
fn lag_help_has_examples() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["lag", "--help"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains(
                "kafcli lag --group pedidos-service --topic pedidos --max-lag 1000",
            )
            .and(predicate::str::contains(
                "kafcli lag --all-groups --sort lag --top 20",
            )),
        );
}

#[test]
fn lag_rejects_zero_watch_interval() {
    let directory = tempfile::tempdir().unwrap();

    build_kafcli_command(directory.path())
        .args(["lag", "--all-groups", "--watch", "0s"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains(
            "watch interval must be 1s to 3600s, like 5s or 1m",
        ));
}
