//! kafcli runs on a worker thread; these tests check that output and exit codes still reach the process.

#[test]
fn help_prints_usage_and_exits_zero_through_worker_thread() {
    assert_cmd::Command::cargo_bin("kafcli")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicates::str::contains("Usage"));
}

#[test]
fn usage_error_exits_one_through_worker_thread() {
    assert_cmd::Command::cargo_bin("kafcli")
        .unwrap()
        .arg("no-such-command")
        .assert()
        .code(1)
        .stderr(predicates::str::contains("no-such-command"));
}
