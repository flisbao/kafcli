#[test]
fn prints_version_with_binary_name() {
    assert_cmd::Command::cargo_bin("kafcli")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicates::str::starts_with("kafcli 0.1.0"));
}
