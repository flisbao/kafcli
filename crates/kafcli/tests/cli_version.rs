#[test]
fn prints_version_with_binary_name() {
    assert_cmd::Command::cargo_bin("kafcli")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicates::str::starts_with(format!(
            "kafcli {}",
            env!("CARGO_PKG_VERSION")
        )));
}
