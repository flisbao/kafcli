use predicates::str::contains;

const GETTING_STARTED_LINE: &str =
    "kafcli context add local --brokers localhost:9092 --auth-type none --use";

#[test]
fn help_subcommand_shows_getting_started_examples() {
    assert_cmd::Command::cargo_bin("kafcli")
        .unwrap()
        .arg("help")
        .assert()
        .success()
        .stdout(contains("Getting started:"))
        .stdout(contains(GETTING_STARTED_LINE))
        .stdout(contains("kafcli help <COMMAND>"));
}

#[test]
fn help_flag_shows_getting_started_examples() {
    assert_cmd::Command::cargo_bin("kafcli")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(contains(GETTING_STARTED_LINE));
}

#[test]
fn help_of_a_command_shows_its_examples() {
    assert_cmd::Command::cargo_bin("kafcli")
        .unwrap()
        .args(["help", "lag"])
        .assert()
        .success()
        .stdout(contains("kafcli lag --all-groups --sort lag --top 20"));
}
