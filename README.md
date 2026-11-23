# kafcli

A command-line client for Apache Kafka and Azure Event Hubs, written in Rust.

kafcli produces and consumes messages (JSON and Avro, with Confluent and Azure schema registries),
shows consumer lag, audits consumer groups, and resets offsets with dry-run by default, automatic
backups and typed confirmation on protected contexts.

**Status: pre-alpha.** Interfaces and behavior may change without notice.

## Install

### Ubuntu / Debian (amd64)

Install the latest release with the install script. It downloads the `.deb` from the
[GitHub releases](https://github.com/flisbao/kafcli/releases), checks its SHA-256 against the
release's `SHA256SUMS` and installs it with `apt`:

```sh
curl -fsSL https://raw.githubusercontent.com/flisbao/kafcli/main/scripts/install.sh | bash
```

To install a specific version:

```sh
curl -fsSL https://raw.githubusercontent.com/flisbao/kafcli/main/scripts/install.sh | KAFCLI_VERSION=0.0.4 bash
```

Or download the package yourself and install it:

```sh
curl -fsSLO https://github.com/flisbao/kafcli/releases/download/v0.0.4/kafcli_0.0.4_amd64.deb
sudo apt install ./kafcli_0.0.4_amd64.deb
```

The binary is statically linked, so the package has no runtime dependencies. Remove it with
`sudo apt remove kafcli`.

### Other platforms

Each release also has `.tar.gz` archives for `x86_64-unknown-linux-musl` (any Linux x86_64) and
`aarch64-apple-darwin` (Apple silicon). Extract the archive and put `kafcli` on your `PATH`.

## Getting help

```sh
kafcli help                 # commands, global options and a getting-started guide
kafcli help consume         # options and examples of one command
kafcli offsets set --help   # the same, for a subcommand
kafcli --version
```

## Quick start

Start a local broker (any Kafka-compatible broker works; this one uses Redpanda):

```sh
docker run -d --name redpanda -p 9092:9092 docker.redpanda.com/redpandadata/redpanda:v25.3.18 \
  redpanda start --mode dev-container --smp 1 --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr localhost:9092
```

Create a context, make it current and test the connection:

```sh
kafcli context add local --brokers localhost:9092 --auth-type none --use
kafcli context test
```

Produce and consume:

```sh
kafcli produce --topic orders --value '{"id": 1, "total": 9.5}'
kafcli consume --topic orders --from earliest --count 10
```

## Examples

### Contexts

A context holds the brokers, the authentication and, optionally, the schema registry of one
cluster. The configuration lives in `~/.config/kafcli/config.json` (override with `--config` or
`KAFCLI_CONFIG`). Secrets are never stored in it: they are references such as `env:VAR`,
`file:/path` or `keyring:service/account`, resolved when a command runs.

```sh
kafcli context list
kafcli context show local

# SASL/SCRAM, marked as protected (writes ask for confirmation, destructive operations ask you
# to type the target name)
kafcli context add prd --brokers prd-kafka:9093 --auth-type sasl-scram-512 \
  --username app --secret-ref env:KAFKA_PASSWORD --protected

# Azure Event Hubs with a service principal and the Azure schema registry
kafcli context add prd-eh --brokers my-namespace.servicebus.windows.net:9093 \
  --auth-type azure-sp-secret --tenant-id <tenant> --client-id <app> --secret-ref env:SP_SECRET \
  --registry-type azure --registry-url https://my-namespace.servicebus.windows.net \
  --registry-group orders --registry-auth-type azure --protected

# Azure Event Hubs with your Azure CLI login
kafcli context add dev-eh --brokers my-namespace.servicebus.windows.net:9093 --auth-type azure-cli

kafcli context use prd
kafcli --context dev-eh consume --topic orders --count 5   # one-off context
```

Supported `--auth-type` values: `none`, `tls`, `mtls`, `sasl-plain`, `sasl-scram-256`,
`sasl-scram-512`, `azure-connection-string`, `bearer`, `oauth-oidc`, `azure-sp-secret`,
`azure-sp-cert`, `azure-workload-identity`, `azure-managed-identity`, `azure-cli`,
`azure-default`.

### Produce

```sh
# Templates: {{uuid}}, {{seq}}, {{field:...}} and more; --dry-run shows what would be sent
kafcli produce --topic orders --value '{"id": "{{uuid}}", "seq": "{{seq}}"}' --count 3 --dry-run

# One message per line of a file, keyed by a field, 10 messages per second
kafcli produce --topic orders --file orders.ndjson --key '{{field:id}}' --rate 10

# Avro with a schema from the registry (Confluent wire format)
kafcli produce --topic orders --format avro --wire confluent --schema-id 7 --value '{"id": 1}'

# Copy a topic: consume as JSON envelopes and produce them again
kafcli consume --topic a --output json | kafcli produce --topic b --stdin --input envelope
```

### Consume

```sh
kafcli consume --topic orders --count 10
kafcli consume --topic orders --partition 0 --from offset:100 --count 5
kafcli consume --topic orders --from time:2026-11-15T10:00:00-03:00 --follow
kafcli consume --topic orders --format avro --wire confluent --count 5
kafcli consume --topic orders --redact cpf,email --filter header:origin=checkout
```

Without `--group`, consuming never commits offsets.

### Schemas

```sh
kafcli schema register --subject orders-value --schema-file order.avsc
kafcli schema get --subject orders-value --output json
kafcli schema check-compat --subject orders-value --schema-file order-v2.avsc
```

### Lag and consumer groups

```sh
kafcli lag --group orders-service
kafcli lag --all-groups --sort lag --top 20
kafcli lag --all-groups --detail topic           # which topic of each group lags
kafcli lag --group orders-service --detail partition
kafcli lag --group orders-service --watch 5s
kafcli lag --group orders-service --topic orders --max-lag 1000   # exits 2 above the limit

kafcli groups audit
kafcli offsets show --group orders-service --output json
```

All of these only read: they never join a group or commit offsets.

Since 0.0.3, `kafcli lag --all-groups` prints one line per group by default. Scripts that read the
per-partition tables or the `TOTAL <group> <lag>` lines should pass `--detail partition`, or read
`--output json`, which does not depend on `--detail`.

### Offsets and poison messages

Destructive commands are dry-run by default. With `--execute`, kafcli checks that the group has
no active members, writes a backup of the current offsets and only then commits.

```sh
kafcli offsets set --group orders-service --topic orders --to-earliest             # dry-run
kafcli offsets set --group orders-service --topic orders --to-latest --execute
kafcli offsets set --group orders-service --topic orders --partition 0 --shift-by -100 --execute
kafcli offsets restore --file ~/.local/state/kafcli/offsets-backup/<backup>.json --execute

kafcli poison find --group orders-service --topic orders
kafcli poison skip --group orders-service --topic orders --partition 0 --offset 5 \
  --dlt orders.DLT --reason "bad json" --execute
```

Every write and destructive command is recorded in `~/.local/state/kafcli/audit.log` (override
the directory with `KAFCLI_STATE_DIR`). The audit log never contains message keys, values,
headers or secrets.

### Output and exit codes

`--output json` prints `{"kafcli_output": 1, "data": ...}` for scripts. Table output replaces
terminal control characters coming from the broker.

| Exit code | Meaning |
|---|---|
| 0 | success |
| 1 | error |
| 2 | limit exceeded (`--max-lag`) |
| 3 | aborted by the guard or by the user |

## Building from source

kafcli links librdkafka, OpenSSL and curl statically; they are compiled from source during the
build. Install these build prerequisites:

- Rust 1.99 (pinned by `rust-toolchain.toml`)
- `cmake`
- A C compiler and a C++ compiler (for example gcc or clang; on macOS, the Xcode command line tools)
- `perl`, used by OpenSSL's `Configure` script

Then run `cargo build --release -p kafcli`. The release binaries need none of these tools to run.

To build the `.deb` from a compiled binary:

```sh
packaging/deb/build-deb.sh target/release/kafcli 0.0.1 amd64 dist
```

## Releasing

Releases are built by `.github/workflows/release.yml` when a version tag is pushed. The tag must
match the version in `Cargo.toml`:

```sh
git tag v0.0.4
git push origin v0.0.4
```

The workflow builds the Linux (musl, static) and macOS binaries, the `.deb`, installs the `.deb`
on the runner as a smoke test, and publishes the release with a `SHA256SUMS` file.

The macOS build is best effort. If it fails or GitHub cancels it while it waits for a runner, the
release is still published with the Linux archive and the `.deb`, and the run shows a warning.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
