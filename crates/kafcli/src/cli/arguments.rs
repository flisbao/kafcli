use crate::cli::consume_arguments::ConsumeArguments;
use crate::cli::context_arguments::ContextCommand;
use crate::cli::produce_arguments::ProduceArguments;
use crate::output::output_format::OutputFormat;
use std::path::PathBuf;

#[derive(clap::Parser)]
#[command(
    name = "kafcli",
    version,
    about = "Produce, consume and operate Kafka topics"
)]
pub struct CommandLineArguments {
    #[command(flatten)]
    pub global: GlobalArguments,
    #[command(subcommand)]
    pub command: TopLevelCommand,
}

#[derive(clap::Args)]
pub struct GlobalArguments {
    /// Context to use; defaults to the current context (see 'kafcli context use').
    #[arg(long, global = true, env = "KAFCLI_CONTEXT")]
    pub context: Option<String>,
    /// Output format of command results.
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Table)]
    pub output: OutputFormat,
    /// Increases log detail on stderr (-v info, -vv debug).
    #[arg(short = 'v', long = "verbose", global = true, action = clap::ArgAction::Count)]
    pub verbosity: u8,
    /// Configuration file; overrides the default location.
    #[arg(long, global = true, env = "KAFCLI_CONFIG")]
    pub config: Option<PathBuf>,
    /// Broker addresses that override the context's brokers.
    #[arg(long, global = true, value_delimiter = ',')]
    pub brokers: Option<Vec<String>>,
    /// Allows TLS settings that a context would otherwise refuse.
    #[arg(long, global = true)]
    pub insecure: bool,
}

#[derive(clap::Subcommand)]
pub enum TopLevelCommand {
    /// Manage connection contexts
    Context {
        #[command(subcommand)]
        command: ContextCommand,
    },
    /// Produce messages to a topic
    #[command(after_help = r#"Examples:
  kafcli produce --topic orders --value '{"id": 1, "total": 9.5}'
  kafcli produce --topic orders --file orders.ndjson --key '{{field:id}}' --count 3 --rate 10
  kafcli produce --topic orders --value '{"id": "{{uuid}}", "seq": "{{seq}}"}' --dry-run
  kafcli produce --topic notes --value '{"text": "a {{ b"}' --no-template
  kafcli consume --topic a --output json | kafcli produce --topic b --stdin --input envelope
  kafcli produce --topic pedidos --format avro --wire azure --schema-subject pedidos-value --value '{"id": 1}'
  kafcli produce --topic pedidos --format avro --wire confluent --schema-id 7 --value '{"id": 1}'
  kafcli produce --topic pedidos --format avro --wire raw --schema-file pedido.avsc --value '{"id": 1}'
  kafcli produce --topic pedidos --format json --wire raw --schema-file pedido.schema.json --no-validate --value '{"id": 1}'

Notes:
  Payload text that contains a literal `{{` needs --no-template.
  --dry-run contacts the schema registry to fetch the schema, never the broker.
  --schema-version takes latest (the default) or a version number, and needs --schema-subject.
  --no-validate sends JSON values without checking them against the schema; it does not apply to Avro."#)]
    Produce(ProduceArguments),
    /// Consume messages from a topic
    #[command(after_help = r#"Examples:
  kafcli consume --topic pedidos --count 10
  kafcli consume --topic pedidos --partition 0 --from offset:100 --count 5
  kafcli consume --topic pedidos --format avro --wire confluent --count 5
  kafcli consume --topic pedidos --format avro --wire azure --count 5
  kafcli consume --topic a --output json | kafcli produce --topic b --stdin --input envelope
  kafcli consume --topic pedidos [--partition 0] \
    [--from earliest|latest|offset:N|time:2026-11-15T10:00:00-03:00] \
    [--count 10] [--follow] [--format json --wire raw] \
    [--max-bytes 2048] [--redact cpf,email] [--filter header:origem=kafcli]

Notes:
  --redact covers decoded JSON values only; keys and headers are not redacted.
  With --redact, oversized or non-JSON values are withheld instead of shown.
  --group reads with a consumer group and commits its offsets; without it nothing is committed.
  Avro values larger than --max-bytes are truncated and not decoded; use --max-bytes 0 for large Avro values.
  Table output shows one line per message; use --output json for the envelopes."#)]
    Consume(ConsumeArguments),
}
