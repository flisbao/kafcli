use crate::cli::consume_arguments::{ConsumeFormat, ConsumeWire};
use crate::cli::schema_source_arguments::SchemaSourceArguments;
use crate::cli::watch_interval::parse_watch_interval;
use kafcli_core::codec::codec_selection::DecoderSelection;
use std::time::Duration;

/// Subcommands of `kafcli poison`.
#[derive(clap::Subcommand)]
pub enum PoisonCommand {
    /// Show the message each lagging partition of a group is stuck on, and whether it decodes
    #[command(after_help = r#"Examples:
  kafcli poison find --group pedidos-service --topic pedidos
  kafcli poison find --group pedidos-service --topic pedidos --format avro --wire confluent --output json
  kafcli poison find --group pedidos-service --topic pedidos --redact cpf,email

Notes:
  Reads only: nothing is committed and the group is never joined.
  For each partition with lag, the message at the committed offset is read and decoded with
  --format and --wire. A message that does not decode is reported as undecodable.
  The verdict uses the whole value; --max-bytes and --redact only limit what is shown."#)]
    Find(FindPoisonArguments),
    /// Copy the message a group is stuck on to a dead-letter topic and commit past it
    #[command(after_help = r#"Examples:
  kafcli poison skip --group G --topic T --partition P --offset O --dlt T.DLT --reason "..."
  kafcli poison skip --group pedidos-service --topic pedidos --partition 0 --offset 5 --dlt pedidos.DLT --reason "bad json" --execute

Notes:
  Dry-run is the default; nothing is copied or committed until you pass --execute.
  --offset must be the group's committed offset of that partition (see 'kafcli poison find').
  The copy keeps key, value and headers and adds kafcli-original-topic, -partition, -offset,
  -reason, -operator and -timestamp. Then offset + 1 is committed, after a backup.
  The group must have no active members; on a protected context you must type the group name.
  To reprocess the dead-letter topic later:
  kafcli consume --topic T.DLT --output json | kafcli produce --topic T --stdin --input envelope"#)]
    Skip(SkipPoisonArguments),
}

#[derive(clap::Args)]
pub struct FindPoisonArguments {
    /// Consumer group to inspect.
    #[arg(long)]
    pub group: String,
    /// Topic to inspect.
    #[arg(long)]
    pub topic: String,
    /// Message format of the values
    #[arg(long, value_enum, default_value_t = ConsumeFormat::Json)]
    pub format: ConsumeFormat,
    /// Wire encoding of the values
    #[arg(long, value_enum, default_value_t = ConsumeWire::Raw)]
    pub wire: ConsumeWire,
    #[command(flatten)]
    pub schema_source: SchemaSourceArguments,
    /// Largest value shown, in bytes; longer values are truncated (0 means no limit)
    #[arg(long, value_name = "BYTES", default_value_t = 2048)]
    pub max_bytes: usize,
    /// Comma-separated JSON object keys whose values are redacted in what is shown
    #[arg(long, value_name = "FIELDS", value_delimiter = ',')]
    pub redact: Vec<String>,
    /// Timeout of each broker request and of the message read, in seconds.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_seconds: u64,
}

impl FindPoisonArguments {
    pub fn decoder_selection(&self) -> DecoderSelection {
        DecoderSelection {
            format: self.format.to_value_format(),
            wire: self.wire.to_wire_format(),
            schema: self.schema_source.to_schema_source(),
        }
    }
}

#[derive(clap::Args)]
pub struct SkipPoisonArguments {
    /// Consumer group that is stuck.
    #[arg(long)]
    pub group: String,
    /// Topic of the message.
    #[arg(long)]
    pub topic: String,
    /// Partition of the message.
    #[arg(long, value_name = "P", value_parser = clap::value_parser!(i32).range(0..))]
    pub partition: i32,
    /// Offset of the message; must be the group's committed offset.
    #[arg(long, value_name = "O")]
    pub offset: i64,
    /// Dead-letter topic that receives the copy; it must exist.
    #[arg(long, value_name = "TOPIC")]
    pub dlt: String,
    /// Why the message is skipped; written to the copy's headers and to the audit log.
    #[arg(long)]
    pub reason: String,
    /// Copies the message and commits; without it nothing changes.
    #[arg(long)]
    pub execute: bool,
    /// Waits up to DURATION (5s, 2m or seconds, 1s to 3600s) for the group to have no members.
    #[arg(long, value_name = "DURATION", value_parser = parse_watch_interval)]
    pub wait_empty: Option<Duration>,
    /// The broker cannot describe the group, and you know its consumers are stopped.
    #[arg(long)]
    pub group_stopped: bool,
    /// Timeout of each broker request, in seconds.
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_seconds: u64,
}
