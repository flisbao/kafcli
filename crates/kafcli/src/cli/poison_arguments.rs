use crate::cli::consume_arguments::{ConsumeFormat, ConsumeWire};
use crate::cli::schema_source_arguments::SchemaSourceArguments;
use kafcli_core::codec::codec_selection::DecoderSelection;

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
