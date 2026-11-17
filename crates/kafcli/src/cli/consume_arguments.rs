use kafcli_core::ops::consume::message_filter::HeaderFilter;
use kafcli_core::ops::consume::start_position::StartPosition;
use std::str::FromStr;

/// Message format of the values. Only `json` exists in this release.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumeFormat {
    Json,
}

/// Wire encoding of the values. Only `raw` exists in this release.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumeWire {
    Raw,
}

#[derive(clap::Args)]
pub struct ConsumeArguments {
    /// Topic to consume from
    #[arg(long)]
    pub topic: String,
    /// Partition to read; repeatable. All partitions when omitted
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(i32).range(0..))]
    pub partition: Vec<i32>,
    /// Where to start: earliest, latest, offset:N or time:RFC3339
    #[arg(long, value_name = "POSITION", default_value = "earliest", value_parser = parse_start_position)]
    pub from: StartPosition,
    /// Messages to read before stopping; all available messages when omitted
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u64).range(1..))]
    pub count: Option<u64>,
    /// Keeps waiting for new messages instead of stopping at the end of the topic
    #[arg(long)]
    pub follow: bool,
    /// Consumer group to read with; the group commits offsets. Conflicts with --partition
    #[arg(long, value_name = "GROUP", conflicts_with = "partition")]
    pub group: Option<String>,
    /// Message format of the values
    #[arg(long, value_enum, default_value_t = ConsumeFormat::Json)]
    pub format: ConsumeFormat,
    /// Wire encoding of the values
    #[arg(long, value_enum, default_value_t = ConsumeWire::Raw)]
    pub wire: ConsumeWire,
    /// Largest value kept, in bytes; longer values are truncated (0 means no limit)
    #[arg(long, value_name = "BYTES", default_value_t = 2048)]
    pub max_bytes: usize,
    /// Comma-separated JSON object keys whose values are redacted; keys and headers are not redacted
    #[arg(long, value_name = "FIELDS", value_delimiter = ',')]
    pub redact: Vec<String>,
    /// Keeps only messages with a matching header; repeatable; format header:NAME=VALUE
    #[arg(long = "filter", value_name = "FILTER", value_parser = parse_header_filter)]
    pub filters: Vec<HeaderFilter>,
    /// Seconds without progress before giving up (1 to 3600)
    #[arg(long, value_name = "N", default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_seconds: u64,
}

fn parse_start_position(text: &str) -> Result<StartPosition, String> {
    StartPosition::from_str(text).map_err(|error| error.to_string())
}

fn parse_header_filter(text: &str) -> Result<HeaderFilter, String> {
    HeaderFilter::from_str(text).map_err(|error| error.to_string())
}
