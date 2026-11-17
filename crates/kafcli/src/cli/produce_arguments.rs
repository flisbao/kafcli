use kafcli_core::ops::produce::produce_input::InputSource;
use std::path::PathBuf;

/// Message format of the values. Only `json` exists in this release.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProduceFormat {
    Json,
}

/// Wire encoding of the values. Only `raw` exists in this release.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProduceWire {
    Raw,
}

/// How the input text is read: one JSON document or NDJSON of payloads, or NDJSON of envelopes.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProduceInputArgument {
    Json,
    Envelope,
}

#[derive(clap::Args)]
#[command(group(
    clap::ArgGroup::new("input_source")
        .required(true)
        .args(["value", "file", "stdin"])
))]
pub struct ProduceArguments {
    /// Topic to produce to
    #[arg(long)]
    pub topic: String,
    /// Message format of the values
    #[arg(long, value_enum, default_value_t = ProduceFormat::Json)]
    pub format: ProduceFormat,
    /// Wire encoding of the values
    #[arg(long, value_enum, default_value_t = ProduceWire::Raw)]
    pub wire: ProduceWire,
    /// Input text given on the command line (one JSON document or NDJSON)
    #[arg(long)]
    pub value: Option<String>,
    /// Input file (one JSON document or NDJSON)
    #[arg(long, value_name = "PATH")]
    pub file: Option<PathBuf>,
    /// Reads the input from standard input
    #[arg(long)]
    pub stdin: bool,
    /// How the input text is read
    #[arg(long, value_enum, default_value_t = ProduceInputArgument::Json)]
    pub input: ProduceInputArgument,
    /// Key template; a key template can read payload fields with {{field:<path>}}
    #[arg(long, value_name = "TEMPLATE")]
    pub key: Option<String>,
    /// Header as NAME=VALUE; repeatable; the value accepts templates
    #[arg(long = "header", value_name = "NAME=VALUE", value_parser = parse_header_assignment)]
    pub headers: Vec<(String, String)>,
    /// Partition to produce to; the broker picks one when omitted
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(i32).range(0..))]
    pub partition: Option<i32>,
    /// How many times the whole input is produced (1 to 10000000)
    #[arg(long, value_name = "N", default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..=10_000_000))]
    pub count: u64,
    /// Messages per second (1 to 1000000); unlimited when omitted
    #[arg(long, value_name = "MSGS_PER_SECOND", value_parser = clap::value_parser!(u32).range(1..=1_000_000))]
    pub rate: Option<u32>,
    /// Sends text as written, without templates. Payload text with a literal `{{` needs this flag.
    #[arg(long)]
    pub no_template: bool,
    /// Renders the messages and shows them without connecting or sending
    #[arg(long)]
    pub dry_run: bool,
    /// Skips the confirmation on a protected context
    #[arg(long)]
    pub yes: bool,
    /// Seconds to wait for the broker before giving up (1 to 3600)
    #[arg(long, value_name = "N", default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_seconds: u64,
}

impl ProduceArguments {
    /// The single input source the argument group guarantees. Clap rejects a command without one.
    pub fn input_source(&self) -> InputSource {
        if self.stdin {
            InputSource::Stdin
        } else if let Some(path) = &self.file {
            InputSource::File(path.clone())
        } else {
            InputSource::Value(self.value.clone().unwrap_or_default())
        }
    }
}

/// Splits `NAME=VALUE` at the first `=`. The name must not be empty; the value may be empty.
fn parse_header_assignment(text: &str) -> Result<(String, String), String> {
    let (name, value) = text
        .split_once('=')
        .ok_or_else(|| "expected NAME=VALUE".to_string())?;
    if name.is_empty() {
        return Err("header name must not be empty".to_string());
    }
    Ok((name.to_string(), value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_splits_at_first_equals() {
        assert_eq!(
            parse_header_assignment("trace=a=b"),
            Ok(("trace".to_string(), "a=b".to_string()))
        );
    }

    #[test]
    fn header_without_equals_is_rejected() {
        assert_eq!(
            parse_header_assignment("trace"),
            Err("expected NAME=VALUE".to_string())
        );
    }

    #[test]
    fn header_with_empty_name_is_rejected() {
        assert_eq!(
            parse_header_assignment("=value"),
            Err("header name must not be empty".to_string())
        );
    }
}
