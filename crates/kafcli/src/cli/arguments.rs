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

Payload text that contains a literal `{{` needs --no-template."#)]
    Produce(ProduceArguments),
}
