use crate::cli::profile_arguments::ProfileCommand;
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
    /// Profile to use; defaults to `current_context` from the configuration.
    #[arg(long, global = true)]
    pub profile: Option<String>,
    /// Output format of command results.
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Table)]
    pub output: OutputFormat,
    /// Increases log detail on stderr (-v info, -vv debug).
    #[arg(short = 'v', long = "verbose", global = true, action = clap::ArgAction::Count)]
    pub verbosity: u8,
    /// Configuration file; overrides the default location.
    #[arg(long, global = true, env = "KAFCLI_CONFIG")]
    pub config: Option<PathBuf>,
    /// Broker addresses that override the profile's brokers.
    #[arg(long, global = true, value_delimiter = ',')]
    pub brokers: Option<Vec<String>>,
    /// Allows TLS settings that a profile would otherwise refuse.
    #[arg(long, global = true)]
    pub insecure: bool,
}

#[derive(clap::Subcommand)]
pub enum TopLevelCommand {
    /// Manage connection profiles
    Profile {
        #[command(subcommand)]
        command: ProfileCommand,
    },
}
