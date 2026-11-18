/// Subcommands of `kafcli offsets`.
#[derive(clap::Subcommand)]
pub enum OffsetsCommand {
    /// Show the committed offsets of a consumer group
    #[command(after_help = r#"Examples:
  kafcli offsets show --group pedidos-service
  kafcli offsets show --group pedidos-service --topic pedidos --output json

Notes:
  Reads only: nothing is committed and no group is joined.
  Without --topic, the topics come from the group's members, or from every topic where the group
  has a committed offset when it has no members.
  A partition without a committed offset shows `-` (null in JSON)."#)]
    Show(ShowOffsetsArguments),
}

#[derive(clap::Args, Debug)]
pub struct ShowOffsetsArguments {
    /// Consumer group to read.
    #[arg(long)]
    pub group: String,
    /// Only partitions of this topic.
    #[arg(long)]
    pub topic: Option<String>,
    /// Timeout of each broker request, in seconds.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_seconds: u64,
}
