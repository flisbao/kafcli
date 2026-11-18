/// How `kafcli lag` orders the groups.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum LagSortOrder {
    /// Highest total lag first; ties ordered by group name.
    Lag,
    /// By group name.
    Group,
}

#[derive(clap::Args, Debug)]
#[command(group(
    clap::ArgGroup::new("target")
        .required(true)
        .args(["group", "all_groups"])
))]
pub struct LagArguments {
    /// Consumer group to read.
    #[arg(long)]
    pub group: Option<String>,
    /// Reads every consumer group of the cluster.
    #[arg(long)]
    pub all_groups: bool,
    /// Only partitions of this topic.
    #[arg(long)]
    pub topic: Option<String>,
    /// Order of the groups in the result.
    #[arg(long, value_enum, default_value_t = LagSortOrder::Lag)]
    pub sort: LagSortOrder,
    /// Keeps only the first N groups, after --sort.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=10_000))]
    pub top: Option<u32>,
    /// Exits with code 2 when the total lag of a group is above N; the report is still printed.
    #[arg(long, value_name = "N")]
    pub max_lag: Option<u64>,
    /// Timeout of each broker request, in seconds.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_seconds: u64,
}
