use crate::cli::watch_interval::parse_watch_interval;
use std::time::Duration;

/// How `kafcli lag` orders the groups.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum LagSortOrder {
    /// Highest total lag first; ties ordered by group name.
    Lag,
    /// By group name.
    Group,
}

/// How much of each group `kafcli lag` shows in a table.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum LagDetail {
    /// One line per group.
    Group,
    /// One line per group and topic.
    Topic,
    /// One line per partition, with the member that reads it.
    Partition,
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
    /// Level of the table: one line per group, per topic or per partition. Defaults to group with
    /// --all-groups and partition with --group.
    #[arg(long, value_enum)]
    pub detail: Option<LagDetail>,
    /// Keeps only the first N groups, after --sort.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=10_000))]
    pub top: Option<u32>,
    /// Exits with code 2 when the total lag of a group is above N; the report is still printed.
    #[arg(long, value_name = "N")]
    pub max_lag: Option<u64>,
    /// Samples again every INTERVAL (5s, 1m or seconds, 1s to 3600s) and shows the change.
    #[arg(long, value_name = "INTERVAL", value_parser = parse_watch_interval)]
    pub watch: Option<Duration>,
    /// Timeout of each broker request, in seconds.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_seconds: u64,
}
