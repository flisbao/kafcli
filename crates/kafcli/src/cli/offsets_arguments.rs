use crate::cli::watch_interval::parse_watch_interval;
use kafcli_core::ops::offsets::offset_target::OffsetTarget;
use std::path::PathBuf;
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

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
    /// Move the committed offsets of a stopped consumer group
    #[command(
        allow_negative_numbers = true,
        after_help = r#"Examples:
  kafcli offsets set --group pedidos-service --topic pedidos --to-earliest
  kafcli offsets set --group pedidos-service --topic pedidos --to-latest --execute
  kafcli offsets set --group pedidos-service --topic pedidos --to-datetime 2026-11-18T10:00:00-03:00
  kafcli offsets set --group pedidos-service --topic pedidos --partition 0 --shift-by -100
  kafcli offsets set --group pedidos-service --topic pedidos --to-latest --execute --wait-empty 2m

Notes:
  Dry-run is the default: the table shows each partition before and after, and nothing is
  committed until you pass --execute.
  The group must have no active members. --wait-empty waits for its consumers to leave.
  --group-stopped is for brokers that cannot describe the group (Azure Event Hubs): it says you
  know its consumers are stopped.
  Before committing, the current offsets are saved under <state dir>/offsets-backup/; pass that
  file to 'kafcli offsets restore' to go back.
  On a protected context you must type the group name to confirm."#
    )]
    Set(SetOffsetsArguments),
    /// Move a stopped consumer group back to the offsets of a backup
    #[command(after_help = r#"Examples:
  kafcli offsets restore --file ~/.local/state/kafcli/offsets-backup/pedidos-service-20261118T130000.000Z.json
  kafcli offsets restore --file pedidos-service-20261118T130000.000Z.json --execute

Notes:
  Dry-run is the default; nothing is committed until you pass --execute.
  Only backup files written by 'kafcli offsets set' (under <state dir>/offsets-backup/) are
  accepted, and only on the context they were taken on.
  Partitions that had no committed offset in the backup are left as they are now.
  The restore backs up the current offsets first, so it can be undone the same way.
  The group must have no active members; on a protected context you must type the group name."#)]
    Restore(RestoreOffsetsArguments),
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

#[derive(clap::Args, Debug)]
#[command(group(
    clap::ArgGroup::new("target")
        .required(true)
        .args(["to_offset", "to_earliest", "to_latest", "to_datetime", "shift_by"])
))]
pub struct SetOffsetsArguments {
    /// Consumer group whose offsets change.
    #[arg(long)]
    pub group: String,
    /// Topic whose partitions change.
    #[arg(long)]
    pub topic: String,
    /// Only this partition; repeat for more. Default: every partition of the topic.
    #[arg(long = "partition", value_name = "P")]
    pub partitions: Vec<i32>,
    /// Moves to offset N, limited to the log start and end.
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(i64).range(0..))]
    pub to_offset: Option<i64>,
    /// Moves to the log start.
    #[arg(long)]
    pub to_earliest: bool,
    /// Moves to the log end.
    #[arg(long)]
    pub to_latest: bool,
    /// Moves to the first message at or after this RFC 3339 instant (the offset is required).
    #[arg(long, value_name = "RFC3339", value_parser = parse_datetime)]
    pub to_datetime: Option<OffsetDateTime>,
    /// Moves by N from the committed offset (negative goes back).
    #[arg(long, value_name = "N")]
    pub shift_by: Option<i64>,
    /// Commits the new offsets; without it nothing changes.
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

impl SetOffsetsArguments {
    pub fn target(&self) -> OffsetTarget {
        if let Some(offset) = self.to_offset {
            OffsetTarget::Offset(offset)
        } else if self.to_earliest {
            OffsetTarget::Earliest
        } else if let Some(instant) = self.to_datetime {
            OffsetTarget::Datetime(instant)
        } else if let Some(delta) = self.shift_by {
            OffsetTarget::ShiftBy(delta)
        } else {
            OffsetTarget::Latest
        }
    }
}

#[derive(clap::Args, Debug)]
pub struct RestoreOffsetsArguments {
    /// Backup file written by 'kafcli offsets set'.
    #[arg(long, value_name = "PATH")]
    pub file: PathBuf,
    /// Commits the backed-up offsets; without it nothing changes.
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

fn parse_datetime(text: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(text, &Rfc3339).map_err(|_| {
        format!(
            "'{text}' is not an RFC 3339 instant with an offset, like 2026-11-18T10:00:00-03:00"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(clap::Parser)]
    struct OffsetsCli {
        #[command(subcommand)]
        command: OffsetsCommand,
    }

    fn parse(arguments: &[&str]) -> Result<SetOffsetsArguments, clap::Error> {
        let mut full = vec!["kafcli", "set", "--group", "g", "--topic", "t"];
        full.extend_from_slice(arguments);
        match OffsetsCli::try_parse_from(full)?.command {
            OffsetsCommand::Set(arguments) => Ok(arguments),
            _ => panic!("parsed another subcommand"),
        }
    }

    #[test]
    fn exactly_one_target_is_required() {
        assert!(parse(&[]).is_err());
        assert!(parse(&["--to-earliest", "--to-latest"]).is_err());
        assert_eq!(
            parse(&["--to-latest"]).unwrap().target(),
            OffsetTarget::Latest
        );
        assert_eq!(
            parse(&["--to-offset", "7"]).unwrap().target(),
            OffsetTarget::Offset(7)
        );
    }

    #[test]
    fn shift_by_accepts_negative_numbers() {
        let arguments =
            parse(&["--shift-by", "-100", "--partition", "0", "--partition", "2"]).unwrap();

        assert_eq!(arguments.target(), OffsetTarget::ShiftBy(-100));
        assert_eq!(arguments.partitions, vec![0, 2]);
    }

    #[test]
    fn datetime_needs_an_offset() {
        assert!(parse(&["--to-datetime", "2026-11-18T10:00:00"]).is_err());
        let arguments = parse(&["--to-datetime", "2026-11-18T10:00:00-03:00"]).unwrap();

        assert!(matches!(arguments.target(), OffsetTarget::Datetime(_)));
    }

    #[test]
    fn negative_to_offset_is_refused() {
        assert!(parse(&["--to-offset", "-1"]).is_err());
    }

    #[test]
    fn wait_empty_and_defaults() {
        let arguments = parse(&["--to-latest", "--wait-empty", "2m"]).unwrap();

        assert_eq!(arguments.wait_empty, Some(Duration::from_secs(120)));
        assert_eq!(arguments.timeout_seconds, 30);
        assert!(!arguments.execute);
        assert!(!arguments.group_stopped);
    }
}
