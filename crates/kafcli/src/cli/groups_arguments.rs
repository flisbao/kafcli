/// Subcommands of `kafcli groups`.
#[derive(clap::Subcommand)]
pub enum GroupsCommand {
    /// Check consumer groups against rules R1 to R5
    #[command(after_help = r#"Examples:
  kafcli groups audit
  kafcli groups audit --group pedidos-service --service-pattern '^(?P<service>[a-z-]+)-\d+$'
  kafcli groups audit --group-name-pattern '^[a-z-]+-(service|worker)$' --output json

Rules:
  R1 ERROR    members of one group belong to more than one service
  R2 ERROR    members of one group subscribe to different topic sets
  R3 WARNING  group without members whose committed offsets lag behind
  R4 INFO     more members than partitions in the subscribed topics
  R5 WARNING  group name does not match --group-name-pattern (off without a pattern)

Notes:
  Reads only: nothing is committed and no group is joined.
  Exit code 1 when any ERROR finding is reported; the report is still printed.
  The service of a member comes from --service-pattern over its client id, then from its host
  name without the Kubernetes pod suffix. A host reported as an IP address names no service.
  --service-pattern and --group-name-pattern override `groups_audit` in the configuration file.
  R2 and R4 apply to the `consumer` protocol only. Groups of the new consumer protocol (KIP-848)
  are listed without decoded members."#)]
    Audit(AuditGroupsArguments),
}

#[derive(clap::Args, Debug)]
pub struct AuditGroupsArguments {
    /// Consumer group to check; every group when omitted.
    #[arg(long)]
    pub group: Option<String>,
    /// Regex over client ids naming the service, with a `service` named group or a first group.
    #[arg(long, value_name = "REGEX")]
    pub service_pattern: Option<String>,
    /// Regex every group name must match (rule R5).
    #[arg(long, value_name = "REGEX")]
    pub group_name_pattern: Option<String>,
    /// Timeout of each broker request, in seconds.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout_seconds: u64,
}
