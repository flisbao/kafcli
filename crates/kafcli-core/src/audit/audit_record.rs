use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// Result of an audited command.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuditOutcome {
    Executed,
    DryRun,
    Aborted,
    Failed,
}

/// One line of the audit log. Never holds secret values.
#[derive(Debug, Serialize)]
pub struct AuditRecord {
    /// RFC 3339 UTC timestamp.
    pub timestamp: String,
    pub operator: String,
    pub profile: String,
    /// Command name, e.g. "offsets set".
    pub command: String,
    /// Target description, e.g. "group=pedidos-service topic=pedidos".
    pub target: String,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
    pub outcome: AuditOutcome,
}

impl AuditRecord {
    /// Builds a record stamped with the current UTC time and the operator from the environment.
    pub fn new(profile: &str, command: &str, target: &str, outcome: AuditOutcome) -> Self {
        Self {
            timestamp: current_timestamp(),
            operator: resolve_operator_name(),
            profile: profile.to_string(),
            command: command.to_string(),
            target: target.to_string(),
            before: None,
            after: None,
            outcome,
        }
    }
}

fn current_timestamp() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| UNKNOWN_TIMESTAMP.to_string())
}

fn resolve_operator_name() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| UNKNOWN_OPERATOR.to_string())
}

const UNKNOWN_TIMESTAMP: &str = "unknown-time";
const UNKNOWN_OPERATOR: &str = "unknown";

#[cfg(test)]
mod tests {
    use super::*;
    use time::OffsetDateTime;
    use time::format_description::well_known::Rfc3339;

    #[test]
    fn timestamp_is_rfc3339_utc() {
        let record = AuditRecord::new("dev", "topics list", "", AuditOutcome::Executed);
        assert!(record.timestamp.ends_with('Z'), "{}", record.timestamp);
        let parsed = OffsetDateTime::parse(&record.timestamp, &Rfc3339);
        assert!(parsed.is_ok(), "{}", record.timestamp);
    }

    #[test]
    fn serializes_outcome_in_kebab_case() {
        let record = AuditRecord::new("dev", "offsets set", "", AuditOutcome::DryRun);
        let value = serde_json::to_value(&record).unwrap();
        assert_eq!(value["outcome"], "dry-run");
    }
}
