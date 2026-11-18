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
    pub context: String,
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
    pub fn new(context: &str, command: &str, target: &str, outcome: AuditOutcome) -> Self {
        Self {
            timestamp: format_current_timestamp(),
            operator: resolve_operator_name(),
            context: context.to_string(),
            command: command.to_string(),
            target: target.to_string(),
            before: None,
            after: None,
            outcome,
        }
    }
}

fn format_current_timestamp() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| UNKNOWN_TIMESTAMP.to_string())
}

/// The operator name recorded in audit records and in `kafcli-original-operator`.
pub fn resolve_operator_name() -> String {
    resolve_operator_name_with(|name| std::env::var(name).ok())
}

/// Picks the operator from `USER`, then `USERNAME`; empty values count as absent.
fn resolve_operator_name_with(lookup: impl Fn(&str) -> Option<String>) -> String {
    ["USER", "USERNAME"]
        .iter()
        .find_map(|name| lookup(name).filter(|value| !value.is_empty()))
        .unwrap_or_else(|| UNKNOWN_OPERATOR.to_string())
}

const UNKNOWN_TIMESTAMP: &str = "unknown-time";
const UNKNOWN_OPERATOR: &str = "unknown";

#[cfg(test)]
mod tests {
    use super::*;

    fn build_lookup_from<'a>(
        values: &'a [(&'a str, &'a str)],
    ) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            values
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        }
    }

    #[test]
    fn operator_prefers_user_then_username_then_unknown() {
        let both = build_lookup_from(&[("USER", "alice"), ("USERNAME", "bob")]);
        assert_eq!(resolve_operator_name_with(both), "alice");
        let only_username = build_lookup_from(&[("USERNAME", "bob")]);
        assert_eq!(resolve_operator_name_with(only_username), "bob");
        let neither = build_lookup_from(&[]);
        assert_eq!(resolve_operator_name_with(neither), "unknown");
    }

    #[test]
    fn empty_user_falls_through_to_username() {
        let empty_user = build_lookup_from(&[("USER", ""), ("USERNAME", "bob")]);
        assert_eq!(resolve_operator_name_with(empty_user), "bob");
        let all_empty = build_lookup_from(&[("USER", ""), ("USERNAME", "")]);
        assert_eq!(resolve_operator_name_with(all_empty), "unknown");
    }

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

    #[test]
    fn serializes_context_key_and_no_profile_key() {
        let record = AuditRecord::new(
            "prd",
            "topics delete",
            "topic=orders",
            AuditOutcome::Executed,
        );
        let value = serde_json::to_value(&record).unwrap();
        assert_eq!(value["context"], "prd");
        assert!(value.get("profile").is_none());
    }
}
