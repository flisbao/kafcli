//! Which service a group member belongs to, from its client id or its pod host name.

use regex::Regex;

use crate::error::{CoreError, CoreResult};
use crate::ops::groups::group_snapshot::GroupMember;

/// Characters Kubernetes uses in generated name suffixes (no vowels, no 0, 1 and 3).
const POD_SUFFIX_ALPHABET: &str = "bcdfghjklmnpqrstvwxz2456789";
const POD_IDENTIFIER_LENGTH: usize = 5;
const REPLICA_SET_HASH_LENGTHS: std::ops::RangeInclusive<usize> = 6..=10;
const SERVICE_GROUP_NAME: &str = "service";

/// A `service_pattern` regex applied to client ids. The service is the `service` named group, or
/// the first capture group when there is no named one.
#[derive(Clone, Debug)]
pub struct ServicePattern(Regex);

impl ServicePattern {
    pub fn compile(text: &str) -> CoreResult<Self> {
        let regex = Regex::new(text).map_err(|error| {
            CoreError::InvalidInput(format!("service_pattern is not a valid regex: {error}"))
        })?;
        let has_named_group = regex
            .capture_names()
            .flatten()
            .any(|name| name == SERVICE_GROUP_NAME);
        if !has_named_group && regex.captures_len() < 2 {
            return Err(CoreError::InvalidInput(
                "service_pattern needs a capture group (preferably (?P<service>...))".to_owned(),
            ));
        }
        Ok(Self(regex))
    }

    fn capture_service(&self, client_id: &str) -> Option<String> {
        let captures = self.0.captures(client_id)?;
        let service = captures
            .name(SERVICE_GROUP_NAME)
            .or_else(|| captures.get(1))?
            .as_str();
        (!service.is_empty()).then(|| service.to_owned())
    }
}

/// Removes a Kubernetes pod suffix from a host name. `None` when there is no suffix to remove.
pub fn strip_pod_suffix(host: &str) -> Option<String> {
    if host.starts_with('/') {
        return None;
    }
    let (rest, identifier) = host.rsplit_once('-')?;
    if !is_pod_segment(identifier, POD_IDENTIFIER_LENGTH..=POD_IDENTIFIER_LENGTH) {
        return None;
    }
    let base = match rest.rsplit_once('-') {
        Some((base, hash)) if is_pod_segment(hash, REPLICA_SET_HASH_LENGTHS) => base,
        _ => rest,
    };
    (!base.is_empty()).then(|| base.to_owned())
}

fn is_pod_segment(segment: &str, lengths: std::ops::RangeInclusive<usize>) -> bool {
    lengths.contains(&segment.len())
        && segment
            .chars()
            .all(|character| POD_SUFFIX_ALPHABET.contains(character))
}

/// The service of `member`: the pattern over `client_id` first, then the pod suffix of `host`.
pub fn identify_member_service(
    member: &GroupMember,
    pattern: Option<&ServicePattern>,
) -> Option<String> {
    pattern
        .and_then(|pattern| pattern.capture_service(&member.client_id))
        .or_else(|| strip_pod_suffix(&member.host))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(client_id: &str, host: &str) -> GroupMember {
        GroupMember {
            member_id: "m".to_owned(),
            client_id: client_id.to_owned(),
            host: host.to_owned(),
            subscription: None,
            assignment: None,
        }
    }

    #[test]
    fn strips_replicaset_pod_suffix() {
        assert_eq!(
            strip_pod_suffix("pedidos-api-7d9f8c6b5-x2kqp").as_deref(),
            Some("pedidos-api")
        );
    }

    #[test]
    fn strips_bare_pod_suffix() {
        assert_eq!(strip_pod_suffix("worker-x2kqp").as_deref(), Some("worker"));
    }

    #[test]
    fn keeps_names_without_pod_pattern() {
        assert_eq!(strip_pod_suffix("pedidos-api").as_deref(), None);
        assert_eq!(strip_pod_suffix("pedidos-abcde").as_deref(), None);
    }

    #[test]
    fn ip_host_is_not_a_service() {
        assert_eq!(strip_pod_suffix("/10.0.0.7"), None);
    }

    #[test]
    fn suffix_only_host_is_not_a_service() {
        assert_eq!(strip_pod_suffix("-x2kqp"), None);
    }

    #[test]
    fn client_id_pattern_wins_over_host() {
        let pattern = ServicePattern::compile(r"^(?P<service>[a-z-]+)-consumer-\d+$").unwrap();
        let member = member("pagamentos-consumer-1", "pedidos-api-7d9f8c6b5-x2kqp");

        assert_eq!(
            identify_member_service(&member, Some(&pattern)).as_deref(),
            Some("pagamentos")
        );
    }

    #[test]
    fn unnamed_capture_group_is_used() {
        let pattern = ServicePattern::compile(r"^([a-z]+)-\d+$").unwrap();

        assert_eq!(
            identify_member_service(&member("estoque-3", "/10.0.0.1"), Some(&pattern)).as_deref(),
            Some("estoque")
        );
    }

    #[test]
    fn host_is_used_when_pattern_does_not_match() {
        let pattern = ServicePattern::compile(r"^(?P<service>[a-z]+)-consumer$").unwrap();

        assert_eq!(
            identify_member_service(&member("rdkafka", "worker-x2kqp"), Some(&pattern)).as_deref(),
            Some("worker")
        );
        assert_eq!(
            identify_member_service(&member("rdkafka", "/10.0.0.1"), None),
            None
        );
    }

    #[test]
    fn pattern_without_capture_group_is_rejected() {
        let error = ServicePattern::compile(r"^[a-z]+$").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("service_pattern needs a capture group (preferably (?P<service>...))"),
            "{error}"
        );
    }

    #[test]
    fn invalid_pattern_is_rejected() {
        let error = ServicePattern::compile(r"(").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("service_pattern is not a valid regex:"),
            "{error}"
        );
    }
}
