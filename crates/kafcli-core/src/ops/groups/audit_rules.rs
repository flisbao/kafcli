//! Rules R1 to R5 of `kafcli groups audit` (spec §6.8), as pure functions over what was read.

use std::collections::{BTreeMap, BTreeSet};

use regex::Regex;
use serde::Serialize;

use crate::ops::groups::group_lag::GroupLag;
use crate::ops::groups::group_snapshot::GroupDescription;
use crate::ops::groups::service_identification::{ServicePattern, identify_member_service};

/// Protocol type whose member metadata kafcli decodes.
const CONSUMER_PROTOCOL_TYPE: &str = "consumer";
/// Rule name of notes about what could not be checked.
const COLLECTION_NOTE: &str = "INFO";

/// Severity of an audit finding, most severe first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum AuditSeverity {
    Error,
    Warning,
    Info,
}

/// One finding. `rule` is `R1` to `R5`, or `INFO` for notes about what could not be checked.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AuditFinding {
    pub group: String,
    pub rule: &'static str,
    pub severity: AuditSeverity,
    pub message: String,
}

/// What the rules need about one group. `lag` is only read by R3, for groups without members.
pub struct AuditInput<'a> {
    pub group: &'a GroupDescription,
    pub lag: Option<&'a GroupLag>,
    pub topic_partition_counts: &'a BTreeMap<String, usize>,
    pub service_pattern: Option<&'a ServicePattern>,
    pub group_name_pattern: Option<&'a Regex>,
}

/// Evaluates every rule for one group; findings are ordered by severity, then by rule.
pub fn evaluate_group_rules(input: &AuditInput) -> Vec<AuditFinding> {
    let mut findings = FindingList {
        group: &input.group.name,
        findings: Vec::new(),
    };
    check_single_service(input, &mut findings);
    if input.group.protocol_type == CONSUMER_PROTOCOL_TYPE {
        check_same_subscription(input.group, &mut findings);
        check_idle_members(input, &mut findings);
    } else if !input.group.members.is_empty() {
        findings.push(
            COLLECTION_NOTE,
            AuditSeverity::Info,
            format!(
                "protocol type '{}' is not decoded; R2 and R4 skipped",
                input.group.protocol_type
            ),
        );
    }
    check_lag_without_members(input, &mut findings);
    check_group_name(input, &mut findings);
    let mut findings = findings.findings;
    findings.sort_by(|left, right| {
        left.severity
            .cmp(&right.severity)
            .then_with(|| left.rule.cmp(right.rule))
    });
    findings
}

struct FindingList<'a> {
    group: &'a str,
    findings: Vec<AuditFinding>,
}

impl FindingList<'_> {
    fn push(&mut self, rule: &'static str, severity: AuditSeverity, message: String) {
        self.findings.push(AuditFinding {
            group: self.group.to_owned(),
            rule,
            severity,
            message,
        });
    }
}

/// R1: every identified member belongs to the same service.
fn check_single_service(input: &AuditInput, findings: &mut FindingList) {
    let mut services = BTreeSet::new();
    let mut unidentified = 0usize;
    for member in &input.group.members {
        match identify_member_service(member, input.service_pattern) {
            Some(service) => {
                services.insert(service);
            }
            None => unidentified += 1,
        }
    }
    if unidentified > 0 {
        findings.push(
            COLLECTION_NOTE,
            AuditSeverity::Info,
            format!("service not identified for {unidentified} member(s)"),
        );
    }
    if services.len() > 1 {
        let names: Vec<String> = services.into_iter().collect();
        findings.push(
            "R1",
            AuditSeverity::Error,
            format!(
                "members belong to more than one service: {}",
                names.join(", ")
            ),
        );
    }
}

/// R2: every decoded member subscribes to the same topic set.
fn check_same_subscription(group: &GroupDescription, findings: &mut FindingList) {
    let mut topic_sets = BTreeSet::new();
    let mut undecoded = 0usize;
    for member in &group.members {
        match &member.subscription {
            Some(subscription) => {
                topic_sets.insert(subscription.topics.clone());
            }
            None => undecoded += 1,
        }
    }
    if undecoded > 0 {
        findings.push(
            COLLECTION_NOTE,
            AuditSeverity::Info,
            format!("subscription not decoded for {undecoded} member(s); R2 skips them"),
        );
    }
    if topic_sets.len() > 1 {
        let described: Vec<String> = topic_sets
            .iter()
            .map(|topics| format!("[{}]", topics.join(", ")))
            .collect();
        findings.push(
            "R2",
            AuditSeverity::Error,
            format!(
                "members subscribe to different topic sets: {}",
                described.join(", ")
            ),
        );
    }
}

/// R4: more members than partitions in the subscribed topics leaves members idle.
fn check_idle_members(input: &AuditInput, findings: &mut FindingList) {
    let subscribed: BTreeSet<&str> = input
        .group
        .members
        .iter()
        .filter_map(|member| member.subscription.as_ref())
        .flat_map(|subscription| subscription.topics.iter().map(String::as_str))
        .collect();
    if subscribed.is_empty() {
        return;
    }
    let partitions: usize = subscribed
        .iter()
        .filter_map(|topic| input.topic_partition_counts.get(*topic))
        .sum();
    let members = input.group.members.len();
    if members > partitions {
        findings.push(
            "R4",
            AuditSeverity::Info,
            format!(
                "{members} members but only {partitions} partition(s) in the subscribed topics"
            ),
        );
    }
}

/// R3: a group without members whose lag keeps growing has no one consuming.
fn check_lag_without_members(input: &AuditInput, findings: &mut FindingList) {
    if !input.group.members.is_empty() {
        return;
    }
    if let Some(lag) = input.lag.filter(|lag| lag.total_lag > 0) {
        findings.push(
            "R3",
            AuditSeverity::Warning,
            format!("no active members and total lag {}", lag.total_lag),
        );
    }
}

/// R5: the group name follows `group_name_pattern`, when one is configured.
fn check_group_name(input: &AuditInput, findings: &mut FindingList) {
    if let Some(pattern) = input
        .group_name_pattern
        .filter(|pattern| !pattern.is_match(&input.group.name))
    {
        findings.push(
            "R5",
            AuditSeverity::Warning,
            format!(
                "group name does not match group_name_pattern '{}'",
                pattern.as_str()
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::groups::consumer_protocol::MemberSubscription;
    use crate::ops::groups::group_snapshot::GroupMember;

    fn member(id: &str, host: &str, topics: Option<&[&str]>) -> GroupMember {
        GroupMember {
            member_id: id.to_owned(),
            client_id: "rdkafka".to_owned(),
            host: host.to_owned(),
            subscription: topics.map(|topics| MemberSubscription {
                topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
            }),
            assignment: None,
        }
    }

    fn group(protocol_type: &str, members: Vec<GroupMember>) -> GroupDescription {
        GroupDescription {
            name: "pedidos-service".to_owned(),
            state: "Stable".to_owned(),
            protocol_type: protocol_type.to_owned(),
            members,
        }
    }

    fn lag(total_lag: i64) -> GroupLag {
        GroupLag {
            group: "pedidos-service".to_owned(),
            state: "Empty".to_owned(),
            has_active_members: false,
            total_lag,
            topics: Vec::new(),
        }
    }

    fn counts(entries: &[(&str, usize)]) -> BTreeMap<String, usize> {
        entries
            .iter()
            .map(|(topic, count)| ((*topic).to_owned(), *count))
            .collect()
    }

    fn evaluate(
        description: &GroupDescription,
        partition_counts: &BTreeMap<String, usize>,
    ) -> Vec<AuditFinding> {
        evaluate_group_rules(&AuditInput {
            group: description,
            lag: None,
            topic_partition_counts: partition_counts,
            service_pattern: None,
            group_name_pattern: None,
        })
    }

    fn rules(findings: &[AuditFinding]) -> Vec<(&'static str, AuditSeverity)> {
        findings
            .iter()
            .map(|finding| (finding.rule, finding.severity))
            .collect()
    }

    #[test]
    fn r1_two_services_in_one_group_is_error() {
        let description = group(
            "consumer",
            vec![
                member("m1", "pedidos-api-7d9f8c6b5-x2kqp", Some(&["pedidos"])),
                member("m2", "estoque-api-5c6d7f8b9-bcdfg", Some(&["pedidos"])),
            ],
        );

        let findings = evaluate(&description, &counts(&[("pedidos", 3)]));

        assert_eq!(rules(&findings), vec![("R1", AuditSeverity::Error)]);
        assert_eq!(
            findings[0].message,
            "members belong to more than one service: estoque-api, pedidos-api"
        );
        assert_eq!(findings[0].group, "pedidos-service");
    }

    #[test]
    fn unidentified_members_are_info_and_not_r1() {
        let description = group(
            "consumer",
            vec![
                member("m1", "pedidos-api-7d9f8c6b5-x2kqp", Some(&["pedidos"])),
                member("m2", "/10.0.0.7", Some(&["pedidos"])),
            ],
        );

        let findings = evaluate(&description, &counts(&[("pedidos", 3)]));

        assert_eq!(rules(&findings), vec![("INFO", AuditSeverity::Info)]);
        assert_eq!(
            findings[0].message,
            "service not identified for 1 member(s)"
        );
    }

    #[test]
    fn r2_different_subscriptions_is_error() {
        let description = group(
            "consumer",
            vec![
                member("m1", "pedidos-api-7d9f8c6b5-x2kqp", Some(&["pedidos"])),
                member(
                    "m2",
                    "pedidos-api-7d9f8c6b5-bcdfg",
                    Some(&["pagamentos", "pedidos"]),
                ),
            ],
        );

        let findings = evaluate(&description, &counts(&[("pedidos", 3), ("pagamentos", 3)]));

        assert_eq!(rules(&findings), vec![("R2", AuditSeverity::Error)]);
        assert_eq!(
            findings[0].message,
            "members subscribe to different topic sets: [pagamentos, pedidos], [pedidos]"
        );
    }

    #[test]
    fn r3_empty_group_with_lag_is_warning() {
        let description = group("consumer", Vec::new());
        let group_lag = lag(42);
        let partition_counts = counts(&[]);

        let findings = evaluate_group_rules(&AuditInput {
            group: &description,
            lag: Some(&group_lag),
            topic_partition_counts: &partition_counts,
            service_pattern: None,
            group_name_pattern: None,
        });

        assert_eq!(rules(&findings), vec![("R3", AuditSeverity::Warning)]);
        assert_eq!(findings[0].message, "no active members and total lag 42");
    }

    #[test]
    fn r3_empty_group_without_lag_is_clean() {
        let description = group("consumer", Vec::new());
        let group_lag = lag(0);
        let partition_counts = counts(&[]);

        let findings = evaluate_group_rules(&AuditInput {
            group: &description,
            lag: Some(&group_lag),
            topic_partition_counts: &partition_counts,
            service_pattern: None,
            group_name_pattern: None,
        });

        assert!(findings.is_empty(), "{findings:?}");
    }

    #[test]
    fn r4_more_members_than_partitions_is_info() {
        let description = group(
            "consumer",
            vec![
                member("m1", "pedidos-api-7d9f8c6b5-x2kqp", Some(&["pedidos"])),
                member("m2", "pedidos-api-7d9f8c6b5-bcdfg", Some(&["pedidos"])),
                member("m3", "pedidos-api-7d9f8c6b5-bcdfh", Some(&["pedidos"])),
            ],
        );

        let findings = evaluate(&description, &counts(&[("pedidos", 2)]));

        assert_eq!(rules(&findings), vec![("R4", AuditSeverity::Info)]);
        assert_eq!(
            findings[0].message,
            "3 members but only 2 partition(s) in the subscribed topics"
        );
    }

    #[test]
    fn r5_disabled_without_pattern_and_warns_with_pattern() {
        let description = group("consumer", Vec::new());
        let partition_counts = counts(&[]);
        let name_pattern = Regex::new(r"^[a-z]+-(service|worker)$").unwrap();
        let other_pattern = Regex::new(r"^team-").unwrap();

        assert!(evaluate(&description, &partition_counts).is_empty());
        let matching = evaluate_group_rules(&AuditInput {
            group: &description,
            lag: None,
            topic_partition_counts: &partition_counts,
            service_pattern: None,
            group_name_pattern: Some(&name_pattern),
        });
        assert!(matching.is_empty(), "{matching:?}");
        let findings = evaluate_group_rules(&AuditInput {
            group: &description,
            lag: None,
            topic_partition_counts: &partition_counts,
            service_pattern: None,
            group_name_pattern: Some(&other_pattern),
        });
        assert_eq!(rules(&findings), vec![("R5", AuditSeverity::Warning)]);
        assert_eq!(
            findings[0].message,
            "group name does not match group_name_pattern '^team-'"
        );
    }

    #[test]
    fn non_consumer_protocol_skips_r2_r4_with_info() {
        let description = group(
            "connect",
            vec![
                member("m1", "pedidos-api-7d9f8c6b5-x2kqp", Some(&["pedidos"])),
                member("m2", "pedidos-api-7d9f8c6b5-bcdfg", Some(&["pagamentos"])),
                member("m3", "pedidos-api-7d9f8c6b5-bcdfh", Some(&["pagamentos"])),
            ],
        );

        let findings = evaluate(&description, &counts(&[("pedidos", 1), ("pagamentos", 1)]));

        assert_eq!(rules(&findings), vec![("INFO", AuditSeverity::Info)]);
        assert_eq!(
            findings[0].message,
            "protocol type 'connect' is not decoded; R2 and R4 skipped"
        );
    }

    #[test]
    fn undecodable_subscription_is_info_not_r2() {
        let description = group(
            "consumer",
            vec![
                member("m1", "pedidos-api-7d9f8c6b5-x2kqp", Some(&["pedidos"])),
                member("m2", "pedidos-api-7d9f8c6b5-bcdfg", None),
            ],
        );

        let findings = evaluate(&description, &counts(&[("pedidos", 3)]));

        assert_eq!(rules(&findings), vec![("INFO", AuditSeverity::Info)]);
        assert_eq!(
            findings[0].message,
            "subscription not decoded for 1 member(s); R2 skips them"
        );
    }

    #[test]
    fn findings_are_ordered_by_severity_then_rule() {
        let description = group(
            "consumer",
            vec![
                member("m1", "pedidos-api-7d9f8c6b5-x2kqp", Some(&["pedidos"])),
                member("m2", "estoque-api-5c6d7f8b9-bcdfg", Some(&["pagamentos"])),
                member("m3", "/10.0.0.7", None),
            ],
        );
        let partition_counts = counts(&[("pedidos", 1), ("pagamentos", 1)]);
        let name_pattern = Regex::new(r"^team-").unwrap();

        let findings = evaluate_group_rules(&AuditInput {
            group: &description,
            lag: None,
            topic_partition_counts: &partition_counts,
            service_pattern: None,
            group_name_pattern: Some(&name_pattern),
        });

        assert_eq!(
            rules(&findings),
            vec![
                ("R1", AuditSeverity::Error),
                ("R2", AuditSeverity::Error),
                ("R5", AuditSeverity::Warning),
                ("INFO", AuditSeverity::Info),
                ("INFO", AuditSeverity::Info),
                ("R4", AuditSeverity::Info),
            ]
        );
    }

    #[test]
    fn severity_serializes_uppercase() {
        assert_eq!(
            serde_json::to_string(&AuditSeverity::Error).unwrap(),
            r#""ERROR""#
        );
        assert_eq!(
            serde_json::to_string(&AuditSeverity::Warning).unwrap(),
            r#""WARNING""#
        );
        assert_eq!(
            serde_json::to_string(&AuditSeverity::Info).unwrap(),
            r#""INFO""#
        );
    }
}
