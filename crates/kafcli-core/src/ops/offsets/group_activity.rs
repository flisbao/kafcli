//! Whether a consumer group has members, and the rule that only a stopped group gets new offsets.
//!
//! The rules follow `docs/adr/0001-event-hubs-capabilities.md`: a generic Kafka broker refuses a
//! commit for a group with members, Azure Event Hubs was not seen to, and Event Hubs may not
//! describe a group at all.

use std::time::{Duration, Instant};

use serde::Serialize;

use crate::capability::broker_capabilities::BrokerFamily;
use crate::error::{CoreError, CoreResult};
use crate::ops::groups::cluster_offsets_reader::ClusterOffsetsReader;

/// How often `wait_until_group_empty` reads the group again.
pub const GROUP_ACTIVITY_POLL_INTERVAL: Duration = Duration::from_secs(2);

const EVENT_HUBS_NOT_DESCRIBED_REASON: &str =
    "Azure Event Hubs did not describe the group (DescribeGroups is unverified, see ADR 0001)";

/// What `DescribeGroups` said about a group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum GroupActivity {
    Empty,
    Active {
        members: usize,
    },
    /// The broker left the group out of its answer.
    NotDescribed,
    DescribeFailed {
        error: String,
    },
}

/// Describes `group` once. Errors become `DescribeFailed`, so the caller decides what they mean.
pub fn read_group_activity(reader: &dyn ClusterOffsetsReader, group: &str) -> GroupActivity {
    match reader.describe_groups(Some(group)) {
        Ok(descriptions) => match descriptions
            .into_iter()
            .find(|description| description.name == group)
        {
            Some(description) if description.members.is_empty() => GroupActivity::Empty,
            Some(description) => GroupActivity::Active {
                members: description.members.len(),
            },
            None => GroupActivity::NotDescribed,
        },
        Err(CoreError::GroupNotFound(_)) => GroupActivity::NotDescribed,
        Err(error) => GroupActivity::DescribeFailed {
            error: error.to_string(),
        },
    }
}

/// Refuses offset changes unless the group is known to be stopped.
///
/// `Empty` passes. `Active` never passes, not even with `--group-stopped`. `NotDescribed` passes on
/// generic Kafka, whose broker refuses commits for active groups anyway, and needs
/// `--group-stopped` on Event Hubs. `DescribeFailed` needs `--group-stopped` everywhere.
pub fn check_group_is_stopped(
    group: &str,
    activity: &GroupActivity,
    family: BrokerFamily,
    group_stopped: bool,
) -> CoreResult<()> {
    let reason = match activity {
        GroupActivity::Empty => return Ok(()),
        GroupActivity::Active { members } => {
            return Err(CoreError::GroupNotEmpty {
                group: group.to_owned(),
                members: *members,
            });
        }
        GroupActivity::NotDescribed if family == BrokerFamily::GenericKafka => return Ok(()),
        GroupActivity::NotDescribed => EVENT_HUBS_NOT_DESCRIBED_REASON.to_owned(),
        GroupActivity::DescribeFailed { error } => format!("DescribeGroups failed: {error}"),
    };
    if group_stopped {
        return Ok(());
    }
    Err(CoreError::GroupStateUnknown(format!(
        "could not confirm that consumer group '{group}' is empty: {reason}; pass --group-stopped if its consumers are stopped"
    )))
}

/// Reads the group every `GROUP_ACTIVITY_POLL_INTERVAL` until it is no longer `Active` or
/// `timeout` passes. Waiting serves the reader's client events, never sleeping.
pub fn wait_until_group_empty(
    reader: &dyn ClusterOffsetsReader,
    group: &str,
    timeout: Duration,
    now: &dyn Fn() -> Instant,
) -> CoreResult<GroupActivity> {
    let deadline = now() + timeout;
    loop {
        let activity = read_group_activity(reader, group);
        let GroupActivity::Active { members } = activity else {
            return Ok(activity);
        };
        let remaining = deadline.saturating_duration_since(now());
        if remaining.is_zero() {
            return Err(CoreError::GroupStillActive {
                group: group.to_owned(),
                members,
                waited_seconds: timeout.as_secs(),
            });
        }
        reader.wait_serving_client_events(remaining.min(GROUP_ACTIVITY_POLL_INTERVAL));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::groups::fake_cluster_reader::FakeClusterReader;
    use crate::ops::groups::group_snapshot::{GroupDescription, GroupMember};

    fn member(id: &str) -> GroupMember {
        GroupMember {
            member_id: id.to_owned(),
            client_id: format!("{id}-client"),
            host: "/10.0.0.7".to_owned(),
            subscription: None,
            assignment: None,
        }
    }

    fn group(members: usize) -> GroupDescription {
        GroupDescription {
            name: "g".to_owned(),
            state: if members == 0 { "Empty" } else { "Stable" }.to_owned(),
            protocol_type: "consumer".to_owned(),
            members: (0..members)
                .map(|index| member(&format!("m{index}")))
                .collect(),
        }
    }

    fn reader_with(groups: Vec<GroupDescription>) -> FakeClusterReader {
        let mut reader = FakeClusterReader::new(BrokerFamily::GenericKafka);
        reader.groups = groups;
        reader
    }

    #[test]
    fn described_group_without_members_is_empty() {
        let reader = reader_with(vec![group(0)]);

        assert_eq!(read_group_activity(&reader, "g"), GroupActivity::Empty);
    }

    #[test]
    fn described_group_with_members_is_active() {
        let reader = reader_with(vec![group(2)]);

        assert_eq!(
            read_group_activity(&reader, "g"),
            GroupActivity::Active { members: 2 }
        );
    }

    #[test]
    fn missing_group_is_not_described() {
        let reader = reader_with(Vec::new());

        assert_eq!(
            read_group_activity(&reader, "g"),
            GroupActivity::NotDescribed
        );
    }

    #[test]
    fn describe_error_is_describe_failed() {
        let mut reader = reader_with(vec![group(0)]);
        reader.describe_failure = Some("CoordinatorNotAvailable".to_owned());

        assert_eq!(
            read_group_activity(&reader, "g"),
            GroupActivity::DescribeFailed {
                error: "kafka error: CoordinatorNotAvailable".to_owned()
            }
        );
    }

    #[test]
    fn empty_group_passes() {
        for family in [BrokerFamily::GenericKafka, BrokerFamily::AzureEventHubs] {
            assert!(check_group_is_stopped("g", &GroupActivity::Empty, family, false).is_ok());
        }
    }

    #[test]
    fn active_group_is_refused_even_with_group_stopped() {
        let activity = GroupActivity::Active { members: 2 };

        let error =
            check_group_is_stopped("g", &activity, BrokerFamily::GenericKafka, true).unwrap_err();

        assert!(
            matches!(error, CoreError::GroupNotEmpty { members: 2, .. }),
            "{error:?}"
        );
        assert_eq!(
            error.to_string(),
            "consumer group 'g' has 2 active member(s); stop its consumers or pass --wait-empty <DURATION>"
        );
    }

    #[test]
    fn not_described_passes_on_generic_kafka() {
        let result = check_group_is_stopped(
            "g",
            &GroupActivity::NotDescribed,
            BrokerFamily::GenericKafka,
            false,
        );

        assert!(result.is_ok(), "{result:?}");
    }

    #[test]
    fn not_described_on_event_hubs_needs_group_stopped() {
        let refused = check_group_is_stopped(
            "g",
            &GroupActivity::NotDescribed,
            BrokerFamily::AzureEventHubs,
            false,
        )
        .unwrap_err();
        let allowed = check_group_is_stopped(
            "g",
            &GroupActivity::NotDescribed,
            BrokerFamily::AzureEventHubs,
            true,
        );

        assert!(
            matches!(refused, CoreError::GroupStateUnknown(_)),
            "{refused:?}"
        );
        assert_eq!(
            refused.to_string(),
            "could not confirm that consumer group 'g' is empty: Azure Event Hubs did not describe the group (DescribeGroups is unverified, see ADR 0001); pass --group-stopped if its consumers are stopped"
        );
        assert!(allowed.is_ok(), "{allowed:?}");
    }

    #[test]
    fn describe_failure_needs_group_stopped_on_any_family() {
        let activity = GroupActivity::DescribeFailed {
            error: "kafka error: CoordinatorNotAvailable".to_owned(),
        };
        for family in [BrokerFamily::GenericKafka, BrokerFamily::AzureEventHubs] {
            let refused = check_group_is_stopped("g", &activity, family, false).unwrap_err();

            assert_eq!(
                refused.to_string(),
                "could not confirm that consumer group 'g' is empty: DescribeGroups failed: kafka error: CoordinatorNotAvailable; pass --group-stopped if its consumers are stopped"
            );
            assert!(check_group_is_stopped("g", &activity, family, true).is_ok());
        }
    }

    #[test]
    fn wait_returns_when_group_becomes_empty() {
        let reader = reader_with(Vec::new());
        reader.group_snapshots.borrow_mut().extend([
            vec![group(1)],
            vec![group(1)],
            vec![group(0)],
        ]);

        let activity =
            wait_until_group_empty(&reader, "g", Duration::from_secs(60), &|| reader.now())
                .unwrap();

        assert_eq!(activity, GroupActivity::Empty);
        assert_eq!(
            reader.waits(),
            vec![Duration::from_secs(2), Duration::from_secs(2)]
        );
    }

    #[test]
    fn wait_times_out_with_members_and_seconds() {
        let reader = reader_with(vec![group(1)]);

        let error = wait_until_group_empty(&reader, "g", Duration::from_secs(5), &|| reader.now())
            .unwrap_err();

        assert!(
            matches!(
                error,
                CoreError::GroupStillActive {
                    members: 1,
                    waited_seconds: 5,
                    ..
                }
            ),
            "{error:?}"
        );
        assert_eq!(
            error.to_string(),
            "consumer group 'g' still has 1 active member(s) after waiting 5s"
        );
        assert_eq!(
            reader.waits(),
            vec![
                Duration::from_secs(2),
                Duration::from_secs(2),
                Duration::from_secs(1)
            ]
        );
    }
}
