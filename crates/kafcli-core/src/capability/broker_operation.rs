//! Broker operations used by the group and offset commands, with their status per broker family.
//!
//! The statuses follow `docs/adr/0001-event-hubs-capabilities.md`.

use crate::capability::broker_capabilities::{BrokerFamily, CapabilityStatus};
use crate::capability::error_classification::is_unsupported_code;
use crate::error::CoreError;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

const INCOMPLETE_GROUP_LIST_WARNING: &str =
    "the group list may be incomplete on Azure Event Hubs (ListGroups is unverified, see ADR 0001)";

/// A Kafka API that the group commands call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BrokerOperation {
    ListGroups,
    DescribeGroups,
    OffsetFetch,
    ListOffsets,
    OffsetCommit,
    OffsetsForTimes,
}

impl BrokerOperation {
    /// The Kafka protocol name of the API, as written in ADR 0001.
    pub fn api_name(self) -> &'static str {
        match self {
            Self::ListGroups => "ListGroups",
            Self::DescribeGroups => "DescribeGroups",
            Self::OffsetFetch => "OffsetFetch",
            Self::ListOffsets => "ListOffsets",
            Self::OffsetCommit => "OffsetCommit",
            Self::OffsetsForTimes => "OffsetsForTimes",
        }
    }
}

/// Whether an operation is confirmed for a broker family.
pub fn operation_status(family: BrokerFamily, operation: BrokerOperation) -> CapabilityStatus {
    match (family, operation) {
        (BrokerFamily::GenericKafka, _) => CapabilityStatus::Documented,
        (
            BrokerFamily::AzureEventHubs,
            BrokerOperation::ListGroups | BrokerOperation::DescribeGroups,
        ) => CapabilityStatus::Unverified,
        (
            BrokerFamily::AzureEventHubs,
            BrokerOperation::OffsetFetch
            | BrokerOperation::ListOffsets
            | BrokerOperation::OffsetCommit
            | BrokerOperation::OffsetsForTimes,
        ) => CapabilityStatus::EmulatorOnly,
    }
}

/// Turns a broker error from an operation into the error kafcli reports.
pub fn map_broker_operation_error(
    operation: BrokerOperation,
    family: BrokerFamily,
    group: Option<&str>,
    error: &KafkaError,
) -> CoreError {
    let code = error.rdkafka_error_code();
    if let (Some(RDKafkaErrorCode::GroupIdNotFound), Some(group)) = (code, group) {
        return CoreError::GroupNotFound(group.to_owned());
    }
    if operation == BrokerOperation::OffsetCommit
        && let (Some(code), Some(group)) = (code, group)
        && is_active_member_code(code)
    {
        return CoreError::CommitRefusedActiveGroup(group.to_owned());
    }
    match code {
        Some(code) if is_unsupported_code(code) => {
            let operation_name = operation.api_name();
            let unconfirmed_on_event_hubs = family == BrokerFamily::AzureEventHubs
                && operation_status(family, operation) != CapabilityStatus::Documented;
            if unconfirmed_on_event_hubs {
                CoreError::UnsupportedOperation(format!(
                    "{operation_name} is not supported by this broker (Azure Event Hubs): {code:?}"
                ))
            } else {
                CoreError::UnsupportedOperation(format!(
                    "{operation_name} is not supported by this broker: {code:?}"
                ))
            }
        }
        _ => CoreError::Kafka(error.to_string()),
    }
}

/// Codes a broker answers an offset commit with when the group has members kafcli is not one of.
fn is_active_member_code(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::UnknownMemberId
            | RDKafkaErrorCode::IllegalGeneration
            | RDKafkaErrorCode::RebalanceInProgress
    )
}

/// Warning added when the group list may be partial, which only applies to Event Hubs.
pub fn incomplete_group_list_warning(family: BrokerFamily) -> Option<&'static str> {
    (family == BrokerFamily::AzureEventHubs).then_some(INCOMPLETE_GROUP_LIST_WARNING)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::error::RDKafkaErrorCode;

    #[test]
    fn api_names_match_the_protocol() {
        assert_eq!(BrokerOperation::ListGroups.api_name(), "ListGroups");
        assert_eq!(BrokerOperation::DescribeGroups.api_name(), "DescribeGroups");
        assert_eq!(BrokerOperation::OffsetFetch.api_name(), "OffsetFetch");
        assert_eq!(BrokerOperation::ListOffsets.api_name(), "ListOffsets");
    }

    #[test]
    fn event_hubs_group_operations_are_unverified() {
        let family = BrokerFamily::AzureEventHubs;

        assert_eq!(
            operation_status(family, BrokerOperation::ListGroups),
            CapabilityStatus::Unverified
        );
        assert_eq!(
            operation_status(family, BrokerOperation::DescribeGroups),
            CapabilityStatus::Unverified
        );
        assert_eq!(
            operation_status(family, BrokerOperation::OffsetFetch),
            CapabilityStatus::EmulatorOnly
        );
        assert_eq!(
            operation_status(family, BrokerOperation::ListOffsets),
            CapabilityStatus::EmulatorOnly
        );
    }

    #[test]
    fn generic_kafka_operations_are_documented() {
        for operation in [
            BrokerOperation::ListGroups,
            BrokerOperation::DescribeGroups,
            BrokerOperation::OffsetFetch,
            BrokerOperation::ListOffsets,
        ] {
            assert_eq!(
                operation_status(BrokerFamily::GenericKafka, operation),
                CapabilityStatus::Documented
            );
        }
    }

    #[test]
    fn group_not_found_is_not_unsupported() {
        let error = KafkaError::GroupListFetch(RDKafkaErrorCode::GroupIdNotFound);

        let mapped = map_broker_operation_error(
            BrokerOperation::DescribeGroups,
            BrokerFamily::AzureEventHubs,
            Some("g"),
            &error,
        );

        assert!(
            matches!(&mapped, CoreError::GroupNotFound(group) if group == "g"),
            "{mapped:?}"
        );
        assert_eq!(mapped.to_string(), "consumer group 'g' not found");
    }

    #[test]
    fn unsupported_on_event_hubs_names_operation_and_service() {
        let error = KafkaError::GroupListFetch(RDKafkaErrorCode::UnsupportedVersion);

        let mapped = map_broker_operation_error(
            BrokerOperation::DescribeGroups,
            BrokerFamily::AzureEventHubs,
            Some("g"),
            &error,
        );

        assert!(matches!(mapped, CoreError::UnsupportedOperation(_)));
        assert_eq!(
            mapped.to_string(),
            "DescribeGroups is not supported by this broker (Azure Event Hubs): UnsupportedVersion"
        );
    }

    #[test]
    fn unsupported_on_generic_kafka_has_no_service_name() {
        let error = KafkaError::OffsetFetch(RDKafkaErrorCode::InvalidRequest);

        let mapped = map_broker_operation_error(
            BrokerOperation::OffsetFetch,
            BrokerFamily::GenericKafka,
            Some("g"),
            &error,
        );

        assert_eq!(
            mapped.to_string(),
            "OffsetFetch is not supported by this broker: InvalidRequest"
        );
    }

    #[test]
    fn other_errors_stay_kafka_errors() {
        let error = KafkaError::GroupListFetch(RDKafkaErrorCode::GroupAuthorizationFailed);

        let mapped = map_broker_operation_error(
            BrokerOperation::ListGroups,
            BrokerFamily::AzureEventHubs,
            None,
            &error,
        );

        assert!(matches!(mapped, CoreError::Kafka(_)), "{mapped:?}");
    }

    #[test]
    fn warning_only_for_event_hubs() {
        assert_eq!(
            incomplete_group_list_warning(BrokerFamily::AzureEventHubs),
            Some(
                "the group list may be incomplete on Azure Event Hubs (ListGroups is unverified, see ADR 0001)"
            )
        );
        assert_eq!(
            incomplete_group_list_warning(BrokerFamily::GenericKafka),
            None
        );
    }

    #[test]
    fn offset_commit_and_offsets_for_times_are_emulator_only_on_event_hubs() {
        assert_eq!(BrokerOperation::OffsetCommit.api_name(), "OffsetCommit");
        assert_eq!(
            BrokerOperation::OffsetsForTimes.api_name(),
            "OffsetsForTimes"
        );
        for operation in [
            BrokerOperation::OffsetCommit,
            BrokerOperation::OffsetsForTimes,
        ] {
            assert_eq!(
                operation_status(BrokerFamily::AzureEventHubs, operation),
                CapabilityStatus::EmulatorOnly
            );
            assert_eq!(
                operation_status(BrokerFamily::GenericKafka, operation),
                CapabilityStatus::Documented
            );
        }
    }

    #[test]
    fn commit_refused_by_member_codes_names_the_group() {
        for code in [
            RDKafkaErrorCode::UnknownMemberId,
            RDKafkaErrorCode::IllegalGeneration,
            RDKafkaErrorCode::RebalanceInProgress,
        ] {
            let error = KafkaError::ConsumerCommit(code);

            let mapped = map_broker_operation_error(
                BrokerOperation::OffsetCommit,
                BrokerFamily::GenericKafka,
                Some("g"),
                &error,
            );

            assert!(
                matches!(&mapped, CoreError::CommitRefusedActiveGroup(group) if group == "g"),
                "{mapped:?}"
            );
            assert_eq!(
                mapped.to_string(),
                "the broker refused the offset commit: consumer group 'g' has active members"
            );
        }
    }

    #[test]
    fn unsupported_commit_on_event_hubs_names_the_service() {
        let error = KafkaError::ConsumerCommit(RDKafkaErrorCode::UnsupportedVersion);

        let mapped = map_broker_operation_error(
            BrokerOperation::OffsetCommit,
            BrokerFamily::AzureEventHubs,
            Some("g"),
            &error,
        );

        assert_eq!(
            mapped.to_string(),
            "OffsetCommit is not supported by this broker (Azure Event Hubs): UnsupportedVersion"
        );
    }
}
