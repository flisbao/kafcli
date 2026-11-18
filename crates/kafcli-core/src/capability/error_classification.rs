use crate::capability::capability_report::CapabilityOutcome;
use crate::error::CoreError;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::types::RDKafkaRespErr;

/// Error for a topic whose metadata entry carries `error`. Only `UnknownTopicOrPartition` means the
/// topic does not exist; any other code, such as `LeaderNotAvailable`, is reported as it is.
pub(crate) fn classify_topic_metadata_error(topic: &str, error: RDKafkaRespErr) -> CoreError {
    match RDKafkaErrorCode::from(error) {
        RDKafkaErrorCode::UnknownTopicOrPartition => CoreError::TopicNotFound(topic.to_string()),
        code => CoreError::Kafka(format!("topic '{topic}' metadata error: {code:?}")),
    }
}

/// Maps a Kafka error to a capability outcome.
///
/// Only errors that mean "the broker does not offer this" are `Unsupported`; every other error is `Failed`.
pub fn classify_kafka_error(error: &KafkaError) -> CapabilityOutcome {
    match error.rdkafka_error_code() {
        Some(code) if is_unsupported_code(code) => CapabilityOutcome::Unsupported {
            error_code: format!("{code:?}"),
            detail: None,
        },
        _ => CapabilityOutcome::Failed {
            message: error.to_string(),
        },
    }
}

pub(crate) fn is_unsupported_code(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::UnsupportedVersion
            | RDKafkaErrorCode::InvalidRequest
            | RDKafkaErrorCode::UnsupportedFeature
            | RDKafkaErrorCode::UnsupportedForMessageFormat
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_version_is_unsupported() {
        let outcome = classify_kafka_error(&KafkaError::MetadataFetch(
            RDKafkaErrorCode::UnsupportedVersion,
        ));

        assert_eq!(
            outcome,
            CapabilityOutcome::Unsupported {
                error_code: "UnsupportedVersion".to_string(),
                detail: None,
            }
        );
    }

    #[test]
    fn unknown_topic_metadata_error_is_topic_not_found() {
        let error = classify_topic_metadata_error(
            "pedidos",
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART,
        );

        assert!(
            matches!(&error, CoreError::TopicNotFound(topic) if topic == "pedidos"),
            "{error:?}"
        );
    }

    #[test]
    fn leader_not_available_is_not_topic_not_found() {
        let error = classify_topic_metadata_error(
            "pedidos",
            RDKafkaRespErr::RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE,
        );

        assert!(matches!(error, CoreError::Kafka(_)), "{error:?}");
        assert_eq!(
            error.to_string(),
            "kafka error: topic 'pedidos' metadata error: LeaderNotAvailable"
        );
    }

    #[test]
    fn authorization_error_is_failed() {
        let outcome = classify_kafka_error(&KafkaError::GroupListFetch(
            RDKafkaErrorCode::GroupAuthorizationFailed,
        ));

        assert!(matches!(outcome, CapabilityOutcome::Failed { .. }));
    }
}
