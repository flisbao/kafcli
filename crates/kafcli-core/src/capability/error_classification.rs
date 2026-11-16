use crate::capability::capability_report::CapabilityOutcome;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

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

fn is_unsupported_code(code: RDKafkaErrorCode) -> bool {
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
    fn authorization_error_is_failed() {
        let outcome = classify_kafka_error(&KafkaError::GroupListFetch(
            RDKafkaErrorCode::GroupAuthorizationFailed,
        ));

        assert!(matches!(outcome, CapabilityOutcome::Failed { .. }));
    }
}
