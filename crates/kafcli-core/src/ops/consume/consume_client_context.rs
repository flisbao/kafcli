//! Client context of the consume consumers.
//!
//! librdkafka also reports the end of a partition through its global error callback. `poll` already
//! returns that end as `PartitionEOF`, so the callback must not log it as an error on stderr.

use rdkafka::ClientContext;
use rdkafka::consumer::{BaseConsumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

/// The consumer type used by the consume operation.
pub(crate) type ConsumeConsumer = BaseConsumer<ConsumeClientContext>;

pub(crate) struct ConsumeClientContext;

impl ClientContext for ConsumeClientContext {
    fn error(&self, error: KafkaError, reason: &str) {
        if matches!(error, KafkaError::Global(RDKafkaErrorCode::PartitionEOF)) {
            return;
        }
        tracing::error!("librdkafka: {error}: {reason}");
    }
}

impl ConsumerContext for ConsumeClientContext {}
