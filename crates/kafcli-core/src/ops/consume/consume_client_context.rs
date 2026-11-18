//! Client context of the consume consumers.
//!
//! librdkafka also reports the end of a partition through its global error callback. `poll` already
//! returns that end as `PartitionEOF`, so the callback must not log it as an error on stderr.

use crate::connection::oauth_token_refresher::OAuthTokenRefresher;
use rdkafka::ClientContext;
use rdkafka::client::OAuthToken;
use rdkafka::consumer::{BaseConsumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use std::error::Error;

/// The consumer type used by the consume operation.
pub(crate) type ConsumeConsumer = BaseConsumer<ConsumeClientContext>;

pub(crate) struct ConsumeClientContext {
    refresher: OAuthTokenRefresher,
}

impl ConsumeClientContext {
    pub(crate) fn new(refresher: OAuthTokenRefresher) -> Self {
        Self { refresher }
    }
}

impl ClientContext for ConsumeClientContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn error(&self, error: KafkaError, reason: &str) {
        if matches!(error, KafkaError::Global(RDKafkaErrorCode::PartitionEOF)) {
            return;
        }
        tracing::error!("librdkafka: {error}: {reason}");
    }

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        self.refresher.generate_kafka_token()
    }
}

impl ConsumerContext for ConsumeClientContext {}
