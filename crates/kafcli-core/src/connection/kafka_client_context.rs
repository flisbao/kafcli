//! Client context of every rdkafka client that kafcli creates, so that OAUTHBEARER tokens are
//! served from the connection's token source.

use crate::connection::oauth_token_refresher::OAuthTokenRefresher;
use rdkafka::ClientContext;
use rdkafka::client::OAuthToken;
use rdkafka::consumer::ConsumerContext;
use std::error::Error;

pub struct KafkaClientContext {
    refresher: OAuthTokenRefresher,
}

impl KafkaClientContext {
    pub fn new(refresher: OAuthTokenRefresher) -> Self {
        Self { refresher }
    }
}

impl ClientContext for KafkaClientContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        self.refresher.generate_kafka_token()
    }
}

impl ConsumerContext for KafkaClientContext {}
