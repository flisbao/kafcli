use crate::auth::provider_factory::create_auth_provider;
use crate::config::model::ConnectionContext;
use crate::connection::client_config_builder::{ConnectionSettings, build_client_config};
use crate::connection::kafka_client_context::KafkaClientContext;
use crate::connection::oauth_token_refresher::OAuthTokenRefresher;
use crate::error::{CoreError, CoreResult};
use crate::secret::resolver::SecretResolver;
use rdkafka::ClientConfig;
use rdkafka::consumer::BaseConsumer;
use rdkafka::error::KafkaError;
use std::time::{Duration, Instant};

/// How long a client waits for its first OAUTHBEARER token before it gives up.
pub const INITIAL_TOKEN_TIMEOUT: Duration = Duration::from_secs(30);
const TOKEN_POLL_SLICE: Duration = Duration::from_millis(100);

/// The librdkafka configuration of one connection and the token refresher its clients share.
#[derive(Clone)]
pub struct KafkaConnection {
    config: ClientConfig,
    refresher: OAuthTokenRefresher,
}

impl KafkaConnection {
    pub fn client_config(&self) -> &ClientConfig {
        &self.config
    }

    pub fn refresher(&self) -> &OAuthTokenRefresher {
        &self.refresher
    }

    /// Creates a consumer from `config` and waits for its first token when there is a token source.
    pub fn create_consumer(
        &self,
        config: ClientConfig,
    ) -> CoreResult<BaseConsumer<KafkaClientContext>> {
        let consumer: BaseConsumer<KafkaClientContext> = config
            .create_with_context(KafkaClientContext::new(self.refresher.clone()))
            .map_err(map_kafka_error)?;
        self.wait_for_initial_token(|timeout| {
            consumer.poll(timeout);
        })?;
        Ok(consumer)
    }

    /// Waits until the first token is served. librdkafka requests tokens only while its client
    /// is polled, so `poll_once` must poll the client that owns the refresher.
    ///
    /// Returns at once when there is no token source.
    pub fn wait_for_initial_token(&self, mut poll_once: impl FnMut(Duration)) -> CoreResult<()> {
        if !self.refresher.has_token_source() {
            return Ok(());
        }
        let deadline = Instant::now() + INITIAL_TOKEN_TIMEOUT;
        loop {
            if self.refresher.has_served_token() {
                return Ok(());
            }
            if let Some(message) = self.refresher.last_error() {
                return Err(CoreError::Authentication(message));
            }
            if Instant::now() >= deadline {
                return Err(CoreError::Authentication(format!(
                    "no OAUTHBEARER token was produced within {} s",
                    INITIAL_TOKEN_TIMEOUT.as_secs()
                )));
            }
            poll_once(TOKEN_POLL_SLICE);
        }
    }
}

/// Builds the connection for `context`: the client configuration plus the token source of its
/// authentication mode.
pub fn build_kafka_connection(
    context: &ConnectionContext,
    resolver: &dyn SecretResolver,
    settings: &ConnectionSettings,
) -> CoreResult<KafkaConnection> {
    let config = build_client_config(context, resolver, settings)?;
    let source =
        create_auth_provider(&context.auth).create_token_source(&context.brokers, resolver)?;
    Ok(KafkaConnection {
        config,
        refresher: OAuthTokenRefresher::new(source),
    })
}

fn map_kafka_error(error: KafkaError) -> CoreError {
    CoreError::Kafka(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::token::access_token::AccessToken;
    use crate::auth::token::token_source::TokenSource;
    use secrecy::SecretString;
    use std::cell::Cell;
    use std::sync::Arc;
    use std::time::UNIX_EPOCH;

    struct FailingTokenSource;

    impl TokenSource for FailingTokenSource {
        fn fetch_access_token(&self) -> CoreResult<AccessToken> {
            Err(CoreError::Authentication(
                "azure token request failed (HTTP 401): invalid_client".to_owned(),
            ))
        }

        fn describe_source(&self) -> &'static str {
            "failing"
        }
    }

    struct FixedTokenSource;

    impl TokenSource for FixedTokenSource {
        fn fetch_access_token(&self) -> CoreResult<AccessToken> {
            Ok(AccessToken::new(
                SecretString::from("opaque-token"),
                UNIX_EPOCH + Duration::from_secs(2_000_000_000),
            ))
        }

        fn describe_source(&self) -> &'static str {
            "fixed"
        }
    }

    fn connection_with(source: Option<Arc<dyn TokenSource>>) -> KafkaConnection {
        KafkaConnection {
            config: ClientConfig::new(),
            refresher: OAuthTokenRefresher::new(source),
        }
    }

    #[test]
    fn wait_returns_immediately_without_token_source() {
        let connection = connection_with(None);
        let polls = Cell::new(0);

        let result = connection.wait_for_initial_token(|_| polls.set(polls.get() + 1));

        assert!(result.is_ok());
        assert_eq!(polls.get(), 0);
    }

    #[test]
    fn wait_returns_source_error() {
        let connection = connection_with(Some(Arc::new(FailingTokenSource)));
        let refresher = connection.refresher().clone();

        let result = connection.wait_for_initial_token(|_| {
            let _ = refresher.generate_kafka_token();
        });

        match result {
            Err(CoreError::Authentication(message)) => {
                assert!(
                    message.contains("HTTP 401"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected an authentication error, got {other:?}"),
        }
    }

    #[test]
    fn wait_succeeds_after_token_is_served() {
        let connection = connection_with(Some(Arc::new(FixedTokenSource)));
        let refresher = connection.refresher().clone();
        let polls = Cell::new(0);

        let result = connection.wait_for_initial_token(|_| {
            polls.set(polls.get() + 1);
            if polls.get() == 2 {
                let _ = refresher.generate_kafka_token();
            }
        });

        assert!(result.is_ok());
        assert_eq!(polls.get(), 2);
    }
}
