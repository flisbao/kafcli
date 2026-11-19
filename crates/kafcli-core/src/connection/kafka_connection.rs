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
        config: &ClientConfig,
    ) -> CoreResult<BaseConsumer<KafkaClientContext>> {
        let refresher = self.refresher.for_new_client();
        let consumer: BaseConsumer<KafkaClientContext> = config
            .create_with_context(KafkaClientContext::new(refresher.clone()))
            .map_err(map_kafka_error)?;
        self.wait_for_initial_token(&refresher, |timeout| {
            consumer.poll(timeout);
        })?;
        Ok(consumer)
    }

    /// Waits until `client` has served its first token. librdkafka requests tokens only while the
    /// client is polled, so `poll_once` must poll the client that owns `client`. `client` must be
    /// the refresher handed to that client, created with `OAuthTokenRefresher::for_new_client`.
    ///
    /// Returns at once when there is no token source.
    pub fn wait_for_initial_token(
        &self,
        client: &OAuthTokenRefresher,
        mut poll_once: impl FnMut(Duration),
    ) -> CoreResult<()> {
        if !client.has_token_source() {
            return Ok(());
        }
        let deadline = Instant::now() + INITIAL_TOKEN_TIMEOUT;
        loop {
            if client.has_served_token() {
                return Ok(());
            }
            if let Some(message) = client.last_error() {
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

#[expect(
    clippy::needless_pass_by_value,
    reason = "map adapter: map_err and Iterator::map pass the value"
)]
fn map_kafka_error(error: KafkaError) -> CoreError {
    CoreError::Kafka(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::token::access_token::AccessToken;
    use crate::auth::token::token_source::TokenSource;
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use secrecy::SecretString;
    use std::cell::Cell;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
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

    /// Fails on its first call and serves a token on every later call.
    struct FailOnceTokenSource {
        calls: AtomicUsize,
    }

    impl TokenSource for FailOnceTokenSource {
        fn fetch_access_token(&self) -> CoreResult<AccessToken> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(CoreError::Authentication("first token failed".to_owned()));
            }
            Ok(serving_token("opaque-token"))
        }

        fn describe_source(&self) -> &'static str {
            "fail-once"
        }
    }

    struct FixedTokenSource {
        token: String,
    }

    impl TokenSource for FixedTokenSource {
        fn fetch_access_token(&self) -> CoreResult<AccessToken> {
            Ok(serving_token(&self.token))
        }

        fn describe_source(&self) -> &'static str {
            "fixed"
        }
    }

    fn serving_token(value: &str) -> AccessToken {
        AccessToken::new(
            SecretString::from(value),
            UNIX_EPOCH + Duration::from_secs(2_000_000_000),
        )
    }

    fn jwt_with_subject(subject: &str) -> String {
        format!(
            "{}.{}.signature",
            URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#),
            URL_SAFE_NO_PAD.encode(format!(r#"{{"sub":"{subject}"}}"#))
        )
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
        let client = connection.refresher().for_new_client();
        let polls = Cell::new(0);

        let result = connection.wait_for_initial_token(&client, |_| polls.set(polls.get() + 1));

        assert!(result.is_ok());
        assert_eq!(polls.get(), 0);
    }

    #[test]
    fn wait_returns_source_error() {
        let connection = connection_with(Some(Arc::new(FailingTokenSource)));
        let client = connection.refresher().for_new_client();

        let result = connection.wait_for_initial_token(&client, |_| {
            let _ = client.generate_kafka_token();
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
        let connection = connection_with(Some(Arc::new(FixedTokenSource {
            token: "opaque-token".to_owned(),
        })));
        let client = connection.refresher().for_new_client();
        let polls = Cell::new(0);

        let result = connection.wait_for_initial_token(&client, |_| {
            polls.set(polls.get() + 1);
            if polls.get() == 2 {
                let _ = client.generate_kafka_token();
            }
        });

        assert!(result.is_ok());
        assert_eq!(polls.get(), 2);
    }

    #[test]
    fn second_client_on_same_connection_still_waits_for_its_own_token() {
        let connection = connection_with(Some(Arc::new(FixedTokenSource {
            token: "opaque-token".to_owned(),
        })));
        let first = connection.refresher().for_new_client();
        let _ = first.generate_kafka_token();
        connection.wait_for_initial_token(&first, |_| {}).unwrap();

        let second = connection.refresher().for_new_client();
        let polls = Cell::new(0);
        let result = connection.wait_for_initial_token(&second, |_| {
            polls.set(polls.get() + 1);
            let _ = second.generate_kafka_token();
        });

        assert!(result.is_ok());
        assert!(polls.get() >= 1, "second client skipped its wait");
    }

    #[test]
    fn stale_error_of_earlier_client_does_not_fail_later_wait() {
        let connection = connection_with(Some(Arc::new(FailOnceTokenSource {
            calls: AtomicUsize::new(0),
        })));
        let failed = connection.refresher().for_new_client();
        assert!(failed.generate_kafka_token().is_err());

        let later = connection.refresher().for_new_client();
        let result = connection.wait_for_initial_token(&later, |_| {
            let _ = later.generate_kafka_token();
        });

        assert!(result.is_ok());
    }

    #[test]
    fn shared_principal_is_visible_from_connection_after_client_served() {
        let connection = connection_with(Some(Arc::new(FixedTokenSource {
            token: jwt_with_subject("svc"),
        })));
        let client = connection.refresher().for_new_client();

        connection
            .wait_for_initial_token(&client, |_| {
                let _ = client.generate_kafka_token();
            })
            .unwrap();

        assert_eq!(
            connection.refresher().last_principal().as_deref(),
            Some("svc")
        );
    }
}
