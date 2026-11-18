use crate::auth::token::access_token::AccessToken;
use crate::auth::token::jwt_claims::read_unverified_jwt_claims;
use crate::auth::token::token_source::TokenSource;
use crate::error::CoreError;
use rdkafka::client::OAuthToken;
use secrecy::ExposeSecret;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

const FALLBACK_PRINCIPAL: &str = "kafcli";
const MISSING_SOURCE_MESSAGE: &str = "no OAUTHBEARER token source configured";

/// Hands tokens from a token source to librdkafka and records what happened, so that a client
/// can wait for its first token and report its failed token request.
///
/// `served` and `last_error` belong to one client: each client gets its own through
/// `for_new_client`. `last_principal` is shared by every refresher derived from one connection,
/// so the connection's refresher reports the last token served by any of its clients. `Clone`
/// hands out another handle to the same client state, not a new client.
#[derive(Clone)]
pub struct OAuthTokenRefresher {
    source: Option<Arc<dyn TokenSource>>,
    served: Arc<AtomicBool>,
    last_error: Arc<Mutex<Option<String>>>,
    last_principal: Arc<Mutex<Option<String>>>,
}

impl OAuthTokenRefresher {
    pub fn new(source: Option<Arc<dyn TokenSource>>) -> Self {
        Self {
            source,
            served: Arc::new(AtomicBool::new(false)),
            last_error: Arc::new(Mutex::new(None)),
            last_principal: Arc::new(Mutex::new(None)),
        }
    }

    pub fn has_token_source(&self) -> bool {
        self.source.is_some()
    }

    /// Refresher for one new client: same token source and shared principal, but its own
    /// served flag and last error.
    pub fn for_new_client(&self) -> Self {
        Self {
            source: self.source.clone(),
            served: Arc::new(AtomicBool::new(false)),
            last_error: Arc::new(Mutex::new(None)),
            last_principal: Arc::clone(&self.last_principal),
        }
    }

    /// Fetches a token from the source for librdkafka. The source's error text is kept as the
    /// last error; it is already redacted by the source.
    pub fn generate_kafka_token(&self) -> Result<OAuthToken, Box<dyn Error>> {
        let Some(source) = self.source.as_ref() else {
            return Err(MISSING_SOURCE_MESSAGE.into());
        };
        match source.fetch_access_token() {
            Ok(access_token) => Ok(self.serve(&access_token)),
            Err(error) => {
                let message = describe_error(error);
                *lock(&self.last_error) = Some(message.clone());
                Err(message.into())
            }
        }
    }

    pub fn has_served_token(&self) -> bool {
        self.served.load(Ordering::SeqCst)
    }

    pub fn last_error(&self) -> Option<String> {
        lock(&self.last_error).clone()
    }

    pub fn last_principal(&self) -> Option<String> {
        lock(&self.last_principal).clone()
    }

    fn serve(&self, access_token: &AccessToken) -> OAuthToken {
        let token = access_token.value.expose_secret().to_owned();
        let claims = read_unverified_jwt_claims(&token).unwrap_or_default();
        *lock(&self.last_principal) = claims.principal.or_else(|| claims.subject.clone());
        *lock(&self.last_error) = None;
        self.served.store(true, Ordering::SeqCst);
        OAuthToken {
            token,
            principal_name: claims
                .subject
                .unwrap_or_else(|| FALLBACK_PRINCIPAL.to_owned()),
            lifetime_ms: lifetime_millis(access_token.expires_at),
        }
    }
}

fn describe_error(error: CoreError) -> String {
    match error {
        CoreError::Authentication(message) | CoreError::AuthenticationTransient(message) => message,
        other => other.to_string(),
    }
}

fn lifetime_millis(expires_at: SystemTime) -> i64 {
    expires_at
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since_epoch| i64::try_from(since_epoch.as_millis()).ok())
        .unwrap_or(0)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CoreResult;
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use std::time::{Duration, UNIX_EPOCH};

    struct FixedTokenSource {
        outcome: Result<AccessToken, String>,
    }

    impl TokenSource for FixedTokenSource {
        fn fetch_access_token(&self) -> CoreResult<AccessToken> {
            self.outcome.clone().map_err(CoreError::Authentication)
        }

        fn describe_source(&self) -> &'static str {
            "fixed"
        }
    }

    fn jwt_with_claims(claims: &str) -> String {
        format!(
            "{}.{}.signature",
            URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#),
            URL_SAFE_NO_PAD.encode(claims)
        )
    }

    fn refresher_serving(token: &str) -> OAuthTokenRefresher {
        let access_token = AccessToken::new(
            secrecy::SecretString::from(token),
            UNIX_EPOCH + Duration::from_secs(2_000_000_000),
        );
        OAuthTokenRefresher::new(Some(Arc::new(FixedTokenSource {
            outcome: Ok(access_token),
        })))
    }

    #[test]
    fn maps_access_token_to_rdkafka_token() {
        let jwt = jwt_with_claims(r#"{"sub":"svc","exp":2000000000}"#);
        let refresher = refresher_serving(&jwt);

        let token = refresher.generate_kafka_token().unwrap();

        assert_eq!(token.token, jwt);
        assert_eq!(token.principal_name, "svc");
        assert_eq!(token.lifetime_ms, 2_000_000_000_000);
        assert!(refresher.has_served_token());
        assert_eq!(refresher.last_principal().as_deref(), Some("svc"));
    }

    #[test]
    fn records_source_error_without_serving_token() {
        let refresher = OAuthTokenRefresher::new(Some(Arc::new(FixedTokenSource {
            outcome: Err("azure token request failed (HTTP 401): invalid_client".to_owned()),
        })));

        let result = refresher.generate_kafka_token();

        assert!(result.is_err());
        assert_eq!(
            refresher.last_error().as_deref(),
            Some("azure token request failed (HTTP 401): invalid_client")
        );
        assert!(!refresher.has_served_token());
    }

    #[test]
    fn opaque_token_uses_kafcli_principal() {
        let refresher = refresher_serving("opaque-token");

        let token = refresher.generate_kafka_token().unwrap();

        assert_eq!(token.principal_name, "kafcli");
        assert_eq!(refresher.last_principal(), None);
    }

    #[test]
    fn shared_principal_is_visible_from_connection_refresher() {
        let jwt = jwt_with_claims(r#"{"sub":"svc","exp":2000000000}"#);
        let connection_refresher = refresher_serving(&jwt);
        let client = connection_refresher.for_new_client();

        client.generate_kafka_token().unwrap();

        assert_eq!(
            connection_refresher.last_principal().as_deref(),
            Some("svc")
        );
        assert!(!connection_refresher.has_served_token());
        assert!(client.has_served_token());
    }

    #[test]
    fn client_error_stays_with_that_client() {
        let failing = OAuthTokenRefresher::new(Some(Arc::new(FixedTokenSource {
            outcome: Err("token request failed".to_owned()),
        })));
        let client = failing.for_new_client();
        let other = client.for_new_client();

        let _ = client.generate_kafka_token();

        assert_eq!(client.last_error().as_deref(), Some("token request failed"));
        assert_eq!(other.last_error(), None);
    }

    #[test]
    fn missing_source_refuses_to_serve_token() {
        let refresher = OAuthTokenRefresher::new(None);

        let error = refresher.generate_kafka_token().err();

        assert!(!refresher.has_token_source());
        assert_eq!(
            error.map(|error| error.to_string()).as_deref(),
            Some("no OAUTHBEARER token source configured")
        );
    }
}
