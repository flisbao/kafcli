use super::client_properties::ClientProperties;
use super::oauth_bearer_settings::apply_oauth_bearer_settings;
use super::provider::AuthProvider;
use crate::auth::token::access_token::AccessToken;
use crate::auth::token::cached_token_source::CachedTokenSource;
use crate::auth::token::jwt_claims::read_unverified_jwt_claims;
use crate::auth::token::token_source::TokenSource;
use crate::config::model::BearerAuthConfig;
use crate::error::{CoreError, CoreResult};
use crate::message::timestamp_text::format_rfc3339_millis;
use crate::secret::resolver::SecretResolver;
use secrecy::{ExposeSecret, SecretString};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use time::OffsetDateTime;

/// Lifetime given to a token that carries no `exp` claim.
const OPAQUE_TOKEN_LIFETIME: Duration = Duration::from_secs(3600);
/// A JWT that expires sooner than this is still served, with a warning in the log.
const EXPIRY_WARNING_WINDOW: Duration = Duration::from_secs(300);

/// Bearer token for OAUTHBEARER. The token is read once when the connection is built.
#[derive(Debug)]
pub struct BearerAuthProvider {
    config: BearerAuthConfig,
}

impl BearerAuthProvider {
    pub fn new(config: BearerAuthConfig) -> Self {
        Self { config }
    }
}

impl AuthProvider for BearerAuthProvider {
    fn build_client_properties(
        &self,
        _resolver: &dyn SecretResolver,
    ) -> CoreResult<ClientProperties> {
        let mut properties = ClientProperties::default();
        apply_oauth_bearer_settings(
            &mut properties,
            self.config.use_tls,
            self.config.ca_location.as_deref(),
        );
        Ok(properties)
    }

    fn create_token_source(
        &self,
        _brokers: &[String],
        resolver: &dyn SecretResolver,
    ) -> CoreResult<Option<Arc<dyn TokenSource>>> {
        // The resolver borrowed here is not `Send + Sync` (the composite resolver holds boxed
        // closures and trait objects without those bounds), so it cannot be kept for later
        // refreshes. The token is resolved once and the cache serves it until it expires.
        let token = resolver.resolve_secret(&self.config.token_ref)?;
        let source = StaticBearerTokenSource::new(token);
        Ok(Some(Arc::new(CachedTokenSource::new(Box::new(source)))))
    }
}

/// Serves the bearer token that was read when the connection was built.
struct StaticBearerTokenSource {
    token: SecretString,
}

impl StaticBearerTokenSource {
    fn new(token: SecretString) -> Self {
        Self { token }
    }
}

impl TokenSource for StaticBearerTokenSource {
    fn fetch_access_token(&self) -> CoreResult<AccessToken> {
        access_token_at(&self.token, SystemTime::now())
    }

    fn describe_source(&self) -> &'static str {
        "bearer"
    }
}

/// Builds the access token for `token` as seen at `now`. A JWT takes its lifetime from `exp`;
/// any other token lives for one hour. Errors name the expiry instant, never the token.
fn access_token_at(token: &SecretString, now: SystemTime) -> CoreResult<AccessToken> {
    let value = token.expose_secret();
    let expires_at = match read_unverified_jwt_claims(value).and_then(|claims| claims.expires_at) {
        Some(expires_at) => {
            if expires_at <= now {
                return Err(CoreError::Authentication(format!(
                    "bearer token expired at {}",
                    describe_instant(expires_at)
                )));
            }
            if let Ok(remaining) = expires_at.duration_since(now)
                && remaining < EXPIRY_WARNING_WINDOW
            {
                let seconds = remaining.as_secs();
                tracing::warn!("bearer token expires in {seconds} seconds");
            }
            expires_at
        }
        None => now + OPAQUE_TOKEN_LIFETIME,
    };
    Ok(AccessToken::new(
        SecretString::from(value.to_owned()),
        expires_at,
    ))
}

fn describe_instant(instant: SystemTime) -> String {
    format_rfc3339_millis(OffsetDateTime::from(instant))
        .unwrap_or_else(|| "an unknown time".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CoreError;
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use crate::secret::reference::SecretReference;
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn bearer_config(use_tls: bool) -> BearerAuthConfig {
        BearerAuthConfig {
            token_ref: "env:KAFKA_TOKEN".parse::<SecretReference>().unwrap(),
            ca_location: None,
            use_tls,
        }
    }

    fn jwt_expiring_at(exp_seconds: u64) -> String {
        format!(
            "{}.{}.",
            URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#),
            URL_SAFE_NO_PAD.encode(format!(r#"{{"sub":"kafcli-it","exp":{exp_seconds}}}"#))
        )
    }

    fn seconds_from_now(offset: u64) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + offset
    }

    fn source_for(token: &str) -> StaticBearerTokenSource {
        StaticBearerTokenSource::new(SecretString::from(token.to_owned()))
    }

    #[test]
    fn sets_oauthbearer_mechanism_and_protocol() {
        let resolver = FixedSecretResolver::returning("unused");

        let tls = BearerAuthProvider::new(bearer_config(true))
            .build_client_properties(&resolver)
            .unwrap();
        assert_eq!(tls.plain_value("security.protocol"), Some("SASL_SSL"));
        assert_eq!(tls.plain_value("sasl.mechanism"), Some("OAUTHBEARER"));

        let plaintext = BearerAuthProvider::new(bearer_config(false))
            .build_client_properties(&resolver)
            .unwrap();
        assert_eq!(
            plaintext.plain_value("security.protocol"),
            Some("SASL_PLAINTEXT")
        );
        assert_eq!(plaintext.plain_value("sasl.mechanism"), Some("OAUTHBEARER"));
    }

    #[test]
    fn creates_token_source_from_resolved_reference() {
        let token = jwt_expiring_at(seconds_from_now(3600));
        let provider = BearerAuthProvider::new(bearer_config(true));

        let source = provider
            .create_token_source(
                &["kafka:9093".to_string()],
                &FixedSecretResolver::returning(&token),
            )
            .unwrap()
            .unwrap();

        assert_eq!(source.describe_source(), "bearer");
        assert!(source.fetch_access_token().is_ok());
    }

    #[test]
    fn expired_jwt_is_authentication_error() {
        let error = source_for(&jwt_expiring_at(1))
            .fetch_access_token()
            .unwrap_err();

        assert!(
            matches!(&error, CoreError::Authentication(message) if message.starts_with("bearer token expired at ")),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn opaque_token_lives_one_hour() {
        let before = SystemTime::now();
        let token = source_for("opaque-token-value")
            .fetch_access_token()
            .unwrap();
        let after = SystemTime::now();

        assert!(token.expires_at >= before + Duration::from_secs(3599));
        assert!(token.expires_at <= after + Duration::from_secs(3601));
    }

    #[test]
    fn error_never_contains_token() {
        let token = jwt_expiring_at(1);

        let error = source_for(&token)
            .fetch_access_token()
            .unwrap_err()
            .to_string();

        assert!(!error.contains(&token));
        assert!(!error.contains("eyJ"));
    }
}
