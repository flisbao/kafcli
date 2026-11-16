use crate::config::model::{AuthConfig, ConnectionContext, SaslCredentialsConfig};
use crate::error::{CoreError, CoreResult};

/// Checks that `context` may be used, given whether the user asked for `--insecure`.
///
/// Protected contexts refuse plaintext authentication, disabled TLS and `--insecure`.
pub fn validate_context(context: &ConnectionContext, insecure_requested: bool) -> CoreResult<()> {
    if context.brokers.is_empty() {
        return Err(build_validation_error("context has no brokers"));
    }
    if context
        .brokers
        .iter()
        .any(|broker| broker.trim().is_empty())
    {
        return Err(build_validation_error(
            "context has an empty broker address",
        ));
    }
    if !context.protected {
        return Ok(());
    }
    if matches!(context.auth, AuthConfig::Plaintext) {
        return Err(build_validation_error(
            "auth type 'none' is not allowed on a protected context",
        ));
    }
    if insecure_requested {
        return Err(build_validation_error(
            "--insecure is not allowed on a protected context",
        ));
    }
    if sasl_credentials_of(&context.auth).is_some_and(|credentials| !credentials.use_tls) {
        return Err(build_validation_error(
            "use_tls = false is not allowed on a protected context",
        ));
    }
    Ok(())
}

fn build_validation_error(message: &str) -> CoreError {
    CoreError::ContextValidation(message.to_string())
}

fn sasl_credentials_of(auth: &AuthConfig) -> Option<&SaslCredentialsConfig> {
    match auth {
        AuthConfig::SaslPlain(credentials)
        | AuthConfig::SaslScram256(credentials)
        | AuthConfig::SaslScram512(credentials) => Some(credentials),
        AuthConfig::Plaintext
        | AuthConfig::Tls(_)
        | AuthConfig::MutualTls(_)
        | AuthConfig::AzureConnectionString(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::TlsAuthConfig;
    use crate::secret::reference::SecretReference;

    fn context(auth: AuthConfig, protected: bool) -> ConnectionContext {
        ConnectionContext {
            brokers: vec!["broker:9093".to_string()],
            auth,
            protected,
            schema_registry: None,
        }
    }

    fn scram512_context(use_tls: bool) -> ConnectionContext {
        context(
            AuthConfig::SaslScram512(SaslCredentialsConfig {
                username: "app".to_string(),
                secret_ref: "env:KAFKA_PASSWORD".parse::<SecretReference>().unwrap(),
                ca_location: None,
                use_tls,
            }),
            true,
        )
    }

    fn validation_message(result: CoreResult<()>) -> String {
        match result {
            Err(CoreError::ContextValidation(message)) => message,
            other => panic!("expected ContextValidation, got {other:?}"),
        }
    }

    #[test]
    fn rejects_plaintext_auth_on_protected_context() {
        let protected_plaintext = context(AuthConfig::Plaintext, true);

        let message = validation_message(validate_context(&protected_plaintext, false));

        assert_eq!(
            message,
            "auth type 'none' is not allowed on a protected context"
        );
    }

    #[test]
    fn rejects_insecure_on_protected_context() {
        let protected_tls = context(AuthConfig::Tls(TlsAuthConfig { ca_location: None }), true);

        let message = validation_message(validate_context(&protected_tls, true));

        assert_eq!(message, "--insecure is not allowed on a protected context");
    }

    #[test]
    fn rejects_disabled_tls_on_protected_context() {
        let message = validation_message(validate_context(&scram512_context(false), false));

        assert_eq!(
            message,
            "use_tls = false is not allowed on a protected context"
        );
    }

    #[test]
    fn rejects_context_without_brokers() {
        let mut no_brokers = context(AuthConfig::Plaintext, false);
        no_brokers.brokers.clear();

        let message = validation_message(validate_context(&no_brokers, false));

        assert_eq!(message, "context has no brokers");
    }

    #[test]
    fn rejects_empty_broker_address() {
        let mut blank_broker = context(AuthConfig::Plaintext, false);
        blank_broker.brokers = vec!["a:9092".to_string(), "  ".to_string()];

        let message = validation_message(validate_context(&blank_broker, false));

        assert_eq!(message, "context has an empty broker address");
    }

    #[test]
    fn accepts_tls_context_marked_protected() {
        let protected_tls = context(AuthConfig::Tls(TlsAuthConfig { ca_location: None }), true);

        assert!(validate_context(&protected_tls, false).is_ok());
    }
}
