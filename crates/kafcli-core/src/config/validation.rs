use crate::config::model::{AuthConfig, Profile, SaslCredentialsConfig};
use crate::error::{CoreError, CoreResult};

/// Checks that `profile` may be used, given whether the user asked for `--insecure`.
///
/// Protected profiles refuse plaintext authentication, disabled TLS and `--insecure`.
pub fn validate_profile(profile: &Profile, insecure_requested: bool) -> CoreResult<()> {
    if profile.brokers.is_empty() {
        return Err(validation_error("profile has no brokers"));
    }
    if profile
        .brokers
        .iter()
        .any(|broker| broker.trim().is_empty())
    {
        return Err(validation_error("profile has an empty broker address"));
    }
    if !profile.protected {
        return Ok(());
    }
    if matches!(profile.auth, AuthConfig::Plaintext) {
        return Err(validation_error(
            "auth type 'none' is not allowed on a protected profile",
        ));
    }
    if insecure_requested {
        return Err(validation_error(
            "--insecure is not allowed on a protected profile",
        ));
    }
    if sasl_credentials_of(&profile.auth).is_some_and(|credentials| !credentials.use_tls) {
        return Err(validation_error(
            "use_tls = false is not allowed on a protected profile",
        ));
    }
    Ok(())
}

fn validation_error(message: &str) -> CoreError {
    CoreError::ProfileValidation(message.to_string())
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

    fn profile(auth: AuthConfig, protected: bool) -> Profile {
        Profile {
            brokers: vec!["broker:9093".to_string()],
            auth,
            protected,
            schema_registry: None,
        }
    }

    fn scram512_profile(use_tls: bool) -> Profile {
        profile(
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
            Err(CoreError::ProfileValidation(message)) => message,
            other => panic!("expected ProfileValidation, got {other:?}"),
        }
    }

    #[test]
    fn rejects_plaintext_auth_on_protected_profile() {
        let protected_plaintext = profile(AuthConfig::Plaintext, true);

        let message = validation_message(validate_profile(&protected_plaintext, false));

        assert_eq!(
            message,
            "auth type 'none' is not allowed on a protected profile"
        );
    }

    #[test]
    fn rejects_insecure_on_protected_profile() {
        let protected_tls = profile(AuthConfig::Tls(TlsAuthConfig { ca_location: None }), true);

        let message = validation_message(validate_profile(&protected_tls, true));

        assert_eq!(message, "--insecure is not allowed on a protected profile");
    }

    #[test]
    fn rejects_disabled_tls_on_protected_profile() {
        let message = validation_message(validate_profile(&scram512_profile(false), false));

        assert_eq!(
            message,
            "use_tls = false is not allowed on a protected profile"
        );
    }

    #[test]
    fn rejects_profile_without_brokers() {
        let mut no_brokers = profile(AuthConfig::Plaintext, false);
        no_brokers.brokers.clear();

        let message = validation_message(validate_profile(&no_brokers, false));

        assert_eq!(message, "profile has no brokers");
    }

    #[test]
    fn rejects_empty_broker_address() {
        let mut blank_broker = profile(AuthConfig::Plaintext, false);
        blank_broker.brokers = vec!["a:9092".to_string(), "  ".to_string()];

        let message = validation_message(validate_profile(&blank_broker, false));

        assert_eq!(message, "profile has an empty broker address");
    }

    #[test]
    fn accepts_tls_profile_marked_protected() {
        let protected_tls = profile(AuthConfig::Tls(TlsAuthConfig { ca_location: None }), true);

        assert!(validate_profile(&protected_tls, false).is_ok());
    }
}
