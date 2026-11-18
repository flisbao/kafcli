use crate::auth::provider_factory::create_auth_provider;
use crate::config::model::ConnectionContext;
use crate::config::validation::validate_context;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;

pub const CLIENT_IDENTIFIER: &str = "kafcli";

const CONNECTION_SETUP_TIMEOUT_MILLIS: &str = "10000";
const INSECURE_CERTIFICATE_VERIFICATION: &str = "false";
const INSECURE_HOSTNAME_VERIFICATION: &str = "none";

/// Options that change how a connection is established for one command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionSettings {
    pub insecure: bool,
}

/// Builds the librdkafka configuration for `context`.
///
/// Insecure overrides are applied last so they win over the provider's TLS properties.
pub(crate) fn build_client_config(
    context: &ConnectionContext,
    resolver: &dyn SecretResolver,
    settings: &ConnectionSettings,
) -> CoreResult<rdkafka::ClientConfig> {
    validate_context(context, settings.insecure)?;
    let provider_properties =
        create_auth_provider(&context.auth).build_client_properties(resolver)?;

    let mut client_config = rdkafka::ClientConfig::new();
    client_config.set("bootstrap.servers", context.brokers.join(","));
    client_config.set("client.id", CLIENT_IDENTIFIER);
    provider_properties.apply_to(&mut client_config);
    client_config.set(
        "socket.connection.setup.timeout.ms",
        CONNECTION_SETUP_TIMEOUT_MILLIS,
    );
    if settings.insecure {
        tracing::warn!("TLS verification disabled by --insecure");
        client_config.set(
            "enable.ssl.certificate.verification",
            INSECURE_CERTIFICATE_VERIFICATION,
        );
        client_config.set(
            "ssl.endpoint.identification.algorithm",
            INSECURE_HOSTNAME_VERIFICATION,
        );
    }
    Ok(client_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{AuthConfig, TlsAuthConfig};
    use crate::error::CoreError;
    use crate::secret::fixed_resolver::FixedSecretResolver;

    fn plaintext_context(broker: &str) -> ConnectionContext {
        ConnectionContext {
            brokers: vec![broker.to_string()],
            auth: AuthConfig::Plaintext,
            protected: false,
            schema_registry: None,
        }
    }

    fn unprotected_tls_context() -> ConnectionContext {
        ConnectionContext {
            brokers: vec!["broker:9093".to_string()],
            auth: AuthConfig::Tls(TlsAuthConfig { ca_location: None }),
            protected: false,
            schema_registry: None,
        }
    }

    fn protected_tls_context() -> ConnectionContext {
        ConnectionContext {
            protected: true,
            ..unprotected_tls_context()
        }
    }

    #[test]
    fn joins_brokers_and_sets_client_identifier() {
        let context = ConnectionContext {
            brokers: vec!["a:9092".to_string(), "b:9092".to_string()],
            ..plaintext_context("unused:9092")
        };

        let client_config = build_client_config(
            &context,
            &FixedSecretResolver::returning("unused"),
            &ConnectionSettings { insecure: false },
        )
        .unwrap();

        assert_eq!(
            client_config.get("bootstrap.servers"),
            Some("a:9092,b:9092")
        );
        assert_eq!(client_config.get("client.id"), Some(CLIENT_IDENTIFIER));
    }

    #[test]
    fn insecure_disables_certificate_and_hostname_verification() {
        let client_config = build_client_config(
            &unprotected_tls_context(),
            &FixedSecretResolver::returning("unused"),
            &ConnectionSettings { insecure: true },
        )
        .unwrap();

        assert_eq!(
            client_config.get("enable.ssl.certificate.verification"),
            Some("false")
        );
        assert_eq!(
            client_config.get("ssl.endpoint.identification.algorithm"),
            Some("none")
        );
    }

    #[test]
    fn refuses_insecure_on_protected_context() {
        let result = build_client_config(
            &protected_tls_context(),
            &FixedSecretResolver::returning("unused"),
            &ConnectionSettings { insecure: true },
        );

        assert!(matches!(result, Err(CoreError::ContextValidation(_))));
    }
}
