use crate::auth::provider_factory::create_auth_provider;
use crate::config::model::ConnectionContext;
use crate::connection::client_config_builder::ConnectionSettings;
use crate::connection::kafka_connection::{KafkaConnection, build_kafka_connection};
use crate::error::{CoreError, CoreResult};
use crate::secret::resolver::SecretResolver;
use rdkafka::consumer::Consumer;
use serde::Serialize;
use std::time::Duration;

#[derive(Debug, Serialize)]
pub struct ConnectionReport {
    pub context_name: String,
    pub auth_type: String,
    pub principal: Option<String>,
    /// Principal of the last token the connection served (`upn`, `appid`, `azp`, `oid` or `sub`).
    /// `None` for modes without a kafcli token source, such as `oauth-oidc`.
    pub authenticated_principal: Option<String>,
    pub broker_count: usize,
    pub visible_topics: Vec<String>,
}

/// Connects once to the context's brokers and reads cluster metadata.
///
/// The consumer is created without a group and dropped when this function returns.
pub fn probe_context_connection(
    context_name: &str,
    context: &ConnectionContext,
    resolver: &dyn SecretResolver,
    settings: &ConnectionSettings,
    timeout: Duration,
) -> CoreResult<ConnectionReport> {
    let connection = build_kafka_connection(context, resolver, settings)?;
    let consumer = connection.create_consumer(&connection.client_config().clone())?;
    let metadata = consumer
        .fetch_metadata(None, timeout)
        .map_err(map_kafka_error)?;

    let visible_topics: Vec<String> = metadata
        .topics()
        .iter()
        .map(|topic| topic.name().to_string())
        .collect();

    Ok(assemble_connection_report(
        context_name,
        context,
        &connection,
        metadata.brokers().len(),
        visible_topics,
    ))
}

/// Builds the report from the context, the connection that served the tokens and the metadata.
fn assemble_connection_report(
    context_name: &str,
    context: &ConnectionContext,
    connection: &KafkaConnection,
    broker_count: usize,
    mut visible_topics: Vec<String>,
) -> ConnectionReport {
    visible_topics.sort();
    ConnectionReport {
        context_name: context_name.to_string(),
        auth_type: context.auth.type_name().to_string(),
        principal: create_auth_provider(&context.auth).describe_principal(),
        authenticated_principal: connection.refresher().last_principal(),
        broker_count,
        visible_topics,
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "map adapter: map_err and Iterator::map pass the value"
)]
fn map_kafka_error(error: rdkafka::error::KafkaError) -> CoreError {
    CoreError::Kafka(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use std::time::Instant;

    fn plaintext_context(broker: &str) -> ConnectionContext {
        serde_json::from_value(serde_json::json!({
            "brokers": [broker],
            "auth": {"type": "none"},
        }))
        .unwrap()
    }

    fn entra_context(authority_host: &str) -> ConnectionContext {
        serde_json::from_value(serde_json::json!({
            "brokers": ["127.0.0.1:1"],
            "auth": {
                "type": "azure-sp-secret",
                "tenant_id": "tenant-1",
                "client_id": "app-123",
                "secret_ref": "env:UNUSED_SECRET",
                "scope": "https://eventhubs.example/.default",
                "authority_host": authority_host,
            },
        }))
        .unwrap()
    }

    /// Unsecured JWT whose claims are `claims_json`, the shape of an Entra access token.
    fn jwt_with_claims(claims_json: &str) -> String {
        format!(
            "{}.{}.signature",
            URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#),
            URL_SAFE_NO_PAD.encode(claims_json)
        )
    }

    #[test]
    fn unreachable_broker_fails_within_timeout() {
        let started = Instant::now();

        let result = probe_context_connection(
            "local",
            &plaintext_context("127.0.0.1:1"),
            &FixedSecretResolver::returning("unused"),
            &ConnectionSettings { insecure: false },
            Duration::from_secs(2),
        );

        assert!(matches!(result, Err(CoreError::Kafka(_))));
        assert!(started.elapsed() < Duration::from_secs(6));
    }

    #[test]
    fn report_carries_principal_of_token_served_to_connection() {
        let token = jwt_with_claims(r#"{"appid":"app-123","sub":"ignored-subject"}"#);
        let server = FakeRegistryServer::start(vec![FakeRoute {
            method: "POST".to_string(),
            path_and_query: "/tenant-1/oauth2/v2.0/token".to_string(),
            status: 200,
            headers: Vec::new(),
            body: format!(r#"{{"access_token":"{token}","expires_in":3600}}"#),
        }])
        .unwrap();
        let context = entra_context(&server.url());
        let connection = build_kafka_connection(
            &context,
            &FixedSecretResolver::returning("client-secret"),
            &ConnectionSettings { insecure: false },
        )
        .unwrap();
        // The consumer's first poll asks for a token; the connection then reports its principal.
        connection
            .create_consumer(&connection.client_config().clone())
            .unwrap();

        let report = assemble_connection_report("dev", &context, &connection, 0, Vec::new());

        assert_eq!(report.authenticated_principal.as_deref(), Some("app-123"));
        assert_eq!(report.principal.as_deref(), Some("app-123"));
    }

    #[test]
    fn report_has_no_authenticated_principal_without_token_source() {
        let context = plaintext_context("127.0.0.1:1");
        let connection = build_kafka_connection(
            &context,
            &FixedSecretResolver::returning("unused"),
            &ConnectionSettings { insecure: false },
        )
        .unwrap();

        let report = assemble_connection_report("local", &context, &connection, 1, Vec::new());

        assert_eq!(report.authenticated_principal, None);
    }
}
