use crate::auth::provider_factory::create_auth_provider;
use crate::config::model::ConnectionContext;
use crate::connection::client_config_builder::{ConnectionSettings, build_client_config};
use crate::error::{CoreError, CoreResult};
use crate::secret::resolver::SecretResolver;
use rdkafka::consumer::{BaseConsumer, Consumer};
use serde::Serialize;
use std::time::Duration;

#[derive(Debug, Serialize)]
pub struct ConnectionReport {
    /// Serialized as `profile_name` so the CLI JSON output stays unchanged until the CLI rename.
    #[serde(rename = "profile_name")]
    pub context_name: String,
    pub auth_type: String,
    pub principal: Option<String>,
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
    let client_config = build_client_config(context, resolver, settings)?;
    let consumer: BaseConsumer = client_config.create().map_err(map_kafka_error)?;
    let metadata = consumer
        .fetch_metadata(None, timeout)
        .map_err(map_kafka_error)?;

    let mut visible_topics: Vec<String> = metadata
        .topics()
        .iter()
        .map(|topic| topic.name().to_string())
        .collect();
    visible_topics.sort();

    Ok(ConnectionReport {
        context_name: context_name.to_string(),
        auth_type: context.auth.type_name().to_string(),
        principal: create_auth_provider(&context.auth).describe_principal(),
        broker_count: metadata.brokers().len(),
        visible_topics,
    })
}

fn map_kafka_error(error: rdkafka::error::KafkaError) -> CoreError {
    CoreError::Kafka(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::AuthConfig;
    use crate::connection::client_config_builder::ConnectionSettings;

    use crate::secret::fixed_resolver::FixedSecretResolver;
    use std::time::Instant;

    fn plaintext_context(broker: &str) -> ConnectionContext {
        ConnectionContext {
            brokers: vec![broker.to_string()],
            auth: AuthConfig::Plaintext,
            protected: false,
            schema_registry: None,
        }
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
}
