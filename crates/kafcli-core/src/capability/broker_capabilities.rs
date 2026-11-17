//! Capability table for the broker families kafcli talks to.
//!
//! The table follows `docs/adr/0001-event-hubs-capabilities.md`. Azure Event Hubs is treated as
//! Standard (the lowest tier with a Kafka endpoint), so compression stays off there.

use crate::config::model::{AuthConfig, ConnectionContext};
use serde::Serialize;

/// Host suffix of an Azure Event Hubs Kafka endpoint, compared case-insensitively.
const EVENT_HUBS_HOST_SUFFIX: &str = ".servicebus.windows.net";

/// The broker family a context points at. Selects which capability rules apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerFamily {
    GenericKafka,
    AzureEventHubs,
}

/// Whether a capability is confirmed for a broker family.
///
/// `Documented` comes from documentation or cited source code. `EmulatorOnly` was seen only on the
/// emulator, which is not evidence for the real service. `Unverified` has no evidence yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityStatus {
    Documented,
    EmulatorOnly,
    Unverified,
}

/// Producer settings that depend on the broker family, beyond the always-on ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerTuning {
    pub compression_disabled: bool,
}

/// Chooses the broker family from the context.
///
/// A context with the `azure-connection-string` auth type is Event Hubs. Otherwise any broker whose
/// host ends with `.servicebus.windows.net` (with or without a `:<port>` suffix) is Event Hubs.
pub fn detect_broker_family(context: &ConnectionContext) -> BrokerFamily {
    if matches!(context.auth, AuthConfig::AzureConnectionString(_)) {
        return BrokerFamily::AzureEventHubs;
    }
    let has_event_hubs_broker = context.brokers.iter().any(|broker| {
        broker_host(broker)
            .to_ascii_lowercase()
            .ends_with(EVENT_HUBS_HOST_SUFFIX)
    });
    if has_event_hubs_broker {
        BrokerFamily::AzureEventHubs
    } else {
        BrokerFamily::GenericKafka
    }
}

/// Producer settings for a broker family. Event Hubs (Standard) rejects compression.
pub fn resolve_producer_tuning(family: BrokerFamily) -> ProducerTuning {
    ProducerTuning {
        compression_disabled: family == BrokerFamily::AzureEventHubs,
    }
}

/// Whether idempotent produce is documented for a broker family.
pub fn idempotent_produce_status(family: BrokerFamily) -> CapabilityStatus {
    match family {
        BrokerFamily::GenericKafka | BrokerFamily::AzureEventHubs => CapabilityStatus::Documented,
    }
}

/// Returns the host part of a `host[:port]` broker address.
///
/// The `:port` suffix is removed only when it is all digits, so a bare host is returned unchanged.
fn broker_host(broker: &str) -> &str {
    match broker.rsplit_once(':') {
        Some((host, port))
            if !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            host
        }
        _ => broker,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{AzureConnectionStringConfig, TlsAuthConfig};
    use crate::secret::reference::SecretReference;

    fn context_with(brokers: &[&str], auth: AuthConfig) -> ConnectionContext {
        ConnectionContext {
            brokers: brokers.iter().map(|broker| (*broker).to_string()).collect(),
            auth,
            protected: false,
            schema_registry: None,
        }
    }

    fn azure_connection_string_auth() -> AuthConfig {
        AuthConfig::AzureConnectionString(AzureConnectionStringConfig {
            secret_ref: SecretReference::Environment {
                variable: "KAFCLI_TEST_CONNECTION_STRING".to_string(),
            },
        })
    }

    #[test]
    fn detects_event_hubs_by_auth_type() {
        let context = context_with(&["localhost:9092"], azure_connection_string_auth());

        assert_eq!(detect_broker_family(&context), BrokerFamily::AzureEventHubs);
    }

    #[test]
    fn detects_event_hubs_by_host_suffix_case_insensitive() {
        let upper = context_with(
            &["MyNamespace.ServiceBus.Windows.NET:9093"],
            AuthConfig::Tls(TlsAuthConfig { ca_location: None }),
        );
        let without_port = context_with(
            &["my-namespace.servicebus.windows.net"],
            AuthConfig::Plaintext,
        );

        assert_eq!(detect_broker_family(&upper), BrokerFamily::AzureEventHubs);
        assert_eq!(
            detect_broker_family(&without_port),
            BrokerFamily::AzureEventHubs
        );
    }

    #[test]
    fn generic_kafka_for_other_hosts() {
        let local = context_with(&["localhost:9092", "broker-1:9093"], AuthConfig::Plaintext);
        let lookalike = context_with(
            &["servicebus.windows.net.example.com:9093"],
            AuthConfig::Plaintext,
        );

        assert_eq!(detect_broker_family(&local), BrokerFamily::GenericKafka);
        assert_eq!(detect_broker_family(&lookalike), BrokerFamily::GenericKafka);
    }

    #[test]
    fn event_hubs_disables_compression() {
        assert!(resolve_producer_tuning(BrokerFamily::AzureEventHubs).compression_disabled);
        assert!(!resolve_producer_tuning(BrokerFamily::GenericKafka).compression_disabled);
    }

    #[test]
    fn idempotent_produce_is_documented_for_both_families() {
        assert_eq!(
            idempotent_produce_status(BrokerFamily::GenericKafka),
            CapabilityStatus::Documented
        );
        assert_eq!(
            idempotent_produce_status(BrokerFamily::AzureEventHubs),
            CapabilityStatus::Documented
        );
    }
}
