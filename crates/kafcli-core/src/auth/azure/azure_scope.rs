use crate::error::{CoreError, CoreResult};

/// Scope of the Schema Registry of Event Hubs. Fixed by Azure.
pub const SCHEMA_REGISTRY_SCOPE: &str = "https://eventhubs.azure.net/.default";

const EVENT_HUBS_HOST_SUFFIX: &str = ".servicebus.windows.net";

/// Chooses the scope of the Event Hubs broker token: the configured scope when present, otherwise
/// the namespace of the first broker when it is `*.servicebus.windows.net`.
pub fn resolve_event_hubs_scope(
    configured: Option<&str>,
    brokers: &[String],
) -> CoreResult<String> {
    if let Some(scope) = configured.map(str::trim).filter(|scope| !scope.is_empty()) {
        return Ok(scope.to_owned());
    }
    let host = brokers
        .first()
        .map(|broker| broker_host(broker).to_ascii_lowercase())
        .unwrap_or_default();
    if host.ends_with(EVENT_HUBS_HOST_SUFFIX) {
        return Ok(format!("https://{host}/.default"));
    }
    Err(CoreError::Authentication(
        "scope is required for azure auth when brokers are not *.servicebus.windows.net".to_owned(),
    ))
}

fn broker_host(broker: &str) -> &str {
    let trimmed = broker.trim();
    trimmed
        .rsplit_once(':')
        .map_or(trimmed, |(host, _port)| host)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn brokers(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn derives_scope_from_event_hubs_broker() {
        let scope =
            resolve_event_hubs_scope(None, &brokers(&["Prd-NS.servicebus.windows.net:9093"]))
                .unwrap();

        assert_eq!(scope, "https://prd-ns.servicebus.windows.net/.default");
    }

    #[test]
    fn explicit_scope_wins() {
        let scope = resolve_event_hubs_scope(
            Some("api://custom/.default"),
            &brokers(&["prd-ns.servicebus.windows.net:9093"]),
        )
        .unwrap();

        assert_eq!(scope, "api://custom/.default");
    }

    #[test]
    fn non_event_hubs_broker_without_scope_is_error() {
        let error = resolve_event_hubs_scope(None, &brokers(&["kafka.internal:9093"])).unwrap_err();

        assert!(matches!(
            &error,
            CoreError::Authentication(message)
                if message == "scope is required for azure auth when brokers are not *.servicebus.windows.net"
        ));
    }
}
