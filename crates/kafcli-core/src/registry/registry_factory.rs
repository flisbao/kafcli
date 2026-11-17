use super::azure_registry::{AZURE_ACCEPT, AzureSchemaRegistry};
use super::confluent_registry::ConfluentSchemaRegistry;
use super::registry_http_client::RegistryHttpClient;
use super::schema_registry::SchemaRegistry;
use crate::config::schema_registry_model::SchemaRegistryConfig;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;
use std::time::Duration;

/// Builds the registry client that matches the context's `type`, resolving its secrets now.
pub fn build_schema_registry(
    config: &SchemaRegistryConfig,
    resolver: &dyn SecretResolver,
    timeout: Duration,
) -> CoreResult<Box<dyn SchemaRegistry>> {
    match config {
        SchemaRegistryConfig::Confluent(confluent) => {
            let http = RegistryHttpClient::new(
                &confluent.url,
                &confluent.auth,
                confluent.ca_location.as_deref(),
                resolver,
                timeout,
            )?;
            Ok(Box::new(ConfluentSchemaRegistry::new(http)))
        }
        SchemaRegistryConfig::Azure(azure) => {
            let http = RegistryHttpClient::new_with_accept(
                &azure.url,
                &azure.auth,
                None,
                resolver,
                timeout,
                AZURE_ACCEPT,
            )?;
            Ok(Box::new(AzureSchemaRegistry::new(
                http,
                azure.group.clone(),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::schema_registry_model::{
        AzureRegistryConfig, ConfluentRegistryConfig, RegistryAuthConfig,
    };
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
    use crate::registry::registered_schema::SchemaId;
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use std::time::Duration;

    const SCHEMA_ID: &str = "0f1e2d3c4b5a69788796a5b4c3d2e1f0";

    fn header_of<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn factory_builds_each_registry_type() {
        let server = FakeRegistryServer::start(vec![]).unwrap();
        let resolver = FixedSecretResolver::returning("unused");
        let confluent = SchemaRegistryConfig::Confluent(ConfluentRegistryConfig {
            url: server.url(),
            auth: RegistryAuthConfig::Anonymous,
            ca_location: None,
        });
        let azure = SchemaRegistryConfig::Azure(AzureRegistryConfig {
            url: server.url(),
            group: "pedidos".into(),
            auth: RegistryAuthConfig::Anonymous,
        });

        let confluent_registry =
            build_schema_registry(&confluent, &resolver, Duration::from_secs(5)).unwrap();
        let azure_registry =
            build_schema_registry(&azure, &resolver, Duration::from_secs(5)).unwrap();

        assert_eq!(confluent_registry.registry_type(), "confluent");
        assert_eq!(azure_registry.registry_type(), "azure");
    }

    #[test]
    fn azure_factory_sends_json_accept_header() {
        let server = FakeRegistryServer::start(vec![FakeRoute {
            method: "GET".into(),
            path_and_query: format!("/$schemaGroups/$schemas/{SCHEMA_ID}?api-version=2023-07-01"),
            status: 200,
            headers: vec![(
                "Content-Type".into(),
                "application/json; serialization=Avro".into(),
            )],
            body: r#""string""#.into(),
        }])
        .unwrap();
        let azure = SchemaRegistryConfig::Azure(AzureRegistryConfig {
            url: server.url(),
            group: "pedidos".into(),
            auth: RegistryAuthConfig::Anonymous,
        });
        let registry = build_schema_registry(
            &azure,
            &FixedSecretResolver::returning("unused"),
            Duration::from_secs(5),
        )
        .unwrap();

        registry
            .fetch_schema_by_id(&SchemaId::Azure(SCHEMA_ID.into()))
            .unwrap();

        let requests = server.recorded_requests();
        assert_eq!(
            header_of(&requests[0].headers, "accept"),
            Some("application/json")
        );
    }
}
