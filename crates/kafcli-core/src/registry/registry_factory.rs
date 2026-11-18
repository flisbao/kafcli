use super::azure_registry::{AZURE_ACCEPT, AzureSchemaRegistry};
use super::confluent_registry::ConfluentSchemaRegistry;
use super::registry_http_client::RegistryHttpClient;
use super::schema_registry::SchemaRegistry;
use crate::auth::azure::azure_environment::ProcessAzureEnvironment;
use crate::auth::azure::azure_scope::SCHEMA_REGISTRY_SCOPE;
use crate::auth::azure::azure_token_source_factory::create_azure_token_source;
use crate::config::azure_credential_model::AzureCredentialConfig;
use crate::config::model::{AuthConfig, ConnectionContext};
use crate::config::schema_registry_model::{
    RegistryAuthConfig, RegistryAzureAuthConfig, SchemaRegistryConfig,
};
use crate::error::{CoreError, CoreResult};
use crate::secret::resolver::SecretResolver;
use std::sync::Arc;
use std::time::Duration;

/// Builds the registry client of the context's `schema_registry`, resolving its secrets now. An
/// `azure` registry without its own credential uses the broker's Azure credential.
pub fn build_schema_registry(
    context: &ConnectionContext,
    resolver: &dyn SecretResolver,
    timeout: Duration,
) -> CoreResult<Box<dyn SchemaRegistry>> {
    let Some(config) = context.schema_registry.as_ref() else {
        return Err(CoreError::Configuration(
            "context has no schema_registry".to_owned(),
        ));
    };
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
            let http = match &azure.auth {
                RegistryAuthConfig::Azure(auth) => {
                    let credential = resolve_registry_credential(auth, &context.auth)?;
                    let token_source = create_azure_token_source(
                        &credential,
                        SCHEMA_REGISTRY_SCOPE,
                        resolver,
                        Arc::new(ProcessAzureEnvironment),
                    )?;
                    RegistryHttpClient::new_with_token_source(
                        &azure.url,
                        token_source,
                        timeout,
                        AZURE_ACCEPT,
                    )?
                }
                _ => RegistryHttpClient::new_with_accept(
                    &azure.url,
                    &azure.auth,
                    None,
                    resolver,
                    timeout,
                    AZURE_ACCEPT,
                )?,
            };
            Ok(Box::new(AzureSchemaRegistry::new(
                http,
                azure.group.clone(),
            )))
        }
    }
}

/// The Azure credential of an `azure` registry: its own when present, otherwise the broker's, which
/// must be an `azure-*` auth other than `azure-connection-string`.
pub fn resolve_registry_credential(
    registry_auth: &RegistryAzureAuthConfig,
    broker_auth: &AuthConfig,
) -> CoreResult<AzureCredentialConfig> {
    if let Some(own) = &registry_auth.credential {
        return Ok(own.clone());
    }
    broker_auth.azure_credential().ok_or_else(|| {
        CoreError::ContextValidation(
            "schema registry auth 'azure' without a credential needs an azure-* broker auth other than azure-connection-string".to_owned(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::azure_credential_model::{
        AzureCliConfig, AzureServicePrincipalSecretConfig,
    };
    use crate::config::model::AzureConnectionStringConfig;
    use crate::config::schema_registry_model::{
        AzureRegistryConfig, ConfluentRegistryConfig, RegistryAuthConfig, RegistryAzureAuthConfig,
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

    fn context_with(auth: AuthConfig, registry: SchemaRegistryConfig) -> ConnectionContext {
        ConnectionContext {
            brokers: vec!["broker:9093".to_string()],
            auth,
            protected: false,
            schema_registry: Some(registry),
        }
    }

    fn azure_registry(url: String, auth: RegistryAuthConfig) -> SchemaRegistryConfig {
        SchemaRegistryConfig::Azure(AzureRegistryConfig {
            url,
            group: "pedidos".into(),
            auth,
        })
    }

    fn service_principal_secret(authority_host: String) -> AzureCredentialConfig {
        AzureCredentialConfig::ServicePrincipalSecret(AzureServicePrincipalSecretConfig {
            tenant_id: "tenant-1".into(),
            client_id: "app-1".into(),
            secret_ref: "env:PRD_SP_SECRET".parse().unwrap(),
            scope: None,
            authority_host: Some(authority_host),
        })
    }

    #[test]
    fn factory_builds_each_registry_type() {
        let server = FakeRegistryServer::start(vec![]).unwrap();
        let resolver = FixedSecretResolver::returning("unused");
        let confluent = context_with(
            AuthConfig::Plaintext,
            SchemaRegistryConfig::Confluent(ConfluentRegistryConfig {
                url: server.url(),
                auth: RegistryAuthConfig::Anonymous,
                ca_location: None,
            }),
        );
        let azure = context_with(
            AuthConfig::Plaintext,
            azure_registry(server.url(), RegistryAuthConfig::Anonymous),
        );

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
        let context = context_with(
            AuthConfig::Plaintext,
            azure_registry(server.url(), RegistryAuthConfig::Anonymous),
        );
        let registry = build_schema_registry(
            &context,
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

    #[test]
    fn registry_inherits_broker_credential() {
        let broker = AuthConfig::AzureServicePrincipalSecret(AzureServicePrincipalSecretConfig {
            tenant_id: "tenant-1".into(),
            client_id: "app-1".into(),
            secret_ref: "env:PRD_SP_SECRET".parse().unwrap(),
            scope: None,
            authority_host: None,
        });
        let registry_auth = RegistryAzureAuthConfig { credential: None };

        let credential = resolve_registry_credential(&registry_auth, &broker).unwrap();

        assert_eq!(
            credential,
            AzureCredentialConfig::ServicePrincipalSecret(AzureServicePrincipalSecretConfig {
                tenant_id: "tenant-1".into(),
                client_id: "app-1".into(),
                secret_ref: "env:PRD_SP_SECRET".parse().unwrap(),
                scope: None,
                authority_host: None,
            })
        );
    }

    #[test]
    fn own_registry_credential_wins_over_broker_credential() {
        let broker = AuthConfig::AzureCli(AzureCliConfig {
            tenant_id: None,
            scope: None,
        });
        let own = service_principal_secret("https://login.example".into());
        let registry_auth = RegistryAzureAuthConfig {
            credential: Some(own.clone()),
        };

        let credential = resolve_registry_credential(&registry_auth, &broker).unwrap();

        assert_eq!(credential, own);
    }

    #[test]
    fn connection_string_broker_cannot_be_inherited() {
        let broker = AuthConfig::AzureConnectionString(AzureConnectionStringConfig {
            secret_ref: "env:EH_CONNECTION".parse().unwrap(),
        });
        let registry_auth = RegistryAzureAuthConfig { credential: None };

        let error = resolve_registry_credential(&registry_auth, &broker)
            .err()
            .unwrap()
            .to_string();

        assert_eq!(
            error,
            "context validation failed: schema registry auth 'azure' without a credential needs an azure-* broker auth other than azure-connection-string"
        );
    }

    #[test]
    fn azure_registry_uses_entra_token_from_inherited_credential() {
        let server = FakeRegistryServer::start(vec![
            FakeRoute {
                method: "POST".into(),
                path_and_query: "/tenant-1/oauth2/v2.0/token".into(),
                status: 200,
                headers: Vec::new(),
                body: r#"{"access_token":"entra-token-1","expires_in":3600}"#.into(),
            },
            FakeRoute {
                method: "GET".into(),
                path_and_query: format!(
                    "/$schemaGroups/$schemas/{SCHEMA_ID}?api-version=2023-07-01"
                ),
                status: 200,
                headers: vec![(
                    "Content-Type".into(),
                    "application/json; serialization=Avro".into(),
                )],
                body: r#""string""#.into(),
            },
        ])
        .unwrap();
        let broker = AuthConfig::AzureServicePrincipalSecret(AzureServicePrincipalSecretConfig {
            tenant_id: "tenant-1".into(),
            client_id: "app-1".into(),
            secret_ref: "env:PRD_SP_SECRET".parse().unwrap(),
            scope: None,
            authority_host: Some(server.url()),
        });
        let context = context_with(
            broker,
            azure_registry(
                server.url(),
                RegistryAuthConfig::Azure(RegistryAzureAuthConfig { credential: None }),
            ),
        );
        let registry = build_schema_registry(
            &context,
            &FixedSecretResolver::returning("client-secret"),
            Duration::from_secs(5),
        )
        .unwrap();

        registry
            .fetch_schema_by_id(&SchemaId::Azure(SCHEMA_ID.into()))
            .unwrap();

        let requests = server.recorded_requests();
        assert_eq!(requests.len(), 2, "requests: {requests:?}");
        assert_eq!(
            header_of(&requests[1].headers, "authorization"),
            Some("Bearer entra-token-1")
        );
    }
}
