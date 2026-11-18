use super::azure_environment::ProcessAzureEnvironment;
use super::azure_scope::resolve_event_hubs_scope;
use super::azure_token_source_factory::create_azure_token_source;
use crate::auth::client_properties::ClientProperties;
use crate::auth::oauth_bearer_settings::apply_oauth_bearer_settings;
use crate::auth::provider::AuthProvider;
use crate::auth::token::token_source::TokenSource;
use crate::config::azure_credential_model::AzureCredentialConfig;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;
use std::sync::Arc;

/// OAUTHBEARER with a token that kafcli fetches from Entra ID and serves to librdkafka.
#[derive(Debug)]
pub struct AzureOAuthAuthProvider {
    credential: AzureCredentialConfig,
}

impl AzureOAuthAuthProvider {
    pub fn new(credential: AzureCredentialConfig) -> Self {
        Self { credential }
    }
}

impl AuthProvider for AzureOAuthAuthProvider {
    fn build_client_properties(
        &self,
        _resolver: &dyn SecretResolver,
    ) -> CoreResult<ClientProperties> {
        // Azure endpoints always use TLS with the public CA store. The token is served through the
        // client context, so it is not a property.
        let mut properties = ClientProperties::default();
        apply_oauth_bearer_settings(&mut properties, true, None);
        Ok(properties)
    }

    fn describe_principal(&self) -> Option<String> {
        match &self.credential {
            AzureCredentialConfig::ServicePrincipalSecret(config) => Some(config.client_id.clone()),
        }
    }

    fn create_token_source(
        &self,
        brokers: &[String],
        resolver: &dyn SecretResolver,
    ) -> CoreResult<Option<Arc<dyn TokenSource>>> {
        let scope = resolve_event_hubs_scope(self.credential.scope(), brokers)?;
        let source = create_azure_token_source(
            &self.credential,
            &scope,
            resolver,
            Arc::new(ProcessAzureEnvironment),
        )?;
        Ok(Some(source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::azure_credential_model::AzureServicePrincipalSecretConfig;
    use crate::secret::fixed_resolver::FixedSecretResolver;

    fn provider() -> AzureOAuthAuthProvider {
        AzureOAuthAuthProvider::new(AzureCredentialConfig::ServicePrincipalSecret(
            AzureServicePrincipalSecretConfig {
                tenant_id: "tenant-1".to_string(),
                client_id: "app-1".to_string(),
                secret_ref: "env:PRD_SP_SECRET".parse().unwrap(),
                scope: None,
                authority_host: None,
            },
        ))
    }

    #[test]
    fn sets_oauthbearer_over_tls_without_token_property() {
        let properties = provider()
            .build_client_properties(&FixedSecretResolver::returning("unused"))
            .unwrap();

        assert_eq!(
            properties.plain_value("security.protocol"),
            Some("SASL_SSL")
        );
        assert_eq!(
            properties.plain_value("sasl.mechanism"),
            Some("OAUTHBEARER")
        );
        assert_eq!(properties.plain_value("sasl.oauthbearer.method"), None);
    }

    #[test]
    fn principal_is_client_id() {
        assert_eq!(provider().describe_principal().as_deref(), Some("app-1"));
    }

    #[test]
    fn token_source_uses_event_hubs_scope_from_broker() {
        let source = provider()
            .create_token_source(
                &["prd-ns.servicebus.windows.net:9093".to_string()],
                &FixedSecretResolver::returning("client-secret-value"),
            )
            .unwrap();

        let source = source.unwrap();
        assert_eq!(source.describe_source(), "azure-sp-secret");
    }

    #[test]
    fn token_source_needs_scope_for_other_brokers() {
        let result = provider().create_token_source(
            &["kafka.internal:9093".to_string()],
            &FixedSecretResolver::returning("client-secret-value"),
        );

        assert!(matches!(
            result,
            Err(crate::error::CoreError::Authentication(_))
        ));
    }
}
