use crate::config::azure_credential_model::AzureCredentialConfig;
use crate::config::model::{AuthConfig, ConnectionContext, OAuthOidcAuthConfig};
use crate::config::schema_registry_model::{
    AzureRegistryConfig, RegistryAuthConfig, SchemaRegistryConfig,
};
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
    if let Some(schema_registry) = &context.schema_registry {
        validate_schema_registry(schema_registry, context.protected)?;
    }
    if let AuthConfig::OAuthOidc(oidc) = &context.auth {
        validate_token_endpoint(oidc, context.protected)?;
    }
    if let Some(credential) = context.auth.azure_credential() {
        validate_azure_credential(&credential, context.protected)?;
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
    if uses_plaintext_transport(&context.auth) {
        return Err(build_validation_error(
            "use_tls = false is not allowed on a protected context",
        ));
    }
    Ok(())
}

/// Checks the Schema Registry of a context. `protected` adds the production rules: HTTPS and authentication.
pub fn validate_schema_registry(config: &SchemaRegistryConfig, protected: bool) -> CoreResult<()> {
    validate_registry_url(config.url())?;
    if let SchemaRegistryConfig::Azure(azure) = config {
        validate_azure_registry(azure)?;
    }
    if protected {
        validate_protected_registry(config)?;
    }
    Ok(())
}

fn validate_registry_url(url: &str) -> CoreResult<()> {
    if !is_http_url(url) {
        return Err(build_validation_error(
            "schema registry url must start with http:// or https://",
        ));
    }
    if url_authority(url).contains('@') {
        return Err(build_validation_error(
            "schema registry url must not contain credentials; use auth.secret_ref",
        ));
    }
    if url.contains('?') || url.contains('#') {
        return Err(build_validation_error(
            "schema registry url must not contain a query or fragment",
        ));
    }
    Ok(())
}

/// The token endpoint must be HTTPS. Plain HTTP is accepted only where the context is not protected.
fn validate_token_endpoint(config: &OAuthOidcAuthConfig, protected: bool) -> CoreResult<()> {
    let url = config.token_endpoint_url.as_str();
    if url_authority(url).contains('@') {
        return Err(build_validation_error(
            "token_endpoint_url must not contain credentials",
        ));
    }
    if is_https_url(url) {
        return Ok(());
    }
    if !is_plain_http_url(url) {
        return Err(build_validation_error(
            "token_endpoint_url must start with https:// or http://",
        ));
    }
    if protected {
        return Err(build_validation_error(
            "token_endpoint_url over http:// is not allowed on a protected context",
        ));
    }
    Ok(())
}

/// Checks the Entra ID identifiers and the authority host of an Azure credential.
fn validate_azure_credential(
    credential: &AzureCredentialConfig,
    protected: bool,
) -> CoreResult<()> {
    match credential {
        AzureCredentialConfig::ServicePrincipalSecret(config) => validate_azure_identifiers(
            &config.tenant_id,
            &config.client_id,
            config.authority_host.as_deref(),
            protected,
        ),
        AzureCredentialConfig::ServicePrincipalCertificate(config) => validate_azure_identifiers(
            &config.tenant_id,
            &config.client_id,
            config.authority_host.as_deref(),
            protected,
        ),
        AzureCredentialConfig::WorkloadIdentity(config) => {
            if let Some(tenant_id) = &config.tenant_id {
                validate_azure_tenant_id(tenant_id)?;
            }
            if let Some(client_id) = &config.client_id {
                validate_azure_client_id(client_id)?;
            }
            validate_authority_host(config.authority_host.as_deref(), protected)
        }
        AzureCredentialConfig::ManagedIdentity(config) => match &config.client_id {
            Some(client_id) => validate_azure_client_id(client_id),
            None => Ok(()),
        },
        AzureCredentialConfig::AzureCli(config) => match &config.tenant_id {
            Some(tenant_id) => validate_azure_tenant_id(tenant_id),
            None => Ok(()),
        },
        AzureCredentialConfig::Default(config) => {
            if let Some(tenant_id) = &config.tenant_id {
                validate_azure_tenant_id(tenant_id)?;
            }
            if let Some(client_id) = &config.client_id {
                validate_azure_client_id(client_id)?;
            }
            Ok(())
        }
    }
}

fn validate_azure_identifiers(
    tenant_id: &str,
    client_id: &str,
    authority_host: Option<&str>,
    protected: bool,
) -> CoreResult<()> {
    validate_azure_tenant_id(tenant_id)?;
    validate_azure_client_id(client_id)?;
    validate_authority_host(authority_host, protected)
}

fn validate_azure_client_id(client_id: &str) -> CoreResult<()> {
    if client_id.trim().is_empty() {
        return Err(build_validation_error("azure client_id must not be empty"));
    }
    Ok(())
}

fn validate_azure_tenant_id(tenant_id: &str) -> CoreResult<()> {
    if tenant_id.trim().is_empty() {
        return Err(build_validation_error("azure tenant_id must not be empty"));
    }
    let only_allowed = tenant_id
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '.' || character == '-');
    if !only_allowed {
        return Err(build_validation_error(
            "azure tenant_id may contain only letters, digits, '.' and '-'",
        ));
    }
    Ok(())
}

/// The authority must be HTTPS and must not carry credentials. Plain HTTP is accepted only for
/// loopback hosts on an unprotected context. Used for the context setting; the environment variable
/// has its own rule, see `validate_environment_authority_host`.
pub fn validate_authority_host(authority_host: Option<&str>, protected: bool) -> CoreResult<()> {
    let Some(url) = authority_host else {
        return Ok(());
    };
    if url_authority(url).contains('@') {
        return Err(build_validation_error(
            "authority_host must not contain credentials",
        ));
    }
    if is_https_url(url) {
        return Ok(());
    }
    if !is_plain_http_url(url) {
        return Err(build_validation_error(
            "authority_host must start with https://",
        ));
    }
    if protected {
        return Err(build_validation_error(
            "authority_host over http:// is not allowed on a protected context",
        ));
    }
    if !matches!(url_host(url), "127.0.0.1" | "localhost") {
        return Err(build_validation_error(
            "authority_host over http:// is only allowed for 127.0.0.1 or localhost",
        ));
    }
    Ok(())
}

/// Environment authority (`AZURE_AUTHORITY_HOST`): always HTTPS, never with credentials, and no
/// plain-HTTP exception, because the value is not part of the context.
pub fn validate_environment_authority_host(authority_host: &str) -> CoreResult<()> {
    if !is_https_url(authority_host) || url_authority(authority_host).contains('@') {
        return Err(build_validation_error(
            "AZURE_AUTHORITY_HOST must be an https:// URL without credentials",
        ));
    }
    Ok(())
}

/// Authority part of an `http://` or `https://` URL: host, optional port and optional `user:password@`.
fn url_authority(url: &str) -> &str {
    let after_scheme = url.split_once("://").map_or("", |(_, rest)| rest);
    after_scheme
        .split_once('/')
        .map_or(after_scheme, |(authority, _)| authority)
}

/// Host part of an `http://` or `https://` URL, without port or path.
fn url_host(url: &str) -> &str {
    let authority = url_authority(url);
    authority
        .split_once(':')
        .map_or(authority, |(host, _)| host)
}

fn validate_azure_registry(azure: &AzureRegistryConfig) -> CoreResult<()> {
    if azure.group.trim().is_empty() {
        return Err(build_validation_error(
            "azure schema registry needs a group",
        ));
    }
    if matches!(azure.auth, RegistryAuthConfig::MutualTls(_)) {
        return Err(build_validation_error(
            "auth type 'mtls' is only supported by the confluent schema registry",
        ));
    }
    if !matches!(azure.auth, RegistryAuthConfig::Bearer(_)) {
        return Err(build_validation_error(
            "azure schema registry needs auth type 'bearer' with a token_ref",
        ));
    }
    Ok(())
}

fn validate_protected_registry(config: &SchemaRegistryConfig) -> CoreResult<()> {
    if is_plain_http_url(config.url()) {
        return Err(build_validation_error(
            "schema registry over http:// is not allowed on a protected context",
        ));
    }
    if matches!(config.auth(), RegistryAuthConfig::Anonymous) {
        return Err(build_validation_error(
            "schema registry auth type 'none' is not allowed on a protected context",
        ));
    }
    Ok(())
}

fn is_http_url(url: &str) -> bool {
    is_plain_http_url(url) || is_https_url(url)
}

fn is_https_url(url: &str) -> bool {
    url.get(..8)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
}

fn is_plain_http_url(url: &str) -> bool {
    url.get(..7)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("http://"))
}

fn build_validation_error(message: &str) -> CoreError {
    CoreError::ContextValidation(message.to_string())
}

/// True when the mode lets the user turn TLS off (`use_tls = false`).
fn uses_plaintext_transport(auth: &AuthConfig) -> bool {
    match auth {
        AuthConfig::SaslPlain(credentials)
        | AuthConfig::SaslScram256(credentials)
        | AuthConfig::SaslScram512(credentials) => !credentials.use_tls,
        AuthConfig::Bearer(bearer) => !bearer.use_tls,
        AuthConfig::OAuthOidc(oidc) => !oidc.use_tls,
        AuthConfig::Plaintext
        | AuthConfig::Tls(_)
        | AuthConfig::MutualTls(_)
        | AuthConfig::AzureConnectionString(_)
        | AuthConfig::AzureServicePrincipalSecret(_)
        | AuthConfig::AzureServicePrincipalCertificate(_)
        | AuthConfig::AzureWorkloadIdentity(_)
        | AuthConfig::AzureManagedIdentity(_)
        | AuthConfig::AzureCli(_)
        | AuthConfig::AzureDefault(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::azure_credential_model::AzureServicePrincipalSecretConfig;
    use crate::config::model::{
        BearerAuthConfig, OAuthOidcAuthConfig, SaslCredentialsConfig, TlsAuthConfig,
    };
    use crate::config::schema_registry_model::{
        ConfluentRegistryConfig, RegistryBasicAuthConfig, RegistryBearerAuthConfig,
        RegistryMutualTlsConfig,
    };
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
    fn protected_bearer_without_tls_is_refused() {
        let bearer = context(
            AuthConfig::Bearer(BearerAuthConfig {
                token_ref: "env:KAFCLI_IT_BEARER_TOKEN"
                    .parse::<SecretReference>()
                    .unwrap(),
                ca_location: None,
                use_tls: false,
            }),
            true,
        );

        let message = validation_message(validate_context(&bearer, false));

        assert_eq!(
            message,
            "use_tls = false is not allowed on a protected context"
        );
    }

    #[test]
    fn accepts_tls_context_marked_protected() {
        let protected_tls = context(AuthConfig::Tls(TlsAuthConfig { ca_location: None }), true);

        assert!(validate_context(&protected_tls, false).is_ok());
    }

    fn bearer_auth() -> RegistryAuthConfig {
        RegistryAuthConfig::Bearer(RegistryBearerAuthConfig {
            token_ref: "env:SR_TOKEN".parse::<SecretReference>().unwrap(),
        })
    }

    fn confluent_registry(url: &str, auth: RegistryAuthConfig) -> SchemaRegistryConfig {
        SchemaRegistryConfig::Confluent(ConfluentRegistryConfig {
            url: url.to_string(),
            auth,
            ca_location: None,
        })
    }

    fn azure_registry(group: &str, auth: RegistryAuthConfig) -> SchemaRegistryConfig {
        SchemaRegistryConfig::Azure(AzureRegistryConfig {
            url: "https://ns.servicebus.windows.net".to_string(),
            group: group.to_string(),
            auth,
        })
    }

    #[test]
    fn rejects_url_with_credentials() {
        let registry = confluent_registry("https://user:pass@sr:8081", bearer_auth());

        let message = validation_message(validate_schema_registry(&registry, false));

        assert_eq!(
            message,
            "schema registry url must not contain credentials; use auth.secret_ref"
        );
    }

    #[test]
    fn rejects_url_with_query() {
        let registry = confluent_registry("https://sr:8081/subjects?x=1", bearer_auth());

        let message = validation_message(validate_schema_registry(&registry, false));

        assert_eq!(
            message,
            "schema registry url must not contain a query or fragment"
        );
    }

    #[test]
    fn rejects_azure_without_group() {
        let registry = azure_registry("   ", bearer_auth());

        let message = validation_message(validate_schema_registry(&registry, false));

        assert_eq!(message, "azure schema registry needs a group");
    }

    #[test]
    fn rejects_azure_without_bearer() {
        let registry = azure_registry("pedidos", RegistryAuthConfig::Anonymous);

        let message = validation_message(validate_schema_registry(&registry, false));

        assert_eq!(
            message,
            "azure schema registry needs auth type 'bearer' with a token_ref"
        );
    }

    #[test]
    fn rejects_mtls_on_azure() {
        let registry = azure_registry(
            "pedidos",
            RegistryAuthConfig::MutualTls(RegistryMutualTlsConfig {
                certificate_location: "/c.pem".into(),
                key_location: "/k.pem".into(),
            }),
        );

        let message = validation_message(validate_schema_registry(&registry, false));

        assert_eq!(
            message,
            "auth type 'mtls' is only supported by the confluent schema registry"
        );
    }

    #[test]
    fn protected_rejects_http_registry() {
        let registry = confluent_registry(
            "http://sr:8081",
            RegistryAuthConfig::Basic(RegistryBasicAuthConfig {
                username: "app".to_string(),
                secret_ref: "env:SR_PASSWORD".parse::<SecretReference>().unwrap(),
            }),
        );

        let message = validation_message(validate_schema_registry(&registry, true));

        assert_eq!(
            message,
            "schema registry over http:// is not allowed on a protected context"
        );
    }

    #[test]
    fn protected_rejects_anonymous_registry() {
        let registry = confluent_registry("https://sr:8081", RegistryAuthConfig::Anonymous);

        let message = validation_message(validate_schema_registry(&registry, true));

        assert_eq!(
            message,
            "schema registry auth type 'none' is not allowed on a protected context"
        );
    }

    #[test]
    fn unprotected_accepts_http_confluent_without_auth() {
        let registry = confluent_registry("http://sr:8081", RegistryAuthConfig::Anonymous);

        assert!(validate_schema_registry(&registry, false).is_ok());
    }

    fn oidc_context(token_endpoint_url: &str, protected: bool) -> ConnectionContext {
        context(
            AuthConfig::OAuthOidc(OAuthOidcAuthConfig {
                token_endpoint_url: token_endpoint_url.to_string(),
                client_id: "kafcli".to_string(),
                secret_ref: "env:KAFCLI_IT_OIDC_SECRET"
                    .parse::<SecretReference>()
                    .unwrap(),
                scope: None,
                ca_location: None,
                use_tls: true,
            }),
            protected,
        )
    }

    #[test]
    fn protected_oidc_requires_https_endpoint() {
        let message = validation_message(validate_context(
            &oidc_context("http://idp.example/token", true),
            false,
        ));

        assert_eq!(
            message,
            "token_endpoint_url over http:// is not allowed on a protected context"
        );
        assert!(validate_context(&oidc_context("https://idp.example/token", true), false).is_ok());
    }

    #[test]
    fn unprotected_oidc_accepts_http_endpoint() {
        assert!(
            validate_context(&oidc_context("http://127.0.0.1:8080/token", false), false).is_ok()
        );
    }

    #[test]
    fn oidc_endpoint_must_use_http_or_https() {
        let message = validation_message(validate_context(
            &oidc_context("idp.example/token", false),
            false,
        ));

        assert_eq!(
            message,
            "token_endpoint_url must start with https:// or http://"
        );
    }

    #[test]
    fn endpoint_with_credentials_is_refused() {
        for url in [
            "https://kafcli:s3cret@idp.example/token",
            "http://kafcli:s3cret@127.0.0.1:8080/token",
        ] {
            let message = validation_message(validate_context(&oidc_context(url, false), false));

            assert_eq!(
                message, "token_endpoint_url must not contain credentials",
                "url {url}"
            );
        }
    }

    fn azure_context(authority_host: Option<&str>, protected: bool) -> ConnectionContext {
        context(
            AuthConfig::AzureServicePrincipalSecret(AzureServicePrincipalSecretConfig {
                tenant_id: "00000000-0000-0000-0000-000000000000".to_string(),
                client_id: "app".to_string(),
                secret_ref: "env:PRD_SP_SECRET".parse::<SecretReference>().unwrap(),
                scope: None,
                authority_host: authority_host.map(str::to_string),
            }),
            protected,
        )
    }

    #[test]
    fn http_authority_only_on_loopback_and_unprotected() {
        assert!(validate_context(&azure_context(Some("http://127.0.0.1:1"), false), false).is_ok());
        let message = validation_message(validate_context(
            &azure_context(Some("http://127.0.0.1:1"), true),
            false,
        ));
        assert_eq!(
            message,
            "authority_host over http:// is not allowed on a protected context"
        );
        let message = validation_message(validate_context(
            &azure_context(Some("http://example.com"), false),
            false,
        ));
        assert_eq!(
            message,
            "authority_host over http:// is only allowed for 127.0.0.1 or localhost"
        );
    }

    #[test]
    fn azure_tenant_and_client_are_checked() {
        let mut blank_client = azure_context(None, false);
        if let AuthConfig::AzureServicePrincipalSecret(config) = &mut blank_client.auth {
            config.client_id = "  ".to_string();
        }
        assert_eq!(
            validation_message(validate_context(&blank_client, false)),
            "azure client_id must not be empty"
        );

        let mut bad_tenant = azure_context(None, false);
        if let AuthConfig::AzureServicePrincipalSecret(config) = &mut bad_tenant.auth {
            config.tenant_id = "tenant/../x".to_string();
        }
        assert_eq!(
            validation_message(validate_context(&bad_tenant, false)),
            "azure tenant_id may contain only letters, digits, '.' and '-'"
        );
    }

    #[test]
    fn authority_host_with_credentials_is_refused() {
        for url in [
            "http://localhost:x@evil.com",
            "https://u:p@h",
            "https://user@idp.example",
        ] {
            let message =
                validation_message(validate_context(&azure_context(Some(url), false), false));
            assert_eq!(
                message, "authority_host must not contain credentials",
                "url {url}"
            );
        }
    }

    #[test]
    fn environment_authority_must_be_https_without_credentials() {
        assert!(validate_environment_authority_host("https://login.example").is_ok());
        for url in [
            "http://example.com",
            "http://127.0.0.1:1",
            "https://u:p@login.example",
        ] {
            let message = validation_message(validate_environment_authority_host(url));
            assert_eq!(
                message, "AZURE_AUTHORITY_HOST must be an https:// URL without credentials",
                "url {url}"
            );
        }
    }

    #[test]
    fn azure_modes_always_use_tls() {
        assert!(!uses_plaintext_transport(&azure_context(None, true).auth));
        assert!(validate_context(&azure_context(None, true), false).is_ok());
    }

    #[test]
    fn workload_identity_checks_authority_and_blank_client_only_when_present() {
        use crate::config::azure_credential_model::{
            AzureManagedIdentityConfig, AzureWorkloadIdentityConfig,
        };
        let workload = |authority: Option<&str>| {
            context(
                AuthConfig::AzureWorkloadIdentity(AzureWorkloadIdentityConfig {
                    tenant_id: None,
                    client_id: None,
                    token_file: None,
                    scope: None,
                    authority_host: authority.map(str::to_string),
                }),
                true,
            )
        };
        assert!(validate_context(&workload(None), false).is_ok());
        assert_eq!(
            validation_message(validate_context(
                &workload(Some("http://example.com")),
                false
            )),
            "authority_host over http:// is not allowed on a protected context"
        );

        let managed = context(
            AuthConfig::AzureManagedIdentity(AzureManagedIdentityConfig {
                client_id: Some("  ".to_string()),
                scope: None,
            }),
            true,
        );
        assert_eq!(
            validation_message(validate_context(&managed, false)),
            "azure client_id must not be empty"
        );
        let system_assigned = context(
            AuthConfig::AzureManagedIdentity(AzureManagedIdentityConfig {
                client_id: None,
                scope: None,
            }),
            true,
        );
        assert!(validate_context(&system_assigned, false).is_ok());
    }

    #[test]
    fn protected_oidc_without_tls_is_refused() {
        let mut context = oidc_context("https://idp.example/token", true);
        if let AuthConfig::OAuthOidc(config) = &mut context.auth {
            config.use_tls = false;
        }

        let message = validation_message(validate_context(&context, false));

        assert_eq!(
            message,
            "use_tls = false is not allowed on a protected context"
        );
    }
}
