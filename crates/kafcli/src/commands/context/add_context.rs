use crate::cli::context_arguments::{
    AddContextArguments, AuthTypeArgument, RegistryAuthTypeArgument, RegistryTypeArgument,
};
use crate::execution_context::ExecutionContext;
use crate::output::context_views::ContextSummary;
use crate::output::production_warning::warn_if_production;
use anyhow::{Context, anyhow, bail};
use kafcli_core::config::azure_credential_model::{
    AzureCliConfig, AzureDefaultConfig, AzureManagedIdentityConfig,
    AzureServicePrincipalCertificateConfig, AzureServicePrincipalSecretConfig,
    AzureWorkloadIdentityConfig,
};
use kafcli_core::config::loader::load_configuration_file_only;
use kafcli_core::config::model::{
    AuthConfig, AzureConnectionStringConfig, BearerAuthConfig, ConnectionContext,
    MutualTlsAuthConfig, OAuthOidcAuthConfig, SaslCredentialsConfig, TlsAuthConfig,
};
use kafcli_core::config::schema_registry_model::{
    AzureRegistryConfig, ConfluentRegistryConfig, RegistryAuthConfig, RegistryAzureAuthConfig,
    RegistryBasicAuthConfig, RegistryBearerAuthConfig, RegistryMutualTlsConfig,
    SchemaRegistryConfig,
};
use kafcli_core::config::validation::validate_context;
use kafcli_core::config::writer::save_configuration;
use kafcli_core::secret::reference::SecretReference;

/// Adds a context after validating it. An existing name is replaced only with `--force`.
pub fn add_context(
    execution: &ExecutionContext,
    arguments: &AddContextArguments,
) -> anyhow::Result<ContextSummary> {
    let mut configuration = load_configuration_file_only(&execution.config_path)?;
    if configuration.contexts.contains_key(&arguments.name) && !arguments.force {
        bail!(
            "context '{}' already exists; pass --force to replace it",
            arguments.name
        );
    }

    let connection_context = ConnectionContext {
        brokers: arguments.brokers.clone(),
        auth: build_auth_config(arguments)?,
        protected: arguments.protected,
        schema_registry: build_schema_registry_config(arguments)?,
    };
    validate_context(&connection_context, false)?;

    let is_current = arguments.use_after_add
        || configuration.current_context.as_deref() == Some(arguments.name.as_str());
    let summary = ContextSummary::from_context(&arguments.name, &connection_context, is_current);
    configuration
        .contexts
        .insert(arguments.name.clone(), connection_context.clone());
    if arguments.use_after_add {
        configuration.current_context = Some(arguments.name.clone());
    }
    save_configuration(&execution.config_path, &configuration)?;
    if arguments.use_after_add {
        warn_if_production(&arguments.name, &connection_context);
    }
    Ok(summary)
}

fn build_auth_config(arguments: &AddContextArguments) -> anyhow::Result<AuthConfig> {
    let auth_type = arguments.auth_type;
    match auth_type {
        AuthTypeArgument::None => Ok(AuthConfig::Plaintext),
        AuthTypeArgument::Tls => Ok(AuthConfig::Tls(TlsAuthConfig {
            ca_location: arguments.ca_location.clone(),
        })),
        AuthTypeArgument::Mtls => Ok(AuthConfig::MutualTls(MutualTlsAuthConfig {
            ca_location: arguments.ca_location.clone(),
            certificate_location: require_value(
                &arguments.certificate_location,
                "--certificate-location",
                auth_type,
            )?,
            key_location: require_value(&arguments.key_location, "--key-location", auth_type)?,
            key_password_ref: arguments
                .key_password_ref
                .as_deref()
                .map(|text| parse_secret_reference(text, "--key-password-ref"))
                .transpose()?,
        })),
        AuthTypeArgument::SaslPlain => {
            Ok(AuthConfig::SaslPlain(build_sasl_credentials(arguments)?))
        }
        AuthTypeArgument::SaslScram256 => {
            Ok(AuthConfig::SaslScram256(build_sasl_credentials(arguments)?))
        }
        AuthTypeArgument::SaslScram512 => {
            Ok(AuthConfig::SaslScram512(build_sasl_credentials(arguments)?))
        }
        AuthTypeArgument::AzureConnectionString => {
            let secret_text = require_value(&arguments.secret_ref, "--secret-ref", auth_type)?;
            Ok(AuthConfig::AzureConnectionString(
                AzureConnectionStringConfig {
                    secret_ref: parse_secret_reference(&secret_text, "--secret-ref")?,
                },
            ))
        }
        AuthTypeArgument::Bearer => Ok(AuthConfig::Bearer(build_bearer_config(arguments)?)),
        AuthTypeArgument::OauthOidc => {
            Ok(AuthConfig::OAuthOidc(build_oauth_oidc_config(arguments)?))
        }
        AuthTypeArgument::AzureSpSecret => Ok(AuthConfig::AzureServicePrincipalSecret(
            build_azure_sp_secret_config(arguments)?,
        )),
        AuthTypeArgument::AzureSpCert => Ok(AuthConfig::AzureServicePrincipalCertificate(
            build_azure_sp_cert_config(arguments)?,
        )),
        AuthTypeArgument::AzureWorkloadIdentity => Ok(AuthConfig::AzureWorkloadIdentity(
            build_azure_workload_identity_config(arguments),
        )),
        AuthTypeArgument::AzureManagedIdentity => Ok(AuthConfig::AzureManagedIdentity(
            AzureManagedIdentityConfig {
                client_id: arguments.client_id.clone(),
                scope: arguments.scope.clone(),
            },
        )),
        AuthTypeArgument::AzureCli => Ok(AuthConfig::AzureCli(AzureCliConfig {
            tenant_id: arguments.tenant_id.clone(),
            scope: arguments.scope.clone(),
        })),
        AuthTypeArgument::AzureDefault => Ok(AuthConfig::AzureDefault(AzureDefaultConfig {
            tenant_id: arguments.tenant_id.clone(),
            client_id: arguments.client_id.clone(),
            scope: arguments.scope.clone(),
        })),
    }
}

/// Every field is optional: the missing ones come from the AKS environment when the context is used.
fn build_azure_workload_identity_config(
    arguments: &AddContextArguments,
) -> AzureWorkloadIdentityConfig {
    AzureWorkloadIdentityConfig {
        tenant_id: arguments.tenant_id.clone(),
        client_id: arguments.client_id.clone(),
        token_file: arguments.federated_token_file.clone(),
        scope: arguments.scope.clone(),
        authority_host: arguments.authority_host.clone(),
    }
}

fn build_azure_sp_cert_config(
    arguments: &AddContextArguments,
) -> anyhow::Result<AzureServicePrincipalCertificateConfig> {
    let (Some(tenant_id), Some(client_id), Some(certificate_location)) = (
        arguments.tenant_id.clone(),
        arguments.client_id.clone(),
        arguments.certificate_location.clone(),
    ) else {
        bail!(
            "--auth-type azure-sp-cert requires --tenant-id, --client-id and --certificate-location"
        );
    };
    Ok(AzureServicePrincipalCertificateConfig {
        tenant_id,
        client_id,
        certificate_location,
        scope: arguments.scope.clone(),
        authority_host: arguments.authority_host.clone(),
    })
}

fn build_azure_sp_secret_config(
    arguments: &AddContextArguments,
) -> anyhow::Result<AzureServicePrincipalSecretConfig> {
    let (Some(tenant_id), Some(client_id), Some(secret_text)) = (
        arguments.tenant_id.clone(),
        arguments.client_id.clone(),
        arguments.secret_ref.as_deref(),
    ) else {
        bail!("--auth-type azure-sp-secret requires --tenant-id, --client-id and --secret-ref");
    };
    Ok(AzureServicePrincipalSecretConfig {
        tenant_id,
        client_id,
        secret_ref: parse_secret_reference(secret_text, "--secret-ref")?,
        scope: arguments.scope.clone(),
        authority_host: arguments.authority_host.clone(),
    })
}

fn build_oauth_oidc_config(arguments: &AddContextArguments) -> anyhow::Result<OAuthOidcAuthConfig> {
    let (Some(token_endpoint_url), Some(client_id), Some(secret_text)) = (
        arguments.token_endpoint_url.clone(),
        arguments.client_id.clone(),
        arguments.secret_ref.as_deref(),
    ) else {
        bail!("--auth-type oauth-oidc requires --token-endpoint-url, --client-id and --secret-ref");
    };
    Ok(OAuthOidcAuthConfig {
        token_endpoint_url,
        client_id,
        secret_ref: parse_secret_reference(secret_text, "--secret-ref")?,
        scope: arguments.scope.clone(),
        ca_location: arguments.ca_location.clone(),
        use_tls: !arguments.no_tls,
    })
}

fn build_bearer_config(arguments: &AddContextArguments) -> anyhow::Result<BearerAuthConfig> {
    let Some(token_text) = arguments.token_ref.as_deref() else {
        bail!("--auth-type bearer requires --token-ref");
    };
    Ok(BearerAuthConfig {
        token_ref: parse_secret_reference(token_text, "--token-ref")?,
        ca_location: arguments.ca_location.clone(),
        use_tls: !arguments.no_tls,
    })
}

/// Builds the Schema Registry of the context; `None` when no `--registry-*` flag is given.
fn build_schema_registry_config(
    arguments: &AddContextArguments,
) -> anyhow::Result<Option<SchemaRegistryConfig>> {
    let Some(registry_type) = arguments.registry_type else {
        return Ok(None);
    };
    let url = arguments
        .registry_url
        .clone()
        .ok_or_else(|| anyhow!("--registry-url is required with --registry-type"))?;
    let auth = build_registry_auth_config(arguments)?;
    Ok(Some(match registry_type {
        RegistryTypeArgument::Confluent => {
            SchemaRegistryConfig::Confluent(ConfluentRegistryConfig {
                url,
                auth,
                ca_location: arguments.registry_ca_location.clone(),
            })
        }
        RegistryTypeArgument::Azure => SchemaRegistryConfig::Azure(AzureRegistryConfig {
            url,
            group: arguments.registry_group.clone().unwrap_or_default(),
            auth,
        }),
    }))
}

fn build_registry_auth_config(
    arguments: &AddContextArguments,
) -> anyhow::Result<RegistryAuthConfig> {
    match arguments.registry_auth_type {
        RegistryAuthTypeArgument::None => Ok(RegistryAuthConfig::Anonymous),
        RegistryAuthTypeArgument::Azure => Ok(RegistryAuthConfig::Azure(RegistryAzureAuthConfig {
            credential: None,
        })),
        RegistryAuthTypeArgument::Basic => {
            let (Some(username), Some(secret_text)) = (
                arguments.registry_username.as_deref(),
                arguments.registry_secret_ref.as_deref(),
            ) else {
                bail!(
                    "--registry-auth-type basic needs --registry-username and --registry-secret-ref"
                );
            };
            Ok(RegistryAuthConfig::Basic(RegistryBasicAuthConfig {
                username: username.to_string(),
                secret_ref: parse_secret_reference(secret_text, "--registry-secret-ref")?,
            }))
        }
        RegistryAuthTypeArgument::Bearer => {
            let Some(secret_text) = arguments.registry_secret_ref.as_deref() else {
                bail!("--registry-auth-type bearer needs --registry-secret-ref");
            };
            Ok(RegistryAuthConfig::Bearer(RegistryBearerAuthConfig {
                token_ref: parse_secret_reference(secret_text, "--registry-secret-ref")?,
            }))
        }
        RegistryAuthTypeArgument::Mtls => {
            let (Some(certificate_location), Some(key_location)) = (
                arguments.registry_certificate_location.clone(),
                arguments.registry_key_location.clone(),
            ) else {
                bail!(
                    "--registry-auth-type mtls needs --registry-certificate-location and --registry-key-location"
                );
            };
            Ok(RegistryAuthConfig::MutualTls(RegistryMutualTlsConfig {
                certificate_location,
                key_location,
            }))
        }
    }
}

fn build_sasl_credentials(
    arguments: &AddContextArguments,
) -> anyhow::Result<SaslCredentialsConfig> {
    let auth_type = arguments.auth_type;
    let secret_text = require_value(&arguments.secret_ref, "--secret-ref", auth_type)?;
    Ok(SaslCredentialsConfig {
        username: require_value(&arguments.username, "--username", auth_type)?,
        secret_ref: parse_secret_reference(&secret_text, "--secret-ref")?,
        ca_location: arguments.ca_location.clone(),
        use_tls: !arguments.no_tls,
    })
}

fn require_value<T: Clone>(
    value: &Option<T>,
    flag_name: &str,
    auth_type: AuthTypeArgument,
) -> anyhow::Result<T> {
    value.clone().ok_or_else(|| {
        anyhow!(
            "{flag_name} is required for auth type {}",
            auth_type.to_flag_spelling()
        )
    })
}

/// The error names the flag and the expected forms, never the rejected text.
fn parse_secret_reference(text: &str, flag_name: &str) -> anyhow::Result<SecretReference> {
    text.parse::<SecretReference>()
        .with_context(|| format!("{flag_name} is not a valid secret reference"))
}
