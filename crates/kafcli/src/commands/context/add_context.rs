use crate::cli::context_arguments::{
    AddContextArguments, AuthTypeArgument, RegistryAuthTypeArgument, RegistryTypeArgument,
};
use crate::execution_context::ExecutionContext;
use crate::output::context_views::ContextSummary;
use crate::output::production_warning::warn_if_production;
use anyhow::{Context, anyhow, bail};
use kafcli_core::config::loader::load_configuration_file_only;
use kafcli_core::config::model::{
    AuthConfig, AzureConnectionStringConfig, BearerAuthConfig, ConnectionContext,
    MutualTlsAuthConfig, SaslCredentialsConfig, TlsAuthConfig,
};
use kafcli_core::config::schema_registry_model::{
    AzureRegistryConfig, ConfluentRegistryConfig, RegistryAuthConfig, RegistryBasicAuthConfig,
    RegistryBearerAuthConfig, RegistryMutualTlsConfig, SchemaRegistryConfig,
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
    }
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
