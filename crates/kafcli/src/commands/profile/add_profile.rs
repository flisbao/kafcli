use crate::cli::profile_arguments::{AddProfileArguments, AuthTypeArgument};
use crate::execution_context::ExecutionContext;
use crate::output::profile_views::ProfileSummary;
use anyhow::{Context, anyhow, bail};
use kafcli_core::config::loader::load_configuration_file_only;
use kafcli_core::config::model::{
    AuthConfig, AzureConnectionStringConfig, MutualTlsAuthConfig, Profile, SaslCredentialsConfig,
    TlsAuthConfig,
};
use kafcli_core::config::validation::validate_profile;
use kafcli_core::config::writer::save_configuration;
use kafcli_core::secret::reference::SecretReference;

/// Adds a profile after validating it. An existing name is replaced only with `--force`.
pub fn add_profile(
    context: &ExecutionContext,
    arguments: &AddProfileArguments,
) -> anyhow::Result<ProfileSummary> {
    let mut configuration = load_configuration_file_only(&context.config_path)?;
    if configuration.profiles.contains_key(&arguments.name) && !arguments.force {
        bail!(
            "profile '{}' already exists; pass --force to replace it",
            arguments.name
        );
    }

    let profile = Profile {
        brokers: arguments.brokers.clone(),
        auth: build_auth_config(arguments)?,
        protected: arguments.protected,
        schema_registry: None,
    };
    validate_profile(&profile, false)?;

    let is_default = arguments.set_default
        || configuration.default_profile.as_deref() == Some(arguments.name.as_str());
    let summary = ProfileSummary::from_profile(&arguments.name, &profile, is_default);
    configuration
        .profiles
        .insert(arguments.name.clone(), profile);
    if arguments.set_default {
        configuration.default_profile = Some(arguments.name.clone());
    }
    save_configuration(&context.config_path, &configuration)?;
    Ok(summary)
}

fn build_auth_config(arguments: &AddProfileArguments) -> anyhow::Result<AuthConfig> {
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
    }
}

fn build_sasl_credentials(
    arguments: &AddProfileArguments,
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
