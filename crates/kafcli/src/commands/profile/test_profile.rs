use crate::execution_context::ExecutionContext;
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::overrides::{ContextOverrides, apply_context_overrides};
use kafcli_core::config::selection::select_context;
use kafcli_core::connection::client_config_builder::ConnectionSettings;
use kafcli_core::connection::connection_probe::{ConnectionReport, probe_context_connection};
use kafcli_core::secret::resolver::SecretResolver;
use std::time::Duration;

/// Connects to the profile's brokers and reports what the credentials can see.
///
/// Without a name, the `--profile` flag and then the configured default are used.
/// `--brokers` replaces the profile's brokers for this run only.
/// The secret resolver is injected by the composition point (`main.rs`).
pub fn test_profile(
    context: &ExecutionContext,
    resolver: &dyn SecretResolver,
    name: Option<&str>,
    timeout: Duration,
) -> anyhow::Result<ConnectionReport> {
    let configuration = load_configuration(&context.config_path)?;
    let requested_name = name.or(context.global.profile.as_deref());
    let selected = select_context(&configuration, requested_name)?;
    let overrides = ContextOverrides {
        brokers: context.global.brokers.clone(),
    };
    let profile = apply_context_overrides(selected.context, &overrides);
    let settings = ConnectionSettings {
        insecure: context.global.insecure,
    };
    let report = probe_context_connection(&selected.name, &profile, resolver, &settings, timeout)?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::arguments::GlobalArguments;
    use crate::output::output_format::OutputFormat;
    use kafcli_core::config::model::{AuthConfig, Configuration, ConnectionContext};
    use kafcli_core::config::writer::save_configuration;
    use kafcli_core::error::CoreError;
    use kafcli_core::secret::fixed_resolver::FixedSecretResolver;
    use std::collections::BTreeMap;

    #[test]
    fn reports_kafka_error_for_unreachable_broker_with_injected_resolver() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.toml");
        let configuration = Configuration {
            current_context: None,
            contexts: BTreeMap::from([(
                "local".to_string(),
                ConnectionContext {
                    brokers: vec!["127.0.0.1:1".to_string()],
                    auth: AuthConfig::Plaintext,
                    protected: false,
                    schema_registry: None,
                },
            )]),
        };
        save_configuration(&config_path, &configuration).unwrap();
        let context = ExecutionContext {
            config_path,
            global: GlobalArguments {
                profile: None,
                output: OutputFormat::Table,
                verbosity: 0,
                config: None,
                brokers: None,
                insecure: false,
            },
        };

        let error = test_profile(
            &context,
            &FixedSecretResolver::returning("unused"),
            Some("local"),
            Duration::from_secs(2),
        )
        .unwrap_err();

        assert!(matches!(
            error.downcast_ref::<CoreError>(),
            Some(CoreError::Kafka(_))
        ));
    }
}
