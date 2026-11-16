use crate::execution_context::ExecutionContext;
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::overrides::{ContextOverrides, apply_context_overrides};
use kafcli_core::config::selection::select_context;
use kafcli_core::connection::client_config_builder::ConnectionSettings;
use kafcli_core::connection::connection_probe::{ConnectionReport, probe_context_connection};
use kafcli_core::secret::resolver::SecretResolver;
use std::time::Duration;

/// Connects to the context's brokers and reports what the credentials can see.
///
/// Without a name, the `--context` flag and then the current context are used.
/// `--brokers` replaces the context's brokers for this run only.
/// The secret resolver is injected by the composition point (`main.rs`).
pub fn test_context(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    name: Option<&str>,
    timeout: Duration,
) -> anyhow::Result<ConnectionReport> {
    let configuration = load_configuration(&execution.config_path)?;
    let requested_name = name.or(execution.global.context.as_deref());
    let selected = select_context(&configuration, requested_name)?;
    let overrides = ContextOverrides {
        brokers: execution.global.brokers.clone(),
    };
    let connection_context = apply_context_overrides(selected.context, &overrides);
    let settings = ConnectionSettings {
        insecure: execution.global.insecure,
    };
    let report = probe_context_connection(
        &selected.name,
        &connection_context,
        resolver,
        &settings,
        timeout,
    )?;
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
        let config_path = directory.path().join("config.json");
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
        let execution = ExecutionContext {
            config_path,
            global: GlobalArguments {
                context: None,
                output: OutputFormat::Table,
                verbosity: 0,
                config: None,
                brokers: None,
                insecure: false,
            },
        };

        let error = test_context(
            &execution,
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
