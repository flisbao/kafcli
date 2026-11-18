use crate::execution_context::ExecutionContext;
use crate::output::production_warning::warn_if_production;
use kafcli_core::config::model::Configuration;
use kafcli_core::config::overrides::{ContextOverrides, apply_context_overrides};
use kafcli_core::config::selection::select_context;
use kafcli_core::connection::client_config_builder::ConnectionSettings;
use kafcli_core::ops::groups::kafka_cluster_reader::KafkaClusterReader;
use kafcli_core::secret::resolver::SecretResolver;
use std::time::Duration;

/// Selects the context, warns when it is protected and opens a read-only cluster reader with the
/// `--brokers` and `--insecure` overrides applied.
pub fn connect_cluster_reader(
    execution: &ExecutionContext,
    configuration: &Configuration,
    resolver: &dyn SecretResolver,
    timeout_seconds: u64,
) -> anyhow::Result<KafkaClusterReader> {
    let selected = select_context(configuration, execution.global.context.as_deref())?;
    warn_if_production(&selected.name, &selected.context);
    let overrides = ContextOverrides {
        brokers: execution.global.brokers.clone(),
    };
    let connection_context = apply_context_overrides(selected.context, &overrides);
    let settings = ConnectionSettings {
        insecure: execution.global.insecure,
    };
    Ok(KafkaClusterReader::connect(
        &connection_context,
        resolver,
        &settings,
        Duration::from_secs(timeout_seconds),
    )?)
}
