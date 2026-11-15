use crate::execution_context::ExecutionContext;
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::overrides::{ProfileOverrides, apply_profile_overrides};
use kafcli_core::config::selection::select_profile;
use kafcli_core::connection::client_config_builder::ConnectionSettings;
use kafcli_core::connection::connection_probe::{ConnectionReport, probe_profile_connection};
use kafcli_core::secret::composite_resolver::CompositeSecretResolver;
use std::time::Duration;

/// Connects to the profile's brokers and reports what the credentials can see.
///
/// Without a name, the `--profile` flag and then the configured default are used.
/// `--brokers` replaces the profile's brokers for this run only.
pub fn test_profile(
    context: &ExecutionContext,
    name: Option<&str>,
    timeout: Duration,
) -> anyhow::Result<ConnectionReport> {
    let configuration = load_configuration(&context.config_path)?;
    let requested_name = name.or(context.global.profile.as_deref());
    let selected = select_profile(&configuration, requested_name)?;
    let overrides = ProfileOverrides {
        brokers: context.global.brokers.clone(),
    };
    let profile = apply_profile_overrides(selected.profile, &overrides);
    let resolver = CompositeSecretResolver::with_default_sources();
    let settings = ConnectionSettings {
        insecure: context.global.insecure,
    };
    let report = probe_profile_connection(&selected.name, &profile, &resolver, &settings, timeout)?;
    Ok(report)
}
