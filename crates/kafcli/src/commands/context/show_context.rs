use crate::execution_context::ExecutionContext;
use crate::output::context_views::{ContextDetailView, RegistryDetailView};
use kafcli_core::auth::provider_factory::create_auth_provider;
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::model::AuthConfig;
use kafcli_core::config::selection::select_context;
use kafcli_core::secret::reference::SecretReference;

/// Describes one context. Secrets are listed by reference only and never resolved.
///
/// Without a name, the `--context` flag and then the current context are used.
pub fn show_context(
    execution: &ExecutionContext,
    name: Option<&str>,
) -> anyhow::Result<ContextDetailView> {
    let configuration = load_configuration(&execution.config_path)?;
    let requested_name = name.or(execution.global.context.as_deref());
    let selected = select_context(&configuration, requested_name)?;
    let principal = create_auth_provider(&selected.context.auth).describe_principal();
    Ok(ContextDetailView {
        name: selected.name,
        brokers: selected.context.brokers.clone(),
        auth_type: selected.context.auth.type_name().to_string(),
        principal,
        secret_references: collect_secret_references(&selected.context.auth),
        protected: selected.context.protected,
        schema_registry: selected
            .context
            .schema_registry
            .as_ref()
            .map(RegistryDetailView::from_config),
    })
}

fn collect_secret_references(auth: &AuthConfig) -> Vec<String> {
    let references: Vec<&SecretReference> = match auth {
        AuthConfig::Plaintext | AuthConfig::Tls(_) => Vec::new(),
        AuthConfig::MutualTls(config) => config.key_password_ref.iter().collect(),
        AuthConfig::SaslPlain(credentials)
        | AuthConfig::SaslScram256(credentials)
        | AuthConfig::SaslScram512(credentials) => vec![&credentials.secret_ref],
        AuthConfig::AzureConnectionString(config) => vec![&config.secret_ref],
        AuthConfig::Bearer(config) => vec![&config.token_ref],
        AuthConfig::OAuthOidc(config) => vec![&config.secret_ref],
    };
    references.into_iter().map(ToString::to_string).collect()
}
