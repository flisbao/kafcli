use crate::execution_context::ExecutionContext;
use crate::output::context_views::{ContextListView, ContextSummary};
use kafcli_core::config::loader::load_configuration;

/// Lists every configured context, marking the effective one: the `--context` flag or
/// `KAFCLI_CONTEXT` when set, otherwise the configured current context (same rule as
/// `context current`).
pub fn list_contexts(execution: &ExecutionContext) -> anyhow::Result<ContextListView> {
    let configuration = load_configuration(&execution.config_path)?;
    let effective_name = execution
        .global
        .context
        .as_deref()
        .or(configuration.current_context.as_deref());
    let contexts = configuration
        .contexts
        .iter()
        .map(|(name, context)| {
            ContextSummary::from_context(name, context, effective_name == Some(name.as_str()))
        })
        .collect();
    Ok(ContextListView { contexts })
}
