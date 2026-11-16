use crate::execution_context::ExecutionContext;
use crate::output::context_views::{ContextListView, ContextSummary};
use kafcli_core::config::loader::load_configuration;

/// Lists every configured context, marking the current one.
pub fn list_contexts(execution: &ExecutionContext) -> anyhow::Result<ContextListView> {
    let configuration = load_configuration(&execution.config_path)?;
    let current_name = configuration.current_context.as_deref();
    let contexts = configuration
        .contexts
        .iter()
        .map(|(name, context)| {
            ContextSummary::from_context(name, context, current_name == Some(name.as_str()))
        })
        .collect();
    Ok(ContextListView { contexts })
}
