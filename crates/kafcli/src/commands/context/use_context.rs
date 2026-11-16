use crate::execution_context::ExecutionContext;
use crate::output::context_views::ContextSummary;
use crate::output::production_warning::warn_if_production;
use kafcli_core::config::loader::load_configuration_file_only;
use kafcli_core::config::selection::select_context;
use kafcli_core::config::writer::save_configuration;

/// Makes `name` the current context. Fails when no context has that name.
pub fn use_context(execution: &ExecutionContext, name: &str) -> anyhow::Result<ContextSummary> {
    let mut configuration = load_configuration_file_only(&execution.config_path)?;
    let selected = select_context(&configuration, Some(name))?;
    configuration.current_context = Some(selected.name.clone());
    save_configuration(&execution.config_path, &configuration)?;
    warn_if_production(&selected.name, &selected.context);
    Ok(ContextSummary::from_context(
        &selected.name,
        &selected.context,
        true,
    ))
}
