use crate::execution_context::ExecutionContext;
use crate::output::context_views::CurrentContextView;
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::selection::select_context;

/// Names the context that commands use: the `--context` flag, then the configured current context.
pub fn show_current_context(execution: &ExecutionContext) -> anyhow::Result<CurrentContextView> {
    let configuration = load_configuration(&execution.config_path)?;
    let selected = select_context(&configuration, execution.global.context.as_deref())?;
    Ok(CurrentContextView {
        name: selected.name,
    })
}
