use crate::execution_context::ExecutionContext;
use crate::output::profile_views::ProfileSummary;
use kafcli_core::config::loader::load_configuration_file_only;
use kafcli_core::config::selection::select_context;
use kafcli_core::config::writer::save_configuration;

/// Makes `name` the default profile. Fails when no profile has that name.
pub fn use_profile(context: &ExecutionContext, name: &str) -> anyhow::Result<ProfileSummary> {
    let mut configuration = load_configuration_file_only(&context.config_path)?;
    let selected = select_context(&configuration, Some(name))?;
    configuration.current_context = Some(selected.name.clone());
    save_configuration(&context.config_path, &configuration)?;
    Ok(ProfileSummary::from_profile(
        &selected.name,
        &selected.context,
        true,
    ))
}
