use crate::execution_context::ExecutionContext;
use crate::output::profile_views::ProfileSummary;
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::selection::select_profile;
use kafcli_core::config::writer::save_configuration;

/// Makes `name` the default profile. Fails when no profile has that name.
pub fn use_profile(context: &ExecutionContext, name: &str) -> anyhow::Result<ProfileSummary> {
    let mut configuration = load_configuration(&context.config_path)?;
    let selected = select_profile(&configuration, Some(name))?;
    configuration.default_profile = Some(selected.name.clone());
    save_configuration(&context.config_path, &configuration)?;
    Ok(ProfileSummary::from_profile(
        &selected.name,
        &selected.profile,
        true,
    ))
}
