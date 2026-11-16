use crate::execution_context::ExecutionContext;
use crate::output::profile_views::{ProfileListView, ProfileSummary};
use kafcli_core::config::loader::load_configuration;

/// Lists every configured profile, marking the default one.
pub fn list_profiles(context: &ExecutionContext) -> anyhow::Result<ProfileListView> {
    let configuration = load_configuration(&context.config_path)?;
    let default_name = configuration.current_context.as_deref();
    let profiles = configuration
        .contexts
        .iter()
        .map(|(name, profile)| {
            ProfileSummary::from_profile(name, profile, default_name == Some(name.as_str()))
        })
        .collect();
    Ok(ProfileListView { profiles })
}
