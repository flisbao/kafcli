use crate::config::model::{Configuration, Profile};
use crate::error::{CoreError, CoreResult};

/// A profile chosen from the configuration, together with its name.
#[derive(Clone, Debug, PartialEq)]
pub struct SelectedProfile {
    pub name: String,
    pub profile: Profile,
}

/// Chooses the requested profile, or the configured default when no name is requested.
pub fn select_profile(
    configuration: &Configuration,
    requested_name: Option<&str>,
) -> CoreResult<SelectedProfile> {
    let profile_name = match requested_name {
        Some(name) => name,
        None => configuration
            .default_profile
            .as_deref()
            .ok_or(CoreError::NoProfileSelected)?,
    };
    let profile = configuration
        .profiles
        .get(profile_name)
        .ok_or_else(|| CoreError::ProfileNotFound(profile_name.to_string()))?;
    Ok(SelectedProfile {
        name: profile_name.to_string(),
        profile: profile.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{AuthConfig, Profile};
    use crate::error::CoreError;
    use std::collections::BTreeMap;

    fn plaintext_profile(broker: &str) -> Profile {
        Profile {
            brokers: vec![broker.to_string()],
            auth: AuthConfig::Plaintext,
            protected: false,
            schema_registry: None,
        }
    }

    fn configuration_with_profiles(default_profile: Option<&str>) -> Configuration {
        let profiles = BTreeMap::from([
            ("dev".to_string(), plaintext_profile("dev:9092")),
            ("prd".to_string(), plaintext_profile("prd:9092")),
        ]);
        Configuration {
            default_profile: default_profile.map(str::to_string),
            profiles,
        }
    }

    #[test]
    fn selects_requested_profile() {
        let selected =
            select_profile(&configuration_with_profiles(Some("dev")), Some("prd")).unwrap();

        assert_eq!(selected.name, "prd");
        assert_eq!(selected.profile.brokers, vec!["prd:9092".to_string()]);
    }

    #[test]
    fn falls_back_to_default_profile() {
        let selected = select_profile(&configuration_with_profiles(Some("dev")), None).unwrap();

        assert_eq!(selected.name, "dev");
    }

    #[test]
    fn reports_missing_requested_profile() {
        let error =
            select_profile(&configuration_with_profiles(Some("dev")), Some("nope")).unwrap_err();

        assert!(matches!(error, CoreError::ProfileNotFound(name) if name == "nope"));
    }

    #[test]
    fn reports_missing_default_profile_by_name() {
        let error = select_profile(&configuration_with_profiles(Some("ghost")), None).unwrap_err();

        assert!(matches!(error, CoreError::ProfileNotFound(name) if name == "ghost"));
    }

    #[test]
    fn reports_when_nothing_is_selected() {
        let error = select_profile(&configuration_with_profiles(None), None).unwrap_err();

        assert!(matches!(error, CoreError::NoProfileSelected));
    }
}
