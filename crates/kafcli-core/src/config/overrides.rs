use crate::config::model::Profile;

/// Values given on the command line that take precedence over the selected profile.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProfileOverrides {
    pub brokers: Option<Vec<String>>,
}

/// Returns `profile` with every value present in `overrides` replaced.
pub fn apply_profile_overrides(profile: Profile, overrides: &ProfileOverrides) -> Profile {
    let mut overridden_profile = profile;
    if let Some(brokers) = &overrides.brokers {
        overridden_profile.brokers = brokers.clone();
    }
    overridden_profile
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::AuthConfig;

    fn profile_with_brokers(brokers: &[&str]) -> Profile {
        Profile {
            brokers: brokers.iter().map(|broker| broker.to_string()).collect(),
            auth: AuthConfig::Plaintext,
            protected: false,
            schema_registry: None,
        }
    }

    #[test]
    fn brokers_override_replaces_profile_brokers() {
        let overrides = ProfileOverrides {
            brokers: Some(vec!["override:9092".to_string()]),
        };

        let applied = apply_profile_overrides(profile_with_brokers(&["base:9092"]), &overrides);

        assert_eq!(applied.brokers, vec!["override:9092".to_string()]);
    }

    #[test]
    fn empty_overrides_keep_profile_unchanged() {
        let profile = profile_with_brokers(&["base:9092"]);

        let applied = apply_profile_overrides(profile.clone(), &ProfileOverrides::default());

        assert_eq!(applied, profile);
    }
}
