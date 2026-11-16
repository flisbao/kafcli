use crate::config::model::ConnectionContext;

/// Values given on the command line that take precedence over the selected context.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContextOverrides {
    pub brokers: Option<Vec<String>>,
}

/// Returns `context` with every value present in `overrides` replaced.
pub fn apply_context_overrides(
    context: ConnectionContext,
    overrides: &ContextOverrides,
) -> ConnectionContext {
    let mut overridden_context = context;
    if let Some(brokers) = &overrides.brokers {
        overridden_context.brokers = brokers.clone();
    }
    overridden_context
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::AuthConfig;

    fn context_with_brokers(brokers: &[&str]) -> ConnectionContext {
        ConnectionContext {
            brokers: brokers.iter().map(|broker| broker.to_string()).collect(),
            auth: AuthConfig::Plaintext,
            protected: false,
            schema_registry: None,
        }
    }

    #[test]
    fn brokers_override_replaces_context_brokers() {
        let overrides = ContextOverrides {
            brokers: Some(vec!["override:9092".to_string()]),
        };

        let applied = apply_context_overrides(context_with_brokers(&["base:9092"]), &overrides);

        assert_eq!(applied.brokers, vec!["override:9092".to_string()]);
    }

    #[test]
    fn empty_overrides_keep_context_unchanged() {
        let context = context_with_brokers(&["base:9092"]);

        let applied = apply_context_overrides(context.clone(), &ContextOverrides::default());

        assert_eq!(applied, context);
    }
}
