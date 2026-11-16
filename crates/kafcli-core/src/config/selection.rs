use crate::config::model::{Configuration, ConnectionContext};
use crate::error::{CoreError, CoreResult};

/// A context chosen from the configuration, together with its name.
#[derive(Clone, Debug, PartialEq)]
pub struct SelectedContext {
    pub name: String,
    pub context: ConnectionContext,
}

/// Chooses the requested context, or the configured default when no name is requested.
pub fn select_context(
    configuration: &Configuration,
    requested_name: Option<&str>,
) -> CoreResult<SelectedContext> {
    let context_name = match requested_name {
        Some(name) => name,
        None => configuration
            .current_context
            .as_deref()
            .ok_or(CoreError::NoContextSelected)?,
    };
    let context = configuration
        .contexts
        .get(context_name)
        .ok_or_else(|| CoreError::ContextNotFound(context_name.to_string()))?;
    Ok(SelectedContext {
        name: context_name.to_string(),
        context: context.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{AuthConfig, ConnectionContext};
    use crate::error::CoreError;
    use std::collections::BTreeMap;

    fn plaintext_context(broker: &str) -> ConnectionContext {
        ConnectionContext {
            brokers: vec![broker.to_string()],
            auth: AuthConfig::Plaintext,
            protected: false,
            schema_registry: None,
        }
    }

    fn configuration_with_contexts(current_context: Option<&str>) -> Configuration {
        let contexts = BTreeMap::from([
            ("dev".to_string(), plaintext_context("dev:9092")),
            ("prd".to_string(), plaintext_context("prd:9092")),
        ]);
        Configuration {
            current_context: current_context.map(str::to_string),
            contexts,
        }
    }

    #[test]
    fn selects_requested_context() {
        let selected =
            select_context(&configuration_with_contexts(Some("dev")), Some("prd")).unwrap();

        assert_eq!(selected.name, "prd");
        assert_eq!(selected.context.brokers, vec!["prd:9092".to_string()]);
    }

    #[test]
    fn falls_back_to_current_context() {
        let selected = select_context(&configuration_with_contexts(Some("dev")), None).unwrap();

        assert_eq!(selected.name, "dev");
    }

    #[test]
    fn reports_missing_requested_context() {
        let error =
            select_context(&configuration_with_contexts(Some("dev")), Some("nope")).unwrap_err();

        assert!(matches!(error, CoreError::ContextNotFound(name) if name == "nope"));
    }

    #[test]
    fn reports_missing_current_context_by_name() {
        let error = select_context(&configuration_with_contexts(Some("ghost")), None).unwrap_err();

        assert!(matches!(error, CoreError::ContextNotFound(name) if name == "ghost"));
    }

    #[test]
    fn reports_when_nothing_is_selected() {
        let error = select_context(&configuration_with_contexts(None), None).unwrap_err();

        assert!(matches!(error, CoreError::NoContextSelected));
    }
}
