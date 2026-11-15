use secrecy::{ExposeSecret, SecretString};
use std::collections::BTreeMap;
use std::fmt;

const MASKED_VALUE: &str = "***";

/// A client property: plain text, or a secret that is only exposed when applied to a client config.
#[derive(Debug)]
pub enum PropertyValue {
    Plain(String),
    Secret(SecretString),
}

/// librdkafka client properties built by auth providers.
#[derive(Default)]
pub struct ClientProperties {
    entries: BTreeMap<String, PropertyValue>,
}

impl ClientProperties {
    pub fn set_plain(&mut self, key: &str, value: impl Into<String>) {
        self.entries
            .insert(key.to_string(), PropertyValue::Plain(value.into()));
    }

    pub fn set_secret(&mut self, key: &str, value: SecretString) {
        self.entries
            .insert(key.to_string(), PropertyValue::Secret(value));
    }

    pub fn plain_value(&self, key: &str) -> Option<&str> {
        match self.entries.get(key) {
            Some(PropertyValue::Plain(value)) => Some(value.as_str()),
            _ => None,
        }
    }

    pub fn is_secret(&self, key: &str) -> bool {
        matches!(self.entries.get(key), Some(PropertyValue::Secret(_)))
    }

    /// Copies every property into the librdkafka configuration.
    /// This is the only place that exposes secret values.
    pub fn apply_to(&self, client_config: &mut rdkafka::ClientConfig) {
        for (key, value) in &self.entries {
            match value {
                PropertyValue::Plain(text) => {
                    client_config.set(key.as_str(), text.as_str());
                }
                PropertyValue::Secret(secret) => {
                    client_config.set(key.as_str(), secret.expose_secret());
                }
            }
        }
    }
}

impl fmt::Debug for ClientProperties {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let masked = self.entries.iter().map(|(key, value)| {
            let shown = match value {
                PropertyValue::Plain(text) => text.as_str(),
                PropertyValue::Secret(_) => MASKED_VALUE,
            };
            (key.as_str(), shown)
        });
        formatter.debug_map().entries(masked).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_masks_secret_values() {
        let mut properties = ClientProperties::default();
        properties.set_secret("sasl.password", "s3cret".to_string().into());
        let rendered = format!("{properties:?}");
        assert!(!rendered.contains("s3cret") && rendered.contains("***"));
    }

    #[test]
    fn applies_secret_values_to_client_config() {
        let mut properties = ClientProperties::default();
        properties.set_plain("security.protocol", "SASL_SSL");
        properties.set_secret("sasl.password", "s3cret".to_string().into());
        let mut client_config = rdkafka::ClientConfig::new();
        properties.apply_to(&mut client_config);
        assert_eq!(client_config.get("sasl.password"), Some("s3cret"));
        assert_eq!(client_config.get("security.protocol"), Some("SASL_SSL"));
    }

    #[test]
    fn plain_value_hides_secrets_and_is_secret_reports_them() {
        let mut properties = ClientProperties::default();
        properties.set_secret("sasl.password", "s3cret".to_string().into());
        assert_eq!(properties.plain_value("sasl.password"), None);
        assert!(properties.is_secret("sasl.password"));
        assert!(!properties.is_secret("missing"));
    }
}
