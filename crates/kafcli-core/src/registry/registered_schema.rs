use crate::error::{CoreError, CoreResult};
use serde::{Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

/// Format of a schema as the registry names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaKind {
    Avro,
    Json,
}

impl SchemaKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Avro => "avro",
            Self::Json => "json",
        }
    }
}

/// Identifier of a schema inside its registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaId {
    Confluent(u32),
    Azure(String),
}

impl fmt::Display for SchemaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Confluent(number) => write!(formatter, "{number}"),
            Self::Azure(text) => formatter.write_str(text),
        }
    }
}

impl Serialize for SchemaId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

/// Version requested from a registry: the latest one or a 1-based number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaVersionSelector {
    Latest,
    Number(u32),
}

impl fmt::Display for SchemaVersionSelector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Latest => formatter.write_str("latest"),
            Self::Number(number) => write!(formatter, "{number}"),
        }
    }
}

impl FromStr for SchemaVersionSelector {
    type Err = CoreError;

    fn from_str(text: &str) -> CoreResult<Self> {
        if text == "latest" {
            return Ok(Self::Latest);
        }
        let digits_only = !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
        match text.parse::<u32>() {
            Ok(number) if digits_only && number >= 1 => Ok(Self::Number(number)),
            _ => Err(CoreError::InvalidInput(format!(
                "invalid schema version '{text}': expected latest or a positive number"
            ))),
        }
    }
}

/// A schema read from or written to a registry.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RegisteredSchema {
    pub id: SchemaId,
    pub subject: Option<String>,
    pub version: Option<u32>,
    pub kind: SchemaKind,
    pub text: String,
}

/// Result of a compatibility check; `messages` explain why a schema is not compatible.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CompatibilityReport {
    pub compatible: bool,
    pub messages: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_version_selector() {
        assert_eq!(
            "latest".parse::<SchemaVersionSelector>().unwrap(),
            SchemaVersionSelector::Latest
        );
        assert_eq!(
            "3".parse::<SchemaVersionSelector>().unwrap(),
            SchemaVersionSelector::Number(3)
        );
        for text in ["0", "-1", "abc", "+2", ""] {
            let error = text
                .parse::<SchemaVersionSelector>()
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&format!(
                    "invalid schema version '{text}': expected latest or a positive number"
                )),
                "error: {error}"
            );
        }
    }
}
