use crate::error::CoreError;
use crate::message::record_header::RecordHeader;
use serde_json::Value;
use std::str::FromStr;

const REDACTED_TEXT: &str = "[redacted]";
const HEADER_FILTER_PREFIX: &str = "header:";

/// Keeps a message only when it has a header `name` whose UTF-8 value equals `value`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderFilter {
    pub name: String,
    pub value: String,
}

impl FromStr for HeaderFilter {
    type Err = CoreError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let parsed = text
            .strip_prefix(HEADER_FILTER_PREFIX)
            .and_then(|rest| rest.split_once('='))
            .filter(|(name, _)| !name.is_empty());
        match parsed {
            Some((name, value)) => Ok(HeaderFilter {
                name: name.to_string(),
                value: value.to_string(),
            }),
            None => Err(CoreError::InvalidInput(format!(
                "invalid filter '{text}': expected header:NAME=VALUE"
            ))),
        }
    }
}

/// True when every filter matches at least one header. With no filters every message matches.
pub fn matches_header_filters(filters: &[HeaderFilter], headers: &[RecordHeader]) -> bool {
    filters.iter().all(|filter| {
        headers.iter().any(|header| {
            header.name == filter.name
                && header
                    .value
                    .as_deref()
                    .and_then(|bytes| std::str::from_utf8(bytes).ok())
                    == Some(filter.value.as_str())
        })
    })
}

/// Replaces, at any depth, the value of every object key whose name is in `field_names`.
pub fn redact_fields(value: &mut Value, field_names: &[String]) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if field_names.iter().any(|name| name == key) {
                    *child = Value::String(REDACTED_TEXT.to_string());
                } else {
                    redact_fields(child, field_names);
                }
            }
        }
        Value::Array(items) => {
            for item in items.iter_mut() {
                redact_fields(item, field_names);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn header(name: &str, value: Option<&[u8]>) -> RecordHeader {
        RecordHeader {
            name: name.to_string(),
            value: value.map(<[u8]>::to_vec),
        }
    }

    #[test]
    fn parses_header_filter_with_equals_in_value() {
        let filter = "header:a=b=c".parse::<HeaderFilter>().unwrap();

        assert_eq!(filter.name, "a");
        assert_eq!(filter.value, "b=c");
    }

    #[test]
    fn rejects_filter_without_name_or_separator() {
        assert!("header:=x".parse::<HeaderFilter>().is_err());
        assert!("header:name".parse::<HeaderFilter>().is_err());
        assert!("name=value".parse::<HeaderFilter>().is_err());
    }

    #[test]
    fn redacts_nested_fields_and_array_items() {
        let mut value = json!({
            "password": "p1",
            "user": {"token": "t1", "name": "ana"},
            "items": [{"secret": "s1", "id": 1}, {"id": 2}],
            "keep": "yes"
        });

        redact_fields(
            &mut value,
            &[
                "password".to_string(),
                "token".to_string(),
                "secret".to_string(),
            ],
        );

        assert_eq!(
            value,
            json!({
                "password": "[redacted]",
                "user": {"token": "[redacted]", "name": "ana"},
                "items": [{"secret": "[redacted]", "id": 1}, {"id": 2}],
                "keep": "yes"
            })
        );
    }

    #[test]
    fn filter_requires_all_headers_to_match() {
        let headers = vec![
            header("type", Some(b"order")),
            header("region", Some(b"eu")),
            header("blob", Some(&[0xff])),
        ];
        let both = [
            HeaderFilter {
                name: "type".into(),
                value: "order".into(),
            },
            HeaderFilter {
                name: "region".into(),
                value: "eu".into(),
            },
        ];
        let one_missing = [
            HeaderFilter {
                name: "type".into(),
                value: "order".into(),
            },
            HeaderFilter {
                name: "region".into(),
                value: "us".into(),
            },
        ];
        let binary = [HeaderFilter {
            name: "blob".into(),
            value: "x".into(),
        }];

        assert!(matches_header_filters(&both, &headers));
        assert!(!matches_header_filters(&one_missing, &headers));
        assert!(!matches_header_filters(&binary, &headers));
        assert!(matches_header_filters(&[], &headers));
    }
}
