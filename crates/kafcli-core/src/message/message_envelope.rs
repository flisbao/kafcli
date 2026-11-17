use super::binary_text::{decode_base64, encode_base64};
use super::record_header::RecordHeader;
use crate::error::{CoreError, CoreResult};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description;

/// The only envelope version this build reads and writes.
const SUPPORTED_ENVELOPE_VERSION: u32 = 1;

const TIMESTAMP_FORMAT: &str =
    "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z";

/// One NDJSON line: a single message with its position, key, value and headers.
///
/// Field order is the stable contract. Absent optional fields are omitted, except
/// `timestamp`, `key` and `value`, which are written as `null`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageEnvelope {
    pub kafcli_envelope: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_base64: Option<String>,
    #[serde(default)]
    pub value: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_base64: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub value_truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decode_error: Option<String>,
    #[serde(default)]
    pub headers: Vec<EnvelopeHeader>,
}

/// Header as written in the envelope: UTF-8 text, or base64 when the bytes are not UTF-8.
///
/// The binary variant is listed first so that `{ "name", "value_base64" }` never matches the text variant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum EnvelopeHeader {
    Binary { name: String, value_base64: String },
    Text { name: String, value: Option<String> },
}

/// Value of a consumed record, as decided by the decoder before it reaches the envelope.
#[derive(Debug, Clone, PartialEq)]
pub enum EnvelopeValue {
    /// Valid JSON, written in the `value` field.
    Decoded(serde_json::Value),
    /// Bytes that could not be decoded, written as `value_base64` with `decode_error`.
    Undecodable { bytes: Vec<u8>, error: String },
    /// Only a prefix of the value was kept, written as `value_base64` with `value_truncated`.
    Truncated { prefix: Vec<u8> },
    /// The record has no value (tombstone).
    Absent,
}

impl MessageEnvelope {
    /// Builds the envelope for a record read from a topic.
    pub fn from_record(
        topic: &str,
        partition: i32,
        offset: i64,
        timestamp_millis: Option<i64>,
        key: Option<&[u8]>,
        value_field: EnvelopeValue,
        headers: &[RecordHeader],
    ) -> MessageEnvelope {
        let (key, key_base64) = match key {
            None => (None, None),
            Some(bytes) => split_text_or_base64(bytes),
        };
        let mut envelope = MessageEnvelope {
            kafcli_envelope: SUPPORTED_ENVELOPE_VERSION,
            topic: Some(topic.to_string()),
            partition: Some(partition),
            offset: Some(offset),
            timestamp: timestamp_millis.and_then(format_timestamp_millis),
            key,
            key_base64,
            value: None,
            value_base64: None,
            value_truncated: false,
            decode_error: None,
            headers: headers
                .iter()
                .map(EnvelopeHeader::from_record_header)
                .collect(),
        };
        envelope.apply_value(value_field);
        envelope
    }

    /// Returns the key bytes. `key_base64` takes precedence over `key`.
    pub fn key_bytes(&self) -> CoreResult<Option<Vec<u8>>> {
        match (&self.key_base64, &self.key) {
            (Some(encoded), _) => decode_base64(encoded).map(Some),
            (None, Some(text)) => Ok(Some(text.clone().into_bytes())),
            (None, None) => Ok(None),
        }
    }

    /// Returns the headers as record headers, with their original bytes.
    pub fn record_headers(&self) -> CoreResult<Vec<RecordHeader>> {
        self.headers
            .iter()
            .map(EnvelopeHeader::to_record_header)
            .collect()
    }

    fn apply_value(&mut self, value_field: EnvelopeValue) {
        match value_field {
            EnvelopeValue::Decoded(value) => self.value = Some(value),
            EnvelopeValue::Undecodable { bytes, error } => {
                self.value_base64 = Some(encode_base64(&bytes));
                self.decode_error = Some(error);
            }
            EnvelopeValue::Truncated { prefix } => {
                self.value_base64 = Some(encode_base64(&prefix));
                self.value_truncated = true;
            }
            EnvelopeValue::Absent => {}
        }
    }
}

impl EnvelopeHeader {
    fn from_record_header(header: &RecordHeader) -> Self {
        match &header.value {
            None => EnvelopeHeader::Text {
                name: header.name.clone(),
                value: None,
            },
            Some(bytes) => match std::str::from_utf8(bytes) {
                Ok(text) => EnvelopeHeader::Text {
                    name: header.name.clone(),
                    value: Some(text.to_string()),
                },
                Err(_) => EnvelopeHeader::Binary {
                    name: header.name.clone(),
                    value_base64: encode_base64(bytes),
                },
            },
        }
    }

    fn to_record_header(&self) -> CoreResult<RecordHeader> {
        match self {
            EnvelopeHeader::Text { name, value } => Ok(RecordHeader {
                name: name.clone(),
                value: value.as_ref().map(|text| text.clone().into_bytes()),
            }),
            EnvelopeHeader::Binary { name, value_base64 } => Ok(RecordHeader {
                name: name.clone(),
                value: Some(decode_base64(value_base64)?),
            }),
        }
    }
}

/// Parses one NDJSON line into an envelope.
///
/// Errors describe the line and column and the kind of problem; they never echo the value.
pub fn parse_envelope_line(line: &str) -> CoreResult<MessageEnvelope> {
    let envelope: MessageEnvelope =
        serde_json::from_str(line).map_err(|error| describe_parse_error(&error))?;
    if envelope.kafcli_envelope != SUPPORTED_ENVELOPE_VERSION {
        return Err(CoreError::InvalidInput(format!(
            "unsupported kafcli_envelope version {}",
            envelope.kafcli_envelope
        )));
    }
    Ok(envelope)
}

fn describe_parse_error(error: &serde_json::Error) -> CoreError {
    let reason = match error.classify() {
        serde_json::error::Category::Syntax => "malformed JSON",
        serde_json::error::Category::Eof => "unexpected end of line",
        serde_json::error::Category::Io => "read error",
        serde_json::error::Category::Data => {
            let message = error.to_string();
            if message.starts_with("unknown field") {
                "unknown envelope field"
            } else if message.starts_with("missing field") {
                "missing required envelope field"
            } else {
                "invalid envelope field value or type"
            }
        }
    };
    CoreError::InvalidInput(format!(
        "invalid envelope at line {} column {}: {}",
        error.line(),
        error.column(),
        reason
    ))
}

/// Writes UTF-8 bytes as text and anything else as base64 (`*_base64` field).
fn split_text_or_base64(bytes: &[u8]) -> (Option<String>, Option<String>) {
    match std::str::from_utf8(bytes) {
        Ok(text) => (Some(text.to_string()), None),
        Err(_) => (None, Some(encode_base64(bytes))),
    }
}

/// Formats epoch milliseconds as RFC 3339 UTC with exactly three fractional digits.
///
/// Returns `None` when the instant is outside the range `time` can represent.
fn format_timestamp_millis(millis: i64) -> Option<String> {
    let nanos = i128::from(millis) * 1_000_000;
    let instant = OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()?;
    let description = format_description::parse_borrowed::<2>(TIMESTAMP_FORMAT).ok()?;
    instant.format(&description).ok()
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_header(name: &str, value: &str) -> RecordHeader {
        RecordHeader {
            name: name.to_string(),
            value: Some(value.as_bytes().to_vec()),
        }
    }

    fn json_of(envelope: &MessageEnvelope) -> CoreResult<String> {
        serde_json::to_string(envelope).map_err(|error| CoreError::InvalidInput(error.to_string()))
    }

    #[test]
    fn serializes_fields_in_contract_order() -> CoreResult<()> {
        let envelope = MessageEnvelope {
            kafcli_envelope: 1,
            topic: Some("pedidos".to_string()),
            partition: Some(2),
            offset: Some(42),
            timestamp: Some("2026-11-17T00:24:07.253Z".to_string()),
            key: Some("k".to_string()),
            key_base64: Some("/wA=".to_string()),
            value: Some(serde_json::json!({"id": 1})),
            value_base64: Some("/w==".to_string()),
            value_truncated: true,
            decode_error: Some("bad".to_string()),
            headers: vec![
                EnvelopeHeader::Text {
                    name: "t".to_string(),
                    value: Some("v".to_string()),
                },
                EnvelopeHeader::Binary {
                    name: "b".to_string(),
                    value_base64: "/wA=".to_string(),
                },
            ],
        };
        let json = json_of(&envelope)?;

        let keys = [
            "\"kafcli_envelope\":1",
            "\"topic\":",
            "\"partition\":",
            "\"offset\":",
            "\"timestamp\":",
            "\"key\":",
            "\"key_base64\":",
            "\"value\":",
            "\"value_base64\":",
            "\"value_truncated\":true",
            "\"decode_error\":",
            "\"headers\":",
        ];
        let positions: Vec<usize> = keys
            .iter()
            .map(|key| {
                json.find(key)
                    .ok_or_else(|| CoreError::InvalidInput(format!("missing {key}")))
            })
            .collect::<CoreResult<_>>()?;
        assert!(positions.windows(2).all(|pair| pair[0] < pair[1]), "{json}");
        Ok(())
    }

    #[test]
    fn utf8_key_and_header_use_text_fields() -> CoreResult<()> {
        let envelope = MessageEnvelope::from_record(
            "pedidos",
            0,
            7,
            None,
            Some(b"pedido-1"),
            EnvelopeValue::Absent,
            &[text_header("origem", "api")],
        );
        let json = json_of(&envelope)?;

        assert!(json.contains("\"key\":\"pedido-1\""), "{json}");
        assert!(!json.contains("key_base64"), "{json}");
        assert!(
            json.contains("{\"name\":\"origem\",\"value\":\"api\"}"),
            "{json}"
        );
        Ok(())
    }

    #[test]
    fn binary_key_and_header_use_base64_fields() -> CoreResult<()> {
        let envelope = MessageEnvelope::from_record(
            "pedidos",
            0,
            7,
            None,
            Some(&[0xff, 0x00]),
            EnvelopeValue::Absent,
            &[RecordHeader {
                name: "raw".to_string(),
                value: Some(vec![0xff, 0x00]),
            }],
        );
        let json = json_of(&envelope)?;

        assert!(
            json.contains("\"key\":null,\"key_base64\":\"/wA=\""),
            "{json}"
        );
        assert!(!json.contains("\"key\":\""), "{json}");
        assert!(
            json.contains("{\"name\":\"raw\",\"value_base64\":\"/wA=\"}"),
            "{json}"
        );
        Ok(())
    }

    #[test]
    fn undecodable_value_keeps_bytes_as_base64() -> CoreResult<()> {
        let envelope = MessageEnvelope::from_record(
            "pedidos",
            0,
            7,
            None,
            None,
            EnvelopeValue::Undecodable {
                bytes: vec![0xff],
                error: "invalid JSON".to_string(),
            },
            &[],
        );
        let json = json_of(&envelope)?;

        assert!(json.contains("\"value\":null"), "{json}");
        assert!(json.contains("\"value_base64\":\"/w==\""), "{json}");
        assert!(json.contains("\"decode_error\":\"invalid JSON\""), "{json}");
        assert!(!json.contains("value_truncated"), "{json}");
        Ok(())
    }

    #[test]
    fn truncated_value_sets_flag() -> CoreResult<()> {
        let envelope = MessageEnvelope::from_record(
            "pedidos",
            0,
            7,
            None,
            None,
            EnvelopeValue::Truncated {
                prefix: b"{\"a\":".to_vec(),
            },
            &[],
        );
        let json = json_of(&envelope)?;

        assert!(json.contains("\"value_truncated\":true"), "{json}");
        assert!(json.contains("\"value_base64\":\"eyJhIjo=\""), "{json}");
        assert!(json.contains("\"value\":null"), "{json}");
        Ok(())
    }

    #[test]
    fn round_trip_preserves_key_value_and_headers() -> CoreResult<()> {
        let original_key = vec![0xff, 0x00];
        let original_headers = vec![
            RecordHeader {
                name: "sem-valor".to_string(),
                value: None,
            },
            RecordHeader {
                name: "binario".to_string(),
                value: Some(vec![0x00, 0xfe]),
            },
            text_header("texto", "ok"),
        ];
        let envelope = MessageEnvelope::from_record(
            "pedidos",
            3,
            99,
            Some(1_794_875_047_253),
            Some(&original_key),
            EnvelopeValue::Decoded(serde_json::json!({"valor": true})),
            &original_headers,
        );
        let line = json_of(&envelope)?;

        let parsed = parse_envelope_line(&line)?;

        assert_eq!(parsed.key_bytes()?, Some(original_key));
        assert_eq!(parsed.record_headers()?, original_headers);
        assert_eq!(parsed, envelope);
        Ok(())
    }

    #[test]
    fn rejects_unknown_field() {
        let error = parse_envelope_line(r#"{"kafcli_envelope":1,"extra":true}"#).err();

        assert!(
            matches!(error, Some(CoreError::InvalidInput(ref message)) if message.contains("unknown envelope field"))
        );
    }

    #[test]
    fn rejects_other_envelope_version() {
        let error = parse_envelope_line(r#"{"kafcli_envelope":2}"#).err();

        assert!(
            matches!(error, Some(CoreError::InvalidInput(ref message)) if message == "unsupported kafcli_envelope version 2")
        );
    }

    #[test]
    fn invalid_line_error_describes_position_without_value() {
        let error = parse_envelope_line(r#"{"kafcli_envelope":1,"partition":"segredo-123"}"#).err();

        let message = match error {
            Some(CoreError::InvalidInput(message)) => message,
            other => format!("unexpected: {other:?}"),
        };
        assert!(message.contains("line 1 column"), "{message}");
        assert!(!message.contains("segredo-123"), "{message}");
    }

    #[test]
    fn timestamp_is_rfc3339_utc_with_millis() {
        let envelope = MessageEnvelope::from_record(
            "pedidos",
            0,
            7,
            Some(1_794_875_047_253),
            None,
            EnvelopeValue::Absent,
            &[],
        );

        assert_eq!(
            envelope.timestamp.as_deref(),
            Some("2026-11-17T00:24:07.253Z")
        );
    }

    #[test]
    fn timestamp_keeps_three_digits_when_millis_are_round() {
        let envelope = MessageEnvelope::from_record(
            "pedidos",
            0,
            7,
            Some(1_794_875_047_250),
            None,
            EnvelopeValue::Absent,
            &[],
        );

        assert_eq!(
            envelope.timestamp.as_deref(),
            Some("2026-11-17T00:24:07.250Z")
        );
    }

    #[test]
    fn absent_key_timestamp_and_value_serialize_as_null() -> CoreResult<()> {
        let envelope =
            MessageEnvelope::from_record("pedidos", 0, 7, None, None, EnvelopeValue::Absent, &[]);
        let json = json_of(&envelope)?;

        assert!(json.contains("\"timestamp\":null"), "{json}");
        assert!(json.contains("\"key\":null"), "{json}");
        assert!(json.contains("\"value\":null"), "{json}");
        assert!(json.contains("\"headers\":[]"), "{json}");
        Ok(())
    }

    #[test]
    fn user_envelope_without_position_parses_with_nulls() -> CoreResult<()> {
        let envelope = parse_envelope_line(r#"{"kafcli_envelope":1,"value":{"a":1}}"#)?;

        assert_eq!(envelope.topic, None);
        assert_eq!(envelope.partition, None);
        assert_eq!(envelope.offset, None);
        assert_eq!(envelope.timestamp, None);
        assert_eq!(envelope.key_bytes()?, None);
        Ok(())
    }
}
