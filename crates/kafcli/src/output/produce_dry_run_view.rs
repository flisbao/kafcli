use crate::output::renderer::TableRenderable;
use crate::output::terminal_text::sanitize_for_terminal;
use comfy_table::presets::UTF8_FULL_CONDENSED;
use comfy_table::{Cell, Table};
use kafcli_core::message::binary_text::encode_base64;
use kafcli_core::message::message_envelope::EnvelopeHeader;
use kafcli_core::message::record_header::RecordHeader;
use kafcli_core::ops::produce::message_producer::PreparedMessage;
use serde::Serialize;
use std::fmt::Write;

/// Messages shown by a dry run, in both table and JSON output.
pub const MAXIMUM_PREVIEWED_MESSAGES: usize = 1_000;
/// Leading bytes of each value shown as hex.
const HEX_PREVIEW_BYTES: usize = 32;

/// What a dry run shows: the messages that a real run would send, and nothing is sent.
#[derive(Serialize)]
pub struct DryRunView {
    pub dry_run: bool,
    /// Registry id of the schema the values were encoded with; `null` without a registry schema.
    pub schema_id: Option<String>,
    pub messages: Vec<DryRunMessage>,
    pub messages_truncated: bool,
}

/// One prepared message. Key and header bytes follow the envelope rules: UTF-8 text, or base64 when
/// the bytes are not UTF-8. `value_bytes` and `value_hex` are `null` for a tombstone.
#[derive(Serialize)]
pub struct DryRunMessage {
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_base64: Option<String>,
    pub headers: Vec<EnvelopeHeader>,
    pub value_bytes: Option<usize>,
    pub value_hex: Option<String>,
}

impl DryRunView {
    pub fn from_messages(messages: &[PreparedMessage], schema_id: Option<String>) -> Self {
        Self {
            dry_run: true,
            schema_id,
            messages: messages
                .iter()
                .take(MAXIMUM_PREVIEWED_MESSAGES)
                .map(DryRunMessage::from_prepared)
                .collect(),
            messages_truncated: messages.len() > MAXIMUM_PREVIEWED_MESSAGES,
        }
    }
}

impl DryRunMessage {
    fn from_prepared(message: &PreparedMessage) -> Self {
        let (key, key_base64) = split_text_or_base64(message.key.as_deref());
        Self {
            key,
            key_base64,
            headers: message.headers.iter().map(envelope_header).collect(),
            value_bytes: message.value.as_ref().map(Vec::len),
            value_hex: message
                .value
                .as_deref()
                .map(|bytes| hex_prefix(bytes, HEX_PREVIEW_BYTES)),
        }
    }
}

fn split_text_or_base64(bytes: Option<&[u8]>) -> (Option<String>, Option<String>) {
    match bytes {
        None => (None, None),
        Some(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => (Some(text.to_string()), None),
            Err(_) => (None, Some(encode_base64(bytes))),
        },
    }
}

fn envelope_header(header: &RecordHeader) -> EnvelopeHeader {
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

/// Lowercase hex of the first `limit` bytes, without separators.
fn hex_prefix(bytes: &[u8], limit: usize) -> String {
    let mut hex = String::with_capacity(limit.saturating_mul(2));
    for byte in bytes.iter().take(limit) {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

impl TableRenderable for DryRunView {
    fn render_table(&self) -> String {
        let mut table = Table::new();
        *table.style_mut() = UTF8_FULL_CONDENSED;
        table.set_header(vec![
            Cell::new("#"),
            Cell::new("KEY"),
            Cell::new("HEADERS"),
            Cell::new("BYTES"),
            Cell::new(format!("HEX (first {HEX_PREVIEW_BYTES} bytes)")),
        ]);
        for (index, message) in self.messages.iter().enumerate() {
            table.add_row(vec![
                Cell::new(index + 1),
                Cell::new(table_key(message)),
                Cell::new(table_headers(&message.headers)),
                Cell::new(
                    message
                        .value_bytes
                        .map_or("-".to_string(), |bytes| bytes.to_string()),
                ),
                Cell::new(message.value_hex.as_deref().unwrap_or("-")),
            ]);
        }

        let mut rendered = String::from("dry run: nothing was sent");
        rendered.push('\n');
        if let Some(schema_id) = &self.schema_id {
            rendered.push_str(&format!(
                "schema id: {}\n",
                sanitize_for_terminal(schema_id)
            ));
        }
        rendered.push_str(&table.to_string());
        if self.messages_truncated {
            rendered.push_str(&format!(
                "\nshowing the first {MAXIMUM_PREVIEWED_MESSAGES} messages only"
            ));
        }
        rendered
    }
}

fn table_key(message: &DryRunMessage) -> String {
    match (&message.key, &message.key_base64) {
        (Some(text), _) => sanitize_for_terminal(text),
        (None, Some(encoded)) => format!("base64:{}", sanitize_for_terminal(encoded)),
        (None, None) => "-".to_string(),
    }
}

fn table_headers(headers: &[EnvelopeHeader]) -> String {
    if headers.is_empty() {
        return "-".to_string();
    }
    headers
        .iter()
        .map(|header| match header {
            EnvelopeHeader::Text {
                name,
                value: Some(text),
            } => {
                format!(
                    "{}={}",
                    sanitize_for_terminal(name),
                    sanitize_for_terminal(text)
                )
            }
            EnvelopeHeader::Text { name, value: None } => sanitize_for_terminal(name),
            EnvelopeHeader::Binary { name, value_base64 } => format!(
                "{}=base64:{}",
                sanitize_for_terminal(name),
                sanitize_for_terminal(value_base64)
            ),
        })
        .collect::<Vec<String>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(key: Option<&[u8]>, value: Option<&[u8]>) -> PreparedMessage {
        PreparedMessage {
            key: key.map(<[u8]>::to_vec),
            value: value.map(<[u8]>::to_vec),
            headers: Vec::new(),
            partition: None,
        }
    }

    #[test]
    fn hex_is_lowercase_first_32_bytes_without_separators() {
        let value = vec![0xAB; 40];
        let view = DryRunView::from_messages(&[message(None, Some(&value))], None);

        let hex = view.messages[0].value_hex.as_deref().unwrap_or_default();
        assert_eq!(hex, "ab".repeat(32));
        assert_eq!(view.messages[0].value_bytes, Some(40));
    }

    #[test]
    fn short_value_shows_all_its_bytes_in_hex() {
        let view = DryRunView::from_messages(&[message(None, Some(br#"{"id":1}"#))], None);

        assert_eq!(
            view.messages[0].value_hex.as_deref(),
            Some("7b226964223a317d")
        );
    }

    #[test]
    fn non_utf8_key_is_shown_as_base64() {
        let view = DryRunView::from_messages(&[message(Some(&[0xff, 0x00]), Some(b"{}"))], None);

        let json = serde_json::to_value(&view).unwrap();
        assert_eq!(json["messages"][0]["key"], serde_json::Value::Null);
        assert_eq!(json["messages"][0]["key_base64"], "/wA=");
    }

    #[test]
    fn tombstone_has_null_value_fields() {
        let view = DryRunView::from_messages(&[message(Some(b"k"), None)], None);

        let json = serde_json::to_value(&view).unwrap();
        assert_eq!(json["messages"][0]["value_bytes"], serde_json::Value::Null);
        assert_eq!(json["messages"][0]["value_hex"], serde_json::Value::Null);
        assert!(json["messages"][0].get("key_base64").is_none());
    }

    #[test]
    fn json_has_dry_run_flag_and_truncation_flag() {
        let messages: Vec<PreparedMessage> = (0..MAXIMUM_PREVIEWED_MESSAGES + 1)
            .map(|_| message(None, Some(b"{}")))
            .collect();
        let value = serde_json::to_value(DryRunView::from_messages(&messages, None)).unwrap();

        assert_eq!(value["dry_run"], true);
        assert_eq!(value["messages_truncated"], true);
        assert_eq!(
            value["messages"].as_array().map(Vec::len),
            Some(MAXIMUM_PREVIEWED_MESSAGES)
        );
    }

    #[test]
    fn table_sanitizes_key_and_header_text() {
        let mut prepared = message(Some(b"\x1b[2Jkey"), Some(b"{}"));
        prepared.headers.push(RecordHeader {
            name: "h\x1b".to_string(),
            value: Some(b"v\x07".to_vec()),
        });

        let rendered = DryRunView::from_messages(&[prepared], None).render_table();

        assert!(!rendered.contains('\x1b'), "rendered: {rendered}");
        assert!(!rendered.contains('\x07'), "rendered: {rendered}");
        assert!(rendered.contains("?[2Jkey"), "rendered: {rendered}");
    }
}
