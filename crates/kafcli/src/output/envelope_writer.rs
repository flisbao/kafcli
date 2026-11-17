use crate::output::terminal_text::sanitize_for_terminal;
use kafcli_core::message::binary_text::decode_base64;
use kafcli_core::message::message_envelope::MessageEnvelope;
use kafcli_core::ops::consume::message_consumer::EnvelopeSink;
use std::fmt::Display;
use std::io::{self, Write};

/// Longest key or value cell of the table, the ellipsis included.
const MAXIMUM_CELL_CHARACTERS: usize = 80;
const ELLIPSIS: char = '…';
const EMPTY_TIMESTAMP: &str = "-";
const NULL_CELL: &str = "<null>";

/// Writes each envelope as one compact JSON line and flushes it, so a reader sees messages as they arrive.
pub struct NdjsonEnvelopeWriter<W: Write> {
    writer: W,
}

impl<W: Write> NdjsonEnvelopeWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }
}

impl<W: Write> EnvelopeSink for NdjsonEnvelopeWriter<W> {
    fn accept_envelope(&mut self, envelope: &MessageEnvelope) -> io::Result<()> {
        let line = serde_json::to_string(envelope).map_err(io::Error::other)?;
        writeln!(self.writer, "{line}")?;
        self.writer.flush()
    }
}

/// Writes each envelope as one table line: `partition  offset  timestamp  key  value`.
///
/// Key and value come from the broker or the producer, so they are sanitized and cut to 80 characters.
pub struct TableEnvelopeWriter<W: Write> {
    writer: W,
}

impl<W: Write> TableEnvelopeWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }
}

impl<W: Write> EnvelopeSink for TableEnvelopeWriter<W> {
    fn accept_envelope(&mut self, envelope: &MessageEnvelope) -> io::Result<()> {
        writeln!(self.writer, "{}", render_table_line(envelope))?;
        self.writer.flush()
    }
}

/// Builds the table line of one envelope without a trailing newline.
pub fn render_table_line(envelope: &MessageEnvelope) -> String {
    format!(
        "{}  {}  {}  {}  {}",
        optional_cell(envelope.partition),
        optional_cell(envelope.offset),
        envelope
            .timestamp
            .as_deref()
            .map_or_else(|| EMPTY_TIMESTAMP.to_string(), cut_cell),
        render_key_cell(envelope),
        render_value_cell(envelope),
    )
}

fn optional_cell<T: Display>(value: Option<T>) -> String {
    value.map_or_else(|| EMPTY_TIMESTAMP.to_string(), |value| value.to_string())
}

fn render_key_cell(envelope: &MessageEnvelope) -> String {
    match (&envelope.key, &envelope.key_base64) {
        (Some(text), _) => cut_cell(&sanitize_for_terminal(text)),
        (None, Some(encoded)) => cut_cell(&sanitize_for_terminal(&format!("<base64: {encoded}>"))),
        (None, None) => NULL_CELL.to_string(),
    }
}

/// Decoded JSON is shown compact. A value kept as bytes says why it has no JSON text.
fn render_value_cell(envelope: &MessageEnvelope) -> String {
    if let Some(value) = &envelope.value {
        return cut_cell(&sanitize_for_terminal(&value.to_string()));
    }
    if envelope.value_truncated {
        let shown = envelope
            .value_base64
            .as_deref()
            .and_then(|encoded| decode_base64(encoded).ok())
            .map_or(0, |bytes| bytes.len());
        return format!("<truncated: {shown} bytes shown>");
    }
    if envelope.value_base64.is_some() {
        let reason = envelope.decode_error.as_deref().unwrap_or("unknown error");
        return cut_cell(&sanitize_for_terminal(&format!("<decode error: {reason}>")));
    }
    if envelope.decode_error.is_some() {
        return "<withheld: --redact>".to_string();
    }
    NULL_CELL.to_string()
}

/// Sanitizes nothing itself: callers pass text that is already safe or sanitize it first.
fn cut_cell(text: &str) -> String {
    if text.chars().count() <= MAXIMUM_CELL_CHARACTERS {
        return text.to_string();
    }
    let mut shortened: String = text.chars().take(MAXIMUM_CELL_CHARACTERS - 1).collect();
    shortened.push(ELLIPSIS);
    shortened
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafcli_core::message::message_envelope::EnvelopeValue;
    use kafcli_core::message::record_header::RecordHeader;

    fn envelope_with(key: Option<&[u8]>, value: EnvelopeValue) -> MessageEnvelope {
        MessageEnvelope::from_record(
            "pedidos",
            1,
            42,
            Some(1_794_875_047_253),
            key,
            value,
            &[] as &[RecordHeader],
        )
    }

    #[test]
    fn ndjson_writes_one_compact_line_per_envelope() -> io::Result<()> {
        let mut output = Vec::new();
        {
            let mut writer = NdjsonEnvelopeWriter::new(&mut output);
            writer.accept_envelope(&envelope_with(
                Some(b"k"),
                EnvelopeValue::Decoded(serde_json::json!({"id": 1})),
            ))?;
            writer.accept_envelope(&envelope_with(None, EnvelopeValue::Absent))?;
        }

        let text = String::from_utf8_lossy(&output).to_string();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("{\"kafcli_envelope\":1,\"topic\":\"pedidos\""));
        assert!(lines[0].contains("\"value\":{\"id\":1}"));
        Ok(())
    }

    #[test]
    fn table_line_shows_decoded_value_and_key() {
        let line = render_table_line(&envelope_with(
            Some(b"pedido-1"),
            EnvelopeValue::Decoded(serde_json::json!({"id": 1})),
        ));

        assert_eq!(
            line,
            "1  42  2026-11-17T00:24:07.253Z  pedido-1  {\"id\":1}"
        );
    }

    #[test]
    fn table_marks_tombstone_as_null() {
        let line = render_table_line(&envelope_with(None, EnvelopeValue::Absent));

        assert!(line.ends_with("  <null>  <null>"), "{line}");
    }

    #[test]
    fn table_marks_withheld_value() {
        let line = render_table_line(&envelope_with(
            None,
            EnvelopeValue::Withheld {
                reason: "value withheld".to_string(),
            },
        ));

        assert!(line.ends_with("  <null>  <withheld: --redact>"), "{line}");
    }

    #[test]
    fn table_marks_undecodable_value_with_its_error() {
        let line = render_table_line(&envelope_with(
            None,
            EnvelopeValue::Undecodable {
                bytes: b"not json".to_vec(),
                error: "invalid JSON".to_string(),
            },
        ));

        assert!(
            line.ends_with("  <null>  <decode error: invalid JSON>"),
            "{line}"
        );
    }

    #[test]
    fn table_marks_truncated_value_with_bytes_shown() {
        let line = render_table_line(&envelope_with(
            None,
            EnvelopeValue::Truncated {
                prefix: b"{\"a\":".to_vec(),
            },
        ));

        assert!(
            line.ends_with("  <null>  <truncated: 5 bytes shown>"),
            "{line}"
        );
    }

    #[test]
    fn table_neutralizes_control_characters_in_key() {
        let line = render_table_line(&envelope_with(Some(b"\x1b[31mred"), EnvelopeValue::Absent));

        assert!(!line.contains('\x1b'), "{line}");
        assert!(line.contains("?[31mred"), "{line}");
    }

    #[test]
    fn long_key_is_cut_to_eighty_characters_with_ellipsis() {
        let key = "k".repeat(100);
        let line = render_table_line(&envelope_with(Some(key.as_bytes()), EnvelopeValue::Absent));

        let expected = format!("{}{ELLIPSIS}", "k".repeat(79));
        assert!(line.contains(&format!("  {expected}  <null>")), "{line}");
    }

    #[test]
    fn short_text_is_not_cut() {
        assert_eq!(cut_cell("abc"), "abc");
        assert_eq!(cut_cell(&"a".repeat(80)).chars().count(), 80);
    }
}
