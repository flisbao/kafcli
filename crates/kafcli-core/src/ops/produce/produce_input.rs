use crate::error::{CoreError, CoreResult};
use crate::message::message_envelope::{MessageEnvelope, parse_envelope_line};
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, Read};
use std::path::PathBuf;

/// Largest input accepted from a file, stdin or the command line.
const MAXIMUM_INPUT_BYTES: u64 = 64 * 1024 * 1024;

/// Where the produce input text comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputSource {
    Value(String),
    File(PathBuf),
    Stdin,
}

/// How the input text is interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputMode {
    /// One JSON document, or NDJSON with one JSON value per line.
    Json,
    /// NDJSON with one message envelope per line.
    Envelope,
}

/// One unit of input. `line` is the 1-based line where the unit starts; a single JSON document is line 1.
#[derive(Debug, Clone, PartialEq)]
pub enum ProduceInput {
    Payload {
        value: Value,
        line: usize,
    },
    Envelope {
        envelope: MessageEnvelope,
        line: usize,
    },
}

/// Reads the whole input and splits it into produce units.
///
/// `reader` is used only for `InputSource::Stdin`. Inputs larger than 64 MiB are rejected.
pub fn read_produce_inputs(
    source: &InputSource,
    mode: InputMode,
    reader: &mut dyn BufRead,
) -> CoreResult<Vec<ProduceInput>> {
    let text = read_source_text(source, reader)?;
    match mode {
        InputMode::Json => parse_json_inputs(&text),
        InputMode::Envelope => parse_envelope_inputs(&text),
    }
}

fn read_source_text(source: &InputSource, reader: &mut dyn BufRead) -> CoreResult<String> {
    let bytes = match source {
        InputSource::Value(text) => read_limited(text.as_bytes())?,
        InputSource::File(path) => read_limited(File::open(path)?)?,
        InputSource::Stdin => read_limited(&mut *reader)?,
    };
    String::from_utf8(bytes)
        .map_err(|_| CoreError::InvalidInput("input is not valid UTF-8".to_string()))
}

/// Reads at most one byte past the limit, so an oversized input is detected without reading it all.
fn read_limited<R: Read>(reader: R) -> CoreResult<Vec<u8>> {
    crate::io_limits::read_at_most(reader, MAXIMUM_INPUT_BYTES)?
        .ok_or_else(|| CoreError::InvalidInput("input larger than 64 MiB".to_string()))
}

/// A text that parses as one JSON document is one value; otherwise each non-blank line is a value.
fn parse_json_inputs(text: &str) -> CoreResult<Vec<ProduceInput>> {
    if let Ok(value) = serde_json::from_str::<Value>(text) {
        return Ok(vec![ProduceInput::Payload { value, line: 1 }]);
    }
    let mut inputs = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        if line.trim().is_empty() {
            continue;
        }
        let value = serde_json::from_str::<Value>(line).map_err(|_| {
            CoreError::InvalidInput(format!("line {line_number}: value is not valid JSON"))
        })?;
        inputs.push(ProduceInput::Payload {
            value,
            line: line_number,
        });
    }
    Ok(inputs)
}

fn parse_envelope_inputs(text: &str) -> CoreResult<Vec<ProduceInput>> {
    let mut inputs = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        if line.trim().is_empty() {
            continue;
        }
        let envelope = parse_envelope_line(line).map_err(|error| match error {
            CoreError::InvalidInput(message) => {
                CoreError::InvalidInput(format!("envelope line {line_number}: {message}"))
            }
            other => other,
        })?;
        if envelope.value_truncated {
            return Err(CoreError::InvalidInput(format!(
                "envelope line {line_number}: value was truncated by --max-bytes; consume again with --max-bytes 0"
            )));
        }
        if envelope.decode_error.is_some() && envelope.value_base64.is_none() {
            return Err(CoreError::InvalidInput(format!(
                "envelope line {line_number}: value was withheld by --redact; consume again without --redact"
            )));
        }
        inputs.push(ProduceInput::Envelope {
            envelope,
            line: line_number,
        });
    }
    Ok(inputs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor};

    fn read_json(text: &str) -> CoreResult<Vec<ProduceInput>> {
        read_produce_inputs(
            &InputSource::Value(text.to_string()),
            InputMode::Json,
            &mut Cursor::new(Vec::new()),
        )
    }

    fn read_envelope(text: &str) -> CoreResult<Vec<ProduceInput>> {
        read_produce_inputs(
            &InputSource::Value(text.to_string()),
            InputMode::Envelope,
            &mut Cursor::new(Vec::new()),
        )
    }

    #[test]
    fn withheld_envelope_value_is_rejected_with_the_line_number() {
        let text = r#"{"kafcli_envelope":1,"topic":"t","partition":0,"offset":1,"timestamp":null,"key":null,"value":null,"decode_error":"value withheld: --redact cannot be applied to a truncated or undecodable value","headers":[]}"#;

        let message = error_message(read_envelope(text));

        assert_eq!(
            message,
            "invalid input: envelope line 1: value was withheld by --redact; consume again without --redact"
        );
    }

    fn error_message(result: CoreResult<Vec<ProduceInput>>) -> String {
        match result {
            Ok(inputs) => format!("no error, {} inputs", inputs.len()),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn single_json_document_is_one_input() -> CoreResult<()> {
        let inputs = read_json("{\n  \"id\": 1,\n  \"items\": [1, 2]\n}")?;

        assert_eq!(
            inputs,
            vec![ProduceInput::Payload {
                value: serde_json::json!({"id": 1, "items": [1, 2]}),
                line: 1,
            }]
        );
        Ok(())
    }

    #[test]
    fn ndjson_lines_are_separate_inputs_and_blank_lines_skipped() -> CoreResult<()> {
        let inputs = read_json("{\"id\":1}\n\n   \n{\"id\":2}\n")?;

        assert_eq!(
            inputs,
            vec![
                ProduceInput::Payload {
                    value: serde_json::json!({"id": 1}),
                    line: 1,
                },
                ProduceInput::Payload {
                    value: serde_json::json!({"id": 2}),
                    line: 4,
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn invalid_ndjson_line_reports_line_number() {
        let message = error_message(read_json("{\"id\":1}\n{\"id\":\n{\"id\":3}"));

        assert_eq!(message, "invalid input: line 2: value is not valid JSON");
    }

    #[test]
    fn truncated_envelope_line_is_rejected() {
        let message = error_message(read_envelope(
            "{\"kafcli_envelope\":1,\"value\":{\"a\":1}}\n{\"kafcli_envelope\":1,\"value_base64\":\"e30=\",\"value_truncated\":true}",
        ));

        assert_eq!(
            message,
            "invalid input: envelope line 2: value was truncated by --max-bytes; consume again with --max-bytes 0"
        );
    }

    #[test]
    fn envelope_mode_parses_each_line() -> CoreResult<()> {
        let inputs = read_envelope("{\"kafcli_envelope\":1,\"value\":{\"a\":1}}")?;

        assert!(matches!(
            inputs.as_slice(),
            [ProduceInput::Envelope { line: 1, .. }]
        ));
        Ok(())
    }

    #[test]
    fn input_larger_than_limit_is_rejected() {
        let mut oversized = BufReader::new(std::io::repeat(b' ').take(MAXIMUM_INPUT_BYTES + 1));

        let message = error_message(read_produce_inputs(
            &InputSource::Stdin,
            InputMode::Json,
            &mut oversized,
        ));

        assert_eq!(message, "invalid input: input larger than 64 MiB");
    }
}
