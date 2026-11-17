use super::json_schema_validator::JsonSchemaValidator;
use super::raw_json_codec::RawJsonCodec;
use super::value_codec::{EncodedValue, ValueDecoder, ValueEncoder};
use crate::error::CoreResult;
use crate::message::record_header::RecordHeader;

/// JSON codec that validates values against a schema before encoding them.
///
/// Encoding produces the same bytes as [`RawJsonCodec`]; decoding does not validate.
#[derive(Debug)]
pub struct SchemaValidatedJsonCodec {
    validator: JsonSchemaValidator,
}

impl SchemaValidatedJsonCodec {
    pub fn new(validator: JsonSchemaValidator) -> Self {
        Self { validator }
    }
}

impl ValueEncoder for SchemaValidatedJsonCodec {
    fn encode_value(&self, value: &serde_json::Value) -> CoreResult<EncodedValue> {
        self.validator.validate_value(value)?;
        RawJsonCodec.encode_value(value)
    }
}

impl ValueDecoder for SchemaValidatedJsonCodec {
    fn decode_value(
        &self,
        bytes: &[u8],
        headers: &[RecordHeader],
    ) -> Result<serde_json::Value, String> {
        RawJsonCodec.decode_value(bytes, headers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::json_schema_validator::JsonSchemaValidator;
    use crate::codec::raw_json_codec::RawJsonCodec;
    use crate::error::CoreError;
    use serde_json::json;

    fn codec() -> SchemaValidatedJsonCodec {
        let validator = JsonSchemaValidator::parse(
            r#"{"type": "object", "required": ["id"], "properties": {"id": {"type": "integer"}}}"#,
        )
        .expect("schema parses");
        SchemaValidatedJsonCodec::new(validator)
    }

    #[test]
    fn codec_encodes_same_bytes_as_raw_json() {
        let value = json!({"id": 7, "tags": ["a", "b"]});

        let validated = codec().encode_value(&value).expect("valid value encodes");
        let raw = RawJsonCodec.encode_value(&value).expect("raw encode");

        assert_eq!(validated, raw);
    }

    #[test]
    fn encode_rejects_value_that_violates_schema() {
        let error = codec()
            .encode_value(&json!({"id": "seven"}))
            .expect_err("string id must not encode");

        assert!(
            matches!(error, CoreError::SchemaValidation { .. }),
            "{error:?}"
        );
    }

    #[test]
    fn decode_does_not_validate() {
        let decoded = codec()
            .decode_value(br#"{"id": "seven"}"#, &[])
            .expect("decode matches raw JSON behavior");

        assert_eq!(decoded, json!({"id": "seven"}));
    }
}
