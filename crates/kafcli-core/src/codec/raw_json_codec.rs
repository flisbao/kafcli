use super::value_codec::{EncodedValue, ValueDecoder, ValueEncoder};
use crate::error::{CoreError, CoreResult};
use crate::message::record_header::RecordHeader;

/// Codec for values that are plain JSON text with no extra headers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RawJsonCodec;

impl ValueEncoder for RawJsonCodec {
    fn encode_value(&self, value: &serde_json::Value) -> CoreResult<EncodedValue> {
        let bytes = serde_json::to_vec(value).map_err(|_| {
            CoreError::InvalidInput("value could not be serialized as JSON".to_string())
        })?;
        Ok(EncodedValue {
            bytes,
            headers: Vec::new(),
        })
    }
}

impl ValueDecoder for RawJsonCodec {
    fn decode_value(
        &self,
        bytes: &[u8],
        _headers: &[RecordHeader],
    ) -> Result<serde_json::Value, String> {
        // serde_json reports invalid UTF-8 as a parse error with a position, so one message covers both.
        serde_json::from_slice::<serde_json::Value>(bytes).map_err(|error| {
            format!(
                "value is not valid JSON: line {} column {}",
                error.line(),
                error.column()
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_json_round_trip() {
        let codec = RawJsonCodec;
        let value = serde_json::json!({"id": 7, "tags": ["a", "b"], "ok": true});

        let encoded = codec.encode_value(&value).expect("encode");
        assert!(encoded.headers.is_empty());
        assert_eq!(
            encoded.bytes,
            serde_json::to_vec(&value).expect("compact JSON")
        );
        assert!(!encoded.bytes.contains(&b' '), "encoding must be compact");

        let decoded = codec
            .decode_value(&encoded.bytes, &encoded.headers)
            .expect("decode");
        assert_eq!(decoded, value);
    }

    #[test]
    fn invalid_json_reports_position_without_content() {
        let codec = RawJsonCodec;

        let error = codec
            .decode_value(br#"{"cpf":"123""#, &[])
            .expect_err("truncated JSON must fail");

        assert!(
            error.starts_with("value is not valid JSON: line "),
            "{error}"
        );
        assert!(!error.contains("123"), "error leaked payload: {error}");
        assert!(!error.contains("cpf"), "error leaked payload: {error}");
    }

    #[test]
    fn non_utf8_is_decode_error() {
        let codec = RawJsonCodec;

        let error = codec
            .decode_value(&[b'{', b'"', b'a', b'"', b':', b'"', 0xff, b'"', b'}'], &[])
            .expect_err("invalid UTF-8 must fail");

        assert!(
            error.starts_with("value is not valid JSON: line "),
            "{error}"
        );
    }
}
