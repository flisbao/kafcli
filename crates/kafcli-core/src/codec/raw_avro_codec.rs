use super::avro_schema::AvroSchema;
use super::value_codec::{EncodedValue, ValueDecoder, ValueEncoder};
use crate::error::CoreResult;
use crate::message::record_header::RecordHeader;

/// Codec for values that are plain Avro binary datums, with no wire headers.
#[derive(Debug, Clone)]
pub struct RawAvroCodec {
    schema: AvroSchema,
}

impl RawAvroCodec {
    pub fn new(schema: AvroSchema) -> Self {
        Self { schema }
    }
}

impl ValueEncoder for RawAvroCodec {
    fn encode_value(&self, value: &serde_json::Value) -> CoreResult<EncodedValue> {
        let bytes = self.schema.encode_json(value)?;
        Ok(EncodedValue {
            bytes,
            headers: Vec::new(),
        })
    }
}

impl ValueDecoder for RawAvroCodec {
    fn decode_value(
        &self,
        bytes: &[u8],
        _headers: &[RecordHeader],
    ) -> Result<serde_json::Value, String> {
        self.schema.decode_to_json(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAIR: &str = r#"{"type":"record","name":"Pair","fields":[
        {"name":"a","type":"int"},{"name":"b","type":"string"}]}"#;

    fn codec() -> RawAvroCodec {
        RawAvroCodec::new(AvroSchema::parse(PAIR).expect("schema must parse"))
    }

    #[test]
    fn raw_avro_round_trip_has_no_headers() {
        let codec = codec();
        let value = serde_json::json!({"a": 1, "b": "x"});

        let encoded = codec.encode_value(&value).expect("encode");
        assert!(encoded.headers.is_empty());
        assert_eq!(encoded.bytes, vec![0x02, 0x02, 0x78]);

        let decoded = codec
            .decode_value(&encoded.bytes, &encoded.headers)
            .expect("decode");
        assert_eq!(decoded, value);
    }

    #[test]
    fn raw_avro_decode_failure_is_text_without_payload() {
        let codec = codec();

        let error = codec
            .decode_value(&[0xff, 0xff, 0xff], &[])
            .expect_err("garbage must fail");

        assert_eq!(error, "value is not valid Avro for the schema");
    }
}
