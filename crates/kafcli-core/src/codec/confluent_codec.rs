//! Codecs for the Confluent wire format: encoders take an already-fetched schema, the decoder
//! owns the registry and a bounded cache.

use super::avro_schema::AvroSchema;
use super::confluent_wire::{frame_confluent_payload, split_confluent_frame};
use super::json_schema_validator::JsonSchemaValidator;
use super::raw_json_codec::RawJsonCodec;
use super::schema_cache::{ParsedSchema, SCHEMA_CACHE_CAPACITY, SchemaCache};
use super::value_codec::{EncodedValue, ValueDecoder, ValueEncoder};
use crate::error::CoreResult;
use crate::message::record_header::RecordHeader;
use crate::registry::registered_schema::{SchemaId, SchemaKind};
use crate::registry::schema_registry::SchemaRegistry;

/// Encodes values as Avro binary behind the Confluent prefix.
pub struct ConfluentAvroEncoder {
    schema_id: u32,
    schema: AvroSchema,
}

impl ConfluentAvroEncoder {
    pub fn new(schema_id: u32, schema: AvroSchema) -> Self {
        Self { schema_id, schema }
    }
}

impl ValueEncoder for ConfluentAvroEncoder {
    fn encode_value(&self, value: &serde_json::Value) -> CoreResult<EncodedValue> {
        let body = self.schema.encode_json(value)?;
        Ok(EncodedValue {
            bytes: frame_confluent_payload(self.schema_id, &body),
            headers: Vec::new(),
        })
    }
}

/// Encodes values as compact JSON behind the Confluent prefix, validating first when a validator is given.
pub struct ConfluentJsonEncoder {
    schema_id: u32,
    validator: Option<JsonSchemaValidator>,
}

impl ConfluentJsonEncoder {
    /// `validator` is `None` when the user passed `--no-validate`.
    pub fn new(schema_id: u32, validator: Option<JsonSchemaValidator>) -> Self {
        Self {
            schema_id,
            validator,
        }
    }
}

impl ValueEncoder for ConfluentJsonEncoder {
    fn encode_value(&self, value: &serde_json::Value) -> CoreResult<EncodedValue> {
        if let Some(validator) = &self.validator {
            validator.validate_value(value)?;
        }
        let body = RawJsonCodec.encode_value(value)?.bytes;
        Ok(EncodedValue {
            bytes: frame_confluent_payload(self.schema_id, &body),
            headers: Vec::new(),
        })
    }
}

/// Decodes Confluent-framed values, fetching each schema id at most once per decoder.
pub struct ConfluentDecoder {
    registry: Box<dyn SchemaRegistry>,
    expected_kind: SchemaKind,
    cache: SchemaCache,
}

impl ConfluentDecoder {
    pub fn new(registry: Box<dyn SchemaRegistry>, expected_kind: SchemaKind) -> Self {
        Self {
            registry,
            expected_kind,
            cache: SchemaCache::new(SCHEMA_CACHE_CAPACITY),
        }
    }
}

impl ValueDecoder for ConfluentDecoder {
    fn decode_value(
        &self,
        bytes: &[u8],
        _headers: &[RecordHeader],
    ) -> Result<serde_json::Value, String> {
        let (schema_id, body) = split_confluent_frame(bytes)?;
        let schema = self
            .cache
            .fetch_schema(&SchemaId::Confluent(schema_id), self.registry.as_ref())?;
        match (self.expected_kind, schema.as_ref()) {
            (SchemaKind::Avro, ParsedSchema::Avro(avro)) => avro.decode_to_json(body),
            (SchemaKind::Json, ParsedSchema::Json) => RawJsonCodec.decode_value(body, &[]),
            (SchemaKind::Avro, ParsedSchema::Json) => Err(format!(
                "schema id {schema_id} is a JSON Schema, but --format avro was given"
            )),
            (SchemaKind::Json, ParsedSchema::Avro(_)) => Err(format!(
                "schema id {schema_id} is an Avro schema, but --format json was given"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::schema_registry_model::RegistryAuthConfig;
    use crate::registry::confluent_registry::ConfluentSchemaRegistry;
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
    use crate::registry::registry_http_client::RegistryHttpClient;
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use serde_json::json;
    use std::time::Duration;

    const PAIR: &str = r#"{"type":"record","name":"Pair","fields":[
        {"name":"a","type":"int"},{"name":"b","type":"string"}]}"#;
    const OBJECT: &str =
        r#"{"type":"object","required":["id"],"properties":{"id":{"type":"integer"}}}"#;

    fn route(id: u32, schema: &str, schema_type: Option<&str>) -> FakeRoute {
        let body = match schema_type {
            Some(kind) => json!({"schema": schema, "schemaType": kind}),
            None => json!({"schema": schema}),
        };
        FakeRoute {
            method: "GET".into(),
            path_and_query: format!("/schemas/ids/{id}"),
            status: 200,
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    fn confluent_registry(server: &FakeRegistryServer) -> Box<dyn SchemaRegistry> {
        let http = RegistryHttpClient::new(
            &server.url(),
            &RegistryAuthConfig::Anonymous,
            None,
            &FixedSecretResolver::returning("unused"),
            Duration::from_secs(5),
        )
        .expect("client builds");
        Box::new(ConfluentSchemaRegistry::new(http))
    }

    #[test]
    fn confluent_avro_round_trip_through_fake_registry() {
        let server = FakeRegistryServer::start(vec![route(7, PAIR, None)]).expect("server starts");
        let schema = AvroSchema::parse(PAIR).expect("schema parses");
        let value = json!({"a": 1, "b": "x"});

        let encoded = ConfluentAvroEncoder::new(7, schema)
            .encode_value(&value)
            .expect("encode");
        assert_eq!(encoded.bytes[..5], [0, 0, 0, 0, 7]);
        assert!(encoded.headers.is_empty());

        let decoder = ConfluentDecoder::new(confluent_registry(&server), SchemaKind::Avro);
        let decoded = decoder
            .decode_value(&encoded.bytes, &encoded.headers)
            .expect("decode");
        assert_eq!(decoded, value);
    }

    #[test]
    fn confluent_json_round_trip() {
        let server =
            FakeRegistryServer::start(vec![route(9, OBJECT, Some("JSON"))]).expect("server starts");
        let validator = JsonSchemaValidator::parse(OBJECT).expect("schema parses");
        let value = json!({"id": 3, "extra": ["a"]});

        let encoded = ConfluentJsonEncoder::new(9, Some(validator))
            .encode_value(&value)
            .expect("encode");
        assert_eq!(encoded.bytes[..5], [0, 0, 0, 0, 9]);
        assert_eq!(
            &encoded.bytes[5..],
            serde_json::to_vec(&value).expect("json").as_slice()
        );

        let decoder = ConfluentDecoder::new(confluent_registry(&server), SchemaKind::Json);
        let decoded = decoder
            .decode_value(&encoded.bytes, &encoded.headers)
            .expect("decode");
        assert_eq!(decoded, value);
    }

    #[test]
    fn json_encoder_without_validator_skips_validation() {
        let encoded = ConfluentJsonEncoder::new(9, None)
            .encode_value(&json!({"id": "not checked"}))
            .expect("encode without validator");

        assert_eq!(encoded.bytes[..5], [0, 0, 0, 0, 9]);
    }

    #[test]
    fn decoder_fetches_each_schema_once() {
        let server = FakeRegistryServer::start(vec![route(7, PAIR, None)]).expect("server starts");
        let encoder = ConfluentAvroEncoder::new(7, AvroSchema::parse(PAIR).expect("parses"));
        let decoder = ConfluentDecoder::new(confluent_registry(&server), SchemaKind::Avro);

        for number in 0..3 {
            let encoded = encoder
                .encode_value(&json!({"a": number, "b": "x"}))
                .expect("encode");
            decoder
                .decode_value(&encoded.bytes, &encoded.headers)
                .expect("decode");
        }

        assert_eq!(server.recorded_requests().len(), 1);
    }

    #[test]
    fn unknown_schema_id_is_fetched_once_and_each_message_gets_decode_error() {
        let server = FakeRegistryServer::start(vec![FakeRoute {
            method: "GET".into(),
            path_and_query: "/schemas/ids/99".into(),
            status: 404,
            headers: Vec::new(),
            body: r#"{"error_code":40403,"message":"Schema not found"}"#.into(),
        }])
        .expect("server starts");
        let decoder = ConfluentDecoder::new(confluent_registry(&server), SchemaKind::Avro);
        let frame = frame_confluent_payload(99, &[0x00]);

        for _ in 0..1000 {
            let error = decoder
                .decode_value(&frame, &[])
                .expect_err("unknown id must fail");
            assert_eq!(
                error,
                "schema id 99 could not be fetched: schema registry error: schema id 99 not found"
            );
        }

        assert_eq!(server.recorded_requests().len(), 1);
    }

    #[test]
    fn kind_mismatch_is_decode_error() {
        let server =
            FakeRegistryServer::start(vec![route(5, OBJECT, Some("JSON")), route(6, PAIR, None)])
                .expect("server starts");
        let avro_decoder = ConfluentDecoder::new(confluent_registry(&server), SchemaKind::Avro);
        let json_decoder = ConfluentDecoder::new(confluent_registry(&server), SchemaKind::Json);

        let error = avro_decoder
            .decode_value(&frame_confluent_payload(5, &[0x00]), &[])
            .expect_err("JSON schema under --format avro must fail");
        assert_eq!(
            error,
            "schema id 5 is a JSON Schema, but --format avro was given"
        );

        let error = json_decoder
            .decode_value(&frame_confluent_payload(6, b"{}"), &[])
            .expect_err("Avro schema under --format json must fail");
        assert_eq!(
            error,
            "schema id 6 is an Avro schema, but --format json was given"
        );
    }
}
