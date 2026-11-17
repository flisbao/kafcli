//! Codecs for the Azure wire format: the schema id is in the `content-type` header and the body
//! is pure Avro binary.

use super::avro_schema::AvroSchema;
use super::azure_wire::{
    AZURE_CONTENT_TYPE_HEADER, build_azure_content_type, read_azure_schema_id,
};
use super::schema_cache::{ParsedSchema, SCHEMA_CACHE_CAPACITY, SchemaCache};
use super::value_codec::{EncodedValue, ValueDecoder, ValueEncoder};
use crate::error::CoreResult;
use crate::message::record_header::RecordHeader;
use crate::registry::registered_schema::SchemaId;
use crate::registry::registry_text::neutralize_registry_text;
use crate::registry::schema_registry::SchemaRegistry;

/// Encodes values as pure Avro binary and attaches the `content-type` header.
pub struct AzureAvroEncoder {
    schema_id: String,
    schema: AvroSchema,
}

impl AzureAvroEncoder {
    pub fn new(schema_id: String, schema: AvroSchema) -> Self {
        Self { schema_id, schema }
    }
}

impl ValueEncoder for AzureAvroEncoder {
    fn encode_value(&self, value: &serde_json::Value) -> CoreResult<EncodedValue> {
        let bytes = self.schema.encode_json(value)?;
        let content_type = build_azure_content_type(&self.schema_id);
        Ok(EncodedValue {
            bytes,
            headers: vec![RecordHeader {
                name: AZURE_CONTENT_TYPE_HEADER.to_string(),
                value: Some(content_type.into_bytes()),
            }],
        })
    }
}

/// Decodes Azure-framed values, fetching each schema id at most once per decoder.
pub struct AzureAvroDecoder {
    registry: Box<dyn SchemaRegistry>,
    cache: SchemaCache,
}

impl AzureAvroDecoder {
    pub fn new(registry: Box<dyn SchemaRegistry>) -> Self {
        Self {
            registry,
            cache: SchemaCache::new(SCHEMA_CACHE_CAPACITY),
        }
    }
}

impl ValueDecoder for AzureAvroDecoder {
    fn decode_value(
        &self,
        bytes: &[u8],
        headers: &[RecordHeader],
    ) -> Result<serde_json::Value, String> {
        let schema_id = read_azure_schema_id(headers)?;
        let schema = self
            .cache
            .fetch_schema(&SchemaId::Azure(schema_id.clone()), self.registry.as_ref())?;
        match schema.as_ref() {
            ParsedSchema::Avro(avro) => avro.decode_to_json(bytes),
            ParsedSchema::Json => Err(format!(
                "schema id {} is a JSON Schema, but --format avro was given",
                neutralize_registry_text(&schema_id)
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::schema_registry_model::RegistryAuthConfig;
    use crate::registry::azure_registry::{AZURE_ACCEPT, AzureSchemaRegistry};
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
    use crate::registry::registry_http_client::RegistryHttpClient;
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use serde_json::json;
    use std::time::Duration;

    const SCHEMA_ID: &str = "0f1e2d3c4b5a69788796a5b4c3d2e1f0";
    const PAIR: &str = r#"{"type":"record","name":"Pair","fields":[
        {"name":"a","type":"int"},{"name":"b","type":"string"}]}"#;

    fn azure_registry(server: &FakeRegistryServer) -> Box<dyn SchemaRegistry> {
        let http = RegistryHttpClient::new_with_accept(
            &server.url(),
            &RegistryAuthConfig::Anonymous,
            None,
            &FixedSecretResolver::returning("unused"),
            Duration::from_secs(5),
            AZURE_ACCEPT,
        )
        .expect("client builds");
        Box::new(AzureSchemaRegistry::new(http, "pedidos".into()))
    }

    fn schema_route() -> FakeRoute {
        FakeRoute {
            method: "GET".into(),
            path_and_query: format!("/$schemaGroups/$schemas/{SCHEMA_ID}?api-version=2023-07-01"),
            status: 200,
            headers: vec![(
                "Content-Type".into(),
                "application/json; serialization=Avro".into(),
            )],
            body: PAIR.into(),
        }
    }

    #[test]
    fn azure_avro_encoder_adds_content_type_header() {
        let encoder = AzureAvroEncoder::new(
            SCHEMA_ID.to_string(),
            AvroSchema::parse(PAIR).expect("schema parses"),
        );

        let encoded = encoder
            .encode_value(&json!({"a": 1, "b": "x"}))
            .expect("encode");

        assert_eq!(encoded.bytes, vec![0x02, 0x02, 0x78]);
        assert_eq!(encoded.headers.len(), 1);
        assert_eq!(encoded.headers[0].name, AZURE_CONTENT_TYPE_HEADER);
        assert_eq!(
            encoded.headers[0].value.as_deref(),
            Some(format!("avro/binary+{SCHEMA_ID}").as_bytes())
        );
    }

    #[test]
    fn azure_round_trip_through_fake_registry() {
        let server = FakeRegistryServer::start(vec![schema_route()]).expect("server starts");
        let value = json!({"a": 7, "b": "azure"});
        let encoded = AzureAvroEncoder::new(
            SCHEMA_ID.to_string(),
            AvroSchema::parse(PAIR).expect("schema parses"),
        )
        .encode_value(&value)
        .expect("encode");

        let decoder = AzureAvroDecoder::new(azure_registry(&server));
        let decoded = decoder
            .decode_value(&encoded.bytes, &encoded.headers)
            .expect("decode");

        assert_eq!(decoded, value);
        assert_eq!(server.recorded_requests().len(), 1);
    }

    #[test]
    fn azure_decode_without_header_names_legacy_format() {
        let server = FakeRegistryServer::start(vec![]).expect("server starts");
        let decoder = AzureAvroDecoder::new(azure_registry(&server));

        let error = decoder
            .decode_value(&[0x02, 0x02, 0x78], &[])
            .expect_err("missing header must fail");

        assert_eq!(
            error,
            "value has no content-type header with avro/binary+<schema id>; the legacy 36-byte prefix format is not supported"
        );
        assert!(server.recorded_requests().is_empty());
    }
}
