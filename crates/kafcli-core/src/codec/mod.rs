//! Value codecs: how a message value becomes bytes on the wire and back.

pub mod avro_schema;
pub mod avro_value_conversion;
pub mod azure_avro_codec;
pub mod azure_wire;
pub mod codec_selection;
pub mod confluent_codec;
pub mod confluent_wire;
pub mod json_schema_validator;
pub mod raw_avro_codec;
pub mod raw_json_codec;
pub mod schema_cache;
pub mod schema_validated_json_codec;
pub mod value_codec;

pub use avro_schema::AvroSchema;
pub use json_schema_validator::JsonSchemaValidator;
pub use raw_avro_codec::RawAvroCodec;
pub use raw_json_codec::RawJsonCodec;
pub use schema_validated_json_codec::SchemaValidatedJsonCodec;
pub use value_codec::{EncodedValue, ValueDecoder, ValueEncoder};
