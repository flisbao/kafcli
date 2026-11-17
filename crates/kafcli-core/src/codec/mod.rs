//! Value codecs: how a message value becomes bytes on the wire and back.

pub mod avro_schema;
pub mod avro_value_conversion;
pub mod raw_avro_codec;
pub mod raw_json_codec;
pub mod value_codec;

pub use avro_schema::AvroSchema;
pub use raw_avro_codec::RawAvroCodec;
pub use raw_json_codec::RawJsonCodec;
pub use value_codec::{EncodedValue, ValueDecoder, ValueEncoder};
