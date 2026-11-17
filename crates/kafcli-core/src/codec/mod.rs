//! Value codecs: how a message value becomes bytes on the wire and back.

pub mod raw_json_codec;
pub mod value_codec;

pub use raw_json_codec::RawJsonCodec;
pub use value_codec::{EncodedValue, ValueDecoder, ValueEncoder};
