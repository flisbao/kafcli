use crate::error::CoreResult;
use crate::message::record_header::RecordHeader;

/// A value ready for the wire: its bytes and the headers the wire format requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedValue {
    pub bytes: Vec<u8>,
    pub headers: Vec<RecordHeader>,
}

/// Turns a JSON value into wire bytes plus any headers the format needs.
pub trait ValueEncoder {
    fn encode_value(&self, value: &serde_json::Value) -> CoreResult<EncodedValue>;
}

/// Turns wire bytes and headers back into a JSON value.
///
/// The error text must never contain the payload; it only says where decoding failed.
/// A decode error is reported in the envelope as `decode_error`.
pub trait ValueDecoder {
    fn decode_value(
        &self,
        bytes: &[u8],
        headers: &[RecordHeader],
    ) -> Result<serde_json::Value, String>;
}
