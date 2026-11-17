//! Message record model shared by produce and consume: headers, binary-safe text and the NDJSON envelope.

pub mod binary_text;
pub mod message_envelope;
pub mod record_header;
