//! Consume operation: read messages from a topic and hand each one to a sink as an envelope.

mod consume_client_context;
pub mod message_consumer;
pub mod message_filter;
pub mod start_position;
