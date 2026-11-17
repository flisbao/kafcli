pub mod confluent_registry;
#[cfg(any(test, feature = "test-support"))]
pub mod fake_registry_server;
pub mod path_segment;
pub mod registered_schema;
pub mod registry_http_client;
pub mod registry_text;
pub mod schema_registry;
