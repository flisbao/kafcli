//! Bounded per-decoder cache of parsed schemas. Failures are cached too (negative cache),
//! so an unknown id costs one registry request per run.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use super::avro_schema::AvroSchema;
use crate::error::CoreResult;
use crate::registry::registered_schema::{RegisteredSchema, SchemaId, SchemaKind};
use crate::registry::registry_text::neutralize_registry_text;
use crate::registry::schema_registry::SchemaRegistry;

/// Maximum entries per decoder, failures included.
pub const SCHEMA_CACHE_CAPACITY: usize = 256;

/// A schema after it was fetched: Avro is parsed, JSON Schema is only tagged by kind.
#[derive(Debug)]
pub enum ParsedSchema {
    Avro(AvroSchema),
    Json,
}

type CachedOutcome = Result<Rc<ParsedSchema>, String>;

/// Fixed-capacity cache that evicts the oldest inserted entry when full.
pub struct SchemaCache {
    capacity: usize,
    entries: RefCell<VecDeque<(SchemaId, CachedOutcome)>>,
}

impl SchemaCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: RefCell::new(VecDeque::new()),
        }
    }

    /// Returns the parsed schema for `id`, fetching it from `registry` on the first request.
    pub fn fetch_schema(
        &self,
        id: &SchemaId,
        registry: &dyn SchemaRegistry,
    ) -> Result<Rc<ParsedSchema>, String> {
        if let Some(outcome) = self.lookup(id) {
            return outcome;
        }
        let outcome = load_schema(id, registry);
        self.store(id.clone(), outcome.clone());
        outcome
    }

    fn lookup(&self, id: &SchemaId) -> Option<CachedOutcome> {
        self.entries
            .borrow()
            .iter()
            .find(|(cached, _)| cached == id)
            .map(|(_, outcome)| outcome.clone())
    }

    fn store(&self, id: SchemaId, outcome: CachedOutcome) {
        if self.capacity == 0 {
            return;
        }
        let mut entries = self.entries.borrow_mut();
        while entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back((id, outcome));
    }
}

fn load_schema(id: &SchemaId, registry: &dyn SchemaRegistry) -> CachedOutcome {
    registry
        .fetch_schema_by_id(id)
        .and_then(parse_registered_schema)
        .map(Rc::new)
        .map_err(|error| {
            // The id comes from message headers and the error may echo it: neutralize both.
            format!(
                "schema id {} could not be fetched: {}",
                neutralize_registry_text(&id.to_string()),
                neutralize_registry_text(&error.to_string())
            )
        })
}

fn parse_registered_schema(schema: RegisteredSchema) -> CoreResult<ParsedSchema> {
    match schema.kind {
        SchemaKind::Avro => Ok(ParsedSchema::Avro(AvroSchema::parse(&schema.text)?)),
        SchemaKind::Json => Ok(ParsedSchema::Json),
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

    fn schema_route(id: u32) -> FakeRoute {
        FakeRoute {
            method: "GET".into(),
            path_and_query: format!("/schemas/ids/{id}"),
            status: 200,
            headers: Vec::new(),
            body: json!({"schema": PAIR}).to_string(),
        }
    }

    fn confluent_registry(server: &FakeRegistryServer) -> ConfluentSchemaRegistry {
        let http = RegistryHttpClient::new(
            &server.url(),
            &RegistryAuthConfig::Anonymous,
            None,
            &FixedSecretResolver::returning("unused"),
            Duration::from_secs(5),
        )
        .expect("client builds");
        ConfluentSchemaRegistry::new(http)
    }

    #[test]
    fn cache_evicts_oldest_beyond_capacity() {
        let server =
            FakeRegistryServer::start(vec![schema_route(1), schema_route(2), schema_route(3)])
                .expect("server starts");
        let registry = confluent_registry(&server);
        let cache = SchemaCache::new(2);

        for id in [1, 2, 3, 1] {
            cache
                .fetch_schema(&SchemaId::Confluent(id), &registry)
                .expect("schema fetches");
        }

        // 1 and 2 fill the cache, 3 evicts 1, then 1 is fetched again and evicts 2.
        assert_eq!(server.recorded_requests().len(), 4);
    }
}
