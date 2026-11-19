//! Bounded per-decoder cache of parsed schemas. Failures are cached too (negative cache): a definitive
//! failure (unknown id, bad schema) lasts for the whole run, so an unknown id costs one registry
//! request. A transient failure (transport, timeout, HTTP 5xx) lasts only `TRANSIENT_FAILURE_TTL`.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::{Duration, Instant};

use super::avro_schema::AvroSchema;
use crate::error::{CoreError, CoreResult};
use crate::registry::registered_schema::{RegisteredSchema, SchemaId, SchemaKind};
use crate::registry::registry_text::neutralize_registry_text;
use crate::registry::schema_registry::SchemaRegistry;

/// Maximum entries per decoder, failures included.
pub const SCHEMA_CACHE_CAPACITY: usize = 256;

/// How long a transient registry failure is remembered before the next lookup fetches again.
pub const TRANSIENT_FAILURE_TTL: Duration = Duration::from_secs(30);

/// Source of the current time. Injected so tests can move time forward without sleeping.
pub type Clock = Box<dyn Fn() -> Instant>;

/// A schema after it was fetched: Avro is parsed, JSON Schema is only tagged by kind.
#[derive(Debug)]
pub enum ParsedSchema {
    Avro(AvroSchema),
    Json,
}

type CachedOutcome = Result<Rc<ParsedSchema>, String>;

struct CacheEntry {
    id: SchemaId,
    outcome: CachedOutcome,
    /// `None` keeps the outcome for the whole run; `Some` is the instant a transient failure expires.
    expires_at: Option<Instant>,
}

/// Fixed-capacity cache that evicts the oldest inserted entry when full.
pub struct SchemaCache {
    capacity: usize,
    transient_ttl: Duration,
    clock: Clock,
    entries: RefCell<VecDeque<CacheEntry>>,
}

impl SchemaCache {
    pub fn new(capacity: usize) -> Self {
        Self::with_clock(capacity, TRANSIENT_FAILURE_TTL, Box::new(Instant::now))
    }

    pub fn with_clock(capacity: usize, transient_ttl: Duration, clock: Clock) -> Self {
        Self {
            capacity,
            transient_ttl,
            clock,
            entries: RefCell::new(VecDeque::new()),
        }
    }

    /// Returns the parsed schema for `id`, fetching it from `registry` when no live entry exists.
    pub fn fetch_schema(
        &self,
        id: &SchemaId,
        registry: &dyn SchemaRegistry,
    ) -> Result<Rc<ParsedSchema>, String> {
        if let Some(outcome) = self.lookup(id) {
            return outcome;
        }
        let (outcome, transient) = load_schema(id, registry);
        self.store(id.clone(), outcome.clone(), transient);
        outcome
    }

    /// Finds a live entry, dropping the expired transient failures first.
    fn lookup(&self, id: &SchemaId) -> Option<CachedOutcome> {
        let now = (self.clock)();
        let mut entries = self.entries.borrow_mut();
        entries.retain(|entry| entry.expires_at.is_none_or(|expires_at| now < expires_at));
        entries
            .iter()
            .find(|entry| &entry.id == id)
            .map(|entry| entry.outcome.clone())
    }

    fn store(&self, id: SchemaId, outcome: CachedOutcome, transient: bool) {
        if self.capacity == 0 {
            return;
        }
        let now = (self.clock)();
        let expires_at = transient.then(|| now.checked_add(self.transient_ttl).unwrap_or(now));
        let mut entries = self.entries.borrow_mut();
        entries.retain(|entry| entry.id != id);
        while entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back(CacheEntry {
            id,
            outcome,
            expires_at,
        });
    }
}

/// Fetches and parses one schema. The flag tells whether a failure is transient.
fn load_schema(id: &SchemaId, registry: &dyn SchemaRegistry) -> (CachedOutcome, bool) {
    match registry.fetch_schema_by_id(id) {
        Ok(schema) => {
            let outcome = parse_registered_schema(&schema)
                .map(Rc::new)
                .map_err(|error| describe_failure(id, &error));
            (outcome, false)
        }
        Err(error) => (
            Err(describe_failure(id, &error)),
            error.is_transient_registry_failure(),
        ),
    }
}

fn describe_failure(id: &SchemaId, error: &CoreError) -> String {
    // The id comes from message headers and the error may echo it: neutralize both.
    format!(
        "schema id {} could not be fetched: {}",
        neutralize_registry_text(&id.to_string()),
        neutralize_registry_text(&error.to_string())
    )
}

fn parse_registered_schema(schema: &RegisteredSchema) -> CoreResult<ParsedSchema> {
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
    use std::cell::Cell;
    use std::time::{Duration, Instant};

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

    /// Clock the test moves by hand, so expiry is checked without sleeping.
    fn manual_clock(start: Instant) -> (Rc<Cell<Instant>>, Box<dyn Fn() -> Instant>) {
        let now = Rc::new(Cell::new(start));
        let reader = Rc::clone(&now);
        (now, Box::new(move || reader.get()))
    }

    fn status_route(id: u32, status: u16, body: &str) -> FakeRoute {
        FakeRoute {
            method: "GET".into(),
            path_and_query: format!("/schemas/ids/{id}"),
            status,
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    #[test]
    fn transient_failure_is_retried_only_after_ttl() {
        let server = FakeRegistryServer::start(vec![status_route(
            1,
            503,
            r#"{"error_code":50301,"message":"unavailable"}"#,
        )])
        .expect("server starts");
        let registry = confluent_registry(&server);
        let (now, clock) = manual_clock(Instant::now());
        let cache = SchemaCache::with_clock(SCHEMA_CACHE_CAPACITY, TRANSIENT_FAILURE_TTL, clock);
        let id = SchemaId::Confluent(1);

        assert!(cache.fetch_schema(&id, &registry).is_err());
        now.set(now.get() + Duration::from_secs(29));
        assert!(cache.fetch_schema(&id, &registry).is_err());
        assert_eq!(
            server.recorded_requests().len(),
            1,
            "no request within the TTL"
        );

        now.set(now.get() + Duration::from_secs(2));
        assert!(cache.fetch_schema(&id, &registry).is_err());
        assert_eq!(
            server.recorded_requests().len(),
            2,
            "re-fetched after the TTL"
        );
    }

    #[test]
    fn definitive_not_found_is_never_refetched() {
        let server = FakeRegistryServer::start(vec![status_route(
            2,
            404,
            r#"{"error_code":40403,"message":"Schema not found"}"#,
        )])
        .expect("server starts");
        let registry = confluent_registry(&server);
        let (now, clock) = manual_clock(Instant::now());
        let cache = SchemaCache::with_clock(SCHEMA_CACHE_CAPACITY, TRANSIENT_FAILURE_TTL, clock);
        let id = SchemaId::Confluent(2);

        assert!(cache.fetch_schema(&id, &registry).is_err());
        now.set(now.get() + Duration::from_secs(3600));
        assert!(cache.fetch_schema(&id, &registry).is_err());

        assert_eq!(server.recorded_requests().len(), 1);
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
