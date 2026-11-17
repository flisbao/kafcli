use super::path_segment::encode_path_segment;
use super::registered_schema::{
    CompatibilityReport, RegisteredSchema, SchemaId, SchemaKind, SchemaVersionSelector,
};
use super::registry_http_client::{RegistryHttpClient, RegistryResponse};
use super::schema_registry::SchemaRegistry;
use crate::error::{CoreError, CoreResult};
use reqwest::Method;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

const CONFLUENT_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";
const MAX_MESSAGE_CHARS: usize = 200;

/// Schema Registry with the Confluent REST API.
pub struct ConfluentSchemaRegistry {
    http: RegistryHttpClient,
}

/// What a request looked up, used to phrase 404 errors.
enum Lookup<'a> {
    Id(u32),
    Subject(&'a str),
    Version { subject: &'a str, version: String },
}

#[derive(Deserialize)]
struct SchemaByIdBody {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
    references: Option<Value>,
}

#[derive(Deserialize)]
struct SubjectVersionBody {
    subject: String,
    id: u32,
    version: u32,
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
    references: Option<Value>,
}

#[derive(Deserialize)]
struct RegisterBody {
    id: u32,
}

#[derive(Deserialize)]
struct CompatibilityBody {
    is_compatible: bool,
    messages: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct ErrorBody {
    error_code: Option<u32>,
    message: Option<String>,
}

impl ConfluentSchemaRegistry {
    pub fn new(http: RegistryHttpClient) -> Self {
        Self { http }
    }

    /// Sends a request and decodes a 2xx JSON answer; any other status becomes an error.
    fn exchange<T: DeserializeOwned>(
        &self,
        method: Method,
        path_and_query: &str,
        payload: Option<&Value>,
        lookup: Lookup<'_>,
    ) -> CoreResult<T> {
        let body = payload
            .map(serde_json::to_string)
            .transpose()
            .map_err(|_| registry_error("could not encode the schema registry request"))?;
        let content_type = body.as_ref().map(|_| CONFLUENT_CONTENT_TYPE);
        let response =
            self.http
                .send_request(method.clone(), path_and_query, content_type, body)?;
        let path = path_without_query(path_and_query);
        if !(200..300).contains(&response.status) {
            return Err(self.error_for_response(&method, path, &lookup, &response));
        }
        serde_json::from_slice(&response.body).map_err(|_| {
            registry_error(format!(
                "schema registry returned an unexpected response for {method} {path}"
            ))
        })
    }

    fn error_for_response(
        &self,
        method: &Method,
        path: &str,
        lookup: &Lookup<'_>,
        response: &RegistryResponse,
    ) -> CoreError {
        let error_body: Option<ErrorBody> = serde_json::from_slice(&response.body).ok();
        if response.status == 404 {
            let code = error_body.as_ref().and_then(|body| body.error_code);
            if let Some(message) = code.and_then(|code| not_found_message(code, lookup)) {
                return registry_error(message);
            }
        }
        let mut text = format!(
            "schema registry returned HTTP {} for {method} {path}",
            response.status
        );
        if let Some(message) = error_body.and_then(|body| body.message) {
            let sanitized = self.sanitize(&message);
            if !sanitized.is_empty() {
                text.push_str(": ");
                text.push_str(&sanitized);
            }
        }
        registry_error(text)
    }

    /// Redacts secrets, replaces control characters with `?` and keeps at most 200 characters.
    fn sanitize(&self, text: &str) -> String {
        self.http
            .redact_secrets(text)
            .chars()
            .map(|character| {
                if character.is_control() {
                    '?'
                } else {
                    character
                }
            })
            .take(MAX_MESSAGE_CHARS)
            .collect()
    }

    fn schema_kind(&self, schema_type: Option<&str>) -> CoreResult<SchemaKind> {
        match schema_type {
            None | Some("AVRO") => Ok(SchemaKind::Avro),
            Some("JSON") => Ok(SchemaKind::Json),
            Some(other) => Err(registry_error(format!(
                "schema type '{}' is not supported",
                self.sanitize(other)
            ))),
        }
    }

    fn check_schema_shape(
        &self,
        schema_type: Option<&str>,
        references: Option<&Value>,
    ) -> CoreResult<SchemaKind> {
        match references {
            None | Some(Value::Null) => {}
            Some(Value::Array(items)) if items.is_empty() => {}
            Some(_) => {
                return Err(registry_error("schema references are not supported"));
            }
        }
        self.schema_kind(schema_type)
    }
}

impl SchemaRegistry for ConfluentSchemaRegistry {
    fn fetch_schema_by_id(&self, id: &SchemaId) -> CoreResult<RegisteredSchema> {
        let SchemaId::Confluent(number) = id else {
            return Err(registry_error(format!(
                "schema id '{}' is not a Confluent schema id",
                self.sanitize(&id.to_string())
            )));
        };
        let path = format!("/schemas/ids/{number}");
        let body: SchemaByIdBody = self.exchange(Method::GET, &path, None, Lookup::Id(*number))?;
        let kind =
            self.check_schema_shape(body.schema_type.as_deref(), body.references.as_ref())?;
        Ok(RegisteredSchema {
            id: id.clone(),
            subject: None,
            version: None,
            kind,
            text: body.schema,
        })
    }

    fn fetch_schema_by_subject(
        &self,
        subject: &str,
        version: SchemaVersionSelector,
    ) -> CoreResult<RegisteredSchema> {
        let path = format!(
            "/subjects/{}/versions/{version}",
            encode_path_segment(subject)
        );
        let lookup = Lookup::Version {
            subject,
            version: version.to_string(),
        };
        let body: SubjectVersionBody = self.exchange(Method::GET, &path, None, lookup)?;
        let kind =
            self.check_schema_shape(body.schema_type.as_deref(), body.references.as_ref())?;
        Ok(RegisteredSchema {
            id: SchemaId::Confluent(body.id),
            subject: Some(body.subject),
            version: Some(body.version),
            kind,
            text: body.schema,
        })
    }

    fn register_schema(
        &self,
        subject: &str,
        kind: SchemaKind,
        text: &str,
    ) -> CoreResult<RegisteredSchema> {
        let path = format!("/subjects/{}/versions", encode_path_segment(subject));
        let payload = schema_payload(kind, text);
        let body: RegisterBody = self.exchange(
            Method::POST,
            &path,
            Some(&payload),
            Lookup::Subject(subject),
        )?;
        Ok(RegisteredSchema {
            id: SchemaId::Confluent(body.id),
            subject: Some(subject.to_string()),
            version: None,
            kind,
            text: text.to_string(),
        })
    }

    fn check_compatibility(
        &self,
        subject: &str,
        version: SchemaVersionSelector,
        kind: SchemaKind,
        text: &str,
    ) -> CoreResult<CompatibilityReport> {
        let path = format!(
            "/compatibility/subjects/{}/versions/{version}?verbose=true",
            encode_path_segment(subject)
        );
        let payload = schema_payload(kind, text);
        let lookup = Lookup::Version {
            subject,
            version: version.to_string(),
        };
        let body: CompatibilityBody = self.exchange(Method::POST, &path, Some(&payload), lookup)?;
        Ok(CompatibilityReport {
            compatible: body.is_compatible,
            messages: body.messages.unwrap_or_default(),
        })
    }

    fn registry_type(&self) -> &'static str {
        "confluent"
    }
}

fn schema_payload(kind: SchemaKind, text: &str) -> Value {
    let schema_type = match kind {
        SchemaKind::Avro => "AVRO",
        SchemaKind::Json => "JSON",
    };
    json!({"schema": text, "schemaType": schema_type})
}

fn not_found_message(code: u32, lookup: &Lookup<'_>) -> Option<String> {
    match (code, lookup) {
        (40401, Lookup::Subject(subject) | Lookup::Version { subject, .. }) => {
            Some(format!("subject '{subject}' not found"))
        }
        (40402, Lookup::Version { subject, version }) => Some(format!(
            "version {version} of subject '{subject}' not found"
        )),
        (40403, Lookup::Id(id)) => Some(format!("schema id {id} not found")),
        _ => None,
    }
}

fn path_without_query(path_and_query: &str) -> &str {
    path_and_query.split('?').next().unwrap_or(path_and_query)
}

fn registry_error(message: impl Into<String>) -> CoreError {
    CoreError::SchemaRegistry(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::schema_registry_model::{
        RegistryAuthConfig, RegistryBasicAuthConfig, RegistryBearerAuthConfig,
    };
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use serde_json::json;
    use std::net::TcpListener;
    use std::time::Duration;

    fn route(method: &str, path: &str, status: u16, body: &str) -> FakeRoute {
        FakeRoute {
            method: method.into(),
            path_and_query: path.into(),
            status,
            headers: vec![("Content-Type".into(), CONFLUENT_CONTENT_TYPE.into())],
            body: body.into(),
        }
    }

    fn registry_with(
        server: &FakeRegistryServer,
        auth: &RegistryAuthConfig,
        secret: &str,
    ) -> ConfluentSchemaRegistry {
        let http = RegistryHttpClient::new(
            &server.url(),
            auth,
            None,
            &FixedSecretResolver::returning(secret),
            Duration::from_secs(5),
        )
        .unwrap();
        ConfluentSchemaRegistry::new(http)
    }

    fn anonymous_registry(server: &FakeRegistryServer) -> ConfluentSchemaRegistry {
        registry_with(server, &RegistryAuthConfig::Anonymous, "unused")
    }

    fn header_of<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn fetches_schema_by_id_defaulting_to_avro() {
        let server = FakeRegistryServer::start(vec![route(
            "GET",
            "/schemas/ids/7",
            200,
            r#"{"schema":"\"string\""}"#,
        )])
        .unwrap();

        let schema = anonymous_registry(&server)
            .fetch_schema_by_id(&SchemaId::Confluent(7))
            .unwrap();

        assert_eq!(schema.id, SchemaId::Confluent(7));
        assert_eq!(schema.subject, None);
        assert_eq!(schema.version, None);
        assert_eq!(schema.kind, SchemaKind::Avro);
        assert_eq!(schema.text, "\"string\"");
        let requests = server.recorded_requests();
        assert_eq!(
            header_of(&requests[0].headers, "accept"),
            Some(CONFLUENT_CONTENT_TYPE)
        );
    }

    #[test]
    fn fetches_subject_version_and_latest() {
        let server = FakeRegistryServer::start(vec![
            route(
                "GET",
                "/subjects/pedidos-value/versions/latest",
                200,
                r#"{"subject":"pedidos-value","id":12,"version":3,"schema":"{\"type\":\"record\"}","schemaType":"AVRO"}"#,
            ),
            route(
                "GET",
                "/subjects/pedidos-value/versions/2",
                200,
                r#"{"subject":"pedidos-value","id":9,"version":2,"schema":"{\"type\":\"object\"}","schemaType":"JSON"}"#,
            ),
        ])
        .unwrap();
        let registry = anonymous_registry(&server);

        let latest = registry
            .fetch_schema_by_subject("pedidos-value", SchemaVersionSelector::Latest)
            .unwrap();
        let second = registry
            .fetch_schema_by_subject("pedidos-value", SchemaVersionSelector::Number(2))
            .unwrap();

        assert_eq!(latest.id, SchemaId::Confluent(12));
        assert_eq!(latest.subject.as_deref(), Some("pedidos-value"));
        assert_eq!(latest.version, Some(3));
        assert_eq!(latest.kind, SchemaKind::Avro);
        assert_eq!(second.id, SchemaId::Confluent(9));
        assert_eq!(second.version, Some(2));
        assert_eq!(second.kind, SchemaKind::Json);
        assert_eq!(second.text, r#"{"type":"object"}"#);
    }

    #[test]
    fn registers_schema_with_schema_type_json() {
        let server = FakeRegistryServer::start(vec![route(
            "POST",
            "/subjects/pedidos-value/versions",
            200,
            r#"{"id":42}"#,
        )])
        .unwrap();

        let schema = anonymous_registry(&server)
            .register_schema("pedidos-value", SchemaKind::Json, r#"{"type":"object"}"#)
            .unwrap();

        assert_eq!(schema.id, SchemaId::Confluent(42));
        assert_eq!(schema.version, None);
        let requests = server.recorded_requests();
        assert_eq!(
            header_of(&requests[0].headers, "content-type"),
            Some(CONFLUENT_CONTENT_TYPE)
        );
        let sent: serde_json::Value = serde_json::from_str(&requests[0].body).unwrap();
        assert_eq!(
            sent,
            json!({"schema": r#"{"type":"object"}"#, "schemaType": "JSON"})
        );
    }

    #[test]
    fn checks_compatibility_verbose() {
        let server = FakeRegistryServer::start(vec![route(
            "POST",
            "/compatibility/subjects/pedidos-value/versions/latest?verbose=true",
            200,
            r#"{"is_compatible":false,"messages":["field removed"]}"#,
        )])
        .unwrap();

        let report = anonymous_registry(&server)
            .check_compatibility(
                "pedidos-value",
                SchemaVersionSelector::Latest,
                SchemaKind::Avro,
                r#"{"type":"string"}"#,
            )
            .unwrap();

        assert!(!report.compatible);
        assert_eq!(report.messages, vec!["field removed".to_string()]);
        let requests = server.recorded_requests();
        assert_eq!(
            requests[0].path_and_query,
            "/compatibility/subjects/pedidos-value/versions/latest?verbose=true"
        );
    }

    #[test]
    fn maps_not_found_error_codes() {
        let server = FakeRegistryServer::start(vec![
            route(
                "GET",
                "/subjects/missing/versions/latest",
                404,
                r#"{"error_code":40401,"message":"Subject not found"}"#,
            ),
            route(
                "GET",
                "/subjects/pedidos-value/versions/9",
                404,
                r#"{"error_code":40402,"message":"Version not found"}"#,
            ),
            route(
                "GET",
                "/schemas/ids/99",
                404,
                r#"{"error_code":40403,"message":"Schema not found"}"#,
            ),
        ])
        .unwrap();
        let registry = anonymous_registry(&server);

        let subject = registry
            .fetch_schema_by_subject("missing", SchemaVersionSelector::Latest)
            .unwrap_err()
            .to_string();
        let version = registry
            .fetch_schema_by_subject("pedidos-value", SchemaVersionSelector::Number(9))
            .unwrap_err()
            .to_string();
        let id = registry
            .fetch_schema_by_id(&SchemaId::Confluent(99))
            .unwrap_err()
            .to_string();

        assert_eq!(
            subject,
            "schema registry error: subject 'missing' not found"
        );
        assert_eq!(
            version,
            "schema registry error: version 9 of subject 'pedidos-value' not found"
        );
        assert_eq!(id, "schema registry error: schema id 99 not found");
    }

    #[test]
    fn rejects_schema_references() {
        let server = FakeRegistryServer::start(vec![route(
            "GET",
            "/schemas/ids/5",
            200,
            r#"{"schema":"{}","references":[{"name":"a","subject":"b","version":1}]}"#,
        )])
        .unwrap();

        let error = anonymous_registry(&server)
            .fetch_schema_by_id(&SchemaId::Confluent(5))
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "schema registry error: schema references are not supported"
        );
    }

    #[test]
    fn rejects_protobuf() {
        let server = FakeRegistryServer::start(vec![route(
            "GET",
            "/schemas/ids/6",
            200,
            r#"{"schema":"syntax = \"proto3\";","schemaType":"PROTOBUF"}"#,
        )])
        .unwrap();

        let error = anonymous_registry(&server)
            .fetch_schema_by_id(&SchemaId::Confluent(6))
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "schema registry error: schema type 'PROTOBUF' is not supported"
        );
    }

    #[test]
    fn error_message_is_truncated_and_neutralized() {
        let message = format!("bad\u{1b}[2J{}", "x".repeat(500));
        let body = json!({"error_code": 50000, "message": message}).to_string();
        let server =
            FakeRegistryServer::start(vec![route("GET", "/schemas/ids/1", 500, &body)]).unwrap();

        let error = anonymous_registry(&server)
            .fetch_schema_by_id(&SchemaId::Confluent(1))
            .unwrap_err()
            .to_string();

        let prefix =
            "schema registry error: schema registry returned HTTP 500 for GET /schemas/ids/1: ";
        let suffix = error.strip_prefix(prefix).unwrap();
        assert_eq!(suffix.chars().count(), 200);
        assert!(suffix.starts_with("bad?[2J"), "suffix: {suffix}");
        assert!(!suffix.contains('\u{1b}'));
    }

    #[test]
    fn registry_error_never_contains_secrets() {
        let password = "pw-Sup3rSecret";
        let echoed = format!("Basic {}", STANDARD.encode(format!("app:{password}")));
        let body =
            json!({"error_code": 40101, "message": format!("rejected {echoed}")}).to_string();
        let server =
            FakeRegistryServer::start(vec![route("GET", "/schemas/ids/1", 401, &body)]).unwrap();
        let auth = RegistryAuthConfig::Basic(RegistryBasicAuthConfig {
            username: "app".into(),
            secret_ref: "env:SR_PASSWORD".parse().unwrap(),
        });

        let error = registry_with(&server, &auth, password)
            .fetch_schema_by_id(&SchemaId::Confluent(1))
            .unwrap_err()
            .to_string();

        assert!(error.contains("HTTP 401"), "error: {error}");
        assert!(!error.contains(password), "error: {error}");
        assert!(
            !error.contains(&STANDARD.encode(format!("app:{password}"))),
            "error: {error}"
        );

        let token = "tok-Sup3rSecret";
        let body =
            json!({"error_code": 40101, "message": format!("rejected Bearer {token}")}).to_string();
        let server =
            FakeRegistryServer::start(vec![route("GET", "/schemas/ids/1", 401, &body)]).unwrap();
        let auth = RegistryAuthConfig::Bearer(RegistryBearerAuthConfig {
            token_ref: "env:SR_TOKEN".parse().unwrap(),
        });

        let error = registry_with(&server, &auth, token)
            .fetch_schema_by_id(&SchemaId::Confluent(1))
            .unwrap_err()
            .to_string();

        assert!(!error.contains(token), "error: {error}");
    }

    #[test]
    fn unreachable_registry_reports_transport_error() {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let http = RegistryHttpClient::new(
            &format!("http://127.0.0.1:{port}"),
            &RegistryAuthConfig::Anonymous,
            None,
            &FixedSecretResolver::returning("unused"),
            Duration::from_secs(2),
        )
        .unwrap();

        let error = ConfluentSchemaRegistry::new(http)
            .fetch_schema_by_id(&SchemaId::Confluent(1))
            .unwrap_err()
            .to_string();

        assert!(
            error.starts_with("schema registry error: request to schema registry failed"),
            "error: {error}"
        );
    }
}
