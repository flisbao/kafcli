use super::path_segment::encode_path_segment;
use super::registered_schema::{
    CompatibilityReport, RegisteredSchema, SchemaId, SchemaKind, SchemaVersionSelector,
};
use super::registry_http_client::{RegistryHttpClient, RegistryResponse, registry_status_error};
use super::schema_registry::SchemaRegistry;
use crate::error::{CoreError, CoreResult};
use reqwest::Method;
use serde::Deserialize;

/// `Accept` media type for Azure requests. The Confluent media type is not valid on Azure.
pub const AZURE_ACCEPT: &str = "application/json";
const API_VERSION_QUERY: &str = "?api-version=2023-07-01";
const AVRO_CONTENT_TYPE: &str = "application/json; serialization=Avro";

/// Schema Registry of an Azure Schema Registry namespace, bound to one schema group.
pub struct AzureSchemaRegistry {
    http: RegistryHttpClient,
    group: String,
}

/// What a request looked up, used to phrase 404 errors.
enum Lookup<'a> {
    Id(&'a str),
    Name { group: &'a str, name: &'a str },
}

impl Lookup<'_> {
    fn not_found_message(&self) -> String {
        match self {
            Self::Id(id) => format!("schema id {id} not found"),
            Self::Name { group, name } => format!("schema '{name}' not found in group '{group}'"),
        }
    }
}

#[derive(Deserialize)]
struct VersionListBody {
    #[serde(rename = "Value", alias = "value")]
    values: Option<Vec<u32>>,
}

#[derive(Deserialize)]
struct ErrorEnvelope {
    error: Option<ErrorDetail>,
}

#[derive(Deserialize)]
struct ErrorDetail {
    message: Option<String>,
}

impl AzureSchemaRegistry {
    pub fn new(http: RegistryHttpClient, group: String) -> Self {
        Self { http, group }
    }

    /// Sends a request and returns the answer only when its status is 2xx.
    fn send(
        &self,
        method: Method,
        path_and_query: &str,
        content_type: Option<&str>,
        body: Option<String>,
        lookup: &Lookup<'_>,
    ) -> CoreResult<RegistryResponse> {
        let response =
            self.http
                .send_request(method.clone(), path_and_query, content_type, body)?;
        if (200..300).contains(&response.status) {
            return Ok(response);
        }
        Err(self.error_for_response(
            &method,
            path_without_query(path_and_query),
            lookup,
            &response,
        ))
    }

    fn error_for_response(
        &self,
        method: &Method,
        path: &str,
        lookup: &Lookup<'_>,
        response: &RegistryResponse,
    ) -> CoreError {
        if response.status == 404 {
            return registry_error(lookup.not_found_message());
        }
        let mut text = format!(
            "schema registry returned HTTP {} for {method} {path}",
            response.status
        );
        let message = serde_json::from_slice::<ErrorEnvelope>(&response.body)
            .ok()
            .and_then(|envelope| envelope.error)
            .and_then(|detail| detail.message);
        if let Some(message) = message {
            let sanitized = self.http.sanitize_text(&message);
            if !sanitized.is_empty() {
                text.push_str(": ");
                text.push_str(&sanitized);
            }
        }
        registry_status_error(response.status, text)
    }

    /// Reads the kind from the `serialization` parameter of `Content-Type`, ignoring case.
    fn read_schema_kind(&self, response: &RegistryResponse) -> CoreResult<SchemaKind> {
        let content_type = response.header("Content-Type").unwrap_or_default();
        let serialization = content_type
            .split(';')
            .skip(1)
            .find_map(|parameter| {
                let (name, value) = parameter.split_once('=')?;
                name.trim()
                    .eq_ignore_ascii_case("serialization")
                    .then(|| value.trim())
            })
            .unwrap_or_default();
        if serialization.eq_ignore_ascii_case("avro") {
            Ok(SchemaKind::Avro)
        } else if serialization.eq_ignore_ascii_case("json") {
            Ok(SchemaKind::Json)
        } else {
            Err(registry_error(format!(
                "schema type '{}' is not supported",
                self.http.sanitize_text(serialization)
            )))
        }
    }

    /// Returns a header value from the registry, sanitized, or `None` when absent or empty.
    fn header_text(&self, response: &RegistryResponse, name: &str) -> Option<String> {
        response
            .header(name)
            .map(|value| self.http.sanitize_text(value.trim()))
            .filter(|value| !value.is_empty())
    }

    fn schema_id_header(
        &self,
        response: &RegistryResponse,
        method: &Method,
        path: &str,
    ) -> CoreResult<SchemaId> {
        self.header_text(response, "Schema-Id")
            .map(SchemaId::Azure)
            .ok_or_else(|| unexpected_response(method, path))
    }

    /// Fetches one version by its number; the name is the kafcli subject.
    fn fetch_version(&self, name: &str, number: u32) -> CoreResult<RegisteredSchema> {
        let path = format!(
            "/$schemaGroups/{}/schemas/{}/versions/{number}{API_VERSION_QUERY}",
            encode_path_segment(&self.group),
            encode_path_segment(name)
        );
        let shown = path_without_query(&path);
        let lookup = Lookup::Name {
            group: &self.group,
            name,
        };
        let response = self.send(Method::GET, &path, None, None, &lookup)?;
        let id = self.schema_id_header(&response, &Method::GET, shown)?;
        let kind = self.read_schema_kind(&response)?;
        let subject = self
            .header_text(&response, "Schema-Name")
            .unwrap_or_else(|| name.to_string());
        let text = body_text(response, &Method::GET, shown)?;
        Ok(RegisteredSchema {
            id,
            subject: Some(subject),
            version: Some(number),
            kind,
            text,
        })
    }
}

impl SchemaRegistry for AzureSchemaRegistry {
    fn fetch_schema_by_id(&self, id: &SchemaId) -> CoreResult<RegisteredSchema> {
        let SchemaId::Azure(azure_id) = id else {
            return Err(registry_error(format!(
                "schema id '{}' is not an Azure schema id",
                self.http.sanitize_text(&id.to_string())
            )));
        };
        let path = format!(
            "/$schemaGroups/$schemas/{}{API_VERSION_QUERY}",
            encode_path_segment(azure_id)
        );
        let shown = path_without_query(&path);
        let lookup = Lookup::Id(azure_id.as_str());
        let response = self.send(Method::GET, &path, None, None, &lookup)?;
        let kind = self.read_schema_kind(&response)?;
        let subject = self.header_text(&response, "Schema-Name");
        let version = version_header(&response);
        let text = body_text(response, &Method::GET, shown)?;
        Ok(RegisteredSchema {
            id: id.clone(),
            subject,
            version,
            kind,
            text,
        })
    }

    fn fetch_schema_by_subject(
        &self,
        subject: &str,
        version: SchemaVersionSelector,
    ) -> CoreResult<RegisteredSchema> {
        let number = match version {
            SchemaVersionSelector::Number(number) => return self.fetch_version(subject, number),
            SchemaVersionSelector::Latest => {
                let path = format!(
                    "/$schemaGroups/{}/schemas/{}/versions{API_VERSION_QUERY}",
                    encode_path_segment(&self.group),
                    encode_path_segment(subject)
                );
                let shown = path_without_query(&path);
                let lookup = Lookup::Name {
                    group: &self.group,
                    name: subject,
                };
                let response = self.send(Method::GET, &path, None, None, &lookup)?;
                let list: VersionListBody = serde_json::from_slice(&response.body)
                    .map_err(|_| unexpected_response(&Method::GET, shown))?;
                let values = list
                    .values
                    .ok_or_else(|| unexpected_response(&Method::GET, shown))?;
                values
                    .into_iter()
                    .max()
                    .ok_or_else(|| registry_error(lookup.not_found_message()))?
            }
        };
        self.fetch_version(subject, number)
    }

    fn register_schema(
        &self,
        subject: &str,
        kind: SchemaKind,
        text: &str,
    ) -> CoreResult<RegisteredSchema> {
        let content_type = match kind {
            SchemaKind::Avro => AVRO_CONTENT_TYPE,
            SchemaKind::Json => {
                return Err(registry_error(
                    "JSON Schema on the Azure Schema Registry is not supported by kafcli",
                ));
            }
        };
        let path = format!(
            "/$schemaGroups/{}/schemas/{}{API_VERSION_QUERY}",
            encode_path_segment(&self.group),
            encode_path_segment(subject)
        );
        let shown = path_without_query(&path);
        let lookup = Lookup::Name {
            group: &self.group,
            name: subject,
        };
        let response = self.send(
            Method::PUT,
            &path,
            Some(content_type),
            Some(text.to_string()),
            &lookup,
        )?;
        let id = self.schema_id_header(&response, &Method::PUT, shown)?;
        Ok(RegisteredSchema {
            id,
            subject: Some(subject.to_string()),
            version: version_header(&response),
            kind,
            text: text.to_string(),
        })
    }

    fn check_compatibility(
        &self,
        _subject: &str,
        _version: SchemaVersionSelector,
        _kind: SchemaKind,
        _text: &str,
    ) -> CoreResult<CompatibilityReport> {
        Err(registry_error(
            "check-compat is not supported by the Azure Schema Registry; the schema group enforces compatibility when a schema is registered",
        ))
    }

    fn registry_type(&self) -> &'static str {
        "azure"
    }
}

fn version_header(response: &RegistryResponse) -> Option<u32> {
    response
        .header("Schema-Version")
        .and_then(|value| value.trim().parse().ok())
}

fn body_text(response: RegistryResponse, method: &Method, path: &str) -> CoreResult<String> {
    String::from_utf8(response.body).map_err(|_| unexpected_response(method, path))
}

fn unexpected_response(method: &Method, path: &str) -> CoreError {
    registry_error(format!(
        "schema registry returned an unexpected response for {method} {path}"
    ))
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
    use crate::config::schema_registry_model::RegistryAuthConfig;
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
    use crate::registry::registered_schema::{SchemaId, SchemaKind, SchemaVersionSelector};
    use crate::registry::registry_http_client::RegistryHttpClient;
    use crate::registry::schema_registry::SchemaRegistry;
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use std::time::Duration;

    const SCHEMA_ID: &str = "0f1e2d3c4b5a69788796a5b4c3d2e1f0";
    const AVRO_RESPONSE_TYPE: &str = "application/json; serialization=Avro";

    fn route(
        method: &str,
        path: &str,
        status: u16,
        headers: &[(&str, &str)],
        body: &str,
    ) -> FakeRoute {
        FakeRoute {
            method: method.into(),
            path_and_query: path.into(),
            status,
            headers: headers
                .iter()
                .map(|(name, value)| ((*name).into(), (*value).into()))
                .collect(),
            body: body.into(),
        }
    }

    fn azure_registry(server: &FakeRegistryServer) -> AzureSchemaRegistry {
        let http = RegistryHttpClient::new_with_accept(
            &server.url(),
            &RegistryAuthConfig::Anonymous,
            None,
            &FixedSecretResolver::returning("unused"),
            Duration::from_secs(5),
            AZURE_ACCEPT,
        )
        .unwrap();
        AzureSchemaRegistry::new(http, "pedidos".into())
    }

    fn header_of<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn fetches_azure_schema_by_id_with_kind_from_content_type() {
        let by_id_path = format!("/$schemaGroups/$schemas/{SCHEMA_ID}?api-version=2023-07-01");
        let json_server = FakeRegistryServer::start(vec![route(
            "GET",
            &by_id_path,
            200,
            &[
                ("Content-Type", "application/json; serialization=Json"),
                ("Schema-Name", "pedidos-value"),
                ("Schema-Version", "3"),
            ],
            r#"{"type":"object"}"#,
        )])
        .unwrap();
        let avro_server = FakeRegistryServer::start(vec![route(
            "GET",
            &by_id_path,
            200,
            &[("Content-Type", "application/json; Serialization=AVRO")],
            r#""string""#,
        )])
        .unwrap();
        let id = SchemaId::Azure(SCHEMA_ID.into());

        let json = azure_registry(&json_server)
            .fetch_schema_by_id(&id)
            .unwrap();
        let avro = azure_registry(&avro_server)
            .fetch_schema_by_id(&id)
            .unwrap();

        assert_eq!(json.id, id);
        assert_eq!(json.kind, SchemaKind::Json);
        assert_eq!(json.text, r#"{"type":"object"}"#);
        assert_eq!(json.subject.as_deref(), Some("pedidos-value"));
        assert_eq!(json.version, Some(3));
        assert_eq!(avro.kind, SchemaKind::Avro);
        assert_eq!(avro.text, r#""string""#);
    }

    #[test]
    fn fetches_azure_version() {
        let server = FakeRegistryServer::start(vec![route(
            "GET",
            "/$schemaGroups/pedidos/schemas/pedidos-value/versions/3?api-version=2023-07-01",
            200,
            &[
                ("Content-Type", AVRO_RESPONSE_TYPE),
                ("Schema-Id", SCHEMA_ID),
                ("Schema-Version", "3"),
                ("Schema-Name", "pedidos-value"),
            ],
            r#"{"type":"string"}"#,
        )])
        .unwrap();

        let schema = azure_registry(&server)
            .fetch_schema_by_subject("pedidos-value", SchemaVersionSelector::Number(3))
            .unwrap();

        assert_eq!(schema.id, SchemaId::Azure(SCHEMA_ID.into()));
        assert_eq!(schema.subject.as_deref(), Some("pedidos-value"));
        assert_eq!(schema.version, Some(3));
        assert_eq!(schema.kind, SchemaKind::Avro);
        assert_eq!(schema.text, r#"{"type":"string"}"#);
    }

    #[test]
    fn latest_uses_highest_listed_version() {
        let server = FakeRegistryServer::start(vec![
            route(
                "GET",
                "/$schemaGroups/pedidos/schemas/pedidos-value/versions?api-version=2023-07-01",
                200,
                &[],
                r#"{"Value":[1,3,2]}"#,
            ),
            route(
                "GET",
                "/$schemaGroups/pedidos/schemas/pedidos-value/versions/3?api-version=2023-07-01",
                200,
                &[
                    ("Content-Type", AVRO_RESPONSE_TYPE),
                    ("Schema-Id", SCHEMA_ID),
                    ("Schema-Version", "3"),
                ],
                r#""latest""#,
            ),
        ])
        .unwrap();

        let schema = azure_registry(&server)
            .fetch_schema_by_subject("pedidos-value", SchemaVersionSelector::Latest)
            .unwrap();

        assert_eq!(schema.version, Some(3));
        assert_eq!(schema.text, r#""latest""#);
        let requests = server.recorded_requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].path_and_query.contains("/versions/3?"));
    }

    #[test]
    fn latest_accepts_lowercase_value_list() {
        let server = FakeRegistryServer::start(vec![
            route(
                "GET",
                "/$schemaGroups/pedidos/schemas/pedidos-value/versions?api-version=2023-07-01",
                200,
                &[],
                r#"{"value":[2]}"#,
            ),
            route(
                "GET",
                "/$schemaGroups/pedidos/schemas/pedidos-value/versions/2?api-version=2023-07-01",
                200,
                &[
                    ("Content-Type", AVRO_RESPONSE_TYPE),
                    ("Schema-Id", SCHEMA_ID),
                ],
                r#""string""#,
            ),
        ])
        .unwrap();

        let schema = azure_registry(&server)
            .fetch_schema_by_subject("pedidos-value", SchemaVersionSelector::Latest)
            .unwrap();

        assert_eq!(schema.version, Some(2));
    }

    #[test]
    fn registers_avro_schema_with_serialization_content_type() {
        let server = FakeRegistryServer::start(vec![route(
            "PUT",
            "/$schemaGroups/pedidos/schemas/pedidos-value?api-version=2023-07-01",
            200,
            &[("Schema-Id", SCHEMA_ID), ("Schema-Version", "4")],
            "",
        )])
        .unwrap();

        let schema = azure_registry(&server)
            .register_schema("pedidos-value", SchemaKind::Avro, r#"{"type":"string"}"#)
            .unwrap();

        assert_eq!(schema.id, SchemaId::Azure(SCHEMA_ID.into()));
        assert_eq!(schema.subject.as_deref(), Some("pedidos-value"));
        assert_eq!(schema.version, Some(4));
        assert_eq!(schema.kind, SchemaKind::Avro);
        let requests = server.recorded_requests();
        assert_eq!(requests[0].method, "PUT");
        assert_eq!(
            requests[0].path_and_query,
            "/$schemaGroups/pedidos/schemas/pedidos-value?api-version=2023-07-01"
        );
        assert_eq!(
            header_of(&requests[0].headers, "content-type"),
            Some("application/json; serialization=Avro")
        );
        assert_eq!(requests[0].body, r#"{"type":"string"}"#);
    }

    #[test]
    fn refuses_json_registration_on_azure() {
        let server = FakeRegistryServer::start(vec![]).unwrap();

        let error = azure_registry(&server)
            .register_schema("pedidos-value", SchemaKind::Json, "{}")
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "schema registry error: JSON Schema on the Azure Schema Registry is not supported by kafcli"
        );
        assert!(server.recorded_requests().is_empty());
    }

    #[test]
    fn check_compatibility_is_unsupported_on_azure() {
        let server = FakeRegistryServer::start(vec![]).unwrap();

        let error = azure_registry(&server)
            .check_compatibility(
                "pedidos-value",
                SchemaVersionSelector::Latest,
                SchemaKind::Avro,
                "{}",
            )
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "schema registry error: check-compat is not supported by the Azure Schema Registry; the schema group enforces compatibility when a schema is registered"
        );
        assert!(server.recorded_requests().is_empty());
    }

    #[test]
    fn maps_azure_not_found() {
        let by_id_path = format!("/$schemaGroups/$schemas/{SCHEMA_ID}?api-version=2023-07-01");
        let server = FakeRegistryServer::start(vec![
            route(
                "GET",
                &by_id_path,
                404,
                &[],
                r#"{"error":{"code":"404","message":"not here"}}"#,
            ),
            route(
                "GET",
                "/$schemaGroups/pedidos/schemas/pedidos-value/versions/9?api-version=2023-07-01",
                404,
                &[],
                "",
            ),
            route(
                "GET",
                "/$schemaGroups/pedidos/schemas/pedidos-value/versions?api-version=2023-07-01",
                200,
                &[],
                r#"{"Value":[]}"#,
            ),
            route(
                "GET",
                "/$schemaGroups/pedidos/schemas/missing/versions?api-version=2023-07-01",
                404,
                &[],
                "",
            ),
        ])
        .unwrap();
        let registry = azure_registry(&server);

        let by_id = registry
            .fetch_schema_by_id(&SchemaId::Azure(SCHEMA_ID.into()))
            .unwrap_err()
            .to_string();
        let version = registry
            .fetch_schema_by_subject("pedidos-value", SchemaVersionSelector::Number(9))
            .unwrap_err()
            .to_string();
        let empty_list = registry
            .fetch_schema_by_subject("pedidos-value", SchemaVersionSelector::Latest)
            .unwrap_err()
            .to_string();
        let missing = registry
            .fetch_schema_by_subject("missing", SchemaVersionSelector::Latest)
            .unwrap_err()
            .to_string();

        assert_eq!(
            by_id,
            format!("schema registry error: schema id {SCHEMA_ID} not found")
        );
        assert_eq!(
            version,
            "schema registry error: schema 'pedidos-value' not found in group 'pedidos'"
        );
        assert_eq!(
            empty_list,
            "schema registry error: schema 'pedidos-value' not found in group 'pedidos'"
        );
        assert_eq!(
            missing,
            "schema registry error: schema 'missing' not found in group 'pedidos'"
        );
    }

    #[test]
    fn other_errors_use_message_from_error_envelope_sanitized() {
        let server = FakeRegistryServer::start(vec![route(
            "GET",
            "/$schemaGroups/pedidos/schemas/pedidos-value/versions/5?api-version=2023-07-01",
            401,
            &[],
            "{\"error\":{\"code\":\"401\",\"message\":\"bad\\u001b[2J token\"}}",
        )])
        .unwrap();

        let error = azure_registry(&server)
            .fetch_schema_by_subject("pedidos-value", SchemaVersionSelector::Number(5))
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "schema registry error: schema registry returned HTTP 401 for GET /$schemaGroups/pedidos/schemas/pedidos-value/versions/5: bad?[2J token"
        );
    }

    #[test]
    fn rejects_confluent_schema_id() {
        let server = FakeRegistryServer::start(vec![]).unwrap();

        let error = azure_registry(&server)
            .fetch_schema_by_id(&SchemaId::Confluent(7))
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "schema registry error: schema id '7' is not an Azure schema id"
        );
    }

    #[test]
    fn rejects_unsupported_serialization() {
        let by_id_path = format!("/$schemaGroups/$schemas/{SCHEMA_ID}?api-version=2023-07-01");
        let server = FakeRegistryServer::start(vec![route(
            "GET",
            &by_id_path,
            200,
            &[("Content-Type", "application/json; serialization=Protobuf")],
            "syntax",
        )])
        .unwrap();

        let error = azure_registry(&server)
            .fetch_schema_by_id(&SchemaId::Azure(SCHEMA_ID.into()))
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "schema registry error: schema type 'Protobuf' is not supported"
        );
    }

    #[test]
    fn register_without_schema_id_is_unexpected_response() {
        let server = FakeRegistryServer::start(vec![route(
            "PUT",
            "/$schemaGroups/pedidos/schemas/pedidos-value?api-version=2023-07-01",
            200,
            &[("Schema-Version", "4")],
            "",
        )])
        .unwrap();

        let error = azure_registry(&server)
            .register_schema("pedidos-value", SchemaKind::Avro, "{}")
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            "schema registry error: schema registry returned an unexpected response for PUT /$schemaGroups/pedidos/schemas/pedidos-value"
        );
    }

    #[test]
    fn schema_name_from_header_is_truncated() {
        let long_name = "n".repeat(300);
        let server = FakeRegistryServer::start(vec![route(
            "GET",
            "/$schemaGroups/pedidos/schemas/pedidos-value/versions/1?api-version=2023-07-01",
            200,
            &[
                ("Content-Type", AVRO_RESPONSE_TYPE),
                ("Schema-Id", SCHEMA_ID),
                ("Schema-Name", &long_name),
            ],
            "{}",
        )])
        .unwrap();

        let schema = azure_registry(&server)
            .fetch_schema_by_subject("pedidos-value", SchemaVersionSelector::Number(1))
            .unwrap();

        assert_eq!(
            schema.subject.as_deref().map(|name| name.chars().count()),
            Some(200)
        );
    }
}
