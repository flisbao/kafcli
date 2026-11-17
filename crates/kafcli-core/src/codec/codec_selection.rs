//! Chooses the value codec from the format, wire and schema source the user gave, and rejects the
//! combinations that cannot work before anything is sent or consumed.
//!
//! Schemas come only from a file or from the registry. `produce` never registers a schema and
//! never parses a schema taken from a message.

use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use super::avro_schema::AvroSchema;
use super::azure_avro_codec::{AzureAvroDecoder, AzureAvroEncoder};
use super::confluent_codec::{ConfluentAvroEncoder, ConfluentDecoder, ConfluentJsonEncoder};
use super::json_schema_validator::JsonSchemaValidator;
use super::raw_avro_codec::RawAvroCodec;
use super::raw_json_codec::RawJsonCodec;
use super::schema_validated_json_codec::SchemaValidatedJsonCodec;
use super::value_codec::{ValueDecoder, ValueEncoder};
use crate::error::{CoreError, CoreResult};
use crate::registry::registered_schema::{SchemaId, SchemaKind, SchemaVersionSelector};
use crate::registry::registry_text::neutralize_registry_text;
use crate::registry::schema_registry::SchemaRegistry;

const MAX_SCHEMA_FILE_BYTES: u64 = 1024 * 1024;
const NEEDS_SCHEMA_SOURCE: &str =
    "--format avro needs --schema-file, --schema-subject or --schema-id";
const MISSING_SCHEMA_SOURCE: &str = "a schema source is required for this format";
const NO_VALIDATE_WITH_AVRO: &str = "--no-validate only applies to --format json";
const AZURE_JSON_UNSUPPORTED: &str = "--format json with --wire azure is not supported: the Azure JSON Schema wire format is not documented";
const REGISTRY_REQUIRED: &str = "a schema registry is required for this format";
const RAW_JSON_SCHEMA_FLAGS: &str =
    "--schema-* flags are not used by --format json --wire raw on consume";

/// Value format chosen with `--format`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueFormat {
    Json,
    Avro,
}

/// Wire format chosen with `--wire`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireFormat {
    Raw,
    Confluent,
    Azure,
}

/// Where the schema comes from: nowhere, a file, or the registry by subject or by id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaSource {
    Absent,
    File(PathBuf),
    Subject {
        subject: String,
        version: SchemaVersionSelector,
    },
    Id(String),
}

/// Everything the encoder choice depends on. `validate` is `false` with `--no-validate`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncoderSelection {
    pub format: ValueFormat,
    pub wire: WireFormat,
    pub schema: SchemaSource,
    pub validate: bool,
}

/// An encoder, and the registry id of its schema when the schema came from the registry.
pub struct SelectedEncoder {
    pub encoder: Box<dyn ValueEncoder>,
    pub schema_id: Option<String>,
}

/// Everything the decoder choice depends on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecoderSelection {
    pub format: ValueFormat,
    pub wire: WireFormat,
    pub schema: SchemaSource,
}

/// Whether building this codec needs a schema registry client.
pub fn selection_needs_registry(
    _format: ValueFormat,
    wire: WireFormat,
    schema: &SchemaSource,
) -> bool {
    wire != WireFormat::Raw || matches!(schema, SchemaSource::Subject { .. } | SchemaSource::Id(_))
}

/// Builds the encoder for `--format`, `--wire` and the schema source.
pub fn build_value_encoder(
    selection: &EncoderSelection,
    registry: Option<&dyn SchemaRegistry>,
) -> CoreResult<SelectedEncoder> {
    if selection.format == ValueFormat::Avro && !selection.validate {
        return Err(invalid(NO_VALIDATE_WITH_AVRO));
    }
    match selection.wire {
        WireFormat::Raw => build_raw_encoder(selection, registry),
        WireFormat::Confluent => build_confluent_encoder(selection, registry),
        WireFormat::Azure => build_azure_encoder(selection, registry),
    }
}

fn build_raw_encoder(
    selection: &EncoderSelection,
    registry: Option<&dyn SchemaRegistry>,
) -> CoreResult<SelectedEncoder> {
    match (selection.format, &selection.schema) {
        (ValueFormat::Json, SchemaSource::Absent) => Ok(unchecked_json_encoder()),
        (ValueFormat::Json, _) if !selection.validate => Ok(unchecked_json_encoder()),
        (ValueFormat::Json, _) => {
            let loaded = load_schema(
                &selection.schema,
                SchemaKind::Json,
                WireFormat::Raw,
                registry,
            )?;
            let validator = JsonSchemaValidator::parse(&loaded.text)?;
            Ok(SelectedEncoder {
                encoder: Box::new(SchemaValidatedJsonCodec::new(validator)),
                schema_id: loaded.schema_id(),
            })
        }
        (ValueFormat::Avro, SchemaSource::Absent) => Err(invalid(NEEDS_SCHEMA_SOURCE)),
        (ValueFormat::Avro, _) => {
            let loaded = load_schema(
                &selection.schema,
                SchemaKind::Avro,
                WireFormat::Raw,
                registry,
            )?;
            let schema = AvroSchema::parse(&loaded.text)?;
            Ok(SelectedEncoder {
                encoder: Box::new(RawAvroCodec::new(schema)),
                schema_id: loaded.schema_id(),
            })
        }
    }
}

fn unchecked_json_encoder() -> SelectedEncoder {
    SelectedEncoder {
        encoder: Box::new(RawJsonCodec),
        schema_id: None,
    }
}

fn build_confluent_encoder(
    selection: &EncoderSelection,
    registry: Option<&dyn SchemaRegistry>,
) -> CoreResult<SelectedEncoder> {
    require_registry_source(&selection.schema, WireFormat::Confluent)?;
    let loaded = load_schema(
        &selection.schema,
        kind_of(selection.format),
        WireFormat::Confluent,
        registry,
    )?;
    let schema_id = confluent_number(loaded.registry_id.as_ref())?;
    let encoder: Box<dyn ValueEncoder> = match selection.format {
        ValueFormat::Avro => Box::new(ConfluentAvroEncoder::new(
            schema_id,
            AvroSchema::parse(&loaded.text)?,
        )),
        ValueFormat::Json => {
            let validator = if selection.validate {
                Some(JsonSchemaValidator::parse(&loaded.text)?)
            } else {
                None
            };
            Box::new(ConfluentJsonEncoder::new(schema_id, validator))
        }
    };
    Ok(SelectedEncoder {
        encoder,
        schema_id: Some(schema_id.to_string()),
    })
}

fn build_azure_encoder(
    selection: &EncoderSelection,
    registry: Option<&dyn SchemaRegistry>,
) -> CoreResult<SelectedEncoder> {
    if selection.format == ValueFormat::Json {
        return Err(invalid(AZURE_JSON_UNSUPPORTED));
    }
    require_registry_source(&selection.schema, WireFormat::Azure)?;
    let loaded = load_schema(
        &selection.schema,
        SchemaKind::Avro,
        WireFormat::Azure,
        registry,
    )?;
    let schema_id = azure_text(loaded.registry_id.as_ref())?;
    let schema = AvroSchema::parse(&loaded.text)?;
    Ok(SelectedEncoder {
        encoder: Box::new(AzureAvroEncoder::new(schema_id.clone(), schema)),
        schema_id: Some(schema_id),
    })
}

/// Builds the decoder for `--format`, `--wire` and the schema source.
///
/// The registry is moved into the decoder, which keeps it for every message of the run.
pub fn build_value_decoder(
    selection: &DecoderSelection,
    registry: Option<Box<dyn SchemaRegistry>>,
) -> CoreResult<Box<dyn ValueDecoder>> {
    match selection.wire {
        WireFormat::Raw => build_raw_decoder(selection, registry.as_deref()),
        WireFormat::Confluent => build_confluent_decoder(selection, registry),
        WireFormat::Azure => build_azure_decoder(selection, registry),
    }
}

fn build_raw_decoder(
    selection: &DecoderSelection,
    registry: Option<&dyn SchemaRegistry>,
) -> CoreResult<Box<dyn ValueDecoder>> {
    match (selection.format, &selection.schema) {
        (ValueFormat::Json, SchemaSource::Absent) => Ok(Box::new(RawJsonCodec)),
        (ValueFormat::Json, _) => Err(invalid(RAW_JSON_SCHEMA_FLAGS)),
        (ValueFormat::Avro, SchemaSource::Absent) => Err(invalid(NEEDS_SCHEMA_SOURCE)),
        (ValueFormat::Avro, _) => {
            let loaded = load_schema(
                &selection.schema,
                SchemaKind::Avro,
                WireFormat::Raw,
                registry,
            )?;
            Ok(Box::new(RawAvroCodec::new(AvroSchema::parse(
                &loaded.text,
            )?)))
        }
    }
}

fn build_confluent_decoder(
    selection: &DecoderSelection,
    registry: Option<Box<dyn SchemaRegistry>>,
) -> CoreResult<Box<dyn ValueDecoder>> {
    reject_wire_schema_flags(&selection.schema, WireFormat::Confluent)?;
    let registry = checked_registry(registry, WireFormat::Confluent)?;
    Ok(Box::new(ConfluentDecoder::new(
        registry,
        kind_of(selection.format),
    )))
}

fn build_azure_decoder(
    selection: &DecoderSelection,
    registry: Option<Box<dyn SchemaRegistry>>,
) -> CoreResult<Box<dyn ValueDecoder>> {
    if selection.format == ValueFormat::Json {
        return Err(invalid(AZURE_JSON_UNSUPPORTED));
    }
    reject_wire_schema_flags(&selection.schema, WireFormat::Azure)?;
    let registry = checked_registry(registry, WireFormat::Azure)?;
    Ok(Box::new(AzureAvroDecoder::new(registry)))
}

/// Schema text and, for a registry schema, the id it was fetched under.
struct LoadedSchema {
    text: String,
    registry_id: Option<SchemaId>,
}

impl LoadedSchema {
    fn schema_id(&self) -> Option<String> {
        self.registry_id.as_ref().map(ToString::to_string)
    }
}

/// Reads the schema text from its source and checks that it is the kind `--format` asks for.
///
/// A file has no kind of its own: the kind comes from `--format`, so only the registry can disagree.
fn load_schema(
    source: &SchemaSource,
    expected: SchemaKind,
    wire: WireFormat,
    registry: Option<&dyn SchemaRegistry>,
) -> CoreResult<LoadedSchema> {
    match source {
        SchemaSource::Absent => Err(invalid(MISSING_SCHEMA_SOURCE)),
        SchemaSource::File(path) => Ok(LoadedSchema {
            text: read_schema_file(path)?,
            registry_id: None,
        }),
        SchemaSource::Subject { subject, version } => {
            let registry = require_registry(registry, wire)?;
            let registered = registry.fetch_schema_by_subject(subject, *version)?;
            ensure_kind(subject, expected, registered.kind)?;
            Ok(LoadedSchema {
                text: registered.text,
                registry_id: Some(registered.id),
            })
        }
        SchemaSource::Id(text) => {
            let registry = require_registry(registry, wire)?;
            let id = schema_id_from_text(text, wire, registry.registry_type())?;
            let registered = registry.fetch_schema_by_id(&id)?;
            ensure_kind(text, expected, registered.kind)?;
            Ok(LoadedSchema {
                text: registered.text,
                registry_id: Some(registered.id),
            })
        }
    }
}

fn require_registry_source(schema: &SchemaSource, wire: WireFormat) -> CoreResult<()> {
    let name = wire_name(wire);
    match schema {
        SchemaSource::Absent => Err(invalid(format!(
            "--wire {name} needs --schema-subject or --schema-id"
        ))),
        SchemaSource::File(_) => Err(invalid(format!(
            "--schema-file cannot be used with --wire {name}: the schema id must come from the registry"
        ))),
        SchemaSource::Subject { .. } | SchemaSource::Id(_) => Ok(()),
    }
}

fn reject_wire_schema_flags(schema: &SchemaSource, wire: WireFormat) -> CoreResult<()> {
    if matches!(schema, SchemaSource::Absent) {
        return Ok(());
    }
    Err(invalid(format!(
        "--wire {} reads the schema id from each message; remove --schema-file, --schema-subject and --schema-id",
        wire_name(wire)
    )))
}

fn require_registry(
    registry: Option<&dyn SchemaRegistry>,
    wire: WireFormat,
) -> CoreResult<&dyn SchemaRegistry> {
    let registry = registry.ok_or_else(|| invalid(REGISTRY_REQUIRED))?;
    check_registry_type(registry, wire)?;
    Ok(registry)
}

fn checked_registry(
    registry: Option<Box<dyn SchemaRegistry>>,
    wire: WireFormat,
) -> CoreResult<Box<dyn SchemaRegistry>> {
    let registry = registry.ok_or_else(|| invalid(REGISTRY_REQUIRED))?;
    check_registry_type(registry.as_ref(), wire)?;
    Ok(registry)
}

/// A Confluent or Azure wire needs the registry of the same type; `raw` accepts either.
fn check_registry_type(registry: &dyn SchemaRegistry, wire: WireFormat) -> CoreResult<()> {
    if wire == WireFormat::Raw {
        return Ok(());
    }
    let required = wire_name(wire);
    let actual = registry.registry_type();
    if actual == required {
        return Ok(());
    }
    Err(invalid(format!(
        "--wire {required} needs a {required} schema_registry, but the context has {actual}"
    )))
}

/// Reads an id the user typed. A Confluent id must be digits; with `raw` the registry type decides.
fn schema_id_from_text(text: &str, wire: WireFormat, registry_type: &str) -> CoreResult<SchemaId> {
    let confluent = match wire {
        WireFormat::Confluent => true,
        WireFormat::Azure => false,
        WireFormat::Raw => registry_type == "confluent",
    };
    if confluent {
        return parse_confluent_id(text).map(SchemaId::Confluent);
    }
    Ok(SchemaId::Azure(text.to_string()))
}

fn parse_confluent_id(text: &str) -> CoreResult<u32> {
    let digits_only = !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
    if let Some(number) = digits_only.then(|| text.parse::<u32>().ok()).flatten() {
        return Ok(number);
    }
    Err(invalid(format!(
        "invalid Confluent schema id '{}'",
        neutralize_registry_text(text)
    )))
}

fn confluent_number(id: Option<&SchemaId>) -> CoreResult<u32> {
    match id {
        Some(SchemaId::Confluent(number)) => Ok(*number),
        _ => Err(CoreError::SchemaRegistry(
            "registry returned a schema id that does not match --wire confluent".to_string(),
        )),
    }
}

fn azure_text(id: Option<&SchemaId>) -> CoreResult<String> {
    match id {
        Some(SchemaId::Azure(text)) => Ok(text.clone()),
        _ => Err(CoreError::SchemaRegistry(
            "registry returned a schema id that does not match --wire azure".to_string(),
        )),
    }
}

fn ensure_kind(name: &str, expected: SchemaKind, actual: SchemaKind) -> CoreResult<()> {
    if expected == actual {
        return Ok(());
    }
    let name = neutralize_registry_text(name);
    let found = match actual {
        SchemaKind::Json => "a JSON Schema",
        SchemaKind::Avro => "an Avro schema",
    };
    Err(invalid(format!(
        "schema '{name}' is {found}, but --format {} was given",
        expected.name()
    )))
}

/// Reads a schema file of at most 1 MiB. The file is never read past the limit.
fn read_schema_file(path: &Path) -> CoreResult<String> {
    let shown = neutralize_registry_text(&path.display().to_string());
    let unreadable =
        |error: io::Error| invalid(format!("schema file '{shown}' could not be read: {error}"));
    let file = File::open(path).map_err(unreadable)?;
    let mut bytes = Vec::new();
    file.take(MAX_SCHEMA_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(unreadable)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_SCHEMA_FILE_BYTES {
        return Err(invalid("schema file larger than 1 MiB"));
    }
    String::from_utf8(bytes).map_err(|_| invalid("schema file is not valid UTF-8"))
}

fn kind_of(format: ValueFormat) -> SchemaKind {
    match format {
        ValueFormat::Json => SchemaKind::Json,
        ValueFormat::Avro => SchemaKind::Avro,
    }
}

fn wire_name(wire: WireFormat) -> &'static str {
    match wire {
        WireFormat::Raw => "raw",
        WireFormat::Confluent => "confluent",
        WireFormat::Azure => "azure",
    }
}

fn invalid(text: impl Into<String>) -> CoreError {
    CoreError::InvalidInput(text.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::schema_registry_model::{
        AzureRegistryConfig, ConfluentRegistryConfig, RegistryAuthConfig, SchemaRegistryConfig,
    };
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
    use crate::registry::registry_factory::build_schema_registry;
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use serde_json::json;
    use std::io::Write;
    use std::time::Duration;
    use tempfile::NamedTempFile;

    const PAIR: &str = r#"{"type":"record","name":"Pair","fields":[
        {"name":"a","type":"int"},{"name":"b","type":"string"}]}"#;
    const OBJECT: &str =
        r#"{"type":"object","required":["id"],"properties":{"id":{"type":"integer"}}}"#;
    const AZURE_SCHEMA_ID: &str = "0f1e2d3c4b5a69788796a5b4c3d2e1f0";

    fn json_route(path: &str, body: serde_json::Value) -> FakeRoute {
        FakeRoute {
            method: "GET".into(),
            path_and_query: path.into(),
            status: 200,
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    /// Confluent registry: id 7 is Avro, id 9 is JSON Schema; `pedidos-avro` and `pedidos-value` are subjects.
    fn confluent_routes() -> Vec<FakeRoute> {
        vec![
            json_route("/schemas/ids/7", json!({"schema": PAIR})),
            json_route(
                "/schemas/ids/9",
                json!({"schema": OBJECT, "schemaType": "JSON"}),
            ),
            json_route(
                "/subjects/pedidos-avro/versions/latest",
                json!({"subject": "pedidos-avro", "id": 7, "version": 1, "schema": PAIR}),
            ),
            json_route(
                "/subjects/pedidos-value/versions/latest",
                json!({"subject": "pedidos-value", "id": 9, "version": 2, "schema": OBJECT, "schemaType": "JSON"}),
            ),
        ]
    }

    fn azure_routes() -> Vec<FakeRoute> {
        vec![FakeRoute {
            method: "GET".into(),
            path_and_query: format!(
                "/$schemaGroups/$schemas/{AZURE_SCHEMA_ID}?api-version=2023-07-01"
            ),
            status: 200,
            headers: vec![(
                "Content-Type".into(),
                "application/json; serialization=Avro".into(),
            )],
            body: PAIR.into(),
        }]
    }

    fn confluent_registry(server: &FakeRegistryServer) -> Box<dyn SchemaRegistry> {
        let config = SchemaRegistryConfig::Confluent(ConfluentRegistryConfig {
            url: server.url(),
            auth: RegistryAuthConfig::Anonymous,
            ca_location: None,
        });
        build_schema_registry(
            &config,
            &FixedSecretResolver::returning("unused"),
            Duration::from_secs(5),
        )
        .expect("confluent registry builds")
    }

    fn azure_registry(server: &FakeRegistryServer) -> Box<dyn SchemaRegistry> {
        let config = SchemaRegistryConfig::Azure(AzureRegistryConfig {
            url: server.url(),
            group: "pedidos".into(),
            auth: RegistryAuthConfig::Anonymous,
        });
        build_schema_registry(
            &config,
            &FixedSecretResolver::returning("unused"),
            Duration::from_secs(5),
        )
        .expect("azure registry builds")
    }

    fn schema_file(text: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(text.as_bytes()).expect("write schema");
        file
    }

    fn file_source(file: &NamedTempFile) -> SchemaSource {
        SchemaSource::File(file.path().to_path_buf())
    }

    fn encoder_selection(
        format: ValueFormat,
        wire: WireFormat,
        schema: SchemaSource,
        validate: bool,
    ) -> EncoderSelection {
        EncoderSelection {
            format,
            wire,
            schema,
            validate,
        }
    }

    fn decoder_selection(
        format: ValueFormat,
        wire: WireFormat,
        schema: SchemaSource,
    ) -> DecoderSelection {
        DecoderSelection {
            format,
            wire,
            schema,
        }
    }

    fn subject(name: &str) -> SchemaSource {
        SchemaSource::Subject {
            subject: name.into(),
            version: SchemaVersionSelector::Latest,
        }
    }

    fn id(text: &str) -> SchemaSource {
        SchemaSource::Id(text.into())
    }

    /// Text of an `InvalidInput` error, the only variant selection errors use.
    fn input_text(error: CoreError) -> String {
        match error {
            CoreError::InvalidInput(text) => text,
            other => panic!("expected InvalidInput, got {other:?}"),
        }
    }

    fn encoder_error(result: CoreResult<SelectedEncoder>) -> String {
        input_text(result.err().expect("selection must be rejected"))
    }

    fn decoder_error(result: CoreResult<Box<dyn ValueDecoder>>) -> String {
        input_text(result.err().expect("selection must be rejected"))
    }

    #[test]
    fn json_raw_without_schema_is_plain() {
        let selected = build_value_encoder(
            &encoder_selection(
                ValueFormat::Json,
                WireFormat::Raw,
                SchemaSource::Absent,
                true,
            ),
            None,
        )
        .expect("plain JSON needs no schema");

        assert_eq!(selected.schema_id, None);
        let encoded = selected
            .encoder
            .encode_value(&json!({"id": "anything"}))
            .expect("plain JSON encodes any value");
        assert_eq!(encoded.bytes, br#"{"id":"anything"}"#);
        assert!(encoded.headers.is_empty());
    }

    #[test]
    fn json_raw_with_file_validates() {
        let file = schema_file(OBJECT);
        let selected = build_value_encoder(
            &encoder_selection(ValueFormat::Json, WireFormat::Raw, file_source(&file), true),
            None,
        )
        .expect("JSON Schema file loads");

        assert_eq!(selected.schema_id, None);
        selected
            .encoder
            .encode_value(&json!({"id": 3}))
            .expect("valid value encodes");
        let error = selected
            .encoder
            .encode_value(&json!({"id": "seven"}))
            .expect_err("invalid value must not encode");
        assert!(
            matches!(error, CoreError::SchemaValidation { .. }),
            "{error:?}"
        );
    }

    #[test]
    fn json_raw_no_validate_ignores_schema() {
        let file = schema_file(OBJECT);
        let selected = build_value_encoder(
            &encoder_selection(
                ValueFormat::Json,
                WireFormat::Raw,
                file_source(&file),
                false,
            ),
            None,
        )
        .expect("no-validate JSON is plain");

        let encoded = selected
            .encoder
            .encode_value(&json!({"id": "not checked"}))
            .expect("unvalidated value encodes");
        assert_eq!(encoded.bytes, br#"{"id":"not checked"}"#);
    }

    #[test]
    fn schema_file_larger_than_one_mib_is_rejected() {
        let limit = 1024 * 1024;
        let oversized = schema_file(&" ".repeat(limit + 1));

        let error = encoder_error(build_value_encoder(
            &encoder_selection(
                ValueFormat::Json,
                WireFormat::Raw,
                file_source(&oversized),
                true,
            ),
            None,
        ));

        assert_eq!(error, "schema file larger than 1 MiB");
    }

    #[test]
    fn schema_file_of_exactly_one_mib_is_read() {
        let limit = 1024 * 1024;
        let padded = format!("{OBJECT}{}", " ".repeat(limit - OBJECT.len()));
        let file = schema_file(&padded);

        build_value_encoder(
            &encoder_selection(ValueFormat::Json, WireFormat::Raw, file_source(&file), true),
            None,
        )
        .expect("a schema of exactly 1 MiB is accepted");
    }

    #[test]
    fn avro_raw_needs_schema() {
        let error = encoder_error(build_value_encoder(
            &encoder_selection(
                ValueFormat::Avro,
                WireFormat::Raw,
                SchemaSource::Absent,
                true,
            ),
            None,
        ));

        assert_eq!(
            error,
            "--format avro needs --schema-file, --schema-subject or --schema-id"
        );
    }

    #[test]
    fn confluent_needs_registry_source() {
        let confluent = encoder_error(build_value_encoder(
            &encoder_selection(
                ValueFormat::Json,
                WireFormat::Confluent,
                SchemaSource::Absent,
                true,
            ),
            None,
        ));
        let azure = encoder_error(build_value_encoder(
            &encoder_selection(
                ValueFormat::Avro,
                WireFormat::Azure,
                SchemaSource::Absent,
                true,
            ),
            None,
        ));

        assert_eq!(
            confluent,
            "--wire confluent needs --schema-subject or --schema-id"
        );
        assert_eq!(azure, "--wire azure needs --schema-subject or --schema-id");
    }

    #[test]
    fn file_with_confluent_is_rejected() {
        let file = schema_file(PAIR);

        let confluent = encoder_error(build_value_encoder(
            &encoder_selection(
                ValueFormat::Avro,
                WireFormat::Confluent,
                file_source(&file),
                true,
            ),
            None,
        ));
        let azure = encoder_error(build_value_encoder(
            &encoder_selection(
                ValueFormat::Avro,
                WireFormat::Azure,
                file_source(&file),
                true,
            ),
            None,
        ));

        assert_eq!(
            confluent,
            "--schema-file cannot be used with --wire confluent: the schema id must come from the registry"
        );
        assert_eq!(
            azure,
            "--schema-file cannot be used with --wire azure: the schema id must come from the registry"
        );
    }

    #[test]
    fn json_azure_is_rejected() {
        let server = FakeRegistryServer::start(azure_routes()).expect("server starts");
        let registry = azure_registry(&server);
        let expected = "--format json with --wire azure is not supported: the Azure JSON Schema wire format is not documented";

        let encoder = encoder_error(build_value_encoder(
            &encoder_selection(
                ValueFormat::Json,
                WireFormat::Azure,
                id(AZURE_SCHEMA_ID),
                true,
            ),
            Some(registry.as_ref()),
        ));
        let decoder = decoder_error(build_value_decoder(
            &decoder_selection(ValueFormat::Json, WireFormat::Azure, SchemaSource::Absent),
            Some(registry),
        ));

        assert_eq!(encoder, expected);
        assert_eq!(decoder, expected);
    }

    #[test]
    fn kind_mismatch_is_rejected() {
        let server = FakeRegistryServer::start(confluent_routes()).expect("server starts");
        let registry = confluent_registry(&server);

        let json_format_avro_subject = encoder_error(build_value_encoder(
            &encoder_selection(
                ValueFormat::Json,
                WireFormat::Confluent,
                subject("pedidos-avro"),
                true,
            ),
            Some(registry.as_ref()),
        ));
        let avro_format_json_id = encoder_error(build_value_encoder(
            &encoder_selection(ValueFormat::Avro, WireFormat::Confluent, id("9"), true),
            Some(registry.as_ref()),
        ));
        let decoder_avro_subject = decoder_error(build_value_decoder(
            &decoder_selection(ValueFormat::Avro, WireFormat::Raw, subject("pedidos-value")),
            Some(confluent_registry(&server)),
        ));

        assert_eq!(
            json_format_avro_subject,
            "schema 'pedidos-avro' is an Avro schema, but --format json was given"
        );
        assert_eq!(
            avro_format_json_id,
            "schema '9' is a JSON Schema, but --format avro was given"
        );
        assert_eq!(
            decoder_avro_subject,
            "schema 'pedidos-value' is a JSON Schema, but --format avro was given"
        );
    }

    #[test]
    fn registry_type_must_match_wire() {
        let confluent_server =
            FakeRegistryServer::start(confluent_routes()).expect("server starts");
        let azure_server = FakeRegistryServer::start(azure_routes()).expect("server starts");

        let confluent_on_azure = encoder_error(build_value_encoder(
            &encoder_selection(ValueFormat::Avro, WireFormat::Confluent, id("7"), true),
            Some(azure_registry(&azure_server).as_ref()),
        ));
        let azure_on_confluent = encoder_error(build_value_encoder(
            &encoder_selection(
                ValueFormat::Avro,
                WireFormat::Azure,
                id(AZURE_SCHEMA_ID),
                true,
            ),
            Some(confluent_registry(&confluent_server).as_ref()),
        ));
        let decoder_on_azure = decoder_error(build_value_decoder(
            &decoder_selection(
                ValueFormat::Avro,
                WireFormat::Confluent,
                SchemaSource::Absent,
            ),
            Some(azure_registry(&azure_server)),
        ));

        assert_eq!(
            confluent_on_azure,
            "--wire confluent needs a confluent schema_registry, but the context has azure"
        );
        assert_eq!(
            azure_on_confluent,
            "--wire azure needs a azure schema_registry, but the context has confluent"
        );
        assert_eq!(
            decoder_on_azure,
            "--wire confluent needs a confluent schema_registry, but the context has azure"
        );
    }

    #[test]
    fn raw_wire_accepts_either_registry_type() {
        let confluent_server =
            FakeRegistryServer::start(confluent_routes()).expect("server starts");
        let azure_server = FakeRegistryServer::start(azure_routes()).expect("server starts");

        build_value_encoder(
            &encoder_selection(ValueFormat::Avro, WireFormat::Raw, id("7"), true),
            Some(confluent_registry(&confluent_server).as_ref()),
        )
        .expect("raw reads a numeric id from a confluent registry");
        build_value_encoder(
            &encoder_selection(
                ValueFormat::Avro,
                WireFormat::Raw,
                id(AZURE_SCHEMA_ID),
                true,
            ),
            Some(azure_registry(&azure_server).as_ref()),
        )
        .expect("raw reads a text id from an azure registry");
    }

    #[test]
    fn subject_without_registry_is_rejected() {
        let error = encoder_error(build_value_encoder(
            &encoder_selection(
                ValueFormat::Avro,
                WireFormat::Raw,
                subject("pedidos-avro"),
                true,
            ),
            None,
        ));

        assert_eq!(error, "a schema registry is required for this format");
    }

    #[test]
    fn no_validate_with_avro_is_rejected() {
        let file = schema_file(PAIR);

        let error = encoder_error(build_value_encoder(
            &encoder_selection(
                ValueFormat::Avro,
                WireFormat::Raw,
                file_source(&file),
                false,
            ),
            None,
        ));

        assert_eq!(error, "--no-validate only applies to --format json");
    }

    #[test]
    fn invalid_confluent_id_is_rejected() {
        let server = FakeRegistryServer::start(confluent_routes()).expect("server starts");
        let registry = confluent_registry(&server);

        let encoder = encoder_error(build_value_encoder(
            &encoder_selection(ValueFormat::Avro, WireFormat::Confluent, id("seven"), true),
            Some(registry.as_ref()),
        ));
        let raw_decoder = decoder_error(build_value_decoder(
            &decoder_selection(ValueFormat::Avro, WireFormat::Raw, id("seven")),
            Some(confluent_registry(&server)),
        ));

        assert_eq!(encoder, "invalid Confluent schema id 'seven'");
        assert_eq!(raw_decoder, "invalid Confluent schema id 'seven'");
        assert!(server.recorded_requests().is_empty());
    }

    #[test]
    fn decoder_rejects_schema_flags_on_confluent() {
        let server = FakeRegistryServer::start(confluent_routes()).expect("server starts");
        let file = schema_file(PAIR);

        let with_file = decoder_error(build_value_decoder(
            &decoder_selection(ValueFormat::Avro, WireFormat::Confluent, file_source(&file)),
            Some(confluent_registry(&server)),
        ));
        let azure_server = FakeRegistryServer::start(azure_routes()).expect("server starts");
        let with_subject = decoder_error(build_value_decoder(
            &decoder_selection(
                ValueFormat::Avro,
                WireFormat::Azure,
                subject("pedidos-value"),
            ),
            Some(azure_registry(&azure_server)),
        ));
        let raw_json = decoder_error(build_value_decoder(
            &decoder_selection(ValueFormat::Json, WireFormat::Raw, id("9")),
            None,
        ));

        assert_eq!(
            with_file,
            "--wire confluent reads the schema id from each message; remove --schema-file, --schema-subject and --schema-id"
        );
        assert_eq!(
            with_subject,
            "--wire azure reads the schema id from each message; remove --schema-file, --schema-subject and --schema-id"
        );
        assert_eq!(
            raw_json,
            "--schema-* flags are not used by --format json --wire raw on consume"
        );
    }

    #[test]
    fn selected_encoder_reports_schema_id() {
        let confluent_server =
            FakeRegistryServer::start(confluent_routes()).expect("server starts");
        let azure_server = FakeRegistryServer::start(azure_routes()).expect("server starts");

        let confluent = build_value_encoder(
            &encoder_selection(ValueFormat::Avro, WireFormat::Confluent, id("7"), true),
            Some(confluent_registry(&confluent_server).as_ref()),
        )
        .expect("confluent encoder builds");
        let azure = build_value_encoder(
            &encoder_selection(
                ValueFormat::Avro,
                WireFormat::Azure,
                id(AZURE_SCHEMA_ID),
                true,
            ),
            Some(azure_registry(&azure_server).as_ref()),
        )
        .expect("azure encoder builds");
        let file = schema_file(PAIR);
        let from_file = build_value_encoder(
            &encoder_selection(ValueFormat::Avro, WireFormat::Raw, file_source(&file), true),
            None,
        )
        .expect("file encoder builds");

        assert_eq!(confluent.schema_id, Some("7".to_string()));
        assert_eq!(azure.schema_id, Some(AZURE_SCHEMA_ID.to_string()));
        assert_eq!(from_file.schema_id, None);
    }

    #[test]
    fn avro_confluent_round_trip_through_selection() {
        let server = FakeRegistryServer::start(confluent_routes()).expect("server starts");
        let value = json!({"a": 4, "b": "selected"});
        let encoded = build_value_encoder(
            &encoder_selection(
                ValueFormat::Avro,
                WireFormat::Confluent,
                subject("pedidos-avro"),
                true,
            ),
            Some(confluent_registry(&server).as_ref()),
        )
        .expect("encoder builds")
        .encoder
        .encode_value(&value)
        .expect("value encodes");
        assert_eq!(encoded.bytes[..5], [0, 0, 0, 0, 7]);

        let decoder = build_value_decoder(
            &decoder_selection(
                ValueFormat::Avro,
                WireFormat::Confluent,
                SchemaSource::Absent,
            ),
            Some(confluent_registry(&server)),
        )
        .expect("decoder builds");

        assert_eq!(
            decoder
                .decode_value(&encoded.bytes, &encoded.headers)
                .expect("decodes"),
            value
        );
    }

    #[test]
    fn selection_needs_registry_matches_wire_and_source() {
        assert!(selection_needs_registry(
            ValueFormat::Json,
            WireFormat::Confluent,
            &SchemaSource::Absent
        ));
        assert!(selection_needs_registry(
            ValueFormat::Avro,
            WireFormat::Azure,
            &SchemaSource::Absent
        ));
        assert!(selection_needs_registry(
            ValueFormat::Avro,
            WireFormat::Raw,
            &subject("pedidos-avro")
        ));
        assert!(selection_needs_registry(
            ValueFormat::Avro,
            WireFormat::Raw,
            &id("7")
        ));
        assert!(!selection_needs_registry(
            ValueFormat::Json,
            WireFormat::Raw,
            &SchemaSource::Absent
        ));
    }
}
