pub mod check_compatibility;
pub mod get_schema;
pub mod register_schema;

use kafcli_core::codec::avro_schema::AvroSchema;
use kafcli_core::codec::codec_selection::read_schema_file;
use kafcli_core::codec::json_schema_validator::JsonSchemaValidator;
use kafcli_core::registry::registered_schema::SchemaKind;
use std::path::Path;

/// Reads a schema file and checks it locally with the parser of its kind. Nothing is sent before this
/// succeeds, and the error never repeats the schema text.
pub fn read_checked_schema_text(path: &Path, kind: SchemaKind) -> anyhow::Result<String> {
    let text = read_schema_file(path)?;
    match kind {
        SchemaKind::Avro => {
            AvroSchema::parse(&text)?;
        }
        SchemaKind::Json => {
            JsonSchemaValidator::parse(&text)?;
        }
    }
    Ok(text)
}
