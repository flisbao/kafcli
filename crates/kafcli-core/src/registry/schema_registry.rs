use super::registered_schema::{
    CompatibilityReport, RegisteredSchema, SchemaId, SchemaKind, SchemaVersionSelector,
};
use crate::error::CoreResult;

/// Operations a schema registry offers to kafcli.
pub trait SchemaRegistry {
    fn fetch_schema_by_id(&self, id: &SchemaId) -> CoreResult<RegisteredSchema>;

    fn fetch_schema_by_subject(
        &self,
        subject: &str,
        version: SchemaVersionSelector,
    ) -> CoreResult<RegisteredSchema>;

    fn register_schema(
        &self,
        subject: &str,
        kind: SchemaKind,
        text: &str,
    ) -> CoreResult<RegisteredSchema>;

    fn check_compatibility(
        &self,
        subject: &str,
        version: SchemaVersionSelector,
        kind: SchemaKind,
        text: &str,
    ) -> CoreResult<CompatibilityReport>;

    fn registry_type(&self) -> &'static str;
}
