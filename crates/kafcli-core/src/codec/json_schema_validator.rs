use crate::error::{CoreError, CoreResult};

/// A compiled JSON Schema that checks values before they are encoded.
///
/// Remote references (`http:`, `file:`) are rejected at parse time.
#[derive(Debug)]
pub struct JsonSchemaValidator {
    validator: jsonschema::Validator,
}

impl JsonSchemaValidator {
    /// Parses and compiles a JSON Schema document.
    ///
    /// The draft comes from `$schema`; without it the crate default applies.
    pub fn parse(text: &str) -> CoreResult<Self> {
        let schema: serde_json::Value = serde_json::from_str(text)
            .map_err(|_| CoreError::InvalidInput("schema is not valid JSON".to_string()))?;
        let validator = jsonschema::validator_for(&schema).map_err(|error| {
            CoreError::InvalidInput(format!("schema is not a valid JSON Schema: {error}"))
        })?;
        Ok(Self { validator })
    }

    /// Checks a value against the schema, reporting only the first error.
    ///
    /// The error never includes the value: it names the instance pointer, the
    /// failing schema keyword and how many further errors exist.
    pub fn validate_value(&self, value: &serde_json::Value) -> CoreResult<()> {
        let mut errors = self.validator.iter_errors(value);
        let Some(first) = errors.next() else {
            return Ok(());
        };
        let additional = errors.count();

        let instance_path = first.instance_path().as_str();
        let pointer = if instance_path.is_empty() {
            "/".to_string()
        } else {
            instance_path.to_string()
        };
        let keyword = format!("failed keyword '{}'", first.schema_path());
        let reason = if additional == 0 {
            keyword
        } else {
            format!("{keyword} (and {additional} more)")
        };
        Err(CoreError::SchemaValidation { pointer, reason })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CoreError;
    use serde_json::json;

    const ORDER_SCHEMA: &str = r#"{
        "type": "object",
        "required": ["id", "items"],
        "properties": {
            "id": {"type": "integer"},
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["sku", "price"],
                    "properties": {
                        "sku": {"type": "string"},
                        "price": {"type": "number"}
                    }
                }
            }
        }
    }"#;

    fn expect_schema_validation(error: CoreError) -> (String, String) {
        match error {
            CoreError::SchemaValidation { pointer, reason } => (pointer, reason),
            other => panic!("expected SchemaValidation, got {other:?}"),
        }
    }

    #[test]
    fn valid_value_passes() {
        let validator = JsonSchemaValidator::parse(ORDER_SCHEMA).expect("schema parses");
        let value = json!({"id": 1, "items": [{"sku": "A-1", "price": 9.5}]});

        validator
            .validate_value(&value)
            .expect("valid value passes");
    }

    #[test]
    fn nested_type_error_reports_pointer_and_keyword() {
        let validator = JsonSchemaValidator::parse(ORDER_SCHEMA).expect("schema parses");
        let value = json!({
            "id": 1,
            "items": [
                {"sku": "A-1", "price": 9.5},
                {"sku": "B-2", "price": "ten"}
            ]
        });

        let error = validator
            .validate_value(&value)
            .expect_err("nested type error must fail");
        let (pointer, reason) = expect_schema_validation(error);

        assert_eq!(pointer, "/items/1/price");
        assert_eq!(
            reason,
            "failed keyword '/properties/items/items/properties/price/type'"
        );
    }

    #[test]
    fn error_never_contains_payload() {
        let validator = JsonSchemaValidator::parse(ORDER_SCHEMA).expect("schema parses");
        let value = json!({"id": "PAYLOAD-SECRET-42", "items": []});

        let error = validator
            .validate_value(&value)
            .expect_err("string id must fail");
        let rendered = error.to_string();

        assert!(
            rendered.starts_with("value does not match schema at '/id': "),
            "{rendered}"
        );
        assert!(
            !rendered.contains("PAYLOAD-SECRET-42"),
            "error leaked payload: {rendered}"
        );
    }

    #[test]
    fn reports_count_of_additional_errors() {
        let validator = JsonSchemaValidator::parse(
            r#"{
                "properties": {
                    "a": {"type": "string"},
                    "b": {"type": "string"},
                    "c": {"type": "string"}
                }
            }"#,
        )
        .expect("schema parses");
        let value = json!({"a": 1, "b": 2, "c": 3});

        let error = validator
            .validate_value(&value)
            .expect_err("three type errors must fail");
        let (pointer, reason) = expect_schema_validation(error);

        assert!(
            ["/a", "/b", "/c"].contains(&pointer.as_str()),
            "only the first error is reported: {pointer}"
        );
        assert!(
            reason.starts_with("failed keyword '/properties/"),
            "{reason}"
        );
        assert!(reason.ends_with(" (and 2 more)"), "{reason}");
    }

    #[test]
    fn local_ref_resolves() {
        let validator = JsonSchemaValidator::parse(
            r##"{
                "$defs": {"sku": {"type": "string", "pattern": "^[A-Z]{3}$"}},
                "type": "object",
                "properties": {"sku": {"$ref": "#/$defs/sku"}}
            }"##,
        )
        .expect("local ref schema parses");

        validator
            .validate_value(&json!({"sku": "ABC"}))
            .expect("matching sku passes");
        let error = validator
            .validate_value(&json!({"sku": "abc"}))
            .expect_err("sku violating the referenced pattern must fail");
        let (pointer, _) = expect_schema_validation(error);

        assert_eq!(pointer, "/sku");
    }

    #[test]
    fn remote_ref_is_rejected() {
        for reference in ["https://example.com/schema.json", "file:///etc/schema.json"] {
            let schema = json!({"$ref": reference}).to_string();

            let error = JsonSchemaValidator::parse(&schema)
                .expect_err("remote reference must not be resolved");

            match error {
                CoreError::InvalidInput(message) => assert!(
                    message.starts_with("schema is not a valid JSON Schema: "),
                    "{message}"
                ),
                other => panic!("expected InvalidInput, got {other:?}"),
            }
        }
    }

    #[test]
    fn draft_07_and_2020_12_are_detected() {
        // prefixItems is a 2020-12 keyword: draft 7 ignores it, 2020-12 enforces it.
        let draft_07 = JsonSchemaValidator::parse(
            r#"{"$schema": "http://json-schema.org/draft-07/schema#", "prefixItems": [{"type": "string"}]}"#,
        )
        .expect("draft 7 schema parses");
        let draft_2020_12 = JsonSchemaValidator::parse(
            r#"{"$schema": "https://json-schema.org/draft/2020-12/schema", "prefixItems": [{"type": "string"}]}"#,
        )
        .expect("2020-12 schema parses");
        let value = json!([1]);

        draft_07
            .validate_value(&value)
            .expect("draft 7 ignores prefixItems");
        expect_schema_validation(
            draft_2020_12
                .validate_value(&value)
                .expect_err("2020-12 enforces prefixItems"),
        );
    }

    #[test]
    fn invalid_json_schema_text_is_rejected() {
        let error = JsonSchemaValidator::parse("{not json").expect_err("broken JSON must fail");

        assert!(
            matches!(&error, CoreError::InvalidInput(message) if message == "schema is not valid JSON"),
            "{error:?}"
        );
    }
}
