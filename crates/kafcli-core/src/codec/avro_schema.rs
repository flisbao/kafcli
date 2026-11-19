use std::sync::Once;

use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::schema::ResolvedSchema;
use apache_avro::writer::datum::GenericDatumWriter;

use super::avro_value_conversion::{NamedSchemas, ROOT_POINTER, avro_to_json, json_to_avro};
use crate::error::{CoreError, CoreResult};

/// Upper bound for one allocation made by apache-avro while decoding untrusted bytes.
const MAX_ALLOCATION_BYTES: usize = 64 * 1024 * 1024;
/// apache-avro counts every schema node it enters, and a nested union or field costs
/// about three of those per JSON level, so 64 kafcli levels need roughly 192 of them.
const MAX_DECODE_RECURSION_DEPTH: usize = 256;
const UNEXPECTED_VALUE: &str = "value is not valid Avro for the schema";

static APACHE_AVRO_LIMITS: Once = Once::new();

/// A parsed Avro schema, ready to encode and decode JSON values.
#[derive(Debug, Clone)]
pub struct AvroSchema {
    root: apache_avro::Schema,
    names: NamedSchemas,
}

impl AvroSchema {
    /// Parses schema text. The text comes from a file or the registry, never from a message.
    pub fn parse(text: &str) -> CoreResult<Self> {
        APACHE_AVRO_LIMITS.call_once(|| {
            apache_avro::util::max_allocation_bytes(MAX_ALLOCATION_BYTES);
            apache_avro::util::max_decode_recursion_depth(MAX_DECODE_RECURSION_DEPTH);
        });
        let invalid = |error: apache_avro::Error| {
            CoreError::InvalidInput(format!("schema is not a valid Avro schema: {error}"))
        };
        let root = apache_avro::Schema::parse_str(text).map_err(invalid)?;
        let names = {
            let resolved = ResolvedSchema::try_from(&root).map_err(invalid)?;
            resolved
                .get_names()
                .iter()
                .map(|(name, schema)| (name.clone(), (*schema).clone()))
                .collect()
        };
        GenericDatumWriter::builder(&root)
            .build()
            .map_err(invalid)?;
        GenericDatumReader::builder(&root)
            .build()
            .map_err(invalid)?;
        Ok(Self { root, names })
    }

    /// Encodes a JSON value as Avro binary datum bytes.
    pub fn encode_json(&self, value: &serde_json::Value) -> CoreResult<Vec<u8>> {
        let avro = json_to_avro(&self.root, &self.names, value, ROOT_POINTER, 0)?;
        let writer = GenericDatumWriter::builder(&self.root)
            .build()
            .map_err(|_| unexpected_value())?;
        writer
            .write_value_to_vec(avro)
            .map_err(|_| unexpected_value())
    }

    /// Decodes Avro binary datum bytes into a JSON value.
    ///
    /// The error text never contains the decoded value.
    pub fn decode_to_json(&self, bytes: &[u8]) -> Result<serde_json::Value, String> {
        let reader = GenericDatumReader::builder(&self.root)
            .build()
            .map_err(|_| UNEXPECTED_VALUE.to_string())?;
        let mut remaining = bytes;
        let value = reader
            .read_value(&mut remaining)
            .map_err(|_| UNEXPECTED_VALUE.to_string())?;
        if !remaining.is_empty() {
            return Err(format!(
                "value has {} bytes after the Avro datum",
                remaining.len()
            ));
        }
        avro_to_json(&self.root, &self.names, value, ROOT_POINTER, 0)
            .map_err(|error| error.to_string())
    }
}

fn unexpected_value() -> CoreError {
    CoreError::SchemaValidation {
        pointer: ROOT_POINTER.to_string(),
        reason: UNEXPECTED_VALUE.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn schema(text: &str) -> AvroSchema {
        AvroSchema::parse(text).expect("test schema must parse")
    }

    fn encode_error(schema: &AvroSchema, value: Value) -> String {
        schema
            .encode_json(&value)
            .expect_err("value must be rejected")
            .to_string()
    }

    const PAIR: &str = r#"{"type":"record","name":"Pair","fields":[
        {"name":"a","type":"int"},{"name":"b","type":"string"}]}"#;

    #[test]
    fn encodes_known_vector() {
        let pair = schema(PAIR);

        let bytes = pair
            .encode_json(&json!({"a": 1, "b": "x"}))
            .expect("encode");

        assert_eq!(bytes, vec![0x02, 0x02, 0x78]);
    }

    #[test]
    fn round_trips_every_primitive_and_complex_type() {
        let every = schema(
            r#"{"type":"record","name":"Every","fields":[
                {"name":"n","type":"null"},
                {"name":"flag","type":"boolean"},
                {"name":"i","type":"int"},
                {"name":"l","type":"long"},
                {"name":"f","type":"float"},
                {"name":"d","type":"double"},
                {"name":"s","type":"string"},
                {"name":"raw","type":"bytes"},
                {"name":"id","type":{"type":"fixed","name":"Id","size":3}},
                {"name":"color","type":{"type":"enum","name":"Color","symbols":["RED","GREEN"]}},
                {"name":"list","type":{"type":"array","items":"long"}},
                {"name":"map","type":{"type":"map","values":"string"}},
                {"name":"inner","type":{"type":"record","name":"Inner","fields":[
                    {"name":"x","type":"int"}]}},
                {"name":"maybe","type":["null","string"]}
            ]}"#,
        );
        let value = json!({
            "n": null,
            "flag": true,
            "i": -7,
            "l": 9_000_000_000_i64,
            "f": 1.5,
            "d": -2.25,
            "s": "olá",
            "raw": "ab\u{00ff}",
            "id": "xyz",
            "color": "GREEN",
            "list": [1, 2, 3],
            "map": {"k": "v"},
            "inner": {"x": 4},
            "maybe": "present"
        });

        let bytes = every.encode_json(&value).expect("encode");
        let decoded = every.decode_to_json(&bytes).expect("decode");

        assert_eq!(decoded, value);
    }

    #[test]
    fn nullable_union_with_default_fills_omitted_field() {
        let record = schema(
            r#"{"type":"record","name":"R","fields":[
                {"name":"a","type":["null","string"],"default":null}]}"#,
        );

        let bytes = record.encode_json(&json!({})).expect("encode");
        assert_eq!(bytes, vec![0x00]);

        let decoded = record.decode_to_json(&bytes).expect("decode");
        assert_eq!(decoded, json!({"a": null}));
    }

    #[test]
    fn union_picks_first_matching_branch() {
        let record = schema(
            r#"{"type":"record","name":"R","fields":[
                {"name":"u","type":["long","int"]}]}"#,
        );

        let bytes = record.encode_json(&json!({"u": 5})).expect("encode");

        // Branch index 0 (long) is the first one that accepts the integer.
        assert_eq!(bytes, vec![0x00, 0x0a]);
    }

    #[test]
    fn union_without_matching_branch_is_rejected() {
        let record = schema(
            r#"{"type":"record","name":"R","fields":[
                {"name":"u","type":["null","int"]}]}"#,
        );

        let error = encode_error(&record, json!({"u": "text"}));

        assert_eq!(
            error,
            "value does not match schema at '/u': matches no branch of union"
        );
    }

    #[test]
    fn unknown_field_is_rejected_with_name() {
        let pair = schema(PAIR);

        let error = encode_error(&pair, json!({"a": 1, "b": "x", "extra": 2}));

        assert_eq!(
            error,
            "value does not match schema at '/': unknown field 'extra'"
        );
    }

    #[test]
    fn missing_field_without_default_is_rejected() {
        let pair = schema(PAIR);

        let error = encode_error(&pair, json!({"a": 1}));

        assert_eq!(
            error,
            "value does not match schema at '/': missing field 'b'"
        );
    }

    #[test]
    fn enum_symbol_is_checked() {
        let record = schema(
            r#"{"type":"record","name":"R","fields":[
                {"name":"c","type":{"type":"enum","name":"Color","symbols":["RED"]}}]}"#,
        );

        let error = encode_error(&record, json!({"c": "BLUE"}));

        assert_eq!(
            error,
            "value does not match schema at '/c': not a symbol of enum 'Color'"
        );
    }

    #[test]
    fn bytes_use_code_point_mapping() {
        let raw = schema(r#"{"type":"bytes"}"#);

        let bytes = raw.encode_json(&json!("ÿ\u{0000}")).expect("encode");
        assert_eq!(bytes, vec![0x04, 0xff, 0x00]);

        let decoded = raw.decode_to_json(&bytes).expect("decode");
        assert_eq!(decoded, json!("ÿ\u{0000}"));
    }

    #[test]
    fn bytes_above_code_point_ff_are_rejected() {
        let raw = schema(r#"{"type":"bytes"}"#);

        let error = encode_error(&raw, json!("Ā"));

        assert_eq!(
            error,
            "value does not match schema at '/': character above U+00FF in bytes"
        );
    }

    #[test]
    fn fixed_size_is_checked() {
        let record = schema(
            r#"{"type":"record","name":"R","fields":[
                {"name":"id","type":{"type":"fixed","name":"Id","size":2}}]}"#,
        );

        let error = encode_error(&record, json!({"id": "abc"}));

        assert_eq!(
            error,
            "value does not match schema at '/id': fixed 'Id' needs 2 bytes"
        );
    }

    #[test]
    fn int_range_is_checked() {
        let record = schema(
            r#"{"type":"record","name":"R","fields":[
                {"name":"i","type":"int"},{"name":"l","type":"long"}]}"#,
        );

        assert_eq!(
            encode_error(&record, json!({"i": 2147483648_i64, "l": 1})),
            "value does not match schema at '/i': integer out of range for int"
        );
        assert_eq!(
            encode_error(&record, json!({"i": 1.5, "l": 1})),
            "value does not match schema at '/i': expected int"
        );
        assert_eq!(
            encode_error(&record, json!({"i": 1, "l": u64::MAX})),
            "value does not match schema at '/l': integer out of range for long"
        );
        assert_eq!(
            encode_error(&record, json!({"i": 1, "l": 2.5})),
            "value does not match schema at '/l': expected long"
        );
    }

    #[test]
    fn error_pointer_names_nested_path() {
        let order = schema(
            r#"{"type":"record","name":"Order","fields":[
                {"name":"items","type":{"type":"array","items":{"type":"record","name":"Item",
                    "fields":[{"name":"price","type":"int"}]}}}]}"#,
        );

        let error = encode_error(&order, json!({"items": [{"price": 1}, {"price": "caro"}]}));

        assert_eq!(
            error,
            "value does not match schema at '/items/1/price': expected int"
        );
    }

    #[test]
    fn validation_error_never_contains_payload() {
        let record =
            schema(r#"{"type":"record","name":"R","fields":[{"name":"cpf","type":"int"}]}"#);

        let error = encode_error(&record, json!({"cpf": "segredo-cpf-123"}));

        assert!(
            !error.contains("segredo-cpf-123"),
            "error leaked payload: {error}"
        );
        assert_eq!(error, "value does not match schema at '/cpf': expected int");
    }

    #[test]
    fn trailing_bytes_are_decode_error() {
        let pair = schema(PAIR);
        let mut bytes = pair
            .encode_json(&json!({"a": 1, "b": "x"}))
            .expect("encode");
        bytes.push(0x00);

        let error = pair
            .decode_to_json(&bytes)
            .expect_err("trailing bytes must fail");

        assert_eq!(error, "value has 1 bytes after the Avro datum");
    }

    #[test]
    fn recursive_schema_through_ref_round_trips() {
        let node = schema(
            r#"{"type":"record","name":"Node","fields":[
                {"name":"value","type":"int"},
                {"name":"next","type":["null","Node"]}]}"#,
        );
        let value = json!({"value": 1, "next": {"value": 2, "next": {"value": 3, "next": null}}});

        let bytes = node.encode_json(&value).expect("encode");
        let decoded = node.decode_to_json(&bytes).expect("decode");

        assert_eq!(decoded, value);
    }

    #[test]
    fn nesting_limit_is_enforced() {
        let node = schema(
            r#"{"type":"record","name":"Node","fields":[
                {"name":"next","type":["null","Node"]}]}"#,
        );
        let mut value = json!({"next": null});
        for _ in 0..70 {
            value = json!({"next": value});
        }

        let error = encode_error(&node, value);

        assert!(
            error.ends_with(": value nests deeper than 64 levels"),
            "{error}"
        );
    }

    #[test]
    fn non_finite_doubles_round_trip_as_strings() {
        let doubles = schema(
            r#"{"type":"record","name":"D","fields":[
                {"name":"a","type":"double"},{"name":"b","type":"double"},
                {"name":"c","type":"float"}]}"#,
        );
        let value = json!({"a": "NaN", "b": "-Infinity", "c": "Infinity"});

        let bytes = doubles.encode_json(&value).expect("encode");
        let decoded = doubles.decode_to_json(&bytes).expect("decode");

        assert_eq!(decoded, value);
    }

    #[test]
    fn logical_types_round_trip() {
        let logical = schema(
            r#"{"type":"record","name":"L","fields":[
                {"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}},
                {"name":"day","type":{"type":"int","logicalType":"date"}},
                {"name":"id","type":{"type":"string","logicalType":"uuid"}},
                {"name":"amount","type":{"type":"bytes","logicalType":"decimal",
                    "precision":4,"scale":2}}]}"#,
        );
        let value = json!({
            "at": 1_700_000_000_000_i64,
            "day": 20_000,
            "id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
            "amount": "\u{0004}\u{00d2}"
        });

        let bytes = logical.encode_json(&value).expect("encode");
        let decoded = logical.decode_to_json(&bytes).expect("decode");

        assert_eq!(decoded, value);
    }

    #[test]
    fn invalid_schema_text_is_rejected() {
        let error = AvroSchema::parse("{not a schema").expect_err("must reject");

        assert!(
            error
                .to_string()
                .starts_with("invalid input: schema is not a valid Avro schema: "),
            "{error}"
        );
    }

    const NODE: &str = r#"{"type":"record","name":"Node","fields":[
        {"name":"value","type":"int"},
        {"name":"next","type":["null","Node"]}]}"#;

    /// Unsigned LEB128 varint, as Avro writes block counts and zig-zag longs.
    fn varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return out;
            }
            out.push(byte | 0x80);
        }
    }

    /// Hand-built `Node` datum: `levels` nodes linked together, then a final node with no next.
    fn linked_list_bytes(levels: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        for _ in 0..levels {
            // value 0, union branch 1 (Node)
            bytes.extend([0x00, 0x02]);
        }
        // value 0, union branch 0 (null)
        bytes.extend([0x00, 0x00]);
        bytes
    }

    /// Debug builds give apache-avro's recursive decoder frames tens of KiB each, so the
    /// depth tests run on a thread with a stack large enough for the 256-level budget.
    fn on_large_stack<T: Send + 'static>(test: impl FnOnce() -> T + Send + 'static) -> T {
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(test)
            .expect("spawn test thread")
            .join()
            .expect("test thread must not panic")
    }

    #[test]
    fn linked_list_30_levels_round_trips() {
        on_large_stack(|| {
            let node = schema(NODE);
            let mut value = json!({"value": 0, "next": null});
            for level in 1..30 {
                value = json!({"value": level, "next": value});
            }

            let bytes = node.encode_json(&value).expect("encode");
            let decoded = node.decode_to_json(&bytes).expect("decode");

            assert_eq!(decoded, value);
        });
    }

    #[test]
    fn bytes_nested_beyond_64_levels_are_rejected_on_decode() {
        on_large_stack(|| {
            let node = schema(NODE);

            let error = node
                .decode_to_json(&linked_list_bytes(70))
                .expect_err("70 levels must exceed the 64-level limit");
            assert!(
                error.ends_with(": value nests deeper than 64 levels"),
                "{error}"
            );

            let error = node
                .decode_to_json(&linked_list_bytes(300))
                .expect_err("deeper bytes must fail without overflowing the stack");
            assert_eq!(error, "value is not valid Avro for the schema");
        });
    }

    #[test]
    fn hostile_100k_level_datum_stops_at_recursion_limit() {
        let error = on_large_stack(|| {
            schema(NODE)
                .decode_to_json(&linked_list_bytes(100_000))
                .expect_err("100k levels must stop at apache-avro's recursion limit")
        });

        assert_eq!(error, "value is not valid Avro for the schema");
    }

    #[test]
    fn zero_width_array_with_huge_block_count_is_rejected() {
        let array = schema(r#"{"type":"array","items":"null"}"#);
        // Block count 2^40 of zero-width items, zig-zag encoded, then one more small block.
        let mut bytes = varint(1 << 41);
        bytes.extend(varint(2));
        bytes.push(0x00);

        let error = array
            .decode_to_json(&bytes)
            .expect_err("a 2^40 block count must not be materialized");

        assert_eq!(error, "value is not valid Avro for the schema");
    }

    #[test]
    fn zero_width_items_share_one_budget_per_datum() {
        let array = schema(r#"{"type":"array","items":"null"}"#);
        // 100 blocks of 2^20 zero-width items: far beyond the per-datum allocation budget.
        let mut bytes = Vec::new();
        for _ in 0..100 {
            bytes.extend(varint(1 << 21));
        }
        bytes.push(0x00);

        let error = array
            .decode_to_json(&bytes)
            .expect_err("items across blocks must share one budget");

        assert_eq!(error, "value is not valid Avro for the schema");
    }
}
