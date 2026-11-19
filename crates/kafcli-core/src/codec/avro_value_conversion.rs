//! Schema-guided conversion between JSON values and `apache_avro::types::Value`.
//!
//! The mapping is kafcli's own (see the 02b plan), not apache-avro's JSON conversion.
//! Every error names only the JSON Pointer, the schema name and the reason, never a payload value.

use std::collections::HashMap;

use apache_avro::schema::{InnerDecimalSchema, Name, Schema};
use apache_avro::types::Value;
use apache_avro::{Decimal, Duration, Uuid};
use serde_json::Value as Json;

use crate::error::{CoreError, CoreResult};

/// Named schemas reachable from the root, keyed by full name, used to resolve `Schema::Ref`.
pub(super) type NamedSchemas = HashMap<Name, Schema>;

pub(super) const MAX_NESTING_DEPTH: usize = 64;
pub(super) const ROOT_POINTER: &str = "/";
const NESTING_REASON: &str = "value nests deeper than 64 levels";
const UNEXPECTED_REASON: &str = "value is not valid Avro for the schema";

/// Converts a JSON value into the Avro value that `schema` describes.
///
/// `depth` counts the JSON containers entered so far; the root call passes 0.
pub(super) fn json_to_avro(
    schema: &Schema,
    names: &NamedSchemas,
    json: &Json,
    pointer: &str,
    depth: usize,
) -> CoreResult<Value> {
    match schema {
        Schema::Null => match json {
            Json::Null => Ok(Value::Null),
            _ => Err(mismatch(pointer, "expected null")),
        },
        Schema::Boolean => json
            .as_bool()
            .map(Value::Boolean)
            .ok_or_else(|| mismatch(pointer, "expected boolean")),
        Schema::Int | Schema::Date | Schema::TimeMillis => {
            let number = json_integer(json, pointer, "int")?;
            let narrowed = i32::try_from(number)
                .map_err(|_| mismatch(pointer, "integer out of range for int"))?;
            Ok(match schema {
                Schema::Date => Value::Date(narrowed),
                Schema::TimeMillis => Value::TimeMillis(narrowed),
                _ => Value::Int(narrowed),
            })
        }
        Schema::Long
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => {
            let number = json_integer(json, pointer, "long")?;
            Ok(match schema {
                Schema::TimeMicros => Value::TimeMicros(number),
                Schema::TimestampMillis => Value::TimestampMillis(number),
                Schema::TimestampMicros => Value::TimestampMicros(number),
                Schema::TimestampNanos => Value::TimestampNanos(number),
                Schema::LocalTimestampMillis => Value::LocalTimestampMillis(number),
                Schema::LocalTimestampMicros => Value::LocalTimestampMicros(number),
                Schema::LocalTimestampNanos => Value::LocalTimestampNanos(number),
                _ => Value::Long(number),
            })
        }
        Schema::Float => {
            let number = json_float(json, pointer, "float")?;
            #[expect(
                clippy::cast_possible_truncation,
                reason = "narrowing is the Avro float conversion; overflow is checked next"
            )]
            let narrowed = number as f32;
            if narrowed.is_infinite() && number.is_finite() {
                return Err(mismatch(pointer, "number out of range for float"));
            }
            Ok(Value::Float(narrowed))
        }
        Schema::Double => json_float(json, pointer, "double").map(Value::Double),
        Schema::String => json
            .as_str()
            .map(|text| Value::String(text.to_string()))
            .ok_or_else(|| mismatch(pointer, "expected string")),
        Schema::Bytes => json_bytes(json, pointer).map(Value::Bytes),
        Schema::Fixed(fixed) => {
            let bytes = json_bytes(json, pointer)?;
            check_fixed_size(&bytes, fixed.size, fixed.name.name(), pointer)?;
            Ok(Value::Fixed(fixed.size, bytes))
        }
        Schema::Enum(enum_schema) => {
            let symbol = json
                .as_str()
                .ok_or_else(|| mismatch(pointer, "expected string"))?;
            let index = enum_schema
                .symbols
                .iter()
                .position(|candidate| candidate == symbol)
                .ok_or_else(|| {
                    mismatch(
                        pointer,
                        format!("not a symbol of enum '{}'", enum_schema.name.name()),
                    )
                })?;
            let index = u32::try_from(index).map_err(|_| unexpected(pointer))?;
            Ok(Value::Enum(index, symbol.to_string()))
        }
        Schema::Array(array) => {
            let items = json
                .as_array()
                .ok_or_else(|| mismatch(pointer, "expected array"))?;
            let inner_depth = enter_container(pointer, depth)?;
            items
                .iter()
                .enumerate()
                .map(|(index, item)| {
                    let item_pointer = child_pointer(pointer, &index.to_string());
                    json_to_avro(&array.items, names, item, &item_pointer, inner_depth)
                })
                .collect::<CoreResult<Vec<Value>>>()
                .map(Value::Array)
        }
        Schema::Map(map) => {
            let entries = json
                .as_object()
                .ok_or_else(|| mismatch(pointer, "expected object"))?;
            let inner_depth = enter_container(pointer, depth)?;
            entries
                .iter()
                .map(|(key, item)| {
                    let item_pointer = child_pointer(pointer, key);
                    json_to_avro(&map.types, names, item, &item_pointer, inner_depth)
                        .map(|value| (key.clone(), value))
                })
                .collect::<CoreResult<HashMap<String, Value>>>()
                .map(Value::Map)
        }
        Schema::Union(union) => {
            for (index, variant) in union.variants().iter().enumerate() {
                match json_to_avro(variant, names, json, pointer, depth) {
                    Ok(value) => {
                        let index = u32::try_from(index).map_err(|_| unexpected(pointer))?;
                        return Ok(Value::Union(index, Box::new(value)));
                    }
                    Err(error) if is_nesting_error(&error) => return Err(error),
                    Err(_) => {}
                }
            }
            Err(mismatch(pointer, "matches no branch of union"))
        }
        Schema::Record(record) => {
            let object = json
                .as_object()
                .ok_or_else(|| mismatch(pointer, "expected object"))?;
            let inner_depth = enter_container(pointer, depth)?;
            if let Some(unknown) = object
                .keys()
                .find(|key| !record.fields.iter().any(|field| field.name == **key))
            {
                return Err(mismatch(pointer, format!("unknown field '{unknown}'")));
            }
            let mut fields = Vec::with_capacity(record.fields.len());
            for field in &record.fields {
                let field_pointer = child_pointer(pointer, &field.name);
                let value = match (object.get(&field.name), &field.default) {
                    (Some(item), _) => {
                        json_to_avro(&field.schema, names, item, &field_pointer, inner_depth)?
                    }
                    (None, Some(default)) => {
                        json_to_avro(&field.schema, names, default, &field_pointer, inner_depth)?
                    }
                    (None, None) => {
                        return Err(mismatch(pointer, format!("missing field '{}'", field.name)));
                    }
                };
                fields.push((field.name.clone(), value));
            }
            Ok(Value::Record(fields))
        }
        Schema::Decimal(decimal) => {
            let bytes = json_bytes(json, pointer)?;
            if let InnerDecimalSchema::Fixed(fixed) = &decimal.inner {
                check_fixed_size(&bytes, fixed.size, fixed.name.name(), pointer)?;
            }
            Decimal::new(&bytes)
                .map(Value::Decimal)
                .map_err(|_| mismatch(pointer, "decimal needs at least one byte"))
        }
        Schema::Uuid(_) => {
            let text = json
                .as_str()
                .ok_or_else(|| mismatch(pointer, "expected uuid string"))?;
            Uuid::parse_str(text)
                .map(Value::Uuid)
                .map_err(|_| mismatch(pointer, "expected uuid string"))
        }
        Schema::Duration(fixed) => {
            let bytes = json_bytes(json, pointer)?;
            check_fixed_size(&bytes, 12, fixed.name.name(), pointer)?;
            let array = <[u8; 12]>::try_from(bytes.as_slice()).map_err(|_| unexpected(pointer))?;
            Ok(Value::Duration(Duration::from(array)))
        }
        Schema::BigDecimal => Err(mismatch(
            pointer,
            "logical type bigdecimal is not supported",
        )),
        Schema::Ref { name } => {
            let target = names.get(name).ok_or_else(|| unexpected(pointer))?;
            json_to_avro(target, names, json, pointer, depth)
        }
    }
}

/// Converts an Avro value, as decoded from the wire, into the JSON value it represents.
pub(super) fn avro_to_json(
    schema: &Schema,
    names: &NamedSchemas,
    value: Value,
    pointer: &str,
    depth: usize,
) -> CoreResult<Json> {
    if let Schema::Ref { name } = schema {
        let target = names.get(name).ok_or_else(|| unexpected(pointer))?;
        return avro_to_json(target, names, value, pointer, depth);
    }
    match value {
        Value::Null => Ok(Json::Null),
        Value::Boolean(flag) => Ok(Json::Bool(flag)),
        Value::Int(number) | Value::Date(number) | Value::TimeMillis(number) => {
            Ok(Json::from(number))
        }
        Value::Long(number)
        | Value::TimeMicros(number)
        | Value::TimestampMillis(number)
        | Value::TimestampMicros(number)
        | Value::TimestampNanos(number)
        | Value::LocalTimestampMillis(number)
        | Value::LocalTimestampMicros(number)
        | Value::LocalTimestampNanos(number) => Ok(Json::from(number)),
        Value::Float(number) => {
            // Shortest text that round-trips the f32, so 0.1 stays 0.1 instead of 0.10000000149011612.
            let shortest = number
                .to_string()
                .parse::<f64>()
                .unwrap_or(f64::from(number));
            float_to_json(shortest, pointer)
        }
        Value::Double(number) => float_to_json(number, pointer),
        Value::String(text) => Ok(Json::String(text)),
        Value::Bytes(bytes) | Value::Fixed(_, bytes) => Ok(Json::String(bytes_text(&bytes))),
        Value::Enum(_, symbol) => Ok(Json::String(symbol)),
        Value::Union(index, inner) => {
            let variant = match schema {
                Schema::Union(union) => usize::try_from(index)
                    .ok()
                    .and_then(|position| union.variants().get(position)),
                _ => None,
            }
            .ok_or_else(|| unexpected(pointer))?;
            avro_to_json(variant, names, *inner, pointer, depth)
        }
        Value::Array(items) => {
            let Schema::Array(array) = schema else {
                return Err(unexpected(pointer));
            };
            let inner_depth = enter_container(pointer, depth)?;
            items
                .into_iter()
                .enumerate()
                .map(|(index, item)| {
                    let item_pointer = child_pointer(pointer, &index.to_string());
                    avro_to_json(&array.items, names, item, &item_pointer, inner_depth)
                })
                .collect::<CoreResult<Vec<Json>>>()
                .map(Json::Array)
        }
        Value::Map(entries) => {
            let Schema::Map(map) = schema else {
                return Err(unexpected(pointer));
            };
            let inner_depth = enter_container(pointer, depth)?;
            entries
                .into_iter()
                .map(|(key, item)| {
                    let item_pointer = child_pointer(pointer, &key);
                    avro_to_json(&map.types, names, item, &item_pointer, inner_depth)
                        .map(|json| (key, json))
                })
                .collect::<CoreResult<serde_json::Map<String, Json>>>()
                .map(Json::Object)
        }
        Value::Record(fields) => {
            let Schema::Record(record) = schema else {
                return Err(unexpected(pointer));
            };
            let inner_depth = enter_container(pointer, depth)?;
            let mut by_name: HashMap<String, Value> = fields.into_iter().collect();
            let mut object = serde_json::Map::new();
            for field in &record.fields {
                let item = by_name
                    .remove(&field.name)
                    .ok_or_else(|| unexpected(pointer))?;
                let field_pointer = child_pointer(pointer, &field.name);
                let json = avro_to_json(&field.schema, names, item, &field_pointer, inner_depth)?;
                object.insert(field.name.clone(), json);
            }
            Ok(Json::Object(object))
        }
        Value::Decimal(decimal) => {
            let bytes = decimal_bytes(&decimal).ok_or_else(|| unexpected(pointer))?;
            Ok(Json::String(bytes_text(&bytes)))
        }
        Value::Uuid(uuid) => Ok(Json::String(uuid.to_string())),
        Value::Duration(duration) => {
            let bytes: [u8; 12] = (&duration).into();
            Ok(Json::String(bytes_text(&bytes)))
        }
        Value::BigDecimal(_) => Err(unexpected(pointer)),
    }
}

fn mismatch(pointer: &str, reason: impl Into<String>) -> CoreError {
    CoreError::SchemaValidation {
        pointer: pointer.to_string(),
        reason: reason.into(),
    }
}

fn unexpected(pointer: &str) -> CoreError {
    mismatch(pointer, UNEXPECTED_REASON)
}

fn is_nesting_error(error: &CoreError) -> bool {
    matches!(error, CoreError::SchemaValidation { reason, .. } if reason == NESTING_REASON)
}

/// Returns the depth inside a container, failing once the 64-level limit is exceeded.
fn enter_container(pointer: &str, depth: usize) -> CoreResult<usize> {
    let inner_depth = depth + 1;
    if inner_depth > MAX_NESTING_DEPTH {
        return Err(mismatch(pointer, NESTING_REASON));
    }
    Ok(inner_depth)
}

/// Appends one JSON Pointer segment, escaping `~` and `/` as RFC 6901 requires.
fn child_pointer(parent: &str, segment: &str) -> String {
    let escaped = segment.replace('~', "~0").replace('/', "~1");
    if parent == ROOT_POINTER {
        format!("/{escaped}")
    } else {
        format!("{parent}/{escaped}")
    }
}

/// Reads a JSON integer for an `int` or `long` slot.
fn json_integer(json: &Json, pointer: &str, name: &str) -> CoreResult<i64> {
    if let Some(number) = json.as_i64() {
        return Ok(number);
    }
    let beyond_i64 = json.is_u64()
        || json.as_f64().is_some_and(|number| {
            number.fract() == 0.0 && number.abs() >= 9.223_372_036_854_776e18
        });
    if beyond_i64 {
        Err(mismatch(
            pointer,
            format!("integer out of range for {name}"),
        ))
    } else {
        Err(mismatch(pointer, format!("expected {name}")))
    }
}

/// Reads a JSON number for a `float` or `double` slot, accepting the non-finite string forms.
fn json_float(json: &Json, pointer: &str, name: &str) -> CoreResult<f64> {
    let expected = || mismatch(pointer, format!("expected {name}"));
    match json {
        Json::Number(number) => number.as_f64().ok_or_else(expected),
        Json::String(text) => match text.as_str() {
            "NaN" => Ok(f64::NAN),
            "Infinity" => Ok(f64::INFINITY),
            "-Infinity" => Ok(f64::NEG_INFINITY),
            _ => Err(expected()),
        },
        _ => Err(expected()),
    }
}

/// Reads a string whose characters U+0000 to U+00FF each stand for one byte.
fn json_bytes(json: &Json, pointer: &str) -> CoreResult<Vec<u8>> {
    let text = json
        .as_str()
        .ok_or_else(|| mismatch(pointer, "expected string"))?;
    text.chars()
        .map(|character| {
            u8::try_from(u32::from(character))
                .map_err(|_| mismatch(pointer, "character above U+00FF in bytes"))
        })
        .collect()
}

fn check_fixed_size(bytes: &[u8], size: usize, name: &str, pointer: &str) -> CoreResult<()> {
    if bytes.len() == size {
        Ok(())
    } else {
        Err(mismatch(
            pointer,
            format!("fixed '{name}' needs {size} bytes"),
        ))
    }
}

fn float_to_json(value: f64, pointer: &str) -> CoreResult<Json> {
    if value.is_nan() {
        Ok(Json::String("NaN".to_string()))
    } else if value == f64::INFINITY {
        Ok(Json::String("Infinity".to_string()))
    } else if value == f64::NEG_INFINITY {
        Ok(Json::String("-Infinity".to_string()))
    } else {
        serde_json::Number::from_f64(value)
            .map(Json::Number)
            .ok_or_else(|| unexpected(pointer))
    }
}

/// Renders bytes as a string where each byte is the character with the same code point.
fn bytes_text(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| char::from(*byte)).collect()
}

/// Reads the bytes of a decimal value, sign-extended to its stored length.
fn decimal_bytes(decimal: &Decimal) -> Option<Vec<u8>> {
    let json = serde_json::to_value(decimal).ok()?;
    json.as_array()?
        .iter()
        .map(|item| item.as_u64().and_then(|byte| u8::try_from(byte).ok()))
        .collect()
}
