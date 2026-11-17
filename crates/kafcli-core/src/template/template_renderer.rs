use super::environment_lookup::EnvironmentLookup;
use super::template_parser::{TemplateSegment, split_template_segments};
use super::template_state::TemplateState;
use crate::error::{CoreError, CoreResult};
use crate::message::timestamp_text::format_rfc3339_millis;
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

/// Whole-leaf placeholder that becomes a JSON number in payloads.
const SEQUENCE_PLACEHOLDER: &str = "{{seq}}";

/// Renders every string leaf of a payload. Object keys are not rendered.
///
/// A leaf that is exactly `{{seq}}` becomes a JSON number. `{{field:...}}` is rejected here,
/// because fields are only read from key and header values.
pub fn render_value_templates(
    value: &Value,
    state: &TemplateState,
    environment: &dyn EnvironmentLookup,
    now: OffsetDateTime,
) -> CoreResult<Value> {
    let context = RenderContext {
        state,
        environment,
        now,
        field_source: FieldSource::Forbidden,
    };
    render_value(value, &context)
}

/// Renders a key or header text. `{{field:...}}` reads from `payload`, the rendered payload.
pub fn render_text_template(
    text: &str,
    state: &TemplateState,
    environment: &dyn EnvironmentLookup,
    now: OffsetDateTime,
    payload: Option<&Value>,
) -> CoreResult<String> {
    let context = RenderContext {
        state,
        environment,
        now,
        field_source: FieldSource::Payload(payload),
    };
    render_text(text, &context)
}

struct RenderContext<'a> {
    state: &'a TemplateState,
    environment: &'a dyn EnvironmentLookup,
    now: OffsetDateTime,
    field_source: FieldSource<'a>,
}

#[derive(Clone, Copy)]
enum FieldSource<'a> {
    Forbidden,
    Payload(Option<&'a Value>),
}

fn render_value(value: &Value, context: &RenderContext<'_>) -> CoreResult<Value> {
    match value {
        Value::String(text) if text == SEQUENCE_PLACEHOLDER => {
            Ok(Value::from(context.state.sequence()))
        }
        Value::String(text) => render_text(text, context).map(Value::String),
        Value::Array(items) => items
            .iter()
            .map(|item| render_value(item, context))
            .collect::<CoreResult<Vec<Value>>>()
            .map(Value::Array),
        Value::Object(entries) => {
            let mut rendered = serde_json::Map::new();
            for (key, item) in entries {
                rendered.insert(key.clone(), render_value(item, context)?);
            }
            Ok(Value::Object(rendered))
        }
        other => Ok(other.clone()),
    }
}

fn render_text(text: &str, context: &RenderContext<'_>) -> CoreResult<String> {
    let mut output = String::with_capacity(text.len());
    for segment in split_template_segments(text)? {
        match segment {
            TemplateSegment::Literal(literal) => output.push_str(literal),
            TemplateSegment::Placeholder(content) => {
                output.push_str(&resolve_placeholder(content, context)?);
            }
        }
    }
    Ok(output)
}

fn resolve_placeholder(content: &str, context: &RenderContext<'_>) -> CoreResult<String> {
    let (name, argument) = match content.split_once(':') {
        Some((name, argument)) => (name, Some(argument)),
        None => (content, None),
    };
    match (name, argument) {
        ("uuid", None) => Ok(Uuid::new_v4().hyphenated().to_string()),
        ("seq", None) => Ok(context.state.sequence().to_string()),
        ("now", None) => format_rfc3339_millis(context.now).ok_or_else(|| {
            CoreError::Template("template 'now' could not be formatted".to_string())
        }),
        ("field", Some(path)) => resolve_field(path, context),
        ("env", Some(variable)) => context.environment.read_variable(variable).ok_or_else(|| {
            CoreError::Template(format!("environment variable '{variable}' is not set"))
        }),
        _ => Err(CoreError::Template(format!(
            "unknown template '{{{{{content}}}}}'"
        ))),
    }
}

fn resolve_field(path: &str, context: &RenderContext<'_>) -> CoreResult<String> {
    let payload = match context.field_source {
        FieldSource::Forbidden => {
            return Err(CoreError::Template(
                "template 'field' is only allowed in key and header values".to_string(),
            ));
        }
        FieldSource::Payload(payload) => payload,
    };
    let found = payload.and_then(|value| find_field(value, path));
    match found {
        Some(Value::String(text)) => Ok(text.clone()),
        Some(other) => serde_json::to_string(other)
            .map_err(|_| CoreError::Template(format!("field '{path}' could not be serialized"))),
        None => Err(CoreError::Template(format!(
            "field '{path}' not found in payload"
        ))),
    }
}

/// Follows a dot-separated path. Numeric segments index arrays.
fn find_field<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.')
        .try_fold(value, |current, segment| match current {
            Value::Object(entries) => entries.get(segment),
            Value::Array(items) => segment
                .parse::<usize>()
                .ok()
                .and_then(|index| items.get(index)),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;
    use uuid::Version;

    /// Environment double with fixed variables.
    #[derive(Default)]
    struct FixedEnvironment {
        variables: BTreeMap<String, String>,
    }

    impl FixedEnvironment {
        fn with(pairs: &[(&str, &str)]) -> Self {
            Self {
                variables: pairs
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.to_string()))
                    .collect(),
            }
        }
    }

    impl EnvironmentLookup for FixedEnvironment {
        fn read_variable(&self, name: &str) -> Option<String> {
            self.variables.get(name).cloned()
        }
    }

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp_nanos(1_794_875_047_253 * 1_000_000)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH)
    }

    fn state_at_three() -> TemplateState {
        let mut state = TemplateState::new();
        state.advance();
        state.advance();
        state
    }

    fn error_message<T>(result: CoreResult<T>) -> String {
        match result {
            Ok(_) => "no error".to_string(),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn renders_each_template_kind() -> CoreResult<()> {
        let environment = FixedEnvironment::with(&[("TOKEN", "abc")]);
        let payload = json!({"a": {"b": "x"}});

        let rendered = render_text_template(
            "{{seq}}|{{now}}|{{field:a.b}}|{{env:TOKEN}}",
            &TemplateState::new(),
            &environment,
            fixed_now(),
            Some(&payload),
        )?;

        assert_eq!(rendered, "1|2026-11-17T00:24:07.253Z|x|abc");
        Ok(())
    }

    #[test]
    fn exact_seq_leaf_becomes_number() -> CoreResult<()> {
        let environment = FixedEnvironment::default();
        let payload = json!({"id": "{{seq}}", "list": ["{{seq}}"]});

        let rendered =
            render_value_templates(&payload, &state_at_three(), &environment, fixed_now())?;

        assert_eq!(rendered, json!({"id": 3, "list": [3]}));
        assert_eq!(rendered["id"].as_u64(), Some(3));
        Ok(())
    }

    #[test]
    fn seq_inside_text_stays_string() -> CoreResult<()> {
        let environment = FixedEnvironment::default();
        let payload = json!({"id": "pedido-{{seq}}"});

        let rendered =
            render_value_templates(&payload, &state_at_three(), &environment, fixed_now())?;

        assert_eq!(rendered, json!({"id": "pedido-3"}));
        Ok(())
    }

    #[test]
    fn object_keys_are_not_rendered() -> CoreResult<()> {
        let environment = FixedEnvironment::default();
        let payload = json!({"{{seq}}": "x"});

        let rendered =
            render_value_templates(&payload, &state_at_three(), &environment, fixed_now())?;

        assert_eq!(rendered, json!({"{{seq}}": "x"}));
        Ok(())
    }

    #[test]
    fn unknown_template_is_rejected() {
        let message = error_message(render_text_template(
            "{{foo}}",
            &TemplateState::new(),
            &FixedEnvironment::default(),
            fixed_now(),
            None,
        ));

        assert_eq!(message, "template error: unknown template '{{foo}}'");
    }

    #[test]
    fn unclosed_template_reports_byte_offset() {
        let message = error_message(render_value_templates(
            &json!({"id": "abc {{seq"}),
            &TemplateState::new(),
            &FixedEnvironment::default(),
            fixed_now(),
        ));

        assert_eq!(
            message,
            "template error: unclosed template starting at byte 4"
        );
    }

    #[test]
    fn missing_environment_variable_names_variable_only() {
        let environment = FixedEnvironment::with(&[("OTHER", "super-secret-value")]);

        let message = error_message(render_text_template(
            "{{env:MISSING}}",
            &TemplateState::new(),
            &environment,
            fixed_now(),
            None,
        ));

        assert_eq!(
            message,
            "template error: environment variable 'MISSING' is not set"
        );
        assert!(!message.contains("super-secret-value"));
    }

    #[test]
    fn field_template_rejected_in_payload() {
        let message = error_message(render_value_templates(
            &json!({"id": "{{field:a}}"}),
            &TemplateState::new(),
            &FixedEnvironment::default(),
            fixed_now(),
        ));

        assert_eq!(
            message,
            "template error: template 'field' is only allowed in key and header values"
        );
    }

    #[test]
    fn field_path_indexes_arrays() -> CoreResult<()> {
        let payload = json!({"items": [{"id": "a"}], "n": 42, "o": {"a": 1}});

        let first_id = render_text_template(
            "{{field:items.0.id}}",
            &TemplateState::new(),
            &FixedEnvironment::default(),
            fixed_now(),
            Some(&payload),
        )?;
        let number = render_text_template(
            "{{field:n}}",
            &TemplateState::new(),
            &FixedEnvironment::default(),
            fixed_now(),
            Some(&payload),
        )?;
        let object = render_text_template(
            "{{field:o}}",
            &TemplateState::new(),
            &FixedEnvironment::default(),
            fixed_now(),
            Some(&payload),
        )?;

        assert_eq!(first_id, "a");
        assert_eq!(number, "42");
        assert_eq!(object, r#"{"a":1}"#);
        Ok(())
    }

    #[test]
    fn missing_field_path_is_rejected() {
        let payload = json!({"items": []});

        let message = error_message(render_text_template(
            "{{field:items.0.id}}",
            &TemplateState::new(),
            &FixedEnvironment::default(),
            fixed_now(),
            Some(&payload),
        ));

        assert_eq!(
            message,
            "template error: field 'items.0.id' not found in payload"
        );
    }

    #[test]
    fn uuid_is_lowercase_v4() -> CoreResult<()> {
        let rendered = render_text_template(
            "{{uuid}}",
            &TemplateState::new(),
            &FixedEnvironment::default(),
            fixed_now(),
            None,
        )?;

        let parsed =
            Uuid::parse_str(&rendered).map_err(|error| CoreError::Template(error.to_string()))?;
        assert_eq!(parsed.get_version(), Some(Version::Random));
        assert_eq!(rendered, rendered.to_lowercase());
        assert_eq!(rendered.len(), 36);
        Ok(())
    }
}
