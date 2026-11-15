use figment::error::Kind;

/// Describes a TOML syntax error as `TOML syntax error at line N, column M: <message>`.
///
/// Only the parser message is used: its default rendering echoes the offending source line.
pub fn describe_toml_syntax_error(error: &toml::de::Error, source: &str) -> String {
    let message = error.message();
    match error.span() {
        Some(span) => {
            let before = source.get(..span.start).unwrap_or_default();
            let line = before.matches('\n').count() + 1;
            let column = before.rsplit('\n').next().map_or(0, str::len) + 1;
            format!("TOML syntax error at line {line}, column {column}: {message}")
        }
        None => format!("TOML syntax error: {message}"),
    }
}

/// Describes every error in a figment failure as `<key path>: <description>`.
///
/// Descriptions never repeat the offending value, so secrets cannot leak through errors.
pub fn describe_figment_error(error: &figment::Error) -> String {
    error
        .clone()
        .into_iter()
        .map(describe_single_error)
        .collect::<Vec<_>>()
        .join("; ")
}

fn describe_single_error(error: figment::Error) -> String {
    let location = error.path.join(".");
    let description = describe_kind(&error.kind);
    if location.is_empty() {
        description
    } else {
        format!("{location}: {description}")
    }
}

fn describe_kind(kind: &Kind) -> String {
    match kind {
        Kind::Message(message) => message.clone(),
        Kind::UnknownField(field, _) => format!("unknown field '{field}'"),
        Kind::MissingField(field) => format!("missing field '{field}'"),
        Kind::UnknownVariant(variant, _) => format!("unknown value '{variant}' for this field"),
        _ => "invalid value".to_string(),
    }
}
