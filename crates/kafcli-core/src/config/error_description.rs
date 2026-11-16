use figment::error::Kind;
use serde_json::error::Category;

/// Describes a JSON syntax error as `JSON syntax error at line N, column M: <category>`.
///
/// Only the line, the column and the error category are used: the parser message can echo the
/// offending source text, which may hold secrets.
pub fn describe_json_syntax_error(error: &serde_json::Error) -> String {
    let category = match error.classify() {
        Category::Syntax => "invalid syntax",
        Category::Eof => "unexpected end of file",
        Category::Data => "invalid value",
        Category::Io => "could not be read",
    };
    format!(
        "JSON syntax error at line {}, column {}: {category}",
        error.line(),
        error.column()
    )
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
