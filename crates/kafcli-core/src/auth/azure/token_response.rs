use crate::auth::token::access_token::AccessToken;
use crate::auth::token::token_text::redact_token_text;
use crate::error::{CoreError, CoreResult};
use secrecy::SecretString;
use serde_json::Value;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Largest token response read from Entra ID, in bytes.
pub const MAX_TOKEN_RESPONSE_BYTES: usize = 1024 * 1024;

/// Reads an Entra ID token answer. A 2xx status yields the token; any other status yields an error
/// that names only the HTTP status, `error` and `error_description`, with every known secret redacted.
pub fn parse_token_response(
    status: u16,
    body: &[u8],
    now: SystemTime,
    secrets: &[&SecretString],
) -> CoreResult<AccessToken> {
    if (200..300).contains(&status) {
        parse_successful_response(body, now)
    } else {
        Err(build_failure_error(status, body, secrets))
    }
}

fn parse_successful_response(body: &[u8], now: SystemTime) -> CoreResult<AccessToken> {
    let document: Value = serde_json::from_slice(body)
        .map_err(|_| build_authentication_error("azure token response is not valid JSON"))?;
    let access_token = document
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| build_authentication_error("azure token response has no access_token"))?;
    let expires_at = read_expiry(&document, now).ok_or_else(|| {
        build_authentication_error("azure token response has no valid expires_in or expires_on")
    })?;
    Ok(AccessToken::new(
        SecretString::from(access_token.to_owned()),
        expires_at,
    ))
}

/// `expires_in` counts seconds from now; `expires_on` is seconds since the epoch. Either may be a
/// number or a string of digits. `expires_in` wins when both are valid.
fn read_expiry(document: &Value, now: SystemTime) -> Option<SystemTime> {
    if let Some(seconds) = read_seconds(document, "expires_in") {
        return now.checked_add(Duration::from_secs(seconds));
    }
    let seconds = read_seconds(document, "expires_on")?;
    UNIX_EPOCH.checked_add(Duration::from_secs(seconds))
}

fn read_seconds(document: &Value, field: &str) -> Option<u64> {
    match document.get(field)? {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

fn build_failure_error(status: u16, body: &[u8], secrets: &[&SecretString]) -> CoreError {
    let document: Option<Value> = serde_json::from_slice(body).ok();
    let read_field = |name: &str| {
        document
            .as_ref()
            .and_then(|value| value.get(name))
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
    };
    let detail = match (read_field("error"), read_field("error_description")) {
        (Some(error), Some(description)) => format!("{error}: {description}"),
        (Some(error), None) => error.to_owned(),
        (None, Some(description)) => description.to_owned(),
        (None, None) => String::new(),
    };
    let mut message = format!("azure token request failed (HTTP {status})");
    if !detail.is_empty() {
        message.push_str(": ");
        message.push_str(&redact_token_text(&detail, secrets));
    }
    CoreError::Authentication(message)
}

fn build_authentication_error(message: &str) -> CoreError {
    CoreError::Authentication(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    const NOW_SECONDS: u64 = 2_000_000_000;

    fn now() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(NOW_SECONDS)
    }

    fn parse_ok(json: &str) -> CoreResult<AccessToken> {
        parse_token_response(200, json.as_bytes(), now(), &[])
    }

    #[test]
    fn accepts_numeric_expires_in() {
        let token = parse_ok(r#"{"access_token":"t","expires_in":3599}"#).unwrap();

        assert_eq!(token.value.expose_secret(), "t");
        assert_eq!(token.expires_at, now() + Duration::from_secs(3599));
    }

    #[test]
    fn accepts_string_expires_on() {
        let token = parse_ok(r#"{"access_token":"t","expires_on":"2000000000"}"#).unwrap();

        assert_eq!(
            token.expires_at,
            UNIX_EPOCH + Duration::from_secs(2_000_000_000)
        );
    }

    #[test]
    fn expires_in_wins_over_expires_on_when_both_exist() {
        let token =
            parse_ok(r#"{"access_token":"t","expires_on":"2000000000","expires_in":"86399"}"#)
                .unwrap();

        assert_eq!(token.expires_at, now() + Duration::from_secs(86_399));
    }

    #[test]
    fn accepts_string_expires_in() {
        let token = parse_ok(r#"{"access_token":"t","expires_in":"3599"}"#).unwrap();

        assert_eq!(token.expires_at, now() + Duration::from_secs(3599));
    }

    #[test]
    fn missing_expiry_is_error() {
        let error = parse_ok(r#"{"access_token":"t"}"#).unwrap_err();

        assert!(
            matches!(&error, CoreError::Authentication(message) if message.contains("no valid expires_in or expires_on")),
            "got {error:?}"
        );
    }

    #[test]
    fn missing_access_token_is_error() {
        let error = parse_ok(r#"{"expires_in":3599}"#).unwrap_err();

        assert!(
            matches!(&error, CoreError::Authentication(message) if message.contains("no access_token")),
            "got {error:?}"
        );
    }

    #[test]
    fn non_json_success_body_is_error_without_echoing_it() {
        let error = parse_token_response(200, b"not-json-t0ken", now(), &[]).unwrap_err();

        assert!(!format!("{error}").contains("not-json-t0ken"));
    }

    #[test]
    fn error_body_is_redacted_and_bounded() {
        let secret = SecretString::from("s3cr3t-value");
        let description = format!(
            "AADSTS7000215: secret s3cr3t-value is wrong {}",
            "x".repeat(700)
        );
        let body = format!(r#"{{"error":"invalid_client","error_description":"{description}"}}"#);

        let error = parse_token_response(401, body.as_bytes(), now(), &[&secret]).unwrap_err();

        let CoreError::Authentication(message) = error else {
            panic!("expected Authentication");
        };
        assert!(
            message.starts_with(
                "azure token request failed (HTTP 401): invalid_client: AADSTS7000215"
            )
        );
        assert!(!message.contains("s3cr3t-value"));
        let detail_chars =
            message.chars().count() - "azure token request failed (HTTP 401): ".chars().count();
        assert!(detail_chars <= 200);
    }

    #[test]
    fn error_without_json_reports_only_status() {
        let error = parse_token_response(503, b"<html>down</html>", now(), &[]).unwrap_err();

        assert_eq!(
            format!("{error}"),
            "authentication error: azure token request failed (HTTP 503)"
        );
    }
}
