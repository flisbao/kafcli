use super::token_http_client::TokenHttpClient;
use super::token_response::parse_token_response;
use crate::auth::token::access_token::AccessToken;
use crate::error::CoreResult;
use crate::registry::path_segment::encode_path_segment;
use secrecy::{ExposeSecret, SecretString};
use std::time::SystemTime;

/// POSTs a form to `<authority>/<tenant>/oauth2/v2.0/token` and reads the token.
///
/// Every secret is redacted from errors in both its raw form and its form-encoded form, because
/// the form is sent encoded and an Entra error that echoes it would carry the encoded value.
pub fn request_entra_token(
    http: &TokenHttpClient,
    authority_host: &str,
    tenant_id: &str,
    form: &[(&str, &str)],
    secrets: &[&SecretString],
) -> CoreResult<AccessToken> {
    let url = format!(
        "{}/{}/oauth2/v2.0/token",
        authority_host.trim_end_matches('/'),
        encode_path_segment(tenant_id)
    );
    let form_encoded = form_encoded_secrets(secrets);
    let redacted: Vec<&SecretString> = secrets.iter().copied().chain(form_encoded.iter()).collect();
    let response = http.post_form(&url, form, &redacted)?;
    parse_token_response(
        response.status,
        &response.body,
        SystemTime::now(),
        &redacted,
    )
}

/// Form-encoded copy of each secret, encoded the way the form itself is encoded.
fn form_encoded_secrets(secrets: &[&SecretString]) -> Vec<SecretString> {
    secrets
        .iter()
        .map(|secret| SecretString::from(encode_path_segment(secret.expose_secret())))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::azure::token_http_client::TOKEN_REQUEST_TIMEOUT;
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
    use crate::registry::path_segment::encode_path_segment;

    /// Contains every character the form encoder escapes: `+`, `/`, `=`, `&`.
    const SECRET: &str = "a+b/c=&x";

    #[test]
    fn refusal_that_echoes_form_never_shows_secret_raw_or_encoded() {
        let encoded = encode_path_segment(SECRET);
        let echoed =
            format!("client_secret={encoded}&client_id=app-123&grant_type=client_credentials");
        let server = FakeRegistryServer::start(vec![FakeRoute {
            method: "POST".to_string(),
            path_and_query: "/tenant-1/oauth2/v2.0/token".to_string(),
            status: 401,
            headers: Vec::new(),
            body: format!(r#"{{"error":"invalid_client","error_description":"{echoed}"}}"#),
        }])
        .unwrap();
        let http = TokenHttpClient::new(TOKEN_REQUEST_TIMEOUT).unwrap();
        let secret = SecretString::from(SECRET);
        let form = [
            ("grant_type", "client_credentials"),
            ("client_id", "app-123"),
            ("client_secret", SECRET),
        ];

        let error =
            request_entra_token(&http, &server.url(), "tenant-1", &form, &[&secret]).unwrap_err();

        let message = error.to_string();
        assert!(message.contains("HTTP 401"), "message: {message}");
        assert!(message.contains("invalid_client"), "message: {message}");
        assert!(
            message.contains("client_secret=*&client_id=app-123"),
            "message: {message}"
        );
        assert!(!message.contains(SECRET), "raw secret leaked: {message}");
        assert!(
            !message.contains(&encoded),
            "encoded secret leaked: {message}"
        );
        // The fake really received the encoded form, so the echo carried the encoded value.
        assert!(
            server.recorded_requests()[0]
                .body
                .contains(&format!("client_secret={encoded}")),
            "the request did not carry the encoded secret"
        );
    }
}
