use super::token_response::MAX_TOKEN_RESPONSE_BYTES;
use crate::auth::token::token_text::redact_token_text;
use crate::error::{CoreError, CoreResult};
use crate::registry::path_segment::encode_path_segment;
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderName, HeaderValue};
use reqwest::redirect::Policy;
use secrecy::SecretString;
use std::error::Error as StdError;
use std::io::{self, Read};
use std::time::Duration;

/// Timeout of one token request, from the global constraints.
pub const TOKEN_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Raw answer of a token endpoint. A status outside 2xx is not an error here.
#[derive(Debug)]
pub struct TokenHttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// Blocking HTTP client for token endpoints. Redirects are off, bodies are capped at 1 MiB and
/// transport errors never carry the URL.
pub struct TokenHttpClient {
    client: Client,
}

impl TokenHttpClient {
    pub fn new(timeout: Duration) -> CoreResult<Self> {
        let client = Client::builder()
            .timeout(timeout)
            .redirect(Policy::none())
            .build()
            .map_err(|_| auth_error("could not build the token HTTP client"))?;
        Ok(Self { client })
    }

    /// Sends a form as `application/x-www-form-urlencoded`. `secrets` are redacted from any error.
    pub fn post_form(
        &self,
        url: &str,
        form: &[(&str, &str)],
        secrets: &[&SecretString],
    ) -> CoreResult<TokenHttpResponse> {
        let body = encode_form(form);
        let request = self
            .client
            .post(url)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(ACCEPT, "application/json")
            .body(body);
        send_and_read(request, secrets)
    }

    /// Sends a GET with extra headers. Header values that are secrets must be listed in `secrets`.
    pub fn get(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        secrets: &[&SecretString],
    ) -> CoreResult<TokenHttpResponse> {
        let mut request = self.client.get(url).header(ACCEPT, "application/json");
        for (name, value) in headers {
            let header_name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| auth_error("invalid header name in token request"))?;
            let header_value = HeaderValue::from_str(value)
                .map_err(|_| auth_error("invalid header value in token request"))?;
            request = request.header(header_name, header_value);
        }
        send_and_read(request, secrets)
    }
}

fn send_and_read(
    request: reqwest::blocking::RequestBuilder,
    secrets: &[&SecretString],
) -> CoreResult<TokenHttpResponse> {
    let response = request
        .send()
        .map_err(|error| transport_error(error, secrets))?;
    let status = response.status().as_u16();
    let mut limited = Read::take(response, MAX_TOKEN_RESPONSE_BYTES as u64 + 1);
    let mut body = Vec::new();
    limited
        .read_to_end(&mut body)
        .map_err(|error| read_error(&error, secrets))?;
    if body.len() > MAX_TOKEN_RESPONSE_BYTES {
        return Err(auth_error("azure token response is larger than 1 MiB"));
    }
    Ok(TokenHttpResponse { status, body })
}

/// Names the kind of failure and the cause chain, without the URL, then redacts secrets.
fn transport_error(error: reqwest::Error, secrets: &[&SecretString]) -> CoreError {
    let kind = if error.is_timeout() {
        "timed out"
    } else if error.is_connect() {
        "could not connect"
    } else if error.is_builder() {
        "invalid request"
    } else {
        "error sending request"
    };
    let error = error.without_url();
    let mut description = kind.to_owned();
    let mut cause = StdError::source(&error);
    while let Some(current) = cause {
        description.push_str(": ");
        description.push_str(&current.to_string());
        cause = current.source();
    }
    auth_error(&format!(
        "azure token request failed: {}",
        redact_token_text(&description, secrets)
    ))
}

fn read_error(error: &io::Error, secrets: &[&SecretString]) -> CoreError {
    let timed_out = error.kind() == io::ErrorKind::TimedOut
        || error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<reqwest::Error>())
            .is_some_and(reqwest::Error::is_timeout);
    if timed_out {
        return auth_error("azure token response timed out");
    }
    auth_error(&format!(
        "azure token response could not be read: {}",
        redact_token_text(&error.to_string(), secrets)
    ))
}

/// Form body with the same unreserved set as a path segment, which form values accept too.
fn encode_form(form: &[(&str, &str)]) -> String {
    form.iter()
        .map(|(name, value)| {
            format!(
                "{}={}",
                encode_path_segment(name),
                encode_path_segment(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn auth_error(message: &str) -> CoreError {
    CoreError::Authentication(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};

    fn route(method: &str, path: &str, body: &str) -> FakeRoute {
        FakeRoute {
            method: method.to_string(),
            path_and_query: path.to_string(),
            status: 200,
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    #[test]
    fn get_sends_headers_and_returns_body() {
        let server = FakeRegistryServer::start(vec![route("GET", "/meta", "ok")]).unwrap();
        let client = TokenHttpClient::new(Duration::from_secs(5)).unwrap();
        let secret = SecretString::from("header-secret");

        let response = client
            .get(
                &format!("{}/meta", server.url()),
                &[("Metadata", "true")],
                &[&secret],
            )
            .unwrap();

        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"ok");
        let requests = server.recorded_requests();
        assert!(
            requests[0]
                .headers
                .iter()
                .any(|(name, value)| name.eq_ignore_ascii_case("metadata") && value == "true")
        );
    }

    #[test]
    fn oversized_body_is_refused() {
        let oversized = "x".repeat(MAX_TOKEN_RESPONSE_BYTES + 1);
        let server = FakeRegistryServer::start(vec![route("POST", "/big", &oversized)]).unwrap();
        let client = TokenHttpClient::new(Duration::from_secs(5)).unwrap();

        let error = client
            .post_form(&format!("{}/big", server.url()), &[("a", "b")], &[])
            .unwrap_err();

        assert!(matches!(&error, CoreError::Authentication(message) if message.contains("1 MiB")));
    }

    #[test]
    fn form_values_are_percent_encoded() {
        assert_eq!(
            encode_form(&[
                ("scope", "https://ns/.default"),
                ("grant_type", "client_credentials")
            ]),
            "scope=https%3A%2F%2Fns%2F.default&grant_type=client_credentials"
        );
    }
}
