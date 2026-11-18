use super::client_assertion::{ClientCertificate, build_client_assertion};
use super::entra_token_client::request_entra_token;
use super::token_http_client::TokenHttpClient;
use crate::auth::token::access_token::AccessToken;
use crate::auth::token::token_source::TokenSource;
use crate::error::CoreResult;
use crate::registry::path_segment::encode_path_segment;
use secrecy::ExposeSecret;
use std::time::SystemTime;
use uuid::Uuid;

const CLIENT_ASSERTION_TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

/// Fetches tokens for an Entra ID service principal with a certificate: every fetch sends a new RS256
/// client assertion signed with the certificate's key (client credentials grant).
pub struct ServicePrincipalCertificateTokenSource {
    http: TokenHttpClient,
    authority_host: String,
    tenant_id: String,
    client_id: String,
    certificate: ClientCertificate,
    scope: String,
}

impl ServicePrincipalCertificateTokenSource {
    pub fn new(
        http: TokenHttpClient,
        authority_host: String,
        tenant_id: String,
        client_id: String,
        certificate: ClientCertificate,
        scope: String,
    ) -> Self {
        Self {
            http,
            authority_host,
            tenant_id,
            client_id,
            certificate,
            scope,
        }
    }

    /// Audience of the assertion: the token endpoint of the tenant, as Entra ID expects it.
    fn token_audience(&self) -> String {
        format!(
            "{}/{}/oauth2/v2.0/token",
            self.authority_host.trim_end_matches('/'),
            encode_path_segment(&self.tenant_id)
        )
    }
}

impl TokenSource for ServicePrincipalCertificateTokenSource {
    fn fetch_access_token(&self) -> CoreResult<AccessToken> {
        let assertion = build_client_assertion(
            &self.certificate,
            &self.client_id,
            &self.token_audience(),
            SystemTime::now(),
            &Uuid::new_v4().to_string(),
        )?;
        let form = [
            ("client_id", self.client_id.as_str()),
            ("scope", self.scope.as_str()),
            ("grant_type", "client_credentials"),
            ("client_assertion_type", CLIENT_ASSERTION_TYPE),
            ("client_assertion", assertion.expose_secret()),
        ];
        request_entra_token(
            &self.http,
            &self.authority_host,
            &self.tenant_id,
            &form,
            &[&assertion],
        )
    }

    fn describe_source(&self) -> &'static str {
        "azure-sp-cert"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::azure::client_assertion::tests::sample_certificate;
    use crate::auth::azure::token_http_client::TOKEN_REQUEST_TIMEOUT;
    use crate::error::CoreError;
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    const SCOPE: &str = "https://ns.servicebus.windows.net/.default";

    fn token_route(status: u16, body: &str) -> FakeRoute {
        FakeRoute {
            method: "POST".to_string(),
            path_and_query: "/tenant-1/oauth2/v2.0/token".to_string(),
            status,
            headers: Vec::new(),
            body: body.to_string(),
        }
    }

    fn source_for(authority_host: String) -> ServicePrincipalCertificateTokenSource {
        ServicePrincipalCertificateTokenSource::new(
            TokenHttpClient::new(TOKEN_REQUEST_TIMEOUT).unwrap(),
            authority_host,
            "tenant-1".to_string(),
            "app".to_string(),
            sample_certificate(),
            SCOPE.to_string(),
        )
    }

    fn form_value<'a>(body: &'a str, name: &str) -> Option<&'a str> {
        body.split('&')
            .find_map(|pair| pair.strip_prefix(&format!("{name}=")))
    }

    #[test]
    fn posts_jwt_bearer_assertion() {
        let server = FakeRegistryServer::start(vec![token_route(
            200,
            r#"{"access_token":"token-from-route","expires_in":3600}"#,
        )])
        .unwrap();
        let source = source_for(server.url());

        let token = source.fetch_access_token().unwrap();

        assert_eq!(token.value.expose_secret(), "token-from-route");
        let requests = server.recorded_requests();
        let body = &requests[0].body;
        assert!(
            body.contains(
                "client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"
            ),
            "body: {body}"
        );
        assert!(
            body.contains("grant_type=client_credentials"),
            "body: {body}"
        );
        assert!(body.contains("client_id=app"), "body: {body}");
        assert!(
            body.contains("scope=https%3A%2F%2Fns.servicebus.windows.net%2F.default"),
            "body: {body}"
        );
        assert!(!body.contains("client_secret"), "body: {body}");
        assert_eq!(source.describe_source(), "azure-sp-cert");
    }

    #[test]
    fn each_fetch_sends_a_new_assertion() {
        let server = FakeRegistryServer::start(vec![token_route(
            200,
            r#"{"access_token":"t","expires_in":3600}"#,
        )])
        .unwrap();
        let source = source_for(server.url());

        source.fetch_access_token().unwrap();
        source.fetch_access_token().unwrap();

        let requests = server.recorded_requests();
        let first = form_value(&requests[0].body, "client_assertion");
        let second = form_value(&requests[1].body, "client_assertion");
        assert!(first.is_some());
        assert_ne!(first, second);
    }

    /// Answers one request with 400 and echoes the request body in `error_description`, the way a
    /// failing Entra ID request can. Returns the URL and the thread that yields the request body.
    fn start_echo_server() -> (String, std::thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let worker = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut content_length = 0usize;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_length = value.trim().parse().unwrap();
                }
            }
            let mut body = vec![0u8; content_length];
            reader.read_exact(&mut body).unwrap();
            let body = String::from_utf8(body).unwrap();
            let payload = serde_json::json!({
                "error": "invalid_client",
                "error_description": body,
            })
            .to_string();
            let mut stream = stream;
            write!(
                stream,
                "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                payload.len(),
                payload
            )
            .unwrap();
            body
        });
        (url, worker)
    }

    #[test]
    fn assertion_is_redacted_from_errors() {
        let (url, worker) = start_echo_server();
        let source = source_for(url);

        let error = source.fetch_access_token().unwrap_err();

        let request_body = worker.join().unwrap();
        let assertion = form_value(&request_body, "client_assertion").unwrap();
        let message = format!("{error}");
        assert!(
            message.contains("azure token request failed (HTTP 400): invalid_client"),
            "message: {message}"
        );
        assert!(!message.contains(assertion), "message: {message}");
        assert!(matches!(error, CoreError::Authentication(_)));
    }
}
