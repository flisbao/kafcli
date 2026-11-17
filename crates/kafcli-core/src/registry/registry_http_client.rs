use super::registry_text::neutralize_registry_text;
use crate::config::schema_registry_model::RegistryAuthConfig;
use crate::error::{CoreError, CoreResult};
use crate::secret::resolver::SecretResolver;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Certificate, Identity, Method, Url};
use secrecy::{ExposeSecret, SecretString};
use std::error::Error as StdError;
use std::io::{self, Read};
use std::path::Path;
use std::time::Duration;

const MAX_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
const SCHEMA_REGISTRY_ACCEPT: &str = "application/vnd.schemaregistry.v1+json";

/// Blocking HTTP client bound to one schema registry, with its credentials already applied.
pub struct RegistryHttpClient {
    client: Client,
    base_url: String,
    /// Secret values, kept only to redact them from registry text.
    secret_fragments: Vec<SecretString>,
}

/// Raw answer of the registry. Status outside 2xx is not an error here; the caller interprets it.
#[derive(Debug)]
pub struct RegistryResponse {
    pub status: u16,
    pub body: Vec<u8>,
    headers: Vec<(String, String)>,
}

impl RegistryResponse {
    /// Returns the first header with this name, comparing names without case.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

impl RegistryHttpClient {
    /// Builds a client that sends the Confluent `Accept` media type on every request.
    pub fn new(
        base_url: &str,
        auth: &RegistryAuthConfig,
        ca_location: Option<&Path>,
        resolver: &dyn SecretResolver,
        timeout: Duration,
    ) -> CoreResult<Self> {
        Self::new_with_accept(
            base_url,
            auth,
            ca_location,
            resolver,
            timeout,
            SCHEMA_REGISTRY_ACCEPT,
        )
    }

    /// Same as `new`, with the `Accept` media type that every request sends by default.
    pub fn new_with_accept(
        base_url: &str,
        auth: &RegistryAuthConfig,
        ca_location: Option<&Path>,
        resolver: &dyn SecretResolver,
        timeout: Duration,
        accept: &'static str,
    ) -> CoreResult<Self> {
        let parsed =
            Url::parse(base_url).map_err(|_| registry_error("schema registry URL is not valid"))?;
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(registry_error(
                "schema registry URL must not contain credentials",
            ));
        }

        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static(accept));
        let mut builder = Client::builder().timeout(timeout);
        let mut secret_fragments = Vec::new();

        if let Some(path) = ca_location {
            let pem = read_file(path, "CA certificate")?;
            let certificate = Certificate::from_pem(&pem).map_err(|_| {
                registry_error(format!(
                    "could not load CA certificate from '{}'",
                    path.display()
                ))
            })?;
            builder = builder.add_root_certificate(certificate);
        }

        match auth {
            RegistryAuthConfig::Anonymous => {}
            RegistryAuthConfig::Basic(config) => {
                let password = resolver.resolve_secret(&config.secret_ref)?;
                let credentials =
                    SecretString::from(format!("{}:{}", config.username, password.expose_secret()));
                let encoded = SecretString::from(STANDARD.encode(credentials.expose_secret()));
                insert_authorization(&mut headers, &format!("Basic {}", encoded.expose_secret()))?;
                secret_fragments.push(password);
                secret_fragments.push(credentials);
                secret_fragments.push(encoded);
            }
            RegistryAuthConfig::Bearer(config) => {
                let token = resolver.resolve_secret(&config.token_ref)?;
                insert_authorization(&mut headers, &format!("Bearer {}", token.expose_secret()))?;
                secret_fragments.push(token);
            }
            RegistryAuthConfig::MutualTls(config) => {
                let mut pem = read_file(&config.certificate_location, "client certificate")?;
                pem.push(b'\n');
                pem.extend(read_file(&config.key_location, "client key")?);
                let identity = Identity::from_pem(&pem)
                    .map_err(|_| registry_error("could not load client certificate and key"))?;
                builder = builder.identity(identity);
            }
        }

        let client = builder
            .default_headers(headers)
            .build()
            .map_err(|_| registry_error("could not build the schema registry HTTP client"))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            secret_fragments,
        })
    }

    /// Replaces every known secret in `text` with `*`.
    pub fn redact_secrets(&self, text: &str) -> String {
        self.secret_fragments
            .iter()
            .map(|fragment| fragment.expose_secret())
            .filter(|fragment| !fragment.is_empty())
            .fold(text.to_string(), |redacted, fragment| {
                redacted.replace(fragment, "*")
            })
    }

    /// Redacts secrets, then neutralizes and truncates text that came from the registry or the network.
    pub fn sanitize_text(&self, text: &str) -> String {
        neutralize_registry_text(&self.redact_secrets(text))
    }

    /// Sends one request and reads the whole body, up to 8 MiB.
    pub fn send_request(
        &self,
        method: Method,
        path_and_query: &str,
        content_type: Option<&str>,
        body: Option<String>,
    ) -> CoreResult<RegistryResponse> {
        let url = format!("{}{}", self.base_url, path_and_query);
        let mut request = self.client.request(method, url);
        if let Some(content_type) = content_type {
            request = request.header(CONTENT_TYPE, content_type);
        }
        if let Some(body) = body {
            request = request.body(body);
        }
        let response = request.send().map_err(|error| {
            registry_error(format!(
                "request to schema registry failed: {}",
                self.describe_transport_error(error)
            ))
        })?;

        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|text| (name.as_str().to_string(), text.to_string()))
            })
            .collect();
        let mut limited = Read::take(response, MAX_RESPONSE_BYTES + 1);
        let mut body = Vec::new();
        limited.read_to_end(&mut body).map_err(|error| {
            registry_error(format!(
                "could not read the schema registry response: {}",
                self.describe_body_error(&error)
            ))
        })?;
        if body.len() as u64 > MAX_RESPONSE_BYTES {
            return Err(registry_error("schema registry response larger than 8 MiB"));
        }
        Ok(RegistryResponse {
            status,
            body,
            headers,
        })
    }

    /// Names the kind of transport failure and appends its cause chain, without any URL or secret.
    fn describe_transport_error(&self, error: reqwest::Error) -> String {
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
        let mut description = kind.to_string();
        let mut cause = StdError::source(&error);
        while let Some(current) = cause {
            description.push_str(": ");
            description.push_str(&current.to_string());
            cause = current.source();
        }
        self.sanitize_text(&description)
    }
    /// Names a body-read timeout, or appends the cause chain of other read failures.
    /// reqwest wraps body timeouts in an io::Error of kind `Other`, so the inner error is inspected.
    fn describe_body_error(&self, error: &io::Error) -> String {
        let inner = error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<reqwest::Error>());
        if error.kind() == io::ErrorKind::TimedOut || inner.is_some_and(|e| e.is_timeout()) {
            return "timed out".to_string();
        }
        let mut description = error.to_string();
        let mut cause = error
            .get_ref()
            .map(|inner| inner as &(dyn StdError + 'static))
            .and_then(StdError::source);
        while let Some(current) = cause {
            description.push_str(": ");
            description.push_str(&current.to_string());
            cause = current.source();
        }
        self.sanitize_text(&description)
    }
}

fn registry_error(message: impl Into<String>) -> CoreError {
    CoreError::SchemaRegistry(message.into())
}

fn read_file(path: &Path, description: &str) -> CoreResult<Vec<u8>> {
    std::fs::read(path).map_err(|_| {
        registry_error(format!(
            "could not read {description} from '{}'",
            path.display()
        ))
    })
}

fn insert_authorization(headers: &mut HeaderMap, value: &str) -> CoreResult<()> {
    let mut header_value = HeaderValue::from_str(value)
        .map_err(|_| registry_error("registry credentials are not a valid HTTP header value"))?;
    header_value.set_sensitive(true);
    headers.insert(AUTHORIZATION, header_value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::schema_registry_model::{RegistryBasicAuthConfig, RegistryBearerAuthConfig};
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    fn get_route(path: &str, status: u16, body: String) -> FakeRoute {
        FakeRoute {
            method: "GET".into(),
            path_and_query: path.into(),
            status,
            headers: Vec::new(),
            body,
        }
    }

    #[test]
    fn basic_auth_sends_expected_header() {
        let server = FakeRegistryServer::start(vec![get_route("/x", 200, "{}".into())]).unwrap();
        let auth = RegistryAuthConfig::Basic(RegistryBasicAuthConfig {
            username: "app".into(),
            secret_ref: "env:SR_PASSWORD".parse().unwrap(),
        });
        let client = RegistryHttpClient::new(
            &server.url(),
            &auth,
            None,
            &FixedSecretResolver::returning("s3cret"),
            Duration::from_secs(5),
        )
        .unwrap();

        client.send_request(Method::GET, "/x", None, None).unwrap();

        let requests = server.recorded_requests();
        assert_eq!(
            header_of(&requests[0].headers, "authorization"),
            Some("Basic YXBwOnMzY3JldA==")
        );
    }

    #[test]
    fn bearer_auth_sends_expected_header() {
        let server = FakeRegistryServer::start(vec![get_route("/x", 200, "{}".into())]).unwrap();
        let auth = RegistryAuthConfig::Bearer(RegistryBearerAuthConfig {
            token_ref: "env:SR_TOKEN".parse().unwrap(),
        });
        let client = RegistryHttpClient::new(
            &server.url(),
            &auth,
            None,
            &FixedSecretResolver::returning("tok-123"),
            Duration::from_secs(5),
        )
        .unwrap();

        client.send_request(Method::GET, "/x", None, None).unwrap();

        let requests = server.recorded_requests();
        assert_eq!(
            header_of(&requests[0].headers, "authorization"),
            Some("Bearer tok-123")
        );
    }

    #[test]
    fn response_body_over_8_mib_is_rejected() {
        let big = "x".repeat(8 * 1024 * 1024 + 1);
        let server = FakeRegistryServer::start(vec![get_route("/big", 200, big)]).unwrap();
        let client = RegistryHttpClient::new(
            &server.url(),
            &RegistryAuthConfig::Anonymous,
            None,
            &FixedSecretResolver::returning("unused"),
            Duration::from_secs(10),
        )
        .unwrap();

        let error = client
            .send_request(Method::GET, "/big", None, None)
            .err()
            .unwrap()
            .to_string();

        assert_eq!(
            error,
            "schema registry error: schema registry response larger than 8 MiB"
        );
    }

    #[test]
    fn rejects_url_with_credentials() {
        let error = RegistryHttpClient::new(
            "https://app:pw@sr.example:8081",
            &RegistryAuthConfig::Anonymous,
            None,
            &FixedSecretResolver::returning("unused"),
            Duration::from_secs(5),
        )
        .err()
        .unwrap()
        .to_string();

        assert_eq!(
            error,
            "schema registry error: schema registry URL must not contain credentials"
        );
    }

    #[test]
    fn silent_registry_reports_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (release, hold) = mpsc::channel::<()>();
        let server = thread::spawn(move || {
            let accepted = listener.accept();
            let _ = hold.recv();
            drop(accepted);
        });
        let client = RegistryHttpClient::new(
            &url,
            &RegistryAuthConfig::Anonymous,
            None,
            &FixedSecretResolver::returning("unused"),
            Duration::from_millis(300),
        )
        .unwrap();

        let error = client
            .send_request(Method::GET, "/x", None, None)
            .err()
            .unwrap()
            .to_string();

        release.send(()).unwrap();
        server.join().unwrap();
        assert!(
            error
                .starts_with("schema registry error: request to schema registry failed: timed out"),
            "error: {error}"
        );
    }

    #[test]
    fn stalled_body_reports_read_timeout() {
        use std::io::Write;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (release, hold) = mpsc::channel::<()>();
        let server = thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut request = [0u8; 1024];
                let _ = stream.read(&mut request);
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nConnection: close\r\n\r\npartial",
                );
                let _ = hold.recv();
            }
        });
        let client = RegistryHttpClient::new(
            &url,
            &RegistryAuthConfig::Anonymous,
            None,
            &FixedSecretResolver::returning("unused"),
            Duration::from_millis(300),
        )
        .unwrap();

        let error = client
            .send_request(Method::GET, "/x", None, None)
            .err()
            .unwrap()
            .to_string();

        release.send(()).unwrap();
        server.join().unwrap();
        assert_eq!(
            error,
            "schema registry error: could not read the schema registry response: timed out"
        );
    }

    fn header_of<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}
