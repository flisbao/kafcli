use super::entra_token_client::request_entra_token;
use super::token_http_client::TokenHttpClient;
use crate::auth::token::access_token::AccessToken;
use crate::auth::token::token_source::TokenSource;
use crate::error::{CoreError, CoreResult};
use secrecy::{ExposeSecret, SecretString};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

const CLIENT_ASSERTION_TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

/// Largest projected token file accepted, in bytes.
const MAX_TOKEN_FILE_BYTES: u64 = 64 * 1024;

/// Fetches tokens for a pod with workload identity: the projected service account token is the
/// client assertion. The file is read on every fetch because the kubelet renews it.
pub struct WorkloadIdentityTokenSource {
    http: TokenHttpClient,
    authority_host: String,
    tenant_id: String,
    client_id: String,
    token_file: PathBuf,
    scope: String,
}

impl WorkloadIdentityTokenSource {
    pub fn new(
        http: TokenHttpClient,
        authority_host: String,
        tenant_id: String,
        client_id: String,
        token_file: PathBuf,
        scope: String,
    ) -> Self {
        Self {
            http,
            authority_host,
            tenant_id,
            client_id,
            token_file,
            scope,
        }
    }
}

impl TokenSource for WorkloadIdentityTokenSource {
    fn fetch_access_token(&self) -> CoreResult<AccessToken> {
        let assertion = read_federated_token(&self.token_file)?;
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
        "azure-workload-identity"
    }
}

/// Reads the projected token, without its surrounding whitespace. The error names the path only.
fn read_federated_token(path: &Path) -> CoreResult<SecretString> {
    let unreadable = || {
        CoreError::Authentication(format!(
            "federated token file '{}' could not be read",
            path.display()
        ))
    };
    let file = File::open(path).map_err(|_| unreadable())?;
    let mut contents = String::new();
    file.take(MAX_TOKEN_FILE_BYTES + 1)
        .read_to_string(&mut contents)
        .map_err(|_| unreadable())?;
    if contents.len() as u64 > MAX_TOKEN_FILE_BYTES {
        return Err(CoreError::Authentication(format!(
            "federated token file '{}' is larger than 64 KiB",
            path.display()
        )));
    }
    let token = contents.trim();
    if token.is_empty() {
        return Err(CoreError::Authentication(format!(
            "federated token file '{}' is empty",
            path.display()
        )));
    }
    Ok(SecretString::from(token.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::azure::token_http_client::TOKEN_REQUEST_TIMEOUT;
    use crate::registry::fake_registry_server::{FakeRegistryServer, FakeRoute};
    use std::io::Write;

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

    fn source_for(authority_host: String, token_file: PathBuf) -> WorkloadIdentityTokenSource {
        WorkloadIdentityTokenSource::new(
            TokenHttpClient::new(TOKEN_REQUEST_TIMEOUT).unwrap(),
            authority_host,
            "tenant-1".to_string(),
            "app-1".to_string(),
            token_file,
            SCOPE.to_string(),
        )
    }

    fn token_file_with(content: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file
    }

    #[test]
    fn posts_federated_token_as_client_assertion() {
        let server = FakeRegistryServer::start(vec![token_route(
            200,
            r#"{"access_token":"token-from-route","expires_in":3600}"#,
        )])
        .unwrap();
        let file = token_file_with("federated-token-value\n");
        let source = source_for(server.url(), file.path().to_path_buf());

        let token = source.fetch_access_token().unwrap();

        assert_eq!(token.value.expose_secret(), "token-from-route");
        let requests = server.recorded_requests();
        let body = &requests[0].body;
        assert!(body.contains("client_id=app-1"), "body: {body}");
        assert!(
            body.contains("scope=https%3A%2F%2Fns.servicebus.windows.net%2F.default"),
            "body: {body}"
        );
        assert!(
            body.contains("grant_type=client_credentials"),
            "body: {body}"
        );
        assert!(
            body.contains(
                "client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"
            ),
            "body: {body}"
        );
        assert!(
            body.ends_with("client_assertion=federated-token-value"),
            "body: {body}"
        );
        assert_eq!(source.describe_source(), "azure-workload-identity");
    }

    #[test]
    fn rereads_token_file_on_every_fetch() {
        let server = FakeRegistryServer::start(vec![token_route(
            200,
            r#"{"access_token":"t","expires_in":3600}"#,
        )])
        .unwrap();
        let file = token_file_with("first-token");
        let source = source_for(server.url(), file.path().to_path_buf());
        source.fetch_access_token().unwrap();

        std::fs::write(file.path(), "second-token").unwrap();
        source.fetch_access_token().unwrap();

        let requests = server.recorded_requests();
        assert!(requests[0].body.contains("client_assertion=first-token"));
        assert!(requests[1].body.contains("client_assertion=second-token"));
    }

    #[test]
    fn surrounding_whitespace_of_token_file_is_removed() {
        let server = FakeRegistryServer::start(vec![token_route(
            200,
            r#"{"access_token":"t","expires_in":3600}"#,
        )])
        .unwrap();
        let file = token_file_with("  padded-token \n");
        let source = source_for(server.url(), file.path().to_path_buf());

        source.fetch_access_token().unwrap();

        let body = &server.recorded_requests()[0].body;
        assert!(
            body.ends_with("client_assertion=padded-token"),
            "body: {body}"
        );
    }

    #[test]
    fn missing_token_file_names_the_path() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("absent-token");
        let source = source_for("https://login.example".to_string(), missing.clone());

        let error = source.fetch_access_token().unwrap_err();

        assert!(matches!(&error, CoreError::Authentication(_)));
        assert_eq!(
            format!("{error}"),
            format!(
                "authentication error: federated token file '{}' could not be read",
                missing.display()
            )
        );
    }

    #[test]
    fn oversized_token_file_is_refused() {
        let file = token_file_with(&"x".repeat(64 * 1024 + 1));
        let source = source_for(
            "https://login.example".to_string(),
            file.path().to_path_buf(),
        );

        let error = source.fetch_access_token().unwrap_err();

        assert!(format!("{error}").contains("larger than 64 KiB"));
    }

    #[test]
    fn federated_token_never_in_error() {
        let server = FakeRegistryServer::start(vec![token_route(
            401,
            r#"{"error":"invalid_client","error_description":"the assertion federated-token-value is bad"}"#,
        )])
        .unwrap();
        let file = token_file_with("federated-token-value");
        let source = source_for(server.url(), file.path().to_path_buf());

        let error = source.fetch_access_token().unwrap_err();

        let message = format!("{error}");
        assert!(message.contains("azure token request failed (HTTP 401): invalid_client"));
        assert!(
            !message.contains("federated-token-value"),
            "message: {message}"
        );
    }
}
