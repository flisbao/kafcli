use super::token_http_client::TokenHttpClient;
use super::token_response::parse_token_response;
use crate::auth::token::access_token::AccessToken;
use crate::error::CoreResult;
use crate::registry::path_segment::encode_path_segment;
use secrecy::SecretString;
use std::time::SystemTime;

/// POSTs a form to `<authority>/<tenant>/oauth2/v2.0/token` and reads the token.
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
    let response = http.post_form(&url, form, secrets)?;
    parse_token_response(response.status, &response.body, SystemTime::now(), secrets)
}
