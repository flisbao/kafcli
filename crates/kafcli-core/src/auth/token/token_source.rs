use super::access_token::AccessToken;
use crate::error::CoreResult;

/// Something that can obtain a fresh access token, such as an Entra ID client or the IMDS endpoint.
pub trait TokenSource: Send + Sync {
    fn fetch_access_token(&self) -> CoreResult<AccessToken>;

    /// Short name of the token type, such as `azure-sp-secret`. Never contains secret material.
    fn describe_source(&self) -> &'static str;
}
