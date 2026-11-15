use super::client_properties::ClientProperties;
use super::tls_settings::apply_tls_settings;
use crate::config::model::SaslCredentialsConfig;
use crate::error::CoreResult;
use crate::secret::resolver::SecretResolver;

/// Sets the SASL settings shared by every username/password mechanism:
/// security protocol, mechanism, username and the resolved password.
pub fn apply_sasl_settings(
    properties: &mut ClientProperties,
    credentials: &SaslCredentialsConfig,
    mechanism_name: &str,
    resolver: &dyn SecretResolver,
) -> CoreResult<()> {
    let security_protocol = if credentials.use_tls {
        "SASL_SSL"
    } else {
        "SASL_PLAINTEXT"
    };
    apply_tls_settings(
        properties,
        security_protocol,
        credentials.ca_location.as_deref(),
    );
    properties.set_plain("sasl.mechanism", mechanism_name);
    properties.set_plain("sasl.username", credentials.username.as_str());
    let password = resolver.resolve_secret(&credentials.secret_ref)?;
    properties.set_secret("sasl.password", password);
    Ok(())
}
