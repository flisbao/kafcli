use super::client_properties::ClientProperties;
use super::tls_settings::apply_tls_settings;
use std::path::Path;

/// Sets the settings shared by every OAUTHBEARER client: security protocol and mechanism.
/// The token is not a property; librdkafka asks for it through the client context.
pub fn apply_oauth_bearer_settings(
    properties: &mut ClientProperties,
    use_tls: bool,
    ca_location: Option<&Path>,
) {
    let security_protocol = if use_tls {
        "SASL_SSL"
    } else {
        "SASL_PLAINTEXT"
    };
    apply_tls_settings(properties, security_protocol, ca_location);
    properties.set_plain("sasl.mechanism", "OAUTHBEARER");
}
