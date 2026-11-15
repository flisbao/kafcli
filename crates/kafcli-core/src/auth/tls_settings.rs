use super::client_properties::ClientProperties;
use std::path::Path;

const HOSTNAME_VERIFICATION_ALGORITHM: &str = "https";

/// Sets the security protocol and, for SSL-based protocols, the CA and hostname verification.
pub fn apply_tls_settings(
    properties: &mut ClientProperties,
    security_protocol: &str,
    ca_location: Option<&Path>,
) {
    properties.set_plain("security.protocol", security_protocol);
    if !matches!(security_protocol, "SSL" | "SASL_SSL") {
        return;
    }
    if let Some(location) = ca_location {
        properties.set_plain("ssl.ca.location", location.to_string_lossy());
    }
    properties.set_plain(
        "ssl.endpoint.identification.algorithm",
        HOSTNAME_VERIFICATION_ALGORITHM,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sets_hostname_verification_for_ssl_protocols() {
        let mut properties = ClientProperties::default();
        apply_tls_settings(&mut properties, "SASL_SSL", None);
        assert_eq!(
            properties.plain_value("security.protocol"),
            Some("SASL_SSL")
        );
        assert_eq!(
            properties.plain_value("ssl.endpoint.identification.algorithm"),
            Some("https")
        );
    }

    #[test]
    fn leaves_ssl_keys_out_for_plaintext_protocols() {
        let mut properties = ClientProperties::default();
        apply_tls_settings(
            &mut properties,
            "SASL_PLAINTEXT",
            Some(Path::new("/ca.pem")),
        );
        assert_eq!(
            properties.plain_value("security.protocol"),
            Some("SASL_PLAINTEXT")
        );
        assert_eq!(properties.plain_value("ssl.ca.location"), None);
        assert_eq!(
            properties.plain_value("ssl.endpoint.identification.algorithm"),
            None
        );
    }
}
