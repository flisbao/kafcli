use crate::error::{CoreError, CoreResult};
use crate::io_limits::{MAX_PEM_FILE_BYTES, read_file_at_most};
use aws_lc_rs::digest::{self, SHA256};
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Value, json};
use std::fmt;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Lifetime of one client assertion, in seconds. Entra ID accepts at most ten minutes.
const ASSERTION_LIFETIME_SECONDS: u64 = 600;

/// Application certificate and its RSA key, read once from a PEM file.
pub struct ClientCertificate {
    key_pair: RsaKeyPair,
    thumbprint_sha256: String,
}

impl fmt::Debug for ClientCertificate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientCertificate")
            .finish_non_exhaustive()
    }
}

impl ClientCertificate {
    /// Reads a PEM file with the first `CERTIFICATE` block and an unencrypted RSA private key.
    /// Errors name the file, never its content.
    pub fn load_from_pem_file(path: &Path) -> CoreResult<Self> {
        let unreadable = || {
            CoreError::Authentication(format!(
                "could not read certificate file '{}'",
                path.display()
            ))
        };
        let bytes = read_file_at_most(path, MAX_PEM_FILE_BYTES)
            .map_err(|_| unreadable())?
            .ok_or_else(unreadable)?;
        // The PEM holds the private key: keep it in memory that is zeroed when dropped.
        let text = SecretString::from(String::from_utf8(bytes).map_err(|_| unreadable())?);
        Self::from_pem_source(text.expose_secret(), &path.display().to_string())
    }

    #[cfg(test)]
    pub(crate) fn from_pem_text(text: &str) -> CoreResult<Self> {
        Self::from_pem_source(text, "<PEM text>")
    }

    fn from_pem_source(text: &str, source: &str) -> CoreResult<Self> {
        let certificate_der = CertificateDer::pem_slice_iter(text.as_bytes())
            .filter_map(Result::ok)
            .next()
            .ok_or_else(|| {
                CoreError::Authentication(format!(
                    "certificate file '{source}' has no CERTIFICATE block"
                ))
            })?;
        let key_pair = read_rsa_key_pair(text.as_bytes()).ok_or_else(|| {
            CoreError::Authentication(format!(
                "certificate file '{source}' has no RSA private key; only unencrypted RSA keys are supported"
            ))
        })?;
        let thumbprint_sha256 = URL_SAFE_NO_PAD.encode(digest::digest(&SHA256, &certificate_der));
        Ok(Self {
            key_pair,
            thumbprint_sha256,
        })
    }
}

/// First RSA key in the PEM text, in PKCS#8 (`PRIVATE KEY`) or PKCS#1 (`RSA PRIVATE KEY`) form.
/// Other key types and encrypted keys are skipped, which leaves no key to return.
fn read_rsa_key_pair(pem: &[u8]) -> Option<RsaKeyPair> {
    PrivateKeyDer::pem_slice_iter(pem)
        .filter_map(Result::ok)
        .find_map(|key| match key {
            PrivateKeyDer::Pkcs8(der) => RsaKeyPair::from_pkcs8(der.secret_pkcs8_der()).ok(),
            PrivateKeyDer::Pkcs1(der) => RsaKeyPair::from_der(der.secret_pkcs1_der()).ok(),
            _ => None,
        })
}

/// Builds the RS256 client assertion that Entra ID accepts in place of a client secret.
/// The header carries the SHA-256 thumbprint of the certificate in `x5t#S256`.
pub fn build_client_assertion(
    certificate: &ClientCertificate,
    client_id: &str,
    audience: &str,
    now: SystemTime,
    assertion_id: &str,
) -> CoreResult<SecretString> {
    let issued_at = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| assertion_error("system clock is before the Unix epoch"))?
        .as_secs();
    let header = json!({
        "alg": "RS256",
        "typ": "JWT",
        "x5t#S256": certificate.thumbprint_sha256,
    });
    let claims = json!({
        "aud": audience,
        "iss": client_id,
        "sub": client_id,
        "jti": assertion_id,
        "nbf": issued_at,
        "iat": issued_at,
        "exp": issued_at + ASSERTION_LIFETIME_SECONDS,
    });
    let signing_input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(encode_json(&header)?),
        URL_SAFE_NO_PAD.encode(encode_json(&claims)?)
    );
    let mut signature = vec![0u8; certificate.key_pair.public_modulus_len()];
    certificate
        .key_pair
        .sign(
            &RSA_PKCS1_SHA256,
            &SystemRandom::new(),
            signing_input.as_bytes(),
            &mut signature,
        )
        .map_err(|_| assertion_error("could not sign the client assertion"))?;
    Ok(SecretString::from(format!(
        "{signing_input}.{}",
        URL_SAFE_NO_PAD.encode(&signature)
    )))
}

fn encode_json(value: &Value) -> CoreResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|_| assertion_error("could not encode the client assertion"))
}

fn assertion_error(message: &str) -> CoreError {
    CoreError::Authentication(message.to_owned())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn pem_file_larger_than_limit_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("large.pem");
        std::fs::write(&path, vec![b'a'; 1024 * 1024 + 1]).unwrap();

        let error = ClientCertificate::load_from_pem_file(&path)
            .err()
            .unwrap()
            .to_string();

        assert!(
            error.contains(&format!(
                "could not read certificate file '{}'",
                path.display()
            )),
            "error: {error}"
        );
    }
    use aws_lc_rs::digest;
    use aws_lc_rs::encoding::AsDer;
    use aws_lc_rs::rsa::{KeyPair, KeySize};
    use aws_lc_rs::signature::{KeyPair as _, RSA_PKCS1_2048_8192_SHA256, UnparsedPublicKey};
    use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
    use secrecy::ExposeSecret;
    use std::time::{Duration, UNIX_EPOCH};

    const AUDIENCE: &str = "https://login.microsoftonline.com/tenant-1/oauth2/v2.0/token";
    const CERTIFICATE_BYTES: &[u8] = b"arbitrary certificate bytes, never validated locally";

    fn pem_block(label: &str, der: &[u8]) -> String {
        let body = STANDARD.encode(der);
        format!("-----BEGIN {label}-----\n{body}\n-----END {label}-----\n")
    }

    fn generated_key() -> KeyPair {
        KeyPair::generate(KeySize::Rsa2048).unwrap()
    }

    fn pkcs8_pem_text(key: &KeyPair) -> String {
        let der: aws_lc_rs::encoding::Pkcs8V1Der<'static> = key.as_der().unwrap();
        pem_block("PRIVATE KEY", der.as_ref())
    }

    /// A certificate and key pair for tests of the sources that sign assertions.
    pub(crate) fn sample_certificate() -> ClientCertificate {
        let key = generated_key();
        let pem = format!(
            "{}{}",
            pem_block("CERTIFICATE", CERTIFICATE_BYTES),
            pkcs8_pem_text(&key)
        );
        ClientCertificate::from_pem_text(&pem).unwrap()
    }

    fn decode_part(part: &str) -> Value {
        let bytes = URL_SAFE_NO_PAD.decode(part).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn assertion_is_signed_rs256_with_sha256_thumbprint() {
        let key = generated_key();
        let pem = format!(
            "{}{}",
            pem_block("CERTIFICATE", CERTIFICATE_BYTES),
            pkcs8_pem_text(&key)
        );
        let certificate = ClientCertificate::from_pem_text(&pem).unwrap();
        let now = UNIX_EPOCH + Duration::from_secs(2_000_000_000);

        let assertion = build_client_assertion(&certificate, "app", AUDIENCE, now, "id-1").unwrap();

        let token = assertion.expose_secret();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3, "token: {token}");
        let header = decode_part(parts[0]);
        assert_eq!(header["alg"], "RS256");
        assert_eq!(header["typ"], "JWT");
        let expected_thumbprint =
            URL_SAFE_NO_PAD.encode(digest::digest(&digest::SHA256, CERTIFICATE_BYTES));
        assert_eq!(header["x5t#S256"], expected_thumbprint.as_str());
        let claims = decode_part(parts[1]);
        assert_eq!(claims["aud"], AUDIENCE);
        assert_eq!(claims["iss"], "app");
        assert_eq!(claims["sub"], "app");
        assert_eq!(claims["jti"], "id-1");
        assert_eq!(claims["nbf"], 2_000_000_000u64);
        assert_eq!(claims["iat"], 2_000_000_000u64);
        assert_eq!(
            claims["exp"].as_u64().unwrap() - claims["nbf"].as_u64().unwrap(),
            600
        );
        let signature = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let public_key =
            UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, key.public_key().as_ref());
        public_key
            .verify(signing_input.as_bytes(), &signature)
            .expect("signature must verify with the certificate's key");
    }

    #[test]
    fn first_certificate_block_is_the_application_certificate() {
        let key = generated_key();
        let pem = format!(
            "{}{}{}",
            pem_block("CERTIFICATE", CERTIFICATE_BYTES),
            pem_block("CERTIFICATE", b"issuer certificate"),
            pkcs8_pem_text(&key)
        );
        let certificate = ClientCertificate::from_pem_text(&pem).unwrap();
        let now = UNIX_EPOCH + Duration::from_secs(2_000_000_000);

        let assertion = build_client_assertion(&certificate, "app", AUDIENCE, now, "id-1").unwrap();

        let header = decode_part(assertion.expose_secret().split('.').next().unwrap());
        let expected = URL_SAFE_NO_PAD.encode(digest::digest(&digest::SHA256, CERTIFICATE_BYTES));
        assert_eq!(header["x5t#S256"], expected.as_str());
    }

    #[test]
    fn pkcs1_rsa_key_is_accepted() {
        let key = generated_key();
        let pkcs8: aws_lc_rs::encoding::Pkcs8V1Der<'static> = key.as_der().unwrap();
        let pkcs1 = pkcs1_from_pkcs8(pkcs8.as_ref());
        let pem = format!(
            "{}{}",
            pem_block("CERTIFICATE", CERTIFICATE_BYTES),
            pem_block("RSA PRIVATE KEY", &pkcs1)
        );

        assert!(ClientCertificate::from_pem_text(&pem).is_ok());
    }

    #[test]
    fn pem_without_certificate_is_error() {
        let key = generated_key();

        let error = ClientCertificate::from_pem_text(&pkcs8_pem_text(&key)).unwrap_err();

        assert!(
            format!("{error}").contains("has no CERTIFICATE block"),
            "message: {error}"
        );
    }

    #[test]
    fn ec_key_is_rejected() {
        let pem = format!(
            "{}{}",
            pem_block("CERTIFICATE", CERTIFICATE_BYTES),
            pem_block("EC PRIVATE KEY", b"not a real key")
        );

        let error = ClientCertificate::from_pem_text(&pem).unwrap_err();

        assert!(
            format!("{error}").contains("only unencrypted RSA keys are supported"),
            "message: {error}"
        );
    }

    #[test]
    fn debug_output_hides_key_material() {
        let key = generated_key();
        let pem = format!(
            "{}{}",
            pem_block("CERTIFICATE", CERTIFICATE_BYTES),
            pkcs8_pem_text(&key)
        );
        let certificate = ClientCertificate::from_pem_text(&pem).unwrap();

        let debug = format!("{certificate:?}");

        assert!(!debug.contains(&URL_SAFE_NO_PAD.encode(CERTIFICATE_BYTES)));
        assert!(!debug.contains("BEGIN"));
    }

    /// Reads the PKCS#1 `RSAPublicKey`-style private key out of a PKCS#8 wrapper for the PKCS#1 test.
    fn pkcs1_from_pkcs8(pkcs8: &[u8]) -> Vec<u8> {
        let (_, outer, _) = read_tlv(pkcs8);
        let (_, _version, after_version) = read_tlv(outer);
        let (_, _algorithm, after_algorithm) = read_tlv(after_version);
        let (tag, private_key, _) = read_tlv(after_algorithm);
        assert_eq!(tag, 0x04, "PKCS#8 private key must be an OCTET STRING");
        private_key.to_vec()
    }

    /// Splits one DER element into its tag, content and the bytes after it.
    fn read_tlv(input: &[u8]) -> (u8, &[u8], &[u8]) {
        let tag = input[0];
        let first_length = input[1];
        let (length, header_length) = if first_length < 0x80 {
            (usize::from(first_length), 2)
        } else {
            let count = usize::from(first_length & 0x7f);
            let length = input[2..2 + count]
                .iter()
                .fold(0usize, |total, byte| (total << 8) | usize::from(*byte));
            (length, 2 + count)
        };
        let end = header_length + length;
        (tag, &input[header_length..end], &input[end..])
    }
}
