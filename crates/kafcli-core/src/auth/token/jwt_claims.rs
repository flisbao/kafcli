use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::{Map, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_PAYLOAD_CHARS: usize = 64 * 1024;
const PRINCIPAL_CLAIMS: [&str; 5] = ["upn", "appid", "azp", "oid", "sub"];

/// Claims read from a JWT payload. The signature is not checked, so these values are only
/// hints for display and expiry planning, never proof of identity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JwtClaims {
    pub expires_at: Option<SystemTime>,
    pub subject: Option<String>,
    pub principal: Option<String>,
}

/// Reads `exp`, `sub` and the first principal claim from a JWT without verifying it.
/// Returns `None` for anything that is not a three-part JWT with a JSON object payload.
pub fn read_unverified_jwt_claims(token: &str) -> Option<JwtClaims> {
    let segments: Vec<&str> = token.split('.').collect();
    let [_header, payload, _signature] = segments.as_slice() else {
        return None;
    };
    if payload.len() > MAX_PAYLOAD_CHARS {
        return None;
    }

    let bytes = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let claims: Value = serde_json::from_slice(&bytes).ok()?;
    let object = claims.as_object()?;

    Some(JwtClaims {
        expires_at: read_expiry(object),
        subject: read_string_claim(object, "sub"),
        principal: PRINCIPAL_CLAIMS
            .iter()
            .find_map(|name| read_string_claim(object, name)),
    })
}

fn read_expiry(object: &Map<String, Value>) -> Option<SystemTime> {
    let seconds = object
        .get("exp")
        .and_then(Value::as_f64)
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)?;
    UNIX_EPOCH.checked_add(Duration::from_secs(seconds.trunc() as u64))
}

fn read_string_claim(object: &Map<String, Value>, name: &str) -> Option<String> {
    object.get(name).and_then(Value::as_str).map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use std::time::{Duration, UNIX_EPOCH};

    fn encode(json: &str) -> String {
        URL_SAFE_NO_PAD.encode(json.as_bytes())
    }

    #[test]
    fn reads_exp_sub_and_principal_without_verifying() {
        let token = format!(
            "{}.{}.",
            encode(r#"{"alg":"none"}"#),
            encode(r#"{"sub":"s","appid":"a","exp":2000000000}"#)
        );

        let claims = read_unverified_jwt_claims(&token).unwrap();

        assert_eq!(
            claims.expires_at,
            Some(UNIX_EPOCH + Duration::from_secs(2_000_000_000))
        );
        assert_eq!(claims.subject.as_deref(), Some("s"));
        assert_eq!(claims.principal.as_deref(), Some("a"));
    }

    #[test]
    fn principal_prefers_upn_over_later_claims() {
        let token = format!(
            "{}.{}.",
            encode(r#"{"alg":"none"}"#),
            encode(r#"{"oid":"o","sub":"s","upn":"user@example.com"}"#)
        );

        let claims = read_unverified_jwt_claims(&token).unwrap();

        assert_eq!(claims.principal.as_deref(), Some("user@example.com"));
        assert_eq!(claims.expires_at, None);
    }

    #[test]
    fn fractional_exp_is_truncated_to_whole_seconds() {
        let token = format!(
            "{}.{}.",
            encode(r#"{"alg":"none"}"#),
            encode(r#"{"exp":2000000000.9}"#)
        );

        let claims = read_unverified_jwt_claims(&token).unwrap();

        assert_eq!(
            claims.expires_at,
            Some(UNIX_EPOCH + Duration::from_secs(2_000_000_000))
        );
    }

    #[test]
    fn opaque_token_has_no_claims() {
        assert!(read_unverified_jwt_claims("not-a-jwt").is_none());
    }

    #[test]
    fn oversized_payload_is_rejected() {
        let payload = encode(&format!(r#"{{"sub":"{}"}}"#, "a".repeat(70 * 1024)));
        let token = format!("{}.{}.", encode(r#"{"alg":"none"}"#), payload);

        assert!(read_unverified_jwt_claims(&token).is_none());
    }
}
