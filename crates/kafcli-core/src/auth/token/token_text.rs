use crate::registry::registry_text::neutralize_registry_text;
use secrecy::{ExposeSecret, SecretString};

/// Makes text safe to show after a token request failed: every known secret becomes `*`,
/// then the text is neutralized and truncated like registry text.
pub fn redact_token_text(text: &str, secrets: &[&SecretString]) -> String {
    let mut values: Vec<&str> = secrets
        .iter()
        .map(|secret| secret.expose_secret())
        .filter(|value| !value.is_empty())
        .collect();
    // Longest first, so a secret that contains another one is not left half-hidden.
    values.sort_by_key(|value| std::cmp::Reverse(value.len()));

    let mut redacted = text.to_owned();
    for value in values {
        redacted = redacted.replace(value, "*");
    }
    neutralize_registry_text(&redacted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::SecretString;

    #[test]
    fn redacts_secret_then_truncates() {
        let secret = SecretString::from("secret-value");
        let text = format!("{}{}{}", "x".repeat(50), "secret-value", "y".repeat(450));

        let redacted = redact_token_text(&text, &[&secret]);

        assert!(!redacted.contains("secret-value"));
        assert_eq!(redacted.chars().count(), 200);
        assert_eq!(redacted, format!("{}*{}", "x".repeat(50), "y".repeat(149)));
    }
}
