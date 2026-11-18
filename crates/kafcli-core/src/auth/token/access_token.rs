use secrecy::SecretString;
use std::fmt;
use std::time::SystemTime;

/// A bearer token and the moment it stops being valid. The value is never printed:
/// `Debug` shows `***` in its place.
#[derive(Clone)]
pub struct AccessToken {
    pub value: SecretString,
    pub expires_at: SystemTime,
}

impl AccessToken {
    pub fn new(value: SecretString, expires_at: SystemTime) -> Self {
        Self { value, expires_at }
    }
}

impl fmt::Debug for AccessToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AccessToken")
            .field("expires_at", &self.expires_at)
            .field("value", &"***")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::SecretString;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn debug_hides_token_value() {
        let token = AccessToken::new(
            SecretString::from("super-secret-token"),
            UNIX_EPOCH + Duration::from_secs(2_000_000_000),
        );

        let rendered = format!("{token:?}");

        assert!(!rendered.contains("super-secret-token"));
        assert!(rendered.contains("***"));
    }
}
