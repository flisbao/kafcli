use std::time::Duration;

const MINIMUM_SECONDS: u64 = 1;
const MAXIMUM_SECONDS: u64 = 3600;
const INTERVAL_ERROR: &str = "watch interval must be 1s to 3600s, like 5s or 1m";

/// Parses a `--watch` interval: `<n>s`, `<n>m` or a bare number of seconds, from 1 s to 3600 s.
pub fn parse_watch_interval(text: &str) -> Result<Duration, String> {
    let (digits, unit_seconds) = match text.strip_suffix('m') {
        Some(minutes) => (minutes, 60),
        None => (text.strip_suffix('s').unwrap_or(text), 1),
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(INTERVAL_ERROR.to_owned());
    }
    digits
        .parse::<u64>()
        .ok()
        .and_then(|value| value.checked_mul(unit_seconds))
        .filter(|seconds| (MINIMUM_SECONDS..=MAXIMUM_SECONDS).contains(seconds))
        .map(Duration::from_secs)
        .ok_or_else(|| INTERVAL_ERROR.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_seconds_minutes_and_bare_numbers() {
        assert_eq!(parse_watch_interval("5s"), Ok(Duration::from_secs(5)));
        assert_eq!(parse_watch_interval("1m"), Ok(Duration::from_secs(60)));
        assert_eq!(parse_watch_interval("30"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_watch_interval("60m"), Ok(Duration::from_secs(3600)));
    }

    #[test]
    fn rejects_zero_and_above_one_hour() {
        let message = "watch interval must be 1s to 3600s, like 5s or 1m";
        for text in [
            "0",
            "0s",
            "3601s",
            "61m",
            "",
            "s",
            "-5s",
            "1h",
            "1.5s",
            "99999999999999999999",
        ] {
            assert_eq!(
                parse_watch_interval(text),
                Err(message.to_owned()),
                "{text}"
            );
        }
    }
}
