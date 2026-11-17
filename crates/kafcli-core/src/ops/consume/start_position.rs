use crate::error::CoreError;
use std::str::FromStr;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// Where a consume run starts reading each selected partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartPosition {
    Earliest,
    Latest,
    Offset(i64),
    Time(OffsetDateTime),
}

impl FromStr for StartPosition {
    type Err = CoreError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        parse_start_position(text).ok_or_else(|| {
            CoreError::InvalidInput(format!(
                "invalid start position '{text}': expected earliest, latest, offset:N or time:RFC3339"
            ))
        })
    }
}

fn parse_start_position(text: &str) -> Option<StartPosition> {
    match text {
        "earliest" => Some(StartPosition::Earliest),
        "latest" => Some(StartPosition::Latest),
        _ => {
            if let Some(number) = text.strip_prefix("offset:") {
                let offset = number.parse::<i64>().ok()?;
                (offset >= 0).then_some(StartPosition::Offset(offset))
            } else if let Some(instant) = text.strip_prefix("time:") {
                // RFC 3339 requires an explicit offset, so a local time without one is rejected.
                OffsetDateTime::parse(instant, &Rfc3339)
                    .ok()
                    .map(StartPosition::Time)
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_each_start_position() {
        assert_eq!(
            "earliest".parse::<StartPosition>().unwrap(),
            StartPosition::Earliest
        );
        assert_eq!(
            "latest".parse::<StartPosition>().unwrap(),
            StartPosition::Latest
        );
        assert_eq!(
            "offset:0".parse::<StartPosition>().unwrap(),
            StartPosition::Offset(0)
        );
        assert_eq!(
            "offset:42".parse::<StartPosition>().unwrap(),
            StartPosition::Offset(42)
        );
        let parsed = "time:2026-11-15T10:00:00Z"
            .parse::<StartPosition>()
            .unwrap();
        assert_eq!(
            parsed,
            StartPosition::Time(OffsetDateTime::parse("2026-11-15T10:00:00Z", &Rfc3339).unwrap())
        );
    }

    #[test]
    fn rejects_negative_offset() {
        let error = "offset:-1".parse::<StartPosition>().unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid input: invalid start position 'offset:-1': expected earliest, latest, offset:N or time:RFC3339"
        );
    }

    #[test]
    fn rejects_time_without_offset_designator() {
        assert!("time:2026-11-15T10:00:00".parse::<StartPosition>().is_err());
        assert!("yesterday".parse::<StartPosition>().is_err());
    }
}
