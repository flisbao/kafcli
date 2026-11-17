use time::{OffsetDateTime, UtcOffset, format_description};

const TIMESTAMP_FORMAT: &str =
    "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z";

/// Formats an instant as RFC 3339 UTC with exactly three fractional digits and a `Z` suffix.
///
/// Returns `None` when the instant cannot be formatted.
pub fn format_rfc3339_millis(instant: OffsetDateTime) -> Option<String> {
    let description = format_description::parse_borrowed::<2>(TIMESTAMP_FORMAT).ok()?;
    instant.to_offset(UtcOffset::UTC).format(&description).ok()
}
