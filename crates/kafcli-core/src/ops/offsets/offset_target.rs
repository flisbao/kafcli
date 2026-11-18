//! Where an offset reset moves each partition, limited to what the log still holds.

use time::OffsetDateTime;

use crate::ops::groups::group_snapshot::PartitionWatermarks;

/// The `--to-*` / `--shift-by` choice of `offsets set`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OffsetTarget {
    Offset(i64),
    Earliest,
    Latest,
    Datetime(OffsetDateTime),
    ShiftBy(i64),
}

/// The offset a partition moves to; `clamped` when the log's range changed the asked value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedOffset {
    pub offset: i64,
    pub clamped: bool,
}

/// Resolves `target` for one partition. The result is always inside `[low, high]`.
///
/// `Datetime` without a message at or after the instant goes to the log end, as
/// `kafka-consumer-groups.sh` does. `ShiftBy` needs the committed offset; the error text is meant
/// to follow the partition name.
pub fn resolve_target_offset(
    target: OffsetTarget,
    current: Option<i64>,
    watermarks: PartitionWatermarks,
    offset_for_time: Option<i64>,
) -> Result<ResolvedOffset, String> {
    let wanted = match target {
        OffsetTarget::Offset(offset) => offset,
        OffsetTarget::Earliest => watermarks.low,
        OffsetTarget::Latest => watermarks.high,
        OffsetTarget::Datetime(_) => offset_for_time.unwrap_or(watermarks.high),
        OffsetTarget::ShiftBy(delta) => {
            let current = current
                .ok_or_else(|| "has no committed offset; --shift-by needs one".to_owned())?;
            match current.checked_add(delta) {
                Some(shifted) => shifted,
                None if delta > 0 => i64::MAX,
                None => i64::MIN,
            }
        }
    };
    let offset = wanted.clamp(watermarks.low, watermarks.high.max(watermarks.low));
    Ok(ResolvedOffset {
        offset,
        clamped: offset != wanted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const WATERMARKS: PartitionWatermarks = PartitionWatermarks { low: 5, high: 20 };

    #[test]
    fn offset_above_log_end_is_clamped() {
        let resolved =
            resolve_target_offset(OffsetTarget::Offset(500), Some(10), WATERMARKS, None).unwrap();

        assert_eq!(
            resolved,
            ResolvedOffset {
                offset: 20,
                clamped: true
            }
        );
    }

    #[test]
    fn offset_below_log_start_is_clamped() {
        let resolved =
            resolve_target_offset(OffsetTarget::Offset(1), Some(10), WATERMARKS, None).unwrap();

        assert_eq!(
            resolved,
            ResolvedOffset {
                offset: 5,
                clamped: true
            }
        );
    }

    #[test]
    fn offset_inside_the_log_is_kept() {
        let resolved =
            resolve_target_offset(OffsetTarget::Offset(12), None, WATERMARKS, None).unwrap();

        assert_eq!(
            resolved,
            ResolvedOffset {
                offset: 12,
                clamped: false
            }
        );
    }

    #[test]
    fn earliest_and_latest_are_the_watermarks() {
        let earliest =
            resolve_target_offset(OffsetTarget::Earliest, Some(10), WATERMARKS, None).unwrap();
        let latest =
            resolve_target_offset(OffsetTarget::Latest, Some(10), WATERMARKS, None).unwrap();

        assert_eq!(
            earliest,
            ResolvedOffset {
                offset: 5,
                clamped: false
            }
        );
        assert_eq!(
            latest,
            ResolvedOffset {
                offset: 20,
                clamped: false
            }
        );
    }

    #[test]
    fn shift_by_moves_from_current() {
        let resolved =
            resolve_target_offset(OffsetTarget::ShiftBy(-3), Some(13), WATERMARKS, None).unwrap();

        assert_eq!(
            resolved,
            ResolvedOffset {
                offset: 10,
                clamped: false
            }
        );
    }

    #[test]
    fn shift_by_without_commit_is_error() {
        let result = resolve_target_offset(OffsetTarget::ShiftBy(-3), None, WATERMARKS, None);

        assert_eq!(
            result,
            Err("has no committed offset; --shift-by needs one".to_owned())
        );
    }

    #[test]
    fn shift_by_overflow_is_clamped_not_panicking() {
        let resolved = resolve_target_offset(
            OffsetTarget::ShiftBy(i64::MAX),
            Some(i64::MAX - 1),
            WATERMARKS,
            None,
        )
        .unwrap();
        let below =
            resolve_target_offset(OffsetTarget::ShiftBy(i64::MIN), Some(-2), WATERMARKS, None)
                .unwrap();

        assert_eq!(
            resolved,
            ResolvedOffset {
                offset: 20,
                clamped: true
            }
        );
        assert_eq!(
            below,
            ResolvedOffset {
                offset: 5,
                clamped: true
            }
        );
    }

    #[test]
    fn datetime_after_last_message_goes_to_log_end() {
        let instant = OffsetDateTime::UNIX_EPOCH;

        let after_last =
            resolve_target_offset(OffsetTarget::Datetime(instant), Some(10), WATERMARKS, None)
                .unwrap();
        let found = resolve_target_offset(
            OffsetTarget::Datetime(instant),
            Some(10),
            WATERMARKS,
            Some(8),
        )
        .unwrap();

        assert_eq!(
            after_last,
            ResolvedOffset {
                offset: 20,
                clamped: false
            }
        );
        assert_eq!(
            found,
            ResolvedOffset {
                offset: 8,
                clamped: false
            }
        );
    }
}
