//! The one write the offset commands make: committing offsets on behalf of a group.

use std::collections::BTreeMap;

use crate::error::CoreResult;

/// Commits offsets for a consumer group that kafcli is not a member of.
pub trait GroupOffsetCommitter {
    /// Commits every `(topic, partition) -> offset` of `offsets` for `group` in one request.
    fn commit_group_offsets(
        &self,
        group: &str,
        offsets: &BTreeMap<(String, i32), i64>,
    ) -> CoreResult<()>;
}
