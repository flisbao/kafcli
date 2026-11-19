//! The checks every destructive flow runs before it writes a backup: the group is stopped, the
//! operator confirms, the group is still stopped, and no commit happened since the plan was made.

use std::collections::BTreeMap;

use crate::error::CoreResult;
use crate::ops::offsets::group_activity::GroupActivity;
use crate::ops::offsets::offset_reset_execution::{
    DestructiveCollaborators, DestructiveRunSettings, check_committed_offsets_unchanged,
    confirm_group_is_stopped,
};

/// Steps 1 to 3 of the destructive order plus the check that the committed offsets are still the
/// planned ones. A failure here means nothing was written; the caller records it as aborted.
pub(crate) fn confirm_and_check(
    collaborators: &DestructiveCollaborators,
    settings: &DestructiveRunSettings,
    group: &str,
    first_activity: GroupActivity,
    confirmation_subject: &str,
    expected_current: &BTreeMap<(String, i32), Option<i64>>,
) -> CoreResult<GroupActivity> {
    let activity = confirm_group_is_stopped(
        collaborators,
        group,
        first_activity,
        confirmation_subject,
        settings,
    )?;
    check_committed_offsets_unchanged(collaborators.reader, group, expected_current)?;
    Ok(activity)
}
