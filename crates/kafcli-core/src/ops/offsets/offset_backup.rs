//! Offset backups written before a destructive change, and read back by `offsets restore`.

use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::{CoreError, CoreResult};
use crate::io_limits::read_file_at_most;
use crate::message::timestamp_text::format_rfc3339_millis;
use crate::ops::offsets::offset_reset_plan::OffsetResetPlan;

pub const OFFSET_BACKUP_VERSION: u32 = 1;
/// Directory under the state directory that holds the backups.
pub const OFFSET_BACKUP_DIRECTORY: &str = "offsets-backup";
pub const MAX_BACKUP_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// Longest group part of a backup file name, in bytes.
const MAX_GROUP_FILE_NAME_BYTES: usize = 100;
/// Suffixes `-1` to `-9` tried when a backup with the same name exists.
const MAX_NAME_SUFFIX: u32 = 9;
#[cfg(unix)]
const BACKUP_DIRECTORY_MODE: u32 = 0o700;
#[cfg(unix)]
const BACKUP_FILE_MODE: u32 = 0o600;

/// One partition as it was before the change. Same fields as `partitions` of `offsets show`
/// (ADR 0006); unknown fields are accepted on read because that contract only grows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupPartition {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: Option<i64>,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
    pub lag: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffsetBackup {
    pub kafcli_offsets_backup: u32,
    pub created_at: String,
    pub context: String,
    pub group: String,
    pub command: String,
    pub partitions: Vec<BackupPartition>,
}

/// The state before `plan`: one partition per planned change, with its current commit.
pub fn build_offset_backup(
    plan: &OffsetResetPlan,
    context: &str,
    command: &str,
    now: OffsetDateTime,
) -> OffsetBackup {
    OffsetBackup {
        kafcli_offsets_backup: OFFSET_BACKUP_VERSION,
        created_at: format_rfc3339_millis(now).unwrap_or_default(),
        context: context.to_owned(),
        group: plan.group.clone(),
        command: command.to_owned(),
        partitions: plan
            .changes
            .iter()
            .map(|change| BackupPartition {
                topic: change.topic.clone(),
                partition: change.partition,
                committed_offset: change.current_offset,
                log_start_offset: change.log_start_offset,
                log_end_offset: change.log_end_offset,
                lag: change
                    .current_offset
                    .map(|current| change.log_end_offset.saturating_sub(current).max(0)),
            })
            .collect(),
    }
}

/// Writes `backup` as a new file in `directory` and returns its path. Never overwrites a file.
///
/// The name is `<group>-<created_at>.json` with the group reduced to `[A-Za-z0-9._-]` and 100
/// bytes, and `-1` to `-9` before `.json` when the name is taken. On unix the directory is 0700
/// and the file 0600. The data is on disk before returning.
pub fn write_offset_backup(directory: &Path, backup: &OffsetBackup) -> CoreResult<PathBuf> {
    create_backup_directory(directory)?;
    let mut content = serde_json::to_string_pretty(backup)
        .map_err(|error| CoreError::Io(std::io::Error::other(error)))?;
    content.push('\n');
    let stem = format!(
        "{}-{}",
        sanitize_group_for_file_name(&backup.group),
        compact_timestamp(&backup.created_at)
    );
    for suffix in 0..=MAX_NAME_SUFFIX {
        let name = if suffix == 0 {
            format!("{stem}.json")
        } else {
            format!("{stem}-{suffix}.json")
        };
        let path = directory.join(name);
        match create_new_file(&path) {
            Ok(mut file) => {
                file.write_all(content.as_bytes())?;
                file.sync_all()?;
                return Ok(path);
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists && suffix < MAX_NAME_SUFFIX => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(CoreError::Io(std::io::Error::from(
        ErrorKind::AlreadyExists,
    )))
}

/// Reads and checks a backup file: at most 16 MiB, valid JSON, version 1, some partitions.
pub fn read_offset_backup(path: &Path) -> CoreResult<OffsetBackup> {
    let bytes =
        read_file_at_most(path, MAX_BACKUP_FILE_BYTES)?.ok_or_else(oversized_backup_error)?;
    let backup: OffsetBackup = serde_json::from_slice(&bytes).map_err(|error| {
        // serde_json errors name a line, a column and the expected token, never the input.
        CoreError::InvalidInput(format!("backup file is not valid JSON: {error}"))
    })?;
    if backup.kafcli_offsets_backup != OFFSET_BACKUP_VERSION {
        return Err(CoreError::InvalidInput(format!(
            "unsupported backup version {}; this build reads version {OFFSET_BACKUP_VERSION}",
            backup.kafcli_offsets_backup
        )));
    }
    if backup.partitions.is_empty() {
        return Err(CoreError::InvalidInput(
            "backup file has no partitions".to_owned(),
        ));
    }
    Ok(backup)
}

fn oversized_backup_error() -> CoreError {
    CoreError::InvalidInput("backup file is larger than 16 MiB".to_owned())
}

fn sanitize_group_for_file_name(group: &str) -> String {
    group
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .take(MAX_GROUP_FILE_NAME_BYTES)
        .collect()
}

/// `2026-11-18T11:06:40.123Z` -> `20261118T110640.123Z`, safe in file names on every system.
fn compact_timestamp(created_at: &str) -> String {
    created_at
        .chars()
        .filter(|character| !matches!(character, '-' | ':'))
        .collect()
}

fn create_backup_directory(directory: &Path) -> std::io::Result<()> {
    let mut builder = DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(BACKUP_DIRECTORY_MODE);
    }
    builder.create(directory)
}

fn create_new_file(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(BACKUP_FILE_MODE);
    }
    options.open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::offsets::offset_reset_plan::{OffsetResetPlan, PlannedOffsetChange};
    use std::fs;
    use time::OffsetDateTime;

    fn plan(group: &str) -> OffsetResetPlan {
        OffsetResetPlan {
            group: group.to_owned(),
            changes: vec![
                PlannedOffsetChange {
                    topic: "t".to_owned(),
                    partition: 0,
                    current_offset: Some(13),
                    new_offset: 20,
                    log_start_offset: 0,
                    log_end_offset: 20,
                    clamped: false,
                    messages_skipped: Some(7),
                    messages_reprocessed: Some(0),
                },
                PlannedOffsetChange {
                    topic: "t".to_owned(),
                    partition: 1,
                    current_offset: None,
                    new_offset: 20,
                    log_start_offset: 0,
                    log_end_offset: 20,
                    clamped: false,
                    messages_skipped: None,
                    messages_reprocessed: None,
                },
            ],
            total_skipped: 7,
            total_reprocessed: 0,
            warnings: Vec::new(),
        }
    }

    fn instant() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp_nanos(1_795_000_000_123_000_000).unwrap()
    }

    fn backup(group: &str) -> OffsetBackup {
        build_offset_backup(&plan(group), "dev", "offsets set", instant())
    }

    #[test]
    fn backup_keeps_state_before_the_change() {
        let backup = backup("g");

        assert_eq!(backup.kafcli_offsets_backup, 1);
        assert_eq!(backup.created_at, "2026-11-18T11:06:40.123Z");
        assert_eq!(backup.context, "dev");
        assert_eq!(backup.group, "g");
        assert_eq!(backup.command, "offsets set");
        assert_eq!(
            backup.partitions[0],
            BackupPartition {
                topic: "t".to_owned(),
                partition: 0,
                committed_offset: Some(13),
                log_start_offset: 0,
                log_end_offset: 20,
                lag: Some(7),
            }
        );
        assert_eq!(backup.partitions[1].committed_offset, None);
        assert_eq!(backup.partitions[1].lag, None);
    }

    #[test]
    fn backup_round_trips_through_file() {
        let directory = tempfile::tempdir().unwrap();
        let written = backup("g");

        let path = write_offset_backup(&directory.path().join("offsets-backup"), &written).unwrap();
        let read = read_offset_backup(&path).unwrap();

        assert_eq!(read, written);
        assert_eq!(
            path.file_name().and_then(|name| name.to_str()),
            Some("g-20261118T110640.123Z.json")
        );
        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("\n  \"group\": \"g\""), "{text}");
    }

    #[cfg(unix)]
    #[test]
    fn backup_file_and_directory_are_private() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let backup_directory = directory.path().join("state").join("offsets-backup");

        let path = write_offset_backup(&backup_directory, &backup("g")).unwrap();

        let mode =
            |path: &std::path::Path| fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&backup_directory), 0o700);
        assert_eq!(mode(&path), 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn existing_backup_directory_keeps_its_mode() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let backup_directory = directory.path().join("offsets-backup");
        fs::create_dir(&backup_directory).unwrap();
        fs::set_permissions(&backup_directory, fs::Permissions::from_mode(0o755)).unwrap();

        write_offset_backup(&backup_directory, &backup("g")).unwrap();

        let mode = fs::metadata(&backup_directory)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o755);
    }

    #[test]
    fn group_name_is_sanitized_in_file_name() {
        let directory = tempfile::tempdir().unwrap();
        let long_group = format!("a/b c{}", "x".repeat(300));

        let path = write_offset_backup(directory.path(), &backup(&long_group)).unwrap();

        let name = path.file_name().and_then(|name| name.to_str()).unwrap();
        assert!(name.starts_with("a_b_c"), "{name}");
        assert_eq!(path.parent(), Some(directory.path()));
        let sanitized = name.trim_end_matches("-20261118T110640.123Z.json");
        assert_eq!(sanitized.len(), 100, "{name}");
    }

    #[test]
    fn existing_file_gets_numbered_suffix() {
        let directory = tempfile::tempdir().unwrap();
        let first_backup = backup("g");
        let mut second_backup = backup("g");
        second_backup.command = "offsets restore".to_owned();

        let first = write_offset_backup(directory.path(), &first_backup).unwrap();
        let second = write_offset_backup(directory.path(), &second_backup).unwrap();

        assert_ne!(first, second);
        assert_eq!(
            second.file_name().and_then(|name| name.to_str()),
            Some("g-20261118T110640.123Z-1.json")
        );
        assert_eq!(read_offset_backup(&first).unwrap(), first_backup);
        assert_eq!(read_offset_backup(&second).unwrap(), second_backup);
    }

    #[test]
    fn unknown_fields_are_accepted() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("b.json");
        fs::write(
            &path,
            r#"{"kafcli_offsets_backup":1,"created_at":"x","context":"dev","group":"g","command":"offsets set","future":true,
               "partitions":[{"topic":"t","partition":0,"committed_offset":3,"log_start_offset":0,"log_end_offset":9,"lag":6,"member":null}]}"#,
        )
        .unwrap();

        let read = read_offset_backup(&path).unwrap();

        assert_eq!(read.partitions[0].committed_offset, Some(3));
    }

    #[test]
    fn other_version_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("b.json");
        fs::write(
            &path,
            r#"{"kafcli_offsets_backup":2,"created_at":"x","context":"dev","group":"g","command":"c","partitions":[]}"#,
        )
        .unwrap();

        let result = read_offset_backup(&path);

        assert!(
            matches!(&result, Err(CoreError::InvalidInput(text)) if text == "unsupported backup version 2; this build reads version 1"),
            "{result:?}"
        );
    }

    #[test]
    fn empty_partitions_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("b.json");
        fs::write(
            &path,
            r#"{"kafcli_offsets_backup":1,"created_at":"x","context":"dev","group":"g","command":"c","partitions":[]}"#,
        )
        .unwrap();

        let result = read_offset_backup(&path);

        assert!(
            matches!(&result, Err(CoreError::InvalidInput(text)) if text == "backup file has no partitions"),
            "{result:?}"
        );
    }

    #[test]
    fn invalid_json_error_does_not_echo_content() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("b.json");
        fs::write(&path, "secret-looking-text").unwrap();

        let error = read_offset_backup(&path).unwrap_err();

        let text = error.to_string();
        assert!(
            text.starts_with("invalid input: backup file is not valid JSON: "),
            "{text}"
        );
        assert!(!text.contains("secret-looking-text"), "{text}");
    }

    #[test]
    fn oversized_file_is_rejected_before_parsing() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("b.json");
        let file = fs::File::create(&path).unwrap();
        file.set_len(MAX_BACKUP_FILE_BYTES + 1).unwrap();

        let result = read_offset_backup(&path);

        assert!(
            matches!(&result, Err(CoreError::InvalidInput(text)) if text == "backup file is larger than 16 MiB"),
            "{result:?}"
        );
    }
}
