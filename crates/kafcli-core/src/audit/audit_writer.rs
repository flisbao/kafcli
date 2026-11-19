use super::audit_record::AuditRecord;
use crate::error::{CoreError, CoreResult};
use std::fs::{DirBuilder, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub const AUDIT_LOG_FILE_NAME: &str = "audit.log";

/// Owner-only access on unix: the log records what was run against which context.
#[cfg(unix)]
const AUDIT_DIRECTORY_MODE: u32 = 0o700;
#[cfg(unix)]
const AUDIT_FILE_MODE: u32 = 0o600;

/// Appends audit records somewhere durable. The binary decides the destination.
pub trait AuditWriter {
    fn append_record(&self, record: &AuditRecord) -> CoreResult<()>;
}

/// Writes one JSON object per line to `audit.log` inside the state directory.
pub struct JsonLinesAuditWriter {
    log_path: PathBuf,
}

impl JsonLinesAuditWriter {
    pub fn create_in_directory(state_directory: &Path) -> Self {
        Self {
            log_path: state_directory.join(AUDIT_LOG_FILE_NAME),
        }
    }
}

impl AuditWriter for JsonLinesAuditWriter {
    /// Creates the directory if needed, then appends the record as one line in a single write.
    ///
    /// On unix the directory and a newly created file are private to the owner (0700 and 0600).
    /// The data is flushed to disk before returning.
    fn append_record(&self, record: &AuditRecord) -> CoreResult<()> {
        if let Some(directory) = self.log_path.parent() {
            create_audit_directory(directory)?;
        }
        refuse_symbolic_link(&self.log_path)?;
        let mut line = serde_json::to_string(record).map_err(map_serialization_error)?;
        line.push('\n');
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(AUDIT_FILE_MODE);
        }
        let mut file = options.open(&self.log_path)?;
        #[cfg(unix)]
        narrow_existing_log_mode(&file, &self.log_path)?;
        file.write_all(line.as_bytes())?;
        file.sync_all()?;
        Ok(())
    }
}

fn create_audit_directory(directory: &Path) -> std::io::Result<()> {
    let mut builder = DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(AUDIT_DIRECTORY_MODE);
    }
    builder.create(directory)
}

/// A log left readable by older versions is narrowed to 0600. This is best effort: a log the user
/// may append to but does not own (a shared `KAFCLI_STATE_DIR`) keeps its mode, with a warning,
/// instead of losing the audit record.
#[cfg(unix)]
fn narrow_existing_log_mode(file: &std::fs::File, log_path: &Path) -> std::io::Result<()> {
    match crate::io_limits::tighten_file_mode(file, AUDIT_FILE_MODE) {
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            tracing::warn!(
                "audit log '{}' is readable by others and kafcli may not change its mode",
                log_path.display()
            );
            Ok(())
        }
        other => other,
    }
}

/// A symbolic link at the log path could send the append to another file, so it is refused.
fn refuse_symbolic_link(log_path: &Path) -> CoreResult<()> {
    match std::fs::symlink_metadata(log_path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err(CoreError::Io(std::io::Error::other(format!(
                "audit log '{}' is a symbolic link",
                log_path.display()
            ))))
        }
        _ => Ok(()),
    }
}

fn map_serialization_error(error: serde_json::Error) -> CoreError {
    CoreError::Io(std::io::Error::other(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::audit_record::AuditOutcome;

    fn sample_record(outcome: AuditOutcome) -> AuditRecord {
        AuditRecord::new("dev", "topics list", "", outcome)
    }

    #[test]
    fn appends_one_json_line_per_record() {
        let directory = tempfile::tempdir().unwrap();
        let writer = JsonLinesAuditWriter::create_in_directory(directory.path());
        writer
            .append_record(&sample_record(AuditOutcome::Executed))
            .unwrap();
        writer
            .append_record(&sample_record(AuditOutcome::Failed))
            .unwrap();

        let content = std::fs::read_to_string(directory.path().join(AUDIT_LOG_FILE_NAME)).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        for line in lines {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(value.get("outcome").is_some());
            assert!(value.get("timestamp").is_some());
        }
    }

    #[cfg(unix)]
    #[test]
    fn creates_log_file_and_directory_private_to_owner() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let state_directory = root.path().join("kafcli");
        let writer = JsonLinesAuditWriter::create_in_directory(&state_directory);

        writer
            .append_record(&sample_record(AuditOutcome::Executed))
            .unwrap();

        let directory_mode = std::fs::metadata(&state_directory)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        let file_mode = std::fs::metadata(state_directory.join(AUDIT_LOG_FILE_NAME))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(directory_mode, 0o700);
        assert_eq!(file_mode, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn refuses_symlinked_audit_log() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("elsewhere.log");
        std::fs::write(&target, "original\n").unwrap();
        let state_directory = root.path().join("kafcli");
        std::fs::create_dir(&state_directory).unwrap();
        std::os::unix::fs::symlink(&target, state_directory.join(AUDIT_LOG_FILE_NAME)).unwrap();
        let writer = JsonLinesAuditWriter::create_in_directory(&state_directory);

        let error = writer
            .append_record(&sample_record(AuditOutcome::Executed))
            .unwrap_err()
            .to_string();

        assert!(error.contains("is a symbolic link"), "error: {error}");
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "original\n");
    }

    #[cfg(unix)]
    #[test]
    fn tightens_existing_audit_log_mode() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let state_directory = root.path().join("kafcli");
        std::fs::create_dir(&state_directory).unwrap();
        std::fs::set_permissions(&state_directory, std::fs::Permissions::from_mode(0o755)).unwrap();
        let log_path = state_directory.join(AUDIT_LOG_FILE_NAME);
        std::fs::write(&log_path, "old\n").unwrap();
        std::fs::set_permissions(&log_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let writer = JsonLinesAuditWriter::create_in_directory(&state_directory);

        writer
            .append_record(&sample_record(AuditOutcome::Executed))
            .unwrap();

        let mode =
            |path: &std::path::Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&log_path), 0o600);
        // The state directory may be one the user shares with other tools: it is never changed.
        assert_eq!(mode(&state_directory), 0o755);
        assert!(
            std::fs::read_to_string(&log_path)
                .unwrap()
                .starts_with("old\n")
        );
    }

    #[test]
    fn creates_state_directory_when_missing() {
        let root = tempfile::tempdir().unwrap();
        let state_directory = root.path().join("nested").join("kafcli");
        let writer = JsonLinesAuditWriter::create_in_directory(&state_directory);
        writer
            .append_record(&sample_record(AuditOutcome::DryRun))
            .unwrap();
        assert!(state_directory.join(AUDIT_LOG_FILE_NAME).is_file());
    }
}
