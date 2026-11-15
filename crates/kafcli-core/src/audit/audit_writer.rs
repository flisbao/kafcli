use super::audit_record::AuditRecord;
use crate::error::{CoreError, CoreResult};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

pub const AUDIT_LOG_FILE_NAME: &str = "audit.log";

/// Appends audit records somewhere durable. The binary decides the destination.
pub trait AuditWriter {
    fn append_record(&self, record: &AuditRecord) -> CoreResult<()>;
}

/// Writes one JSON object per line to `audit.log` inside the state directory.
pub struct JsonLinesAuditWriter {
    log_path: PathBuf,
}

impl JsonLinesAuditWriter {
    pub fn in_directory(state_directory: &Path) -> Self {
        Self {
            log_path: state_directory.join(AUDIT_LOG_FILE_NAME),
        }
    }
}

impl AuditWriter for JsonLinesAuditWriter {
    /// Creates the directory if needed, then appends the record as one line in a single write.
    fn append_record(&self, record: &AuditRecord) -> CoreResult<()> {
        if let Some(directory) = self.log_path.parent() {
            std::fs::create_dir_all(directory)?;
        }
        let mut line = serde_json::to_string(record).map_err(serialization_error)?;
        line.push('\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        file.write_all(line.as_bytes())?;
        Ok(())
    }
}

fn serialization_error(error: serde_json::Error) -> CoreError {
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
        let writer = JsonLinesAuditWriter::in_directory(directory.path());
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

    #[test]
    fn creates_state_directory_when_missing() {
        let root = tempfile::tempdir().unwrap();
        let state_directory = root.path().join("nested").join("kafcli");
        let writer = JsonLinesAuditWriter::in_directory(&state_directory);
        writer
            .append_record(&sample_record(AuditOutcome::DryRun))
            .unwrap();
        assert!(state_directory.join(AUDIT_LOG_FILE_NAME).is_file());
    }
}
