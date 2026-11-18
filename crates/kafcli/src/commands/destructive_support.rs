//! What the destructive commands share in the binary: the audit writer and the backup directory.

use crate::output::terminal_text::sanitize_for_terminal;
use kafcli_core::audit::audit_record::AuditRecord;
use kafcli_core::audit::audit_writer::{AuditWriter, JsonLinesAuditWriter};
use kafcli_core::config::paths::default_state_directory;
use kafcli_core::error::CoreResult;
use kafcli_core::ops::offsets::offset_backup::OFFSET_BACKUP_DIRECTORY;
use std::io::{self, Write};
use std::path::PathBuf;

/// Appends to `audit.log` in the state directory. A failed write becomes a warning on stderr and
/// never fails the command, as in `schema register`.
pub struct WarningAuditWriter;

impl AuditWriter for WarningAuditWriter {
    fn append_record(&self, record: &AuditRecord) -> CoreResult<()> {
        let written = default_state_directory().and_then(|directory| {
            JsonLinesAuditWriter::create_in_directory(&directory).append_record(record)
        });
        if let Err(error) = written {
            let _ = writeln!(
                io::stderr().lock(),
                "{}",
                sanitize_for_terminal(&format!("warning: audit log not written: {error}"))
            );
        }
        Ok(())
    }
}

/// `<state dir>/offsets-backup`, where offset backups are written.
pub fn resolve_backup_directory() -> anyhow::Result<PathBuf> {
    Ok(default_state_directory()?.join(OFFSET_BACKUP_DIRECTORY))
}
