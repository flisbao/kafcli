//! Reads that stop one byte past a size limit, so an oversized source is never read whole.

use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

/// Largest PEM file (certificate and key) kafcli reads.
pub const MAX_PEM_FILE_BYTES: u64 = 1024 * 1024;

/// Reads `reader` to the end, or returns `None` once it holds more than `limit` bytes. At most
/// `limit + 1` bytes are read.
pub fn read_at_most<R: Read>(reader: R, limit: u64) -> io::Result<Option<Vec<u8>>> {
    let mut bytes = Vec::new();
    reader
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limit {
        return Ok(None);
    }
    Ok(Some(bytes))
}

/// Reads a file of at most `limit` bytes. The declared length is checked before reading, and the
/// read stops past the limit for a file that grows meanwhile.
pub fn read_file_at_most(path: &Path, limit: u64) -> io::Result<Option<Vec<u8>>> {
    let file = File::open(path)?;
    if file.metadata()?.len() > limit {
        return Ok(None);
    }
    read_at_most(file, limit)
}

/// Narrows the permissions of an open file to `mode` when group or others have any access. A
/// mode that is already private, even narrower than `mode`, is left alone. It works on the open
/// descriptor, so a path swapped after opening is never touched.
#[cfg(unix)]
pub fn tighten_file_mode(file: &File, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let current = file.metadata()?.permissions().mode();
    if current & 0o077 != 0 {
        file.set_permissions(std::fs::Permissions::from_mode(mode))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn read_at_most_accepts_exact_limit() {
        assert_eq!(
            read_at_most(&[7u8; 10][..], 10).unwrap(),
            Some(vec![7u8; 10])
        );
    }

    #[test]
    fn read_at_most_rejects_one_past_limit() {
        assert_eq!(read_at_most(&[7u8; 11][..], 10).unwrap(), None);
    }

    #[test]
    fn read_file_at_most_rejects_large_declared_length() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&[1u8; 32]).unwrap();

        assert_eq!(read_file_at_most(file.path(), 16).unwrap(), None);
        assert_eq!(
            read_file_at_most(file.path(), 32).unwrap(),
            Some(vec![1u8; 32])
        );
    }

    #[cfg(unix)]
    #[test]
    fn tighten_file_mode_only_narrows_open_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let file = tempfile::NamedTempFile::new().unwrap();
        let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o644)).unwrap();

        tighten_file_mode(file.as_file(), 0o600).unwrap();
        assert_eq!(mode(file.path()), 0o600);

        std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o400)).unwrap();
        tighten_file_mode(file.as_file(), 0o600).unwrap();
        assert_eq!(mode(file.path()), 0o400);
    }
}
