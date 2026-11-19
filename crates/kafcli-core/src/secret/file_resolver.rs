use super::reference::SecretReference;
use super::resolver::SecretResolver;
use crate::error::{CoreError, CoreResult};
use secrecy::SecretString;
use std::fs::File;
use std::path::Path;

/// Largest secret file accepted; anything bigger is refused before it is read in full.
pub const MAXIMUM_SECRET_FILE_BYTES: u64 = 64 * 1024;

/// Resolves `file:/path` references, dropping one trailing newline.
pub struct FileSecretResolver;

impl SecretResolver for FileSecretResolver {
    fn resolve_secret(&self, reference: &SecretReference) -> CoreResult<SecretString> {
        let SecretReference::File { path } = reference else {
            return Err(CoreError::SecretResolution(
                "file resolver only handles file: references".to_string(),
            ));
        };
        let contents = read_bounded_secret_file(path)?;
        let value = strip_single_trailing_newline(&contents);
        if value.is_empty() {
            return Err(CoreError::SecretResolution(format!(
                "secret file '{}' is empty",
                path.display()
            )));
        }
        Ok(SecretString::from(value.to_string()))
    }
}

fn read_bounded_secret_file(path: &Path) -> CoreResult<String> {
    let unreadable = |error: std::io::Error| {
        CoreError::SecretResolution(format!(
            "cannot read secret file '{}': {error}",
            path.display()
        ))
    };
    let too_large = || {
        CoreError::SecretResolution(format!(
            "secret file '{}' exceeds {MAXIMUM_SECRET_FILE_BYTES} bytes",
            path.display()
        ))
    };

    // Checked before opening: opening a named pipe blocks until a writer appears.
    let path_metadata = std::fs::metadata(path).map_err(unreadable)?;
    if !path_metadata.is_file() {
        return Err(CoreError::SecretResolution(format!(
            "secret file '{}' is not a regular file",
            path.display()
        )));
    }

    let file = File::open(path).map_err(unreadable)?;
    let declared_length = file.metadata().map_err(unreadable)?.len();
    if declared_length > MAXIMUM_SECRET_FILE_BYTES {
        return Err(too_large());
    }

    let bytes = crate::io_limits::read_at_most(file, MAXIMUM_SECRET_FILE_BYTES)
        .map_err(unreadable)?
        .ok_or_else(too_large)?;
    String::from_utf8(bytes).map_err(|_| {
        CoreError::SecretResolution(format!(
            "secret file '{}' is not valid UTF-8",
            path.display()
        ))
    })
}

fn strip_single_trailing_newline(contents: &str) -> &str {
    contents
        .strip_suffix("\r\n")
        .or_else(|| contents.strip_suffix('\n'))
        .unwrap_or(contents)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::reference::SecretReference;
    use crate::secret::resolver::SecretResolver;
    use secrecy::ExposeSecret;
    use std::io::Write;

    fn reference_to_file_with_contents(
        contents: &[u8],
    ) -> (tempfile::NamedTempFile, SecretReference) {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(contents).unwrap();
        let reference = SecretReference::File {
            path: file.path().to_path_buf(),
        };
        (file, reference)
    }

    #[test]
    fn trims_trailing_newline() {
        let (_file, reference) = reference_to_file_with_contents(b"s3cret\n");
        let secret = FileSecretResolver.resolve_secret(&reference).unwrap();
        assert_eq!(secret.expose_secret(), "s3cret");
    }
    #[test]
    fn trims_trailing_crlf() {
        let (_file, reference) = reference_to_file_with_contents(b"s3cret\r\n");
        let secret = FileSecretResolver.resolve_secret(&reference).unwrap();
        assert_eq!(secret.expose_secret(), "s3cret");
    }
    #[test]
    fn rejects_file_larger_than_limit() {
        let contents = vec![b'a'; usize::try_from(MAXIMUM_SECRET_FILE_BYTES + 1).unwrap()];
        let (_file, reference) = reference_to_file_with_contents(&contents);
        assert!(FileSecretResolver.resolve_secret(&reference).is_err());
    }
    #[test]
    fn reports_missing_file_by_path() {
        let reference = SecretReference::File {
            path: "/nonexistent/kafcli-secret".into(),
        };
        let error = FileSecretResolver
            .resolve_secret(&reference)
            .unwrap_err()
            .to_string();
        assert!(error.contains("/nonexistent/kafcli-secret"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_named_pipe_without_blocking() {
        let directory = tempfile::tempdir().unwrap();
        let pipe_path = directory.path().join("secret-pipe");
        let status = std::process::Command::new("mkfifo")
            .arg(&pipe_path)
            .status()
            .unwrap();
        assert!(status.success());
        let reference = SecretReference::File {
            path: pipe_path.clone(),
        };

        let error = FileSecretResolver
            .resolve_secret(&reference)
            .unwrap_err()
            .to_string();

        assert_eq!(
            error,
            format!(
                "secret could not be resolved: secret file '{}' is not a regular file",
                pipe_path.display()
            )
        );
    }

    #[test]
    fn rejects_non_file_reference() {
        let reference = SecretReference::Environment {
            variable: "X".into(),
        };
        assert!(FileSecretResolver.resolve_secret(&reference).is_err());
    }
}
