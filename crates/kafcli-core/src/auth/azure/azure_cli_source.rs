use super::default_credential_chain::{CredentialLink, LinkFailure};
use super::token_response::MAX_TOKEN_RESPONSE_BYTES;
use crate::auth::token::access_token::AccessToken;
use crate::auth::token::token_source::TokenSource;
use crate::error::{CoreError, CoreResult};
use crate::registry::registry_text::neutralize_registry_text;
use secrecy::SecretString;
use serde_json::Value;
use std::ffi::OsString;
use std::io::{self, Read};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};

/// How long one `az account get-access-token` may run before it is killed.
const AZURE_CLI_TIMEOUT: Duration = Duration::from_secs(30);
/// Only the start of stderr is kept, and it is shown after neutralizing.
const STDERR_LIMIT_BYTES: usize = 64 * 1024;
const SOURCE_NAME: &str = "azure-cli";
const NOT_FOUND_MESSAGE: &str =
    "azure-cli: 'az' was not found in PATH; install the Azure CLI and run 'az login'";

/// Token of the Azure CLI login (`az login`), read from `az account get-access-token`.
/// stdout carries the token and is never shown; only stderr may appear in an error.
pub struct AzureCliTokenSource {
    program: PathBuf,
    leading_arguments: Vec<OsString>,
    tenant_id: Option<String>,
    scope: String,
    timeout: Duration,
}

impl AzureCliTokenSource {
    pub(crate) fn new(tenant_id: Option<String>, scope: String) -> Self {
        let (program, leading_arguments) = default_program();
        Self {
            program,
            leading_arguments,
            tenant_id,
            scope,
            timeout: AZURE_CLI_TIMEOUT,
        }
    }

    /// Test seam: runs `program` in place of `az`.
    #[cfg(test)]
    pub(crate) fn with_program(
        program: PathBuf,
        tenant_id: Option<String>,
        scope: String,
        timeout: Duration,
    ) -> Self {
        Self {
            program,
            leading_arguments: Vec::new(),
            tenant_id,
            scope,
            timeout,
        }
    }

    /// Test seam: runs a fake `az` script through `/bin/sh`, so the script is only read and never
    /// executed directly (executing a freshly written file can fail with ETXTBSY under parallel tests).
    #[cfg(all(test, unix))]
    pub(crate) fn with_script(
        script: PathBuf,
        tenant_id: Option<String>,
        scope: String,
        timeout: Duration,
    ) -> Self {
        Self {
            program: PathBuf::from("/bin/sh"),
            leading_arguments: vec![script.into_os_string()],
            tenant_id,
            scope,
            timeout,
        }
    }

    fn build_command(&self) -> Command {
        let mut command = Command::new(&self.program);
        command
            .args(&self.leading_arguments)
            .args(["account", "get-access-token", "--scope"])
            .arg(&self.scope)
            .args(["--output", "json"]);
        if let Some(tenant_id) = &self.tenant_id {
            command.args(["--tenant"]).arg(tenant_id);
        }
        #[cfg(windows)]
        command.current_dir(windows_system_directory());
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    /// Refuses values that `az`, or `cmd` on Windows, could read as more than one argument or as
    /// a command. Runs before any spawn, on every platform.
    fn validate_arguments(&self) -> Result<(), LinkFailure> {
        check_argument("scope", &self.scope, is_scope_character)?;
        if let Some(tenant_id) = &self.tenant_id {
            check_argument("tenant_id", tenant_id, is_tenant_character)?;
        }
        Ok(())
    }

    fn run_az(&self) -> Result<AccessToken, LinkFailure> {
        self.validate_arguments()?;
        let mut child = self.build_command().spawn().map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                LinkFailure::Unavailable(NOT_FOUND_MESSAGE.to_owned())
            } else {
                failure("azure-cli: could not start 'az'")
            }
        })?;
        let deadline = Instant::now() + self.timeout;
        let stdout = read_in_background(child.stdout.take(), MAX_TOKEN_RESPONSE_BYTES);
        let stderr = read_in_background(child.stderr.take(), STDERR_LIMIT_BYTES);
        // Both pipes reach end of file when `az` exits, so the deadline is enforced on the pipes:
        // the parent still owns the child and can kill it, without sleeping in a poll loop.
        // Limits of this wait:
        // - a child that closes both pipes and keeps running makes the final `child.wait()`
        //   unbounded;
        // - a grandchild that inherits the pipes keeps them open, so a successful run then reports
        //   a timeout;
        // - kill reaches only the direct child, never its grandchildren.
        let stdout = receive_output_before(&stdout, deadline, &mut child)?;
        let stderr = receive_output_before(&stderr, deadline, &mut child)?;
        let status = child
            .wait()
            .map_err(|_| failure("azure-cli: could not wait for 'az'"))?;
        if !status.success() {
            // A failed run is what a CLI that is not logged in produces, so the chain may skip it.
            return Err(LinkFailure::Unavailable(failed_message(&stderr)));
        }
        parse_token_output(&stdout)
    }
}

#[cfg(windows)]
fn default_program() -> (PathBuf, Vec<OsString>) {
    // `az` is a batch file on Windows, so it runs through the command interpreter.
    (
        PathBuf::from("cmd"),
        vec![OsString::from("/C"), OsString::from("az")],
    )
}

#[cfg(not(windows))]
fn default_program() -> (PathBuf, Vec<OsString>) {
    (PathBuf::from("az"), Vec::new())
}

fn failure(message: &str) -> LinkFailure {
    LinkFailure::Error(CoreError::Authentication(message.to_owned()))
}

/// Scope of an Entra ID resource: letters, digits and `_ - . : /`, and never a leading `-`.
fn is_scope_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.' | ':' | '/')
}

/// Tenant id or domain: letters, digits, `.` and `-`.
fn is_tenant_character(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '.' | '-')
}

fn check_argument(
    name: &str,
    value: &str,
    is_allowed: fn(char) -> bool,
) -> Result<(), LinkFailure> {
    if value.starts_with('-') {
        return Err(failure(&format!(
            "azure-cli: {name} must not start with '-'"
        )));
    }
    if value.is_empty() || !value.chars().all(is_allowed) {
        return Err(failure(&format!(
            "azure-cli: {name} contains characters that are not allowed"
        )));
    }
    Ok(())
}

/// `cmd` resolves `az` from its working directory first, so Windows runs it from the system
/// directory instead of the caller's.
#[cfg(windows)]
fn windows_system_directory() -> PathBuf {
    std::env::var_os("SYSTEMROOT")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(r"C:\Windows"))
}

/// Drains a pipe on its own thread, so a large output cannot block the parent. At most
/// `limit + 1` bytes are read, which lets the caller detect an oversized stdout. The result is
/// sent once the pipe reaches end of file.
fn read_in_background<R>(pipe: Option<R>, limit: usize) -> Receiver<io::Result<Vec<u8>>>
where
    R: Read + Send + 'static,
{
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut buffer = Vec::new();
        let result = match pipe {
            Some(pipe) => pipe
                .take(limit as u64 + 1)
                .read_to_end(&mut buffer)
                .map(|_| buffer),
            None => Ok(buffer),
        };
        let _ = sender.send(result);
    });
    receiver
}

/// Waits for one pipe until the deadline. On timeout the child is killed and reaped.
fn receive_output_before(
    receiver: &Receiver<io::Result<Vec<u8>>>,
    deadline: Instant,
    child: &mut Child,
) -> Result<Vec<u8>, LinkFailure> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    match receiver.recv_timeout(remaining) {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(_)) | Err(RecvTimeoutError::Disconnected) => {
            Err(failure("azure-cli: could not read the output of 'az'"))
        }
        Err(RecvTimeoutError::Timeout) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(failure("azure-cli: az account get-access-token timed out"))
        }
    }
}

fn failed_message(stderr: &[u8]) -> String {
    let text = String::from_utf8_lossy(stderr);
    let text = neutralize_registry_text(text.trim());
    let detail = if text.is_empty() {
        "no error output".to_owned()
    } else {
        text
    };
    format!("azure-cli: az account get-access-token failed: {detail}")
}

/// Reads the token and its expiry from the JSON that `az` prints. Errors never echo the output.
fn parse_token_output(stdout: &[u8]) -> Result<AccessToken, LinkFailure> {
    if stdout.len() > MAX_TOKEN_RESPONSE_BYTES {
        return Err(failure("azure-cli: az output is larger than 1 MiB"));
    }
    let document: Value = serde_json::from_slice(stdout)
        .map_err(|_| failure("azure-cli: az output is not valid JSON"))?;
    let token = document
        .get("accessToken")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| failure("azure-cli: az output has no accessToken"))?;
    let seconds = read_expires_on(&document).ok_or_else(|| {
        failure("azure-cli: 'az' 2.54.0 or newer is required (expires_on missing)")
    })?;
    let expires_at = UNIX_EPOCH
        .checked_add(Duration::from_secs(seconds))
        .ok_or_else(|| failure("azure-cli: expires_on is out of range"))?;
    Ok(AccessToken::new(
        SecretString::from(token.to_owned()),
        expires_at,
    ))
}

/// `expires_on` is seconds since the epoch, printed as a number or as a string of digits.
fn read_expires_on(document: &Value) -> Option<u64> {
    match document.get("expires_on")? {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

impl CredentialLink for AzureCliTokenSource {
    fn fetch_link_token(&self) -> Result<AccessToken, LinkFailure> {
        self.run_az()
    }

    fn source_name(&self) -> &'static str {
        SOURCE_NAME
    }
}

impl TokenSource for AzureCliTokenSource {
    fn fetch_access_token(&self) -> CoreResult<AccessToken> {
        self.run_az().map_err(LinkFailure::into_error)
    }

    fn describe_source(&self) -> &'static str {
        SOURCE_NAME
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use crate::error::CoreError;
    use secrecy::ExposeSecret;
    use std::time::{Duration, Instant, UNIX_EPOCH};

    const SCOPE: &str = "s";

    /// Writes a fake `az` shell script named `fake-az` into `directory`.
    fn write_fake_az(directory: &std::path::Path, body: &str) -> PathBuf {
        let path = directory.join("fake-az");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        path
    }

    fn source_for(
        script: PathBuf,
        tenant_id: Option<&str>,
        timeout: Duration,
    ) -> AzureCliTokenSource {
        AzureCliTokenSource::with_script(
            script,
            tenant_id.map(str::to_string),
            SCOPE.to_string(),
            timeout,
        )
    }

    #[test]
    fn reads_access_token_and_expires_on() {
        let directory = tempfile::tempdir().unwrap();
        let args_file = directory.path().join("args");
        let body = format!(
            "printf '%s\\n' \"$@\" > '{}'\necho '{{\"accessToken\":\"tok\",\"expires_on\":2000000000}}'",
            args_file.display()
        );
        let program = write_fake_az(directory.path(), &body);
        let source = source_for(program, None, Duration::from_secs(30));

        let token = source.fetch_access_token().unwrap();

        assert_eq!(token.value.expose_secret(), "tok");
        assert_eq!(
            token.expires_at,
            UNIX_EPOCH + Duration::from_secs(2_000_000_000)
        );
        let recorded = std::fs::read_to_string(&args_file).unwrap();
        let arguments: Vec<&str> = recorded.lines().collect();
        assert_eq!(
            arguments,
            [
                "account",
                "get-access-token",
                "--scope",
                "s",
                "--output",
                "json"
            ]
        );
        assert_eq!(source.describe_source(), "azure-cli");
    }

    #[test]
    fn tenant_is_passed_as_tenant_argument() {
        let directory = tempfile::tempdir().unwrap();
        let args_file = directory.path().join("args");
        let body = format!(
            "printf '%s\\n' \"$@\" > '{}'\necho '{{\"accessToken\":\"tok\",\"expires_on\":2000000000}}'",
            args_file.display()
        );
        let program = write_fake_az(directory.path(), &body);
        let source = source_for(program, Some("tenant-1"), Duration::from_secs(30));

        source.fetch_access_token().unwrap();

        let recorded = std::fs::read_to_string(&args_file).unwrap();
        assert!(
            recorded
                .lines()
                .collect::<Vec<_>>()
                .ends_with(&["--tenant", "tenant-1"])
        );
    }

    #[test]
    fn old_az_without_expires_on_is_error() {
        let directory = tempfile::tempdir().unwrap();
        let program = write_fake_az(directory.path(), "echo '{\"accessToken\":\"tok\"}'");
        let source = source_for(program, None, Duration::from_secs(30));

        let error = source.fetch_access_token().unwrap_err();

        assert_eq!(
            format!("{error}"),
            "authentication error: azure-cli: 'az' 2.54.0 or newer is required (expires_on missing)"
        );
    }

    #[test]
    fn missing_program_has_install_hint() {
        let source = AzureCliTokenSource::with_program(
            PathBuf::from("/nonexistent/az"),
            None,
            SCOPE.to_string(),
            Duration::from_secs(30),
        );

        let error = source.fetch_access_token().unwrap_err();

        assert_eq!(
            format!("{error}"),
            "authentication error: azure-cli: 'az' was not found in PATH; install the Azure CLI and run 'az login'"
        );
    }

    #[test]
    fn nonzero_exit_reports_stderr_not_stdout() {
        let directory = tempfile::tempdir().unwrap();
        let body = "echo '{\"accessToken\":\"stdout-token-value\",\"expires_on\":2000000000}'\necho \"ERROR: Please run 'az login'\" >&2\nexit 1";
        let program = write_fake_az(directory.path(), body);
        let source = source_for(program, None, Duration::from_secs(30));

        let error = source.fetch_access_token().unwrap_err();

        let message = format!("{error}");
        assert!(message.contains("az login"), "message: {message}");
        assert!(
            !message.contains("stdout-token-value"),
            "message: {message}"
        );
    }

    #[test]
    fn hanging_az_is_killed_after_timeout() {
        let directory = tempfile::tempdir().unwrap();
        let program = write_fake_az(directory.path(), "exec sleep 30");
        let source = source_for(program, None, Duration::from_secs(1));

        let started = Instant::now();
        let error = source.fetch_access_token().unwrap_err();

        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(matches!(error, CoreError::Authentication(_)));
    }

    #[test]
    fn scope_with_shell_metacharacter_is_refused_before_spawn() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("spawned");
        let program = write_fake_az(directory.path(), &format!("touch '{}'", marker.display()));
        let source = AzureCliTokenSource::with_script(
            program,
            None,
            "https://ns.servicebus.windows.net/.default&calc".to_string(),
            Duration::from_secs(30),
        );

        let error = source.fetch_access_token().unwrap_err();

        assert_eq!(
            format!("{error}"),
            "authentication error: azure-cli: scope contains characters that are not allowed"
        );
        assert!(!marker.exists(), "az must not be spawned");
    }

    #[test]
    fn scope_starting_with_dash_is_refused_before_spawn() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("spawned");
        let program = write_fake_az(directory.path(), &format!("touch '{}'", marker.display()));
        let source = AzureCliTokenSource::with_script(
            program,
            None,
            "--output=none".to_string(),
            Duration::from_secs(30),
        );

        let error = source.fetch_access_token().unwrap_err();

        assert_eq!(
            format!("{error}"),
            "authentication error: azure-cli: scope must not start with '-'"
        );
        assert!(!marker.exists(), "az must not be spawned");
    }

    #[test]
    fn tenant_with_metacharacter_is_refused_before_spawn() {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("spawned");
        let program = write_fake_az(directory.path(), &format!("touch '{}'", marker.display()));
        let source = AzureCliTokenSource::with_script(
            program,
            Some("tenant&calc".to_string()),
            "s".to_string(),
            Duration::from_secs(30),
        );

        let error = source.fetch_access_token().unwrap_err();

        assert_eq!(
            format!("{error}"),
            "authentication error: azure-cli: tenant_id contains characters that are not allowed"
        );
        assert!(!marker.exists(), "az must not be spawned");
    }
}
