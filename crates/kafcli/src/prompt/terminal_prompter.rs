use crate::output::terminal_text::sanitize_for_terminal;
use kafcli_core::error::CoreResult;
use kafcli_core::guard::confirmation_prompter::ConfirmationPrompter;
use std::io::{self, BufRead, IsTerminal, Read, Write};

/// Longest answer read from stdin; longer input is cut off so a stray pipe cannot exhaust memory.
const MAXIMUM_CONFIRMATION_INPUT_BYTES: u64 = 1024;

/// Asks questions on stderr and reads answers from stdin.
#[allow(
    dead_code,
    reason = "the prompter is wired into the guarded commands of a later plan"
)]
pub struct TerminalPrompter;

impl ConfirmationPrompter for TerminalPrompter {
    fn is_interactive(&self) -> bool {
        decide_interactivity(io::stdin().is_terminal(), io::stderr().is_terminal())
    }

    /// The guard builds the question text (including `[y/N]` or the typed-value instruction),
    /// so the prompter prints it as-is and does not add its own.
    fn confirm_yes_no(&self, message: &str) -> CoreResult<bool> {
        let answer = ask(message)?;
        Ok(matches!(answer.to_ascii_lowercase().as_str(), "y" | "yes"))
    }

    fn confirm_typed_value(&self, message: &str, expected_value: &str) -> CoreResult<bool> {
        let answer = ask(message)?;
        Ok(answer == expected_value)
    }
}

/// Prompts need both an interactive stdin (answers) and stderr (questions).
#[allow(
    dead_code,
    reason = "only reachable through TerminalPrompter, which a later plan wires in"
)]
fn decide_interactivity(stdin_is_terminal: bool, stderr_is_terminal: bool) -> bool {
    stdin_is_terminal && stderr_is_terminal
}

/// The question as shown: the guard's text, sanitized (it can contain broker-controlled names),
/// followed by a space before the cursor.
#[allow(
    dead_code,
    reason = "only reachable through TerminalPrompter, which a later plan wires in"
)]
fn format_prompt_line(question: &str) -> String {
    sanitize_for_terminal(&format!("{question} "))
}

/// Writes `question` to stderr and reads one line, without its line break.
#[allow(
    dead_code,
    reason = "only reachable through TerminalPrompter, which a later plan wires in"
)]
fn ask(question: &str) -> CoreResult<String> {
    let mut stderr = io::stderr();
    stderr.write_all(format_prompt_line(question).as_bytes())?;
    stderr.flush()?;

    let mut limited_stdin = io::stdin().lock().take(MAXIMUM_CONFIRMATION_INPUT_BYTES);
    let mut line = String::new();
    limited_stdin.read_line(&mut line)?;
    Ok(line.trim_end_matches(['\r', '\n']).trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_non_interactive_when_stdin_is_not_terminal() {
        assert!(!decide_interactivity(false, true));
        assert!(!decide_interactivity(false, false));
    }

    #[test]
    fn prompt_line_is_printed_once_and_sanitized() {
        let line = format_prompt_line("Type 'orders\x1b[2J' to confirm: reset offsets [y/N]");

        assert_eq!(line, "Type 'orders?[2J' to confirm: reset offsets [y/N] ");
        assert_eq!(line.matches("to confirm").count(), 1);
    }

    #[test]
    fn reports_interactive_only_when_both_streams_are_terminals() {
        assert!(decide_interactivity(true, true));
        assert!(!decide_interactivity(true, false));
    }
}
