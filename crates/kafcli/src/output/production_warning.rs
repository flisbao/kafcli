use crate::output::terminal_text::sanitize_for_terminal;
use kafcli_core::config::model::ConnectionContext;
use std::io::{self, IsTerminal, Write};

const WARNING_STYLE_START: &str = "\x1b[1;33m";
const STYLE_RESET: &str = "\x1b[0m";

/// The text is built here once so that every command warns with the same words.
pub fn format_production_warning(context_name: &str, use_color: bool) -> String {
    let text = format!(
        "WARNING: context '{}' is PRODUCTION",
        sanitize_for_terminal(context_name)
    );
    if use_color {
        format!("{WARNING_STYLE_START}{text}{STYLE_RESET}")
    } else {
        text
    }
}

/// Writes one warning line to stderr when the context is protected. Commands call it right after
/// selecting the context, so the warning is printed before any connection attempt.
/// Color is used only on a terminal and when `NO_COLOR` is unset or empty.
pub fn warn_if_production(context_name: &str, context: &ConnectionContext) {
    if !context.protected {
        return;
    }
    let use_color = io::stderr().is_terminal() && is_color_allowed_by_environment();
    let line = format_production_warning(context_name, use_color);
    let _ = writeln!(io::stderr().lock(), "{line}");
}

fn is_color_allowed_by_environment() -> bool {
    std::env::var_os("NO_COLOR").is_none_or(|value| value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_warning_has_exact_text() {
        assert_eq!(
            format_production_warning("prd-eh", false),
            "WARNING: context 'prd-eh' is PRODUCTION"
        );
    }

    #[test]
    fn colored_warning_wraps_text_in_bold_yellow() {
        let warning = format_production_warning("prd-eh", true);

        assert!(warning.starts_with("\x1b[1;33m"));
        assert!(warning.ends_with("\x1b[0m"));
        assert!(warning.contains("WARNING: context 'prd-eh' is PRODUCTION"));
    }

    #[test]
    fn warning_neutralizes_control_characters_in_name() {
        let warning = format_production_warning("\x1b[2Jprd", false);

        assert!(warning.contains("?[2Jprd"));
        assert!(!warning.contains('\x1b'));
    }
}
