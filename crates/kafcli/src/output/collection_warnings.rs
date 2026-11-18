use crate::output::output_format::OutputFormat;
use crate::output::terminal_text::sanitize_for_terminal;
use std::io::{self, Write};

/// Writes the warnings of a read to stderr in table output. JSON output carries them in
/// `warnings` instead, so stderr stays free for errors.
pub fn write_collection_warnings(warnings: &[String], format: OutputFormat) {
    if format != OutputFormat::Table {
        return;
    }
    let mut stderr = io::stderr().lock();
    for warning in warnings {
        let _ = writeln!(stderr, "warning: {}", sanitize_for_terminal(warning));
    }
}
