use std::io;
use tracing::level_filters::LevelFilter;

/// Starts stderr logging: 0 `-v` flags is warnings, 1 is info, 2 or more is debug.
///
/// Logs never go to stdout, which is reserved for command output.
pub fn initialize_logging(verbosity: u8) {
    let level = match verbosity {
        0 => LevelFilter::WARN,
        1 => LevelFilter::INFO,
        _ => LevelFilter::DEBUG,
    };
    // A second initialization (for example from a test) is harmless, so the error is ignored.
    let _ = tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_max_level(level)
        .without_time()
        .try_init();
}
