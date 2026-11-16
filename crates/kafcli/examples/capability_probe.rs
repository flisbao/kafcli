//! Checks which Kafka capabilities a context supports against one topic, or dumps messages from a
//! topic without writing anything.
//!
//! Examples:
//!   cargo run -p kafcli --example capability_probe -- --context dev --topic orders
//!   cargo run -p kafcli --example capability_probe -- --context prd --topic orders --dump 20
//!
//! Exit codes: 0 success, 1 runtime error, 2 protected-context refusal or usage error.

use clap::{Parser, ValueEnum};
use kafcli_core::capability::capability_probe::{ProbeOptions, probe_capabilities};
use kafcli_core::capability::capability_report::{CapabilityOutcome, CapabilityReport};
use kafcli_core::capability::message_dump::{MessageDump, dump_topic_messages};
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::paths::default_config_path;
use kafcli_core::config::selection::select_context;
use kafcli_core::connection::client_config_builder::ConnectionSettings;
use kafcli_core::error::CoreError;
use kafcli_core::secret::composite_resolver::CompositeSecretResolver;
use serde::Serialize;
use std::fmt::{self, Display};
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

const DEFAULT_TIMEOUT_SECONDS: u64 = 15;
const MAXIMUM_TIMEOUT_SECONDS: u64 = 300;
const REFUSED_EXIT_CODE: u8 = 2;
const FAILURE_EXIT_CODE: u8 = 1;
const HEX_VALUE_MARKER: &str = "hex:";
const MISSING_VALUE_MARKER: &str = "-";

#[derive(Parser)]
#[command(
    name = "capability_probe",
    about = "Checks Kafka capabilities of a context against one topic, or dumps the topic's messages"
)]
struct ProbeArguments {
    /// Configuration file. Also read from KAFCLI_CONFIG; defaults to the kafcli configuration file.
    #[arg(long, env = "KAFCLI_CONFIG", value_name = "PATH")]
    config: Option<PathBuf>,
    /// Context to use; defaults to the current context of the configuration file.
    #[arg(long, value_name = "NAME")]
    context: Option<String>,
    /// Topic to probe or dump. The probe never creates or deletes topics.
    #[arg(long, value_name = "TOPIC")]
    topic: String,
    /// Reads up to N messages from the topic and prints them. Writes nothing, so it is allowed on
    /// protected contexts.
    #[arg(long, value_name = "N")]
    dump: Option<usize>,
    /// Time budget in seconds for the probe, or idle time before a dump stops.
    #[arg(
        long,
        value_name = "SECONDS",
        default_value_t = DEFAULT_TIMEOUT_SECONDS,
        value_parser = clap::value_parser!(u64).range(1..=MAXIMUM_TIMEOUT_SECONDS)
    )]
    timeout_seconds: u64,
    /// Output format of the report or of the dumped messages.
    #[arg(long, value_enum, default_value = "table")]
    output: OutputFormat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
}

/// A failed run: a protected-context refusal (exit 2) or a runtime error (exit 1).
enum ProbeError {
    Refused { context_name: String },
    Core(CoreError),
    Output(io::Error),
}

impl ProbeError {
    fn exit_code(&self) -> u8 {
        match self {
            Self::Refused { .. } => REFUSED_EXIT_CODE,
            Self::Core(_) | Self::Output(_) => FAILURE_EXIT_CODE,
        }
    }
}

impl Display for ProbeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { context_name } => write!(
                formatter,
                "capability probe refuses protected context '{context_name}'"
            ),
            Self::Core(error) => write!(formatter, "{error}"),
            Self::Output(error) => write!(formatter, "could not write output: {error}"),
        }
    }
}

impl From<CoreError> for ProbeError {
    fn from(error: CoreError) -> Self {
        Self::Core(error)
    }
}

impl From<io::Error> for ProbeError {
    fn from(error: io::Error) -> Self {
        Self::Output(error)
    }
}

fn main() -> ExitCode {
    let arguments = ProbeArguments::parse();
    match run(&arguments) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let _ = writeln!(
                io::stderr(),
                "error: {}",
                neutralize_control_characters(&error.to_string())
            );
            ExitCode::from(error.exit_code())
        }
    }
}

fn run(arguments: &ProbeArguments) -> Result<(), ProbeError> {
    let config_path = match &arguments.config {
        Some(path) => path.clone(),
        None => default_config_path()?,
    };
    let configuration = load_configuration(&config_path)?;
    let selected = select_context(&configuration, arguments.context.as_deref())?;
    let timeout = Duration::from_secs(arguments.timeout_seconds);
    let settings = ConnectionSettings { insecure: false };

    match arguments.dump {
        Some(maximum_messages) => {
            let resolver = CompositeSecretResolver::with_default_sources();
            let messages = dump_topic_messages(
                &selected.context,
                &resolver,
                &settings,
                &arguments.topic,
                maximum_messages,
                timeout,
            )?;
            write_dump(arguments.output, &messages)
        }
        None => {
            if selected.context.protected {
                return Err(ProbeError::Refused {
                    context_name: selected.name,
                });
            }
            let resolver = CompositeSecretResolver::with_default_sources();
            let report = probe_capabilities(
                &selected.name,
                &selected.context,
                &resolver,
                &settings,
                &ProbeOptions {
                    topic: arguments.topic.clone(),
                    timeout,
                },
            )?;
            write_report(arguments.output, &report)
        }
    }
}

fn write_report(format: OutputFormat, report: &CapabilityReport) -> Result<(), ProbeError> {
    let text = match format {
        OutputFormat::Table => render_report_table(report),
        OutputFormat::Json => to_pretty_json(report)?,
    };
    write_line(&text)?;
    Ok(())
}

fn write_dump(format: OutputFormat, messages: &[MessageDump]) -> Result<(), ProbeError> {
    let text = match format {
        OutputFormat::Table => render_dump_table(messages),
        OutputFormat::Json => to_pretty_json(messages)?,
    };
    write_line(&text)?;
    Ok(())
}

fn to_pretty_json<T: Serialize + ?Sized>(value: &T) -> Result<String, ProbeError> {
    serde_json::to_string_pretty(value).map_err(|error| ProbeError::Output(io::Error::other(error)))
}

fn write_line(text: &str) -> io::Result<()> {
    let mut output = io::stdout().lock();
    writeln!(output, "{text}")
}

fn render_report_table(report: &CapabilityReport) -> String {
    let mut lines = vec![
        format!(
            "context: {}  topic: {}  probe group: {}",
            neutralize_control_characters(&report.context_name),
            neutralize_control_characters(&report.topic),
            neutralize_control_characters(&report.probe_group)
        ),
        String::new(),
        "| capability | outcome | detail | ms |".to_string(),
        "| --- | --- | --- | --- |".to_string(),
    ];
    for check in &report.checks {
        let (outcome, detail) = describe_outcome(&check.outcome);
        lines.push(format!(
            "| {} | {} | {} | {} |",
            snake_case_name(&check.name),
            outcome,
            escape_table_cell(&neutralize_control_characters(&detail)),
            check.elapsed_millis
        ));
    }
    lines.join("\n")
}

fn render_dump_table(messages: &[MessageDump]) -> String {
    if messages.is_empty() {
        return "no messages".to_string();
    }
    let mut lines = Vec::new();
    for message in messages {
        lines.push(format!(
            "p{} o{} ts={} key={} value={} prefix={}",
            message.partition,
            message.offset,
            display_optional(message.timestamp_millis),
            display_optional(message.key_length),
            display_optional(message.value_length),
            message.value_prefix_hex
        ));
        for header in &message.headers {
            let value = match &header.value_utf8 {
                Some(text) => neutralize_control_characters(text),
                None => format!("{HEX_VALUE_MARKER}{}", header.value_hex),
            };
            lines.push(format!(
                "  header {} = {}",
                neutralize_control_characters(&header.name),
                value
            ));
        }
    }
    lines.join("\n")
}

fn describe_outcome(outcome: &CapabilityOutcome) -> (&'static str, String) {
    match outcome {
        CapabilityOutcome::Supported { detail } => {
            ("supported", detail.clone().unwrap_or_default())
        }
        CapabilityOutcome::Unsupported { error_code, detail } => match detail {
            Some(detail) => ("unsupported", format!("{error_code}: {detail}")),
            None => ("unsupported", error_code.clone()),
        },
        CapabilityOutcome::Failed { message } => ("failed", message.clone()),
    }
}

fn snake_case_name<T: Serialize>(name: &T) -> String {
    serde_json::to_value(name)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_default()
}

fn display_optional<T: Display>(value: Option<T>) -> String {
    value.map_or_else(|| MISSING_VALUE_MARKER.to_string(), |text| text.to_string())
}

fn escape_table_cell(text: &str) -> String {
    text.replace('|', "\\|")
}

/// Replaces control characters so that text from the broker, the configuration or message
/// headers cannot move the cursor, change colors or rewrite earlier lines.
fn neutralize_control_characters(text: &str) -> String {
    text.chars()
        .map(|character| {
            if character.is_control() {
                '?'
            } else {
                character
            }
        })
        .collect()
}
