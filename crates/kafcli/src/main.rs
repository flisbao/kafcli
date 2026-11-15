mod cli;
mod commands;
mod execution_context;
mod exit_code;
mod logging;
mod output;
mod prompt;

use clap::Parser;
use cli::arguments::{CommandLineArguments, TopLevelCommand};
use cli::profile_arguments::ProfileCommand;
use commands::profile::{add_profile, list_profiles, show_profile, use_profile};
use execution_context::ExecutionContext;
use exit_code::resolve_exit_code;
use output::renderer::render_output;
use std::io::{self, ErrorKind, Write};
use std::process::ExitCode;

fn main() -> ExitCode {
    let arguments = CommandLineArguments::parse();
    logging::initialize_logging(arguments.global.verbosity);
    match run(arguments) {
        Ok(rendered) => write_result_to_stdout(&rendered),
        Err(error) => {
            eprintln!("error: {error:#}");
            resolve_exit_code(&error)
        }
    }
}

/// A reader that closes the pipe early (for example `kafcli ... | head`) is a normal end, not a failure.
fn write_result_to_stdout(rendered: &str) -> ExitCode {
    match writeln!(io::stdout().lock(), "{rendered}") {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) if error.kind() == ErrorKind::BrokenPipe => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            resolve_exit_code(&anyhow::Error::from(error))
        }
    }
}

fn run(arguments: CommandLineArguments) -> anyhow::Result<String> {
    let output_format = arguments.global.output;
    let context = ExecutionContext::from_global_arguments(arguments.global)?;
    let rendered = match arguments.command {
        TopLevelCommand::Profile { command } => match command {
            ProfileCommand::List => render_output(&list_profiles(&context)?, output_format)?,
            ProfileCommand::Show { name } => {
                render_output(&show_profile(&context, name.as_deref())?, output_format)?
            }
            ProfileCommand::Add(add_arguments) => {
                render_output(&add_profile(&context, &add_arguments)?, output_format)?
            }
            ProfileCommand::Use { name } => {
                render_output(&use_profile(&context, &name)?, output_format)?
            }
            ProfileCommand::Test { .. } => anyhow::bail!("not implemented"),
        },
    };
    Ok(rendered)
}
