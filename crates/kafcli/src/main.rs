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
use exit_code::exit_code_for_error;
use output::renderer::render_output;
use std::process::ExitCode;

fn main() -> ExitCode {
    let arguments = CommandLineArguments::parse();
    logging::initialize_logging(arguments.global.verbosity);
    match run(arguments) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            exit_code_for_error(&error)
        }
    }
}

fn run(arguments: CommandLineArguments) -> anyhow::Result<()> {
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
    println!("{rendered}");
    Ok(())
}
