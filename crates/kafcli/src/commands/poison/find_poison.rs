use crate::cli::poison_arguments::FindPoisonArguments;
use crate::commands::cluster_reader_connection::connect_cluster_reader;
use crate::commands::registry_connection::{connect_schema_registry, decoder_needs_registry};
use crate::execution_context::ExecutionContext;
use crate::output::collection_warnings::write_collection_warnings;
use crate::output::poison_views::PoisonView;
use crate::output::renderer::{RenderedOutput, render_output};
use kafcli_core::codec::codec_selection::build_value_decoder;
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::selection::select_context;
use kafcli_core::ops::poison::kafka_message_reader::KafkaPartitionMessageReader;
use kafcli_core::ops::poison::poison_finder::{PoisonFindOptions, find_poison_candidates};
use kafcli_core::secret::resolver::SecretResolver;
use std::time::Duration;

/// Shows, for each partition of `--topic` where `--group` has lag, the message at the committed
/// offset and whether it decodes. Reads only: no guard, no audit record, no commit. Exits 0 even
/// when a message does not decode.
pub fn find_poison(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    arguments: &FindPoisonArguments,
) -> anyhow::Result<RenderedOutput> {
    let configuration = load_configuration(&execution.config_path)?;
    let selected = select_context(&configuration, execution.global.context.as_deref())?;
    let timeout = Duration::from_secs(arguments.timeout_seconds);
    let selection = arguments.decoder_selection();
    // The decoder reads the registry before any broker connection, as `consume` does.
    let registry = if decoder_needs_registry(&selection) {
        Some(connect_schema_registry(
            &selected.name,
            &selected.context,
            resolver,
            timeout,
        )?)
    } else {
        None
    };
    let decoder = build_value_decoder(&selection, registry)?;
    let reader = connect_cluster_reader(
        execution,
        &configuration,
        resolver,
        arguments.timeout_seconds,
    )?;
    let messages = KafkaPartitionMessageReader::new(reader.connection().clone(), timeout);
    let options = PoisonFindOptions {
        max_bytes: arguments.max_bytes,
        redact: arguments.redact.clone(),
    };
    let report = find_poison_candidates(
        &reader,
        &messages,
        decoder.as_ref(),
        &arguments.group,
        &arguments.topic,
        &options,
    )?;
    let output_format = execution.global.output;
    write_collection_warnings(&report.warnings, output_format);
    Ok(RenderedOutput::complete(render_output(
        &PoisonView { report },
        output_format,
    )?))
}
