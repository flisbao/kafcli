use crate::cli::consume_arguments::{ConsumeArguments, ConsumeFormat, ConsumeWire};
use crate::execution_context::ExecutionContext;
use crate::output::envelope_writer::{NdjsonEnvelopeWriter, TableEnvelopeWriter};
use crate::output::output_format::OutputFormat;
use crate::output::production_warning::warn_if_production;
use crate::output::renderer::RenderedOutput;
use kafcli_core::codec::{RawJsonCodec, ValueDecoder};
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::overrides::{ContextOverrides, apply_context_overrides};
use kafcli_core::config::selection::select_context;
use kafcli_core::connection::client_config_builder::ConnectionSettings;
use kafcli_core::message::message_envelope::MessageEnvelope;
use kafcli_core::ops::consume::message_consumer::{
    ConsumeOptions, ConsumeSummary, EnvelopeSink, consume_messages,
};
use kafcli_core::secret::resolver::SecretResolver;
use std::io::{self, ErrorKind, Write};
use std::time::Duration;

/// Reads messages from `--topic` of the selected context and writes each envelope to stdout as it arrives.
///
/// Order: select the context and warn if it is protected; stream the envelopes. Consume only reads, so
/// it asks no guard and writes no audit record. On success it writes nothing more to stdout; outside
/// JSON output it reports the message count on stderr. A reader that closes the pipe early ends the run
/// as a success without that count.
pub fn consume(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    arguments: &ConsumeArguments,
) -> anyhow::Result<RenderedOutput> {
    let configuration = load_configuration(&execution.config_path)?;
    let selected = select_context(&configuration, execution.global.context.as_deref())?;
    warn_if_production(&selected.name, &selected.context);

    let overrides = ContextOverrides {
        brokers: execution.global.brokers.clone(),
    };
    let connection_context = apply_context_overrides(selected.context, &overrides);
    let settings = ConnectionSettings {
        insecure: execution.global.insecure,
    };
    let options = build_options(arguments);
    let decoder = select_value_decoder(arguments.format, arguments.wire);

    let (summary, pipe_closed) = match execution.global.output {
        OutputFormat::Json => stream_envelopes(
            &connection_context,
            resolver,
            &settings,
            &options,
            decoder,
            &mut NdjsonEnvelopeWriter::new(io::stdout().lock()),
        )?,
        OutputFormat::Table => stream_envelopes(
            &connection_context,
            resolver,
            &settings,
            &options,
            decoder,
            &mut TableEnvelopeWriter::new(io::stdout().lock()),
        )?,
    };

    if !pipe_closed && execution.global.output != OutputFormat::Json {
        let _ = writeln!(
            io::stderr().lock(),
            "consumed {} messages",
            summary.delivered
        );
    }
    Ok(RenderedOutput::streamed())
}

/// Runs the consume operation into `sink`. Returns the summary and whether the reader closed the pipe.
fn stream_envelopes(
    connection_context: &kafcli_core::config::model::ConnectionContext,
    resolver: &dyn SecretResolver,
    settings: &ConnectionSettings,
    options: &ConsumeOptions,
    decoder: &dyn ValueDecoder,
    sink: &mut dyn EnvelopeSink,
) -> anyhow::Result<(ConsumeSummary, bool)> {
    let mut tracking = PipeTrackingSink {
        inner: sink,
        pipe_closed: false,
    };
    let summary = consume_messages(
        connection_context,
        resolver,
        settings,
        options,
        decoder,
        &mut tracking,
    )?;
    Ok((summary, tracking.pipe_closed))
}

/// Remembers whether the sink reported a closed pipe, so the command can stay silent about it.
struct PipeTrackingSink<'a> {
    inner: &'a mut dyn EnvelopeSink,
    pipe_closed: bool,
}

impl EnvelopeSink for PipeTrackingSink<'_> {
    fn accept_envelope(&mut self, envelope: &MessageEnvelope) -> io::Result<()> {
        let result = self.inner.accept_envelope(envelope);
        if result
            .as_ref()
            .is_err_and(|error| error.kind() == ErrorKind::BrokenPipe)
        {
            self.pipe_closed = true;
        }
        result
    }
}

fn build_options(arguments: &ConsumeArguments) -> ConsumeOptions {
    ConsumeOptions {
        topic: arguments.topic.clone(),
        partitions: (!arguments.partition.is_empty()).then(|| arguments.partition.clone()),
        start: arguments.from,
        count: arguments.count,
        follow: arguments.follow,
        group: arguments.group.clone(),
        max_bytes: arguments.max_bytes,
        redact: arguments.redact.clone(),
        filters: arguments.filters.clone(),
        timeout: Duration::from_secs(arguments.timeout_seconds),
    }
}

/// Only JSON values on the raw wire exist in this release, so the pair selects the one codec.
fn select_value_decoder(format: ConsumeFormat, wire: ConsumeWire) -> &'static dyn ValueDecoder {
    match (format, wire) {
        (ConsumeFormat::Json, ConsumeWire::Raw) => &RawJsonCodec,
    }
}
