use crate::cli::offsets_arguments::ShowOffsetsArguments;
use crate::commands::cluster_reader_connection::connect_cluster_reader;
use crate::execution_context::ExecutionContext;
use crate::output::collection_warnings::write_collection_warnings;
use crate::output::offsets_views::OffsetsView;
use crate::output::renderer::{RenderedOutput, render_output};
use kafcli_core::config::loader::load_configuration;
use kafcli_core::error::CoreError;
use kafcli_core::ops::groups::group_lag::{LagRequest, collect_group_lag};
use kafcli_core::secret::resolver::SecretResolver;

/// Shows the committed offsets of `--group` next to the log start and end of each partition.
/// Reads only: no guard, no audit record, no commit.
pub fn show_offsets(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    arguments: &ShowOffsetsArguments,
) -> anyhow::Result<RenderedOutput> {
    let configuration = load_configuration(&execution.config_path)?;
    let reader = connect_cluster_reader(
        execution,
        &configuration,
        resolver,
        arguments.timeout_seconds,
    )?;
    let request = LagRequest {
        group: Some(arguments.group.clone()),
        topic: arguments.topic.clone(),
    };
    let report = collect_group_lag(&reader, &request)?;
    let group = report
        .groups
        .into_iter()
        .next()
        .ok_or_else(|| CoreError::GroupNotFound(arguments.group.clone()))?;
    let output_format = execution.global.output;
    write_collection_warnings(&report.warnings, output_format);
    let view = OffsetsView::from_group_lag(group, report.warnings);
    Ok(RenderedOutput::complete(render_output(
        &view,
        output_format,
    )?))
}
