use crate::cli::lag_arguments::{LagArguments, LagSortOrder};
use crate::execution_context::ExecutionContext;
use crate::output::collection_warnings::write_collection_warnings;
use crate::output::lag_views::{LagView, render_watch_frame};
use crate::output::output_format::OutputFormat;
use crate::output::production_warning::warn_if_production;
use crate::output::renderer::{RenderedOutput, render_output};
use kafcli_core::config::loader::load_configuration;
use kafcli_core::config::overrides::{ContextOverrides, apply_context_overrides};
use kafcli_core::config::selection::select_context;
use kafcli_core::connection::client_config_builder::ConnectionSettings;
use kafcli_core::error::CoreError;
use kafcli_core::ops::groups::group_lag::{GroupLagReport, LagRequest, collect_group_lag};
use kafcli_core::ops::groups::kafka_cluster_reader::KafkaClusterReader;
use kafcli_core::ops::groups::lag_trend::compare_lag_samples;
use kafcli_core::secret::resolver::SecretResolver;
use std::io::{self, ErrorKind, IsTerminal, Write};
use std::time::{Duration, Instant};

/// Shows the consumer lag of one group or of every group.
///
/// Reads only: no guard, no audit record, no commit. With `--max-lag`, a group above the limit
/// prints the full report and then fails with `LimitExceeded`, so the process exits with code 2.
pub fn lag(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    arguments: &LagArguments,
) -> anyhow::Result<RenderedOutput> {
    let reader = connect_cluster_reader(execution, resolver, arguments.timeout_seconds)?;
    let request = LagRequest {
        group: arguments.group.clone(),
        topic: arguments.topic.clone(),
    };
    if let Some(interval) = arguments.watch {
        return watch_lag(
            &reader,
            &request,
            arguments,
            interval,
            execution.global.output,
        );
    }
    let mut report = collect_group_lag(&reader, &request)?;
    order_groups(&mut report, arguments.sort, arguments.top);
    let trailing_error = arguments
        .max_lag
        .and_then(|maximum| check_max_lag(&report, maximum))
        .map(anyhow::Error::new);
    let output_format = execution.global.output;
    write_collection_warnings(&report.warnings, output_format);
    let view = LagView {
        groups: report.groups,
        warnings: report.warnings,
        changes: None,
    };
    Ok(RenderedOutput {
        text: Some(render_output(&view, output_format)?),
        trailing_error,
    })
}

/// Samples the lag every `interval` until stopped, keeping only the previous sample.
///
/// Each frame goes to stdout as soon as it is rendered. The first sample above `--max-lag` ends the
/// run with `LimitExceeded` after its frame; a closed stdout pipe ends it as a success.
fn watch_lag(
    reader: &KafkaClusterReader,
    request: &LagRequest,
    arguments: &LagArguments,
    interval: Duration,
    output_format: OutputFormat,
) -> anyhow::Result<RenderedOutput> {
    let is_terminal = io::stdout().is_terminal();
    let mut previous: Option<(GroupLagReport, Instant)> = None;
    loop {
        let sampled_at = Instant::now();
        let mut report = collect_group_lag(reader, request)?;
        order_groups(&mut report, arguments.sort, arguments.top);
        let changes = previous
            .as_ref()
            .map(|(before, before_at)| {
                compare_lag_samples(before, &report, sampled_at.duration_since(*before_at))
            })
            .unwrap_or_default();
        let limit_error = arguments
            .max_lag
            .and_then(|maximum| check_max_lag(&report, maximum));
        write_collection_warnings(&report.warnings, output_format);
        let view = LagView {
            groups: report.groups,
            warnings: report.warnings,
            changes: Some(changes),
        };
        let frame = render_watch_frame(&view, output_format, is_terminal)?;
        match write_frame(&frame) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::BrokenPipe => {
                return Ok(RenderedOutput::streamed());
            }
            Err(error) => return Err(error.into()),
        }
        if let Some(error) = limit_error {
            return Ok(RenderedOutput {
                text: None,
                trailing_error: Some(anyhow::Error::new(error)),
            });
        }
        previous = Some((
            GroupLagReport {
                groups: view.groups,
                warnings: view.warnings,
            },
            sampled_at,
        ));
        wait_for_next_sample(interval);
    }
}

/// The pause between `--watch` samples is what the user asked for, not a wait on a client, so no
/// client timeout can replace it (ADR 0006).
#[allow(
    clippy::disallowed_methods,
    reason = "the --watch interval is a user-chosen pause between samples, see ADR 0006"
)]
fn wait_for_next_sample(interval: Duration) {
    std::thread::sleep(interval);
}

fn write_frame(frame: &str) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{frame}")?;
    stdout.flush()
}

/// Selects the context, warns when it is protected and opens a read-only cluster reader.
pub fn connect_cluster_reader(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    timeout_seconds: u64,
) -> anyhow::Result<KafkaClusterReader> {
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
    Ok(KafkaClusterReader::connect(
        &connection_context,
        resolver,
        &settings,
        Duration::from_secs(timeout_seconds),
    )?)
}

/// Orders the groups as `--sort` says and keeps the first `--top`.
pub fn order_groups(report: &mut GroupLagReport, sort: LagSortOrder, top: Option<u32>) {
    match sort {
        LagSortOrder::Lag => report.groups.sort_by(|left, right| {
            right
                .total_lag
                .cmp(&left.total_lag)
                .then_with(|| left.group.cmp(&right.group))
        }),
        LagSortOrder::Group => report
            .groups
            .sort_by(|left, right| left.group.cmp(&right.group)),
    }
    if let Some(top) = top {
        report.groups.truncate(top as usize);
    }
}

/// The `--max-lag` error for the first group, in report order, whose total lag is above `maximum`.
pub fn check_max_lag(report: &GroupLagReport, maximum: u64) -> Option<CoreError> {
    report
        .groups
        .iter()
        .find(|group| u64::try_from(group.total_lag).is_ok_and(|total| total > maximum))
        .map(|group| {
            CoreError::LimitExceeded(format!(
                "group '{}' lag {} exceeds --max-lag {maximum}",
                group.group, group.total_lag
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafcli_core::ops::groups::group_lag::GroupLag;

    fn group(name: &str, total_lag: i64) -> GroupLag {
        GroupLag {
            group: name.to_owned(),
            state: "Stable".to_owned(),
            has_active_members: true,
            total_lag,
            topics: Vec::new(),
        }
    }

    fn report(groups: Vec<GroupLag>) -> GroupLagReport {
        GroupLagReport {
            groups,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn max_lag_error_message() {
        let report = report(vec![group("a", 5), group("b", 1500), group("c", 2000)]);

        let error = check_max_lag(&report, 1000).unwrap();

        assert!(matches!(error, CoreError::LimitExceeded(_)));
        assert_eq!(
            error.to_string(),
            "limit exceeded: group 'b' lag 1500 exceeds --max-lag 1000"
        );
    }

    #[test]
    fn max_lag_at_the_limit_passes() {
        let report = report(vec![group("a", 1000)]);

        assert!(check_max_lag(&report, 1000).is_none());
    }

    #[test]
    fn sorts_by_lag_descending_then_name_and_keeps_top() {
        let mut report = report(vec![
            group("c", 5),
            group("a", 9),
            group("b", 9),
            group("d", 1),
        ]);

        order_groups(&mut report, LagSortOrder::Lag, Some(3));

        let names: Vec<&str> = report
            .groups
            .iter()
            .map(|group| group.group.as_str())
            .collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn sorts_by_group_name() {
        let mut report = report(vec![group("c", 5), group("a", 1), group("b", 9)]);

        order_groups(&mut report, LagSortOrder::Group, None);

        let names: Vec<&str> = report
            .groups
            .iter()
            .map(|group| group.group.as_str())
            .collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }
}
