use crate::cli::groups_arguments::AuditGroupsArguments;
use crate::commands::cluster_reader_connection::connect_cluster_reader;
use crate::execution_context::ExecutionContext;
use crate::output::audit_views::AuditView;
use crate::output::collection_warnings::write_collection_warnings;
use crate::output::renderer::{RenderedOutput, render_output};
use kafcli_core::capability::broker_operation::incomplete_group_list_warning;
use kafcli_core::config::loader::load_configuration;
use kafcli_core::error::CoreError;
use kafcli_core::ops::groups::audit_rules::{
    AuditFinding, AuditInput, AuditSeverity, evaluate_group_rules,
};
use kafcli_core::ops::groups::cluster_offsets_reader::ClusterOffsetsReader;
use kafcli_core::ops::groups::group_lag::{GroupLag, LagRequest, collect_group_lag};
use kafcli_core::ops::groups::group_snapshot::GroupDescription;
use kafcli_core::ops::groups::service_identification::ServicePattern;
use kafcli_core::secret::resolver::SecretResolver;
use regex::Regex;
use std::collections::BTreeMap;

/// Checks consumer groups against rules R1 to R5.
///
/// Reads only: no guard, no audit record, no commit. The lag is read only for groups without
/// members, which is what R3 needs. Any ERROR finding prints the report and then fails with
/// `AuditFailed`, so the process exits with code 1.
pub fn audit_groups(
    execution: &ExecutionContext,
    resolver: &dyn SecretResolver,
    arguments: &AuditGroupsArguments,
) -> anyhow::Result<RenderedOutput> {
    let configuration = load_configuration(&execution.config_path)?;
    let file_settings = configuration.groups_audit.clone().unwrap_or_default();
    let service_pattern = arguments
        .service_pattern
        .as_deref()
        .or(file_settings.service_pattern.as_deref())
        .map(ServicePattern::compile)
        .transpose()?;
    let group_name_pattern = arguments
        .group_name_pattern
        .as_deref()
        .or(file_settings.group_name_pattern.as_deref())
        .map(compile_group_name_pattern)
        .transpose()?;
    let reader = connect_cluster_reader(
        execution,
        &configuration,
        resolver,
        arguments.timeout_seconds,
    )?;

    let mut warnings = Vec::new();
    let groups = read_audited_groups(&reader, arguments.group.as_deref(), &mut warnings)?;
    let topic_partition_counts: BTreeMap<String, usize> = reader
        .list_topic_partitions(None)?
        .into_iter()
        .map(|(topic, partitions)| (topic, partitions.len()))
        .collect();
    let mut findings = Vec::new();
    for group in &groups {
        let lag = read_lag_of_empty_group(&reader, group, &mut warnings)?;
        findings.extend(evaluate_group_rules(&AuditInput {
            group,
            lag: lag.as_ref(),
            topic_partition_counts: &topic_partition_counts,
            service_pattern: service_pattern.as_ref(),
            group_name_pattern: group_name_pattern.as_ref(),
        }));
    }

    let output_format = execution.global.output;
    write_collection_warnings(&warnings, output_format);
    let trailing_error = summarize_audit(&findings).map(anyhow::Error::new);
    let view = AuditView {
        groups_checked: groups.len(),
        findings,
        warnings,
    };
    Ok(RenderedOutput {
        text: Some(render_output(&view, output_format)?),
        trailing_error,
    })
}

fn compile_group_name_pattern(text: &str) -> Result<Regex, CoreError> {
    Regex::new(text).map_err(|error| {
        CoreError::InvalidInput(format!("group_name_pattern is not a valid regex: {error}"))
    })
}

/// The groups to audit, sorted by name. A requested group the broker does not describe is an error.
fn read_audited_groups(
    reader: &dyn ClusterOffsetsReader,
    group: Option<&str>,
    warnings: &mut Vec<String>,
) -> anyhow::Result<Vec<GroupDescription>> {
    let mut groups = reader.describe_groups(group)?;
    if let Some(name) = group {
        groups.retain(|description| description.name == name);
    }
    if groups.is_empty()
        && let Some(warning) = incomplete_group_list_warning(reader.broker_family())
    {
        warnings.push(warning.to_owned());
    }
    if let Some(name) = group
        && groups.is_empty()
    {
        return Err(CoreError::GroupNotFound(name.to_owned()).into());
    }
    groups.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(groups)
}

/// The lag of a group without members, for R3. Groups with members need none.
fn read_lag_of_empty_group(
    reader: &dyn ClusterOffsetsReader,
    group: &GroupDescription,
    warnings: &mut Vec<String>,
) -> anyhow::Result<Option<GroupLag>> {
    if !group.members.is_empty() {
        return Ok(None);
    }
    let request = LagRequest {
        group: Some(group.name.clone()),
        topic: None,
    };
    match collect_group_lag(reader, &request) {
        Ok(mut report) => {
            for warning in report.warnings.drain(..) {
                if !warnings.contains(&warning) {
                    warnings.push(warning);
                }
            }
            Ok(report.groups.into_iter().next())
        }
        Err(CoreError::GroupNotFound(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// `AuditFailed` with the number of ERROR findings, or `None` when there is none.
pub fn summarize_audit(findings: &[AuditFinding]) -> Option<CoreError> {
    let errors = findings
        .iter()
        .filter(|finding| finding.severity == AuditSeverity::Error)
        .count();
    (errors > 0).then_some(CoreError::AuditFailed(errors))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafcli_core::ops::groups::audit_rules::AuditSeverity;

    fn finding(rule: &'static str, severity: AuditSeverity) -> AuditFinding {
        AuditFinding {
            group: "g".to_owned(),
            rule,
            severity,
            message: "m".to_owned(),
        }
    }

    #[test]
    fn error_findings_produce_audit_failed() {
        let findings = vec![
            finding("R1", AuditSeverity::Error),
            finding("R2", AuditSeverity::Error),
            finding("R3", AuditSeverity::Warning),
        ];

        let error = summarize_audit(&findings).unwrap();

        assert!(matches!(error, CoreError::AuditFailed(2)), "{error:?}");
        assert_eq!(error.to_string(), "groups audit found 2 ERROR finding(s)");
    }

    #[test]
    fn warnings_and_info_pass() {
        let findings = vec![
            finding("R3", AuditSeverity::Warning),
            finding("INFO", AuditSeverity::Info),
        ];

        assert!(summarize_audit(&findings).is_none());
    }
}
