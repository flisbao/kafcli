//! The destructive flow shared by `offsets set` and `offsets restore`.
//!
//! With `--execute` the order never changes: group check (and `--wait-empty`), guard, group check
//! again, backup, commit, read back, audit. Without it nothing is written but the audit record.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::{Value, json};
use time::OffsetDateTime;

use crate::audit::audit_record::{AuditOutcome, AuditRecord};
use crate::audit::audit_writer::AuditWriter;
use crate::capability::broker_capabilities::BrokerFamily;
use crate::error::{CoreError, CoreResult};
use crate::guard::confirmation_prompter::ConfirmationPrompter;
use crate::guard::guard_enforcer::enforce_guard;
use crate::guard::guard_request::GuardRequest;
use crate::guard::operation_level::OperationLevel;
use crate::ops::groups::cluster_offsets_reader::ClusterOffsetsReader;
use crate::ops::offsets::group_activity::{
    GroupActivity, check_group_is_stopped, read_group_activity, wait_until_group_empty,
};
use crate::ops::offsets::group_offset_committer::GroupOffsetCommitter;
use crate::ops::offsets::offset_backup::{build_offset_backup, write_offset_backup};
use crate::ops::offsets::offset_reset_plan::OffsetResetPlan;

pub(crate) const EVENT_HUBS_COMMIT_WARNING: &str = "Azure Event Hubs does not refuse commits while a group has members (ADR 0001); kafcli checked the group just before committing";
const NOTHING_TO_COMMIT_WARNING: &str =
    "nothing to commit: every partition is already at its target offset";

/// What the user asked for and where things go. The binary builds `audit_target` per command.
pub struct DestructiveRunSettings<'a> {
    pub context_name: &'a str,
    pub context_protected: bool,
    pub execute: bool,
    pub wait_empty: Option<Duration>,
    pub group_stopped: bool,
    pub backup_directory: &'a Path,
    pub audit_target: String,
}

/// The cluster, the operator and the audit log, as the destructive flows see them.
pub struct DestructiveCollaborators<'a> {
    pub reader: &'a dyn ClusterOffsetsReader,
    pub committer: &'a dyn GroupOffsetCommitter,
    pub prompter: &'a dyn ConfirmationPrompter,
    pub audit: &'a dyn AuditWriter,
    pub now: &'a dyn Fn() -> Instant,
}

#[derive(Debug, Serialize)]
pub struct OffsetResetOutcome {
    pub plan: OffsetResetPlan,
    pub dry_run: bool,
    pub group_activity: GroupActivity,
    pub backup_file: Option<PathBuf>,
    pub committed: bool,
    pub warnings: Vec<String>,
}

/// Applies `plan`, or only shows it without `--execute`. Every outcome writes one audit record.
pub fn execute_offset_reset(
    collaborators: &DestructiveCollaborators,
    plan: OffsetResetPlan,
    command_name: &str,
    settings: &DestructiveRunSettings,
) -> CoreResult<OffsetResetOutcome> {
    let audit = ResetAudit {
        collaborators,
        settings,
        command_name,
        before: offsets_before(&plan),
        after: offsets_after(&plan),
    };
    let mut warnings = plan.warnings.clone();
    let first_activity = read_group_activity(collaborators.reader, &plan.group);
    if !settings.execute {
        if let GroupActivity::Active { members } = first_activity {
            warnings.push(active_group_dry_run_warning(&plan.group, members));
        }
        audit.record(AuditOutcome::DryRun, audit.after.clone());
        return Ok(OffsetResetOutcome {
            plan,
            dry_run: true,
            group_activity: first_activity,
            backup_file: None,
            committed: false,
            warnings,
        });
    }

    let group_activity = match confirm_group_is_stopped(
        collaborators,
        &plan.group,
        first_activity,
        &format!("{command_name} for group {}", plan.group),
        settings,
    ) {
        Ok(activity) => activity,
        Err(error) => {
            audit.record(AuditOutcome::Aborted, audit.after.clone());
            return Err(error);
        }
    };

    let offsets = plan.offsets_to_commit();
    if offsets.is_empty() {
        warnings.push(NOTHING_TO_COMMIT_WARNING.to_owned());
        audit.record(AuditOutcome::Executed, Vec::new());
        return Ok(OffsetResetOutcome {
            plan,
            dry_run: false,
            group_activity,
            backup_file: None,
            committed: false,
            warnings,
        });
    }

    let backup = build_offset_backup(
        &plan,
        settings.context_name,
        command_name,
        OffsetDateTime::now_utc(),
    );
    let backup_file = match write_offset_backup(settings.backup_directory, &backup) {
        Ok(path) => path,
        Err(error) => {
            audit.record(AuditOutcome::Failed, audit.after.clone());
            return Err(error.with_context("no offset was committed"));
        }
    };
    let after = with_backup_file(audit.after.clone(), &backup_file);
    let backup_context = backup_location_context(&backup_file);
    let written = collaborators
        .committer
        .commit_group_offsets(&plan.group, &offsets)
        .and_then(|()| verify_committed_offsets(collaborators.reader, &plan.group, &offsets));
    if let Err(error) = written {
        audit.record(AuditOutcome::Failed, after);
        return Err(error.with_context(&backup_context));
    }
    if collaborators.reader.broker_family() == BrokerFamily::AzureEventHubs {
        warnings.push(EVENT_HUBS_COMMIT_WARNING.to_owned());
    }
    audit.record(AuditOutcome::Executed, after);
    Ok(OffsetResetOutcome {
        plan,
        dry_run: false,
        group_activity,
        backup_file: Some(backup_file),
        committed: true,
        warnings,
    })
}

/// Steps 1 to 3 of the destructive order: the group is stopped (after `--wait-empty`), the
/// operator confirms, and the group is still stopped. Returns the last activity read.
pub(crate) fn confirm_group_is_stopped(
    collaborators: &DestructiveCollaborators,
    group: &str,
    first_activity: GroupActivity,
    description: &str,
    settings: &DestructiveRunSettings,
) -> CoreResult<GroupActivity> {
    let family = collaborators.reader.broker_family();
    let activity = match (settings.wait_empty, &first_activity) {
        (Some(timeout), GroupActivity::Active { .. }) => {
            wait_until_group_empty(collaborators.reader, group, timeout, collaborators.now)?
        }
        _ => first_activity,
    };
    check_group_is_stopped(group, &activity, family, settings.group_stopped)?;
    enforce_guard(
        &GuardRequest {
            level: OperationLevel::Destructive,
            description: description.to_owned(),
            confirmation_target: group.to_owned(),
            assume_yes: false,
            execute: true,
        },
        settings.context_protected,
        collaborators.prompter,
    )?;
    let activity = read_group_activity(collaborators.reader, group);
    check_group_is_stopped(group, &activity, family, settings.group_stopped)?;
    Ok(activity)
}

/// Step 6: the committed offsets read back must be the ones just committed.
pub(crate) fn verify_committed_offsets(
    reader: &dyn ClusterOffsetsReader,
    group: &str,
    offsets: &BTreeMap<(String, i32), i64>,
) -> CoreResult<()> {
    let mut partitions: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for (topic, partition) in offsets.keys() {
        partitions
            .entry(topic.clone())
            .or_default()
            .push(*partition);
    }
    let committed = reader.fetch_committed_offsets(group, &partitions)?;
    for ((topic, partition), expected) in offsets {
        let found = committed
            .get(&(topic.clone(), *partition))
            .copied()
            .flatten();
        if found != Some(*expected) {
            let found = found.map_or_else(|| "no commit".to_owned(), |offset| offset.to_string());
            return Err(CoreError::Kafka(format!(
                "committed offsets do not match the plan for {topic}/{partition}: expected {expected}, found {found}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn backup_location_context(backup_file: &Path) -> String {
    format!("offsets before the change are in {}", backup_file.display())
}

pub(crate) fn active_group_dry_run_warning(group: &str, members: usize) -> String {
    format!(
        "consumer group '{group}' has {members} active member(s); --execute will refuse until it is empty"
    )
}

fn offsets_before(plan: &OffsetResetPlan) -> Vec<Value> {
    plan.changes
        .iter()
        .map(|change| {
            json!({"topic": change.topic, "partition": change.partition, "offset": change.current_offset})
        })
        .collect()
}

fn offsets_after(plan: &OffsetResetPlan) -> Vec<Value> {
    plan.changes
        .iter()
        .map(|change| {
            json!({"topic": change.topic, "partition": change.partition, "offset": change.new_offset})
        })
        .collect()
}

fn with_backup_file(mut after: Vec<Value>, backup_file: &Path) -> Vec<Value> {
    after.push(json!({"backup_file": backup_file.display().to_string()}));
    after
}

/// Writes the reset's audit records. A failed write is logged and never fails the command.
struct ResetAudit<'a> {
    collaborators: &'a DestructiveCollaborators<'a>,
    settings: &'a DestructiveRunSettings<'a>,
    command_name: &'a str,
    before: Vec<Value>,
    after: Vec<Value>,
}

impl ResetAudit<'_> {
    fn record(&self, outcome: AuditOutcome, after: Vec<Value>) {
        let mut record = AuditRecord::new(
            self.settings.context_name,
            self.command_name,
            &self.settings.audit_target,
            outcome,
        );
        record.before = Some(Value::Array(self.before.clone()));
        record.after = Some(Value::Array(after));
        if let Err(error) = self.collaborators.audit.append_record(&record) {
            tracing::warn!(%error, "audit log not written");
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::audit::audit_record::AuditRecord;
    use crate::capability::broker_capabilities::BrokerFamily;
    use crate::ops::groups::fake_cluster_reader::FakeClusterReader;
    use crate::ops::groups::group_snapshot::{GroupDescription, GroupMember, PartitionWatermarks};
    use crate::ops::offsets::fake_offset_committer::FakeOffsetCommitter;
    use crate::ops::offsets::offset_backup::read_offset_backup;
    use crate::ops::offsets::offset_reset_plan::{OffsetResetRequest, plan_offset_reset};
    use crate::ops::offsets::offset_target::OffsetTarget;
    use std::cell::{Cell, RefCell};

    /// Prompter with one scripted typed answer that counts prompts.
    pub(crate) struct ScriptedPrompter {
        pub interactive: bool,
        pub typed_answer: bool,
        pub calls: Cell<u32>,
    }

    impl ScriptedPrompter {
        pub(crate) fn answering(typed_answer: bool) -> Self {
            Self {
                interactive: true,
                typed_answer,
                calls: Cell::new(0),
            }
        }
    }

    impl ConfirmationPrompter for ScriptedPrompter {
        fn is_interactive(&self) -> bool {
            self.interactive
        }

        fn confirm_yes_no(&self, _message: &str) -> CoreResult<bool> {
            self.calls.set(self.calls.get() + 1);
            Ok(false)
        }

        fn confirm_typed_value(&self, _message: &str, _expected_value: &str) -> CoreResult<bool> {
            self.calls.set(self.calls.get() + 1);
            Ok(self.typed_answer)
        }
    }

    /// Keeps every record as JSON.
    #[derive(Default)]
    pub(crate) struct RecordingAuditWriter {
        pub records: RefCell<Vec<serde_json::Value>>,
    }

    impl RecordingAuditWriter {
        pub(crate) fn outcomes(&self) -> Vec<String> {
            self.records
                .borrow()
                .iter()
                .map(|record| record["outcome"].as_str().unwrap_or_default().to_owned())
                .collect()
        }
    }

    impl AuditWriter for RecordingAuditWriter {
        fn append_record(&self, record: &AuditRecord) -> CoreResult<()> {
            self.records.borrow_mut().push(
                serde_json::to_value(record)
                    .map_err(|error| CoreError::Io(std::io::Error::other(error)))?,
            );
            Ok(())
        }
    }

    pub(crate) fn group(members: usize) -> GroupDescription {
        GroupDescription {
            name: "g".to_owned(),
            state: if members == 0 { "Empty" } else { "Stable" }.to_owned(),
            protocol_type: "consumer".to_owned(),
            members: (0..members)
                .map(|index| GroupMember {
                    member_id: format!("m{index}"),
                    client_id: "c".to_owned(),
                    host: "/10.0.0.7".to_owned(),
                    subscription: None,
                    assignment: None,
                })
                .collect(),
        }
    }

    /// Topic `t` with one partition (log 0..20), group `g` committed at 13.
    pub(crate) fn reader_with_group(members: usize) -> FakeClusterReader {
        let mut reader = FakeClusterReader::new(BrokerFamily::GenericKafka);
        reader.topics.insert("t".to_owned(), vec![0]);
        reader.watermarks.insert(
            ("t".to_owned(), 0),
            PartitionWatermarks { low: 0, high: 20 },
        );
        reader
            .committed
            .insert(("g".to_owned(), "t".to_owned(), 0), 13);
        reader.groups = vec![group(members)];
        reader
    }

    fn plan_to(reader: &FakeClusterReader, target: OffsetTarget) -> OffsetResetPlan {
        plan_offset_reset(
            reader,
            &OffsetResetRequest {
                group: "g".to_owned(),
                topic: "t".to_owned(),
                partitions: None,
                target,
            },
        )
        .unwrap()
    }

    struct Harness {
        directory: tempfile::TempDir,
        prompter: ScriptedPrompter,
        audit: RecordingAuditWriter,
    }

    impl Harness {
        fn new() -> Self {
            Self {
                directory: tempfile::tempdir().unwrap(),
                prompter: ScriptedPrompter::answering(true),
                audit: RecordingAuditWriter::default(),
            }
        }

        fn settings(&self, execute: bool) -> DestructiveRunSettings<'_> {
            DestructiveRunSettings {
                context_name: "dev",
                context_protected: false,
                execute,
                wait_empty: None,
                group_stopped: false,
                backup_directory: self.directory.path(),
                audit_target: "group=g topic=t".to_owned(),
            }
        }

        fn run(
            &self,
            reader: &FakeClusterReader,
            committer: &dyn GroupOffsetCommitter,
            settings: &DestructiveRunSettings,
        ) -> CoreResult<OffsetResetOutcome> {
            let plan = plan_to(reader, OffsetTarget::Latest);
            let now = || reader.now();
            let collaborators = DestructiveCollaborators {
                reader,
                committer,
                prompter: &self.prompter,
                audit: &self.audit,
                now: &now,
            };
            execute_offset_reset(&collaborators, plan, "offsets set", settings)
        }

        fn backup_files(&self) -> usize {
            std::fs::read_dir(self.directory.path()).unwrap().count()
        }
    }

    #[test]
    fn dry_run_writes_nothing_but_audit() {
        let harness = Harness::new();
        let reader = reader_with_group(0);
        let committer = FakeOffsetCommitter::linked_to(&reader);

        let outcome = harness
            .run(&reader, &committer, &harness.settings(false))
            .unwrap();

        assert!(outcome.dry_run);
        assert!(!outcome.committed);
        assert_eq!(outcome.backup_file, None);
        assert!(committer.commits().is_empty());
        assert_eq!(harness.backup_files(), 0);
        assert_eq!(harness.audit.outcomes(), vec!["dry-run"]);
        assert_eq!(harness.prompter.calls.get(), 0);
    }

    #[test]
    fn dry_run_warns_about_active_group() {
        let harness = Harness::new();
        let reader = reader_with_group(2);
        let committer = FakeOffsetCommitter::new();

        let outcome = harness
            .run(&reader, &committer, &harness.settings(false))
            .unwrap();

        assert_eq!(outcome.group_activity, GroupActivity::Active { members: 2 });
        assert!(
            outcome.warnings.contains(
                &"consumer group 'g' has 2 active member(s); --execute will refuse until it is empty"
                    .to_owned()
            ),
            "{:?}",
            outcome.warnings
        );
    }

    #[test]
    fn execute_on_empty_group_backs_up_commits_and_verifies() {
        let harness = Harness::new();
        let reader = reader_with_group(0);
        let committer = FakeOffsetCommitter::linked_to(&reader);

        let outcome = harness
            .run(&reader, &committer, &harness.settings(true))
            .unwrap();

        assert!(outcome.committed);
        assert!(!outcome.dry_run);
        assert_eq!(
            committer.commits(),
            vec![("g".to_owned(), BTreeMap::from([(("t".to_owned(), 0), 20)]))]
        );
        let backup_file = outcome.backup_file.unwrap();
        let backup = read_offset_backup(&backup_file).unwrap();
        assert_eq!(backup.partitions[0].committed_offset, Some(13));
        assert_eq!(backup.command, "offsets set");
        assert_eq!(harness.audit.outcomes(), vec!["executed"]);
        let record = &harness.audit.records.borrow()[0];
        assert_eq!(record["command"], "offsets set");
        assert_eq!(record["target"], "group=g topic=t");
        assert_eq!(record["before"][0]["offset"], 13);
        assert_eq!(record["after"][0]["offset"], 20);
        assert_eq!(
            record["after"][1]["backup_file"],
            backup_file.display().to_string()
        );
    }

    #[test]
    fn active_group_is_refused_before_prompting() {
        let harness = Harness::new();
        let reader = reader_with_group(1);
        let committer = FakeOffsetCommitter::new();

        let result = harness.run(&reader, &committer, &harness.settings(true));

        assert!(
            matches!(result, Err(CoreError::GroupNotEmpty { members: 1, .. })),
            "{result:?}"
        );
        assert_eq!(harness.prompter.calls.get(), 0);
        assert_eq!(harness.backup_files(), 0);
        assert!(committer.commits().is_empty());
        assert_eq!(harness.audit.outcomes(), vec!["aborted"]);
    }

    #[test]
    fn member_joining_after_confirmation_is_refused() {
        let harness = Harness::new();
        let reader = reader_with_group(0);
        reader
            .group_snapshots
            .borrow_mut()
            .extend([vec![group(0)], vec![group(1)]]);
        let committer = FakeOffsetCommitter::new();
        let mut settings = harness.settings(true);
        settings.context_protected = true;

        let result = harness.run(&reader, &committer, &settings);

        assert!(
            matches!(result, Err(CoreError::GroupNotEmpty { .. })),
            "{result:?}"
        );
        assert_eq!(harness.prompter.calls.get(), 1);
        assert_eq!(harness.backup_files(), 0);
        assert!(committer.commits().is_empty());
        assert_eq!(harness.audit.outcomes(), vec!["aborted"]);
    }

    #[test]
    fn wait_empty_waits_then_proceeds() {
        let harness = Harness::new();
        let reader = reader_with_group(0);
        reader.group_snapshots.borrow_mut().extend([
            vec![group(1)],
            vec![group(1)],
            vec![group(0)],
        ]);
        let committer = FakeOffsetCommitter::linked_to(&reader);
        let mut settings = harness.settings(true);
        settings.wait_empty = Some(Duration::from_secs(60));

        let outcome = harness.run(&reader, &committer, &settings).unwrap();

        assert!(outcome.committed);
        assert_eq!(reader.waits(), vec![Duration::from_secs(2)]);
    }

    #[test]
    fn protected_context_requires_typed_group_name() {
        let mut harness = Harness::new();
        harness.prompter.interactive = false;
        let reader = reader_with_group(0);
        let committer = FakeOffsetCommitter::new();
        let mut settings = harness.settings(true);
        settings.context_protected = true;

        let result = harness.run(&reader, &committer, &settings);

        assert!(
            matches!(result, Err(CoreError::OperationAborted(_))),
            "{result:?}"
        );
        assert!(committer.commits().is_empty());
        assert_eq!(harness.audit.outcomes(), vec!["aborted"]);
    }

    #[test]
    fn commit_failure_names_the_backup() {
        let harness = Harness::new();
        let reader = reader_with_group(0);
        let mut committer = FakeOffsetCommitter::new();
        committer.failure = Some("boom".to_owned());

        let error = harness
            .run(&reader, &committer, &harness.settings(true))
            .unwrap_err();

        assert!(matches!(error, CoreError::Kafka(_)), "{error:?}");
        let text = error.to_string();
        assert!(
            text.starts_with("kafka error: boom; offsets before the change are in "),
            "{text}"
        );
        assert!(
            text.contains(&harness.directory.path().display().to_string()),
            "{text}"
        );
        assert_eq!(harness.backup_files(), 1);
        assert_eq!(harness.audit.outcomes(), vec!["failed"]);
    }

    #[test]
    fn refused_commit_keeps_its_variant() {
        let harness = Harness::new();
        let reader = reader_with_group(0);
        let mut committer = FakeOffsetCommitter::new();
        committer.refuse_active_group = true;

        let error = harness
            .run(&reader, &committer, &harness.settings(true))
            .unwrap_err();

        assert!(
            matches!(&error, CoreError::CommitRefusedActiveGroup { group, .. } if group == "g"),
            "{error:?}"
        );
        let text = error.to_string();
        assert!(
            text.starts_with("the broker refused the offset commit: consumer group 'g' has active members; offsets before the change are in "),
            "{text}"
        );
    }

    #[test]
    fn commit_not_read_back_is_failure() {
        let harness = Harness::new();
        let reader = reader_with_group(0);
        let committer = FakeOffsetCommitter::new();

        let error = harness
            .run(&reader, &committer, &harness.settings(true))
            .unwrap_err();

        let text = error.to_string();
        assert!(
            text.starts_with("kafka error: committed offsets do not match the plan for t/0: expected 20, found 13; offsets before the change are in "),
            "{text}"
        );
        assert_eq!(harness.audit.outcomes(), vec!["failed"]);
    }

    #[test]
    fn nothing_to_commit_skips_backup_and_commit() {
        let harness = Harness::new();
        let reader = reader_with_group(0);
        let committer = FakeOffsetCommitter::new();
        let plan = plan_to(&reader, OffsetTarget::Offset(13));
        let now = || reader.now();
        let collaborators = DestructiveCollaborators {
            reader: &reader,
            committer: &committer,
            prompter: &harness.prompter,
            audit: &harness.audit,
            now: &now,
        };

        let outcome =
            execute_offset_reset(&collaborators, plan, "offsets set", &harness.settings(true))
                .unwrap();

        assert!(!outcome.committed);
        assert!(outcome.warnings.contains(
            &"nothing to commit: every partition is already at its target offset".to_owned()
        ));
        assert!(committer.commits().is_empty());
        assert_eq!(harness.backup_files(), 0);
        assert_eq!(harness.audit.outcomes(), vec!["executed"]);
        assert_eq!(
            harness.audit.records.borrow()[0]["after"],
            serde_json::json!([])
        );
    }

    #[test]
    fn event_hubs_commit_adds_warning() {
        let harness = Harness::new();
        let mut reader = reader_with_group(0);
        reader.family = BrokerFamily::AzureEventHubs;
        let committer = FakeOffsetCommitter::linked_to(&reader);

        let outcome = harness
            .run(&reader, &committer, &harness.settings(true))
            .unwrap();

        assert!(outcome.warnings.contains(
            &"Azure Event Hubs does not refuse commits while a group has members (ADR 0001); kafcli checked the group just before committing".to_owned()
        ));
    }

    #[test]
    fn audit_never_carries_message_data() {
        let harness = Harness::new();
        let reader = reader_with_group(0);
        let committer = FakeOffsetCommitter::linked_to(&reader);

        harness
            .run(&reader, &committer, &harness.settings(true))
            .unwrap();

        let records = harness.audit.records.borrow();
        for side in ["before", "after"] {
            for entry in records[0][side].as_array().unwrap() {
                for key in entry.as_object().unwrap().keys() {
                    assert!(
                        ["topic", "partition", "offset", "backup_file"].contains(&key.as_str()),
                        "{side}: {key}"
                    );
                }
            }
        }
    }
}
