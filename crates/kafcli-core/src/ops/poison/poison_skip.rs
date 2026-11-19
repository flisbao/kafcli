//! `poison skip`: copy the message a group is stuck on to a dead-letter topic, then commit past it.
//!
//! Same order as `offsets set`: group check, guard, group check again, backup; then the copy, the
//! commit of `offset + 1` and the read back. The audit log never holds key, value or headers.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};
use time::OffsetDateTime;

use crate::audit::audit_record::{AuditOutcome, resolve_operator_name};
use crate::capability::broker_capabilities::BrokerFamily;
use crate::error::{CoreError, CoreResult};
use crate::message::record_header::RecordHeader;
use crate::message::timestamp_text::format_rfc3339_millis;
use crate::ops::offsets::destructive_preflight::confirm_and_check;
use crate::ops::offsets::group_activity::{GroupActivity, read_group_activity};
use crate::ops::offsets::offset_backup::{build_offset_backup, write_offset_backup};
use crate::ops::offsets::offset_reset_execution::{
    DestructiveCollaborators, DestructiveRunSettings, EVENT_HUBS_COMMIT_WARNING,
    active_group_dry_run_warning, backup_location_context, verify_committed_offsets,
    write_destructive_audit,
};
use crate::ops::offsets::offset_reset_plan::{
    PlanBuilder, missing_partition_error, read_topic_partitions,
};
use crate::ops::offsets::offset_target::ResolvedOffset;
use crate::ops::poison::dead_letter_publisher::DeadLetterPublisher;
use crate::ops::poison::partition_message_reader::{FetchedRecord, PartitionMessageReader};
use crate::ops::produce::message_producer::{DeliveryReport, PreparedMessage};

const COMMAND_NAME: &str = "poison skip";
const MAX_REASON_BYTES: usize = 1024;
/// Prefix of every header kafcli adds to a dead-letter copy.
const ORIGINAL_HEADER_PREFIX: &str = "kafcli-original-";
pub const ORIGINAL_TOPIC_HEADER: &str = "kafcli-original-topic";
pub const ORIGINAL_PARTITION_HEADER: &str = "kafcli-original-partition";
pub const ORIGINAL_OFFSET_HEADER: &str = "kafcli-original-offset";
pub const ORIGINAL_REASON_HEADER: &str = "kafcli-original-reason";
pub const ORIGINAL_OPERATOR_HEADER: &str = "kafcli-original-operator";
pub const ORIGINAL_TIMESTAMP_HEADER: &str = "kafcli-original-timestamp";

/// What `poison skip` was asked to do.
pub struct PoisonSkipRequest {
    pub group: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub dead_letter_topic: String,
    pub reason: String,
}

#[derive(Debug, Serialize)]
pub struct PoisonSkipOutcome {
    pub group: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub dead_letter_topic: String,
    pub dry_run: bool,
    pub group_activity: GroupActivity,
    pub headers_added: Vec<String>,
    pub dead_letter_delivery: Option<DeliveryReport>,
    pub committed_offset: Option<i64>,
    pub backup_file: Option<PathBuf>,
    pub warnings: Vec<String>,
}

/// Checks the request without contacting anything.
pub fn validate_poison_skip_request(request: &PoisonSkipRequest) -> CoreResult<()> {
    let invalid = |text: &str| Err(CoreError::InvalidInput(text.to_owned()));
    if request.reason.trim().is_empty() {
        return invalid("--reason must not be empty");
    }
    if request.reason.len() > MAX_REASON_BYTES {
        return invalid("--reason must be at most 1024 bytes");
    }
    if request.dead_letter_topic == request.topic {
        return invalid("--dlt must differ from --topic");
    }
    if request.offset < 0 {
        return invalid("--offset must be zero or positive");
    }
    Ok(())
}

/// The dead-letter copy of `record`: same key, value and headers (minus earlier
/// `kafcli-original-*` ones), then the six `kafcli-original-*` headers. The timestamp header is
/// left out when the message has no timestamp.
pub fn build_dead_letter_copy(
    record: &FetchedRecord,
    reason: &str,
    operator: &str,
) -> PreparedMessage {
    let mut headers: Vec<RecordHeader> = record
        .headers
        .iter()
        .filter(|header| !header.name.starts_with(ORIGINAL_HEADER_PREFIX))
        .cloned()
        .collect();
    let mut add = |name: &str, value: String| {
        headers.push(RecordHeader {
            name: name.to_owned(),
            value: Some(value.into_bytes()),
        });
    };
    add(ORIGINAL_TOPIC_HEADER, record.topic.clone());
    add(ORIGINAL_PARTITION_HEADER, record.partition.to_string());
    add(ORIGINAL_OFFSET_HEADER, record.offset.to_string());
    add(ORIGINAL_REASON_HEADER, reason.to_owned());
    add(ORIGINAL_OPERATOR_HEADER, operator.to_owned());
    if let Some(timestamp) = record
        .timestamp_millis
        .and_then(|millis| {
            OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000).ok()
        })
        .and_then(format_rfc3339_millis)
    {
        add(ORIGINAL_TIMESTAMP_HEADER, timestamp);
    }
    PreparedMessage {
        key: record.key.clone(),
        value: record.value.clone(),
        headers,
        partition: None,
    }
}

/// Copies the message at `request.offset` to the dead-letter topic and commits `offset + 1`, or
/// only shows what would happen without `--execute`.
pub fn execute_poison_skip(
    collaborators: &DestructiveCollaborators,
    messages: &dyn PartitionMessageReader,
    publisher: &dyn DeadLetterPublisher,
    request: &PoisonSkipRequest,
    settings: &DestructiveRunSettings,
) -> CoreResult<PoisonSkipOutcome> {
    validate_poison_skip_request(request)?;
    let reader = collaborators.reader;
    let (topic, partition, offset) = (request.topic.as_str(), request.partition, request.offset);
    if !read_topic_partitions(reader, topic)?.contains(&partition) {
        return Err(missing_partition_error(topic, partition));
    }
    let committed = reader
        .fetch_committed_offsets(
            &request.group,
            &BTreeMap::from([(topic.to_owned(), vec![partition])]),
        )?
        .get(&(topic.to_owned(), partition))
        .copied()
        .flatten();
    match committed {
        None => {
            return Err(CoreError::InvalidInput(format!(
                "{topic}/{partition} has no committed offset for group '{}'",
                request.group
            )));
        }
        Some(committed) if committed != offset => {
            return Err(CoreError::InvalidInput(format!(
                "committed offset of {topic}/{partition} is {committed}, not {offset}; run 'kafcli poison find' first"
            )));
        }
        Some(_) => {}
    }
    let record = messages
        .read_first_messages(topic, &BTreeMap::from([(partition, offset)]))?
        .remove(&partition)
        .filter(|record| record.offset == offset)
        .ok_or_else(|| {
            CoreError::InvalidInput(format!(
                "no message at offset {offset} in {topic}/{partition}"
            ))
        })?;
    read_topic_partitions(reader, &request.dead_letter_topic)?;
    let watermarks = reader.fetch_watermarks(topic, partition)?;
    let copy = build_dead_letter_copy(&record, &request.reason, &resolve_operator_name());
    let headers_added: Vec<String> = copy
        .headers
        .iter()
        .filter(|header| header.name.starts_with(ORIGINAL_HEADER_PREFIX))
        .map(|header| header.name.clone())
        .collect();
    let next_offset = offset.saturating_add(1);
    let before = vec![json!({"topic": topic, "partition": partition, "offset": offset})];
    let audit = |outcome: AuditOutcome, after: Vec<Value>| {
        write_destructive_audit(
            collaborators,
            settings,
            COMMAND_NAME,
            outcome,
            before.clone(),
            after,
        );
    };
    let planned_after = |delivery: Option<&DeliveryReport>, backup: Option<&PathBuf>| {
        vec![json!({
            "topic": topic,
            "partition": partition,
            "offset": next_offset,
            "dead_letter_topic": request.dead_letter_topic,
            "dead_letter_partition": delivery.map(|delivery| delivery.partition),
            "dead_letter_offset": delivery.map(|delivery| delivery.offset),
            "reason": request.reason,
            "backup_file": backup.map(|path| path.display().to_string()),
        })]
    };
    let mut outcome = PoisonSkipOutcome {
        group: request.group.clone(),
        topic: topic.to_owned(),
        partition,
        offset,
        dead_letter_topic: request.dead_letter_topic.clone(),
        dry_run: !settings.execute,
        group_activity: read_group_activity(reader, &request.group),
        headers_added,
        dead_letter_delivery: None,
        committed_offset: None,
        backup_file: None,
        warnings: Vec::new(),
    };

    if !settings.execute {
        if let GroupActivity::Active { members } = outcome.group_activity {
            outcome
                .warnings
                .push(active_group_dry_run_warning(&request.group, members));
        }
        audit(AuditOutcome::DryRun, planned_after(None, None));
        return Ok(outcome);
    }

    // The offset check covers a consumer that left during the wait after moving past the message;
    // skipping then would copy a consumed message and rewind the group.
    outcome.group_activity = match confirm_and_check(
        collaborators,
        settings,
        &request.group,
        outcome.group_activity.clone(),
        &format!("{COMMAND_NAME} for group {}", request.group),
        &BTreeMap::from([((topic.to_owned(), partition), Some(offset))]),
    ) {
        Ok(activity) => activity,
        Err(error) => {
            audit(AuditOutcome::Aborted, planned_after(None, None));
            return Err(error);
        }
    };

    let mut plan = PlanBuilder::new(&request.group);
    plan.push(
        topic,
        partition,
        Some(offset),
        watermarks,
        ResolvedOffset {
            offset: next_offset,
            clamped: false,
        },
    );
    let backup = build_offset_backup(
        &plan.finish(),
        settings.context_name,
        COMMAND_NAME,
        OffsetDateTime::now_utc(),
    );
    let backup_file = match write_offset_backup(settings.backup_directory, &backup) {
        Ok(path) => path,
        Err(error) => {
            audit(AuditOutcome::Failed, planned_after(None, None));
            return Err(error.with_context("nothing was copied or committed"));
        }
    };
    let backup_context = backup_location_context(&backup_file);
    let delivery = match publisher.publish_copy(&request.dead_letter_topic, copy) {
        Ok(delivery) => delivery,
        Err(error) => {
            audit(
                AuditOutcome::Failed,
                planned_after(None, Some(&backup_file)),
            );
            return Err(error.with_context(&format!("nothing was committed; {backup_context}")));
        }
    };
    let offsets = BTreeMap::from([((topic.to_owned(), partition), next_offset)]);
    let written = collaborators
        .committer
        .commit_group_offsets(&request.group, &offsets)
        .and_then(|()| verify_committed_offsets(reader, &request.group, &offsets));
    let after = planned_after(Some(&delivery), Some(&backup_file));
    if let Err(error) = written {
        audit(AuditOutcome::Failed, after);
        return Err(error.with_context(&format!(
            "the message was already copied to {} partition {} offset {}, running skip again would copy it twice; {backup_context}",
            request.dead_letter_topic, delivery.partition, delivery.offset
        )));
    }
    if reader.broker_family() == BrokerFamily::AzureEventHubs {
        outcome.warnings.push(EVENT_HUBS_COMMIT_WARNING.to_owned());
    }
    audit(AuditOutcome::Executed, after);
    outcome.dead_letter_delivery = Some(delivery);
    outcome.committed_offset = Some(next_offset);
    outcome.backup_file = Some(backup_file);
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::groups::fake_cluster_reader::FakeClusterReader;
    use crate::ops::offsets::fake_offset_committer::FakeOffsetCommitter;
    use crate::ops::offsets::group_offset_committer::GroupOffsetCommitter;
    use crate::ops::offsets::offset_backup::read_offset_backup;
    use crate::ops::offsets::offset_reset_execution::tests::{
        RecordingAuditWriter, ScriptedPrompter, reader_with_group,
    };
    use crate::ops::poison::dead_letter_publisher::FakeDeadLetterPublisher;
    use crate::ops::poison::fake_message_reader::FakePartitionMessageReader;
    use std::time::Duration;

    fn header(name: &str, value: &str) -> RecordHeader {
        RecordHeader {
            name: name.to_owned(),
            value: Some(value.as_bytes().to_vec()),
        }
    }

    fn stuck_record() -> FetchedRecord {
        FetchedRecord {
            topic: "t".to_owned(),
            partition: 0,
            offset: 13,
            timestamp_millis: Some(1_795_000_000_123),
            key: Some(b"k-1".to_vec()),
            value: Some(vec![0xFF, 0x00, 0x01]),
            headers: vec![header("trace", "abc")],
        }
    }

    fn header_text(message: &PreparedMessage, name: &str) -> Option<String> {
        message
            .headers
            .iter()
            .find(|header| header.name == name)
            .and_then(|header| header.value.clone())
            .map(|value| String::from_utf8(value).unwrap())
    }

    #[test]
    fn copy_keeps_key_value_and_headers_and_adds_original_headers() {
        let copy = build_dead_letter_copy(&stuck_record(), "bad json", "alice");

        assert_eq!(copy.key, Some(b"k-1".to_vec()));
        assert_eq!(copy.value, Some(vec![0xFF, 0x00, 0x01]));
        assert_eq!(copy.partition, None);
        let names: Vec<&str> = copy
            .headers
            .iter()
            .map(|header| header.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "trace",
                ORIGINAL_TOPIC_HEADER,
                ORIGINAL_PARTITION_HEADER,
                ORIGINAL_OFFSET_HEADER,
                ORIGINAL_REASON_HEADER,
                ORIGINAL_OPERATOR_HEADER,
                ORIGINAL_TIMESTAMP_HEADER,
            ]
        );
        assert_eq!(
            header_text(&copy, "kafcli-original-topic").as_deref(),
            Some("t")
        );
        assert_eq!(
            header_text(&copy, "kafcli-original-partition").as_deref(),
            Some("0")
        );
        assert_eq!(
            header_text(&copy, "kafcli-original-offset").as_deref(),
            Some("13")
        );
        assert_eq!(
            header_text(&copy, "kafcli-original-reason").as_deref(),
            Some("bad json")
        );
        assert_eq!(
            header_text(&copy, "kafcli-original-operator").as_deref(),
            Some("alice")
        );
        assert_eq!(
            header_text(&copy, "kafcli-original-timestamp").as_deref(),
            Some("2026-11-18T11:06:40.123Z")
        );
    }

    #[test]
    fn copy_replaces_previous_kafcli_original_headers() {
        let mut record = stuck_record();
        record.headers.push(header("kafcli-original-offset", "2"));
        record.headers.push(header("kafcli-original-reason", "old"));

        let copy = build_dead_letter_copy(&record, "bad json", "alice");

        let offsets: Vec<&RecordHeader> = copy
            .headers
            .iter()
            .filter(|header| header.name == "kafcli-original-offset")
            .collect();
        assert_eq!(offsets.len(), 1);
        assert_eq!(
            header_text(&copy, "kafcli-original-reason").as_deref(),
            Some("bad json")
        );
    }

    #[test]
    fn copy_without_timestamp_omits_timestamp_header() {
        let mut record = stuck_record();
        record.timestamp_millis = None;

        let copy = build_dead_letter_copy(&record, "bad json", "alice");

        assert_eq!(header_text(&copy, "kafcli-original-timestamp"), None);
    }

    #[test]
    fn tombstone_copy_stays_tombstone() {
        let mut record = stuck_record();
        record.value = None;

        let copy = build_dead_letter_copy(&record, "bad json", "alice");

        assert_eq!(copy.value, None);
    }

    struct Harness {
        reader: FakeClusterReader,
        messages: FakePartitionMessageReader,
        publisher: FakeDeadLetterPublisher,
        prompter: ScriptedPrompter,
        audit: RecordingAuditWriter,
        directory: tempfile::TempDir,
    }

    impl Harness {
        fn new(members: usize) -> Self {
            let mut reader = reader_with_group(members);
            reader.topics.insert("t.DLT".to_owned(), vec![0]);
            let mut messages = FakePartitionMessageReader::new();
            messages
                .records
                .insert(("t".to_owned(), 0), vec![stuck_record()]);
            Self {
                reader,
                messages,
                publisher: FakeDeadLetterPublisher::new(),
                prompter: ScriptedPrompter::answering(true),
                audit: RecordingAuditWriter::default(),
                directory: tempfile::tempdir().unwrap(),
            }
        }

        fn request(&self) -> PoisonSkipRequest {
            PoisonSkipRequest {
                group: "g".to_owned(),
                topic: "t".to_owned(),
                partition: 0,
                offset: 13,
                dead_letter_topic: "t.DLT".to_owned(),
                reason: "bad json".to_owned(),
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
                audit_target: "group=g topic=t partition=0 offset=13 dlt=t.DLT".to_owned(),
            }
        }

        fn run(
            &self,
            committer: &dyn GroupOffsetCommitter,
            request: &PoisonSkipRequest,
            execute: bool,
        ) -> CoreResult<PoisonSkipOutcome> {
            let now = || self.reader.now();
            let collaborators = DestructiveCollaborators {
                reader: &self.reader,
                committer,
                prompter: &self.prompter,
                audit: &self.audit,
                now: &now,
            };
            execute_poison_skip(
                &collaborators,
                &self.messages,
                &self.publisher,
                request,
                &self.settings(execute),
            )
        }

        fn backup_files(&self) -> usize {
            std::fs::read_dir(self.directory.path()).unwrap().count()
        }
    }

    #[test]
    fn empty_reason_is_rejected_before_reading() {
        let harness = Harness::new(0);
        let committer = FakeOffsetCommitter::new();
        let mut request = harness.request();
        request.reason = "   ".to_owned();

        let result = harness.run(&committer, &request, true);

        assert!(
            matches!(&result, Err(CoreError::InvalidInput(text)) if text == "--reason must not be empty"),
            "{result:?}"
        );
        assert!(harness.messages.reads().is_empty());
        assert!(harness.reader.committed_offset_requests().is_empty());
    }

    #[test]
    fn request_validation_messages() {
        let harness = Harness::new(0);
        let mut long_reason = harness.request();
        long_reason.reason = "x".repeat(1025);
        let mut same_topic = harness.request();
        same_topic.dead_letter_topic = "t".to_owned();
        let mut negative = harness.request();
        negative.offset = -1;

        let messages: Vec<String> = [long_reason, same_topic, negative]
            .iter()
            .map(|request| {
                validate_poison_skip_request(request)
                    .unwrap_err()
                    .to_string()
            })
            .collect();

        assert_eq!(
            messages,
            vec![
                "invalid input: --reason must be at most 1024 bytes",
                "invalid input: --dlt must differ from --topic",
                "invalid input: --offset must be zero or positive",
            ]
        );
    }

    #[test]
    fn offset_other_than_committed_is_refused_before_writing() {
        let harness = Harness::new(0);
        let committer = FakeOffsetCommitter::new();
        let mut request = harness.request();
        request.offset = 12;

        let result = harness.run(&committer, &request, true);

        assert!(
            matches!(&result, Err(CoreError::InvalidInput(text)) if text == "committed offset of t/0 is 13, not 12; run 'kafcli poison find' first"),
            "{result:?}"
        );
        assert!(harness.publisher.published().is_empty());
        assert!(committer.commits().is_empty());
    }

    #[test]
    fn missing_message_is_refused() {
        let mut harness = Harness::new(0);
        harness.messages.records.clear();
        let committer = FakeOffsetCommitter::new();

        let result = harness.run(&committer, &harness.request(), true);

        assert!(
            matches!(&result, Err(CoreError::InvalidInput(text)) if text == "no message at offset 13 in t/0"),
            "{result:?}"
        );
    }

    #[test]
    fn missing_dead_letter_topic_is_refused() {
        let mut harness = Harness::new(0);
        harness.reader.topics.remove("t.DLT");
        let committer = FakeOffsetCommitter::new();

        let result = harness.run(&committer, &harness.request(), true);

        assert!(
            matches!(result, Err(CoreError::TopicNotFound(_))),
            "{result:?}"
        );
    }

    #[test]
    fn dry_run_publishes_and_commits_nothing() {
        let harness = Harness::new(0);
        let committer = FakeOffsetCommitter::new();

        let outcome = harness.run(&committer, &harness.request(), false).unwrap();

        assert!(outcome.dry_run);
        assert_eq!(outcome.committed_offset, None);
        assert_eq!(outcome.headers_added.len(), 6);
        assert!(harness.publisher.published().is_empty());
        assert!(committer.commits().is_empty());
        assert_eq!(harness.backup_files(), 0);
        assert_eq!(harness.audit.outcomes(), vec!["dry-run"]);
    }

    #[test]
    fn execute_copies_then_commits_next_offset() {
        let harness = Harness::new(0);
        let committer = FakeOffsetCommitter::linked_to(&harness.reader);

        let outcome = harness.run(&committer, &harness.request(), true).unwrap();

        let published = harness.publisher.published();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].0, "t.DLT");
        assert_eq!(
            committer.commits(),
            vec![("g".to_owned(), BTreeMap::from([(("t".to_owned(), 0), 14)]))]
        );
        assert_eq!(outcome.committed_offset, Some(14));
        assert_eq!(
            outcome
                .dead_letter_delivery
                .as_ref()
                .map(|delivery| delivery.offset),
            Some(0)
        );
        let backup = read_offset_backup(outcome.backup_file.as_ref().unwrap()).unwrap();
        assert_eq!(backup.partitions[0].committed_offset, Some(13));
        assert_eq!(backup.command, "poison skip");
        assert_eq!(harness.audit.outcomes(), vec!["executed"]);
    }

    #[test]
    fn publish_failure_commits_nothing() {
        let mut harness = Harness::new(0);
        harness.publisher.failure = Some("broker down".to_owned());
        let committer = FakeOffsetCommitter::new();

        let error = harness
            .run(&committer, &harness.request(), true)
            .unwrap_err();

        assert!(committer.commits().is_empty());
        assert!(
            error
                .to_string()
                .contains("offsets before the change are in "),
            "{error}"
        );
        assert_eq!(harness.audit.outcomes(), vec!["failed"]);
    }

    #[test]
    fn commit_failure_after_copy_warns_about_duplicate() {
        let harness = Harness::new(0);
        let mut committer = FakeOffsetCommitter::new();
        committer.failure = Some("boom".to_owned());

        let error = harness
            .run(&committer, &harness.request(), true)
            .unwrap_err();

        let text = error.to_string();
        assert!(matches!(error, CoreError::Kafka(_)), "{text}");
        assert!(
            text.contains("the message was already copied to t.DLT partition 0 offset 0"),
            "{text}"
        );
        assert!(
            text.contains("running skip again would copy it twice"),
            "{text}"
        );
        assert!(text.contains("offsets before the change are in "), "{text}");
        assert_eq!(harness.audit.outcomes(), vec!["failed"]);
    }

    #[test]
    fn active_group_is_refused_before_copying() {
        let harness = Harness::new(1);
        let committer = FakeOffsetCommitter::new();

        let result = harness.run(&committer, &harness.request(), true);

        assert!(
            matches!(result, Err(CoreError::GroupNotEmpty { .. })),
            "{result:?}"
        );
        assert!(harness.publisher.published().is_empty());
        assert_eq!(harness.prompter.calls.get(), 0);
        assert_eq!(harness.backup_files(), 0);
        assert_eq!(harness.audit.outcomes(), vec!["aborted"]);
    }

    #[test]
    fn wait_empty_is_honoured() {
        let harness = Harness::new(0);
        harness.reader.group_snapshots.borrow_mut().extend([
            vec![crate::ops::offsets::offset_reset_execution::tests::group(1)],
            vec![crate::ops::offsets::offset_reset_execution::tests::group(1)],
            vec![crate::ops::offsets::offset_reset_execution::tests::group(0)],
        ]);
        let committer = FakeOffsetCommitter::linked_to(&harness.reader);
        let now = || harness.reader.now();
        let collaborators = DestructiveCollaborators {
            reader: &harness.reader,
            committer: &committer,
            prompter: &harness.prompter,
            audit: &harness.audit,
            now: &now,
        };
        let mut settings = harness.settings(true);
        settings.wait_empty = Some(Duration::from_secs(30));

        let outcome = execute_poison_skip(
            &collaborators,
            &harness.messages,
            &harness.publisher,
            &harness.request(),
            &settings,
        )
        .unwrap();

        assert_eq!(outcome.committed_offset, Some(14));
        assert_eq!(harness.reader.waits(), vec![Duration::from_secs(2)]);
    }

    #[test]
    fn audit_has_no_message_bytes() {
        let harness = Harness::new(0);
        let committer = FakeOffsetCommitter::linked_to(&harness.reader);

        harness.run(&committer, &harness.request(), true).unwrap();

        let records = harness.audit.records.borrow();
        let text = serde_json::to_string(&records[0]).unwrap();
        assert!(!text.contains("k-1"), "{text}");
        assert!(!text.contains("trace"), "{text}");
        assert_eq!(
            records[0]["target"],
            "group=g topic=t partition=0 offset=13 dlt=t.DLT"
        );
        assert_eq!(records[0]["before"][0]["offset"], 13);
        let after = &records[0]["after"][0];
        assert_eq!(after["offset"], 14);
        assert_eq!(after["dead_letter_topic"], "t.DLT");
        assert_eq!(after["dead_letter_partition"], 0);
        assert_eq!(after["dead_letter_offset"], 0);
        assert_eq!(after["reason"], "bad json");
        assert!(after["backup_file"].is_string());
        let keys: Vec<&String> = after.as_object().unwrap().keys().collect();
        assert_eq!(keys.len(), 8, "{keys:?}");
    }

    #[test]
    fn group_that_moved_during_wait_is_not_rewound() {
        let harness = Harness::new(0);
        harness.reader.group_snapshots.borrow_mut().extend([
            vec![crate::ops::offsets::offset_reset_execution::tests::group(1)],
            vec![crate::ops::offsets::offset_reset_execution::tests::group(0)],
        ]);
        let committer = FakeOffsetCommitter::linked_to(&harness.reader);
        // The consumer that leaves during --wait-empty commits past the stuck message.
        let moving_clock = || {
            harness
                .reader
                .committed_overrides
                .borrow_mut()
                .insert(("g".to_owned(), "t".to_owned(), 0), 15);
            harness.reader.now()
        };
        let collaborators = DestructiveCollaborators {
            reader: &harness.reader,
            committer: &committer,
            prompter: &harness.prompter,
            audit: &harness.audit,
            now: &moving_clock,
        };
        let mut settings = harness.settings(true);
        settings.wait_empty = Some(Duration::from_secs(30));

        let result = execute_poison_skip(
            &collaborators,
            &harness.messages,
            &harness.publisher,
            &harness.request(),
            &settings,
        );

        assert!(
            matches!(result, Err(CoreError::OperationAborted(_))),
            "{result:?}"
        );
        assert!(harness.publisher.published().is_empty());
        assert!(committer.commits().is_empty());
        assert_eq!(harness.backup_files(), 0);
        assert_eq!(harness.audit.outcomes(), vec!["aborted"]);
    }
}
