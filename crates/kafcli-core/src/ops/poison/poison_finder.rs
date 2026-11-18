//! `poison find`: the message each lagging partition of a group is stuck on, and whether it decodes.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::codec::ValueDecoder;
use crate::error::{CoreError, CoreResult};
use crate::message::message_envelope::MessageEnvelope;
use crate::ops::consume::message_consumer::build_envelope_value;
use crate::ops::groups::cluster_offsets_reader::ClusterOffsetsReader;
use crate::ops::groups::group_lag::{LagRequest, PartitionLag, collect_group_lag};
use crate::ops::offsets::offset_reset_plan::read_topic_partitions;
use crate::ops::poison::partition_message_reader::{FetchedRecord, PartitionMessageReader};

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PoisonStatus {
    /// The decoder rejected the value: the likely reason the group is stuck.
    Undecodable,
    Decodable,
    /// The committed offset is below the log start; the group will follow `auto.offset.reset`.
    BelowLogStart,
    /// No message arrived from the committed offset within the timeout.
    NoMessage,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PoisonCandidate {
    pub partition: i32,
    pub committed_offset: i64,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
    pub lag: i64,
    pub status: PoisonStatus,
    pub message_offset: Option<i64>,
    pub key_size: Option<usize>,
    pub value_size: Option<usize>,
    pub decode_error: Option<String>,
    pub envelope: Option<MessageEnvelope>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PoisonReport {
    pub group: String,
    pub topic: String,
    pub candidates: Vec<PoisonCandidate>,
    pub warnings: Vec<String>,
}

/// What is shown of each message; the verdict always uses the whole value.
pub struct PoisonFindOptions {
    pub max_bytes: usize,
    pub redact: Vec<String>,
}

/// Reads the message at the committed offset of every partition of `topic` where `group` has lag,
/// and tries to decode it. Reads only: nothing is committed.
pub fn find_poison_candidates(
    reader: &dyn ClusterOffsetsReader,
    messages: &dyn PartitionMessageReader,
    decoder: &dyn ValueDecoder,
    group: &str,
    topic: &str,
    options: &PoisonFindOptions,
) -> CoreResult<PoisonReport> {
    let report = collect_group_lag(
        reader,
        &LagRequest {
            group: Some(group.to_owned()),
            topic: Some(topic.to_owned()),
        },
    )?;
    let mut warnings = report.warnings;
    let group_lag = report
        .groups
        .into_iter()
        .next()
        .ok_or_else(|| CoreError::GroupNotFound(group.to_owned()))?;
    let mut lagging: Vec<(PartitionLag, i64)> = Vec::new();
    let mut committed_partitions = Vec::new();
    for partition in group_lag
        .topics
        .into_iter()
        .flat_map(|topic_lag| topic_lag.partitions)
    {
        let Some(committed) = partition.committed_offset else {
            continue;
        };
        committed_partitions.push(partition.partition);
        if partition.lag.is_some_and(|lag| lag > 0) {
            lagging.push((partition, committed));
        }
    }
    // The lag report leaves out partitions of an empty group that have no commit.
    for partition in read_topic_partitions(reader, topic)? {
        if !committed_partitions.contains(&partition) {
            warnings.push(format!(
                "{topic}/{partition} has no committed offset; the group starts it from its auto.offset.reset policy"
            ));
        }
    }
    lagging.sort_by_key(|(partition, _)| partition.partition);

    let to_read: BTreeMap<i32, i64> = lagging
        .iter()
        .filter(|(partition, committed)| *committed >= partition.log_start_offset)
        .map(|(partition, committed)| (partition.partition, *committed))
        .collect();
    let fetched = if to_read.is_empty() {
        BTreeMap::new()
    } else {
        messages.read_first_messages(topic, &to_read)?
    };

    let mut candidates = Vec::with_capacity(lagging.len());
    for (partition, committed) in lagging {
        let mut candidate = PoisonCandidate {
            partition: partition.partition,
            committed_offset: committed,
            log_start_offset: partition.log_start_offset,
            log_end_offset: partition.log_end_offset,
            lag: partition.lag.unwrap_or_default(),
            status: PoisonStatus::NoMessage,
            message_offset: None,
            key_size: None,
            value_size: None,
            decode_error: None,
            envelope: None,
        };
        if committed < partition.log_start_offset {
            candidate.status = PoisonStatus::BelowLogStart;
        } else if let Some(record) = fetched.get(&partition.partition) {
            if record.offset != committed {
                warnings.push(format!(
                    "{topic}/{}: no message at committed offset {committed}; the next one is {}",
                    partition.partition, record.offset
                ));
            }
            judge_record(&mut candidate, record, decoder, options);
        }
        candidates.push(candidate);
    }
    Ok(PoisonReport {
        group: group.to_owned(),
        topic: topic.to_owned(),
        candidates,
        warnings,
    })
}

fn judge_record(
    candidate: &mut PoisonCandidate,
    record: &FetchedRecord,
    decoder: &dyn ValueDecoder,
    options: &PoisonFindOptions,
) {
    candidate.message_offset = Some(record.offset);
    candidate.key_size = record.key.as_ref().map(Vec::len);
    candidate.value_size = record.value.as_ref().map(Vec::len);
    let verdict = match &record.value {
        // A tombstone has nothing to decode.
        None => Ok(()),
        Some(bytes) => decoder.decode_value(bytes, &record.headers).map(|_| ()),
    };
    match verdict {
        Ok(()) => candidate.status = PoisonStatus::Decodable,
        Err(error) => {
            candidate.status = PoisonStatus::Undecodable;
            candidate.decode_error = Some(error);
        }
    }
    let shown = build_envelope_value(
        record.value.as_deref(),
        &record.headers,
        decoder,
        options.max_bytes,
        &options.redact,
    );
    candidate.envelope = Some(MessageEnvelope::from_record(
        &record.topic,
        record.partition,
        record.offset,
        record.timestamp_millis,
        record.key.as_deref(),
        shown,
        &record.headers,
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::broker_capabilities::BrokerFamily;
    use crate::codec::raw_json_codec::RawJsonCodec;
    use crate::message::record_header::RecordHeader;
    use crate::ops::groups::fake_cluster_reader::FakeClusterReader;
    use crate::ops::groups::group_snapshot::{GroupDescription, PartitionWatermarks};
    use crate::ops::poison::fake_message_reader::FakePartitionMessageReader;
    use std::collections::BTreeMap;

    /// Fails when the value starts with 0xFF, decodes anything else to `{"ok": true}`.
    struct FailingOnMarker;

    impl ValueDecoder for FailingOnMarker {
        fn decode_value(
            &self,
            bytes: &[u8],
            _headers: &[RecordHeader],
        ) -> Result<serde_json::Value, String> {
            if bytes.first() == Some(&0xFF) {
                Err("value starts with 0xFF".to_owned())
            } else {
                Ok(serde_json::json!({"ok": true}))
            }
        }
    }

    /// Topic `t` with 2 partitions (log 0..10), empty group `g` committed at 5 on both.
    fn reader() -> FakeClusterReader {
        let mut reader = FakeClusterReader::new(BrokerFamily::GenericKafka);
        reader.topics.insert("t".to_owned(), vec![0, 1]);
        for partition in [0, 1] {
            reader.watermarks.insert(
                ("t".to_owned(), partition),
                PartitionWatermarks { low: 0, high: 10 },
            );
            reader
                .committed
                .insert(("g".to_owned(), "t".to_owned(), partition), 5);
        }
        reader.groups.push(GroupDescription {
            name: "g".to_owned(),
            state: "Empty".to_owned(),
            protocol_type: "consumer".to_owned(),
            members: Vec::new(),
        });
        reader
    }

    fn record(partition: i32, offset: i64, value: Vec<u8>) -> FetchedRecord {
        FetchedRecord {
            topic: "t".to_owned(),
            partition,
            offset,
            timestamp_millis: Some(1_795_000_000_000),
            key: Some(b"k".to_vec()),
            value: Some(value),
            headers: vec![RecordHeader {
                name: "trace".to_owned(),
                value: Some(b"1".to_vec()),
            }],
        }
    }

    fn messages(records: Vec<FetchedRecord>) -> FakePartitionMessageReader {
        let mut reader = FakePartitionMessageReader::new();
        for record in records {
            reader
                .records
                .entry((record.topic.clone(), record.partition))
                .or_default()
                .push(record);
        }
        reader
    }

    fn options() -> PoisonFindOptions {
        PoisonFindOptions {
            max_bytes: 2048,
            redact: Vec::new(),
        }
    }

    fn find(
        reader: &FakeClusterReader,
        messages: &FakePartitionMessageReader,
        decoder: &dyn ValueDecoder,
        options: &PoisonFindOptions,
    ) -> PoisonReport {
        find_poison_candidates(reader, messages, decoder, "g", "t", options).unwrap()
    }

    #[test]
    fn undecodable_message_at_committed_offset_is_reported() {
        let reader = reader();
        let messages = messages(vec![
            record(0, 5, vec![0xFF, 1, 2]),
            record(1, 5, b"{}".to_vec()),
        ]);

        let report = find(&reader, &messages, &FailingOnMarker, &options());

        let candidate = &report.candidates[0];
        assert_eq!(report.group, "g");
        assert_eq!(report.topic, "t");
        assert_eq!(candidate.partition, 0);
        assert_eq!(candidate.status, PoisonStatus::Undecodable);
        assert_eq!(candidate.committed_offset, 5);
        assert_eq!(candidate.lag, 5);
        assert_eq!(candidate.message_offset, Some(5));
        assert_eq!(candidate.value_size, Some(3));
        assert_eq!(candidate.key_size, Some(1));
        assert_eq!(
            candidate.decode_error.as_deref(),
            Some("value starts with 0xFF")
        );
        let envelope = candidate.envelope.as_ref().unwrap();
        assert_eq!(envelope.offset, Some(5));
        assert_eq!(envelope.headers.len(), 1);
    }

    #[test]
    fn decodable_message_is_not_poison() {
        let reader = reader();
        let messages = messages(vec![
            record(0, 5, b"{}".to_vec()),
            record(1, 5, b"{}".to_vec()),
        ]);

        let report = find(&reader, &messages, &FailingOnMarker, &options());

        assert!(
            report
                .candidates
                .iter()
                .all(|candidate| candidate.status == PoisonStatus::Decodable)
        );
        assert_eq!(report.candidates[1].decode_error, None);
    }

    #[test]
    fn tombstone_is_decodable() {
        let reader = reader();
        let mut tombstone = record(0, 5, Vec::new());
        tombstone.value = None;
        let messages = messages(vec![tombstone]);

        let report = find(&reader, &messages, &FailingOnMarker, &options());

        assert_eq!(report.candidates[0].status, PoisonStatus::Decodable);
        assert_eq!(report.candidates[0].value_size, None);
    }

    #[test]
    fn partition_without_lag_is_not_read() {
        let mut reader = reader();
        reader
            .committed
            .insert(("g".to_owned(), "t".to_owned(), 1), 10);
        let messages = messages(vec![record(0, 5, b"{}".to_vec())]);

        let report = find(&reader, &messages, &FailingOnMarker, &options());

        assert_eq!(report.candidates.len(), 1);
        assert_eq!(
            messages.reads(),
            vec![("t".to_owned(), BTreeMap::from([(0, 5)]))]
        );
    }

    #[test]
    fn committed_offset_below_log_start_is_reported_without_reading() {
        let mut reader = reader();
        reader.watermarks.insert(
            ("t".to_owned(), 1),
            PartitionWatermarks { low: 7, high: 10 },
        );
        let messages = messages(vec![record(0, 5, b"{}".to_vec())]);

        let report = find(&reader, &messages, &FailingOnMarker, &options());

        assert_eq!(report.candidates[1].status, PoisonStatus::BelowLogStart);
        assert_eq!(report.candidates[1].message_offset, None);
        assert_eq!(
            messages.reads(),
            vec![("t".to_owned(), BTreeMap::from([(0, 5)]))]
        );
    }

    #[test]
    fn missing_message_is_no_message() {
        let reader = reader();
        let messages = messages(vec![record(0, 5, b"{}".to_vec())]);

        let report = find(&reader, &messages, &FailingOnMarker, &options());

        assert_eq!(report.candidates[1].status, PoisonStatus::NoMessage);
        assert_eq!(report.candidates[1].envelope, None);
    }

    #[test]
    fn partition_without_commit_is_left_out_with_warning() {
        let mut reader = reader();
        reader
            .committed
            .remove(&("g".to_owned(), "t".to_owned(), 1));
        let messages = messages(vec![record(0, 5, b"{}".to_vec())]);

        let report = find(&reader, &messages, &FailingOnMarker, &options());

        assert_eq!(report.candidates.len(), 1);
        assert!(
            report.warnings.contains(
                &"t/1 has no committed offset; the group starts it from its auto.offset.reset policy"
                    .to_owned()
            ),
            "{:?}",
            report.warnings
        );
    }

    #[test]
    fn verdict_uses_full_value_even_when_display_is_truncated() {
        let reader = reader();
        let mut value = vec![0xFF];
        value.extend(std::iter::repeat_n(b'a', 4999));
        let messages = messages(vec![record(0, 5, value)]);

        let report = find(&reader, &messages, &FailingOnMarker, &options());

        let candidate = &report.candidates[0];
        assert_eq!(candidate.status, PoisonStatus::Undecodable);
        assert_eq!(candidate.value_size, Some(5000));
        assert!(candidate.envelope.as_ref().unwrap().value_truncated);
    }

    #[test]
    fn redact_withholds_undecodable_value_in_envelope() {
        let reader = reader();
        let messages = messages(vec![record(0, 5, vec![0xFF, 1])]);
        let options = PoisonFindOptions {
            max_bytes: 2048,
            redact: vec!["cpf".to_owned()],
        };

        let report = find(&reader, &messages, &FailingOnMarker, &options);

        let candidate = &report.candidates[0];
        assert_eq!(candidate.status, PoisonStatus::Undecodable);
        let envelope = candidate.envelope.as_ref().unwrap();
        assert_eq!(envelope.value_base64, None);
        assert!(envelope.decode_error.is_some());
    }

    #[test]
    fn decode_error_never_contains_payload() {
        let reader = reader();
        let messages = messages(vec![record(0, 5, b"segredo-123".to_vec())]);

        let report = find(&reader, &messages, &RawJsonCodec, &options());

        let error = report.candidates[0].decode_error.clone().unwrap();
        assert!(!error.contains("segredo-123"), "{error}");
    }

    #[test]
    fn gap_at_committed_offset_warns() {
        let reader = reader();
        let messages = messages(vec![record(0, 7, b"{}".to_vec())]);

        let report = find(&reader, &messages, &FailingOnMarker, &options());

        assert_eq!(report.candidates[0].message_offset, Some(7));
        assert!(
            report
                .warnings
                .contains(&"t/0: no message at committed offset 5; the next one is 7".to_owned()),
            "{:?}",
            report.warnings
        );
    }
}
