use crate::capability::capability_report::{
    CapabilityCheck, CapabilityName, CapabilityOutcome, CapabilityReport,
};
use crate::capability::error_classification::classify_kafka_error;
use crate::config::model::ConnectionContext;
use crate::connection::client_config_builder::ConnectionSettings;
use crate::connection::kafka_client_context::KafkaClientContext;
use crate::connection::kafka_connection::{KafkaConnection, build_kafka_connection};
use crate::connection::oauth_token_refresher::OAuthTokenRefresher;
use crate::error::{CoreError, CoreResult};
use crate::secret::resolver::SecretResolver;
use rdkafka::ClientConfig;
use rdkafka::client::{ClientContext, OAuthToken};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::{BorrowedMessage, DeliveryResult, Header, Headers, Message, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer, ProducerContext};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::sync::mpsc::{self, Sender};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const RUN_HEADER_NAME: &str = "kafcli-probe-run";
const PROBE_GROUP_PREFIX: &str = "kafcli-probe-";
const ACTIVE_GROUP_SUFFIX: &str = "-active";
const PROBE_MESSAGE_COUNT: usize = 3;
const PROBE_PARTITION: i32 = 0;
const TIMESTAMP_LOOKUP_SKEW_MILLIS: i64 = 1;
const POLL_SLICE: Duration = Duration::from_millis(100);

/// Target topic and the timeout applied to each network operation of one probe run.
#[derive(Clone, Debug)]
pub struct ProbeOptions {
    pub topic: String,
    pub timeout: Duration,
}

/// Offsets and send time of the messages written by `idempotent_produce`.
#[derive(Clone, Copy, Debug)]
struct ProducedBatch {
    first_offset: i64,
    send_timestamp_millis: i64,
}

struct TopicMetadataSummary {
    broker_count: usize,
    partition_count: usize,
}

type DeliveryOutcome = Result<i64, CapabilityOutcome>;

/// Forwards each delivery report to the probe, which reads them after flushing.
struct ProbeProducerContext {
    deliveries: Sender<DeliveryOutcome>,
    refresher: OAuthTokenRefresher,
}

impl ClientContext for ProbeProducerContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        self.refresher.generate_kafka_token()
    }
}

impl ProducerContext for ProbeProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, delivery_result: &DeliveryResult<'_>, _delivery_opaque: ()) {
        let outcome = match delivery_result {
            Ok(message) => Ok(message.offset()),
            Err((error, _message)) => Err(classify_kafka_error(error)),
        };
        // The receiver outlives the producer inside `produce_probe_messages`, so a failed send can only mean it was dropped early.
        let _ = self.deliveries.send(outcome);
    }
}

/// Runs every capability check against `options.topic` and returns the report.
///
/// The probe never creates or deletes topics. A missing topic or a failing cluster metadata
/// request aborts the run; any other failing check is recorded and the next one still runs.
pub fn probe_capabilities(
    context_name: &str,
    context: &ConnectionContext,
    resolver: &dyn SecretResolver,
    settings: &ConnectionSettings,
    options: &ProbeOptions,
) -> CoreResult<CapabilityReport> {
    let connection = build_kafka_connection(context, resolver, settings)?;
    let run_identifier = build_run_identifier(SystemTime::now());
    let session = ProbeSession {
        connection,
        topic: &options.topic,
        timeout: options.timeout,
        run_identifier: &run_identifier,
        probe_group: format!("{PROBE_GROUP_PREFIX}{run_identifier}"),
    };

    let metadata_started = Instant::now();
    let metadata = session.fetch_topic_metadata()?;
    let mut checks = vec![CapabilityCheck {
        name: CapabilityName::ClusterMetadata,
        outcome: CapabilityOutcome::Supported {
            detail: Some(format!(
                "{} brokers, {} partitions",
                metadata.broker_count, metadata.partition_count
            )),
        },
        elapsed_millis: measure_elapsed_millis(metadata_started),
    }];

    let mut batch = None;
    checks.push(run_check(CapabilityName::IdempotentProduce, || {
        let (outcome, produced) = session.produce_probe_messages();
        batch = produced;
        Ok(outcome)
    }));
    let first_offset = batch.map(|produced| produced.first_offset);

    checks.push(run_check(CapabilityName::HeadersRoundTrip, || {
        session.check_headers_round_trip(first_offset)
    }));
    checks.push(run_check(CapabilityName::ListOffsets, || {
        session.check_list_offsets()
    }));
    checks.push(run_check(CapabilityName::OffsetsForTimes, || {
        session.check_offsets_for_times(batch)
    }));
    checks.push(run_check(CapabilityName::CommitOffsetsOutsideGroup, || {
        session.check_commit_outside_group(first_offset)
    }));
    checks.push(run_check(CapabilityName::FetchCommittedOffsets, || {
        session.check_fetch_committed_offsets(first_offset)
    }));
    checks.push(run_check(CapabilityName::ListGroups, || {
        session.check_list_groups()
    }));
    checks.push(run_check(CapabilityName::DescribeGroup, || {
        session.check_describe_group()
    }));
    checks.push(run_check(
        CapabilityName::CommitRejectedWhileGroupActive,
        || {
            // The committed value does not matter here: the check is about group membership.
            session.check_commit_rejected_while_group_active(first_offset.unwrap_or(0))
        },
    ));

    Ok(CapabilityReport {
        context_name: context_name.to_string(),
        run_identifier: run_identifier.clone(),
        topic: options.topic.clone(),
        probe_group: session.probe_group.clone(),
        broker_count: metadata.broker_count,
        checks,
    })
}

/// Decides the outcome of committing to a group while another member is active.
///
/// A rejected commit is what a correct broker does, so it counts as supported. An accepted commit
/// means the broker does not enforce group membership on simple commits.
///
/// Only membership and generation rejections count as supported. Any other error (timeout,
/// authorization, unsupported version, unavailable coordinator) is classified like every other check.
fn classify_active_group_commit(result: KafkaResult<()>) -> CapabilityOutcome {
    match result {
        Ok(()) => CapabilityOutcome::Unsupported {
            error_code: "CommitAccepted".to_string(),
            detail: Some("commit accepted while group had an active member".to_string()),
        },
        Err(error) => match error.rdkafka_error_code() {
            Some(code) if is_membership_rejection(code) => CapabilityOutcome::Supported {
                detail: Some(format!("rejected with {code:?}")),
            },
            _ => classify_kafka_error(&error),
        },
    }
}

fn is_membership_rejection(code: RDKafkaErrorCode) -> bool {
    matches!(
        code,
        RDKafkaErrorCode::UnknownMemberId
            | RDKafkaErrorCode::IllegalGeneration
            | RDKafkaErrorCode::RebalanceInProgress
            | RDKafkaErrorCode::FencedInstanceId
    )
}

struct ProbeSession<'a> {
    connection: KafkaConnection,
    topic: &'a str,
    timeout: Duration,
    run_identifier: &'a str,
    probe_group: String,
}

impl ProbeSession<'_> {
    fn build_guarded_config(&self) -> ClientConfig {
        let mut config = self.connection.client_config().clone();
        // Missing topics must never be created by a metadata fetch, a consumer or a producer.
        config.set("allow.auto.create.topics", "false");
        config
    }

    fn build_consumer_config(&self, group_id: Option<&str>) -> ClientConfig {
        let mut config = self.build_guarded_config();
        config.set("enable.auto.commit", "false");
        if let Some(group) = group_id {
            config.set("group.id", group);
        }
        config
    }

    fn build_producer_config(&self) -> ClientConfig {
        let mut config = self.build_guarded_config();
        config.set("enable.idempotence", "true");
        config.set("acks", "all");
        config
    }

    fn fetch_topic_metadata(&self) -> CoreResult<TopicMetadataSummary> {
        let consumer = self
            .connection
            .create_consumer(self.build_consumer_config(None))?;
        let metadata = consumer
            .fetch_metadata(Some(self.topic), self.timeout)
            .map_err(map_kafka_error)?;
        let topic = metadata
            .topics()
            .iter()
            .find(|topic| topic.name() == self.topic && topic.error().is_none())
            .ok_or_else(|| CoreError::TopicNotFound(self.topic.to_string()))?;

        Ok(TopicMetadataSummary {
            broker_count: metadata.brokers().len(),
            partition_count: topic.partitions().len(),
        })
    }

    fn produce_probe_messages(&self) -> (CapabilityOutcome, Option<ProducedBatch>) {
        let (sender, receiver) = mpsc::channel();
        let refresher = self.connection.refresher().for_new_client();
        let producer: BaseProducer<ProbeProducerContext> = match self
            .build_producer_config()
            .create_with_context(ProbeProducerContext {
                deliveries: sender,
                refresher: refresher.clone(),
            }) {
            Ok(producer) => producer,
            Err(error) => return (classify_kafka_error(&error), None),
        };
        if let Err(error) = self
            .connection
            .wait_for_initial_token(&refresher, |timeout| {
                producer.poll(timeout);
            })
        {
            return (classify_kafka_error(&to_client_creation_error(error)), None);
        }

        let send_timestamp_millis = to_unix_millis(SystemTime::now());
        for index in 0..PROBE_MESSAGE_COUNT {
            let key = format!("probe-{index}");
            let payload = format!("kafcli probe message {index}");
            let headers = OwnedHeaders::new().insert(Header {
                key: RUN_HEADER_NAME,
                value: Some(self.run_identifier),
            });
            let record = BaseRecord::to(self.topic)
                .partition(PROBE_PARTITION)
                .key(key.as_str())
                .payload(payload.as_str())
                .timestamp(send_timestamp_millis)
                .headers(headers);
            if let Err((error, _record)) = producer.send(record) {
                return (classify_kafka_error(&error), None);
            }
        }

        if let Err(error) = producer.flush(self.timeout) {
            return (classify_kafka_error(&error), None);
        }

        let mut offsets = Vec::with_capacity(PROBE_MESSAGE_COUNT);
        for delivery in receiver.try_iter() {
            match delivery {
                Ok(offset) => offsets.push(offset),
                Err(failure) => return (failure, None),
            }
        }
        if offsets.len() != PROBE_MESSAGE_COUNT {
            let message = format!(
                "{} of {PROBE_MESSAGE_COUNT} messages were acknowledged",
                offsets.len()
            );
            return (CapabilityOutcome::Failed { message }, None);
        }

        let first_offset = offsets.iter().copied().fold(i64::MAX, i64::min);
        (
            CapabilityOutcome::Supported {
                detail: Some(format!(
                    "{PROBE_MESSAGE_COUNT} messages acknowledged in partition {PROBE_PARTITION}"
                )),
            },
            Some(ProducedBatch {
                first_offset,
                send_timestamp_millis,
            }),
        )
    }

    fn check_headers_round_trip(
        &self,
        first_offset: Option<i64>,
    ) -> KafkaResult<CapabilityOutcome> {
        let Some(first_offset) = first_offset else {
            return Ok(build_missing_batch_outcome());
        };
        // librdkafka rejects `assign` without a group id, so the probe group is used. It only
        // assigns here and never commits, so no offset is written for it.
        let consumer = create_probe_consumer(
            &self.connection,
            self.build_consumer_config(Some(self.probe_group.as_str())),
        )?;
        consumer.assign(&build_partition_list(self.topic, first_offset)?)?;

        let deadline = Instant::now() + self.timeout;
        let mut read = 0;
        let mut carrying_header = 0;
        while read < PROBE_MESSAGE_COUNT && Instant::now() < deadline {
            let Some(received) = consumer.poll(POLL_SLICE) else {
                continue;
            };
            let message = received?;
            read += 1;
            if carries_run_header(&message, self.run_identifier) {
                carrying_header += 1;
            }
        }

        if carrying_header == PROBE_MESSAGE_COUNT {
            return Ok(CapabilityOutcome::Supported {
                detail: Some(format!(
                    "{carrying_header} messages read back with the run header"
                )),
            });
        }
        Ok(CapabilityOutcome::Failed {
            message: format!(
                "{carrying_header} of {PROBE_MESSAGE_COUNT} messages carried the run header ({read} read)"
            ),
        })
    }

    fn check_list_offsets(&self) -> KafkaResult<CapabilityOutcome> {
        let consumer = create_probe_consumer(&self.connection, self.build_consumer_config(None))?;
        let (low, high) = consumer.fetch_watermarks(self.topic, PROBE_PARTITION, self.timeout)?;
        Ok(CapabilityOutcome::Supported {
            detail: Some(format!("low={low} high={high}")),
        })
    }

    fn check_offsets_for_times(
        &self,
        batch: Option<ProducedBatch>,
    ) -> KafkaResult<CapabilityOutcome> {
        let Some(batch) = batch else {
            return Ok(build_missing_batch_outcome());
        };
        let consumer = create_probe_consumer(&self.connection, self.build_consumer_config(None))?;
        let lookup_millis = batch.send_timestamp_millis - TIMESTAMP_LOOKUP_SKEW_MILLIS;
        let mut query = TopicPartitionList::new();
        query.add_partition_offset(self.topic, PROBE_PARTITION, Offset::Offset(lookup_millis))?;

        let found = consumer.offsets_for_times(query, self.timeout)?;
        let returned = found
            .find_partition(self.topic, PROBE_PARTITION)
            .map(|element| element.offset());
        if returned == Some(Offset::Offset(batch.first_offset)) {
            return Ok(CapabilityOutcome::Supported {
                detail: Some(format!("offset {}", batch.first_offset)),
            });
        }
        Ok(CapabilityOutcome::Failed {
            message: format!(
                "expected offset {} for timestamp {lookup_millis}, got {returned:?}",
                batch.first_offset
            ),
        })
    }

    fn check_commit_outside_group(
        &self,
        first_offset: Option<i64>,
    ) -> KafkaResult<CapabilityOutcome> {
        let Some(first_offset) = first_offset else {
            return Ok(build_missing_batch_outcome());
        };
        let committed_offset = first_offset + 1;
        Ok(
            match self.commit_within_timeout(
                self.probe_group.as_str(),
                first_offset,
                committed_offset,
            ) {
                None => self.commit_timeout_outcome(),
                Some(Ok(())) => CapabilityOutcome::Supported {
                    detail: Some(format!("committed offset {committed_offset}")),
                },
                Some(Err(error)) => classify_kafka_error(&error),
            },
        )
    }

    fn check_fetch_committed_offsets(
        &self,
        first_offset: Option<i64>,
    ) -> KafkaResult<CapabilityOutcome> {
        let Some(first_offset) = first_offset else {
            return Ok(build_missing_batch_outcome());
        };
        let consumer = create_probe_consumer(
            &self.connection,
            self.build_consumer_config(Some(self.probe_group.as_str())),
        )?;
        let committed = consumer.committed_offsets(
            build_partition_list(self.topic, first_offset)?,
            self.timeout,
        )?;
        let expected = first_offset + 1;
        let returned = committed
            .find_partition(self.topic, PROBE_PARTITION)
            .map(|element| element.offset());
        if returned == Some(Offset::Offset(expected)) {
            return Ok(CapabilityOutcome::Supported {
                detail: Some(format!("committed offset {expected}")),
            });
        }
        Ok(CapabilityOutcome::Failed {
            message: format!("expected committed offset {expected}, got {returned:?}"),
        })
    }

    fn check_list_groups(&self) -> KafkaResult<CapabilityOutcome> {
        let consumer = create_probe_consumer(&self.connection, self.build_consumer_config(None))?;
        let groups = consumer.fetch_group_list(None, self.timeout)?;
        let listed = groups
            .groups()
            .iter()
            .any(|group| group.name() == self.probe_group);
        let answer = if listed { "yes" } else { "no" };
        Ok(CapabilityOutcome::Supported {
            detail: Some(format!("probe group listed: {answer}")),
        })
    }

    fn check_describe_group(&self) -> KafkaResult<CapabilityOutcome> {
        let consumer = create_probe_consumer(&self.connection, self.build_consumer_config(None))?;
        let groups = consumer.fetch_group_list(Some(self.probe_group.as_str()), self.timeout)?;
        let outcome = match groups
            .groups()
            .iter()
            .find(|group| group.name() == self.probe_group)
        {
            Some(group) => CapabilityOutcome::Supported {
                detail: Some(format!(
                    "state={} members={}",
                    group.state(),
                    group.members().len()
                )),
            },
            None => CapabilityOutcome::Failed {
                message: "probe group not found".to_string(),
            },
        };
        Ok(outcome)
    }

    fn check_commit_rejected_while_group_active(
        &self,
        committed_offset: i64,
    ) -> KafkaResult<CapabilityOutcome> {
        let active_group = format!("{}{ACTIVE_GROUP_SUFFIX}", self.probe_group);
        let member = create_probe_consumer(
            &self.connection,
            self.build_consumer_config(Some(active_group.as_str())),
        )?;
        member.subscribe(&[self.topic])?;
        if !self.wait_for_assignment(&member)? {
            return Ok(CapabilityOutcome::Failed {
                message: "active member never joined".to_string(),
            });
        }

        Ok(
            match self.commit_within_timeout(
                active_group.as_str(),
                committed_offset,
                committed_offset,
            ) {
                None => self.commit_timeout_outcome(),
                Some(result) => classify_active_group_commit(result),
            },
        )
    }

    /// Runs a sync commit on a worker thread and waits at most `self.timeout` for it.
    ///
    /// Returns `None` when the commit did not finish in time. Async commit replies are served by
    /// the client's main queue, which `BaseConsumer::poll` does not service, so a callback-based wait
    /// cannot complete. The worker thread may outlive the probe until the socket timeout.
    fn commit_within_timeout(
        &self,
        group_id: &str,
        assigned_offset: i64,
        committed_offset: i64,
    ) -> Option<KafkaResult<()>> {
        let config = self.build_consumer_config(Some(group_id));
        let connection = self.connection.clone();
        let topic = self.topic.to_string();
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let outcome = commit_offset_in_group(
                &connection,
                config,
                &topic,
                assigned_offset,
                committed_offset,
            );
            // The receiver stops waiting after the timeout, so a failed send is harmless.
            let _ = sender.send(outcome);
        });
        receiver.recv_timeout(self.timeout).ok()
    }

    fn commit_timeout_outcome(&self) -> CapabilityOutcome {
        CapabilityOutcome::Failed {
            message: format!(
                "commit did not complete within the timeout of {:?}",
                self.timeout
            ),
        }
    }

    /// Polls `member` until it holds a partition, returning `false` when the timeout ends first.
    fn wait_for_assignment(&self, member: &BaseConsumer<KafkaClientContext>) -> KafkaResult<bool> {
        let deadline = Instant::now() + self.timeout;
        while Instant::now() < deadline {
            if let Some(received) = member.poll(POLL_SLICE) {
                received?;
            }
            if matches!(member.assignment(), Ok(list) if list.count() > 0) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// Creates a consumer in `config`, assigns `assigned_offset` and commits `committed_offset` synchronously.
fn commit_offset_in_group(
    connection: &KafkaConnection,
    config: ClientConfig,
    topic: &str,
    assigned_offset: i64,
    committed_offset: i64,
) -> KafkaResult<()> {
    let consumer = create_probe_consumer(connection, config)?;
    consumer.assign(&build_partition_list(topic, assigned_offset)?)?;
    consumer.commit(
        &build_partition_list(topic, committed_offset)?,
        CommitMode::Sync,
    )
}

/// Creates a consumer with the connection's refresher and waits for its first token. The
/// `KafkaError` type is kept so that error codes still classify the check.
fn create_probe_consumer(
    connection: &KafkaConnection,
    config: ClientConfig,
) -> KafkaResult<BaseConsumer<KafkaClientContext>> {
    let refresher = connection.refresher().for_new_client();
    let consumer: BaseConsumer<KafkaClientContext> =
        config.create_with_context(KafkaClientContext::new(refresher.clone()))?;
    connection
        .wait_for_initial_token(&refresher, |timeout| {
            consumer.poll(timeout);
        })
        .map_err(to_client_creation_error)?;
    Ok(consumer)
}

fn to_client_creation_error(error: CoreError) -> KafkaError {
    KafkaError::ClientCreation(error.to_string())
}

/// Runs one check, turning a Kafka error into the outcome for that check.
fn run_check(
    name: CapabilityName,
    step: impl FnOnce() -> KafkaResult<CapabilityOutcome>,
) -> CapabilityCheck {
    let started = Instant::now();
    let outcome = step().unwrap_or_else(|error| classify_kafka_error(&error));
    CapabilityCheck {
        name,
        outcome,
        elapsed_millis: measure_elapsed_millis(started),
    }
}

fn build_missing_batch_outcome() -> CapabilityOutcome {
    CapabilityOutcome::Failed {
        message: "skipped: idempotent_produce did not acknowledge probe messages".to_string(),
    }
}

fn build_partition_list(topic: &str, offset: i64) -> KafkaResult<TopicPartitionList> {
    let mut list = TopicPartitionList::new();
    list.add_partition_offset(topic, PROBE_PARTITION, Offset::Offset(offset))?;
    Ok(list)
}

fn carries_run_header(message: &BorrowedMessage<'_>, run_identifier: &str) -> bool {
    message.headers().is_some_and(|headers| {
        headers.iter().any(|header| {
            header.key == RUN_HEADER_NAME && header.value == Some(run_identifier.as_bytes())
        })
    })
}

fn build_run_identifier(now: SystemTime) -> String {
    to_unix_millis(now).to_string()
}

fn to_unix_millis(instant: SystemTime) -> i64 {
    instant
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}

fn measure_elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn map_kafka_error(error: KafkaError) -> CoreError {
    CoreError::Kafka(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::{AuthConfig, ConnectionContext};
    use crate::connection::client_config_builder::ConnectionSettings;
    use crate::error::CoreError;
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use rdkafka::error::RDKafkaErrorCode;
    use std::time::{Duration, Instant, UNIX_EPOCH};

    #[test]
    fn accepted_commit_with_active_member_is_unsupported() {
        let outcome = classify_active_group_commit(Ok(()));

        assert_eq!(
            outcome,
            CapabilityOutcome::Unsupported {
                error_code: "CommitAccepted".to_string(),
                detail: Some("commit accepted while group had an active member".to_string()),
            }
        );
    }

    #[test]
    fn illegal_generation_commit_is_supported() {
        let outcome = classify_active_group_commit(Err(KafkaError::ConsumerCommit(
            RDKafkaErrorCode::IllegalGeneration,
        )));

        match outcome {
            CapabilityOutcome::Supported {
                detail: Some(detail),
            } => assert!(detail.contains("IllegalGeneration"), "detail: {detail}"),
            other => panic!("expected supported, got {other:?}"),
        }
    }

    #[test]
    fn authorization_failure_on_commit_is_failed() {
        let outcome = classify_active_group_commit(Err(KafkaError::ConsumerCommit(
            RDKafkaErrorCode::GroupAuthorizationFailed,
        )));

        assert!(
            matches!(outcome, CapabilityOutcome::Failed { .. }),
            "{outcome:?}"
        );
    }

    #[test]
    fn unsupported_version_on_commit_is_unsupported() {
        let outcome = classify_active_group_commit(Err(KafkaError::ConsumerCommit(
            RDKafkaErrorCode::UnsupportedVersion,
        )));

        assert_eq!(
            outcome,
            CapabilityOutcome::Unsupported {
                error_code: "UnsupportedVersion".to_string(),
                detail: None,
            }
        );
    }

    #[test]
    fn rejected_commit_with_active_member_is_supported() {
        let outcome = classify_active_group_commit(Err(KafkaError::ConsumerCommit(
            RDKafkaErrorCode::UnknownMemberId,
        )));

        match outcome {
            CapabilityOutcome::Supported {
                detail: Some(detail),
            } => assert!(detail.contains("UnknownMemberId"), "detail: {detail}"),
            other => panic!("expected supported, got {other:?}"),
        }
    }

    #[test]
    fn run_identifier_is_unix_millis_in_decimal() {
        let identifier =
            build_run_identifier(UNIX_EPOCH + Duration::from_millis(1_700_000_000_123));

        assert_eq!(identifier, "1700000000123");
    }

    #[test]
    fn unreachable_broker_fails_within_timeout() {
        let context = ConnectionContext {
            brokers: vec!["127.0.0.1:1".to_string()],
            auth: AuthConfig::Plaintext,
            protected: false,
            schema_registry: None,
        };
        let options = ProbeOptions {
            topic: "unused-topic".to_string(),
            timeout: Duration::from_secs(2),
        };
        let started = Instant::now();

        let result = probe_capabilities(
            "local",
            &context,
            &FixedSecretResolver::returning("unused"),
            &ConnectionSettings { insecure: false },
            &options,
        );

        assert!(matches!(result, Err(CoreError::Kafka(_))));
        assert!(started.elapsed() < Duration::from_secs(10));
    }
}
