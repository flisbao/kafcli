//! `ClusterOffsetsReader` backed by librdkafka consumers that never join, assign or commit.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::groups::GroupMemberInfo;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use crate::capability::broker_capabilities::{BrokerFamily, detect_broker_family};
use crate::capability::broker_operation::{BrokerOperation, map_broker_operation_error};
use crate::capability::error_classification::classify_topic_metadata_error;
use crate::config::model::ConnectionContext;
use crate::connection::client_config_builder::ConnectionSettings;
use crate::connection::kafka_client_context::KafkaClientContext;
use crate::connection::kafka_connection::{KafkaConnection, build_kafka_connection};
use crate::error::{CoreError, CoreResult};
use crate::ops::groups::cluster_offsets_reader::ClusterOffsetsReader;
use crate::ops::groups::consumer_protocol::{decode_assignment, decode_subscription};
use crate::ops::groups::group_snapshot::{GroupDescription, GroupMember, PartitionWatermarks};
use crate::secret::resolver::SecretResolver;

/// Protocol type of groups whose member metadata follows the consumer protocol.
const CONSUMER_PROTOCOL_TYPE: &str = "consumer";
/// State `DescribeGroups` reports for a group that does not exist.
const DEAD_GROUP_STATE: &str = "Dead";

/// Reads the cluster through one metadata consumer, plus one short-lived consumer per group for
/// its committed offsets.
pub struct KafkaClusterReader {
    connection: KafkaConnection,
    metadata_consumer: BaseConsumer<KafkaClientContext>,
    family: BrokerFamily,
    timeout: Duration,
}

impl KafkaClusterReader {
    pub fn connect(
        context: &ConnectionContext,
        resolver: &dyn SecretResolver,
        settings: &ConnectionSettings,
        timeout: Duration,
    ) -> CoreResult<Self> {
        let connection = build_kafka_connection(context, resolver, settings)?;
        let metadata_consumer = connection.create_consumer(&build_read_only_config(&connection))?;
        Ok(Self {
            connection,
            metadata_consumer,
            family: detect_broker_family(context),
            timeout,
        })
    }

    /// The connection the reader was built on, for the clients that write (offset commits).
    pub fn connection(&self) -> &KafkaConnection {
        &self.connection
    }

    pub fn family(&self) -> BrokerFamily {
        self.family
    }

    fn map_error(
        &self,
        operation: BrokerOperation,
        group: Option<&str>,
        error: &KafkaError,
    ) -> CoreError {
        map_broker_operation_error(operation, self.family, group, error)
    }
}

/// Settings every offset-reading consumer carries: nothing is ever committed, stored or created.
pub(crate) fn build_read_only_config(connection: &KafkaConnection) -> ClientConfig {
    let mut config = connection.client_config().clone();
    config.set("enable.auto.commit", "false");
    config.set("enable.auto.offset.store", "false");
    config.set("allow.auto.create.topics", "false");
    config
}

impl ClusterOffsetsReader for KafkaClusterReader {
    fn describe_groups(&self, group: Option<&str>) -> CoreResult<Vec<GroupDescription>> {
        let operation = match group {
            Some(_) => BrokerOperation::DescribeGroups,
            None => BrokerOperation::ListGroups,
        };
        let list = self
            .metadata_consumer
            .fetch_group_list(group, self.timeout)
            .map_err(|error| self.map_error(operation, group, &error))?;
        let groups = list
            .groups()
            .iter()
            .filter(|info| !(info.state() == DEAD_GROUP_STATE && info.members().is_empty()))
            .map(|info| {
                let decodes_members = info.protocol_type() == CONSUMER_PROTOCOL_TYPE;
                GroupDescription {
                    name: info.name().to_owned(),
                    state: info.state().to_owned(),
                    protocol_type: info.protocol_type().to_owned(),
                    members: info
                        .members()
                        .iter()
                        .map(|member| describe_member(info.name(), member, decodes_members))
                        .collect(),
                }
            })
            .collect();
        Ok(keep_most_complete_answer_of_each_group(groups))
    }

    fn list_topic_partitions(&self, topic: Option<&str>) -> CoreResult<BTreeMap<String, Vec<i32>>> {
        let metadata = self
            .metadata_consumer
            .fetch_metadata(topic, self.timeout)
            .map_err(|error| CoreError::Kafka(error.to_string()))?;
        let mut partitions = BTreeMap::new();
        for topic_metadata in metadata.topics() {
            let name = topic_metadata.name();
            if let Some(error) = topic_metadata.error() {
                if topic.is_some() {
                    return Err(classify_topic_metadata_error(name, error));
                }
                continue;
            }
            if topic.is_none() && name.starts_with("__") {
                continue;
            }
            let mut identifiers: Vec<i32> = topic_metadata
                .partitions()
                .iter()
                .map(|partition| partition.id())
                .collect();
            identifiers.sort_unstable();
            partitions.insert(name.to_owned(), identifiers);
        }
        if let Some(name) = topic
            && !partitions.contains_key(name)
        {
            return Err(CoreError::TopicNotFound(name.to_owned()));
        }
        Ok(partitions)
    }

    fn fetch_committed_offsets(
        &self,
        group: &str,
        partitions: &BTreeMap<String, Vec<i32>>,
    ) -> CoreResult<BTreeMap<(String, i32), Option<i64>>> {
        let mut list = TopicPartitionList::new();
        for (topic, topic_partitions) in partitions {
            for partition in topic_partitions {
                list.add_partition(topic, *partition);
            }
        }
        let mut config = build_read_only_config(&self.connection);
        // The group id only selects whose offsets OffsetFetch returns; the consumer never joins.
        config.set("group.id", group);
        let consumer = self.connection.create_consumer(&config)?;
        let committed = consumer
            .committed_offsets(list, self.timeout)
            .map_err(|error| self.map_error(BrokerOperation::OffsetFetch, Some(group), &error))?;
        drop(consumer);
        let mut offsets = BTreeMap::new();
        for element in committed.elements() {
            // A partition-level error is not "no commit": reading it as None would hide an offset.
            if let Err(error) = element.error() {
                return Err(self.map_error(BrokerOperation::OffsetFetch, Some(group), &error));
            }
            let offset = match element.offset() {
                Offset::Offset(value) if value >= 0 => Some(value),
                _ => None,
            };
            offsets.insert((element.topic().to_owned(), element.partition()), offset);
        }
        Ok(offsets)
    }

    fn fetch_watermarks(&self, topic: &str, partition: i32) -> CoreResult<PartitionWatermarks> {
        let (low, high) = self
            .metadata_consumer
            .fetch_watermarks(topic, partition, self.timeout)
            .map_err(|error| self.map_error(BrokerOperation::ListOffsets, None, &error))?;
        Ok(PartitionWatermarks { low, high })
    }

    fn find_offsets_for_time(
        &self,
        topic: &str,
        partitions: &[i32],
        timestamp_millis: i64,
    ) -> CoreResult<BTreeMap<i32, Option<i64>>> {
        let mut list = TopicPartitionList::new();
        for partition in partitions {
            list.add_partition_offset(topic, *partition, Offset::Offset(timestamp_millis))
                .map_err(|error| self.map_error(BrokerOperation::OffsetsForTimes, None, &error))?;
        }
        let found = self
            .metadata_consumer
            .offsets_for_times(list, self.timeout)
            .map_err(|error| self.map_error(BrokerOperation::OffsetsForTimes, None, &error))?;
        let mut offsets = BTreeMap::new();
        for element in found.elements() {
            if let Err(error) = element.error() {
                return Err(self.map_error(BrokerOperation::OffsetsForTimes, None, &error));
            }
            // `Offset::End` means no message at or after the timestamp.
            let offset = match element.offset() {
                Offset::Offset(value) if value >= 0 => Some(value),
                _ => None,
            };
            offsets.insert(element.partition(), offset);
        }
        Ok(offsets)
    }

    /// Waits `interval` while polling the metadata consumer, so a long `--watch` keeps serving its
    /// OAUTHBEARER token refreshes; librdkafka only hands them out from `poll`. The consumer has no
    /// assignment, so a poll returns no message.
    fn wait_serving_client_events(&self, interval: Duration) {
        wait_serving_client_events(interval, Instant::now, |slice| {
            let _ = self.metadata_consumer.poll(slice);
        });
    }

    fn broker_family(&self) -> BrokerFamily {
        self.family
    }
}

/// Keeps one answer per group name, in the order the names first appear.
///
/// librdkafka asks every broker for its groups. Azure Event Hubs advertises several brokers that
/// each return the whole namespace, so without this every group would appear once per broker. The
/// answers can disagree (one broker may still see the group `Empty`), so the one with the most
/// members wins, and the first of those on a tie: reading an active group as empty would weaken
/// the checks that guard offset resets.
fn keep_most_complete_answer_of_each_group(groups: Vec<GroupDescription>) -> Vec<GroupDescription> {
    let mut kept: Vec<GroupDescription> = Vec::with_capacity(groups.len());
    let mut index_by_name: BTreeMap<String, usize> = BTreeMap::new();
    for group in groups {
        match index_by_name.get(&group.name) {
            Some(&index) => {
                if group.members.len() > kept[index].members.len() {
                    kept[index] = group;
                }
            }
            None => {
                index_by_name.insert(group.name.clone(), kept.len());
                kept.push(group);
            }
        }
    }
    kept
}

/// Decodes one member. Unreadable metadata leaves the field `None` and logs a warning that names
/// the member but never carries the bytes.
fn describe_member(group: &str, member: &GroupMemberInfo, decodes_members: bool) -> GroupMember {
    let readable =
        |bytes: Option<&[u8]>| bytes.is_some_and(|bytes| !bytes.is_empty()) && decodes_members;
    let subscription = member
        .metadata()
        .filter(|_| readable(member.metadata()))
        .and_then(|bytes| {
            decode_subscription(bytes)
                .inspect_err(|error| warn_undecodable(group, member.id(), "subscription", error))
                .ok()
        });
    let assignment = member
        .assignment()
        .filter(|_| readable(member.assignment()))
        .and_then(|bytes| {
            decode_assignment(bytes)
                .inspect_err(|error| warn_undecodable(group, member.id(), "assignment", error))
                .ok()
        });
    GroupMember {
        member_id: member.id().to_owned(),
        client_id: member.client_id().to_owned(),
        host: member.client_host().to_owned(),
        subscription,
        assignment,
    }
}

fn warn_undecodable(group: &str, member_id: &str, field: &str, error: &CoreError) {
    tracing::warn!(group, member_id, field, %error, "group member metadata could not be decoded");
}

/// Longest single poll while waiting, so the wait ends close to its deadline.
const EVENT_POLL_SLICE: Duration = Duration::from_secs(1);

/// Waits `interval` by calling `poll_once` with slices of at most `EVENT_POLL_SLICE` until `now`
/// passes the deadline. A poll returns early when the client has an event, so the deadline is read
/// from the clock and not from the sum of the slices.
pub(crate) fn wait_serving_client_events(
    interval: Duration,
    now: impl Fn() -> Instant,
    mut poll_once: impl FnMut(Duration),
) {
    let deadline = now() + interval;
    loop {
        let remaining = deadline.saturating_duration_since(now());
        if remaining.is_zero() {
            return;
        }
        poll_once(remaining.min(EVENT_POLL_SLICE));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    #[test]
    fn wait_polls_in_slices_until_the_deadline() {
        let start = Instant::now();
        let clock = Cell::new(start);
        let polls = RefCell::new(Vec::new());

        wait_serving_client_events(
            Duration::from_millis(2500),
            || clock.get(),
            |slice| {
                polls.borrow_mut().push(slice);
                clock.set(clock.get() + slice);
            },
        );

        assert_eq!(
            polls.into_inner(),
            vec![
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_millis(500)
            ]
        );
    }

    #[test]
    fn early_returning_polls_keep_waiting() {
        let start = Instant::now();
        let clock = Cell::new(start);
        let polls = Cell::new(0);

        wait_serving_client_events(
            Duration::from_secs(1),
            || clock.get(),
            |_| {
                polls.set(polls.get() + 1);
                clock.set(clock.get() + Duration::from_millis(250));
            },
        );

        assert_eq!(polls.get(), 4);
        assert!(clock.get() >= start + Duration::from_secs(1));
    }

    fn description(name: &str, state: &str) -> GroupDescription {
        GroupDescription {
            name: name.to_owned(),
            state: state.to_owned(),
            protocol_type: "consumer".to_owned(),
            members: Vec::new(),
        }
    }

    #[test]
    fn groups_listed_by_several_brokers_are_kept_once() {
        // Azure Event Hubs answers ListGroups on every advertised broker with the whole list.
        let listed = vec![
            description("b", "Stable"),
            description("a", "Stable"),
            description("b", "Empty"),
            description("a", "Stable"),
            description("b", "Stable"),
        ];

        let kept = keep_most_complete_answer_of_each_group(listed);

        let kept: Vec<(&str, &str)> = kept
            .iter()
            .map(|group| (group.name.as_str(), group.state.as_str()))
            .collect();
        assert_eq!(kept, vec![("b", "Stable"), ("a", "Stable")]);
    }

    #[test]
    fn repeated_group_keeps_the_answer_with_members() {
        let member = GroupMember {
            member_id: "consumer-1-abc".to_owned(),
            client_id: "consumer-1".to_owned(),
            host: "/10.0.0.7".to_owned(),
            subscription: None,
            assignment: None,
        };
        let mut active = description("a", "Stable");
        active.members = vec![member.clone(), member];
        let listed = vec![description("a", "Empty"), active, description("a", "Empty")];

        let kept = keep_most_complete_answer_of_each_group(listed);

        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].state, "Stable");
        assert_eq!(kept[0].members.len(), 2);
    }
}
