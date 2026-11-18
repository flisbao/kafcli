//! `GroupOffsetCommitter` backed by a librdkafka consumer that assigns and commits, never joining.

use std::collections::BTreeMap;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use crate::capability::broker_capabilities::BrokerFamily;
use crate::capability::broker_operation::{BrokerOperation, map_broker_operation_error};
use crate::connection::kafka_connection::KafkaConnection;
use crate::error::{CoreError, CoreResult};
use crate::ops::groups::kafka_cluster_reader::build_read_only_config;
use crate::ops::offsets::group_offset_committer::GroupOffsetCommitter;

/// Commits offsets with a consumer that carries the group's id, assigns the partitions and runs a
/// synchronous commit. It is the only client in kafcli that commits for another group.
pub struct KafkaOffsetCommitter {
    connection: KafkaConnection,
    family: BrokerFamily,
    timeout: Duration,
}

impl KafkaOffsetCommitter {
    pub fn new(connection: KafkaConnection, family: BrokerFamily, timeout: Duration) -> Self {
        Self {
            connection,
            family,
            timeout,
        }
    }
}

/// Failure of the commit itself, kept apart so the broker's code can still be classified.
enum CommitFailure {
    Client(CoreError),
    Broker(KafkaError),
}

impl GroupOffsetCommitter for KafkaOffsetCommitter {
    fn commit_group_offsets(
        &self,
        group: &str,
        offsets: &BTreeMap<(String, i32), i64>,
    ) -> CoreResult<()> {
        if offsets.values().any(|offset| *offset < 0) {
            return Err(CoreError::InvalidInput(
                "offset must be zero or positive".to_owned(),
            ));
        }
        let connection = self.connection.clone();
        let group_id = group.to_owned();
        let offsets = offsets.clone();
        let (sender, receiver) = mpsc::channel();
        // A sync commit can block past any timeout librdkafka offers, so it runs on a worker and
        // the caller waits at most `timeout`. The worker may outlive the call until its socket
        // times out.
        thread::spawn(move || {
            let outcome = commit_on_worker(&connection, &group_id, &offsets);
            // The receiver stops waiting after the timeout, so a failed send is harmless.
            let _ = sender.send(outcome);
        });
        match receiver.recv_timeout(self.timeout) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(CommitFailure::Client(error))) => Err(error),
            Ok(Err(CommitFailure::Broker(error))) => Err(map_broker_operation_error(
                BrokerOperation::OffsetCommit,
                self.family,
                Some(group),
                &error,
            )),
            Err(_) => Err(CoreError::Kafka(format!(
                "offset commit did not complete within {}s; the outcome is unknown, check with 'kafcli offsets show'",
                self.timeout.as_secs()
            ))),
        }
    }
}

fn commit_on_worker(
    connection: &KafkaConnection,
    group: &str,
    offsets: &BTreeMap<(String, i32), i64>,
) -> Result<(), CommitFailure> {
    let mut config = build_read_only_config(connection);
    config.set("group.id", group);
    let consumer = connection
        .create_consumer(config)
        .map_err(CommitFailure::Client)?;
    let list = build_commit_list(offsets).map_err(CommitFailure::Broker)?;
    consumer.assign(&list).map_err(CommitFailure::Broker)?;
    consumer
        .commit(&list, CommitMode::Sync)
        .map_err(CommitFailure::Broker)
}

fn build_commit_list(
    offsets: &BTreeMap<(String, i32), i64>,
) -> Result<TopicPartitionList, KafkaError> {
    let mut list = TopicPartitionList::new();
    for ((topic, partition), offset) in offsets {
        list.add_partition_offset(topic, *partition, Offset::Offset(*offset))?;
    }
    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::model::ConnectionContext;
    use crate::connection::client_config_builder::ConnectionSettings;
    use crate::connection::kafka_connection::build_kafka_connection;
    use crate::secret::fixed_resolver::FixedSecretResolver;
    use std::time::Instant;

    fn unreachable_connection() -> KafkaConnection {
        let context: ConnectionContext = serde_json::from_value(serde_json::json!({
            "brokers": ["127.0.0.1:1"],
            "auth": {"type": "none"},
        }))
        .unwrap();
        build_kafka_connection(
            &context,
            &FixedSecretResolver::returning("unused"),
            &ConnectionSettings { insecure: false },
        )
        .unwrap()
    }

    #[test]
    fn negative_offset_is_refused_before_connecting() {
        let committer = KafkaOffsetCommitter::new(
            unreachable_connection(),
            BrokerFamily::GenericKafka,
            Duration::from_secs(30),
        );
        let started = Instant::now();

        let result = committer.commit_group_offsets(
            "g",
            &BTreeMap::from([
                (("pedidos".to_owned(), 0), 4),
                (("pedidos".to_owned(), 1), -1),
            ]),
        );

        assert!(
            matches!(&result, Err(CoreError::InvalidInput(text)) if text == "offset must be zero or positive"),
            "{result:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
