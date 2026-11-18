//! Writing the copy of a skipped message to its dead-letter topic.

use std::time::Duration;

use crate::config::model::ConnectionContext;
use crate::connection::client_config_builder::ConnectionSettings;
use crate::error::{CoreError, CoreResult};
use crate::ops::produce::message_producer::{
    DeliveryReport, PreparedMessage, ProduceOptions, produce_messages,
};
use crate::secret::resolver::SecretResolver;

/// Publishes one message and returns where the broker stored it.
pub trait DeadLetterPublisher {
    fn publish_copy(&self, topic: &str, message: PreparedMessage) -> CoreResult<DeliveryReport>;
}

/// Publishes through `produce_messages`: one message, no templates, partition chosen by the key.
pub struct KafkaDeadLetterPublisher<'a> {
    context: ConnectionContext,
    resolver: &'a dyn SecretResolver,
    settings: ConnectionSettings,
    timeout: Duration,
}

impl<'a> KafkaDeadLetterPublisher<'a> {
    pub fn new(
        context: ConnectionContext,
        resolver: &'a dyn SecretResolver,
        settings: ConnectionSettings,
        timeout: Duration,
    ) -> Self {
        Self {
            context,
            resolver,
            settings,
            timeout,
        }
    }
}

impl DeadLetterPublisher for KafkaDeadLetterPublisher<'_> {
    fn publish_copy(&self, topic: &str, message: PreparedMessage) -> CoreResult<DeliveryReport> {
        let options = ProduceOptions {
            topic: topic.to_owned(),
            key_template: None,
            header_templates: Vec::new(),
            partition: None,
            count: 1,
            rate_per_second: None,
            render_templates: false,
            timeout: self.timeout,
        };
        let report = produce_messages(
            &self.context,
            self.resolver,
            &self.settings,
            &options,
            vec![message],
        )?;
        match report.deliveries.into_iter().next() {
            Some(delivery) if report.sent == 1 => Ok(delivery),
            _ => Err(CoreError::Kafka(format!(
                "dead-letter copy was not acknowledged: {}",
                report
                    .first_error
                    .unwrap_or_else(|| "no delivery report".to_owned())
            ))),
        }
    }
}

/// In-memory publisher for tests: records every copy and answers with increasing offsets.
#[cfg(any(test, feature = "test-support"))]
#[derive(Default)]
pub struct FakeDeadLetterPublisher {
    pub failure: Option<String>,
    published: std::cell::RefCell<Vec<(String, PreparedMessage)>>,
}

#[cfg(any(test, feature = "test-support"))]
impl FakeDeadLetterPublisher {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every copy that was accepted, in order.
    pub fn published(&self) -> Vec<(String, PreparedMessage)> {
        self.published.borrow().clone()
    }
}

#[cfg(any(test, feature = "test-support"))]
impl DeadLetterPublisher for FakeDeadLetterPublisher {
    fn publish_copy(&self, topic: &str, message: PreparedMessage) -> CoreResult<DeliveryReport> {
        if let Some(failure) = &self.failure {
            return Err(CoreError::Kafka(failure.clone()));
        }
        let mut published = self.published.borrow_mut();
        published.push((topic.to_owned(), message));
        Ok(DeliveryReport {
            partition: 0,
            offset: i64::try_from(published.len() - 1).unwrap_or(i64::MAX),
            timestamp: None,
        })
    }
}
