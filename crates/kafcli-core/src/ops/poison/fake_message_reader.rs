//! In-memory `PartitionMessageReader` for tests.

use std::cell::RefCell;
use std::collections::BTreeMap;

use crate::error::CoreResult;
use crate::ops::poison::partition_message_reader::{FetchedRecord, PartitionMessageReader};

/// One `read_first_messages` call: the topic and the start offsets.
pub type MessageReadRequest = (String, BTreeMap<i32, i64>);

/// Serves `records` (per `(topic, partition)`, in offset order) and records every read.
#[derive(Default)]
pub struct FakePartitionMessageReader {
    pub records: BTreeMap<(String, i32), Vec<FetchedRecord>>,
    reads: RefCell<Vec<MessageReadRequest>>,
}

impl FakePartitionMessageReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every read, in order.
    pub fn reads(&self) -> Vec<MessageReadRequest> {
        self.reads.borrow().clone()
    }
}

impl PartitionMessageReader for FakePartitionMessageReader {
    fn read_first_messages(
        &self,
        topic: &str,
        start_offsets: &BTreeMap<i32, i64>,
    ) -> CoreResult<BTreeMap<i32, FetchedRecord>> {
        self.reads
            .borrow_mut()
            .push((topic.to_owned(), start_offsets.clone()));
        Ok(start_offsets
            .iter()
            .filter_map(|(partition, start)| {
                self.records
                    .get(&(topic.to_owned(), *partition))
                    .and_then(|records| records.iter().find(|record| record.offset >= *start))
                    .map(|record| (*partition, record.clone()))
            })
            .collect())
    }
}
