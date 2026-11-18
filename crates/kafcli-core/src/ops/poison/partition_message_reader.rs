//! Reading the message at a given offset of some partitions.

use std::collections::BTreeMap;

use crate::error::CoreResult;
use crate::message::record_header::RecordHeader;

/// One message as the broker returned it, bytes untouched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchedRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_millis: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<RecordHeader>,
}

/// Reads single messages without joining any group and without committing.
pub trait PartitionMessageReader {
    /// For each `partition -> start offset`, the first message at or after the start. A partition
    /// with no message within the reader's timeout is left out of the result.
    fn read_first_messages(
        &self,
        topic: &str,
        start_offsets: &BTreeMap<i32, i64>,
    ) -> CoreResult<BTreeMap<i32, FetchedRecord>>;
}
