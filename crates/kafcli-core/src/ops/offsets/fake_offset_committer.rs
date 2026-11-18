//! In-memory `GroupOffsetCommitter` for tests.

use std::cell::RefCell;
use std::collections::BTreeMap;

use crate::error::{CoreError, CoreResult};
use crate::ops::groups::fake_cluster_reader::FakeClusterReader;
use crate::ops::offsets::group_offset_committer::GroupOffsetCommitter;

/// One `commit_group_offsets` call: the group and its offsets.
pub type RecordedCommit = (String, BTreeMap<(String, i32), i64>);

/// Records every commit. Linked to a `FakeClusterReader`, a successful commit is also what the
/// reader returns afterwards, as a broker would.
pub struct FakeOffsetCommitter<'a> {
    pub failure: Option<String>,
    pub refuse_active_group: bool,
    reader: Option<&'a FakeClusterReader>,
    commits: RefCell<Vec<RecordedCommit>>,
}

impl FakeOffsetCommitter<'static> {
    pub fn new() -> Self {
        Self {
            failure: None,
            refuse_active_group: false,
            reader: None,
            commits: RefCell::new(Vec::new()),
        }
    }
}

impl Default for FakeOffsetCommitter<'static> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> FakeOffsetCommitter<'a> {
    pub fn linked_to(reader: &'a FakeClusterReader) -> Self {
        Self {
            failure: None,
            refuse_active_group: false,
            reader: Some(reader),
            commits: RefCell::new(Vec::new()),
        }
    }

    /// Every attempted commit, failed ones included, in order.
    pub fn commits(&self) -> Vec<RecordedCommit> {
        self.commits.borrow().clone()
    }
}

impl GroupOffsetCommitter for FakeOffsetCommitter<'_> {
    fn commit_group_offsets(
        &self,
        group: &str,
        offsets: &BTreeMap<(String, i32), i64>,
    ) -> CoreResult<()> {
        self.commits
            .borrow_mut()
            .push((group.to_owned(), offsets.clone()));
        if self.refuse_active_group {
            return Err(CoreError::CommitRefusedActiveGroup {
                group: group.to_owned(),
                detail: String::new(),
            });
        }
        if let Some(failure) = &self.failure {
            return Err(CoreError::Kafka(failure.clone()));
        }
        if let Some(reader) = self.reader {
            let mut overrides = reader.committed_overrides.borrow_mut();
            for ((topic, partition), offset) in offsets {
                overrides.insert((group.to_owned(), topic.clone(), *partition), *offset);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::broker_capabilities::BrokerFamily;
    use crate::ops::groups::cluster_offsets_reader::ClusterOffsetsReader;

    #[test]
    fn linked_commit_is_read_back() {
        let reader = FakeClusterReader::new(BrokerFamily::GenericKafka);
        let committer = FakeOffsetCommitter::linked_to(&reader);

        committer
            .commit_group_offsets("g", &BTreeMap::from([(("pedidos".to_owned(), 0), 7)]))
            .unwrap();

        let offsets = reader
            .fetch_committed_offsets("g", &BTreeMap::from([("pedidos".to_owned(), vec![0])]))
            .unwrap();
        assert_eq!(offsets.get(&("pedidos".to_owned(), 0)), Some(&Some(7)));
        assert_eq!(committer.commits().len(), 1);
    }
}
