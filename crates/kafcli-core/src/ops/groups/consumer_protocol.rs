//! Decoding of the consumer protocol metadata that group members publish.

use std::collections::BTreeMap;

use crate::error::{CoreError, CoreResult};

/// Topics a group member subscribed to, sorted and without repetition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberSubscription {
    pub topics: Vec<String>,
}

/// Partitions assigned to a group member, sorted per topic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberAssignment {
    pub partitions: BTreeMap<String, Vec<i32>>,
}

const MAXIMUM_TOPIC_COUNT: i32 = 10_000;
const MAXIMUM_PARTITION_COUNT: i32 = 100_000;

/// Decodes a `ConsumerProtocolSubscription` (versions 0 to 3), ignoring user data and the tail.
pub fn decode_subscription(bytes: &[u8]) -> CoreResult<MemberSubscription> {
    let mut reader = ProtocolReader::new(bytes, "subscription");
    reader.read_version()?;
    let topic_count = reader.read_count(MAXIMUM_TOPIC_COUNT, "topic count above 10000")?;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        topics.push(reader.read_string()?);
    }
    topics.sort();
    topics.dedup();
    Ok(MemberSubscription { topics })
}

/// Decodes a `ConsumerProtocolAssignment`, ignoring user data and the tail.
pub fn decode_assignment(bytes: &[u8]) -> CoreResult<MemberAssignment> {
    let mut reader = ProtocolReader::new(bytes, "assignment");
    reader.read_version()?;
    let topic_count = reader.read_count(MAXIMUM_TOPIC_COUNT, "topic count above 10000")?;
    let mut partitions: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    let mut total_partitions: usize = 0;
    for _ in 0..topic_count {
        let topic = reader.read_string()?;
        let partition_count =
            reader.read_count(MAXIMUM_PARTITION_COUNT, "partition count above 100000")?;
        total_partitions = total_partitions.saturating_add(partition_count);
        if total_partitions > MAXIMUM_PARTITION_COUNT as usize {
            return Err(reader.error("partition count above 100000"));
        }
        let topic_partitions = partitions.entry(topic).or_default();
        topic_partitions.reserve(partition_count);
        for _ in 0..partition_count {
            topic_partitions.push(reader.read_i32()?);
        }
    }
    for topic_partitions in partitions.values_mut() {
        topic_partitions.sort_unstable();
        topic_partitions.dedup();
    }
    Ok(MemberAssignment { partitions })
}

/// Big-endian cursor over protocol bytes; every read checks the remaining length first.
struct ProtocolReader<'a> {
    bytes: &'a [u8],
    position: usize,
    kind: &'static str,
}

impl<'a> ProtocolReader<'a> {
    fn new(bytes: &'a [u8], kind: &'static str) -> Self {
        Self {
            bytes,
            position: 0,
            kind,
        }
    }

    fn error(&self, reason: &str) -> CoreError {
        CoreError::InvalidInput(format!("consumer protocol {}: {reason}", self.kind))
    }

    fn take(&mut self, length: usize) -> CoreResult<&'a [u8]> {
        let end = self.position.checked_add(length);
        match end.and_then(|end| self.bytes.get(self.position..end)) {
            Some(slice) => {
                self.position += length;
                Ok(slice)
            }
            None => Err(self.error(&format!("truncated at byte {}", self.position))),
        }
    }

    fn read_i16(&mut self) -> CoreResult<i16> {
        let slice = self.take(2)?;
        Ok(i16::from_be_bytes([slice[0], slice[1]]))
    }

    fn read_i32(&mut self) -> CoreResult<i32> {
        let slice = self.take(4)?;
        Ok(i32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
    }

    fn read_version(&mut self) -> CoreResult<i16> {
        let version = self.read_i16()?;
        if version < 0 {
            return Err(self.error("negative version"));
        }
        Ok(version)
    }

    /// Reads an `i32` array count and checks it against `maximum` before anything is allocated.
    fn read_count(&mut self, maximum: i32, above_maximum: &str) -> CoreResult<usize> {
        let count = self.read_i32()?;
        if count < 0 {
            return Err(self.error("negative length"));
        }
        if count > maximum {
            return Err(self.error(above_maximum));
        }
        usize::try_from(count).map_err(|_| self.error(above_maximum))
    }

    fn read_string(&mut self) -> CoreResult<String> {
        let length = self.read_i16()?;
        if length < 0 {
            return Err(self.error("negative length"));
        }
        let length = usize::try_from(length).map_err(|_| self.error("negative length"))?;
        let slice = self.take(length)?;
        std::str::from_utf8(slice)
            .map(str::to_owned)
            .map_err(|_| self.error("topic name is not UTF-8"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_test_string(text: &str) -> Vec<u8> {
        let mut bytes = i16::try_from(text.len()).unwrap().to_be_bytes().to_vec();
        bytes.extend_from_slice(text.as_bytes());
        bytes
    }

    fn encode_subscription_header(version: i16, topics: &[&str]) -> Vec<u8> {
        let mut bytes = version.to_be_bytes().to_vec();
        bytes.extend_from_slice(&i32::try_from(topics.len()).unwrap().to_be_bytes());
        for topic in topics {
            bytes.extend(encode_test_string(topic));
        }
        bytes
    }

    fn error_text(result: CoreResult<impl std::fmt::Debug>) -> String {
        match result {
            Ok(value) => panic!("expected an error, got {value:?}"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn decodes_v0_subscription() {
        let mut bytes = encode_subscription_header(0, &["pedidos", "pagamentos", "pedidos"]);
        bytes.extend_from_slice(&(-1i32).to_be_bytes());

        let subscription = decode_subscription(&bytes).unwrap();

        assert_eq!(subscription.topics, vec!["pagamentos", "pedidos"]);
    }

    #[test]
    fn decodes_v3_subscription_ignoring_tail() {
        let mut bytes = encode_subscription_header(3, &["pedidos"]);
        bytes.extend_from_slice(&3i32.to_be_bytes());
        bytes.extend_from_slice(&[1, 2, 3]);
        bytes.extend_from_slice(&1i32.to_be_bytes());
        bytes.extend(encode_test_string("pedidos"));
        bytes.extend_from_slice(&1i32.to_be_bytes());
        bytes.extend_from_slice(&0i32.to_be_bytes());
        bytes.extend_from_slice(&7i32.to_be_bytes());
        bytes.extend(encode_test_string("rack-a"));

        let subscription = decode_subscription(&bytes).unwrap();

        assert_eq!(subscription.topics, vec!["pedidos"]);
    }

    #[test]
    fn decodes_assignment() {
        let mut bytes = 0i16.to_be_bytes().to_vec();
        bytes.extend_from_slice(&1i32.to_be_bytes());
        bytes.extend(encode_test_string("pedidos"));
        bytes.extend_from_slice(&3i32.to_be_bytes());
        for partition in [2i32, 0, 1] {
            bytes.extend_from_slice(&partition.to_be_bytes());
        }
        bytes.extend_from_slice(&(-1i32).to_be_bytes());

        let assignment = decode_assignment(&bytes).unwrap();

        assert_eq!(assignment.partitions["pedidos"], vec![0, 1, 2]);
    }

    #[test]
    fn huge_topic_count_fails_before_allocating() {
        let mut bytes = 0i16.to_be_bytes().to_vec();
        bytes.extend_from_slice(&i32::MAX.to_be_bytes());

        let text = error_text(decode_subscription(&bytes));

        assert!(
            text.contains("consumer protocol subscription: topic count above 10000"),
            "{text}"
        );
    }

    #[test]
    fn huge_partition_count_fails_before_allocating() {
        let mut bytes = 0i16.to_be_bytes().to_vec();
        bytes.extend_from_slice(&1i32.to_be_bytes());
        bytes.extend(encode_test_string("pedidos"));
        bytes.extend_from_slice(&i32::MAX.to_be_bytes());

        let text = error_text(decode_assignment(&bytes));

        assert!(
            text.contains("consumer protocol assignment: partition count above 100000"),
            "{text}"
        );
    }

    #[test]
    fn negative_string_length_is_error() {
        let mut bytes = 0i16.to_be_bytes().to_vec();
        bytes.extend_from_slice(&1i32.to_be_bytes());
        bytes.extend_from_slice(&(-5i16).to_be_bytes());

        let text = error_text(decode_subscription(&bytes));

        assert!(text.contains("negative length"), "{text}");
    }

    #[test]
    fn negative_version_is_error() {
        let bytes = encode_subscription_header(-1, &["pedidos"]);

        assert!(decode_subscription(&bytes).is_err());
    }

    #[test]
    fn non_utf8_topic_name_is_error() {
        let mut bytes = 0i16.to_be_bytes().to_vec();
        bytes.extend_from_slice(&1i32.to_be_bytes());
        bytes.extend_from_slice(&2i16.to_be_bytes());
        bytes.extend_from_slice(&[0xff, 0xfe]);

        let text = error_text(decode_subscription(&bytes));

        assert!(text.contains("topic name is not UTF-8"), "{text}");
    }

    #[test]
    fn truncated_bytes_report_position() {
        let bytes = encode_subscription_header(0, &["pedidos"]);
        let truncated = &bytes[..bytes.len() - 3];

        let text = error_text(decode_subscription(truncated));

        assert!(
            text.contains("consumer protocol subscription: truncated at byte 8"),
            "{text}"
        );
    }

    #[test]
    fn empty_input_is_truncated() {
        let text = error_text(decode_subscription(&[]));

        assert!(text.contains("truncated at byte 0"), "{text}");
    }
}
