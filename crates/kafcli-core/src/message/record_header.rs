/// One Kafka record header. A `None` value is a header without a value, which differs from an empty value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    pub name: String,
    pub value: Option<Vec<u8>>,
}
