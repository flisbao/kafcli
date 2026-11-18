//! Consumer group data as read from the cluster, before any lag is computed.

use crate::ops::groups::consumer_protocol::{MemberAssignment, MemberSubscription};

/// One member of a consumer group. `subscription` and `assignment` are `None` when the member
/// published no consumer protocol metadata or it could not be decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMember {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    pub subscription: Option<MemberSubscription>,
    pub assignment: Option<MemberAssignment>,
}

/// A consumer group as `DescribeGroups` reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupDescription {
    pub name: String,
    pub state: String,
    pub protocol_type: String,
    pub members: Vec<GroupMember>,
}

/// Log start (`low`) and log end (`high`) offsets of one partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionWatermarks {
    pub low: i64,
    pub high: i64,
}
