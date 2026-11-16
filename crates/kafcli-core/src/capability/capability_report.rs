use serde::Serialize;

/// Kafka capabilities checked by the probe, in execution order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityName {
    ClusterMetadata,
    IdempotentProduce,
    HeadersRoundTrip,
    ListOffsets,
    OffsetsForTimes,
    CommitOffsetsOutsideGroup,
    FetchCommittedOffsets,
    ListGroups,
    DescribeGroup,
    CommitRejectedWhileGroupActive,
}

/// Result of one capability check.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum CapabilityOutcome {
    Supported {
        detail: Option<String>,
    },
    Unsupported {
        error_code: String,
        detail: Option<String>,
    },
    Failed {
        message: String,
    },
}

/// One capability check together with the time it took.
#[derive(Clone, Debug, Serialize)]
pub struct CapabilityCheck {
    pub name: CapabilityName,
    #[serde(flatten)]
    pub outcome: CapabilityOutcome,
    pub elapsed_millis: u64,
}

/// Full result of a capability probe run against one topic.
#[derive(Clone, Debug, Serialize)]
pub struct CapabilityReport {
    pub context_name: String,
    pub run_identifier: String,
    pub topic: String,
    pub probe_group: String,
    pub broker_count: usize,
    pub checks: Vec<CapabilityCheck>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_serializes_flat() {
        let check = CapabilityCheck {
            name: CapabilityName::ListGroups,
            outcome: CapabilityOutcome::Supported {
                detail: Some("probe group listed: yes".to_string()),
            },
            elapsed_millis: 7,
        };

        let value = serde_json::to_value(&check).unwrap();

        assert_eq!(value["name"], "list_groups");
        assert_eq!(value["outcome"], "supported");
        assert_eq!(value["detail"], "probe group listed: yes");
        assert_eq!(value["elapsed_millis"], 7);
        assert!(value.get("checks").is_none());
    }
}
