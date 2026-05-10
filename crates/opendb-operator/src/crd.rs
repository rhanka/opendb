use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::recovery::ClusterRecoveryAggregate;

pub const PHASE_PENDING: &str = "Pending";
pub const PHASE_DEGRADED: &str = "Degraded";
pub const PHASE_READY: &str = "Ready";
pub const MIN_REPLICAS: i32 = 1;
pub const APP_LABEL_KEY: &str = "app.kubernetes.io/name";
pub const APP_LABEL_VALUE: &str = "opendb";
pub const INSTANCE_LABEL_KEY: &str = "app.kubernetes.io/instance";
pub const NODE_CONTAINER_NAME: &str = "opendb-node";

const CONDITION_TRUE: &str = "True";
const CONDITION_FALSE: &str = "False";
const CONDITION_UNKNOWN: &str = "Unknown";

const CONDITION_ROOT_DESCRIPTOR_KNOWN: &str = "RootDescriptorKnown";
const CONDITION_WAL_REPLAY_COMPLETED: &str = "WalReplayCompleted";
const CONDITION_ARCHIVE_METADATA_KNOWN: &str = "ArchiveMetadataKnown";
const CONDITION_RECOVERED: &str = "Recovered";

#[derive(CustomResource, Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "db.opendb.dev",
    version = "v1alpha1",
    kind = "OpenDbCluster",
    plural = "opendbclusters",
    namespaced,
    status = "OpenDbClusterStatus",
    derive = "PartialEq",
    shortname = "odb"
)]
#[serde(rename_all = "camelCase")]
pub struct OpenDbClusterSpec {
    #[schemars(range(min = 1))]
    pub replicas: i32,
    pub image: String,
    pub storage_class_name: String,
    pub storage_size: String,
    pub pgwire_port: i32,
    pub health_port: i32,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenDbClusterStatus {
    pub ready_replicas: i32,
    pub phase: String,
    pub leader_pod: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<OpenDbClusterRecoverySummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<OpenDbClusterCondition>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenDbClusterRecoverySummary {
    pub root_descriptor_known_replicas: i32,
    pub wal_replay_completed_replicas: i32,
    pub archive_metadata_replayed_replicas: i32,
    pub unreachable_replicas: i32,
    pub last_replayed_tx_id: Option<u64>,
    pub last_replayed_ts: Option<u64>,
    pub latest_recovery_artifact: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenDbClusterCondition {
    #[serde(rename = "type")]
    pub r#type: String,
    pub status: String,
    pub reason: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<String>")]
    pub last_transition_time: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OpenDbClusterStatusSnapshot {
    pub desired_replicas: i32,
    pub ready_pods: i32,
    pub leader_pod: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedOpenDbPod {
    pub name: String,
    pub node_running: bool,
    pub leader_ready: bool,
    pub pod_ip: Option<String>,
}

pub fn open_db_pod_label_selector(cluster_name: &str) -> String {
    format!("{APP_LABEL_KEY}={APP_LABEL_VALUE},{INSTANCE_LABEL_KEY}={cluster_name}")
}

pub fn snapshot_from_observed_pods(
    desired_replicas: i32,
    pods: &[ObservedOpenDbPod],
) -> OpenDbClusterStatusSnapshot {
    OpenDbClusterStatusSnapshot {
        desired_replicas,
        ready_pods: pods.iter().filter(|pod| pod.node_running).count() as i32,
        leader_pod: pods
            .iter()
            .find(|pod| pod.leader_ready)
            .map(|pod| pod.name.clone()),
    }
}

pub fn compute_open_db_cluster_status(
    snapshot: OpenDbClusterStatusSnapshot,
) -> OpenDbClusterStatus {
    let desired_replicas = snapshot.desired_replicas.max(MIN_REPLICAS);
    let ready_replicas = snapshot.ready_pods.max(0);
    let leader_pod = snapshot.leader_pod.and_then(|pod| {
        let trimmed = pod.trim().to_string();
        (!trimmed.is_empty()).then_some(trimmed)
    });

    let (phase, ready_reason, ready_message) = if ready_replicas == 0 {
        (
            PHASE_PENDING,
            "NoReadyReplicas",
            format!("No OpenDb pods are ready; desired replicas: {desired_replicas}."),
        )
    } else if ready_replicas == desired_replicas && leader_pod.is_some() {
        (
            PHASE_READY,
            "ClusterReady",
            format!("{ready_replicas} desired OpenDb pods are ready and a leader is known."),
        )
    } else if ready_replicas != desired_replicas {
        (
            PHASE_DEGRADED,
            "ReplicasNotReady",
            format!("{ready_replicas} of {desired_replicas} desired OpenDb pods are ready."),
        )
    } else {
        (
            PHASE_DEGRADED,
            "LeaderMissing",
            "All desired OpenDb pods are ready, but no leader pod is known.".to_string(),
        )
    };

    let ready_condition_status = if phase == PHASE_READY {
        CONDITION_TRUE
    } else {
        CONDITION_FALSE
    };

    let leader_condition = match &leader_pod {
        Some(pod) => OpenDbClusterCondition {
            r#type: "LeaderKnown".to_string(),
            status: CONDITION_TRUE.to_string(),
            reason: "LeaderKnown".to_string(),
            message: format!("Leader pod is {pod}."),
            last_transition_time: None,
        },
        None => OpenDbClusterCondition {
            r#type: "LeaderKnown".to_string(),
            status: CONDITION_FALSE.to_string(),
            reason: "LeaderMissing".to_string(),
            message: "No leader pod has been observed.".to_string(),
            last_transition_time: None,
        },
    };

    OpenDbClusterStatus {
        ready_replicas,
        phase: phase.to_string(),
        leader_pod,
        recovery: None,
        conditions: vec![
            OpenDbClusterCondition {
                r#type: "Ready".to_string(),
                status: ready_condition_status.to_string(),
                reason: ready_reason.to_string(),
                message: ready_message,
                last_transition_time: None,
            },
            leader_condition,
        ],
    }
}

pub fn compute_open_db_cluster_status_with_recovery(
    snapshot: OpenDbClusterStatusSnapshot,
    recovery: Option<ClusterRecoveryAggregate>,
) -> OpenDbClusterStatus {
    compute_open_db_cluster_status_at(snapshot, recovery, chrono::Utc::now())
}

pub fn compute_open_db_cluster_status_at(
    snapshot: OpenDbClusterStatusSnapshot,
    recovery: Option<ClusterRecoveryAggregate>,
    now: chrono::DateTime<chrono::Utc>,
) -> OpenDbClusterStatus {
    let mut status = compute_open_db_cluster_status(snapshot);
    for condition in status.conditions.iter_mut() {
        condition.last_transition_time = Some(now);
    }

    let Some(aggregate) = recovery else {
        return status;
    };

    let ready_condition_true = status
        .conditions
        .iter()
        .any(|c| c.r#type == "Ready" && c.status == CONDITION_TRUE);

    status.recovery = Some(OpenDbClusterRecoverySummary {
        root_descriptor_known_replicas: aggregate.root_descriptor_known_pods,
        wal_replay_completed_replicas: aggregate.wal_replay_completed_pods,
        archive_metadata_replayed_replicas: aggregate.archive_metadata_replayed_pods,
        unreachable_replicas: aggregate.unreachable_pods,
        last_replayed_tx_id: aggregate.last_replayed_tx_id,
        last_replayed_ts: aggregate.last_replayed_ts,
        latest_recovery_artifact: aggregate.latest_recovery_artifact.clone(),
    });

    let triple = recovery_condition_triple(&aggregate);
    status.conditions.push(condition_for(
        CONDITION_ROOT_DESCRIPTOR_KNOWN,
        &triple.root,
        now,
    ));
    status.conditions.push(condition_for(
        CONDITION_WAL_REPLAY_COMPLETED,
        &triple.wal,
        now,
    ));
    status.conditions.push(condition_for(
        CONDITION_ARCHIVE_METADATA_KNOWN,
        &triple.archive,
        now,
    ));
    status
        .conditions
        .push(recovered_condition(&triple, ready_condition_true, now));

    status
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ConditionStatus {
    True,
    False,
    Unknown,
}

impl ConditionStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::True => CONDITION_TRUE,
            Self::False => CONDITION_FALSE,
            Self::Unknown => CONDITION_UNKNOWN,
        }
    }
}

#[derive(Clone, Debug)]
struct RecoveryConditionState {
    status: ConditionStatus,
    reason: &'static str,
    message: String,
}

#[derive(Clone, Debug)]
struct RecoveryConditionTriple {
    root: RecoveryConditionState,
    wal: RecoveryConditionState,
    archive: RecoveryConditionState,
}

fn recovery_condition_triple(aggregate: &ClusterRecoveryAggregate) -> RecoveryConditionTriple {
    let observed = aggregate.observed_running_pods;
    let unreachable = aggregate.unreachable_pods;

    RecoveryConditionTriple {
        root: single_condition_state(
            observed,
            unreachable,
            aggregate.root_descriptor_known_pods,
            "AllReplicasReportRoot",
            "RootDescriptorMissing",
            "PodsUnreachable",
            "root descriptor",
        ),
        wal: single_condition_state(
            observed,
            unreachable,
            aggregate.wal_replay_completed_pods,
            "AllReplicasReplayed",
            "WalReplayIncomplete",
            "PodsUnreachable",
            "WAL replay",
        ),
        archive: single_condition_state(
            observed,
            unreachable,
            aggregate.archive_metadata_replayed_pods,
            "AllReplicasReportedArchive",
            "ArchiveMetadataIncomplete",
            "PodsUnreachable",
            "archive metadata",
        ),
    }
}

fn single_condition_state(
    observed_running: i32,
    unreachable: i32,
    matching: i32,
    true_reason: &'static str,
    false_reason: &'static str,
    unknown_reason: &'static str,
    feature_label: &str,
) -> RecoveryConditionState {
    if unreachable > 0 {
        return RecoveryConditionState {
            status: ConditionStatus::Unknown,
            reason: unknown_reason,
            message: format!(
                "{unreachable} of {observed_running} running pods are unreachable while checking {feature_label}."
            ),
        };
    }
    if matching == observed_running {
        return RecoveryConditionState {
            status: ConditionStatus::True,
            reason: true_reason,
            message: format!(
                "{matching} of {observed_running} running pods report {feature_label}."
            ),
        };
    }
    RecoveryConditionState {
        status: ConditionStatus::False,
        reason: false_reason,
        message: format!(
            "only {matching} of {observed_running} running pods report {feature_label}."
        ),
    }
}

fn condition_for(
    name: &'static str,
    state: &RecoveryConditionState,
    now: chrono::DateTime<chrono::Utc>,
) -> OpenDbClusterCondition {
    OpenDbClusterCondition {
        r#type: name.to_string(),
        status: state.status.as_str().to_string(),
        reason: state.reason.to_string(),
        message: state.message.clone(),
        last_transition_time: Some(now),
    }
}

fn recovered_condition(
    triple: &RecoveryConditionTriple,
    ready_true: bool,
    now: chrono::DateTime<chrono::Utc>,
) -> OpenDbClusterCondition {
    let any_unknown = matches!(triple.root.status, ConditionStatus::Unknown)
        || matches!(triple.wal.status, ConditionStatus::Unknown)
        || matches!(triple.archive.status, ConditionStatus::Unknown);
    let (status, reason, message) = if any_unknown {
        (
            ConditionStatus::Unknown,
            "PodsUnreachable",
            "at least one running pod is unreachable; recovery state is unknown.".to_string(),
        )
    } else if matches!(triple.root.status, ConditionStatus::False) {
        (
            ConditionStatus::False,
            "RootDescriptorMissing",
            "root descriptor is not known on every running pod.".to_string(),
        )
    } else if matches!(triple.wal.status, ConditionStatus::False) {
        (
            ConditionStatus::False,
            "WalReplayIncomplete",
            "WAL replay has not completed on every running pod.".to_string(),
        )
    } else if matches!(triple.archive.status, ConditionStatus::False) {
        (
            ConditionStatus::False,
            "ArchiveMetadataIncomplete",
            "archive metadata is not known on every running pod.".to_string(),
        )
    } else if !ready_true {
        (
            ConditionStatus::False,
            "NotReady",
            "all running pods report recovery but cluster Ready condition is False.".to_string(),
        )
    } else {
        (
            ConditionStatus::True,
            "RecoveredAndReady",
            "all running pods report recovery and the cluster is Ready.".to_string(),
        )
    };
    OpenDbClusterCondition {
        r#type: CONDITION_RECOVERED.to_string(),
        status: status.as_str().to_string(),
        reason: reason.to_string(),
        message,
        last_transition_time: Some(now),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ObservedOpenDbPod, OpenDbClusterStatusSnapshot, PHASE_DEGRADED, PHASE_PENDING, PHASE_READY,
        compute_open_db_cluster_status, snapshot_from_observed_pods,
    };

    #[test]
    fn status_is_pending_when_no_pods_are_ready() {
        let status = compute_open_db_cluster_status(OpenDbClusterStatusSnapshot {
            desired_replicas: 3,
            ready_pods: 0,
            leader_pod: None,
        });

        assert_eq!(status.ready_replicas, 0);
        assert_eq!(status.phase, PHASE_PENDING);
        assert_eq!(status.leader_pod, None);
        assert!(status.conditions.iter().any(|condition| {
            condition.r#type == "Ready"
                && condition.status == "False"
                && condition.reason == "NoReadyReplicas"
        }));
    }

    #[test]
    fn status_is_degraded_when_ready_pods_are_below_desired() {
        let status = compute_open_db_cluster_status(OpenDbClusterStatusSnapshot {
            desired_replicas: 3,
            ready_pods: 2,
            leader_pod: Some("opendb-0".to_string()),
        });

        assert_eq!(status.ready_replicas, 2);
        assert_eq!(status.phase, PHASE_DEGRADED);
        assert_eq!(status.leader_pod.as_deref(), Some("opendb-0"));
        assert!(status.conditions.iter().any(|condition| {
            condition.r#type == "Ready"
                && condition.status == "False"
                && condition.reason == "ReplicasNotReady"
        }));
    }

    #[test]
    fn status_is_degraded_when_leader_is_absent() {
        let status = compute_open_db_cluster_status(OpenDbClusterStatusSnapshot {
            desired_replicas: 3,
            ready_pods: 3,
            leader_pod: None,
        });

        assert_eq!(status.ready_replicas, 3);
        assert_eq!(status.phase, PHASE_DEGRADED);
        assert!(status.conditions.iter().any(|condition| {
            condition.r#type == "Ready"
                && condition.status == "False"
                && condition.reason == "LeaderMissing"
        }));
    }

    #[test]
    fn status_is_ready_when_desired_replicas_and_leader_are_observed() {
        let status = compute_open_db_cluster_status(OpenDbClusterStatusSnapshot {
            desired_replicas: 3,
            ready_pods: 3,
            leader_pod: Some("opendb-0".to_string()),
        });

        assert_eq!(status.ready_replicas, 3);
        assert_eq!(status.phase, PHASE_READY);
        assert_eq!(status.leader_pod.as_deref(), Some("opendb-0"));
        assert!(status.conditions.iter().any(|condition| {
            condition.r#type == "Ready"
                && condition.status == "True"
                && condition.reason == "ClusterReady"
        }));
    }

    #[test]
    fn status_clamps_zero_desired_replicas_to_minimum() {
        let status = compute_open_db_cluster_status(OpenDbClusterStatusSnapshot {
            desired_replicas: 0,
            ready_pods: 0,
            leader_pod: None,
        });

        assert_eq!(status.ready_replicas, 0);
        assert_eq!(status.phase, PHASE_PENDING);
        assert!(status.conditions.iter().any(|condition| {
            condition.r#type == "Ready" && condition.message.contains("desired replicas: 1")
        }));
    }

    #[test]
    fn blank_leader_pod_is_treated_as_missing() {
        let status = compute_open_db_cluster_status(OpenDbClusterStatusSnapshot {
            desired_replicas: 3,
            ready_pods: 3,
            leader_pod: Some("  ".to_string()),
        });

        assert_eq!(status.phase, PHASE_DEGRADED);
        assert_eq!(status.leader_pod, None);
        assert!(status.conditions.iter().any(|condition| {
            condition.r#type == "LeaderKnown"
                && condition.status == "False"
                && condition.reason == "LeaderMissing"
        }));
    }

    #[test]
    fn status_serializes_camel_case_fields_and_condition_type() {
        let status = compute_open_db_cluster_status(OpenDbClusterStatusSnapshot {
            desired_replicas: 1,
            ready_pods: 1,
            leader_pod: Some("opendb-0".to_string()),
        });
        let json = serde_json::to_value(status).expect("serialize status");

        assert_eq!(json["readyReplicas"], 1);
        assert_eq!(json["leaderPod"], "opendb-0");
        assert_eq!(json["conditions"][0]["type"], "Ready");
    }

    #[test]
    fn snapshot_counts_running_db_processes_without_requiring_leader_readiness() {
        let snapshot = snapshot_from_observed_pods(
            3,
            &[
                ObservedOpenDbPod {
                    name: "opendb-0".to_string(),
                    node_running: true,
                    leader_ready: true,
                    pod_ip: None,
                },
                ObservedOpenDbPod {
                    name: "opendb-1".to_string(),
                    node_running: true,
                    leader_ready: false,
                    pod_ip: None,
                },
                ObservedOpenDbPod {
                    name: "opendb-2".to_string(),
                    node_running: true,
                    leader_ready: false,
                    pod_ip: None,
                },
            ],
        );

        assert_eq!(
            snapshot,
            OpenDbClusterStatusSnapshot {
                desired_replicas: 3,
                ready_pods: 3,
                leader_pod: Some("opendb-0".to_string()),
            }
        );
    }

    #[test]
    fn snapshot_reports_missing_leader_when_no_pod_is_leader_ready() {
        let snapshot = snapshot_from_observed_pods(
            2,
            &[
                ObservedOpenDbPod {
                    name: "opendb-0".to_string(),
                    node_running: true,
                    leader_ready: false,
                    pod_ip: None,
                },
                ObservedOpenDbPod {
                    name: "opendb-1".to_string(),
                    node_running: false,
                    leader_ready: false,
                    pod_ip: None,
                },
            ],
        );

        assert_eq!(
            snapshot,
            OpenDbClusterStatusSnapshot {
                desired_replicas: 2,
                ready_pods: 1,
                leader_pod: None,
            }
        );
    }

    #[test]
    fn pod_label_selector_scopes_observation_to_cluster_instance() {
        assert_eq!(
            super::open_db_pod_label_selector("opendb"),
            "app.kubernetes.io/name=opendb,app.kubernetes.io/instance=opendb"
        );
    }

    mod recovery_status {
        use crate::crd::{
            OpenDbClusterCondition, OpenDbClusterStatus, OpenDbClusterStatusSnapshot,
            compute_open_db_cluster_status_at, compute_open_db_cluster_status_with_recovery,
        };
        use crate::recovery::ClusterRecoveryAggregate;
        use std::collections::BTreeMap;

        fn condition_map(
            conditions: &[OpenDbClusterCondition],
        ) -> BTreeMap<String, OpenDbClusterCondition> {
            conditions
                .iter()
                .map(|c| (c.r#type.clone(), c.clone()))
                .collect()
        }

        fn snapshot(ready_pods: i32, leader: Option<&str>) -> OpenDbClusterStatusSnapshot {
            OpenDbClusterStatusSnapshot {
                desired_replicas: 3,
                ready_pods,
                leader_pod: leader.map(str::to_string),
            }
        }

        fn aggregate(
            running: i32,
            root: i32,
            wal: i32,
            archive: i32,
            unreachable: i32,
        ) -> ClusterRecoveryAggregate {
            ClusterRecoveryAggregate {
                observed_running_pods: running,
                root_descriptor_known_pods: root,
                wal_replay_completed_pods: wal,
                archive_metadata_replayed_pods: archive,
                unreachable_pods: unreachable,
                last_replayed_tx_id: Some(7),
                last_replayed_ts: Some(7),
                latest_recovery_artifact: None,
            }
        }

        #[test]
        fn status_emits_recovered_true_when_all_running_pods_report_recovery_and_ready() {
            let status: OpenDbClusterStatus = compute_open_db_cluster_status_with_recovery(
                snapshot(3, Some("opendb-0")),
                Some(aggregate(3, 3, 3, 3, 0)),
            );

            let by_type = condition_map(&status.conditions);
            assert_eq!(by_type["RootDescriptorKnown"].status, "True");
            assert_eq!(by_type["WalReplayCompleted"].status, "True");
            assert_eq!(by_type["ArchiveMetadataKnown"].status, "True");
            assert_eq!(by_type["Recovered"].status, "True");
            assert_eq!(by_type["Recovered"].reason, "RecoveredAndReady");
            let recovery = status.recovery.expect("recovery summary present");
            assert_eq!(recovery.last_replayed_tx_id, Some(7));
            assert_eq!(recovery.root_descriptor_known_replicas, 3);
            assert_eq!(recovery.unreachable_replicas, 0);
        }

        #[test]
        fn status_emits_recovered_unknown_when_any_running_pod_is_unreachable() {
            let status = compute_open_db_cluster_status_with_recovery(
                snapshot(3, Some("opendb-0")),
                Some(aggregate(3, 2, 2, 2, 1)),
            );

            let by_type = condition_map(&status.conditions);
            assert_eq!(by_type["RootDescriptorKnown"].status, "Unknown");
            assert_eq!(by_type["WalReplayCompleted"].status, "Unknown");
            assert_eq!(by_type["ArchiveMetadataKnown"].status, "Unknown");
            assert_eq!(by_type["Recovered"].status, "Unknown");
            assert_eq!(by_type["Recovered"].reason, "PodsUnreachable");
        }

        #[test]
        fn status_emits_recovered_false_when_any_running_pod_reports_false() {
            let status = compute_open_db_cluster_status_with_recovery(
                snapshot(3, Some("opendb-0")),
                Some(aggregate(3, 3, 2, 3, 0)),
            );

            let by_type = condition_map(&status.conditions);
            assert_eq!(by_type["RootDescriptorKnown"].status, "True");
            assert_eq!(by_type["WalReplayCompleted"].status, "False");
            assert_eq!(by_type["ArchiveMetadataKnown"].status, "True");
            assert_eq!(by_type["Recovered"].status, "False");
            assert_eq!(by_type["Recovered"].reason, "WalReplayIncomplete");
        }

        #[test]
        fn status_emits_recovered_false_when_subconditions_true_but_ready_false() {
            let status = compute_open_db_cluster_status_with_recovery(
                snapshot(0, None),
                Some(aggregate(3, 3, 3, 3, 0)),
            );

            let by_type = condition_map(&status.conditions);
            assert_eq!(by_type["Ready"].status, "False");
            assert_eq!(by_type["Recovered"].status, "False");
            assert_eq!(by_type["Recovered"].reason, "NotReady");
        }

        #[test]
        fn status_recovery_block_is_omitted_when_no_running_pods_observed() {
            let status = compute_open_db_cluster_status_with_recovery(snapshot(0, None), None);

            assert!(status.recovery.is_none());
            let json = serde_json::to_value(&status).expect("serialize");
            assert!(json.get("recovery").is_none());
            let by_type = condition_map(&status.conditions);
            assert!(!by_type.contains_key("Recovered"));
        }

        #[test]
        fn condition_carries_last_transition_time() {
            let now = chrono::Utc::now();
            let status = compute_open_db_cluster_status_at(
                snapshot(3, Some("opendb-0")),
                Some(aggregate(3, 3, 3, 3, 0)),
                now,
            );

            for condition in &status.conditions {
                assert_eq!(condition.last_transition_time, Some(now));
            }
        }

        #[test]
        fn status_serializes_recovery_camel_case_fields() {
            let status = compute_open_db_cluster_status_with_recovery(
                snapshot(3, Some("opendb-0")),
                Some(aggregate(3, 3, 3, 3, 0)),
            );
            let json = serde_json::to_value(&status).expect("serialize");

            let recovery = &json["recovery"];
            assert_eq!(recovery["rootDescriptorKnownReplicas"], 3);
            assert_eq!(recovery["walReplayCompletedReplicas"], 3);
            assert_eq!(recovery["archiveMetadataReplayedReplicas"], 3);
            assert_eq!(recovery["unreachableReplicas"], 0);
            assert_eq!(recovery["lastReplayedTxId"], 7);
        }
    }
}
