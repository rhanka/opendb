use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<OpenDbClusterCondition>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenDbClusterCondition {
    #[serde(rename = "type")]
    pub r#type: String,
    pub status: String,
    pub reason: String,
    pub message: String,
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
        },
        None => OpenDbClusterCondition {
            r#type: "LeaderKnown".to_string(),
            status: CONDITION_FALSE.to_string(),
            reason: "LeaderMissing".to_string(),
            message: "No leader pod has been observed.".to_string(),
        },
    };

    OpenDbClusterStatus {
        ready_replicas,
        phase: phase.to_string(),
        leader_pod,
        conditions: vec![
            OpenDbClusterCondition {
                r#type: "Ready".to_string(),
                status: ready_condition_status.to_string(),
                reason: ready_reason.to_string(),
                message: ready_message,
            },
            leader_condition,
        ],
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
                },
                ObservedOpenDbPod {
                    name: "opendb-1".to_string(),
                    node_running: true,
                    leader_ready: false,
                },
                ObservedOpenDbPod {
                    name: "opendb-2".to_string(),
                    node_running: true,
                    leader_ready: false,
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
                },
                ObservedOpenDbPod {
                    name: "opendb-1".to_string(),
                    node_running: false,
                    leader_ready: false,
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
}
