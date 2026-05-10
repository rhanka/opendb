mod crd;
mod recovery;

use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client, CustomResourceExt, ResourceExt,
    api::{ListParams, Patch, PatchParams},
};

#[derive(Debug, Parser)]
#[command(author, version, about)]
enum Command {
    PrintCrd,
    Run {
        #[arg(long, env = "OPENDB_NAMESPACE", default_value = "opendb-system")]
        namespace: String,
        #[arg(
            long,
            env = "OPENDB_OPERATOR_RECONCILE_INTERVAL_MS",
            default_value_t = 5000
        )]
        reconcile_interval_ms: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    match Command::parse() {
        Command::PrintCrd => {
            println!("{}", serde_yaml::to_string(&crd::OpenDbCluster::crd())?);
        }
        Command::Run {
            namespace,
            reconcile_interval_ms,
        } => {
            run_operator(
                namespace,
                Duration::from_millis(reconcile_interval_ms.max(1)),
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_operator(namespace: String, reconcile_interval: Duration) -> Result<()> {
    let client = Client::try_default()
        .await
        .context("create kubernetes client for opendb operator")?;
    let clusters: Api<crd::OpenDbCluster> = Api::namespaced(client.clone(), &namespace);
    let pods: Api<Pod> = Api::namespaced(client, &namespace);
    let mut interval = tokio::time::interval(reconcile_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    tracing::info!(
        namespace = %namespace,
        reconcile_interval_ms = reconcile_interval.as_millis(),
        "opendb operator-lite started"
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(error) = reconcile_all(&clusters, &pods).await {
                    tracing::warn!(
                        error = ?error,
                        "opendb operator-lite reconcile failed; will retry"
                    );
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("wait for ctrl-c")?;
                tracing::info!("opendb operator-lite stopped");
                return Ok(());
            }
        }
    }
}

async fn reconcile_all(clusters: &Api<crd::OpenDbCluster>, pods: &Api<Pod>) -> Result<()> {
    let cluster_list = clusters
        .list(&ListParams::default())
        .await
        .context("list OpenDbCluster resources")?;
    if cluster_list.items.is_empty() {
        tracing::debug!("no OpenDbCluster resources found");
        return Ok(());
    }

    for cluster in cluster_list {
        let name = cluster.name_any();
        let observed_pods = pods
            .list(&ListParams::default().labels(&crd::open_db_pod_label_selector(&name)))
            .await
            .with_context(|| format!("list OpenDb pods for OpenDbCluster/{name}"))?
            .items
            .iter()
            .filter_map(observed_open_db_pod_from_kube)
            .collect::<Vec<_>>();
        let snapshot = crd::snapshot_from_observed_pods(cluster.spec.replicas, &observed_pods);
        let status = crd::compute_open_db_cluster_status(snapshot);
        let patch = status_merge_patch(&status);

        clusters
            .patch_status(&name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .with_context(|| format!("patch status for OpenDbCluster/{name}"))?;

        tracing::info!(
            cluster = %name,
            phase = %status.phase,
            ready_replicas = status.ready_replicas,
            leader_pod = status.leader_pod.as_deref().unwrap_or(""),
            "reconciled OpenDbCluster status"
        );
    }

    Ok(())
}

fn observed_open_db_pod_from_kube(pod: &Pod) -> Option<crd::ObservedOpenDbPod> {
    if pod.metadata.deletion_timestamp.is_some() {
        return None;
    }

    let name = pod.metadata.name.clone()?;
    let status = pod.status.as_ref();
    let node_running = status.is_some_and(|status| {
        status.phase.as_deref() == Some("Running")
            && status
                .container_statuses
                .as_deref()
                .unwrap_or_default()
                .iter()
                .any(|container| {
                    container.name == crd::NODE_CONTAINER_NAME
                        && container
                            .state
                            .as_ref()
                            .and_then(|state| state.running.as_ref())
                            .is_some()
                })
    });
    let leader_ready = status.is_some_and(|status| {
        status
            .conditions
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|condition| condition.type_ == "Ready" && condition.status == "True")
    });

    Some(crd::ObservedOpenDbPod {
        name,
        node_running,
        leader_ready,
    })
}

fn status_merge_patch(status: &crd::OpenDbClusterStatus) -> serde_json::Value {
    serde_json::json!({ "status": status })
}

#[cfg(test)]
mod tests {
    use super::{observed_open_db_pod_from_kube, status_merge_patch};
    use crate::crd::{NODE_CONTAINER_NAME, ObservedOpenDbPod, OpenDbClusterStatus};
    use k8s_openapi::api::core::v1::{
        ContainerState, ContainerStateRunning, ContainerStatus, Pod, PodCondition, PodStatus,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};

    #[test]
    fn observes_running_node_container_and_leader_ready_condition_independently() {
        let pod = pod(
            "opendb-0",
            Some("Running"),
            true,
            Some(PodCondition {
                type_: "Ready".to_string(),
                status: "True".to_string(),
                ..Default::default()
            }),
        );

        assert_eq!(
            observed_open_db_pod_from_kube(&pod),
            Some(ObservedOpenDbPod {
                name: "opendb-0".to_string(),
                node_running: true,
                leader_ready: true,
            })
        );
    }

    #[test]
    fn observes_running_follower_even_when_pod_ready_is_false() {
        let pod = pod(
            "opendb-1",
            Some("Running"),
            true,
            Some(PodCondition {
                type_: "Ready".to_string(),
                status: "False".to_string(),
                ..Default::default()
            }),
        );

        assert_eq!(
            observed_open_db_pod_from_kube(&pod),
            Some(ObservedOpenDbPod {
                name: "opendb-1".to_string(),
                node_running: true,
                leader_ready: false,
            })
        );
    }

    #[test]
    fn status_patch_is_a_merge_patch_under_status_key() {
        let patch = status_merge_patch(&OpenDbClusterStatus {
            ready_replicas: 1,
            phase: "Ready".to_string(),
            leader_pod: Some("opendb-0".to_string()),
            conditions: Vec::new(),
        });

        assert_eq!(patch["status"]["readyReplicas"], 1);
        assert_eq!(patch["status"]["phase"], "Ready");
        assert_eq!(patch["status"]["leaderPod"], "opendb-0");
    }

    #[test]
    fn status_patch_clears_missing_leader_pod() {
        let patch = status_merge_patch(&OpenDbClusterStatus {
            ready_replicas: 0,
            phase: "Pending".to_string(),
            leader_pod: None,
            conditions: Vec::new(),
        });
        let status = patch["status"].as_object().expect("status object");

        assert!(status.contains_key("leaderPod"));
        assert!(status["leaderPod"].is_null());
    }

    #[test]
    fn ignores_terminating_pods() {
        let mut pod = pod(
            "opendb-0",
            Some("Running"),
            true,
            Some(PodCondition {
                type_: "Ready".to_string(),
                status: "True".to_string(),
                ..Default::default()
            }),
        );
        pod.metadata.deletion_timestamp = Some(Time(k8s_openapi::chrono::Utc::now()));

        assert_eq!(observed_open_db_pod_from_kube(&pod), None);
    }

    fn pod(
        name: &str,
        phase: Option<&str>,
        container_running: bool,
        ready_condition: Option<PodCondition>,
    ) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: phase.map(str::to_string),
                container_statuses: Some(vec![ContainerStatus {
                    name: NODE_CONTAINER_NAME.to_string(),
                    image: "opendb-node:dev".to_string(),
                    image_id: "opendb-node:dev".to_string(),
                    ready: false,
                    restart_count: 0,
                    state: Some(ContainerState {
                        running: container_running.then_some(ContainerStateRunning::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                conditions: ready_condition.map(|condition| vec![condition]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}
