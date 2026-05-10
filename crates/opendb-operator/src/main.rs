mod crd;
mod recovery;

use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client, CustomResourceExt, ResourceExt,
    api::{ListParams, Patch, PatchParams},
};

const DEFAULT_STATUS_TIMEOUT_MS: u64 = 2000;
const MIN_STATUS_TIMEOUT_MS: u64 = 100;

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
        #[arg(
            long,
            env = "OPENDB_OPERATOR_STATUS_TIMEOUT_MS",
            default_value_t = DEFAULT_STATUS_TIMEOUT_MS,
        )]
        status_timeout_ms: u64,
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
            status_timeout_ms,
        } => {
            run_operator(
                namespace,
                Duration::from_millis(reconcile_interval_ms.max(1)),
                Duration::from_millis(status_timeout_ms.max(MIN_STATUS_TIMEOUT_MS)),
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_operator(
    namespace: String,
    reconcile_interval: Duration,
    status_timeout: Duration,
) -> Result<()> {
    let client = Client::try_default()
        .await
        .context("create kubernetes client for opendb operator")?;
    let clusters: Api<crd::OpenDbCluster> = Api::namespaced(client.clone(), &namespace);
    let pods: Api<Pod> = Api::namespaced(client, &namespace);
    let fetcher = recovery::HttpRecoveryStatusFetcher::new(status_timeout);
    let mut interval = tokio::time::interval(reconcile_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    tracing::info!(
        namespace = %namespace,
        reconcile_interval_ms = reconcile_interval.as_millis(),
        status_timeout_ms = status_timeout.as_millis(),
        "opendb operator-lite started"
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(error) = reconcile_all(&clusters, &pods, &fetcher).await {
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

async fn reconcile_all<F>(
    clusters: &Api<crd::OpenDbCluster>,
    pods: &Api<Pod>,
    fetcher: &F,
) -> Result<()>
where
    F: recovery::RecoveryStatusFetcher,
{
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
        let health_port = clamp_health_port(cluster.spec.health_port);
        let recoveries = collect_pod_recovery(&observed_pods, health_port, fetcher).await;
        let aggregate = recovery::aggregate_cluster_recovery(&recoveries);
        let snapshot = crd::snapshot_from_observed_pods(cluster.spec.replicas, &observed_pods);
        let status = crd::compute_open_db_cluster_status_with_recovery(snapshot, aggregate);
        let patch = status_merge_patch(&status);

        clusters
            .patch_status(&name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .with_context(|| format!("patch status for OpenDbCluster/{name}"))?;

        let recovery_phase = status
            .conditions
            .iter()
            .find(|c| c.r#type == "Recovered")
            .map(|c| c.status.clone())
            .unwrap_or_else(|| "unknown".to_string());

        tracing::info!(
            cluster = %name,
            phase = %status.phase,
            ready_replicas = status.ready_replicas,
            leader_pod = status.leader_pod.as_deref().unwrap_or(""),
            recovered = %recovery_phase,
            "reconciled OpenDbCluster status"
        );
    }

    Ok(())
}

async fn collect_pod_recovery<F>(
    pods: &[crd::ObservedOpenDbPod],
    health_port: u16,
    fetcher: &F,
) -> Vec<recovery::ObservedPodRecovery>
where
    F: recovery::RecoveryStatusFetcher,
{
    let mut tasks = FuturesUnordered::new();
    for pod in pods {
        let pod_name = pod.name.clone();
        let pod_running = pod.node_running;
        let pod_ip = pod.pod_ip.clone();
        tasks.push(async move {
            let status = match (pod_running, pod_ip.as_deref()) {
                (true, Some(ip)) => match fetcher.fetch(&pod_name, ip, health_port).await {
                    Ok(status) => Ok(status),
                    Err(error) => Err(recovery::summarize_fetch_error(&error)),
                },
                (true, None) => Err(recovery::FetchErrorSummary::Unreachable),
                (false, _) => Err(recovery::FetchErrorSummary::Unreachable),
            };
            recovery::ObservedPodRecovery {
                name: pod_name,
                running: pod_running,
                status,
            }
        });
    }

    let mut out = Vec::new();
    while let Some(observed) = tasks.next().await {
        out.push(observed);
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

fn clamp_health_port(port: i32) -> u16 {
    port.clamp(1, u16::MAX as i32) as u16
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
    let pod_ip = status
        .and_then(|status| status.pod_ip.as_ref())
        .map(|ip| ip.trim().to_string())
        .filter(|ip| !ip.is_empty());

    Some(crd::ObservedOpenDbPod {
        name,
        node_running,
        leader_ready,
        pod_ip,
    })
}

fn status_merge_patch(status: &crd::OpenDbClusterStatus) -> serde_json::Value {
    serde_json::json!({ "status": status })
}

#[cfg(test)]
mod tests {
    use super::{
        clamp_health_port, collect_pod_recovery, observed_open_db_pod_from_kube, status_merge_patch,
    };
    use crate::crd::{NODE_CONTAINER_NAME, ObservedOpenDbPod, OpenDbClusterStatus};
    use crate::recovery::{
        FetchError, FetchErrorSummary, PodRecoveryStatus, RecoveryStatusFetcher,
    };
    use k8s_openapi::api::core::v1::{
        ContainerState, ContainerStateRunning, ContainerStatus, Pod, PodCondition, PodStatus,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
    use std::sync::Mutex;

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
                pod_ip: None,
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
                pod_ip: None,
            })
        );
    }

    #[test]
    fn observes_pod_ip_when_present() {
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
        if let Some(status) = pod.status.as_mut() {
            status.pod_ip = Some("10.0.0.1".to_string());
        }

        let observed = observed_open_db_pod_from_kube(&pod).expect("observed");
        assert_eq!(observed.pod_ip.as_deref(), Some("10.0.0.1"));
    }

    #[test]
    fn observes_empty_pod_ip_as_missing() {
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
        if let Some(status) = pod.status.as_mut() {
            status.pod_ip = Some("   ".to_string());
        }

        let observed = observed_open_db_pod_from_kube(&pod).expect("observed");
        assert_eq!(observed.pod_ip, None);
    }

    #[test]
    fn clamp_health_port_handles_negative_and_huge_values() {
        assert_eq!(clamp_health_port(-1), 1);
        assert_eq!(clamp_health_port(0), 1);
        assert_eq!(clamp_health_port(8080), 8080);
        assert_eq!(clamp_health_port(70_000), u16::MAX);
    }

    #[test]
    fn status_patch_is_a_merge_patch_under_status_key() {
        let patch = status_merge_patch(&OpenDbClusterStatus {
            ready_replicas: 1,
            phase: "Ready".to_string(),
            leader_pod: Some("opendb-0".to_string()),
            recovery: None,
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
            recovery: None,
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

    struct StubFetcher {
        calls: Mutex<Vec<(String, String, u16)>>,
        result: PodRecoveryStatus,
    }

    #[async_trait::async_trait]
    impl RecoveryStatusFetcher for StubFetcher {
        async fn fetch(
            &self,
            pod_name: &str,
            pod_ip: &str,
            port: u16,
        ) -> Result<PodRecoveryStatus, FetchError> {
            self.calls
                .lock()
                .expect("lock")
                .push((pod_name.to_string(), pod_ip.to_string(), port));
            Ok(self.result.clone())
        }
    }

    fn observed(name: &str, running: bool, ip: Option<&str>) -> ObservedOpenDbPod {
        ObservedOpenDbPod {
            name: name.to_string(),
            node_running: running,
            leader_ready: false,
            pod_ip: ip.map(str::to_string),
        }
    }

    #[tokio::test]
    async fn collect_pod_recovery_dials_running_pods_with_ip() {
        let pods = vec![
            observed("opendb-0", true, Some("10.0.0.1")),
            observed("opendb-1", true, Some("10.0.0.2")),
        ];

        let stub = StubFetcher {
            calls: Mutex::new(Vec::new()),
            result: PodRecoveryStatus {
                root_descriptor_known: true,
                wal_replay_completed: true,
                last_replayed_tx_id: Some(2),
                last_replayed_ts: Some(2),
                archive_metadata_replayed: true,
                latest_recovery_artifact: None,
            },
        };

        let observed = collect_pod_recovery(&pods, 8080, &stub).await;

        let mut calls = stub.calls.lock().expect("lock").clone();
        calls.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            calls,
            vec![
                ("opendb-0".to_string(), "10.0.0.1".to_string(), 8080),
                ("opendb-1".to_string(), "10.0.0.2".to_string(), 8080),
            ]
        );
        assert_eq!(observed.len(), 2);
        assert!(observed[0].status.is_ok());
        assert!(observed[1].status.is_ok());
    }

    #[tokio::test]
    async fn collect_pod_recovery_marks_running_pod_without_ip_as_unreachable() {
        let pods = vec![observed("opendb-0", true, None)];
        let stub = StubFetcher {
            calls: Mutex::new(Vec::new()),
            result: PodRecoveryStatus::default(),
        };

        let observed = collect_pod_recovery(&pods, 8080, &stub).await;

        assert_eq!(observed.len(), 1);
        assert!(matches!(
            observed[0].status,
            Err(FetchErrorSummary::Unreachable)
        ));
        assert!(stub.calls.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn collect_pod_recovery_skips_dial_for_non_running_pods() {
        let pods = vec![observed("opendb-0", false, Some("10.0.0.1"))];
        let stub = StubFetcher {
            calls: Mutex::new(Vec::new()),
            result: PodRecoveryStatus::default(),
        };

        let _ = collect_pod_recovery(&pods, 8080, &stub).await;

        assert!(stub.calls.lock().expect("lock").is_empty());
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
