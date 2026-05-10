# OpenDB Milestone 2 Sprint 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the recovery contract introduced in Sprint 2 visible end-to-end through `kubectl` by having the operator-lite consume each node's `/status` endpoint and reflect it as standard Kubernetes conditions plus a recovery summary on `OpenDbCluster.status`. Add an opt-in restart recovery UAT to `tools/k3s-smoke.ts` while keeping the default smoke non-destructive.

**Architecture:** The canonical commit stream remains untouched. Sprint 3 is purely additive on the operator and smoke tooling: a per-pod recovery fetcher, an aggregation function, four new conditions, a `recovery` summary block, and an opt-in flag in the smoke tool. No new mutation, no new range catalog change, no object storage client.

**Tech Stack:** Rust, Tokio, Serde, serde_json, kube, k8s-openapi, schemars, TypeScript, Vitest, tsx, Kubernetes/k3s manifests. No Python.

---

## Source Spec

Implement the approved design:

- `docs/superpowers/specs/2026-05-09-opendb-milestone-2-sprint-3-design.md`

## File Structure

- `crates/opendb-operator/src/crd.rs`
  Owns `OpenDbClusterStatus`, conditions, the new optional `OpenDbClusterRecoverySummary` block, the four new condition emitters, and `lastTransitionTime` on conditions.
- `crates/opendb-operator/src/recovery.rs` (new)
  Owns `PodRecoveryStatus`, `RecoveryStatusFetcher` trait, `HttpRecoveryStatusFetcher` production impl, and `aggregate_cluster_recovery`.
- `crates/opendb-operator/src/main.rs`
  Wires the fetcher and aggregator into the existing reconcile loop and patches the new `recovery` block + conditions onto the CRD status.
- `crates/opendb-operator/Cargo.toml`
  Adds `chrono` (already a transitive dep via k8s-openapi) and `futures` (for concurrent fetches) only if not already present in workspace deps; otherwise no change.
- `tools/k3s-smoke.ts`
  Adds `--with-restart-recovery` flag, the restart-recovery execution path, and updates the printed plan to mention the flag and non-destructive default.
- `tests/cluster/k3s-smoke.test.ts`
  Asserts default plan/run is non-destructive and that the restart-recovery flag is documented.
- `tests/cluster/restart-recovery.test.ts` (new)
  Exercises the new flag's option parsing and the wait-for-Recovered helper against fake `kubectl get` outputs.
- `docs/k3s-uat.md`
  Documents the new flag, kube-visible conditions, and the unchanged non-destructive default.
- `deploy/k8s/base/opendb-cluster.yaml`
  No spec change, but the generated CRD JSON-Schema must continue to validate `npm run check:manifests` after the new optional fields are added.

## Task 1: Per-Pod Recovery Status Fetcher

**Ownership:** One worker owns `crates/opendb-operator/src/recovery.rs`, the fetcher trait, the production HTTP impl, and the related unit tests for this task. Other workers must not modify `crd.rs` or `main.rs` until Task 1 lands.

**Files:**
- Create: `crates/opendb-operator/src/recovery.rs`
- Modify: `crates/opendb-operator/src/main.rs` (only to register the new module)

- [ ] **Step 1: Add failing fetcher tests**

Create `crates/opendb-operator/src/recovery.rs` and add an initial module that compiles with the trait alone. Then add the following failing tests at the bottom of the module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    async fn spawn_fake_status_server(body: &'static str, status: u16) -> u16 {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind fake /status server");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let response = format!(
                    "HTTP/1.1 {status} OK\r\ncontent-length: {len}\r\nconnection: close\r\n\r\n{body}",
                    len = body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        port
    }

    #[tokio::test]
    async fn http_fetcher_parses_recovery_status() {
        let port = spawn_fake_status_server(
            r#"{"rootDescriptorKnown":true,"walReplayCompleted":true,"lastReplayedTxId":7,"lastReplayedTs":7,"archiveMetadataReplayed":true,"latestRecoveryArtifact":null}"#,
            200,
        )
        .await;

        let fetcher = HttpRecoveryStatusFetcher::new(std::time::Duration::from_secs(2));
        let status = fetcher
            .fetch("opendb-0", "127.0.0.1", port)
            .await
            .expect("recovery status fetch");

        assert!(status.root_descriptor_known);
        assert!(status.wal_replay_completed);
        assert_eq!(status.last_replayed_tx_id, Some(7));
        assert_eq!(status.last_replayed_ts, Some(7));
        assert!(status.archive_metadata_replayed);
        assert_eq!(status.latest_recovery_artifact, None);
    }

    #[tokio::test]
    async fn http_fetcher_rejects_non_200() {
        let port = spawn_fake_status_server("not found\n", 404).await;

        let fetcher = HttpRecoveryStatusFetcher::new(std::time::Duration::from_secs(2));
        let error = fetcher
            .fetch("opendb-0", "127.0.0.1", port)
            .await
            .expect_err("non-200 must be a fetch error");

        assert!(matches!(error, FetchError::HttpStatus(404)));
    }

    #[tokio::test]
    async fn http_fetcher_rejects_unknown_field_in_body() {
        let port = spawn_fake_status_server(
            r#"{"rootDescriptorKnown":true,"walReplayCompleted":true,"lastReplayedTxId":1,"lastReplayedTs":1,"archiveMetadataReplayed":true,"latestRecoveryArtifact":null,"surprise":true}"#,
            200,
        )
        .await;

        let fetcher = HttpRecoveryStatusFetcher::new(std::time::Duration::from_secs(2));
        let error = fetcher
            .fetch("opendb-0", "127.0.0.1", port)
            .await
            .expect_err("unknown field must fail strict decode");

        assert!(matches!(error, FetchError::Decode(_)));
    }
}
```

Run:

```bash
cargo test -p opendb-operator http_fetcher
```

Expected before implementation: all three fail to compile because the types do not exist yet.

- [ ] **Step 2: Implement the fetcher trait and types**

Add to `crates/opendb-operator/src/recovery.rs`:

```rust
use std::time::Duration;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PodRecoveryStatus {
    pub root_descriptor_known: bool,
    pub wal_replay_completed: bool,
    pub last_replayed_tx_id: Option<u64>,
    pub last_replayed_ts: Option<u64>,
    pub archive_metadata_replayed: bool,
    pub latest_recovery_artifact: Option<String>,
}

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("non-200 status: {0}")]
    HttpStatus(u16),
    #[error("malformed http response")]
    MalformedResponse,
    #[error("decode: {0}")]
    Decode(serde_json::Error),
    #[error("timeout")]
    Timeout,
}

#[async_trait::async_trait]
pub trait RecoveryStatusFetcher: Send + Sync {
    async fn fetch(
        &self,
        pod_name: &str,
        pod_ip: &str,
        port: u16,
    ) -> Result<PodRecoveryStatus, FetchError>;
}

#[derive(Clone, Debug)]
pub struct HttpRecoveryStatusFetcher {
    timeout: Duration,
}

impl HttpRecoveryStatusFetcher {
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }
}

#[async_trait::async_trait]
impl RecoveryStatusFetcher for HttpRecoveryStatusFetcher {
    async fn fetch(
        &self,
        _pod_name: &str,
        pod_ip: &str,
        port: u16,
    ) -> Result<PodRecoveryStatus, FetchError> {
        let request = format!(
            "GET /status HTTP/1.1\r\nhost: {pod_ip}:{port}\r\nconnection: close\r\n\r\n"
        );
        let conn = tokio::time::timeout(
            self.timeout,
            TcpStream::connect((pod_ip, port)),
        )
        .await
        .map_err(|_| FetchError::Timeout)??;

        let result = tokio::time::timeout(self.timeout, async move {
            let mut conn = conn;
            conn.write_all(request.as_bytes()).await?;
            let mut buffer = Vec::with_capacity(4096);
            conn.read_to_end(&mut buffer).await?;
            Ok::<_, std::io::Error>(buffer)
        })
        .await
        .map_err(|_| FetchError::Timeout)??;

        let (status_code, body) = parse_http_response(&result)?;
        if status_code != 200 {
            return Err(FetchError::HttpStatus(status_code));
        }
        serde_json::from_slice::<PodRecoveryStatus>(body).map_err(FetchError::Decode)
    }
}

fn parse_http_response(buffer: &[u8]) -> Result<(u16, &[u8]), FetchError> {
    let header_end = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or(FetchError::MalformedResponse)?;
    let header_text = std::str::from_utf8(&buffer[..header_end])
        .map_err(|_| FetchError::MalformedResponse)?;
    let mut header_lines = header_text.split("\r\n");
    let status_line = header_lines.next().ok_or(FetchError::MalformedResponse)?;
    let mut status_parts = status_line.split_whitespace();
    let _http = status_parts.next().ok_or(FetchError::MalformedResponse)?;
    let code: u16 = status_parts
        .next()
        .ok_or(FetchError::MalformedResponse)?
        .parse()
        .map_err(|_| FetchError::MalformedResponse)?;
    Ok((code, &buffer[header_end + 4..]))
}
```

Add `async-trait`, `thiserror`, and (if not already present) `serde`/`serde_json`/`tokio` to `crates/opendb-operator/Cargo.toml`. Prefer reusing workspace deps. Note that `serde_json::Error` is `!Clone`, so `FetchError::Decode` carries it directly.

In `crates/opendb-operator/src/main.rs`, add `mod recovery;` near the existing `mod crd;`.

Run:

```bash
cargo test -p opendb-operator http_fetcher
```

Expected: all three pass.

- [ ] **Step 3: Add aggregator unit tests**

Add to `crates/opendb-operator/src/recovery.rs`:

```rust
#[cfg(test)]
mod aggregate_tests {
    use super::*;

    fn ok_pod(name: &str, tx: u64) -> ObservedPodRecovery {
        ObservedPodRecovery {
            name: name.to_string(),
            running: true,
            status: Ok(PodRecoveryStatus {
                root_descriptor_known: true,
                wal_replay_completed: true,
                last_replayed_tx_id: Some(tx),
                last_replayed_ts: Some(tx),
                archive_metadata_replayed: true,
                latest_recovery_artifact: None,
            }),
        }
    }

    #[test]
    fn aggregate_is_empty_when_no_running_pods() {
        let aggregate = aggregate_cluster_recovery(&[]);
        assert!(aggregate.is_none());
    }

    #[test]
    fn aggregate_reports_max_tx_and_ts_across_running_pods() {
        let pods = vec![ok_pod("opendb-0", 3), ok_pod("opendb-1", 7), ok_pod("opendb-2", 5)];
        let aggregate = aggregate_cluster_recovery(&pods).expect("aggregate");
        assert_eq!(aggregate.last_replayed_tx_id, Some(7));
        assert_eq!(aggregate.last_replayed_ts, Some(7));
        assert_eq!(aggregate.observed_running_pods, 3);
        assert_eq!(aggregate.unreachable_pods, 0);
    }

    #[test]
    fn aggregate_marks_unreachable_when_status_is_err() {
        let pods = vec![
            ok_pod("opendb-0", 3),
            ObservedPodRecovery {
                name: "opendb-1".to_string(),
                running: true,
                status: Err(FetchErrorSummary::Unreachable),
            },
        ];
        let aggregate = aggregate_cluster_recovery(&pods).expect("aggregate");
        assert_eq!(aggregate.unreachable_pods, 1);
    }

    #[test]
    fn aggregate_uses_smallest_pod_name_for_latest_recovery_artifact() {
        let mut pods = vec![ok_pod("opendb-2", 1), ok_pod("opendb-0", 1), ok_pod("opendb-1", 1)];
        for pod in pods.iter_mut() {
            if let Ok(status) = &mut pod.status {
                status.latest_recovery_artifact = Some(format!("artifact-{}", pod.name));
            }
        }
        let aggregate = aggregate_cluster_recovery(&pods).expect("aggregate");
        assert_eq!(
            aggregate.latest_recovery_artifact.as_deref(),
            Some("artifact-opendb-0")
        );
    }
}
```

Add the supporting types if not yet present:

```rust
#[derive(Clone, Debug, PartialEq)]
pub enum FetchErrorSummary {
    Unreachable,
    HttpStatus(u16),
    MalformedResponse,
    Decode,
    Timeout,
}

#[derive(Clone, Debug)]
pub struct ObservedPodRecovery {
    pub name: String,
    pub running: bool,
    pub status: Result<PodRecoveryStatus, FetchErrorSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterRecoveryAggregate {
    pub observed_running_pods: i32,
    pub root_descriptor_known_pods: i32,
    pub wal_replay_completed_pods: i32,
    pub archive_metadata_replayed_pods: i32,
    pub unreachable_pods: i32,
    pub last_replayed_tx_id: Option<u64>,
    pub last_replayed_ts: Option<u64>,
    pub latest_recovery_artifact: Option<String>,
}
```

Run:

```bash
cargo test -p opendb-operator aggregate
```

Expected before implementation: tests fail because `aggregate_cluster_recovery` is unimplemented.

- [ ] **Step 4: Implement the aggregator**

Add:

```rust
pub fn aggregate_cluster_recovery(
    observed: &[ObservedPodRecovery],
) -> Option<ClusterRecoveryAggregate> {
    let running: Vec<&ObservedPodRecovery> = observed.iter().filter(|pod| pod.running).collect();
    if running.is_empty() {
        return None;
    }

    let mut root = 0;
    let mut wal = 0;
    let mut archive = 0;
    let mut unreachable = 0;
    let mut last_tx: Option<u64> = None;
    let mut last_ts: Option<u64> = None;
    let mut artifact_candidates: Vec<(String, String)> = Vec::new();

    for pod in &running {
        match &pod.status {
            Ok(status) => {
                if status.root_descriptor_known {
                    root += 1;
                }
                if status.wal_replay_completed {
                    wal += 1;
                }
                if status.archive_metadata_replayed {
                    archive += 1;
                }
                last_tx = max_option(last_tx, status.last_replayed_tx_id);
                last_ts = max_option(last_ts, status.last_replayed_ts);
                if let Some(artifact) = &status.latest_recovery_artifact {
                    artifact_candidates.push((pod.name.clone(), artifact.clone()));
                }
            }
            Err(_) => unreachable += 1,
        }
    }

    artifact_candidates.sort_by(|a, b| a.0.cmp(&b.0));
    let latest_recovery_artifact = artifact_candidates.into_iter().next().map(|(_, a)| a);

    Some(ClusterRecoveryAggregate {
        observed_running_pods: running.len() as i32,
        root_descriptor_known_pods: root,
        wal_replay_completed_pods: wal,
        archive_metadata_replayed_pods: archive,
        unreachable_pods: unreachable,
        last_replayed_tx_id: last_tx,
        last_replayed_ts: last_ts,
        latest_recovery_artifact,
    })
}

fn max_option(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}
```

Run:

```bash
cargo test -p opendb-operator aggregate
cargo test -p opendb-operator http_fetcher
cargo fmt --all -- --check
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/opendb-operator/src/recovery.rs crates/opendb-operator/src/main.rs crates/opendb-operator/Cargo.toml
git commit -m "feat: add operator recovery status fetcher and aggregator"
git push origin HEAD:main
```

## Task 2: CRD Status Shape With Recovery Conditions

**Ownership:** One worker owns `crates/opendb-operator/src/crd.rs` for this task after Task 1 lands. Do not edit `recovery.rs` or `main.rs` here.

**Files:**
- Modify: `crates/opendb-operator/src/crd.rs`

- [ ] **Step 1: Add failing tests for new conditions and recovery summary**

Add to the existing test module in `crd.rs`:

```rust
#[test]
fn status_emits_recovered_unknown_when_any_running_pod_is_unreachable() {
    let aggregate = ClusterRecoveryAggregate {
        observed_running_pods: 3,
        root_descriptor_known_pods: 2,
        wal_replay_completed_pods: 2,
        archive_metadata_replayed_pods: 2,
        unreachable_pods: 1,
        last_replayed_tx_id: Some(7),
        last_replayed_ts: Some(7),
        latest_recovery_artifact: None,
    };

    let status = compute_open_db_cluster_status_with_recovery(
        OpenDbClusterStatusSnapshot {
            desired_replicas: 3,
            ready_pods: 3,
            leader_pod: Some("opendb-0".to_string()),
        },
        Some(aggregate),
    );

    let by_type = condition_map(&status.conditions);
    assert_eq!(by_type["RootDescriptorKnown"].status, "Unknown");
    assert_eq!(by_type["WalReplayCompleted"].status, "Unknown");
    assert_eq!(by_type["ArchiveMetadataKnown"].status, "Unknown");
    assert_eq!(by_type["Recovered"].status, "Unknown");
    assert_eq!(by_type["Recovered"].reason, "PodsUnreachable");
}

#[test]
fn status_emits_recovered_true_when_all_running_pods_report_recovery_and_ready() {
    let aggregate = ClusterRecoveryAggregate {
        observed_running_pods: 3,
        root_descriptor_known_pods: 3,
        wal_replay_completed_pods: 3,
        archive_metadata_replayed_pods: 3,
        unreachable_pods: 0,
        last_replayed_tx_id: Some(7),
        last_replayed_ts: Some(7),
        latest_recovery_artifact: None,
    };

    let status = compute_open_db_cluster_status_with_recovery(
        OpenDbClusterStatusSnapshot {
            desired_replicas: 3,
            ready_pods: 3,
            leader_pod: Some("opendb-0".to_string()),
        },
        Some(aggregate.clone()),
    );

    let by_type = condition_map(&status.conditions);
    assert_eq!(by_type["RootDescriptorKnown"].status, "True");
    assert_eq!(by_type["WalReplayCompleted"].status, "True");
    assert_eq!(by_type["ArchiveMetadataKnown"].status, "True");
    assert_eq!(by_type["Recovered"].status, "True");
    let recovery = status.recovery.expect("recovery summary present");
    assert_eq!(recovery.last_replayed_tx_id, Some(7));
}

#[test]
fn status_emits_recovered_false_when_any_running_pod_reports_false() {
    let aggregate = ClusterRecoveryAggregate {
        observed_running_pods: 3,
        root_descriptor_known_pods: 3,
        wal_replay_completed_pods: 2,
        archive_metadata_replayed_pods: 3,
        unreachable_pods: 0,
        last_replayed_tx_id: Some(7),
        last_replayed_ts: Some(7),
        latest_recovery_artifact: None,
    };

    let status = compute_open_db_cluster_status_with_recovery(
        OpenDbClusterStatusSnapshot {
            desired_replicas: 3,
            ready_pods: 3,
            leader_pod: Some("opendb-0".to_string()),
        },
        Some(aggregate),
    );

    let by_type = condition_map(&status.conditions);
    assert_eq!(by_type["WalReplayCompleted"].status, "False");
    assert_eq!(by_type["Recovered"].status, "False");
    assert_eq!(by_type["Recovered"].reason, "WalReplayIncomplete");
}

#[test]
fn status_recovery_block_is_omitted_when_no_running_pods_observed() {
    let status = compute_open_db_cluster_status_with_recovery(
        OpenDbClusterStatusSnapshot {
            desired_replicas: 3,
            ready_pods: 0,
            leader_pod: None,
        },
        None,
    );

    assert!(status.recovery.is_none());
    let json = serde_json::to_value(&status).expect("serialize");
    assert!(json.get("recovery").is_none());
}

#[test]
fn condition_carries_last_transition_time() {
    let aggregate = ClusterRecoveryAggregate {
        observed_running_pods: 1,
        root_descriptor_known_pods: 1,
        wal_replay_completed_pods: 1,
        archive_metadata_replayed_pods: 1,
        unreachable_pods: 0,
        last_replayed_tx_id: Some(1),
        last_replayed_ts: Some(1),
        latest_recovery_artifact: None,
    };

    let now = chrono::Utc::now();
    let status = compute_open_db_cluster_status_at(
        OpenDbClusterStatusSnapshot {
            desired_replicas: 1,
            ready_pods: 1,
            leader_pod: Some("opendb-0".to_string()),
        },
        Some(aggregate),
        now,
    );

    for condition in &status.conditions {
        assert_eq!(
            condition.last_transition_time.as_ref().map(|t| t.0),
            Some(now)
        );
    }
}
```

Add a small `condition_map` test helper in the same module:

```rust
fn condition_map(conditions: &[OpenDbClusterCondition]) -> std::collections::BTreeMap<String, OpenDbClusterCondition> {
    conditions
        .iter()
        .map(|condition| (condition.r#type.clone(), condition.clone()))
        .collect()
}
```

Pull `ClusterRecoveryAggregate` from `super::super::recovery` (`use crate::recovery::ClusterRecoveryAggregate`).

Run:

```bash
cargo test -p opendb-operator status_emits
cargo test -p opendb-operator recovery_block
cargo test -p opendb-operator condition_carries_last_transition_time
```

Expected before implementation: fail because the new types and signatures are missing.

- [ ] **Step 2: Extend status types and condition emitter**

Update `OpenDbClusterStatus`:

```rust
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

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenDbClusterCondition {
    #[serde(rename = "type")]
    pub r#type: String,
    pub status: String,
    pub reason: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<k8s_openapi::apimachinery::pkg::apis::meta::v1::Time>,
}
```

Add the new emitter:

```rust
pub fn compute_open_db_cluster_status_with_recovery(
    snapshot: OpenDbClusterStatusSnapshot,
    recovery: Option<crate::recovery::ClusterRecoveryAggregate>,
) -> OpenDbClusterStatus {
    compute_open_db_cluster_status_at(snapshot, recovery, chrono::Utc::now())
}

pub fn compute_open_db_cluster_status_at(
    snapshot: OpenDbClusterStatusSnapshot,
    recovery: Option<crate::recovery::ClusterRecoveryAggregate>,
    now: chrono::DateTime<chrono::Utc>,
) -> OpenDbClusterStatus {
    let mut status = compute_open_db_cluster_status(snapshot);
    let stamp = k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(now);
    for condition in status.conditions.iter_mut() {
        condition.last_transition_time = Some(stamp.clone());
    }

    let Some(aggregate) = recovery else {
        return status;
    };

    let ready_condition_true = status
        .conditions
        .iter()
        .any(|c| c.r#type == "Ready" && c.status == "True");

    let recovery_summary = OpenDbClusterRecoverySummary {
        root_descriptor_known_replicas: aggregate.root_descriptor_known_pods,
        wal_replay_completed_replicas: aggregate.wal_replay_completed_pods,
        archive_metadata_replayed_replicas: aggregate.archive_metadata_replayed_pods,
        unreachable_replicas: aggregate.unreachable_pods,
        last_replayed_tx_id: aggregate.last_replayed_tx_id,
        last_replayed_ts: aggregate.last_replayed_ts,
        latest_recovery_artifact: aggregate.latest_recovery_artifact.clone(),
    };
    status.recovery = Some(recovery_summary);

    let triple = recovery_condition_triple(&aggregate);
    status.conditions.push(condition_for("RootDescriptorKnown", triple.root, &stamp));
    status.conditions.push(condition_for("WalReplayCompleted", triple.wal, &stamp));
    status.conditions.push(condition_for("ArchiveMetadataKnown", triple.archive, &stamp));
    status.conditions.push(recovered_condition(&triple, ready_condition_true, &stamp));

    status
}
```

Add the small private helper functions `recovery_condition_triple`, `condition_for`, and `recovered_condition`. The triple computes per-feature `Status` / `Reason` / `Message`. `Recovered` reasons:

- `RecoveredAndReady` when all three subconditions are True and Ready is True;
- `WalReplayIncomplete` / `RootDescriptorMissing` / `ArchiveMetadataIncomplete` when one or more subconditions are False;
- `PodsUnreachable` when any subcondition is Unknown;
- `NotReady` when subconditions are True but Ready is False.

Add `chrono` to `crates/opendb-operator/Cargo.toml` if not already pulled in via `k8s-openapi`. Workspace already exposes `k8s-openapi`, which re-exports `chrono::DateTime<chrono::Utc>` via `Time`; verify with `cargo build -p opendb-operator` and add a direct dependency only if needed for `Utc::now()` in tests.

- [ ] **Step 3: Update the existing pre-recovery emitter to keep semantics**

Inside `compute_open_db_cluster_status`, leave the legacy two-condition behavior unchanged. The new emitter wraps it. Existing tests in `crd.rs` must keep passing without modification.

Run:

```bash
cargo test -p opendb-operator
cargo fmt --all -- --check
```

Expected: all tests pass, including the original Sprint 1/2 conditions tests.

- [ ] **Step 4: Commit**

```bash
git add crates/opendb-operator/src/crd.rs crates/opendb-operator/Cargo.toml
git commit -m "feat: emit recovery conditions on opendb cluster status"
git push origin HEAD:main
```

## Task 3: Wire Reconcile Loop Through The Recovery Fetcher

**Ownership:** One worker owns `crates/opendb-operator/src/main.rs` for this task after Tasks 1 and 2 land.

**Files:**
- Modify: `crates/opendb-operator/src/main.rs`

- [ ] **Step 1: Add failing reconcile-level test**

Add to the test module in `main.rs`:

```rust
#[tokio::test]
async fn reconcile_uses_pod_ip_to_fetch_status_and_emits_recovered_condition() {
    use crate::recovery::{FetchErrorSummary, ObservedPodRecovery, PodRecoveryStatus, RecoveryStatusFetcher};
    use std::sync::Mutex;

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
        ) -> Result<PodRecoveryStatus, crate::recovery::FetchError> {
            self.calls
                .lock()
                .expect("lock")
                .push((pod_name.to_string(), pod_ip.to_string(), port));
            Ok(self.result.clone())
        }
    }

    let pods = vec![
        observed_pod("opendb-0", true, true, Some("10.0.0.1")),
        observed_pod("opendb-1", true, false, Some("10.0.0.2")),
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

    let calls = stub.calls.lock().expect("lock").clone();
    assert_eq!(
        calls,
        vec![
            ("opendb-0".to_string(), "10.0.0.1".to_string(), 8080),
            ("opendb-1".to_string(), "10.0.0.2".to_string(), 8080),
        ]
    );
    assert_eq!(observed.len(), 2);
    assert!(matches!(observed[0].status, Ok(_)));
}
```

Add a small in-test helper `observed_pod(name, running, leader_ready, ip)`.

Run:

```bash
cargo test -p opendb-operator reconcile_uses_pod_ip
```

Expected before implementation: fail because `collect_pod_recovery` does not exist and `ObservedOpenDbPod` does not yet carry an IP.

- [ ] **Step 2: Extend ObservedOpenDbPod with pod IP**

In `crd.rs`:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedOpenDbPod {
    pub name: String,
    pub node_running: bool,
    pub leader_ready: bool,
    pub pod_ip: Option<String>,
}
```

In `main.rs`, populate `pod_ip` from `pod.status.pod_ip`. Pre-existing tests construct `ObservedOpenDbPod` literally; update them to include `pod_ip: None` so the build stays green. Run `cargo test -p opendb-operator` to confirm.

- [ ] **Step 3: Implement collect_pod_recovery**

Add to `main.rs`:

```rust
async fn collect_pod_recovery(
    pods: &[crd::ObservedOpenDbPod],
    health_port: u16,
    fetcher: &(dyn recovery::RecoveryStatusFetcher),
) -> Vec<recovery::ObservedPodRecovery> {
    use futures::stream::{FuturesUnordered, StreamExt};
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
                _ => Err(recovery::FetchErrorSummary::Unreachable),
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
```

Add `summarize_fetch_error` helper in `recovery.rs`:

```rust
pub fn summarize_fetch_error(error: &FetchError) -> FetchErrorSummary {
    match error {
        FetchError::Io(_) => FetchErrorSummary::Unreachable,
        FetchError::HttpStatus(code) => FetchErrorSummary::HttpStatus(*code),
        FetchError::MalformedResponse => FetchErrorSummary::MalformedResponse,
        FetchError::Decode(_) => FetchErrorSummary::Decode,
        FetchError::Timeout => FetchErrorSummary::Timeout,
    }
}
```

Add `futures` to `crates/opendb-operator/Cargo.toml` from the workspace.

- [ ] **Step 4: Wire fetcher into reconcile_all**

In `run_operator`, build a `HttpRecoveryStatusFetcher` once and pass it to `reconcile_all`. Update `reconcile_all`:

- read `cluster.spec.health_port` (cast to `u16`);
- after building the existing `observed_pods`, call `collect_pod_recovery`;
- pass the resulting list to `aggregate_cluster_recovery`;
- pass the aggregate to `compute_open_db_cluster_status_with_recovery`.

Use a per-tick HTTP timeout shorter than `reconcile_interval` (default `Duration::from_millis(2000)`). Make it overridable via env `OPENDB_OPERATOR_STATUS_TIMEOUT_MS`.

- [ ] **Step 5: Run focused verification and commit**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p opendb-operator
```

Expected: all tests pass.

```bash
git add crates/opendb-operator/src/main.rs crates/opendb-operator/src/crd.rs crates/opendb-operator/src/recovery.rs crates/opendb-operator/Cargo.toml
git commit -m "feat: reconcile recovery status into opendb cluster"
git push origin HEAD:main
```

## Task 4: Restart Recovery Smoke Flag (Opt-In, Non-Destructive Default)

**Ownership:** One worker owns `tools/k3s-smoke.ts`, the smoke tests, and `docs/k3s-uat.md` for this task after Tasks 1 to 3 land.

**Files:**
- Modify: `tools/k3s-smoke.ts`
- Modify: `tests/cluster/k3s-smoke.test.ts`
- Create: `tests/cluster/restart-recovery.test.ts`
- Modify: `docs/k3s-uat.md`

- [ ] **Step 1: Add failing TS tests**

In `tests/cluster/k3s-smoke.test.ts`, add:

```typescript
it("default plan describes the opt-in restart recovery flag and stays non-destructive", () => {
  const plan = buildK3sSmokePlan({
    namespace: "opendb-system",
    clusterName: "opendb",
    expectedReplicas: 3,
    timeoutMs: 120_000,
    withRestartRecovery: false,
  });
  const text = plan.map(commandText).join("\n");
  expect(text).toContain("non-destructive default");
  expect(text).not.toContain("kubectl delete pod");
});

it("plan with --with-restart-recovery describes the restart-recovery scenario", () => {
  const plan = buildK3sSmokePlan({
    namespace: "opendb-system",
    clusterName: "opendb",
    expectedReplicas: 3,
    timeoutMs: 120_000,
    withRestartRecovery: true,
  });
  const text = plan.map(commandText).join("\n");
  expect(text).toContain("kubectl delete pod");
  expect(text).toContain("Recovered");
});
```

In a new `tests/cluster/restart-recovery.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { parseSmokeOptions, recoveryConditionIsTrue } from "../../tools/k3s-smoke";

describe("restart recovery flag", () => {
  it("parses --with-restart-recovery", () => {
    const options = parseSmokeOptions(["--with-restart-recovery"]);
    expect(options.withRestartRecovery).toBe(true);
  });

  it("default does not enable restart recovery", () => {
    const options = parseSmokeOptions([]);
    expect(options.withRestartRecovery).toBe(false);
  });

  it("recoveryConditionIsTrue reads OpenDbCluster status conditions", () => {
    expect(
      recoveryConditionIsTrue({
        status: {
          conditions: [
            { type: "Ready", status: "True" },
            { type: "Recovered", status: "True" },
          ],
        },
      }),
    ).toBe(true);

    expect(
      recoveryConditionIsTrue({
        status: {
          conditions: [
            { type: "Ready", status: "True" },
            { type: "Recovered", status: "Unknown" },
          ],
        },
      }),
    ).toBe(false);
  });
});
```

Run:

```bash
npm run check:ts
npm run test:cluster -- restart-recovery
npm run test:cluster -- k3s-smoke
```

Expected before implementation: tests fail because the option and helper do not exist.

- [ ] **Step 2: Extend the smoke options and plan builder**

In `tools/k3s-smoke.ts`:

```typescript
export type SmokeOptions = {
  namespace: string;
  clusterName: string;
  expectedReplicas: number;
  timeoutMs: number;
  printPlan: boolean;
  allowNonLocalContext: boolean;
  withRestartRecovery: boolean;
};

export type K3sSmokePlanOptions = {
  namespace: string;
  clusterName: string;
  expectedReplicas: number;
  timeoutMs: number;
  withRestartRecovery: boolean;
};
```

Update `parseSmokeOptions` to accept `--with-restart-recovery` and default to `false`. Update `parseArgs` similarly.

In `buildK3sSmokePlan`, when `withRestartRecovery` is `false`, append a final step describing the non-destructive default:

```typescript
{
  description:
    "(skipped by default) restart recovery UAT is non-destructive default; run with --with-restart-recovery to execute",
  command: { tool: "echo", args: ["non-destructive default"] },
}
```

When `true`, append explicit steps:

```typescript
[
  { description: "create recovery_smoke table and insert smoke row through pgwire", command: { ... } },
  { description: "kubectl delete pod <leader> in namespace", command: { tool: "kubectl", args: ["delete", "pod", "<leader>", "-n", "<ns>"] } },
  { description: "wait for OpenDbCluster.status.conditions[type=Recovered].status=True with leader", command: { ... } },
  { description: "select smoke row through pgwire", command: { ... } },
]
```

- [ ] **Step 3: Implement recoveryConditionIsTrue and the wait helper**

Add:

```typescript
export function recoveryConditionIsTrue(value: unknown): boolean {
  const status = extractStatus(value);
  if (!status) return false;
  const conditions = Array.isArray(status.conditions) ? status.conditions : [];
  return conditions.some(
    (condition) =>
      typeof condition === "object" &&
      condition !== null &&
      "type" in condition &&
      "status" in condition &&
      (condition as { type: unknown }).type === "Recovered" &&
      (condition as { status: unknown }).status === "True",
  );
}

function extractStatus(value: unknown): Record<string, unknown> | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  const status = (value as Record<string, unknown>).status;
  return typeof status === "object" && status !== null ? (status as Record<string, unknown>) : undefined;
}
```

Add `waitForRecoveredCondition(namespace, clusterName, timeoutMs)` that polls `kubectl get opendbcluster -o json` and uses `recoveryConditionIsTrue` and `clusterStatusIsReady`.

- [ ] **Step 4: Execute the restart recovery scenario**

Behind `withRestartRecovery`, after the existing pgwire smoke completes:

1. open the existing pgwire port-forward;
2. issue `CREATE TABLE IF NOT EXISTS recovery_smoke (id integer PRIMARY KEY);`;
3. insert a deterministic row (e.g. `INSERT INTO recovery_smoke (id) VALUES (1);`);
4. close the port-forward;
5. resolve the leader pod from the latest `OpenDbCluster.status.leaderPod`;
6. `kubectl delete pod <leader> -n <ns>`;
7. call `waitForRecoveredCondition` with the smoke timeout;
8. open a fresh port-forward and `SELECT id FROM recovery_smoke WHERE id = 1;`;
9. fail with a descriptive message if the row is missing or the condition never converges.

When `withRestartRecovery` is set, enforce `allowNonLocalContext` semantics so a destructive flow cannot run against a non-local context unless the user explicitly opted in. The existing `kubeContextIsAllowed` guard already covers this; reuse it.

- [ ] **Step 5: Update docs**

In `docs/k3s-uat.md`, add a paragraph and a code block:

```markdown
The default `npm run smoke:k3s` is non-destructive: it does not delete pods. To
exercise the restart recovery contract end-to-end, run:

```bash
npm run smoke:k3s -- --with-restart-recovery
```

This will create a smoke table through pgwire, delete the current leader pod,
wait for `OpenDbCluster.status.conditions[type=Recovered].status=True`, and
re-query the inserted row. The flag still respects the kube context allow-list
and requires `--allow-nonlocal-context` for non-local clusters.
```

- [ ] **Step 6: Run focused verification and commit**

```bash
npm run check:ts
npm run check:no-python
npm run check:manifests
npm run test:cluster
```

Expected: all checks pass. The default `npm run smoke:k3s -- --print-plan` no longer mentions destructive operations as defaults.

```bash
git add tools/k3s-smoke.ts tests/cluster/k3s-smoke.test.ts tests/cluster/restart-recovery.test.ts docs/k3s-uat.md
git commit -m "feat: add opt-in restart recovery smoke flag"
git push origin HEAD:main
```

## Final Verification

No Sprint 3 implementation is complete unless these commands pass:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm run check:ts
npm run check:no-python
npm run check:manifests
npm test
git diff --check HEAD
```

`cargo test --workspace` may require execution outside the sandbox because OpenRaft tests bind local loopback ports. `tsx` commands may also require execution outside the sandbox when the IPC pipe is blocked.

## Review Checklist

- [ ] The operator polls each running pod's `/status` and aggregates the result.
- [ ] `OpenDbCluster.status.conditions` includes `RootDescriptorKnown`, `WalReplayCompleted`, `ArchiveMetadataKnown`, and `Recovered`.
- [ ] `OpenDbCluster.status.recovery` summary is populated when at least one running pod has been observed and omitted otherwise.
- [ ] `phase` semantics are unchanged; `Recovered` is its own condition.
- [ ] Unreachable pods produce `Unknown` conditions, not `False`.
- [ ] `tools/k3s-smoke.ts` exposes `--with-restart-recovery`, off by default, and reuses the existing kube-context allow-list.
- [ ] The default `npm run smoke:k3s` does not call `kubectl delete pod`.
- [ ] `docs/k3s-uat.md` documents the flag, the conditions, and the unchanged non-destructive default.
- [ ] No Python files or scripts are introduced.
- [ ] No new mutation, range catalog change, or object storage client introduced.
