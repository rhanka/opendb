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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FetchErrorSummary {
    Unreachable,
    HttpStatus(u16),
    MalformedResponse,
    Decode,
    Timeout,
}

pub fn summarize_fetch_error(error: &FetchError) -> FetchErrorSummary {
    match error {
        FetchError::Io(_) => FetchErrorSummary::Unreachable,
        FetchError::HttpStatus(code) => FetchErrorSummary::HttpStatus(*code),
        FetchError::MalformedResponse => FetchErrorSummary::MalformedResponse,
        FetchError::Decode(_) => FetchErrorSummary::Decode,
        FetchError::Timeout => FetchErrorSummary::Timeout,
    }
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
        let request =
            format!("GET /status HTTP/1.1\r\nhost: {pod_ip}:{port}\r\nconnection: close\r\n\r\n");
        let mut conn = tokio::time::timeout(self.timeout, TcpStream::connect((pod_ip, port)))
            .await
            .map_err(|_| FetchError::Timeout)??;

        let buffer = tokio::time::timeout(self.timeout, async {
            conn.write_all(request.as_bytes()).await?;
            let mut buffer = Vec::with_capacity(4096);
            conn.read_to_end(&mut buffer).await?;
            Ok::<_, std::io::Error>(buffer)
        })
        .await
        .map_err(|_| FetchError::Timeout)??;

        let (status_code, body) = parse_http_response(&buffer)?;
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
    let header_text =
        std::str::from_utf8(&buffer[..header_end]).map_err(|_| FetchError::MalformedResponse)?;
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

#[cfg(test)]
mod fetcher_tests {
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

        let fetcher = HttpRecoveryStatusFetcher::new(Duration::from_secs(2));
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

        let fetcher = HttpRecoveryStatusFetcher::new(Duration::from_secs(2));
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

        let fetcher = HttpRecoveryStatusFetcher::new(Duration::from_secs(2));
        let error = fetcher
            .fetch("opendb-0", "127.0.0.1", port)
            .await
            .expect_err("unknown field must fail strict decode");

        assert!(matches!(error, FetchError::Decode(_)));
    }
}

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
    fn aggregate_is_empty_when_all_observed_pods_are_not_running() {
        let pods = vec![ObservedPodRecovery {
            name: "opendb-0".to_string(),
            running: false,
            status: Err(FetchErrorSummary::Unreachable),
        }];
        let aggregate = aggregate_cluster_recovery(&pods);
        assert!(aggregate.is_none());
    }

    #[test]
    fn aggregate_reports_max_tx_and_ts_across_running_pods() {
        let pods = vec![
            ok_pod("opendb-0", 3),
            ok_pod("opendb-1", 7),
            ok_pod("opendb-2", 5),
        ];
        let aggregate = aggregate_cluster_recovery(&pods).expect("aggregate");
        assert_eq!(aggregate.last_replayed_tx_id, Some(7));
        assert_eq!(aggregate.last_replayed_ts, Some(7));
        assert_eq!(aggregate.observed_running_pods, 3);
        assert_eq!(aggregate.root_descriptor_known_pods, 3);
        assert_eq!(aggregate.wal_replay_completed_pods, 3);
        assert_eq!(aggregate.archive_metadata_replayed_pods, 3);
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
        assert_eq!(aggregate.root_descriptor_known_pods, 1);
        assert_eq!(aggregate.observed_running_pods, 2);
    }

    #[test]
    fn aggregate_uses_smallest_pod_name_for_latest_recovery_artifact() {
        let mut pods = vec![
            ok_pod("opendb-2", 1),
            ok_pod("opendb-0", 1),
            ok_pod("opendb-1", 1),
        ];
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
