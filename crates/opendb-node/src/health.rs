use anyhow::Context;
use std::{
    net::SocketAddr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Clone, Debug)]
pub struct HealthState {
    ready: Arc<AtomicBool>,
    recovery_status: Arc<RwLock<RecoveryStatus>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryStatus {
    pub root_descriptor_known: bool,
    pub wal_replay_completed: bool,
    pub last_replayed_tx_id: Option<u64>,
    pub last_replayed_ts: Option<u64>,
    pub archive_metadata_replayed: bool,
    pub latest_recovery_artifact: Option<String>,
}

impl HealthState {
    pub fn new(ready: bool) -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(ready)),
            recovery_status: Arc::new(RwLock::new(RecoveryStatus::default())),
        }
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn set_recovery_status(&self, recovery_status: RecoveryStatus) {
        *self
            .recovery_status
            .write()
            .expect("health recovery status lock poisoned") = recovery_status;
    }

    pub fn recovery_status(&self) -> RecoveryStatus {
        self.recovery_status
            .read()
            .expect("health recovery status lock poisoned")
            .clone()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HealthResponse {
    status_code: u16,
    reason: &'static str,
    body: String,
}

pub async fn serve(addr: SocketAddr, state: HealthState) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind health listener on {addr}"))?;
    tracing::info!(%addr, "health listener ready");

    loop {
        let (stream, peer) = listener.accept().await.context("accept health client")?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, state).await {
                tracing::debug!(%peer, %error, "health connection failed");
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream, state: HealthState) -> anyhow::Result<()> {
    let mut request = [0_u8; 1024];
    let read = stream.read(&mut request).await?;
    let path = request_path(&request[..read]).unwrap_or("/live");
    let response = response_for_path(path, &state);
    stream.write_all(response.to_http().as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

fn request_path(request: &[u8]) -> Option<&str> {
    let request = std::str::from_utf8(request).ok()?;
    let first_line = request.lines().next()?;
    let mut parts = first_line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;
    if method == "GET" { Some(path) } else { None }
}

fn response_for_path(path: &str, state: &HealthState) -> HealthResponse {
    match path {
        "/ready" if !state.is_ready() => HealthResponse {
            status_code: 503,
            reason: "Service Unavailable",
            body: "not ready\n".to_string(),
        },
        "/ready" | "/live" | "/healthz" | "/" => HealthResponse {
            status_code: 200,
            reason: "OK",
            body: "ok\n".to_string(),
        },
        "/status" => HealthResponse {
            status_code: 200,
            reason: "OK",
            body: serde_json::to_string(&state.recovery_status())
                .expect("serialize recovery status")
                + "\n",
        },
        _ => HealthResponse {
            status_code: 404,
            reason: "Not Found",
            body: "not found\n".to_string(),
        },
    }
}

impl HealthResponse {
    fn to_http(&self) -> String {
        format!(
            "HTTP/1.1 {} {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            self.status_code,
            self.reason,
            self.body.len(),
            self.body
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_returns_unavailable_until_state_is_marked_ready() {
        let state = HealthState::new(false);

        assert_eq!(response_for_path("/ready", &state).status_code, 503);

        state.set_ready(true);

        assert_eq!(response_for_path("/ready", &state).status_code, 200);
    }

    #[test]
    fn live_ignores_readiness_state() {
        let state = HealthState::new(false);

        assert_eq!(response_for_path("/live", &state).status_code, 200);
    }

    #[test]
    fn root_path_keeps_legacy_health_behavior() {
        let state = HealthState::new(false);

        assert_eq!(response_for_path("/", &state).status_code, 200);
    }

    #[test]
    fn healthz_keeps_legacy_health_behavior() {
        let state = HealthState::new(false);

        assert_eq!(response_for_path("/healthz", &state).status_code, 200);
    }

    #[test]
    fn status_reports_recovery_watermark() {
        let state = HealthState::new(false);
        state.set_recovery_status(RecoveryStatus {
            root_descriptor_known: true,
            wal_replay_completed: true,
            last_replayed_tx_id: Some(2),
            last_replayed_ts: Some(2),
            archive_metadata_replayed: true,
            latest_recovery_artifact: None,
        });

        let response = response_for_path("/status", &state);

        assert_eq!(response.status_code, 200);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&response.body).expect("status json"),
            serde_json::json!({
                "rootDescriptorKnown": true,
                "walReplayCompleted": true,
                "lastReplayedTxId": 2,
                "lastReplayedTs": 2,
                "archiveMetadataReplayed": true,
                "latestRecoveryArtifact": null,
            })
        );
        assert!(
            response
                .to_http()
                .contains(&format!("content-length: {}", response.body.len()))
        );
    }

    #[test]
    fn status_does_not_affect_readiness() {
        let state = HealthState::new(false);
        state.set_recovery_status(RecoveryStatus {
            root_descriptor_known: true,
            wal_replay_completed: true,
            last_replayed_tx_id: Some(0),
            last_replayed_ts: Some(0),
            archive_metadata_replayed: true,
            latest_recovery_artifact: None,
        });

        assert_eq!(response_for_path("/status", &state).status_code, 200);
        assert_eq!(response_for_path("/ready", &state).status_code, 503);
    }
}
