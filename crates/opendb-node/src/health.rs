use anyhow::Context;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
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
}

impl HealthState {
    pub fn new(ready: bool) -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(ready)),
        }
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HealthResponse {
    status_code: u16,
    reason: &'static str,
    body: &'static str,
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
            body: "not ready\n",
        },
        "/ready" | "/live" | "/healthz" | "/" => HealthResponse {
            status_code: 200,
            reason: "OK",
            body: "ok\n",
        },
        _ => HealthResponse {
            status_code: 404,
            reason: "Not Found",
            body: "not found\n",
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
}
