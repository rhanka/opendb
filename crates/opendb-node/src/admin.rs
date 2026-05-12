use crate::database::{
    Database, ProposedRangeMerge, ProposedRangeSplit, RangeMergeProposalResult,
    RangeSplitProposalResult,
};
use anyhow::Context;
use opendb_common::{OpenDbError, RangeId};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

#[derive(Clone, Debug)]
pub struct AdminState {
    pub database: Arc<Mutex<Database>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SplitRequest {
    pub source_range_id: u64,
    pub split_key: String,
    #[serde(default)]
    pub left_range_id: Option<u64>,
    #[serde(default)]
    pub right_range_id: Option<u64>,
    #[serde(default)]
    pub left_replica_node_ids: Option<Vec<u64>>,
    #[serde(default)]
    pub right_replica_node_ids: Option<Vec<u64>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SplitResponse {
    pub range_ids: [u64; 2],
    pub tx_id: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MergeRequest {
    pub source_range_ids: Vec<u64>,
    #[serde(default)]
    pub merged_range_id: Option<u64>,
    #[serde(default)]
    pub replica_node_ids: Option<Vec<u64>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MergeResponse {
    pub merged_range_id: u64,
    pub tx_id: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ErrorBody {
    pub error: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_addr: Option<String>,
}

impl From<SplitRequest> for ProposedRangeSplit {
    fn from(request: SplitRequest) -> Self {
        Self {
            source_range_id: RangeId(request.source_range_id),
            split_key: request.split_key,
            left_range_id: request.left_range_id.map(RangeId),
            right_range_id: request.right_range_id.map(RangeId),
            left_replica_node_ids: request.left_replica_node_ids,
            right_replica_node_ids: request.right_replica_node_ids,
        }
    }
}

impl From<RangeSplitProposalResult> for SplitResponse {
    fn from(result: RangeSplitProposalResult) -> Self {
        Self {
            range_ids: [result.left_range_id.0, result.right_range_id.0],
            tx_id: result.tx_id,
        }
    }
}

impl From<MergeRequest> for ProposedRangeMerge {
    fn from(request: MergeRequest) -> Self {
        Self {
            source_range_ids: request.source_range_ids.into_iter().map(RangeId).collect(),
            merged_range_id: request.merged_range_id.map(RangeId),
            replica_node_ids: request.replica_node_ids,
        }
    }
}

impl From<RangeMergeProposalResult> for MergeResponse {
    fn from(result: RangeMergeProposalResult) -> Self {
        Self {
            merged_range_id: result.merged_range_id.0,
            tx_id: result.tx_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AdminResponse {
    status_code: u16,
    reason: &'static str,
    body: String,
}

impl AdminResponse {
    fn to_http(&self) -> String {
        format!(
            "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            self.status_code,
            self.reason,
            self.body.len(),
            self.body
        )
    }

    fn ok(body: String) -> Self {
        Self {
            status_code: 202,
            reason: "Accepted",
            body,
        }
    }

    fn bad_request(message: String) -> Self {
        Self {
            status_code: 400,
            reason: "Bad Request",
            body: serde_json::to_string(&ErrorBody {
                error: message,
                leader_id: None,
                leader_addr: None,
            })
            .expect("serialize error body"),
        }
    }

    fn misdirected(leader_id: Option<u64>, leader_addr: Option<String>) -> Self {
        Self {
            status_code: 421,
            reason: "Misdirected Request",
            body: serde_json::to_string(&ErrorBody {
                error: "not leader".to_string(),
                leader_id,
                leader_addr,
            })
            .expect("serialize error body"),
        }
    }

    fn not_found() -> Self {
        Self {
            status_code: 404,
            reason: "Not Found",
            body: serde_json::to_string(&ErrorBody {
                error: "not found".to_string(),
                leader_id: None,
                leader_addr: None,
            })
            .expect("serialize error body"),
        }
    }

    fn method_not_allowed() -> Self {
        Self {
            status_code: 405,
            reason: "Method Not Allowed",
            body: serde_json::to_string(&ErrorBody {
                error: "method not allowed".to_string(),
                leader_id: None,
                leader_addr: None,
            })
            .expect("serialize error body"),
        }
    }

    fn internal(message: String) -> Self {
        Self {
            status_code: 500,
            reason: "Internal Server Error",
            body: serde_json::to_string(&ErrorBody {
                error: message,
                leader_id: None,
                leader_addr: None,
            })
            .expect("serialize error body"),
        }
    }
}

fn map_error(error: OpenDbError) -> AdminResponse {
    match error {
        OpenDbError::NotLeader {
            leader_id,
            leader_addr,
        } => AdminResponse::misdirected(leader_id, leader_addr),
        OpenDbError::InvalidInput(message) | OpenDbError::NotFound(message) => {
            AdminResponse::bad_request(message)
        }
        other => AdminResponse::internal(other.to_string()),
    }
}

async fn handle_split_body(state: &AdminState, body: &[u8]) -> AdminResponse {
    let request: SplitRequest = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(error) => return AdminResponse::bad_request(format!("invalid request body: {error}")),
    };
    let mut database = state.database.lock().await;
    match database.propose_range_split(request.into()).await {
        Ok(result) => AdminResponse::ok(
            serde_json::to_string(&SplitResponse::from(result)).expect("serialize split response"),
        ),
        Err(error) => map_error(error),
    }
}

async fn handle_merge_body(state: &AdminState, body: &[u8]) -> AdminResponse {
    let request: MergeRequest = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(error) => return AdminResponse::bad_request(format!("invalid request body: {error}")),
    };
    let mut database = state.database.lock().await;
    match database.propose_range_merge(request.into()).await {
        Ok(result) => AdminResponse::ok(
            serde_json::to_string(&MergeResponse::from(result)).expect("serialize merge response"),
        ),
        Err(error) => map_error(error),
    }
}

pub async fn serve(addr: SocketAddr, state: AdminState) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind admin listener on {addr}"))?;
    tracing::info!(%addr, "admin listener ready");

    loop {
        let (stream, peer) = listener.accept().await.context("accept admin client")?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, state).await {
                tracing::debug!(%peer, %error, "admin connection failed");
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream, state: AdminState) -> anyhow::Result<()> {
    let response = read_and_dispatch(&mut stream, &state).await;
    stream.write_all(response.to_http().as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn read_and_dispatch(stream: &mut TcpStream, state: &AdminState) -> AdminResponse {
    let mut buffer = Vec::with_capacity(4096);
    let mut chunk = [0_u8; 1024];
    let max_request_size = 64 * 1024;

    // Read until we have the full headers
    loop {
        if buffer.len() >= max_request_size {
            return AdminResponse::bad_request("request too large".to_string());
        }
        let read = match stream.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => n,
            Err(error) => {
                return AdminResponse::bad_request(format!("read error: {error}"));
            }
        };
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(header_end) = find_double_crlf(&buffer) {
            let (method, path, content_length) =
                match parse_request_line_and_headers(&buffer[..header_end + 4]) {
                    Some(parsed) => parsed,
                    None => return AdminResponse::bad_request("malformed request".to_string()),
                };

            if method != "POST" {
                return AdminResponse::method_not_allowed();
            }

            let route = match path.as_str() {
                "/admin/ranges/split" => Route::Split,
                "/admin/ranges/merge" => Route::Merge,
                _ => return AdminResponse::not_found(),
            };

            // Read body up to content_length
            let body_start = header_end + 4;
            let total_expected = body_start + content_length;
            while buffer.len() < total_expected {
                if buffer.len() >= max_request_size {
                    return AdminResponse::bad_request("request too large".to_string());
                }
                let read = match stream.read(&mut chunk).await {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(error) => {
                        return AdminResponse::bad_request(format!("read error: {error}"));
                    }
                };
                buffer.extend_from_slice(&chunk[..read]);
            }
            if buffer.len() < total_expected {
                return AdminResponse::bad_request("incomplete request body".to_string());
            }
            let body = &buffer[body_start..total_expected];

            return match route {
                Route::Split => handle_split_body(state, body).await,
                Route::Merge => handle_merge_body(state, body).await,
            };
        }
    }
    AdminResponse::bad_request("malformed request (no header terminator)".to_string())
}

enum Route {
    Split,
    Merge,
}

fn find_double_crlf(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|w| w == b"\r\n\r\n")
}

fn parse_request_line_and_headers(bytes: &[u8]) -> Option<(String, String, usize)> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    let mut content_length: usize = 0;
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse().ok()?;
        }
    }
    Some((method, path, content_length))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendb_consensus::root_range::RootRange;
    use opendb_sql::parser::parse;

    async fn open_database_with_table() -> (tempfile::TempDir, Arc<Mutex<Database>>) {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        let mut database = Database::open_with_root_range(root_range)
            .await
            .expect("open database");
        database
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .await
            .expect("create");
        (temp_dir, Arc::new(Mutex::new(database)))
    }

    #[tokio::test]
    async fn split_endpoint_returns_202_with_allocated_ids() {
        let (_temp, database) = open_database_with_table().await;
        let state = AdminState { database };
        let body = br#"{"sourceRangeId":1,"splitKey":"accounts/5"}"#;

        let response = handle_split_body(&state, body).await;

        assert_eq!(response.status_code, 202);
        let parsed: SplitResponse =
            serde_json::from_str(&response.body).expect("parse split response");
        assert_eq!(parsed.range_ids, [2, 3]);
        assert!(parsed.tx_id >= 2);
    }

    #[tokio::test]
    async fn split_endpoint_returns_400_for_invalid_boundary() {
        let (_temp, database) = open_database_with_table().await;
        let state = AdminState { database };
        let body = br#"{"sourceRangeId":404,"splitKey":"accounts/5"}"#;

        let response = handle_split_body(&state, body).await;

        assert_eq!(response.status_code, 400);
        let parsed: ErrorBody = serde_json::from_str(&response.body).expect("parse error body");
        assert!(parsed.error.contains("does not exist"));
    }

    #[tokio::test]
    async fn split_endpoint_rejects_unknown_field() {
        let (_temp, database) = open_database_with_table().await;
        let state = AdminState { database };
        let body = br#"{"sourceRangeId":1,"splitKey":"accounts/5","unexpected":true}"#;

        let response = handle_split_body(&state, body).await;

        assert_eq!(response.status_code, 400);
    }

    #[tokio::test]
    async fn merge_endpoint_round_trip() {
        let (_temp, database) = open_database_with_table().await;
        let state = AdminState { database };
        let split_body = br#"{"sourceRangeId":1,"splitKey":"accounts/5"}"#;
        let split = handle_split_body(&state, split_body).await;
        assert_eq!(split.status_code, 202);

        let merge_body = br#"{"sourceRangeIds":[2,3]}"#;
        let response = handle_merge_body(&state, merge_body).await;

        assert_eq!(response.status_code, 202);
        let parsed: MergeResponse =
            serde_json::from_str(&response.body).expect("parse merge response");
        assert_eq!(parsed.merged_range_id, 4);
    }

    #[tokio::test]
    async fn merge_endpoint_rejects_inactive_source() {
        let (_temp, database) = open_database_with_table().await;
        let state = AdminState { database };
        let body = br#"{"sourceRangeIds":[99,100]}"#;

        let response = handle_merge_body(&state, body).await;

        assert_eq!(response.status_code, 400);
    }

    #[test]
    fn request_line_and_headers_extract_content_length() {
        let raw = b"POST /admin/ranges/split HTTP/1.1\r\ncontent-length: 12\r\nhost: x\r\n\r\n";
        let parsed = parse_request_line_and_headers(raw).expect("parse");
        assert_eq!(parsed.0, "POST");
        assert_eq!(parsed.1, "/admin/ranges/split");
        assert_eq!(parsed.2, 12);
    }

    #[test]
    fn admin_response_serializes_http_with_content_length() {
        let response = AdminResponse::ok("{}".to_string());
        let http = response.to_http();
        assert!(http.starts_with("HTTP/1.1 202 Accepted\r\n"));
        assert!(http.contains("content-length: 2"));
        assert!(http.contains("content-type: application/json"));
    }
}
