use crate::raft::{RootRangeRaftHarness, RootRangeResponse};
use opendb_common::{OpenDbError, OpenDbResult, RangeId};
use opendb_storage::{commit_stream::CommitRecord, wal::Wal};
use openraft::BasicNode;
use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

// Milestone 1 keeps the public consensus boundary here. OpenRaft integration
// must stay behind RootRange so SQL, storage, pgwire, and Kubernetes code do
// not depend directly on OpenRaft types.
pub type OpenDbRaftNodeId = u64;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RootRangeCommand {
    pub record: CommitRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RootRangeAuthority {
    Standalone,
    Leader {
        node_id: OpenDbRaftNodeId,
    },
    Follower {
        leader_id: Option<OpenDbRaftNodeId>,
        leader_addr: Option<String>,
    },
}

impl RootRangeAuthority {
    pub fn leader(node_id: OpenDbRaftNodeId) -> Self {
        Self::Leader { node_id }
    }

    pub fn follower(leader_id: OpenDbRaftNodeId, leader_addr: Option<String>) -> Self {
        Self::Follower {
            leader_id: Some(leader_id),
            leader_addr,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootRangePeer {
    pub node_id: OpenDbRaftNodeId,
    pub addr: String,
}

#[derive(Clone)]
enum RootRangeProposalPath {
    Local(RootRangeAuthority),
    OpenRaft(Arc<RootRangeRaftHarness>),
}

impl fmt::Debug for RootRangeProposalPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(authority) => formatter.debug_tuple("Local").field(authority).finish(),
            Self::OpenRaft(_) => formatter.write_str("OpenRaft(..)"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RootRange {
    range_id: RangeId,
    wal: Wal,
    proposal_path: RootRangeProposalPath,
}

impl RootRange {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self::new_with_authority(data_dir, RootRangeAuthority::Standalone)
    }

    pub fn new_with_authority(data_dir: impl AsRef<Path>, authority: RootRangeAuthority) -> Self {
        Self {
            range_id: RangeId::ROOT,
            wal: Wal::new(data_dir.as_ref().join("root-range").join("commit.wal")),
            proposal_path: RootRangeProposalPath::Local(authority),
        }
    }

    pub async fn new_raft_backed(
        data_dir: impl AsRef<Path>,
        node_id: OpenDbRaftNodeId,
        peers: Vec<RootRangePeer>,
    ) -> OpenDbResult<(Self, RootRangePeerServer)> {
        let members = peers
            .into_iter()
            .map(|peer| (peer.node_id, BasicNode::new(peer.addr)))
            .collect::<BTreeMap<_, _>>();
        if members.is_empty() {
            return Err(OpenDbError::InvalidInput(
                "root range OpenRaft requires at least one initial peer".to_string(),
            ));
        }
        if !members.contains_key(&node_id) {
            return Err(OpenDbError::InvalidInput(format!(
                "root range OpenRaft node id {node_id} is missing from initial peers"
            )));
        }
        let raft = Arc::new(RootRangeRaftHarness::new(node_id, data_dir.as_ref(), members).await?);
        let root_range = Self {
            range_id: RangeId::ROOT,
            wal: Wal::new(data_dir.as_ref().join("root-range").join("commit.wal")),
            proposal_path: RootRangeProposalPath::OpenRaft(Arc::clone(&raft)),
        };

        Ok((root_range, RootRangePeerServer { raft }))
    }

    pub fn range_id(&self) -> RangeId {
        self.range_id
    }

    /// Applies a root-range record that has already been committed.
    ///
    /// Milestone 1 only wires this apply-side path. Callers must not use it as
    /// a proposal path; use `submit` for the reserved OpenRaft-facing API.
    pub async fn apply_committed(&self, record: &CommitRecord) -> OpenDbResult<()> {
        self.validate_apply_record(record)?;
        self.wal.append(record).await
    }

    /// Compatibility wrapper for existing plan-era callers.
    ///
    /// This still performs apply-side validation and persistence only. It does
    /// not submit a proposal to consensus.
    pub async fn append_committed(&self, record: &CommitRecord) -> OpenDbResult<()> {
        self.apply_committed(record).await
    }

    /// Submits a root-range command through the consensus boundary.
    ///
    /// Raft-backed ranges propose through OpenRaft `client_write`. Local
    /// standalone/leader modes are kept for single-process tests and bootstrap
    /// milestones; followers reject before touching the local WAL.
    pub async fn submit(&self, command: RootRangeCommand) -> OpenDbResult<()> {
        self.validate_apply_record(&command.record)?;
        match &self.proposal_path {
            RootRangeProposalPath::OpenRaft(raft) => {
                let response = raft.submit(command).await?;
                if response == RootRangeResponse::Applied {
                    Ok(())
                } else {
                    Err(OpenDbError::Storage(format!(
                        "root range raft returned unexpected write response: {response:?}"
                    )))
                }
            }
            RootRangeProposalPath::Local(authority) => match authority {
                RootRangeAuthority::Standalone | RootRangeAuthority::Leader { .. } => {
                    self.apply_committed(&command.record).await
                }
                RootRangeAuthority::Follower {
                    leader_id,
                    leader_addr,
                } => Err(OpenDbError::NotLeader {
                    leader_id: *leader_id,
                    leader_addr: leader_addr.clone(),
                }),
            },
        }
    }

    pub async fn replay(&self) -> OpenDbResult<Vec<CommitRecord>> {
        let records = self.wal.read_all().await?;
        for (index, record) in records.iter().enumerate() {
            self.validate_replayed_record(index, record)?;
        }
        Ok(records)
    }

    fn validate_apply_record(&self, record: &CommitRecord) -> OpenDbResult<()> {
        if record.range_id != self.range_id {
            return Err(OpenDbError::InvalidInput(format!(
                "root range requires record range_id {:?}, got {:?}",
                self.range_id, record.range_id
            )));
        }
        Ok(())
    }

    fn validate_replayed_record(&self, index: usize, record: &CommitRecord) -> OpenDbResult<()> {
        if record.range_id != self.range_id {
            return Err(OpenDbError::Storage(format!(
                "root range WAL record {index} has range_id {:?}, expected {:?}",
                record.range_id, self.range_id
            )));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct RootRangePeerServer {
    raft: Arc<RootRangeRaftHarness>,
}

impl fmt::Debug for RootRangePeerServer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RootRangePeerServer(..)")
    }
}

impl RootRangePeerServer {
    pub async fn initialize_cluster(&self) -> OpenDbResult<()> {
        self.raft.initialize_cluster().await
    }

    pub async fn serve(&self, addr: SocketAddr) -> OpenDbResult<()> {
        Arc::clone(&self.raft).serve_peer_rpc(addr).await
    }

    pub async fn is_leader(&self, node_id: OpenDbRaftNodeId) -> bool {
        self.raft.is_leader(node_id).await
    }

    pub async fn ensure_leader(&self) -> OpenDbResult<()> {
        self.raft.ensure_linearizable_leader().await.map_err(|err| {
            if let Some(forward) = err.forward_to_leader() {
                OpenDbError::NotLeader {
                    leader_id: forward.leader_id,
                    leader_addr: forward.leader_node.as_ref().map(|node| node.addr.clone()),
                }
            } else {
                OpenDbError::Storage(format!("root range leadership check failed: {err}"))
            }
        })
    }

    #[cfg(test)]
    async fn wait_for_leader(
        &self,
        node_id: OpenDbRaftNodeId,
        timeout: std::time::Duration,
    ) -> OpenDbResult<()> {
        self.raft.wait_for_leader(node_id, timeout).await
    }

    #[cfg(test)]
    async fn wait_for_any_leader(
        &self,
        timeout: std::time::Duration,
    ) -> OpenDbResult<OpenDbRaftNodeId> {
        self.raft.wait_for_any_leader(timeout).await
    }

    pub async fn shutdown(&self) -> OpenDbResult<()> {
        self.raft.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendb_common::{LogicalTimestamp, OpenDbError, TransactionId};
    use opendb_storage::commit_stream::Mutation;
    use opendb_storage::wal::Wal;
    use std::time::Duration;

    #[test]
    fn open_db_raft_node_id_satisfies_openraft_adapter_bounds() {
        fn assert_bounds<T: Default + std::fmt::Display>() {}

        assert_bounds::<OpenDbRaftNodeId>();
    }

    #[tokio::test]
    async fn root_range_replays_committed_records_after_restart() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let record = CommitRecord::new(
            TransactionId(7),
            LogicalTimestamp(11),
            vec![Mutation::CreateTable {
                table: "accounts".to_string(),
                columns: vec!["id".to_string(), "name".to_string()],
            }],
        );

        let root_range = RootRange::new(temp_dir.path());
        assert_eq!(root_range.range_id(), RangeId::ROOT);
        root_range
            .apply_committed(&record)
            .await
            .expect("append committed record");

        let restarted_root_range = RootRange::new(temp_dir.path());
        assert_eq!(
            restarted_root_range
                .replay()
                .await
                .expect("replay committed records"),
            vec![record]
        );
    }

    #[tokio::test]
    async fn apply_committed_rejects_non_root_records_without_persisting() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        let mut forged_record = CommitRecord::new(
            TransactionId(8),
            LogicalTimestamp(12),
            vec![Mutation::CreateTable {
                table: "orders".to_string(),
                columns: vec!["id".to_string()],
            }],
        );
        forged_record.range_id = RangeId(98);

        let result = root_range.apply_committed(&forged_record).await;

        assert!(matches!(
            result,
            Err(OpenDbError::InvalidInput(message))
                if message.contains("root range") && message.contains("98")
        ));
        assert_eq!(
            root_range
                .replay()
                .await
                .expect("replay after rejected apply"),
            Vec::new()
        );
    }

    #[tokio::test]
    async fn append_committed_rejects_non_root_records_without_persisting() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        let mut forged_record = CommitRecord::new(
            TransactionId(8),
            LogicalTimestamp(12),
            vec![Mutation::CreateTable {
                table: "orders".to_string(),
                columns: vec!["id".to_string()],
            }],
        );
        forged_record.range_id = RangeId(99);

        let result = root_range.append_committed(&forged_record).await;

        assert!(matches!(
            result,
            Err(OpenDbError::InvalidInput(message))
                if message.contains("root range") && message.contains("99")
        ));
        assert_eq!(
            root_range
                .replay()
                .await
                .expect("replay after rejected append"),
            Vec::new()
        );
    }

    #[tokio::test]
    async fn replay_rejects_forged_non_root_records_in_root_wal() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut forged_record = CommitRecord::new(
            TransactionId(9),
            LogicalTimestamp(13),
            vec![Mutation::CreateTable {
                table: "payments".to_string(),
                columns: vec!["id".to_string()],
            }],
        );
        forged_record.range_id = RangeId(100);
        let wal = Wal::new(temp_dir.path().join("root-range").join("commit.wal"));
        wal.append(&forged_record)
            .await
            .expect("forge root wal record");

        let root_range = RootRange::new(temp_dir.path());
        let result = root_range.replay().await;

        assert!(matches!(
            result,
            Err(OpenDbError::Storage(message))
                if message.contains("root range WAL")
                    && message.contains("record 0")
                    && message.contains("100")
        ));
    }

    #[tokio::test]
    async fn submit_rejects_non_root_commands_before_proposal_path() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        let mut forged_record = CommitRecord::new(
            TransactionId(10),
            LogicalTimestamp(14),
            vec![Mutation::CreateTable {
                table: "ledger".to_string(),
                columns: vec!["id".to_string()],
            }],
        );
        forged_record.range_id = RangeId(101);

        let result = root_range
            .submit(RootRangeCommand {
                record: forged_record,
            })
            .await;

        assert!(matches!(
            result,
            Err(OpenDbError::InvalidInput(message))
                if message.contains("root range") && message.contains("101")
        ));
    }

    #[tokio::test]
    async fn leader_submit_persists_root_commands_through_consensus_boundary() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range =
            RootRange::new_with_authority(temp_dir.path(), RootRangeAuthority::leader(0));
        let record = CommitRecord::new(
            TransactionId(11),
            LogicalTimestamp(15),
            vec![Mutation::CreateTable {
                table: "audit_log".to_string(),
                columns: vec!["id".to_string()],
            }],
        );

        root_range
            .submit(RootRangeCommand {
                record: record.clone(),
            })
            .await
            .expect("leader submit");

        assert_eq!(
            root_range
                .replay()
                .await
                .expect("replay leader-submitted command"),
            vec![record]
        );
    }

    #[tokio::test]
    async fn standalone_submit_persists_root_commands_through_consensus_boundary() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        let record = CommitRecord::new(
            TransactionId(12),
            LogicalTimestamp(16),
            vec![Mutation::CreateTable {
                table: "events".to_string(),
                columns: vec!["id".to_string()],
            }],
        );

        root_range
            .submit(RootRangeCommand {
                record: record.clone(),
            })
            .await
            .expect("submit root command");

        assert_eq!(
            root_range.replay().await.expect("replay submitted command"),
            vec![record]
        );
    }

    #[tokio::test]
    async fn follower_submit_rejects_root_commands_without_persisting() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new_with_authority(
            temp_dir.path(),
            RootRangeAuthority::follower(0, Some("opendb-0.opendb-peer:7000".to_string())),
        );
        let record = CommitRecord::new(
            TransactionId(13),
            LogicalTimestamp(17),
            vec![Mutation::CreateTable {
                table: "events".to_string(),
                columns: vec!["id".to_string()],
            }],
        );

        let result = root_range.submit(RootRangeCommand { record }).await;

        assert!(matches!(
            result,
            Err(OpenDbError::NotLeader {
                leader_id: Some(0),
                leader_addr: Some(addr),
            }) if addr == "opendb-0.opendb-peer:7000"
        ));
        assert_eq!(
            root_range
                .replay()
                .await
                .expect("replay after rejected follower submit"),
            Vec::new()
        );
    }

    #[tokio::test]
    async fn openraft_single_node_client_write_applies_root_range_command_to_wal() {
        use crate::raft::{RootRangeRaftHarness, RootRangeResponse};

        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let harness = RootRangeRaftHarness::new_single_node(0, temp_dir.path())
            .await
            .expect("start single-node root range raft");
        harness
            .initialize_single_node()
            .await
            .expect("initialize single-node root range raft");
        harness
            .wait_for_leader(0, Duration::from_secs(3))
            .await
            .expect("single node elects itself");

        let record = CommitRecord::new(
            TransactionId(14),
            LogicalTimestamp(18),
            vec![Mutation::CreateTable {
                table: "raft_events".to_string(),
                columns: vec!["id".to_string()],
            }],
        );

        let response = harness
            .submit(RootRangeCommand {
                record: record.clone(),
            })
            .await
            .expect("client_write root range command");

        assert_eq!(response, RootRangeResponse::Applied);
        assert_eq!(
            RootRange::new(temp_dir.path())
                .replay()
                .await
                .expect("replay root-range WAL after client_write"),
            vec![record]
        );

        harness
            .shutdown()
            .await
            .expect("shutdown single-node root range raft");
    }

    #[tokio::test]
    async fn raft_backed_root_range_submit_uses_openraft_client_write() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let (root_range, peer_server) = RootRange::new_raft_backed(
            temp_dir.path(),
            0,
            vec![RootRangePeer {
                node_id: 0,
                addr: "127.0.0.1:0".to_string(),
            }],
        )
        .await
        .expect("create raft-backed root range");
        peer_server
            .initialize_cluster()
            .await
            .expect("initialize single-node root range raft");
        peer_server
            .wait_for_leader(0, Duration::from_secs(3))
            .await
            .expect("single-node root range leader");

        let record = CommitRecord::new(
            TransactionId(15),
            LogicalTimestamp(19),
            vec![Mutation::CreateTable {
                table: "facade_events".to_string(),
                columns: vec!["id".to_string()],
            }],
        );

        root_range
            .submit(RootRangeCommand {
                record: record.clone(),
            })
            .await
            .expect("submit through raft-backed root range");

        assert_eq!(
            root_range
                .replay()
                .await
                .expect("replay root-range WAL after raft-backed submit"),
            vec![record]
        );

        peer_server
            .shutdown()
            .await
            .expect("shutdown raft-backed root range");
    }

    #[tokio::test]
    async fn raft_backed_root_range_recovers_raft_state_after_restart() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let peer = RootRangePeer {
            node_id: 0,
            addr: "127.0.0.1:0".to_string(),
        };
        let (root_range, peer_server) =
            RootRange::new_raft_backed(temp_dir.path(), 0, vec![peer.clone()])
                .await
                .expect("create raft-backed root range");
        peer_server
            .initialize_cluster()
            .await
            .expect("initialize single-node root range raft");
        peer_server
            .wait_for_leader(0, Duration::from_secs(3))
            .await
            .expect("single-node root range leader");

        let first_record = CommitRecord::new(
            TransactionId(16),
            LogicalTimestamp(20),
            vec![Mutation::CreateTable {
                table: "restart_events".to_string(),
                columns: vec!["id".to_string()],
            }],
        );
        root_range
            .submit(RootRangeCommand {
                record: first_record.clone(),
            })
            .await
            .expect("submit first root range command");
        peer_server
            .shutdown()
            .await
            .expect("shutdown first raft instance");

        let (restarted_root_range, restarted_peer_server) =
            RootRange::new_raft_backed(temp_dir.path(), 0, vec![peer])
                .await
                .expect("restart raft-backed root range");
        restarted_peer_server
            .wait_for_leader(0, Duration::from_secs(3))
            .await
            .expect("restarted single-node root range leader without reinitialize");

        let second_record = CommitRecord::new(
            TransactionId(17),
            LogicalTimestamp(21),
            vec![Mutation::InsertRow {
                table: "restart_events".to_string(),
                key: "1".to_string(),
                values: Vec::new(),
            }],
        );
        restarted_root_range
            .submit(RootRangeCommand {
                record: second_record.clone(),
            })
            .await
            .expect("submit second root range command after restart");

        assert_eq!(
            restarted_root_range
                .replay()
                .await
                .expect("replay root-range WAL after raft restart"),
            vec![first_record, second_record]
        );

        restarted_peer_server
            .shutdown()
            .await
            .expect("shutdown restarted raft instance");
    }

    #[tokio::test]
    async fn raft_backed_root_range_replicates_writes_over_peer_rpc() {
        let addr_0 = reserve_loopback_addr();
        let addr_1 = reserve_loopback_addr();
        let temp_dir_0 = tempfile::tempdir().expect("create node 0 temp dir");
        let temp_dir_1 = tempfile::tempdir().expect("create node 1 temp dir");
        let peers = vec![
            RootRangePeer {
                node_id: 0,
                addr: addr_0.to_string(),
            },
            RootRangePeer {
                node_id: 1,
                addr: addr_1.to_string(),
            },
        ];
        let (root_range_0, peer_server_0) =
            RootRange::new_raft_backed(temp_dir_0.path(), 0, peers.clone())
                .await
                .expect("create node 0 raft-backed root range");
        let (root_range_1, peer_server_1) = RootRange::new_raft_backed(temp_dir_1.path(), 1, peers)
            .await
            .expect("create node 1 raft-backed root range");
        let serve_0 = tokio::spawn({
            let peer_server = peer_server_0.clone();
            async move { peer_server.serve(addr_0).await }
        });
        let serve_1 = tokio::spawn({
            let peer_server = peer_server_1.clone();
            async move { peer_server.serve(addr_1).await }
        });
        peer_server_0
            .initialize_cluster()
            .await
            .expect("initialize node 0 root range raft");
        peer_server_1
            .initialize_cluster()
            .await
            .expect("initialize node 1 root range raft");
        let leader_id = peer_server_0
            .wait_for_any_leader(Duration::from_secs(5))
            .await
            .expect("elect a root range leader");

        let record = CommitRecord::new(
            TransactionId(18),
            LogicalTimestamp(22),
            vec![Mutation::CreateTable {
                table: "replicated_events".to_string(),
                columns: vec!["id".to_string()],
            }],
        );
        let writer = if leader_id == 0 {
            &root_range_0
        } else {
            &root_range_1
        };
        writer
            .submit(RootRangeCommand {
                record: record.clone(),
            })
            .await
            .expect("submit through elected root range leader");

        wait_for_replayed_records(&root_range_0, vec![record.clone()]).await;
        wait_for_replayed_records(&root_range_1, vec![record]).await;

        serve_0.abort();
        serve_1.abort();
        peer_server_0
            .shutdown()
            .await
            .expect("shutdown node 0 raft");
        peer_server_1
            .shutdown()
            .await
            .expect("shutdown node 1 raft");
    }

    fn reserve_loopback_addr() -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
        let addr = listener.local_addr().expect("read reserved loopback addr");
        drop(listener);
        addr
    }

    async fn wait_for_replayed_records(root_range: &RootRange, expected: Vec<CommitRecord>) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if root_range.replay().await.expect("replay root range WAL") == expected {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for replicated root range WAL"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
