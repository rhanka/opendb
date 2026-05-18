use crate::raft::{RootRangeRaftHarness, RootRangeResponse};
use opendb_common::{OpenDbError, OpenDbResult, RangeId};
use opendb_storage::{
    archive_manifest::ArchiveManifest,
    commit_stream::{CommitRecord, Mutation},
    range_catalog::RangeCatalog,
    row_projection::RowProjection,
    wal::Wal,
};
use openraft::BasicNode;
use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

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
    semantic_append_lock: Arc<Mutex<()>>,
    openraft_submit_lock: Arc<Mutex<()>>,
    bootstrap_replica_node_ids: Vec<OpenDbRaftNodeId>,
}

impl RootRange {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self::new_with_authority(data_dir, RootRangeAuthority::Standalone)
    }

    pub fn new_with_authority(data_dir: impl AsRef<Path>, authority: RootRangeAuthority) -> Self {
        Self::new_with_authority_and_bootstrap_replicas(data_dir, authority, vec![0])
    }

    pub(crate) fn new_with_authority_and_bootstrap_replicas(
        data_dir: impl AsRef<Path>,
        authority: RootRangeAuthority,
        mut bootstrap_replica_node_ids: Vec<OpenDbRaftNodeId>,
    ) -> Self {
        bootstrap_replica_node_ids.sort_unstable();
        bootstrap_replica_node_ids.dedup();
        Self {
            range_id: RangeId::ROOT,
            wal: Wal::new(data_dir.as_ref().join("root-range").join("commit.wal")),
            proposal_path: RootRangeProposalPath::Local(authority),
            semantic_append_lock: Arc::new(Mutex::new(())),
            openraft_submit_lock: Arc::new(Mutex::new(())),
            bootstrap_replica_node_ids,
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
        let bootstrap_replica_node_ids = members.keys().copied().collect::<Vec<_>>();
        let raft = Arc::new(RootRangeRaftHarness::new(node_id, data_dir.as_ref(), members).await?);
        let root_range = Self {
            range_id: RangeId::ROOT,
            wal: Wal::new(data_dir.as_ref().join("root-range").join("commit.wal")),
            proposal_path: RootRangeProposalPath::OpenRaft(Arc::clone(&raft)),
            semantic_append_lock: Arc::new(Mutex::new(())),
            openraft_submit_lock: Arc::new(Mutex::new(())),
            bootstrap_replica_node_ids,
        };

        Ok((root_range, RootRangePeerServer { raft }))
    }

    pub fn range_id(&self) -> RangeId {
        self.range_id
    }

    pub async fn ensure_bootstrapped(&self) -> OpenDbResult<()> {
        match &self.proposal_path {
            RootRangeProposalPath::OpenRaft(raft) => {
                let _guard = self.openraft_submit_lock.lock().await;
                let records = self.wal.read_all().await?;
                let expected = self.expected_bootstrap_record();
                match records.first() {
                    None => {
                        self.validate_apply_record(&expected)?;
                        self.validate_replay_sequence(std::slice::from_ref(&expected))?;
                        let response = raft.submit(RootRangeCommand { record: expected }).await?;
                        if response == RootRangeResponse::Applied {
                            Ok(())
                        } else {
                            Err(OpenDbError::Storage(format!(
                                "root range raft returned unexpected bootstrap response: {response:?}"
                            )))
                        }
                    }
                    Some(_) => self.validate_bootstrap_records(&records),
                }
            }
            RootRangeProposalPath::Local(_) => {
                let _guard = self.semantic_append_lock.lock().await;
                let records = self.wal.read_all().await?;
                let expected = self.expected_bootstrap_record();
                match records.first() {
                    None => match self.local_bootstrap_authority() {
                        Ok(()) => {
                            self.validate_apply_record(&expected)?;
                            self.wal.append(&expected).await
                        }
                        Err(error) => Err(error),
                    },
                    Some(_) => self.validate_bootstrap_records(&records),
                }
            }
        }
    }

    fn local_bootstrap_authority(&self) -> OpenDbResult<()> {
        match &self.proposal_path {
            RootRangeProposalPath::Local(
                RootRangeAuthority::Standalone | RootRangeAuthority::Leader { .. },
            ) => Ok(()),
            RootRangeProposalPath::Local(RootRangeAuthority::Follower {
                leader_id,
                leader_addr,
            }) => Err(OpenDbError::NotLeader {
                leader_id: *leader_id,
                leader_addr: leader_addr.clone(),
            }),
            RootRangeProposalPath::OpenRaft(_) => Ok(()),
        }
    }

    pub async fn ensure_client_query_leader(&self) -> OpenDbResult<()> {
        match &self.proposal_path {
            RootRangeProposalPath::OpenRaft(_) => Err(OpenDbError::NotLeader {
                leader_id: None,
                leader_addr: None,
            }),
            RootRangeProposalPath::Local(authority) => match authority {
                RootRangeAuthority::Standalone | RootRangeAuthority::Leader { .. } => Ok(()),
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

    /// Applies a root-range record that has already been committed.
    ///
    /// Milestone 1 only wires this apply-side path. Callers must not use it as
    /// a proposal path; use `submit` for the reserved OpenRaft-facing API.
    pub async fn apply_committed(&self, record: &CommitRecord) -> OpenDbResult<()> {
        self.validate_apply_record(record)?;
        let _guard = self.semantic_append_lock.lock().await;
        self.validate_semantic_append(record).await?;
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
                let _guard = self.openraft_submit_lock.lock().await;
                self.validate_semantic_append(&command.record).await?;
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
        Ok(self.replay_with_wal_len().await?.0)
    }

    pub async fn replay_with_wal_len(&self) -> OpenDbResult<(Vec<CommitRecord>, u64)> {
        let (records, wal_len) = self.wal.read_all_with_len().await?;
        for (index, record) in records.iter().enumerate() {
            self.validate_replayed_record(index, record)?;
        }
        self.validate_replay_sequence(&records)?;
        RowProjection::rebuild(&records).map_err(|error| {
            OpenDbError::Storage(format!(
                "root range WAL failed semantic replay validation: {error}"
            ))
        })?;
        RangeCatalog::rebuild(&records).map_err(|error| {
            OpenDbError::Storage(format!(
                "root range WAL failed range catalog replay validation: {error}"
            ))
        })?;
        ArchiveManifest::rebuild(&records).map_err(|error| {
            OpenDbError::Storage(format!(
                "root range WAL failed archive manifest replay validation: {error}"
            ))
        })?;
        validate_record_routes(&records)?;
        Ok((records, wal_len))
    }

    pub async fn wal_byte_len(&self) -> OpenDbResult<u64> {
        self.wal.byte_len().await
    }

    fn validate_apply_record(&self, record: &CommitRecord) -> OpenDbResult<()> {
        if record.version != CommitRecord::VERSION {
            return Err(OpenDbError::InvalidInput(format!(
                "root range requires commit record version {}, got {}",
                CommitRecord::VERSION,
                record.version
            )));
        }
        Ok(())
    }

    async fn validate_semantic_append(&self, record: &CommitRecord) -> OpenDbResult<()> {
        validate_metadata_record_range(record).map_err(sequence_validation_error_for_append)?;
        let mut records = self.replay().await?;
        if records.is_empty() && !record.is_root_bootstrap() {
            return Err(OpenDbError::InvalidInput(
                "root descriptor bootstrap must be committed before user records".to_string(),
            ));
        }
        records.push(record.clone());
        self.validate_replay_sequence(&records)
            .map_err(sequence_validation_error_for_append)?;
        RowProjection::rebuild(&records)?;
        RangeCatalog::rebuild(&records)?;
        ArchiveManifest::rebuild(&records)?;
        validate_record_routes(&records).map_err(sequence_validation_error_for_append)?;
        Ok(())
    }

    fn validate_replayed_record(&self, index: usize, record: &CommitRecord) -> OpenDbResult<()> {
        if record.version != CommitRecord::VERSION {
            return Err(OpenDbError::Storage(format!(
                "root range WAL record {index} has commit version {}, expected {}",
                record.version,
                CommitRecord::VERSION
            )));
        }
        Ok(())
    }

    fn expected_bootstrap_record(&self) -> CommitRecord {
        CommitRecord::root_bootstrap(self.bootstrap_replica_node_ids.clone())
    }

    fn validate_bootstrap_records(&self, records: &[CommitRecord]) -> OpenDbResult<()> {
        match records.first() {
            Some(first) if first == &self.expected_bootstrap_record() => {
                self.validate_replay_sequence(records)
            }
            Some(first) if first.is_root_bootstrap() => Err(OpenDbError::Storage(format!(
                "root descriptor bootstrap does not match expected replicas {:?}: got {:?}",
                self.bootstrap_replica_node_ids, first
            ))),
            Some(_) => Err(OpenDbError::Storage(
                "root descriptor bootstrap is missing from the first WAL record".to_string(),
            )),
            None => Ok(()),
        }
    }

    fn validate_replay_sequence(&self, records: &[CommitRecord]) -> OpenDbResult<()> {
        let Some(first) = records.first() else {
            return Ok(());
        };
        let expected = self.expected_bootstrap_record();
        if first != &expected {
            if first.is_root_bootstrap() {
                return Err(OpenDbError::Storage(format!(
                    "root descriptor bootstrap does not match expected replicas {:?}: got {:?}",
                    self.bootstrap_replica_node_ids, first
                )));
            }
            return Err(OpenDbError::Storage(
                "root descriptor bootstrap is missing from the first WAL record".to_string(),
            ));
        }

        let mut previous_tx_id = first.tx_id;
        let mut previous_ts = first.ts;
        for (index, record) in records.iter().enumerate().skip(1) {
            if record.is_root_bootstrap() || record.tx_id == opendb_common::TransactionId(0) {
                return Err(OpenDbError::Storage(format!(
                    "root range record {index} repeats root descriptor bootstrap tx_id"
                )));
            }
            if record.ts == opendb_common::LogicalTimestamp(0) {
                return Err(OpenDbError::Storage(format!(
                    "root range record {index} repeats root descriptor bootstrap timestamp"
                )));
            }
            if record.mutations.is_empty() {
                return Err(OpenDbError::Storage(format!(
                    "root range record {index} must contain at least one mutation"
                )));
            }
            if record.actor.trim().is_empty() || record.actor != record.actor.trim() {
                return Err(OpenDbError::Storage(format!(
                    "root range record {index} actor must not be empty or padded"
                )));
            }
            if record.tx_id <= previous_tx_id {
                return Err(OpenDbError::Storage(format!(
                    "root range record {index} must have strictly increasing tx_id"
                )));
            }
            if record.ts <= previous_ts {
                return Err(OpenDbError::Storage(format!(
                    "root range record {index} must have strictly increasing ts"
                )));
            }
            previous_tx_id = record.tx_id;
            previous_ts = record.ts;
        }

        Ok(())
    }
}

fn validate_record_routes(records: &[CommitRecord]) -> OpenDbResult<()> {
    let mut catalog = RangeCatalog::default();
    for record in records {
        validate_metadata_record_range(record)?;
        catalog.apply(record)?;
        for mutation in &record.mutations {
            if let Mutation::InsertRow { table, key, .. } = mutation {
                let route_key = format!("{table}/{key}");
                let expected = catalog
                    .route_key(&route_key)
                    .map(|descriptor| descriptor.range_id)
                    .ok_or_else(|| {
                        OpenDbError::Storage(format!("no range route for key {route_key}"))
                    })?;
                if record.range_id != expected {
                    return Err(OpenDbError::Storage(format!(
                        "row route key {route_key} expected range {:?}, got {:?}",
                        expected, record.range_id
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_metadata_record_range(record: &CommitRecord) -> OpenDbResult<()> {
    let has_metadata = record.mutations.iter().any(|mutation| {
        matches!(
            mutation,
            Mutation::CreateTable { .. }
                | Mutation::PutRangeDescriptor { .. }
                | Mutation::SplitRange { .. }
                | Mutation::MergeRanges { .. }
                | Mutation::PutArchiveObjectPointer { .. }
                | Mutation::PutRecoveryArtifactPointer { .. }
        )
    });
    let has_row = record
        .mutations
        .iter()
        .any(|mutation| matches!(mutation, Mutation::InsertRow { .. }));
    if has_metadata && has_row {
        return Err(OpenDbError::Storage(
            "commit record must not mix metadata and row mutations".to_string(),
        ));
    }
    if has_metadata && record.range_id != opendb_common::RangeId::ROOT {
        return Err(OpenDbError::Storage(format!(
            "metadata mutation must use root range, got {:?}",
            record.range_id
        )));
    }
    Ok(())
}

fn sequence_validation_error_for_append(error: OpenDbError) -> OpenDbError {
    match error {
        OpenDbError::Storage(message) => OpenDbError::InvalidInput(message),
        error => error,
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
    use opendb_storage::archive_manifest::{
        ArchiveBackendKind, ArchiveObjectPointer, CompressionKind, RecoveryArtifactKind,
        RecoveryArtifactPointer,
    };
    use opendb_storage::commit_stream::{
        ColumnDefinition, ColumnType, ColumnValue, Mutation, RangeSplit, Value,
    };
    use opendb_storage::range_catalog::RangeDescriptor;
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
                columns: account_columns(),
            }],
        );

        let root_range = RootRange::new(temp_dir.path());
        assert_eq!(root_range.range_id(), RangeId::ROOT);
        bootstrap(&root_range).await;
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
            with_bootstrap(&restarted_root_range, vec![record])
        );
    }

    #[tokio::test]
    async fn root_range_bootstraps_root_descriptor_once() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());

        root_range.ensure_bootstrapped().await.expect("bootstrap");
        root_range
            .ensure_bootstrapped()
            .await
            .expect("idempotent bootstrap");

        assert_eq!(
            root_range.replay().await.expect("replay"),
            vec![CommitRecord::root_bootstrap(vec![0])]
        );
    }

    #[tokio::test]
    async fn user_record_before_bootstrap_is_rejected() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        let record = CommitRecord::new(
            TransactionId(1),
            LogicalTimestamp(1),
            vec![Mutation::CreateTable {
                table: "accounts".to_string(),
                columns: account_columns(),
            }],
        );

        let error = root_range
            .apply_committed(&record)
            .await
            .expect_err("reject missing bootstrap");

        assert!(
            error.to_string().contains("root descriptor bootstrap"),
            "unexpected error: {error}"
        );
        assert_eq!(root_range.replay().await.expect("replay"), Vec::new());
    }

    #[tokio::test]
    async fn openraft_root_range_requires_peer_server_for_client_query_leadership() {
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
        .expect("reopen raft-backed root range");

        let result = root_range.ensure_client_query_leader().await;

        assert!(matches!(
            result,
            Err(OpenDbError::NotLeader {
                leader_id: None,
                leader_addr: None,
            })
        ));
        peer_server.shutdown().await.expect("shutdown raft");
    }

    #[tokio::test]
    async fn replay_rejects_non_monotonic_user_tx_id() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal = Wal::new(temp_dir.path().join("root-range").join("commit.wal"));
        wal.append(&CommitRecord::root_bootstrap(vec![0]))
            .await
            .expect("append bootstrap");
        wal.append(&CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(2),
            vec![Mutation::CreateTable {
                table: "accounts".to_string(),
                columns: account_columns(),
            }],
        ))
        .await
        .expect("append first user record");
        wal.append(&CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(3),
            vec![Mutation::CreateTable {
                table: "orders".to_string(),
                columns: id_columns(),
            }],
        ))
        .await
        .expect("append forged duplicate tx");

        let error = RootRange::new(temp_dir.path())
            .replay()
            .await
            .expect_err("reject duplicate tx");

        assert!(
            error.to_string().contains("strictly increasing tx_id"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn root_range_replays_range_descriptor_metadata_after_restart() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let record = CommitRecord::new(
            TransactionId(8),
            LogicalTimestamp(12),
            vec![Mutation::PutRangeDescriptor {
                descriptor: RangeDescriptor {
                    range_id: RangeId(2),
                    parent_range_id: Some(RangeId::ROOT),
                    key_start: Some("accounts/".to_string()),
                    key_end: Some("orders/".to_string()),
                    replica_node_ids: vec![0],
                },
            }],
        );

        let root_range = RootRange::new(temp_dir.path());
        bootstrap(&root_range).await;
        root_range
            .apply_committed(&record)
            .await
            .expect("append range descriptor record");

        let restarted_root_range = RootRange::new(temp_dir.path());
        assert_eq!(
            restarted_root_range
                .replay()
                .await
                .expect("replay range catalog metadata"),
            with_bootstrap(&restarted_root_range, vec![record])
        );
    }

    #[tokio::test]
    async fn root_range_replays_archive_object_pointer_after_restart() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let record = CommitRecord::new(
            TransactionId(9),
            LogicalTimestamp(13),
            vec![Mutation::PutArchiveObjectPointer {
                pointer: archive_object_pointer(),
            }],
        );

        let root_range = RootRange::new(temp_dir.path());
        bootstrap(&root_range).await;
        root_range
            .apply_committed(&record)
            .await
            .expect("append archive object pointer record");

        let restarted_root_range = RootRange::new(temp_dir.path());
        assert_eq!(
            restarted_root_range
                .replay()
                .await
                .expect("replay archive manifest metadata"),
            with_bootstrap(&restarted_root_range, vec![record])
        );
    }

    #[tokio::test]
    async fn root_range_replays_recovery_artifact_after_restart() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let record = CommitRecord::new(
            TransactionId(10),
            LogicalTimestamp(14),
            vec![Mutation::PutRecoveryArtifactPointer {
                artifact: recovery_artifact_pointer(),
            }],
        );

        let root_range = RootRange::new(temp_dir.path());
        bootstrap(&root_range).await;
        root_range
            .apply_committed(&record)
            .await
            .expect("append recovery artifact record");

        let restarted_root_range = RootRange::new(temp_dir.path());
        assert_eq!(
            restarted_root_range
                .replay()
                .await
                .expect("replay recovery artifact metadata"),
            with_bootstrap(&restarted_root_range, vec![record])
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
                columns: id_columns(),
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
                columns: id_columns(),
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
    async fn apply_committed_rejects_range_descriptor_with_missing_parent_without_persisting() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        bootstrap(&root_range).await;
        let record = CommitRecord::new(
            TransactionId(9),
            LogicalTimestamp(13),
            vec![Mutation::PutRangeDescriptor {
                descriptor: RangeDescriptor {
                    range_id: RangeId(2),
                    parent_range_id: Some(RangeId(404)),
                    key_start: Some("accounts/".to_string()),
                    key_end: Some("orders/".to_string()),
                    replica_node_ids: vec![0, 1, 2],
                },
            }],
        );

        let result = root_range.apply_committed(&record).await;

        assert!(matches!(
            result,
            Err(OpenDbError::InvalidInput(message)) if message.contains("missing parent range")
        ));
        assert_eq!(
            root_range
                .replay()
                .await
                .expect("replay after rejected range descriptor"),
            vec![root_range.expected_bootstrap_record()]
        );
    }

    #[tokio::test]
    async fn apply_committed_rejects_invalid_archive_pointer_without_persisting() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        bootstrap(&root_range).await;
        let record = CommitRecord::new(
            TransactionId(10),
            LogicalTimestamp(14),
            vec![Mutation::PutArchiveObjectPointer {
                pointer: ArchiveObjectPointer {
                    backend: ArchiveBackendKind::S3Compatible,
                    bucket: "opendb-archives".to_string(),
                    key: "root-range/00000004.wal".to_string(),
                    content_sha256: "not-a-sha".to_string(),
                },
            }],
        );

        let result = root_range.apply_committed(&record).await;

        assert!(matches!(
            result,
            Err(OpenDbError::InvalidInput(message)) if message.contains("content_sha256")
        ));
        assert_eq!(
            root_range
                .replay()
                .await
                .expect("replay after rejected archive pointer"),
            vec![root_range.expected_bootstrap_record()]
        );
    }

    #[tokio::test]
    async fn apply_committed_rejects_invalid_recovery_artifact_without_persisting() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        bootstrap(&root_range).await;
        let record = CommitRecord::new(
            TransactionId(10),
            LogicalTimestamp(14),
            vec![Mutation::PutRecoveryArtifactPointer {
                artifact: RecoveryArtifactPointer {
                    record_count: 0,
                    ..recovery_artifact_pointer()
                },
            }],
        );

        let result = root_range.apply_committed(&record).await;

        assert!(matches!(
            result,
            Err(OpenDbError::InvalidInput(message)) if message.contains("record_count")
        ));
        assert_eq!(
            root_range
                .replay()
                .await
                .expect("replay after rejected recovery artifact"),
            vec![root_range.expected_bootstrap_record()]
        );
    }

    #[tokio::test]
    async fn replay_rejects_invalid_recovery_artifact_in_wal() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal = Wal::new(temp_dir.path().join("root-range").join("commit.wal"));
        wal.append(&CommitRecord::root_bootstrap(vec![0]))
            .await
            .expect("append bootstrap");
        wal.append(&CommitRecord::new(
            TransactionId(1),
            LogicalTimestamp(1),
            vec![Mutation::PutRecoveryArtifactPointer {
                artifact: RecoveryArtifactPointer {
                    record_count: 0,
                    ..recovery_artifact_pointer()
                },
            }],
        ))
        .await
        .expect("append forged recovery artifact");

        let error = RootRange::new(temp_dir.path())
            .replay()
            .await
            .expect_err("reject invalid recovery artifact");

        assert!(
            error
                .to_string()
                .contains("root range WAL failed archive manifest replay validation"),
            "unexpected error: {error}"
        );
        assert!(
            error.to_string().contains("record_count"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn concurrent_apply_committed_serializes_validation_with_wal_append() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        bootstrap(&root_range).await;
        let first = CommitRecord::new(
            TransactionId(10),
            LogicalTimestamp(14),
            vec![Mutation::CreateTable {
                table: "events".to_string(),
                columns: id_columns(),
            }],
        );
        let second = CommitRecord::new(
            TransactionId(11),
            LogicalTimestamp(15),
            vec![Mutation::CreateTable {
                table: "events".to_string(),
                columns: id_columns(),
            }],
        );
        let left = root_range.clone();
        let right = root_range.clone();

        let (left_result, right_result) =
            tokio::join!(left.apply_committed(&first), right.apply_committed(&second));

        let results = [&left_result, &right_result];
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            1,
            "exactly one duplicate table create should commit"
        );
        assert!(
            results.iter().any(|result| matches!(
                result,
                Err(OpenDbError::InvalidInput(message)) if message.contains("table already exists")
            )),
            "one concurrent duplicate table create should be rejected"
        );
        let records = root_range
            .replay()
            .await
            .expect("replay after concurrent apply");
        assert_eq!(records.len(), 2);
        assert!(
            records == with_bootstrap(&root_range, vec![first])
                || records == with_bootstrap(&root_range, vec![second]),
            "unexpected committed record: {records:?}"
        );
    }

    #[tokio::test]
    async fn replay_accepts_known_routed_non_root_row_record() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal = Wal::new(temp_dir.path().join("root-range").join("commit.wal"));
        wal.append(&CommitRecord::root_bootstrap(vec![0]))
            .await
            .expect("append bootstrap");
        wal.append(&CommitRecord::new(
            TransactionId(1),
            LogicalTimestamp(1),
            vec![Mutation::CreateTable {
                table: "accounts".to_owned(),
                columns: account_columns(),
            }],
        ))
        .await
        .expect("append create");
        wal.append(&CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(2),
            vec![Mutation::SplitRange {
                split: RangeSplit {
                    source_range_id: RangeId::ROOT,
                    split_key: "orders/".to_owned(),
                    left: RangeDescriptor {
                        range_id: RangeId(2),
                        parent_range_id: Some(RangeId::ROOT),
                        key_start: None,
                        key_end: Some("orders/".to_owned()),
                        replica_node_ids: vec![0],
                    },
                    right: RangeDescriptor {
                        range_id: RangeId(3),
                        parent_range_id: Some(RangeId::ROOT),
                        key_start: Some("orders/".to_owned()),
                        key_end: None,
                        replica_node_ids: vec![0],
                    },
                },
            }],
        ))
        .await
        .expect("append split");
        wal.append(&CommitRecord::new_for_range(
            RangeId(2),
            TransactionId(3),
            LogicalTimestamp(3),
            CommitRecord::BOOTSTRAP_ACTOR,
            vec![Mutation::InsertRow {
                table: "accounts".to_owned(),
                key: "1".to_owned(),
                values: vec![
                    ColumnValue {
                        column: "id".to_owned(),
                        value: Value::Int64(1),
                    },
                    ColumnValue {
                        column: "name".to_owned(),
                        value: Value::Text("Ada".to_owned()),
                    },
                ],
            }],
        ))
        .await
        .expect("append routed insert");

        let records = RootRange::new(temp_dir.path())
            .replay()
            .await
            .expect("replay routed WAL");

        assert_eq!(records.last().expect("last").range_id, RangeId(2));
    }

    #[tokio::test]
    async fn replay_rejects_unknown_non_root_row_range() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let wal = Wal::new(temp_dir.path().join("root-range").join("commit.wal"));
        wal.append(&CommitRecord::root_bootstrap(vec![0]))
            .await
            .expect("append bootstrap");
        wal.append(&CommitRecord::new(
            TransactionId(1),
            LogicalTimestamp(1),
            vec![Mutation::CreateTable {
                table: "accounts".to_owned(),
                columns: vec![ColumnDefinition::primary_key("id", ColumnType::Int64)],
            }],
        ))
        .await
        .expect("append create");
        wal.append(&CommitRecord::new_for_range(
            RangeId(404),
            TransactionId(2),
            LogicalTimestamp(2),
            CommitRecord::BOOTSTRAP_ACTOR,
            vec![Mutation::InsertRow {
                table: "accounts".to_owned(),
                key: "1".to_owned(),
                values: vec![ColumnValue {
                    column: "id".to_owned(),
                    value: Value::Int64(1),
                }],
            }],
        ))
        .await
        .expect("append forged insert");

        let error = RootRange::new(temp_dir.path())
            .replay()
            .await
            .expect_err("reject unknown range");

        assert!(error.to_string().contains("range"));
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
                columns: id_columns(),
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
    async fn submit_rejects_invalid_schema_before_wal_append() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        let invalid_record = CommitRecord::new(
            TransactionId(10),
            LogicalTimestamp(14),
            vec![Mutation::CreateTable {
                table: "ledger".to_string(),
                columns: vec![ColumnDefinition::new("id", ColumnType::Int64)],
            }],
        );

        let result = root_range
            .submit(RootRangeCommand {
                record: invalid_record,
            })
            .await;

        assert!(matches!(result, Err(OpenDbError::InvalidInput(_))));
        assert!(
            !temp_dir
                .path()
                .join("root-range")
                .join("commit.wal")
                .exists(),
            "invalid root-range command must not create a WAL"
        );
    }

    #[tokio::test]
    async fn submit_rejects_unsupported_commit_version_before_wal_append() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        let mut invalid_record = CommitRecord::new(
            TransactionId(10),
            LogicalTimestamp(14),
            vec![Mutation::CreateTable {
                table: "ledger".to_string(),
                columns: id_columns(),
            }],
        );
        invalid_record.version = CommitRecord::VERSION + 1;

        let result = root_range
            .submit(RootRangeCommand {
                record: invalid_record,
            })
            .await;

        assert!(matches!(result, Err(OpenDbError::InvalidInput(_))));
        assert!(
            !temp_dir
                .path()
                .join("root-range")
                .join("commit.wal")
                .exists(),
            "unsupported commit version must not create a WAL"
        );
    }

    #[tokio::test]
    async fn leader_submit_persists_root_commands_through_consensus_boundary() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range =
            RootRange::new_with_authority(temp_dir.path(), RootRangeAuthority::leader(0));
        bootstrap(&root_range).await;
        let record = CommitRecord::new(
            TransactionId(11),
            LogicalTimestamp(15),
            vec![Mutation::CreateTable {
                table: "audit_log".to_string(),
                columns: id_columns(),
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
            with_bootstrap(&root_range, vec![record])
        );
    }

    #[tokio::test]
    async fn standalone_submit_persists_root_commands_through_consensus_boundary() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        bootstrap(&root_range).await;
        let record = CommitRecord::new(
            TransactionId(12),
            LogicalTimestamp(16),
            vec![Mutation::CreateTable {
                table: "events".to_string(),
                columns: id_columns(),
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
            with_bootstrap(&root_range, vec![record])
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
                columns: id_columns(),
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
        let bootstrap_record = CommitRecord::root_bootstrap(vec![0]);
        harness
            .submit(RootRangeCommand {
                record: bootstrap_record.clone(),
            })
            .await
            .expect("client_write root bootstrap");

        let record = CommitRecord::new(
            TransactionId(14),
            LogicalTimestamp(18),
            vec![Mutation::CreateTable {
                table: "raft_events".to_string(),
                columns: id_columns(),
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
            vec![bootstrap_record, record]
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
        bootstrap(&root_range).await;

        let record = CommitRecord::new(
            TransactionId(15),
            LogicalTimestamp(19),
            vec![Mutation::CreateTable {
                table: "facade_events".to_string(),
                columns: id_columns(),
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
            with_bootstrap(&root_range, vec![record])
        );

        peer_server
            .shutdown()
            .await
            .expect("shutdown raft-backed root range");
    }

    #[tokio::test]
    async fn raft_backed_concurrent_submit_serializes_validation_before_proposal() {
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
        bootstrap(&root_range).await;

        let first = CommitRecord::new(
            TransactionId(18),
            LogicalTimestamp(22),
            vec![Mutation::CreateTable {
                table: "serialized_events".to_string(),
                columns: id_columns(),
            }],
        );
        let second = CommitRecord::new(
            TransactionId(19),
            LogicalTimestamp(23),
            vec![Mutation::CreateTable {
                table: "serialized_events".to_string(),
                columns: id_columns(),
            }],
        );
        let left = root_range.clone();
        let right = root_range.clone();

        let (left_result, right_result) = tokio::join!(
            left.submit(RootRangeCommand {
                record: first.clone(),
            }),
            right.submit(RootRangeCommand {
                record: second.clone(),
            })
        );

        let results = [&left_result, &right_result];
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            1,
            "exactly one duplicate table create should commit through OpenRaft"
        );
        assert!(
            results.iter().any(|result| matches!(
                result,
                Err(OpenDbError::InvalidInput(message)) if message.contains("table already exists")
            )),
            "one concurrent duplicate table create should be rejected before proposal"
        );
        let records = root_range
            .replay()
            .await
            .expect("replay after concurrent raft submit");
        assert_eq!(records.len(), 2);
        assert!(
            records == with_bootstrap(&root_range, vec![first])
                || records == with_bootstrap(&root_range, vec![second]),
            "unexpected committed record: {records:?}"
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
        bootstrap(&root_range).await;

        let first_record = CommitRecord::new(
            TransactionId(16),
            LogicalTimestamp(20),
            vec![Mutation::CreateTable {
                table: "restart_events".to_string(),
                columns: id_columns(),
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
        restarted_root_range
            .ensure_bootstrapped()
            .await
            .expect("restarted bootstrap is idempotent");
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
                values: vec![opendb_storage::commit_stream::ColumnValue {
                    column: "id".to_string(),
                    value: opendb_storage::commit_stream::Value::Int64(1),
                }],
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
            with_bootstrap(&restarted_root_range, vec![first_record, second_record])
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
        let writer = if leader_id == 0 {
            &root_range_0
        } else {
            &root_range_1
        };
        bootstrap(writer).await;

        let record = CommitRecord::new(
            TransactionId(18),
            LogicalTimestamp(22),
            vec![Mutation::CreateTable {
                table: "replicated_events".to_string(),
                columns: id_columns(),
            }],
        );
        writer
            .submit(RootRangeCommand {
                record: record.clone(),
            })
            .await
            .expect("submit through elected root range leader");

        wait_for_replayed_records(
            &root_range_0,
            with_bootstrap(&root_range_0, vec![record.clone()]),
        )
        .await;
        wait_for_replayed_records(&root_range_1, with_bootstrap(&root_range_1, vec![record])).await;

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

    async fn bootstrap(root_range: &RootRange) {
        root_range
            .ensure_bootstrapped()
            .await
            .expect("bootstrap root range");
    }

    fn with_bootstrap(root_range: &RootRange, records: Vec<CommitRecord>) -> Vec<CommitRecord> {
        let mut expected = vec![root_range.expected_bootstrap_record()];
        expected.extend(records);
        expected
    }

    fn reserve_loopback_addr() -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
        let addr = listener.local_addr().expect("read reserved loopback addr");
        drop(listener);
        addr
    }

    fn id_columns() -> Vec<ColumnDefinition> {
        vec![ColumnDefinition::primary_key("id", ColumnType::Int64)]
    }

    fn account_columns() -> Vec<ColumnDefinition> {
        vec![
            ColumnDefinition::primary_key("id", ColumnType::Int64),
            ColumnDefinition::new("name", ColumnType::Text),
        ]
    }

    fn archive_object_pointer() -> ArchiveObjectPointer {
        ArchiveObjectPointer {
            backend: ArchiveBackendKind::S3Compatible,
            bucket: "opendb-archives".to_string(),
            key: "root-range/00000004.wal".to_string(),
            content_sha256: "2222222222222222222222222222222222222222222222222222222222222222"
                .to_string(),
        }
    }

    fn recovery_artifact_pointer() -> RecoveryArtifactPointer {
        RecoveryArtifactPointer {
            artifact_kind: RecoveryArtifactKind::WalSegment,
            range_id: RangeId::ROOT,
            object: ArchiveObjectPointer {
                key: "root-range/00000005.wal".to_string(),
                ..archive_object_pointer()
            },
            format_version: 1,
            tx_id_start: TransactionId(0),
            tx_id_end: TransactionId(10),
            ts_start: LogicalTimestamp(0),
            ts_end: LogicalTimestamp(10),
            record_count: 11,
            byte_len: 4096,
            compression: CompressionKind::None,
        }
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
