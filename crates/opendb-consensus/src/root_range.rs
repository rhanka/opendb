use opendb_common::{OpenDbError, OpenDbResult, RangeId};
use opendb_storage::{commit_stream::CommitRecord, wal::Wal};
use std::path::Path;

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

#[derive(Clone, Debug)]
pub struct RootRange {
    range_id: RangeId,
    wal: Wal,
    authority: RootRangeAuthority,
}

impl RootRange {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self::new_with_authority(data_dir, RootRangeAuthority::Standalone)
    }

    pub fn new_with_authority(data_dir: impl AsRef<Path>, authority: RootRangeAuthority) -> Self {
        Self {
            range_id: RangeId::ROOT,
            wal: Wal::new(data_dir.as_ref().join("root-range").join("commit.wal")),
            authority,
        }
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
    /// Standalone and explicit leader modes can commit locally for Milestone 1.
    /// Followers must reject before touching the local WAL; the future OpenRaft
    /// client-write path replaces the leader arm without changing SQL/pgwire.
    pub async fn submit(&self, command: RootRangeCommand) -> OpenDbResult<()> {
        self.validate_apply_record(&command.record)?;
        match &self.authority {
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
            .raft()
            .wait(Some(Duration::from_secs(3)))
            .current_leader(0, "single-node leader")
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
            .raft()
            .client_write(RootRangeCommand {
                record: record.clone(),
            })
            .await
            .expect("client_write root range command");

        assert_eq!(response.data, RootRangeResponse::Applied);
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
}
