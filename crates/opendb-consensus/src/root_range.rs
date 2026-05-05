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

#[derive(Clone, Debug)]
pub struct RootRange {
    range_id: RangeId,
    wal: Wal,
}

impl RootRange {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            range_id: RangeId::ROOT,
            wal: Wal::new(data_dir.as_ref().join("root-range").join("commit.wal")),
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

    /// Validates a root-range command reserved for future OpenRaft proposals.
    ///
    /// Valid commands return a not-yet-wired error until the real OpenRaft
    /// proposal path exists. Invalid non-root commands are rejected first.
    pub async fn submit(&self, command: RootRangeCommand) -> OpenDbResult<()> {
        self.validate_apply_record(&command.record)?;
        Err(OpenDbError::InvalidInput(
            "root range OpenRaft proposal submit path is not yet wired".to_string(),
        ))
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
    async fn submit_returns_not_yet_wired_for_valid_root_commands() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        let record = CommitRecord::new(
            TransactionId(11),
            LogicalTimestamp(15),
            vec![Mutation::CreateTable {
                table: "audit_log".to_string(),
                columns: vec!["id".to_string()],
            }],
        );

        let result = root_range.submit(RootRangeCommand { record }).await;

        assert!(matches!(
            result,
            Err(OpenDbError::InvalidInput(message))
                if message.contains("not yet wired") && message.contains("OpenRaft")
        ));
    }
}
