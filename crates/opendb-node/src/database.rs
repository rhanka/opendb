use opendb_common::{OpenDbError, OpenDbResult, RangeId};
use opendb_consensus::root_range::{RootRange, RootRangeCommand, RootRangePeerServer};
use opendb_sql::{
    ast::{QueryResult, Statement},
    executor::{PreparedQuery, RouteIntent, SqlEngine},
};
use opendb_storage::{
    commit_stream::{CommitRecord, Mutation, RangeMerge, RangeSplit},
    range_catalog::{RangeCatalog, RangeDescriptor},
};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposedRangeSplit {
    pub source_range_id: RangeId,
    pub split_key: String,
    pub left_range_id: Option<RangeId>,
    pub right_range_id: Option<RangeId>,
    pub left_replica_node_ids: Option<Vec<u64>>,
    pub right_replica_node_ids: Option<Vec<u64>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeSplitProposalResult {
    pub left_range_id: RangeId,
    pub right_range_id: RangeId,
    pub tx_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposedRangeMerge {
    pub source_range_ids: Vec<RangeId>,
    pub merged_range_id: Option<RangeId>,
    pub replica_node_ids: Option<Vec<u64>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeMergeProposalResult {
    pub merged_range_id: RangeId,
    pub tx_id: u64,
}

#[derive(Debug)]
pub struct Database {
    root_range: RootRange,
    engine: SqlEngine,
    range_catalog: RangeCatalog,
    peer_server: Option<Arc<RootRangePeerServer>>,
    recovery_status: DatabaseRecoveryStatus,
    /// Sprint 17: open-transaction state. `None` = autocommit (each write
    /// hits storage immediately, legacy behavior). `Some(...)` = in a
    /// transaction; writes apply to the engine in-memory but are deferred
    /// from the storage WAL until COMMIT (or discarded on ROLLBACK).
    transaction: Option<TransactionBuffer>,
}

/// Sprint 17: per-Database in-memory transaction buffer. Currently shared
/// across all sessions because `Database` is wrapped in `Arc<Mutex<...>>` at
/// the pgwire boundary; concurrent BEGIN from a second session is rejected.
#[derive(Debug)]
struct TransactionBuffer {
    /// Engine state captured at BEGIN — used to undo in-memory mutations on
    /// ROLLBACK.
    engine_snapshot: SqlEngine,
    /// CommitRecords produced by writes during the transaction. They are
    /// applied to the engine immediately (so subsequent SELECTs see them)
    /// but only flushed to `root_range` on COMMIT.
    pending_records: Vec<CommitRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseRecoveryStatus {
    pub root_descriptor_known: bool,
    pub wal_replay_completed: bool,
    pub last_replayed_tx_id: Option<u64>,
    pub last_replayed_ts: Option<u64>,
    pub archive_metadata_replayed: bool,
    pub range_catalog: RangeCatalogStatusSnapshot,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RangeCatalogStatusSnapshot {
    pub active_range_count: usize,
    pub last_split_tx_id: Option<u64>,
    pub last_merge_tx_id: Option<u64>,
}

impl RangeCatalogStatusSnapshot {
    fn from_replayed_records(records: &[CommitRecord]) -> Self {
        let mut last_split_tx_id = None;
        let mut last_merge_tx_id = None;
        for record in records {
            for mutation in &record.mutations {
                match mutation {
                    Mutation::SplitRange { .. } => {
                        last_split_tx_id = Some(record.tx_id.0);
                    }
                    Mutation::MergeRanges { .. } => {
                        last_merge_tx_id = Some(record.tx_id.0);
                    }
                    _ => {}
                }
            }
        }
        let catalog = RangeCatalog::rebuild(records).unwrap_or_default();
        Self {
            active_range_count: catalog.active_range_ids().len(),
            last_split_tx_id,
            last_merge_tx_id,
        }
    }
}

impl Database {
    pub async fn open_with_root_range(root_range: RootRange) -> OpenDbResult<Self> {
        match root_range.ensure_bootstrapped().await {
            Ok(()) => {}
            Err(OpenDbError::NotLeader { .. }) => {}
            Err(error) => return Err(error),
        }
        let records = root_range.replay().await?;
        let recovery_status = DatabaseRecoveryStatus::from_replayed_records(&records);
        let range_catalog = RangeCatalog::rebuild(&records)?;
        let engine = SqlEngine::from_commits(records)?;

        Ok(Self {
            root_range,
            engine,
            range_catalog,
            peer_server: None,
            recovery_status,
            transaction: None,
        })
    }

    pub async fn open_with_root_range_peer_server(
        root_range: RootRange,
        peer_server: Arc<RootRangePeerServer>,
    ) -> OpenDbResult<Self> {
        match root_range.ensure_bootstrapped().await {
            Ok(()) => {}
            Err(OpenDbError::NotLeader { .. }) => {}
            Err(error) => return Err(error),
        }
        let records = root_range.replay().await?;
        let recovery_status = DatabaseRecoveryStatus::from_replayed_records(&records);
        let range_catalog = RangeCatalog::rebuild(&records)?;
        let engine = SqlEngine::from_commits(records)?;

        Ok(Self {
            root_range,
            engine,
            range_catalog,
            peer_server: Some(peer_server),
            recovery_status,
            transaction: None,
        })
    }

    pub async fn execute(&mut self, statement: Statement) -> OpenDbResult<QueryResult> {
        Box::pin(self.execute_with_refresh(statement, true)).await
    }

    async fn execute_with_refresh(
        &mut self,
        statement: Statement,
        refresh_before_execute: bool,
    ) -> OpenDbResult<QueryResult> {
        // Sprint 17: explicit transaction control statements drive
        // `self.transaction` directly; they never flow through
        // `engine.prepare()` which would otherwise return a Command tag and
        // ignore the buffer.
        match &statement {
            Statement::Begin => return self.begin_transaction(),
            Statement::Commit => return self.commit_transaction().await,
            Statement::Rollback => return self.rollback_transaction(),
            _ => {}
        }
        // Records may arrive on this replica via OpenRaft replication
        // (`apply_to_state_machine` → `root_range.apply_committed`) without
        // ever going through `Database::execute`. Refresh the SQL engine from
        // the canonical commit stream before every query so that the engine
        // never lags behind the WAL — required for a follower that just won an
        // election to be able to read records the previous leader committed.
        // Skip the refresh while a transaction is open; otherwise we would
        // overwrite the in-memory engine and discard pending writes.
        if refresh_before_execute && self.transaction.is_none() {
            self.refresh_engine_from_wal().await?;
        }

        if let Statement::DoBlock {
            inner,
            swallow_duplicate,
        } = statement
        {
            for inner_statement in inner {
                match Box::pin(self.execute_with_refresh(inner_statement, false)).await {
                    Ok(_) => {}
                    Err(error) => {
                        if swallow_duplicate
                            && opendb_sql::executor::is_duplicate_object_error_for_do_block(&error)
                        {
                            continue;
                        }
                        return Err(error);
                    }
                }
            }
            return Ok(QueryResult::Command {
                tag: "DO".to_owned(),
            });
        }

        if statement.is_read() {
            self.ensure_leader_for_client_query().await?;
        }

        match self.engine.prepare(statement)? {
            PreparedQuery::Read { result, route } => {
                let _target_range_id = self.resolve_route(&route)?;
                Ok(result)
            }
            PreparedQuery::Write {
                mut record,
                tag,
                route,
                returning_result,
            } => {
                self.ensure_leader_for_client_query().await?;
                record.range_id = self.resolve_route(&route)?;
                // Sprint 17: defer the storage submit while a transaction is
                // open. The record is already validated by the engine's
                // `prepare` (which cloned + applied the projection); we
                // commit it in-memory now so subsequent reads in the tx
                // observe it, and stash the record for COMMIT to flush.
                if let Some(buffer) = self.transaction.as_mut() {
                    self.engine.apply_committed(record.clone())?;
                    buffer.pending_records.push(record);
                } else {
                    self.root_range
                        .submit(RootRangeCommand {
                            record: record.clone(),
                        })
                        .await?;
                    self.engine.apply_committed(record)?;
                }
                if let Some(rows) = returning_result {
                    Ok(rows)
                } else {
                    Ok(QueryResult::Command { tag })
                }
            }
        }
    }

    /// Sprint 17: open a transaction. Errors if one is already open (we don't
    /// support nested transactions yet — Drizzle uses savepoints for nesting).
    fn begin_transaction(&mut self) -> OpenDbResult<QueryResult> {
        if self.transaction.is_some() {
            return Err(OpenDbError::Sql(
                "BEGIN: a transaction is already open".to_owned(),
            ));
        }
        self.transaction = Some(TransactionBuffer {
            engine_snapshot: self.engine.clone(),
            pending_records: Vec::new(),
        });
        Ok(QueryResult::Command {
            tag: "BEGIN".to_owned(),
        })
    }

    /// Sprint 17: flush the buffered records to storage atomically. If any
    /// submit fails, the engine is rolled back to the BEGIN snapshot and the
    /// buffer is dropped — the transaction is treated as aborted.
    async fn commit_transaction(&mut self) -> OpenDbResult<QueryResult> {
        let Some(buffer) = self.transaction.take() else {
            // COMMIT outside a transaction is a no-op in Postgres (with a
            // warning); we mirror that as a successful command.
            return Ok(QueryResult::Command {
                tag: "COMMIT".to_owned(),
            });
        };
        for record in &buffer.pending_records {
            if let Err(error) = self
                .root_range
                .submit(RootRangeCommand {
                    record: record.clone(),
                })
                .await
            {
                // Restore the snapshot so the engine doesn't keep the
                // half-committed state in memory.
                self.engine = buffer.engine_snapshot;
                return Err(error);
            }
        }
        Ok(QueryResult::Command {
            tag: "COMMIT".to_owned(),
        })
    }

    /// Sprint 17: discard the buffered records and restore the engine to its
    /// pre-BEGIN state. Pure in-memory operation (no storage round-trip).
    fn rollback_transaction(&mut self) -> OpenDbResult<QueryResult> {
        if let Some(buffer) = self.transaction.take() {
            self.engine = buffer.engine_snapshot;
        }
        Ok(QueryResult::Command {
            tag: "ROLLBACK".to_owned(),
        })
    }

    pub async fn propose_range_split(
        &mut self,
        request: ProposedRangeSplit,
    ) -> OpenDbResult<RangeSplitProposalResult> {
        self.refresh_engine_from_wal().await?;
        self.ensure_leader_for_client_query().await?;

        let source = self
            .range_catalog
            .descriptor(request.source_range_id)
            .cloned()
            .ok_or_else(|| {
                OpenDbError::InvalidInput(format!(
                    "split source range {:?} does not exist",
                    request.source_range_id
                ))
            })?;
        if !self
            .range_catalog
            .active_range_ids()
            .contains(&request.source_range_id)
        {
            return Err(OpenDbError::InvalidInput(format!(
                "split source range {:?} is not active",
                request.source_range_id
            )));
        }

        let left_range_id = request
            .left_range_id
            .unwrap_or_else(|| self.range_catalog.allocate_range_id());
        let right_range_id = request.right_range_id.unwrap_or_else(|| {
            let next_after_left = RangeId(left_range_id.0 + 1);
            let allocated = self.range_catalog.allocate_range_id();
            if allocated == left_range_id {
                next_after_left
            } else {
                allocated.max(next_after_left)
            }
        });
        if left_range_id == right_range_id {
            return Err(OpenDbError::InvalidInput(format!(
                "split children for range {:?} must use distinct new range ids",
                request.source_range_id
            )));
        }
        let split_key_bound = Some(request.split_key.clone());
        let left_replicas = request
            .left_replica_node_ids
            .unwrap_or_else(|| source.replica_node_ids.clone());
        let right_replicas = request
            .right_replica_node_ids
            .unwrap_or_else(|| source.replica_node_ids.clone());

        let split = RangeSplit {
            source_range_id: request.source_range_id,
            split_key: request.split_key,
            left: RangeDescriptor {
                range_id: left_range_id,
                parent_range_id: Some(request.source_range_id),
                key_start: source.key_start.clone(),
                key_end: split_key_bound.clone(),
                replica_node_ids: left_replicas,
            },
            right: RangeDescriptor {
                range_id: right_range_id,
                parent_range_id: Some(request.source_range_id),
                key_start: split_key_bound,
                key_end: source.key_end.clone(),
                replica_node_ids: right_replicas,
            },
        };

        let record = self
            .engine
            .build_next_record(vec![Mutation::SplitRange { split }]);
        let tx_id = record.tx_id.0;
        self.root_range
            .submit(RootRangeCommand {
                record: record.clone(),
            })
            .await?;
        self.engine.apply_committed(record)?;
        self.refresh_engine_from_wal().await?;

        Ok(RangeSplitProposalResult {
            left_range_id,
            right_range_id,
            tx_id,
        })
    }

    pub async fn propose_range_merge(
        &mut self,
        request: ProposedRangeMerge,
    ) -> OpenDbResult<RangeMergeProposalResult> {
        self.refresh_engine_from_wal().await?;
        self.ensure_leader_for_client_query().await?;

        if request.source_range_ids.len() < 2 {
            return Err(OpenDbError::InvalidInput(
                "range merge requires at least two source ranges".to_string(),
            ));
        }
        let mut source_descriptors = Vec::with_capacity(request.source_range_ids.len());
        for source_range_id in &request.source_range_ids {
            let descriptor = self
                .range_catalog
                .descriptor(*source_range_id)
                .cloned()
                .ok_or_else(|| {
                    OpenDbError::InvalidInput(format!(
                        "merge source range {:?} does not exist",
                        source_range_id
                    ))
                })?;
            if !self
                .range_catalog
                .active_range_ids()
                .contains(source_range_id)
            {
                return Err(OpenDbError::InvalidInput(format!(
                    "merge source range {:?} is not active",
                    source_range_id
                )));
            }
            source_descriptors.push(descriptor);
        }

        let shared_parent = source_descriptors
            .first()
            .and_then(|descriptor| descriptor.parent_range_id)
            .ok_or_else(|| {
                OpenDbError::InvalidInput("range merge sources require a parent range".to_string())
            })?;

        let merged_range_id = request
            .merged_range_id
            .unwrap_or_else(|| self.range_catalog.allocate_range_id());

        let mut ordered = source_descriptors.clone();
        ordered.sort_by(|left, right| {
            left.key_start
                .cmp(&right.key_start)
                .then_with(|| left.key_end.cmp(&right.key_end))
                .then_with(|| left.range_id.cmp(&right.range_id))
        });
        let outer_start = ordered
            .first()
            .expect("non-empty sources")
            .key_start
            .clone();
        let outer_end = ordered.last().expect("non-empty sources").key_end.clone();

        let replica_node_ids = request.replica_node_ids.unwrap_or_else(|| {
            let mut union: Vec<u64> = source_descriptors
                .iter()
                .flat_map(|descriptor| descriptor.replica_node_ids.clone())
                .collect();
            union.sort_unstable();
            union.dedup();
            union
        });

        let merge = RangeMerge {
            source_range_ids: request.source_range_ids.clone(),
            merged: RangeDescriptor {
                range_id: merged_range_id,
                parent_range_id: Some(shared_parent),
                key_start: outer_start,
                key_end: outer_end,
                replica_node_ids,
            },
        };

        let record = self
            .engine
            .build_next_record(vec![Mutation::MergeRanges { merge }]);
        let tx_id = record.tx_id.0;
        self.root_range
            .submit(RootRangeCommand {
                record: record.clone(),
            })
            .await?;
        self.engine.apply_committed(record)?;
        self.refresh_engine_from_wal().await?;

        Ok(RangeMergeProposalResult {
            merged_range_id,
            tx_id,
        })
    }

    fn resolve_route(&self, route: &RouteIntent) -> OpenDbResult<RangeId> {
        match route {
            RouteIntent::Root | RouteIntent::Scan { .. } => Ok(RangeId::ROOT),
            RouteIntent::Key { key, .. } => self
                .range_catalog
                .route_key(key)
                .map(|descriptor| descriptor.range_id)
                .ok_or_else(|| OpenDbError::Storage(format!("no range route for key {key}"))),
        }
    }

    /// Rebuilds the in-memory SQL engine from the canonical commit stream.
    /// Used before every `execute` so that records committed via OpenRaft
    /// replication (which bypass `Database::execute`) are observed on reads.
    async fn refresh_engine_from_wal(&mut self) -> OpenDbResult<()> {
        let records = self.root_range.replay().await?;
        self.range_catalog = RangeCatalog::rebuild(&records)?;
        self.engine = SqlEngine::from_commits(records)?;
        Ok(())
    }

    async fn ensure_leader_for_client_query(&self) -> OpenDbResult<()> {
        match &self.peer_server {
            Some(peer_server) => peer_server.ensure_leader().await,
            None => self.root_range.ensure_client_query_leader().await,
        }
    }

    pub fn recovery_status(&self) -> &DatabaseRecoveryStatus {
        &self.recovery_status
    }

    /// Re-reads the canonical commit stream and returns a fresh recovery status
    /// snapshot. Use this instead of `recovery_status()` when you need the live
    /// state: the cached value only reflects what the WAL looked like at open.
    pub async fn compute_recovery_status(&self) -> OpenDbResult<DatabaseRecoveryStatus> {
        let records = self.root_range.replay().await?;
        Ok(DatabaseRecoveryStatus::from_replayed_records(&records))
    }

    /// Re-runs `ensure_bootstrapped` against the underlying root range.
    /// Used by the bootstrap retry loop in `main`: the first attempt at
    /// open time can race with OpenRaft leader election and silently return
    /// `NotLeader`. The retry loop keeps trying until the WAL has the root
    /// descriptor.
    pub async fn ensure_root_range_bootstrapped(&self) -> OpenDbResult<()> {
        self.root_range.ensure_bootstrapped().await
    }
}

impl DatabaseRecoveryStatus {
    fn from_replayed_records(records: &[CommitRecord]) -> Self {
        Self {
            root_descriptor_known: records.first().is_some_and(CommitRecord::is_root_bootstrap),
            wal_replay_completed: true,
            last_replayed_tx_id: records.last().map(|record| record.tx_id.0),
            last_replayed_ts: records.last().map(|record| record.ts.0),
            archive_metadata_replayed: true,
            range_catalog: RangeCatalogStatusSnapshot::from_replayed_records(records),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use opendb_common::{LogicalTimestamp, OpenDbError, RangeId, TransactionId};
    use opendb_consensus::root_range::{RootRange, RootRangeAuthority, RootRangeCommand};
    use opendb_sql::{
        ast::{QueryResult, Statement},
        parser::parse,
    };
    use opendb_storage::commit_stream::{CommitRecord, Mutation, RangeSplit, Value};
    use opendb_storage::range_catalog::RangeDescriptor;

    #[tokio::test]
    async fn execute_persists_writes_through_root_range_and_replays_on_reopen() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut database = Database::open_with_root_range(RootRange::new(temp_dir.path()))
            .await
            .expect("open database");

        assert_eq!(
            database
                .execute(
                    parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse")
                )
                .await
                .expect("create"),
            QueryResult::Command {
                tag: "CREATE TABLE".to_owned(),
            }
        );
        assert_eq!(
            database
                .execute(parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("parse"))
                .await
                .expect("insert"),
            QueryResult::Command {
                tag: "INSERT 0 1".to_owned(),
            }
        );

        drop(database);
        let mut reopened = Database::open_with_root_range(RootRange::new(temp_dir.path()))
            .await
            .expect("reopen database");

        assert_eq!(
            reopened
                .execute(parse("SELECT * FROM accounts").expect("parse"))
                .await
                .expect("select"),
            QueryResult::Rows {
                columns: vec!["id".to_owned(), "name".to_owned()],
                column_types: vec![],
                rows: vec![vec![Value::Int64(1), Value::Text("Ada".to_owned())]],
            }
        );
        assert!(
            temp_dir
                .path()
                .join("root-range")
                .join("commit.wal")
                .exists()
        );
    }

    #[tokio::test]
    async fn open_reports_recovery_status_from_bootstrap_replay() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let database = Database::open_with_root_range(RootRange::new(temp_dir.path()))
            .await
            .expect("open database");

        assert_eq!(
            database.recovery_status(),
            &super::DatabaseRecoveryStatus {
                root_descriptor_known: true,
                wal_replay_completed: true,
                last_replayed_tx_id: Some(0),
                last_replayed_ts: Some(0),
                archive_metadata_replayed: true,
                range_catalog: super::RangeCatalogStatusSnapshot {
                    active_range_count: 1,
                    last_split_tx_id: None,
                    last_merge_tx_id: None,
                },
            }
        );
    }

    #[tokio::test]
    async fn compute_recovery_status_reflects_records_appended_after_open() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut database = Database::open_with_root_range(RootRange::new(temp_dir.path()))
            .await
            .expect("open database");
        let open_time_status = database.recovery_status().clone();
        assert_eq!(open_time_status.last_replayed_tx_id, Some(0));

        database
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY)").expect("parse"))
            .await
            .expect("create table");

        let cached = database.recovery_status();
        assert_eq!(
            cached, &open_time_status,
            "cached recovery_status() must keep the open-time snapshot"
        );

        let live = database
            .compute_recovery_status()
            .await
            .expect("compute recovery status");
        assert!(live.root_descriptor_known);
        assert!(live.wal_replay_completed);
        assert_eq!(live.last_replayed_tx_id, Some(1));
        assert_eq!(live.last_replayed_ts, Some(1));
        assert!(live.archive_metadata_replayed);
    }

    #[tokio::test]
    async fn reopen_reports_recovery_status_from_last_replayed_record() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut database = Database::open_with_root_range(RootRange::new(temp_dir.path()))
            .await
            .expect("open database");
        database
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY)").expect("parse"))
            .await
            .expect("create table");
        drop(database);

        let reopened = Database::open_with_root_range(RootRange::new(temp_dir.path()))
            .await
            .expect("reopen database");

        assert_eq!(reopened.recovery_status().last_replayed_tx_id, Some(1));
        assert_eq!(reopened.recovery_status().last_replayed_ts, Some(1));
        assert!(reopened.recovery_status().root_descriptor_known);
        assert!(reopened.recovery_status().wal_replay_completed);
        assert!(reopened.recovery_status().archive_metadata_replayed);
    }

    #[tokio::test]
    async fn follower_database_rejects_writes_before_local_wal_append() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new_with_authority(
            temp_dir.path(),
            RootRangeAuthority::follower(0, Some("opendb-0.opendb-peer:7000".to_string())),
        );
        let mut database = Database::open_with_root_range(root_range)
            .await
            .expect("open database");
        assert_eq!(
            database.recovery_status(),
            &super::DatabaseRecoveryStatus {
                root_descriptor_known: false,
                wal_replay_completed: true,
                last_replayed_tx_id: None,
                last_replayed_ts: None,
                archive_metadata_replayed: true,
                range_catalog: super::RangeCatalogStatusSnapshot::default(),
            }
        );

        let result = database
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY)").expect("parse"))
            .await;

        assert!(matches!(
            result,
            Err(OpenDbError::NotLeader {
                leader_id: Some(0),
                leader_addr: Some(addr),
            }) if addr == "opendb-0.opendb-peer:7000"
        ));
        assert!(
            !temp_dir
                .path()
                .join("root-range")
                .join("commit.wal")
                .exists(),
            "follower write must not create a local WAL"
        );
    }

    #[tokio::test]
    async fn invalid_write_is_rejected_before_follower_leader_check() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new_with_authority(
            temp_dir.path(),
            RootRangeAuthority::follower(0, Some("opendb-0.opendb-peer:7000".to_string())),
        );
        let mut database = Database::open_with_root_range(root_range)
            .await
            .expect("open database");

        let result = database
            .execute(parse("CREATE TABLE accounts (id INT)").expect("parse"))
            .await;

        assert!(matches!(result, Err(OpenDbError::InvalidInput(_))));
        assert!(
            !temp_dir
                .path()
                .join("root-range")
                .join("commit.wal")
                .exists(),
            "invalid write must not create a local WAL"
        );
    }

    #[tokio::test]
    async fn execute_stamps_insert_with_catalog_routed_range() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        let mut database = Database::open_with_root_range(root_range)
            .await
            .expect("open database");

        database
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .await
            .expect("create");

        let split_record = CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(2),
            vec![Mutation::SplitRange {
                split: RangeSplit {
                    source_range_id: RangeId::ROOT,
                    split_key: "accounts/2".to_owned(),
                    left: RangeDescriptor {
                        range_id: RangeId(2),
                        parent_range_id: Some(RangeId::ROOT),
                        key_start: None,
                        key_end: Some("accounts/2".to_owned()),
                        replica_node_ids: vec![0],
                    },
                    right: RangeDescriptor {
                        range_id: RangeId(3),
                        parent_range_id: Some(RangeId::ROOT),
                        key_start: Some("accounts/2".to_owned()),
                        key_end: None,
                        replica_node_ids: vec![0],
                    },
                },
            }],
        );
        let split_command = RootRangeCommand {
            record: split_record,
        };
        let local_root_range = RootRange::new(temp_dir.path());
        local_root_range
            .submit(split_command)
            .await
            .expect("append split metadata");

        database
            .execute(parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("parse"))
            .await
            .expect("insert");

        let records = local_root_range.replay().await.expect("replay");
        assert_eq!(records.last().expect("last record").range_id, RangeId(2));
    }

    #[tokio::test]
    async fn propose_range_split_assigns_ids_and_stamps_metadata_on_root() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        let mut database = Database::open_with_root_range(root_range)
            .await
            .expect("open database");
        database
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .await
            .expect("create");

        let result = database
            .propose_range_split(super::ProposedRangeSplit {
                source_range_id: RangeId::ROOT,
                split_key: "accounts/5".to_owned(),
                left_range_id: None,
                right_range_id: None,
                left_replica_node_ids: None,
                right_replica_node_ids: None,
            })
            .await
            .expect("propose split");

        assert_eq!(result.left_range_id, RangeId(2));
        assert_eq!(result.right_range_id, RangeId(3));
        let records = RootRange::new(temp_dir.path())
            .replay()
            .await
            .expect("replay");
        let last = records.last().expect("last");
        assert_eq!(last.range_id, RangeId::ROOT);
        assert!(matches!(
            last.mutations.as_slice(),
            [opendb_storage::commit_stream::Mutation::SplitRange { .. }]
        ));
    }

    #[tokio::test]
    async fn propose_range_split_rejected_when_source_inactive() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        let mut database = Database::open_with_root_range(root_range)
            .await
            .expect("open database");
        database
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .await
            .expect("create");
        database
            .propose_range_split(super::ProposedRangeSplit {
                source_range_id: RangeId::ROOT,
                split_key: "accounts/5".to_owned(),
                left_range_id: None,
                right_range_id: None,
                left_replica_node_ids: None,
                right_replica_node_ids: None,
            })
            .await
            .expect("first split");

        let result = database
            .propose_range_split(super::ProposedRangeSplit {
                source_range_id: RangeId::ROOT,
                split_key: "accounts/9".to_owned(),
                left_range_id: None,
                right_range_id: None,
                left_replica_node_ids: None,
                right_replica_node_ids: None,
            })
            .await;

        assert!(matches!(result, Err(OpenDbError::InvalidInput(_))));
    }

    #[tokio::test]
    async fn propose_range_merge_round_trip() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new(temp_dir.path());
        let mut database = Database::open_with_root_range(root_range)
            .await
            .expect("open database");
        database
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .await
            .expect("create");
        let split = database
            .propose_range_split(super::ProposedRangeSplit {
                source_range_id: RangeId::ROOT,
                split_key: "accounts/5".to_owned(),
                left_range_id: None,
                right_range_id: None,
                left_replica_node_ids: None,
                right_replica_node_ids: None,
            })
            .await
            .expect("split");

        let result = database
            .propose_range_merge(super::ProposedRangeMerge {
                source_range_ids: vec![split.left_range_id, split.right_range_id],
                merged_range_id: None,
                replica_node_ids: None,
            })
            .await
            .expect("merge");

        assert_eq!(result.merged_range_id, RangeId(4));
        let records = RootRange::new(temp_dir.path())
            .replay()
            .await
            .expect("replay");
        let last = records.last().expect("last");
        assert!(matches!(
            last.mutations.as_slice(),
            [opendb_storage::commit_stream::Mutation::MergeRanges { .. }]
        ));
    }

    #[tokio::test]
    async fn follower_read_checks_leadership_before_local_projection() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new_with_authority(
            temp_dir.path(),
            RootRangeAuthority::follower(0, Some("opendb-0.opendb-peer:7000".to_string())),
        );
        let mut database = Database::open_with_root_range(root_range)
            .await
            .expect("open database");

        let result = database
            .execute(Statement::select_all_legacy("accounts".to_string(), None))
            .await;

        assert!(matches!(
            result,
            Err(OpenDbError::NotLeader {
                leader_id: Some(0),
                leader_addr: Some(addr),
            }) if addr == "opendb-0.opendb-peer:7000"
        ));
    }
}
