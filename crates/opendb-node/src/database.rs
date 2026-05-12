use opendb_common::{OpenDbError, OpenDbResult, RangeId};
use opendb_consensus::root_range::{RootRange, RootRangeCommand, RootRangePeerServer};
use opendb_sql::{
    ast::{QueryResult, Statement},
    executor::{PreparedQuery, RouteIntent, SqlEngine},
};
use opendb_storage::{commit_stream::CommitRecord, range_catalog::RangeCatalog};
use std::sync::Arc;

#[derive(Debug)]
pub struct Database {
    root_range: RootRange,
    engine: SqlEngine,
    range_catalog: RangeCatalog,
    peer_server: Option<Arc<RootRangePeerServer>>,
    recovery_status: DatabaseRecoveryStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseRecoveryStatus {
    pub root_descriptor_known: bool,
    pub wal_replay_completed: bool,
    pub last_replayed_tx_id: Option<u64>,
    pub last_replayed_ts: Option<u64>,
    pub archive_metadata_replayed: bool,
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
        })
    }

    pub async fn execute(&mut self, statement: Statement) -> OpenDbResult<QueryResult> {
        // Records may arrive on this replica via OpenRaft replication
        // (`apply_to_state_machine` → `root_range.apply_committed`) without
        // ever going through `Database::execute`. Refresh the SQL engine from
        // the canonical commit stream before every query so that the engine
        // never lags behind the WAL — required for a follower that just won an
        // election to be able to read records the previous leader committed.
        self.refresh_engine_from_wal().await?;

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
            } => {
                self.ensure_leader_for_client_query().await?;
                record.range_id = self.resolve_route(&route)?;
                self.root_range
                    .submit(RootRangeCommand {
                        record: record.clone(),
                    })
                    .await?;
                self.engine.apply_committed(record)?;
                Ok(QueryResult::Command { tag })
            }
        }
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
            .execute(Statement::SelectAll {
                table: "accounts".to_string(),
                predicate: None,
            })
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
