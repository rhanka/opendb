use opendb_common::OpenDbResult;
use opendb_consensus::root_range::{RootRange, RootRangeCommand, RootRangePeerServer};
use opendb_sql::{
    ast::{QueryResult, Statement},
    executor::{PreparedQuery, SqlEngine},
};
use std::sync::Arc;

#[derive(Debug)]
pub struct Database {
    root_range: RootRange,
    engine: SqlEngine,
    peer_server: Option<Arc<RootRangePeerServer>>,
}

impl Database {
    pub async fn open_with_root_range(root_range: RootRange) -> OpenDbResult<Self> {
        let records = root_range.replay().await?;
        let engine = SqlEngine::from_commits(records)?;

        Ok(Self {
            root_range,
            engine,
            peer_server: None,
        })
    }

    pub async fn open_with_root_range_peer_server(
        root_range: RootRange,
        peer_server: Arc<RootRangePeerServer>,
    ) -> OpenDbResult<Self> {
        let mut database = Self::open_with_root_range(root_range).await?;
        database.peer_server = Some(peer_server);
        Ok(database)
    }

    pub async fn execute(&mut self, statement: Statement) -> OpenDbResult<QueryResult> {
        if statement.is_read() {
            self.ensure_leader_for_client_query().await?;
        }

        match self.engine.prepare(statement)? {
            PreparedQuery::Read(result) => Ok(result),
            PreparedQuery::Write { record, tag } => {
                self.ensure_leader_for_client_query().await?;
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

    async fn ensure_leader_for_client_query(&self) -> OpenDbResult<()> {
        match &self.peer_server {
            Some(peer_server) => peer_server.ensure_leader().await,
            None => self.root_range.ensure_client_query_leader().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use opendb_common::OpenDbError;
    use opendb_consensus::root_range::{RootRange, RootRangeAuthority};
    use opendb_sql::{
        ast::{QueryResult, Statement},
        parser::parse,
    };
    use opendb_storage::commit_stream::Value;

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
    async fn follower_database_rejects_writes_before_local_wal_append() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let root_range = RootRange::new_with_authority(
            temp_dir.path(),
            RootRangeAuthority::follower(0, Some("opendb-0.opendb-peer:7000".to_string())),
        );
        let mut database = Database::open_with_root_range(root_range)
            .await
            .expect("open database");

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
