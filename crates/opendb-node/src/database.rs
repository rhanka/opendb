use opendb_common::OpenDbResult;
use opendb_consensus::root_range::RootRange;
use opendb_sql::{
    ast::{QueryResult, Statement},
    executor::{PreparedQuery, SqlEngine},
};
use std::path::Path;

#[derive(Debug)]
pub struct Database {
    root_range: RootRange,
    engine: SqlEngine,
}

impl Database {
    pub async fn open(data_dir: impl AsRef<Path>) -> OpenDbResult<Self> {
        let root_range = RootRange::new(data_dir.as_ref());
        let records = root_range.replay().await?;
        let engine = SqlEngine::from_commits(records)?;

        Ok(Self { root_range, engine })
    }

    pub async fn execute(&mut self, statement: Statement) -> OpenDbResult<QueryResult> {
        match self.engine.prepare(statement)? {
            PreparedQuery::Read(result) => Ok(result),
            PreparedQuery::Write { record, tag } => {
                self.root_range.apply_committed(&record).await?;
                self.engine.apply_committed(record)?;
                Ok(QueryResult::Command { tag })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use opendb_sql::{ast::QueryResult, parser::parse};
    use opendb_storage::commit_stream::Value;

    #[tokio::test]
    async fn execute_persists_writes_through_root_range_and_replays_on_reopen() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut database = Database::open(temp_dir.path())
            .await
            .expect("open database");

        assert_eq!(
            database
                .execute(parse("CREATE TABLE accounts (id INT, name TEXT)").expect("parse"))
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
        let mut reopened = Database::open(temp_dir.path())
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
}
