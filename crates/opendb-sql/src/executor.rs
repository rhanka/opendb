use crate::ast::{QueryResult, Statement};
use opendb_common::{LogicalTimestamp, OpenDbError, OpenDbResult, TransactionId};
use opendb_storage::commit_stream::{ColumnType, ColumnValue, CommitRecord, Mutation, Value};
use opendb_storage::row_projection::RowProjection;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedQuery {
    Read(QueryResult),
    Write { record: CommitRecord, tag: String },
}

#[derive(Debug, Default)]
pub struct SqlEngine {
    next_tx: u64,
    projection: RowProjection,
    commits: Vec<CommitRecord>,
}

impl SqlEngine {
    pub fn execute(&mut self, statement: Statement) -> OpenDbResult<QueryResult> {
        match self.prepare(statement)? {
            PreparedQuery::Read(result) => Ok(result),
            PreparedQuery::Write { record, tag } => {
                self.apply_committed(record)?;
                Ok(QueryResult::Command { tag })
            }
        }
    }

    pub fn prepare(&self, statement: Statement) -> OpenDbResult<PreparedQuery> {
        match statement {
            Statement::CreateTable { table, columns } => self.prepare_write(
                vec![Mutation::CreateTable { table, columns }],
                "CREATE TABLE",
            ),
            Statement::Insert { table, values } => {
                let table_state = self
                    .projection
                    .table(&table)
                    .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
                if values.len() != table_state.columns.len() {
                    return Err(OpenDbError::Sql(format!(
                        "expected {} values, got {}",
                        table_state.columns.len(),
                        values.len()
                    )));
                }
                let primary_key_index = table_state.primary_key_index().ok_or_else(|| {
                    OpenDbError::InvalidInput(format!("table {table} has no primary key"))
                })?;
                let primary_key_column =
                    table_state.columns.get(primary_key_index).ok_or_else(|| {
                        OpenDbError::InvalidInput(format!("table {table} has no primary key"))
                    })?;
                let primary_key_value = values
                    .get(primary_key_index)
                    .ok_or_else(|| OpenDbError::Sql("INSERT requires a primary key".to_owned()))?;
                if !value_matches_type(primary_key_value, &primary_key_column.data_type) {
                    return Err(OpenDbError::InvalidInput(format!(
                        "value for primary key column {} on table {} does not match {:?}",
                        primary_key_column.name, table, primary_key_column.data_type
                    )));
                }
                let row_key = value_to_key(primary_key_value);
                let columns = table_state.columns.clone();
                let column_values = columns
                    .into_iter()
                    .zip(values)
                    .map(|(column, value)| ColumnValue {
                        column: column.name,
                        value,
                    })
                    .collect();
                self.prepare_write(
                    vec![Mutation::InsertRow {
                        table,
                        key: row_key,
                        values: column_values,
                    }],
                    "INSERT 0 1",
                )
            }
            Statement::SelectAll { table } => self.select_all(&table).map(PreparedQuery::Read),
        }
    }

    pub fn commits(&self) -> &[CommitRecord] {
        &self.commits
    }

    pub fn apply_committed(&mut self, record: CommitRecord) -> OpenDbResult<()> {
        self.projection.apply(&record)?;
        self.next_tx = self.next_tx.max(record.tx_id.0);
        self.commits.push(record);
        Ok(())
    }

    pub fn from_commits(records: Vec<CommitRecord>) -> OpenDbResult<Self> {
        let mut engine = Self::default();
        for record in records {
            engine.apply_committed(record)?;
        }
        Ok(engine)
    }

    fn build_next_record(&self, mutations: Vec<Mutation>) -> CommitRecord {
        let next_tx = self.next_tx + 1;
        CommitRecord::new(TransactionId(next_tx), LogicalTimestamp(next_tx), mutations)
    }

    fn prepare_write(&self, mutations: Vec<Mutation>, tag: &str) -> OpenDbResult<PreparedQuery> {
        let record = self.build_next_record(mutations);
        let mut validated_projection = self.projection.clone();
        validated_projection.apply(&record)?;
        Ok(PreparedQuery::Write {
            record,
            tag: tag.to_owned(),
        })
    }

    fn select_all(&self, table: &str) -> OpenDbResult<QueryResult> {
        let table_state = self
            .projection
            .table(table)
            .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
        let column_names = table_state.column_names();
        let rows = table_state
            .rows
            .values()
            .map(|row| {
                column_names
                    .iter()
                    .map(|column| {
                        row.get(column).cloned().ok_or_else(|| {
                            OpenDbError::Storage(format!(
                                "missing projected column {column} on table {table}"
                            ))
                        })
                    })
                    .collect::<OpenDbResult<Vec<_>>>()
            })
            .collect::<OpenDbResult<Vec<_>>>()?;
        Ok(QueryResult::Rows {
            columns: column_names,
            rows,
        })
    }
}

fn value_to_key(value: &Value) -> String {
    match value {
        Value::Int64(value) => value.to_string(),
        Value::Text(value) => value.clone(),
    }
}

fn value_matches_type(value: &Value, data_type: &ColumnType) -> bool {
    matches!(
        (value, data_type),
        (Value::Int64(_), ColumnType::Int64) | (Value::Text(_), ColumnType::Text)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse;

    #[test]
    fn executes_create_insert_select_against_row_projection() {
        let mut engine = SqlEngine::default();
        assert_eq!(
            engine
                .execute(
                    parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse")
                )
                .expect("create"),
            QueryResult::Command {
                tag: "CREATE TABLE".to_owned()
            }
        );
        assert_eq!(
            engine
                .execute(parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("parse"))
                .expect("insert"),
            QueryResult::Command {
                tag: "INSERT 0 1".to_owned()
            }
        );
        assert_eq!(
            engine
                .execute(parse("SELECT * FROM accounts").expect("parse"))
                .expect("select"),
            QueryResult::Rows {
                columns: vec!["id".to_owned(), "name".to_owned()],
                rows: vec![vec![Value::Int64(1), Value::Text("Ada".to_owned())]],
            }
        );
        assert_eq!(engine.commits().len(), 2);
    }

    #[test]
    fn quoted_string_with_comma_executes() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        engine
            .execute(parse("INSERT INTO accounts VALUES (1, 'Ada, Lovelace')").expect("parse"))
            .expect("insert");

        assert_eq!(
            engine
                .execute(parse("SELECT * FROM accounts").expect("parse"))
                .expect("select"),
            QueryResult::Rows {
                columns: vec!["id".to_owned(), "name".to_owned()],
                rows: vec![vec![
                    Value::Int64(1),
                    Value::Text("Ada, Lovelace".to_owned())
                ]],
            }
        );
    }

    #[test]
    fn failed_duplicate_key_insert_does_not_commit_or_consume_tx_id() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        engine
            .execute(parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("parse"))
            .expect("insert");

        let before_commits = engine.commits().len();
        let duplicate =
            engine.execute(parse("INSERT INTO accounts VALUES (1, 'Grace')").expect("parse"));

        assert!(duplicate.is_err());
        assert_eq!(engine.commits().len(), before_commits);

        engine
            .execute(parse("INSERT INTO accounts VALUES (2, 'Grace')").expect("parse"))
            .expect("second insert");
        let last = engine.commits().last().expect("last commit");
        assert_eq!(last.tx_id, TransactionId(3));
        assert_eq!(last.ts, LogicalTimestamp(3));
    }

    #[test]
    fn multi_row_select_order_is_deterministic() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        engine
            .execute(parse("INSERT INTO accounts VALUES (2, 'Grace')").expect("parse"))
            .expect("insert 2");
        engine
            .execute(parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("parse"))
            .expect("insert 1");

        assert_eq!(
            engine
                .execute(parse("SELECT * FROM accounts").expect("parse"))
                .expect("select"),
            QueryResult::Rows {
                columns: vec!["id".to_owned(), "name".to_owned()],
                rows: vec![
                    vec![Value::Int64(1), Value::Text("Ada".to_owned())],
                    vec![Value::Int64(2), Value::Text("Grace".to_owned())],
                ],
            }
        );
    }

    #[test]
    fn prepared_invalid_write_does_not_mutate_or_consume_tx_id() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        engine
            .execute(parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("parse"))
            .expect("insert");

        let before_commits = engine.commits().len();
        let duplicate =
            engine.prepare(parse("INSERT INTO accounts VALUES (1, 'Grace')").expect("parse"));

        assert!(duplicate.is_err());
        assert_eq!(engine.commits().len(), before_commits);
        assert_eq!(
            engine
                .execute(parse("SELECT * FROM accounts").expect("parse"))
                .expect("select"),
            QueryResult::Rows {
                columns: vec!["id".to_owned(), "name".to_owned()],
                rows: vec![vec![Value::Int64(1), Value::Text("Ada".to_owned())]],
            }
        );

        engine
            .execute(parse("INSERT INTO accounts VALUES (2, 'Grace')").expect("parse"))
            .expect("second insert");
        let last = engine.commits().last().expect("last commit");
        assert_eq!(last.tx_id, TransactionId(3));
        assert_eq!(last.ts, LogicalTimestamp(3));
    }

    #[test]
    fn prepared_write_does_not_mutate_until_applied() {
        let mut engine = SqlEngine::default();
        let prepared_create = engine
            .prepare(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("prepare create");

        assert_eq!(engine.commits().len(), 0);
        assert!(
            engine
                .execute(parse("SELECT * FROM accounts").expect("parse"))
                .is_err()
        );

        let PreparedQuery::Write { record, tag } = prepared_create else {
            panic!("create should prepare as write");
        };
        assert_eq!(tag, "CREATE TABLE");
        engine.apply_committed(record).expect("apply create");

        assert_eq!(
            engine
                .execute(parse("SELECT * FROM accounts").expect("parse"))
                .expect("select"),
            QueryResult::Rows {
                columns: vec!["id".to_owned(), "name".to_owned()],
                rows: Vec::new(),
            }
        );
    }

    #[test]
    fn from_commits_rebuilds_rows_and_next_tx() {
        let mut source = SqlEngine::default();
        source
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        source
            .execute(parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("parse"))
            .expect("insert");

        let mut rebuilt = SqlEngine::from_commits(source.commits().to_vec()).expect("rebuild");

        assert_eq!(
            rebuilt
                .execute(parse("SELECT * FROM accounts").expect("parse"))
                .expect("select"),
            QueryResult::Rows {
                columns: vec!["id".to_owned(), "name".to_owned()],
                rows: vec![vec![Value::Int64(1), Value::Text("Ada".to_owned())]],
            }
        );

        rebuilt
            .execute(parse("INSERT INTO accounts VALUES (2, 'Grace')").expect("parse"))
            .expect("second insert");
        let last = rebuilt.commits().last().expect("last commit");
        assert_eq!(last.tx_id, TransactionId(3));
        assert_eq!(last.ts, LogicalTimestamp(3));
    }

    #[test]
    fn create_table_requires_explicit_primary_key() {
        let mut engine = SqlEngine::default();

        let result =
            engine.execute(parse("CREATE TABLE accounts (id INT, name TEXT)").expect("parse"));

        assert!(matches!(result, Err(OpenDbError::InvalidInput(_))));
        assert_eq!(engine.commits().len(), 0);
    }

    #[test]
    fn primary_key_column_drives_row_identity_even_when_not_first() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE accounts (name TEXT, id INT PRIMARY KEY)").expect("parse"))
            .expect("create");
        engine
            .execute(parse("INSERT INTO accounts VALUES ('Ada', 1)").expect("parse"))
            .expect("insert first");

        let duplicate =
            engine.execute(parse("INSERT INTO accounts VALUES ('Grace', 1)").expect("parse"));

        assert!(matches!(duplicate, Err(OpenDbError::InvalidInput(_))));
        assert_eq!(engine.commits().len(), 2);
    }

    #[test]
    fn insert_values_must_match_declared_column_types() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");

        let invalid_id =
            engine.execute(parse("INSERT INTO accounts VALUES ('one', 'Ada')").expect("parse"));
        let invalid_name =
            engine.execute(parse("INSERT INTO accounts VALUES (1, 42)").expect("parse"));

        assert!(matches!(invalid_id, Err(OpenDbError::InvalidInput(_))));
        assert!(matches!(invalid_name, Err(OpenDbError::InvalidInput(_))));
        assert_eq!(engine.commits().len(), 1);
    }
}
