use crate::ast::{QueryResult, Statement};
use opendb_common::{LogicalTimestamp, OpenDbError, OpenDbResult, TransactionId};
use opendb_storage::commit_stream::{ColumnValue, CommitRecord, Mutation, Value};
use opendb_storage::row_projection::RowProjection;

#[derive(Debug, Default)]
pub struct SqlEngine {
    next_tx: u64,
    projection: RowProjection,
    commits: Vec<CommitRecord>,
}

impl SqlEngine {
    pub fn execute(&mut self, statement: Statement) -> OpenDbResult<QueryResult> {
        match statement {
            Statement::CreateTable { table, columns } => {
                let record = self.build_next_record(vec![Mutation::CreateTable { table, columns }]);
                self.apply(record)?;
                Ok(QueryResult::Command {
                    tag: "CREATE TABLE".to_owned(),
                })
            }
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
                let row_key = match values.first() {
                    Some(Value::Int64(value)) => value.to_string(),
                    Some(Value::Text(value)) => value.clone(),
                    None => {
                        return Err(OpenDbError::Sql(
                            "INSERT requires at least one value".to_owned(),
                        ));
                    }
                };
                let column_values = table_state
                    .columns
                    .iter()
                    .cloned()
                    .zip(values)
                    .map(|(column, value)| ColumnValue { column, value })
                    .collect();
                let record = self.build_next_record(vec![Mutation::InsertRow {
                    table,
                    key: row_key,
                    values: column_values,
                }]);
                self.apply(record)?;
                Ok(QueryResult::Command {
                    tag: "INSERT 0 1".to_owned(),
                })
            }
            Statement::SelectAll { table } => {
                let table_state = self
                    .projection
                    .table(&table)
                    .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
                let rows = table_state
                    .rows
                    .values()
                    .map(|row| {
                        table_state
                            .columns
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
                    columns: table_state.columns.clone(),
                    rows,
                })
            }
        }
    }

    pub fn commits(&self) -> &[CommitRecord] {
        &self.commits
    }

    fn build_next_record(&self, mutations: Vec<Mutation>) -> CommitRecord {
        let next_tx = self.next_tx + 1;
        CommitRecord::new(TransactionId(next_tx), LogicalTimestamp(next_tx), mutations)
    }

    fn apply(&mut self, record: CommitRecord) -> OpenDbResult<()> {
        self.projection.apply(&record)?;
        self.next_tx = record.tx_id.0;
        self.commits.push(record);
        Ok(())
    }
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
                .execute(parse("CREATE TABLE accounts (id INT, name TEXT)").expect("parse"))
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
            .execute(parse("CREATE TABLE accounts (id INT, name TEXT)").expect("parse"))
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
            .execute(parse("CREATE TABLE accounts (id INT, name TEXT)").expect("parse"))
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
            .execute(parse("CREATE TABLE accounts (id INT, name TEXT)").expect("parse"))
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
}
