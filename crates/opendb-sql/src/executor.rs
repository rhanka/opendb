use crate::ast::{Predicate, QueryResult, Statement};
use opendb_common::{LogicalTimestamp, OpenDbError, OpenDbResult, TransactionId};
use opendb_storage::commit_stream::{
    ColumnDefinition, ColumnType, ColumnValue, CommitRecord, DefaultExpr, Mutation, Value,
};
use opendb_storage::row_projection::RowProjection;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RouteIntent {
    Root,
    Key { table: String, key: String },
    Scan { table: String },
}

#[derive(Clone, Debug, PartialEq)]
pub enum PreparedQuery {
    Read {
        result: QueryResult,
        route: RouteIntent,
    },
    Write {
        record: CommitRecord,
        tag: String,
        route: RouteIntent,
    },
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
            PreparedQuery::Read { result, .. } => Ok(result),
            PreparedQuery::Write { record, tag, .. } => {
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
                RouteIntent::Root,
            ),
            Statement::Insert {
                table,
                columns: named_columns,
                values,
            } => {
                let table_state = self
                    .projection
                    .table(&table)
                    .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
                let columns = table_state.columns.clone();
                let now_microseconds = (self.next_tx + 1) as i64;
                let column_values = materialize_insert_values(
                    &table,
                    &columns,
                    &named_columns,
                    values,
                    now_microseconds,
                )?;

                let primary_key_index = table_state.primary_key_index().ok_or_else(|| {
                    OpenDbError::InvalidInput(format!("table {table} has no primary key"))
                })?;
                let primary_key_column = columns.get(primary_key_index).ok_or_else(|| {
                    OpenDbError::InvalidInput(format!("table {table} has no primary key"))
                })?;
                let primary_key_value = column_values
                    .get(primary_key_index)
                    .map(|v| &v.value)
                    .ok_or_else(|| OpenDbError::Sql("INSERT requires a primary key".to_owned()))?;
                if !value_matches_type(primary_key_value, &primary_key_column.data_type) {
                    return Err(OpenDbError::InvalidInput(format!(
                        "value for primary key column {} on table {} does not match {:?}",
                        primary_key_column.name, table, primary_key_column.data_type
                    )));
                }
                if primary_key_value.is_null() {
                    return Err(OpenDbError::InvalidInput(format!(
                        "primary key column {} on table {table} must not be NULL",
                        primary_key_column.name
                    )));
                }
                let row_key = value_to_key(primary_key_value);
                let route = RouteIntent::Key {
                    table: table.clone(),
                    key: route_key(&table, &row_key),
                };
                self.prepare_write(
                    vec![Mutation::InsertRow {
                        table,
                        key: row_key,
                        values: column_values,
                    }],
                    "INSERT 0 1",
                    route,
                )
            }
            Statement::SelectAll { table, predicate } => self
                .select_all(&table, predicate.as_ref())
                .map(|(result, route)| PreparedQuery::Read { result, route }),
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

    pub fn build_next_record(&self, mutations: Vec<Mutation>) -> CommitRecord {
        let next_tx = self.next_tx + 1;
        CommitRecord::new(TransactionId(next_tx), LogicalTimestamp(next_tx), mutations)
    }

    fn prepare_write(
        &self,
        mutations: Vec<Mutation>,
        tag: &str,
        route: RouteIntent,
    ) -> OpenDbResult<PreparedQuery> {
        let record = self.build_next_record(mutations);
        let mut validated_projection = self.projection.clone();
        validated_projection.apply(&record)?;
        Ok(PreparedQuery::Write {
            record,
            tag: tag.to_owned(),
            route,
        })
    }

    fn select_all(
        &self,
        table: &str,
        predicate: Option<&Predicate>,
    ) -> OpenDbResult<(QueryResult, RouteIntent)> {
        let table_state = self
            .projection
            .table(table)
            .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
        let column_names = table_state.column_names();
        let (rows, route) = match predicate {
            Some(predicate) => {
                let primary_key = table_state.primary_key_column().ok_or_else(|| {
                    OpenDbError::InvalidInput(format!("table {table} has no primary key"))
                })?;
                if predicate.column != primary_key.name {
                    return Err(OpenDbError::Sql(format!(
                        "SELECT WHERE only supports primary key equality on {}",
                        primary_key.name
                    )));
                }
                if !value_matches_type(&predicate.value, &primary_key.data_type) {
                    return Err(OpenDbError::Sql(format!(
                        "WHERE value for primary key column {} does not match {:?}",
                        primary_key.name, primary_key.data_type
                    )));
                }
                let row_key = value_to_key(&predicate.value);
                let route = RouteIntent::Key {
                    table: table.to_owned(),
                    key: route_key(table, &row_key),
                };
                let rows = match table_state.rows.get(&row_key) {
                    Some(row) if row.get(&primary_key.name) == Some(&predicate.value) => {
                        vec![project_row(table, &column_names, row)?]
                    }
                    Some(_) | None => Vec::new(),
                };
                (rows, route)
            }
            None => {
                let rows = table_state
                    .rows
                    .values()
                    .map(|row| project_row(table, &column_names, row))
                    .collect::<OpenDbResult<Vec<_>>>()?;
                (
                    rows,
                    RouteIntent::Scan {
                        table: table.to_owned(),
                    },
                )
            }
        };
        Ok((
            QueryResult::Rows {
                columns: column_names,
                column_types: Vec::new(),
                rows,
            },
            route,
        ))
    }
}

fn route_key(table: &str, row_key: &str) -> String {
    format!("{table}/{row_key}")
}

fn project_row(
    table: &str,
    column_names: &[String],
    row: &std::collections::BTreeMap<String, Value>,
) -> OpenDbResult<Vec<Value>> {
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
}

fn coerce_value(value: Value, target: &ColumnType) -> Value {
    match (value, target) {
        (Value::Int64(v), ColumnType::Float64) => Value::Float64(v as f64),
        (Value::Int64(v), ColumnType::Timestamp) => Value::Timestamp(v),
        (other, _) => other,
    }
}

fn materialize_insert_values(
    table: &str,
    schema: &[ColumnDefinition],
    named_columns: &Option<Vec<String>>,
    values: Vec<Value>,
    now_microseconds: i64,
) -> OpenDbResult<Vec<ColumnValue>> {
    match named_columns {
        None => {
            if values.len() != schema.len() {
                return Err(OpenDbError::Sql(format!(
                    "expected {} values, got {}",
                    schema.len(),
                    values.len()
                )));
            }
            let mut result = Vec::with_capacity(schema.len());
            for (column, value) in schema.iter().zip(values.into_iter()) {
                let value = coerce_value(value, &column.data_type);
                if value.is_null() && !column.nullable {
                    return Err(OpenDbError::InvalidInput(format!(
                        "column {} on table {table} is NOT NULL",
                        column.name
                    )));
                }
                if !value.is_null() && !value_matches_type(&value, &column.data_type) {
                    return Err(OpenDbError::InvalidInput(format!(
                        "value for column {} on table {table} does not match {:?}",
                        column.name, column.data_type
                    )));
                }
                result.push(ColumnValue {
                    column: column.name.clone(),
                    value,
                });
            }
            Ok(result)
        }
        Some(names) => {
            if names.len() != values.len() {
                return Err(OpenDbError::Sql(format!(
                    "INSERT column list ({} columns) does not match values ({} values)",
                    names.len(),
                    values.len()
                )));
            }
            let mut seen = std::collections::BTreeSet::new();
            for name in names {
                if !seen.insert(name.clone()) {
                    return Err(OpenDbError::Sql(format!(
                        "INSERT column {name} listed more than once"
                    )));
                }
                if !schema.iter().any(|column| &column.name == name) {
                    return Err(OpenDbError::Sql(format!(
                        "unknown column {name} on table {table}"
                    )));
                }
            }
            let provided: std::collections::BTreeMap<String, Value> =
                names.iter().cloned().zip(values).collect();

            let mut result = Vec::with_capacity(schema.len());
            for column in schema {
                let value = match provided.get(&column.name) {
                    Some(value) => {
                        let value = coerce_value(value.clone(), &column.data_type);
                        if value.is_null() {
                            if !column.nullable {
                                return Err(OpenDbError::InvalidInput(format!(
                                    "column {} on table {table} is NOT NULL",
                                    column.name
                                )));
                            }
                            value
                        } else {
                            if !value_matches_type(&value, &column.data_type) {
                                return Err(OpenDbError::InvalidInput(format!(
                                    "value for column {} on table {table} does not match {:?}",
                                    column.name, column.data_type
                                )));
                            }
                            value
                        }
                    }
                    None => default_value_for_column(table, column, now_microseconds)?,
                };
                result.push(ColumnValue {
                    column: column.name.clone(),
                    value,
                });
            }
            Ok(result)
        }
    }
}

fn default_value_for_column(
    table: &str,
    column: &ColumnDefinition,
    now_microseconds: i64,
) -> OpenDbResult<Value> {
    match &column.default {
        Some(DefaultExpr::Const(value)) => {
            if value.is_null() && !column.nullable {
                return Err(OpenDbError::InvalidInput(format!(
                    "column {} on table {table} is NOT NULL but DEFAULT NULL",
                    column.name
                )));
            }
            if !value.is_null() && !value_matches_type(value, &column.data_type) {
                return Err(OpenDbError::InvalidInput(format!(
                    "DEFAULT for column {} on table {table} does not match {:?}",
                    column.name, column.data_type
                )));
            }
            Ok(value.clone())
        }
        Some(DefaultExpr::Now) => {
            if !matches!(column.data_type, ColumnType::Timestamp) {
                return Err(OpenDbError::InvalidInput(format!(
                    "DEFAULT NOW() requires TIMESTAMP column on {}",
                    column.name
                )));
            }
            Ok(Value::Timestamp(now_microseconds))
        }
        None => {
            if column.nullable {
                Ok(Value::Null)
            } else {
                Err(OpenDbError::InvalidInput(format!(
                    "column {} on table {table} is NOT NULL and has no DEFAULT; INSERT must provide a value",
                    column.name
                )))
            }
        }
    }
}

fn value_to_key(value: &Value) -> String {
    match value {
        Value::Int64(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Timestamp(value) => value.to_string(),
        Value::Json(value) => value.to_string(),
        Value::Null => "null".to_string(),
    }
}

fn value_matches_type(value: &Value, data_type: &ColumnType) -> bool {
    matches!(
        (value, data_type),
        (Value::Int64(_), ColumnType::Int64)
            | (Value::Text(_), ColumnType::Text)
            | (Value::Bool(_), ColumnType::Bool)
            | (Value::Float64(_), ColumnType::Float64)
            | (Value::Timestamp(_), ColumnType::Timestamp)
            | (Value::Json(_), ColumnType::Json)
            | (Value::Null, _)
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
                column_types: vec![],
                rows: vec![vec![Value::Int64(1), Value::Text("Ada".to_owned())]],
            }
        );
        assert_eq!(engine.commits().len(), 2);
    }

    #[test]
    fn select_where_primary_key_returns_matching_row_only() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        engine
            .execute(parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("parse"))
            .expect("insert 1");
        engine
            .execute(parse("INSERT INTO accounts VALUES (2, 'Grace')").expect("parse"))
            .expect("insert 2");

        assert_eq!(
            engine
                .execute(parse("SELECT * FROM accounts WHERE id = 2").expect("parse"))
                .expect("select"),
            QueryResult::Rows {
                columns: vec!["id".to_owned(), "name".to_owned()],
                column_types: vec![],
                rows: vec![vec![Value::Int64(2), Value::Text("Grace".to_owned())]],
            }
        );
        assert_eq!(
            engine
                .execute(parse("SELECT * FROM accounts WHERE id = 3").expect("parse"))
                .expect("select missing"),
            QueryResult::Rows {
                columns: vec!["id".to_owned(), "name".to_owned()],
                column_types: vec![],
                rows: Vec::new(),
            }
        );
    }

    #[test]
    fn select_where_rejects_non_primary_key_predicates() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        engine
            .execute(parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("parse"))
            .expect("insert");

        let result =
            engine.execute(parse("SELECT * FROM accounts WHERE name = 'Ada'").expect("parse"));

        assert!(matches!(result, Err(OpenDbError::Sql(_))));
    }

    #[test]
    fn select_where_text_primary_key_supports_equals_inside_literal() {
        let mut engine = SqlEngine::default();
        engine
            .execute(
                parse("CREATE TABLE sessions (token TEXT PRIMARY KEY, name TEXT)").expect("parse"),
            )
            .expect("create");
        engine
            .execute(parse("INSERT INTO sessions VALUES ('a=b', 'Ada')").expect("parse"))
            .expect("insert");

        assert_eq!(
            engine
                .execute(parse("SELECT * FROM sessions WHERE token = 'a=b'").expect("parse"))
                .expect("select"),
            QueryResult::Rows {
                columns: vec!["token".to_owned(), "name".to_owned()],
                column_types: vec![],
                rows: vec![vec![
                    Value::Text("a=b".to_owned()),
                    Value::Text("Ada".to_owned())
                ]],
            }
        );
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
                column_types: vec![],
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
                column_types: vec![],
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
                column_types: vec![],
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

        let PreparedQuery::Write { record, tag, route } = prepared_create else {
            panic!("create should prepare as write");
        };
        assert_eq!(tag, "CREATE TABLE");
        assert_eq!(route, RouteIntent::Root);
        engine.apply_committed(record).expect("apply create");

        assert_eq!(
            engine
                .execute(parse("SELECT * FROM accounts").expect("parse"))
                .expect("select"),
            QueryResult::Rows {
                columns: vec!["id".to_owned(), "name".to_owned()],
                column_types: vec![],
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
                column_types: vec![],
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
    fn insert_prepare_returns_primary_key_route_intent() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");

        let prepared = engine
            .prepare(parse("INSERT INTO accounts VALUES (7, 'Ada')").expect("parse"))
            .expect("prepare insert");

        assert!(matches!(
            prepared,
            PreparedQuery::Write {
                route: RouteIntent::Key { ref table, ref key },
                ..
            } if table == "accounts" && key == "accounts/7"
        ));
    }

    #[test]
    fn select_where_prepare_returns_primary_key_route_intent() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");

        let prepared = engine
            .prepare(parse("SELECT * FROM accounts WHERE id = 7").expect("parse"))
            .expect("prepare select");

        assert!(matches!(
            prepared,
            PreparedQuery::Read {
                route: RouteIntent::Key { ref table, ref key },
                ..
            } if table == "accounts" && key == "accounts/7"
        ));
    }

    #[test]
    fn create_table_prepare_returns_root_route_intent() {
        let engine = SqlEngine::default();

        let prepared = engine
            .prepare(parse("CREATE TABLE accounts (id INT PRIMARY KEY)").expect("parse"))
            .expect("prepare create");

        assert!(matches!(
            prepared,
            PreparedQuery::Write {
                route: RouteIntent::Root,
                ..
            }
        ));
    }

    #[test]
    fn select_scan_prepare_returns_scan_route_intent() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");

        let prepared = engine
            .prepare(parse("SELECT * FROM accounts").expect("parse"))
            .expect("prepare scan");

        assert!(matches!(
            prepared,
            PreparedQuery::Read {
                route: RouteIntent::Scan { ref table },
                ..
            } if table == "accounts"
        ));
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

    #[test]
    fn named_insert_applies_const_default_for_missing_column() {
        let mut engine = SqlEngine::default();
        engine
            .execute(
                parse("CREATE TABLE t (id INT PRIMARY KEY, status TEXT DEFAULT 'completed')")
                    .expect("parse"),
            )
            .expect("create");
        engine
            .execute(parse("INSERT INTO t (id) VALUES (1)").expect("parse"))
            .expect("insert");

        let result = engine
            .execute(parse("SELECT * FROM t").expect("parse"))
            .expect("select");
        assert_eq!(
            result,
            QueryResult::Rows {
                columns: vec!["id".to_owned(), "status".to_owned()],
                column_types: vec![],
                rows: vec![vec![Value::Int64(1), Value::Text("completed".to_owned())]],
            }
        );
    }

    #[test]
    fn named_insert_applies_default_now_as_logical_timestamp() {
        let mut engine = SqlEngine::default();
        engine
            .execute(
                parse(
                    "CREATE TABLE t (id INT PRIMARY KEY, created_at TIMESTAMP NOT NULL DEFAULT NOW())",
                )
                .expect("parse"),
            )
            .expect("create");
        engine
            .execute(parse("INSERT INTO t (id) VALUES (1)").expect("parse"))
            .expect("insert");

        let last = engine.commits().last().expect("commit");
        let Mutation::InsertRow { values, .. } = &last.mutations[0] else {
            panic!("expected InsertRow");
        };
        let created_at = values
            .iter()
            .find(|cv| cv.column == "created_at")
            .expect("created_at column");
        assert_eq!(created_at.value, Value::Timestamp(last.tx_id.0 as i64));
    }

    #[test]
    fn named_insert_rejects_missing_not_null_column_without_default() {
        let mut engine = SqlEngine::default();
        engine
            .execute(
                parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)").expect("parse"),
            )
            .expect("create");

        let result = engine.execute(parse("INSERT INTO t (id) VALUES (1)").expect("parse"));
        assert!(matches!(result, Err(OpenDbError::InvalidInput(_))));
    }

    #[test]
    fn named_insert_rejects_unknown_column() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY)").expect("parse"))
            .expect("create");

        let result =
            engine.execute(parse("INSERT INTO t (id, surprise) VALUES (1, 'x')").expect("parse"));
        assert!(matches!(result, Err(OpenDbError::Sql(_))));
    }

    #[test]
    fn positional_insert_rejects_null_into_not_null_column() {
        let mut engine = SqlEngine::default();
        engine
            .execute(
                parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)").expect("parse"),
            )
            .expect("create");

        let result = engine.execute(parse("INSERT INTO t VALUES (1, NULL)").expect("parse"));
        assert!(matches!(result, Err(OpenDbError::InvalidInput(_))));
    }

    #[test]
    fn named_insert_supports_bool_float_timestamp_values() {
        let mut engine = SqlEngine::default();
        engine
            .execute(
                parse("CREATE TABLE t (id INT PRIMARY KEY, done BOOL, ratio FLOAT8, at TIMESTAMP)")
                    .expect("parse"),
            )
            .expect("create");
        engine
            .execute(
                parse("INSERT INTO t (id, done, ratio, at) VALUES (1, TRUE, 0.5, 42)")
                    .expect("parse"),
            )
            .expect("insert");

        let last = engine.commits().last().expect("commit");
        let Mutation::InsertRow { values, .. } = &last.mutations[0] else {
            panic!("expected InsertRow");
        };
        let mut by_name = std::collections::BTreeMap::new();
        for cv in values {
            by_name.insert(cv.column.clone(), cv.value.clone());
        }
        assert_eq!(by_name["done"], Value::Bool(true));
        assert_eq!(by_name["ratio"], Value::Float64(0.5));
        assert_eq!(by_name["at"], Value::Timestamp(42));
    }
}
