use crate::commit_stream::{CommitRecord, Mutation, Value};
use opendb_common::{OpenDbError, OpenDbResult};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Table {
    pub columns: Vec<String>,
    pub rows: BTreeMap<String, BTreeMap<String, Value>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RowProjection {
    tables: BTreeMap<String, Table>,
}

impl RowProjection {
    pub fn apply(&mut self, record: &CommitRecord) -> OpenDbResult<()> {
        let mut next = self.clone();
        next.apply_inner(record)?;
        *self = next;
        Ok(())
    }

    fn apply_inner(&mut self, record: &CommitRecord) -> OpenDbResult<()> {
        for mutation in &record.mutations {
            match mutation {
                Mutation::CreateTable { table, columns } => {
                    if self.tables.contains_key(table) {
                        return Err(OpenDbError::InvalidInput(format!(
                            "table already exists: {table}"
                        )));
                    }
                    let mut seen_columns = BTreeMap::new();
                    for column in columns {
                        if seen_columns.insert(column, ()).is_some() {
                            return Err(OpenDbError::InvalidInput(format!(
                                "duplicate column {column} on table {table}"
                            )));
                        }
                    }
                    self.tables.insert(
                        table.clone(),
                        Table {
                            columns: columns.clone(),
                            rows: BTreeMap::new(),
                        },
                    );
                }
                Mutation::InsertRow { table, key, values } => {
                    let table_state = self.tables.get_mut(table).ok_or_else(|| {
                        OpenDbError::NotFound(format!("table not found: {table}"))
                    })?;
                    if table_state.rows.contains_key(key) {
                        return Err(OpenDbError::InvalidInput(format!(
                            "row already exists: {table}/{key}"
                        )));
                    }
                    if values.len() != table_state.columns.len() {
                        return Err(OpenDbError::InvalidInput(format!(
                            "column set does not match table {table}"
                        )));
                    }
                    let mut row = BTreeMap::new();
                    for column_value in values {
                        if !table_state.columns.contains(&column_value.column) {
                            return Err(OpenDbError::InvalidInput(format!(
                                "unknown column {} on table {}",
                                column_value.column, table
                            )));
                        }
                        if row
                            .insert(column_value.column.clone(), column_value.value.clone())
                            .is_some()
                        {
                            return Err(OpenDbError::InvalidInput(format!(
                                "duplicate column {} on table {}",
                                column_value.column, table
                            )));
                        }
                    }
                    for column in &table_state.columns {
                        if !row.contains_key(column) {
                            return Err(OpenDbError::InvalidInput(format!(
                                "missing column {column} on table {table}"
                            )));
                        }
                    }
                    table_state.rows.insert(key.clone(), row);
                }
            }
        }
        Ok(())
    }

    pub fn rebuild(records: &[CommitRecord]) -> OpenDbResult<Self> {
        let mut projection = Self::default();
        for record in records {
            projection.apply(record)?;
        }
        Ok(projection)
    }

    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_stream::{ColumnValue, CommitRecord, Mutation, Value};
    use opendb_common::{LogicalTimestamp, TransactionId};

    fn create_accounts_record(tx_id: u64) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::CreateTable {
                table: "accounts".to_owned(),
                columns: vec!["id".to_owned(), "name".to_owned()],
            }],
        )
    }

    fn insert_account_record(tx_id: u64, key: &str) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::InsertRow {
                table: "accounts".to_owned(),
                key: key.to_owned(),
                values: vec![
                    ColumnValue {
                        column: "id".to_owned(),
                        value: Value::Int64(key.parse().expect("integer key")),
                    },
                    ColumnValue {
                        column: "name".to_owned(),
                        value: Value::Text("Ada".to_owned()),
                    },
                ],
            }],
        )
    }

    fn rebuilt_accounts_projection() -> RowProjection {
        RowProjection::rebuild(&[create_accounts_record(1)]).expect("rebuild")
    }

    #[test]
    fn row_projection_rebuilds_from_commit_stream() {
        let records = vec![
            CommitRecord::new(
                TransactionId(1),
                LogicalTimestamp(1),
                vec![Mutation::CreateTable {
                    table: "accounts".to_owned(),
                    columns: vec!["id".to_owned(), "name".to_owned()],
                }],
            ),
            CommitRecord::new(
                TransactionId(2),
                LogicalTimestamp(2),
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
            ),
        ];

        let projection = RowProjection::rebuild(&records).expect("rebuild");
        let accounts = projection.table("accounts").expect("accounts table");
        assert_eq!(accounts.rows.len(), 1);
        assert_eq!(
            accounts.rows.get("1").and_then(|row| row.get("name")),
            Some(&Value::Text("Ada".to_owned()))
        );
    }

    #[test]
    fn duplicate_table_errors() {
        let mut projection = rebuilt_accounts_projection();

        assert!(projection.apply(&create_accounts_record(2)).is_err());
    }

    #[test]
    fn unknown_table_insert_errors() {
        let mut projection = RowProjection::default();

        assert!(projection.apply(&insert_account_record(1, "1")).is_err());
    }

    #[test]
    fn unknown_column_insert_errors() {
        let mut projection = rebuilt_accounts_projection();
        let record = CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(2),
            vec![Mutation::InsertRow {
                table: "accounts".to_owned(),
                key: "1".to_owned(),
                values: vec![
                    ColumnValue {
                        column: "id".to_owned(),
                        value: Value::Int64(1),
                    },
                    ColumnValue {
                        column: "email".to_owned(),
                        value: Value::Text("ada@example.test".to_owned()),
                    },
                ],
            }],
        );

        assert!(projection.apply(&record).is_err());
    }

    #[test]
    fn duplicate_row_key_errors() {
        let mut projection = rebuilt_accounts_projection();
        projection
            .apply(&insert_account_record(2, "1"))
            .expect("first insert");

        assert!(projection.apply(&insert_account_record(3, "1")).is_err());
    }

    #[test]
    fn omitted_column_errors() {
        let mut projection = rebuilt_accounts_projection();
        let record = CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(2),
            vec![Mutation::InsertRow {
                table: "accounts".to_owned(),
                key: "1".to_owned(),
                values: vec![ColumnValue {
                    column: "id".to_owned(),
                    value: Value::Int64(1),
                }],
            }],
        );

        assert!(projection.apply(&record).is_err());
    }

    #[test]
    fn duplicate_column_value_errors() {
        let mut projection = rebuilt_accounts_projection();
        let record = CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(2),
            vec![Mutation::InsertRow {
                table: "accounts".to_owned(),
                key: "1".to_owned(),
                values: vec![
                    ColumnValue {
                        column: "id".to_owned(),
                        value: Value::Int64(1),
                    },
                    ColumnValue {
                        column: "id".to_owned(),
                        value: Value::Int64(1),
                    },
                ],
            }],
        );

        assert!(projection.apply(&record).is_err());
    }

    #[test]
    fn duplicate_create_table_column_errors() {
        let record = CommitRecord::new(
            TransactionId(1),
            LogicalTimestamp(1),
            vec![Mutation::CreateTable {
                table: "accounts".to_owned(),
                columns: vec!["id".to_owned(), "id".to_owned()],
            }],
        );

        assert!(RowProjection::rebuild(&[record]).is_err());
    }

    #[test]
    fn multi_mutation_record_failure_leaves_projection_unchanged() {
        let mut projection = rebuilt_accounts_projection();
        let before = projection.clone();
        let record = CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(2),
            vec![
                Mutation::CreateTable {
                    table: "orders".to_owned(),
                    columns: vec!["id".to_owned()],
                },
                Mutation::InsertRow {
                    table: "accounts".to_owned(),
                    key: "1".to_owned(),
                    values: vec![ColumnValue {
                        column: "id".to_owned(),
                        value: Value::Int64(1),
                    }],
                },
            ],
        );

        assert!(projection.apply(&record).is_err());
        assert_eq!(projection, before);
        assert!(projection.table("orders").is_none());
    }
}
