use crate::commit_stream::{ColumnDefinition, ColumnType, CommitRecord, Mutation, Value};
use opendb_common::{OpenDbError, OpenDbResult};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Table {
    pub columns: Vec<ColumnDefinition>,
    pub rows: BTreeMap<String, BTreeMap<String, Value>>,
}

impl Table {
    pub fn column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    }

    pub fn primary_key_index(&self) -> Option<usize> {
        self.columns.iter().position(|column| column.primary_key)
    }

    pub fn primary_key_column(&self) -> Option<&ColumnDefinition> {
        self.primary_key_index()
            .and_then(|index| self.columns.get(index))
    }
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
                    if columns.is_empty() {
                        return Err(OpenDbError::InvalidInput(format!(
                            "table {table} requires at least one column"
                        )));
                    }
                    let mut seen_columns = BTreeMap::new();
                    let mut primary_key_count = 0_usize;
                    for column in columns {
                        if column.name.trim().is_empty() {
                            return Err(OpenDbError::InvalidInput(format!(
                                "table {table} has an empty column name"
                            )));
                        }
                        if seen_columns.insert(&column.name, ()).is_some() {
                            return Err(OpenDbError::InvalidInput(format!(
                                "duplicate column {} on table {table}",
                                column.name
                            )));
                        }
                        if column.primary_key {
                            primary_key_count += 1;
                        }
                    }
                    if primary_key_count != 1 {
                        return Err(OpenDbError::InvalidInput(format!(
                            "table {table} requires exactly one primary key column"
                        )));
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
                        let column_definition = table_state
                            .columns
                            .iter()
                            .find(|column| column.name == column_value.column)
                            .ok_or_else(|| {
                                OpenDbError::InvalidInput(format!(
                                    "unknown column {} on table {}",
                                    column_value.column, table
                                ))
                            })?;
                        if !value_matches_type(&column_value.value, &column_definition.data_type) {
                            return Err(OpenDbError::InvalidInput(format!(
                                "value for column {} on table {} does not match {:?}",
                                column_value.column, table, column_definition.data_type
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
                        if !row.contains_key(&column.name) {
                            return Err(OpenDbError::InvalidInput(format!(
                                "missing column {} on table {table}",
                                column.name
                            )));
                        }
                    }
                    let primary_key = table_state.primary_key_column().ok_or_else(|| {
                        OpenDbError::InvalidInput(format!("table {table} has no primary key"))
                    })?;
                    let projected_key = row.get(&primary_key.name).ok_or_else(|| {
                        OpenDbError::InvalidInput(format!(
                            "missing primary key column {} on table {table}",
                            primary_key.name
                        ))
                    })?;
                    if value_to_key(projected_key) != *key {
                        return Err(OpenDbError::InvalidInput(format!(
                            "row key {key} does not match primary key column {} on table {table}",
                            primary_key.name
                        )));
                    }
                    table_state.rows.insert(key.clone(), row);
                }
                Mutation::PutRangeDescriptor { .. }
                | Mutation::SplitRange { .. }
                | Mutation::MergeRanges { .. }
                | Mutation::PutArchiveObjectPointer { .. }
                | Mutation::PutRecoveryArtifactPointer { .. } => {}
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

fn value_matches_type(value: &Value, data_type: &ColumnType) -> bool {
    matches!(
        (value, data_type),
        (Value::Int64(_), ColumnType::Int64) | (Value::Text(_), ColumnType::Text)
    )
}

fn value_to_key(value: &Value) -> String {
    match value {
        Value::Int64(value) => value.to_string(),
        Value::Text(value) => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive_manifest::{
        ArchiveBackendKind, ArchiveObjectPointer, CompressionKind, RecoveryArtifactKind,
        RecoveryArtifactPointer,
    };
    use crate::commit_stream::{
        ColumnDefinition, ColumnType, ColumnValue, CommitRecord, Mutation, RangeMerge, RangeSplit,
        Value,
    };
    use crate::range_catalog::RangeDescriptor;
    use opendb_common::{LogicalTimestamp, RangeId, TransactionId};

    fn create_accounts_record(tx_id: u64) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::CreateTable {
                table: "accounts".to_owned(),
                columns: accounts_columns(),
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

    fn accounts_columns() -> Vec<ColumnDefinition> {
        vec![
            ColumnDefinition::primary_key("id", ColumnType::Int64),
            ColumnDefinition::new("name", ColumnType::Text),
        ]
    }

    fn id_only_columns() -> Vec<ColumnDefinition> {
        vec![ColumnDefinition::primary_key("id", ColumnType::Int64)]
    }

    fn recovery_artifact_record(tx_id: u64) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::PutRecoveryArtifactPointer {
                artifact: RecoveryArtifactPointer {
                    artifact_kind: RecoveryArtifactKind::WalSegment,
                    range_id: RangeId::ROOT,
                    object: ArchiveObjectPointer {
                        backend: ArchiveBackendKind::S3Compatible,
                        bucket: "opendb-archives".to_owned(),
                        key: "root-range/00000005.wal".to_owned(),
                        content_sha256: "not-validated-by-row-projection".to_owned(),
                    },
                    format_version: 0,
                    tx_id_start: TransactionId(0),
                    tx_id_end: TransactionId(10),
                    ts_start: LogicalTimestamp(0),
                    ts_end: LogicalTimestamp(10),
                    record_count: 0,
                    byte_len: 0,
                    compression: CompressionKind::None,
                },
            }],
        )
    }

    fn split_range_record(tx_id: u64) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::SplitRange {
                split: RangeSplit {
                    source_range_id: RangeId::ROOT,
                    split_key: "orders/".to_owned(),
                    left: RangeDescriptor {
                        range_id: RangeId(2),
                        parent_range_id: Some(RangeId::ROOT),
                        key_start: None,
                        key_end: Some("orders/".to_owned()),
                        replica_node_ids: vec![0],
                    },
                    right: RangeDescriptor {
                        range_id: RangeId(3),
                        parent_range_id: Some(RangeId::ROOT),
                        key_start: Some("orders/".to_owned()),
                        key_end: None,
                        replica_node_ids: vec![0],
                    },
                },
            }],
        )
    }

    fn merge_ranges_record(tx_id: u64) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::MergeRanges {
                merge: RangeMerge {
                    source_range_ids: vec![RangeId(2), RangeId(3)],
                    merged: RangeDescriptor {
                        range_id: RangeId(4),
                        parent_range_id: Some(RangeId::ROOT),
                        key_start: None,
                        key_end: None,
                        replica_node_ids: vec![0],
                    },
                },
            }],
        )
    }

    #[test]
    fn row_projection_rebuilds_from_commit_stream() {
        let records = vec![
            CommitRecord::new(
                TransactionId(1),
                LogicalTimestamp(1),
                vec![Mutation::CreateTable {
                    table: "accounts".to_owned(),
                    columns: accounts_columns(),
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
                columns: vec![
                    ColumnDefinition::primary_key("id", ColumnType::Int64),
                    ColumnDefinition::new("id", ColumnType::Text),
                ],
            }],
        );

        assert!(RowProjection::rebuild(&[record]).is_err());
    }

    #[test]
    fn create_table_requires_exactly_one_primary_key() {
        let missing_key = CommitRecord::new(
            TransactionId(1),
            LogicalTimestamp(1),
            vec![Mutation::CreateTable {
                table: "accounts".to_owned(),
                columns: vec![
                    ColumnDefinition::new("id", ColumnType::Int64),
                    ColumnDefinition::new("name", ColumnType::Text),
                ],
            }],
        );
        let duplicate_key = CommitRecord::new(
            TransactionId(1),
            LogicalTimestamp(1),
            vec![Mutation::CreateTable {
                table: "accounts".to_owned(),
                columns: vec![
                    ColumnDefinition::primary_key("id", ColumnType::Int64),
                    ColumnDefinition::primary_key("name", ColumnType::Text),
                ],
            }],
        );

        assert!(RowProjection::rebuild(&[missing_key]).is_err());
        assert!(RowProjection::rebuild(&[duplicate_key]).is_err());
    }

    #[test]
    fn insert_row_values_must_match_schema_types() {
        let mut projection = rebuilt_accounts_projection();
        let invalid_id = CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(2),
            vec![Mutation::InsertRow {
                table: "accounts".to_owned(),
                key: "one".to_owned(),
                values: vec![
                    ColumnValue {
                        column: "id".to_owned(),
                        value: Value::Text("one".to_owned()),
                    },
                    ColumnValue {
                        column: "name".to_owned(),
                        value: Value::Text("Ada".to_owned()),
                    },
                ],
            }],
        );

        assert!(projection.apply(&invalid_id).is_err());
    }

    #[test]
    fn insert_row_key_must_match_primary_key_value() {
        let mut projection = rebuilt_accounts_projection();
        let mismatched_key = CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(2),
            vec![Mutation::InsertRow {
                table: "accounts".to_owned(),
                key: "2".to_owned(),
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
        );

        assert!(projection.apply(&mismatched_key).is_err());
        assert!(
            projection
                .table("accounts")
                .expect("accounts")
                .rows
                .is_empty()
        );
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
                    columns: id_only_columns(),
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

    #[test]
    fn row_projection_ignores_recovery_artifact_metadata() {
        let projection = RowProjection::rebuild(&[
            create_accounts_record(1),
            recovery_artifact_record(2),
            insert_account_record(3, "1"),
        ])
        .expect("rebuild projection");
        let accounts = projection.table("accounts").expect("accounts table");

        assert_eq!(accounts.rows.len(), 1);
        assert_eq!(
            accounts.rows.get("1").and_then(|row| row.get("name")),
            Some(&Value::Text("Ada".to_owned()))
        );
    }

    #[test]
    fn row_projection_ignores_range_split_merge_metadata() {
        let projection = RowProjection::rebuild(&[
            create_accounts_record(1),
            split_range_record(2),
            merge_ranges_record(3),
            insert_account_record(4, "1"),
        ])
        .expect("rebuild projection");
        let accounts = projection.table("accounts").expect("accounts table");

        assert_eq!(accounts.rows.len(), 1);
        assert_eq!(
            accounts.rows.get("1").and_then(|row| row.get("name")),
            Some(&Value::Text("Ada".to_owned()))
        );
    }
}
