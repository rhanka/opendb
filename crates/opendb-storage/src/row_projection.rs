use crate::commit_stream::{
    AlterTableOp, ColumnDefinition, ColumnType, CommitRecord, ConstraintKind, IndexDescriptor,
    Mutation, NamedConstraint, ReferentialAction, Value,
};

#[derive(Clone, Debug)]
struct FkDependent {
    table: String,
    key: String,
    columns: Vec<String>,
    constraint_name: String,
    action: ReferentialAction,
}
use opendb_common::{OpenDbError, OpenDbResult};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Table {
    pub columns: Vec<ColumnDefinition>,
    pub rows: BTreeMap<String, BTreeMap<String, Value>>,
    pub constraints: Vec<NamedConstraint>,
    pub indexes: Vec<IndexDescriptor>,
}

impl Table {
    pub fn column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    }

    pub fn column_types(&self) -> Vec<ColumnType> {
        self.columns
            .iter()
            .map(|column| column.data_type.clone())
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

#[derive(Clone, Debug, Default, PartialEq)]
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
                            constraints: Vec::new(),
                            indexes: Vec::new(),
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
                    // Enforce UNIQUE / FK constraints before insertion.
                    {
                        let table_ref = self.tables.get(table).ok_or_else(|| {
                            OpenDbError::NotFound(format!("table not found: {table}"))
                        })?;
                        self.enforce_unique_constraints(table_ref, key, &row)?;
                        self.enforce_fk_constraints(table_ref, &row)?;
                    }
                    let table_state = self.tables.get_mut(table).expect("table existed above");
                    table_state.rows.insert(key.clone(), row);
                }
                Mutation::PutRangeDescriptor { .. }
                | Mutation::SplitRange { .. }
                | Mutation::MergeRanges { .. }
                | Mutation::PutArchiveObjectPointer { .. }
                | Mutation::PutRecoveryArtifactPointer { .. } => {}
                Mutation::AlterTable { table, op } => {
                    self.apply_alter_table(table, op)?;
                }
                Mutation::DeleteRow { table, key } => {
                    self.apply_delete_row(table, key)?;
                }
                Mutation::UpdateRow {
                    table,
                    key,
                    assignments,
                } => {
                    self.apply_update_row(table, key, assignments)?;
                }
            }
        }
        Ok(())
    }

    fn apply_update_row(
        &mut self,
        table: &str,
        key: &str,
        assignments: &[crate::commit_stream::ColumnValue],
    ) -> OpenDbResult<()> {
        let updated_row = {
            let table_state = self
                .tables
                .get(table)
                .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
            let existing =
                table_state.rows.get(key).cloned().ok_or_else(|| {
                    OpenDbError::NotFound(format!("row not found: {table}/{key}"))
                })?;
            let mut next = existing;
            let primary_key_name = table_state.primary_key_column().map(|c| c.name.clone());
            for assignment in assignments {
                let column = table_state
                    .columns
                    .iter()
                    .find(|c| c.name == assignment.column)
                    .ok_or_else(|| {
                        OpenDbError::InvalidInput(format!(
                            "unknown column {} on table {table}",
                            assignment.column
                        ))
                    })?;
                if Some(&column.name) == primary_key_name.as_ref() {
                    return Err(OpenDbError::InvalidInput(format!(
                        "UPDATE cannot change primary key column {}",
                        column.name
                    )));
                }
                if !column.nullable && matches!(assignment.value, Value::Null) {
                    return Err(OpenDbError::InvalidInput(format!(
                        "column {} on table {table} is NOT NULL",
                        column.name
                    )));
                }
                if !matches!(assignment.value, Value::Null)
                    && !value_matches_type(&assignment.value, &column.data_type)
                {
                    return Err(OpenDbError::InvalidInput(format!(
                        "value for column {} on table {table} does not match {:?}",
                        column.name, column.data_type
                    )));
                }
                next.insert(column.name.clone(), assignment.value.clone());
            }
            next
        };
        // Re-run UNIQUE / FK enforcement on the candidate row.
        let table_ref = self
            .tables
            .get(table)
            .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
        self.enforce_unique_constraints(table_ref, key, &updated_row)?;
        self.enforce_fk_constraints(table_ref, &updated_row)?;
        let table_state = self.tables.get_mut(table).expect("table existed above");
        table_state.rows.insert(key.to_owned(), updated_row);
        Ok(())
    }

    fn apply_delete_row(&mut self, table: &str, key: &str) -> OpenDbResult<()> {
        let target_row = {
            let table_state = self
                .tables
                .get(table)
                .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
            table_state
                .rows
                .get(key)
                .cloned()
                .ok_or_else(|| OpenDbError::NotFound(format!("row not found: {table}/{key}")))?
        };
        // Walk every other table looking for FKs that point at this table.
        // Sprint 9 restricts cascades to one hop and references that map onto
        // the parent's primary key column.
        let dependents = self.collect_fk_dependents(table, &target_row)?;
        for dependent in dependents {
            match dependent.action {
                ReferentialAction::Cascade => {
                    self.apply_delete_row(&dependent.table, &dependent.key)?;
                }
                ReferentialAction::NoAction | ReferentialAction::Restrict => {
                    return Err(OpenDbError::InvalidInput(format!(
                        "DELETE on {table}/{key} violates FK {} on table {}",
                        dependent.constraint_name, dependent.table
                    )));
                }
                ReferentialAction::SetNull => {
                    self.apply_set_null(&dependent)?;
                }
                ReferentialAction::SetDefault => {
                    self.apply_set_default(&dependent)?;
                }
            }
        }
        let table_state = self
            .tables
            .get_mut(table)
            .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
        table_state.rows.remove(key);
        Ok(())
    }

    fn collect_fk_dependents(
        &self,
        parent_table: &str,
        parent_row: &BTreeMap<String, Value>,
    ) -> OpenDbResult<Vec<FkDependent>> {
        let mut dependents = Vec::new();
        for (child_table_name, child_table) in &self.tables {
            for constraint in &child_table.constraints {
                let ConstraintKind::ForeignKey {
                    columns,
                    references_table,
                    references_columns,
                    on_delete,
                    ..
                } = &constraint.kind
                else {
                    continue;
                };
                if references_table != parent_table {
                    continue;
                }
                if columns.len() != references_columns.len() {
                    return Err(OpenDbError::InvalidInput(format!(
                        "FK {} on {child_table_name} has unbalanced column count",
                        constraint.name
                    )));
                }
                let parent_values: Vec<&Value> = references_columns
                    .iter()
                    .map(|column| parent_row.get(column).unwrap_or(&Value::Null))
                    .collect();
                for (child_key, child_row) in &child_table.rows {
                    let matches = columns.iter().enumerate().all(|(i, column)| {
                        let parent_value = parent_values[i];
                        let child_value = child_row.get(column).unwrap_or(&Value::Null);
                        child_value == parent_value
                    });
                    if matches {
                        dependents.push(FkDependent {
                            table: child_table_name.clone(),
                            key: child_key.clone(),
                            columns: columns.clone(),
                            constraint_name: constraint.name.clone(),
                            action: *on_delete,
                        });
                    }
                }
            }
        }
        Ok(dependents)
    }

    fn apply_set_null(&mut self, dependent: &FkDependent) -> OpenDbResult<()> {
        let table_state = self.tables.get_mut(&dependent.table).ok_or_else(|| {
            OpenDbError::NotFound(format!("table not found: {}", dependent.table))
        })?;
        for column_name in &dependent.columns {
            let column_def = table_state
                .columns
                .iter()
                .find(|c| &c.name == column_name)
                .ok_or_else(|| {
                    OpenDbError::InvalidInput(format!(
                        "FK column {} missing on {}",
                        column_name, dependent.table
                    ))
                })?;
            if !column_def.nullable {
                return Err(OpenDbError::InvalidInput(format!(
                    "SET NULL on {} violates NOT NULL on {}",
                    dependent.table, column_name
                )));
            }
        }
        if let Some(row) = table_state.rows.get_mut(&dependent.key) {
            for column_name in &dependent.columns {
                row.insert(column_name.clone(), Value::Null);
            }
        }
        Ok(())
    }

    fn apply_set_default(&mut self, dependent: &FkDependent) -> OpenDbResult<()> {
        let table_state = self.tables.get_mut(&dependent.table).ok_or_else(|| {
            OpenDbError::NotFound(format!("table not found: {}", dependent.table))
        })?;
        let mut updates: Vec<(String, Value)> = Vec::new();
        for column_name in &dependent.columns {
            let column_def = table_state
                .columns
                .iter()
                .find(|c| &c.name == column_name)
                .ok_or_else(|| {
                    OpenDbError::InvalidInput(format!(
                        "FK column {} missing on {}",
                        column_name, dependent.table
                    ))
                })?;
            let value = match &column_def.default {
                Some(crate::commit_stream::DefaultExpr::Const(value)) => value.clone(),
                _ if column_def.nullable => Value::Null,
                _ => {
                    return Err(OpenDbError::InvalidInput(format!(
                        "SET DEFAULT on {} has no DEFAULT for column {}",
                        dependent.table, column_name
                    )));
                }
            };
            updates.push((column_name.clone(), value));
        }
        if let Some(row) = table_state.rows.get_mut(&dependent.key) {
            for (column, value) in updates {
                row.insert(column, value);
            }
        }
        Ok(())
    }

    fn enforce_unique_constraints(
        &self,
        table_state: &Table,
        key: &str,
        row: &BTreeMap<String, Value>,
    ) -> OpenDbResult<()> {
        for constraint in &table_state.constraints {
            let ConstraintKind::Unique { columns } = &constraint.kind else {
                continue;
            };
            let candidate: Vec<&Value> = columns
                .iter()
                .map(|column| row.get(column).unwrap_or(&Value::Null))
                .collect();
            if candidate.iter().all(|value| matches!(value, Value::Null)) {
                continue;
            }
            for (existing_key, existing_row) in &table_state.rows {
                if existing_key == key {
                    continue;
                }
                let existing: Vec<&Value> = columns
                    .iter()
                    .map(|column| existing_row.get(column).unwrap_or(&Value::Null))
                    .collect();
                if candidate == existing {
                    return Err(OpenDbError::InvalidInput(format!(
                        "UNIQUE constraint {} violated on table",
                        constraint.name
                    )));
                }
            }
        }
        Ok(())
    }

    fn enforce_fk_constraints(
        &self,
        table_state: &Table,
        row: &BTreeMap<String, Value>,
    ) -> OpenDbResult<()> {
        for constraint in &table_state.constraints {
            let ConstraintKind::ForeignKey {
                columns,
                references_table,
                references_columns,
                ..
            } = &constraint.kind
            else {
                continue;
            };
            let child_values: Vec<&Value> = columns
                .iter()
                .map(|column| row.get(column).unwrap_or(&Value::Null))
                .collect();
            if child_values
                .iter()
                .any(|value| matches!(value, Value::Null))
            {
                continue;
            }
            let Some(parent_table) = self.tables.get(references_table) else {
                return Err(OpenDbError::InvalidInput(format!(
                    "FK {} on table references unknown table {references_table}",
                    constraint.name
                )));
            };
            let exists = parent_table.rows.values().any(|parent_row| {
                references_columns.iter().enumerate().all(|(i, column)| {
                    let parent_value = parent_row.get(column).unwrap_or(&Value::Null);
                    child_values[i] == parent_value
                })
            });
            if !exists {
                return Err(OpenDbError::InvalidInput(format!(
                    "FK {} on table not satisfied: parent row missing in {references_table}",
                    constraint.name
                )));
            }
        }
        Ok(())
    }

    fn apply_alter_table(&mut self, table: &str, op: &AlterTableOp) -> OpenDbResult<()> {
        let table_state = self
            .tables
            .get_mut(table)
            .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
        match op {
            AlterTableOp::AddColumn(column) => {
                if column.name.trim().is_empty() {
                    return Err(OpenDbError::InvalidInput(format!(
                        "ALTER TABLE {table} ADD COLUMN requires a name"
                    )));
                }
                if table_state
                    .columns
                    .iter()
                    .any(|existing| existing.name == column.name)
                {
                    return Err(OpenDbError::InvalidInput(format!(
                        "column {} already exists on table {table}",
                        column.name
                    )));
                }
                if column.primary_key {
                    return Err(OpenDbError::InvalidInput(format!(
                        "ALTER TABLE {table} cannot add a new primary key column"
                    )));
                }
                let backfill = match &column.default {
                    Some(crate::commit_stream::DefaultExpr::Const(value)) => value.clone(),
                    _ if column.nullable => Value::Null,
                    _ => {
                        return Err(OpenDbError::InvalidInput(format!(
                            "ALTER TABLE {table} ADD COLUMN {} requires DEFAULT or NULL allowance",
                            column.name
                        )));
                    }
                };
                table_state.columns.push(column.clone());
                for row in table_state.rows.values_mut() {
                    row.entry(column.name.clone())
                        .or_insert_with(|| backfill.clone());
                }
            }
            AlterTableOp::DropColumn { column } => {
                let position = table_state
                    .columns
                    .iter()
                    .position(|c| &c.name == column)
                    .ok_or_else(|| {
                        OpenDbError::NotFound(format!("column {column} not found on table {table}"))
                    })?;
                if table_state
                    .columns
                    .get(position)
                    .is_some_and(|c| c.primary_key)
                {
                    return Err(OpenDbError::InvalidInput(format!(
                        "cannot drop primary key column {column} on table {table}"
                    )));
                }
                table_state.columns.remove(position);
                for row in table_state.rows.values_mut() {
                    row.remove(column);
                }
            }
            AlterTableOp::RenameColumn { from, to } => {
                if table_state.columns.iter().any(|c| &c.name == to) {
                    return Err(OpenDbError::InvalidInput(format!(
                        "rename target {to} already exists on table {table}"
                    )));
                }
                let column = table_state
                    .columns
                    .iter_mut()
                    .find(|c| &c.name == from)
                    .ok_or_else(|| {
                        OpenDbError::NotFound(format!("column {from} not found on table {table}"))
                    })?;
                column.name = to.clone();
                for row in table_state.rows.values_mut() {
                    if let Some(value) = row.remove(from) {
                        row.insert(to.clone(), value);
                    }
                }
            }
            AlterTableOp::AddConstraint(constraint) => {
                if table_state
                    .constraints
                    .iter()
                    .any(|existing| existing.name == constraint.name)
                {
                    return Err(OpenDbError::InvalidInput(format!(
                        "constraint {} already exists on table {table}",
                        constraint.name
                    )));
                }
                table_state.constraints.push(constraint.clone());
            }
            AlterTableOp::AddIndex(index) => {
                if let Some(existing) = table_state.indexes.iter().find(|i| i.name == index.name) {
                    if index.if_not_exists {
                        return Ok(());
                    }
                    return Err(OpenDbError::InvalidInput(format!(
                        "index {} already exists on table {table} (current columns: {:?})",
                        index.name, existing.columns
                    )));
                }
                for column in &index.columns {
                    if !table_state.columns.iter().any(|c| &c.name == column) {
                        return Err(OpenDbError::InvalidInput(format!(
                            "index {} references unknown column {column} on table {table}",
                            index.name
                        )));
                    }
                }
                table_state.indexes.push(index.clone());
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
        (Value::Int64(_), ColumnType::Int64)
            | (Value::Text(_), ColumnType::Text)
            | (Value::Bool(_), ColumnType::Bool)
            | (Value::Float64(_), ColumnType::Float64)
            | (Value::Timestamp(_), ColumnType::Timestamp)
            | (Value::Json(_), ColumnType::Json)
            | (Value::Null, _)
    )
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

    fn alter_record(tx_id: u64, op: AlterTableOp) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::AlterTable {
                table: "accounts".to_owned(),
                op,
            }],
        )
    }

    #[test]
    fn alter_table_add_column_backfills_existing_rows_with_default() {
        let projection = RowProjection::rebuild(&[
            create_accounts_record(1),
            insert_account_record(2, "1"),
            alter_record(
                3,
                AlterTableOp::AddColumn(
                    ColumnDefinition::new("status", ColumnType::Text).with_default(
                        crate::commit_stream::DefaultExpr::Const(Value::Text("active".to_owned())),
                    ),
                ),
            ),
        ])
        .expect("rebuild");
        let accounts = projection.table("accounts").expect("accounts");
        assert_eq!(accounts.columns.last().unwrap().name, "status");
        assert_eq!(
            accounts.rows.get("1").and_then(|row| row.get("status")),
            Some(&Value::Text("active".to_owned()))
        );
    }

    #[test]
    fn alter_table_drop_column_removes_values() {
        let projection = RowProjection::rebuild(&[
            create_accounts_record(1),
            insert_account_record(2, "1"),
            alter_record(
                3,
                AlterTableOp::DropColumn {
                    column: "name".to_owned(),
                },
            ),
        ])
        .expect("rebuild");
        let accounts = projection.table("accounts").expect("accounts");
        assert!(accounts.columns.iter().all(|c| c.name != "name"));
        assert!(
            accounts
                .rows
                .get("1")
                .map(|row| !row.contains_key("name"))
                .unwrap_or(false)
        );
    }

    #[test]
    fn alter_table_drop_column_rejects_primary_key() {
        let result = RowProjection::rebuild(&[
            create_accounts_record(1),
            alter_record(
                2,
                AlterTableOp::DropColumn {
                    column: "id".to_owned(),
                },
            ),
        ]);
        assert!(matches!(result, Err(OpenDbError::InvalidInput(_))));
    }

    #[test]
    fn alter_table_rename_column_updates_schema_and_rows() {
        let projection = RowProjection::rebuild(&[
            create_accounts_record(1),
            insert_account_record(2, "1"),
            alter_record(
                3,
                AlterTableOp::RenameColumn {
                    from: "name".to_owned(),
                    to: "display_name".to_owned(),
                },
            ),
        ])
        .expect("rebuild");
        let accounts = projection.table("accounts").expect("accounts");
        assert!(accounts.columns.iter().any(|c| c.name == "display_name"));
        assert!(accounts.columns.iter().all(|c| c.name != "name"));
        assert_eq!(
            accounts
                .rows
                .get("1")
                .and_then(|row| row.get("display_name")),
            Some(&Value::Text("Ada".to_owned()))
        );
    }

    #[test]
    fn unique_constraint_rejects_duplicate_insert() {
        let projection_result = RowProjection::rebuild(&[
            create_accounts_record(1),
            alter_record(
                2,
                AlterTableOp::AddConstraint(NamedConstraint {
                    name: "accounts_name_unique".to_owned(),
                    kind: crate::commit_stream::ConstraintKind::Unique {
                        columns: vec!["name".to_owned()],
                    },
                }),
            ),
            insert_account_record(3, "1"),
            insert_account_record(4, "2"),
        ]);
        assert!(matches!(
            projection_result,
            Err(OpenDbError::InvalidInput(_))
        ));
    }

    fn create_orders_record(tx_id: u64) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::CreateTable {
                table: "orders".to_owned(),
                columns: vec![
                    ColumnDefinition::primary_key("id", ColumnType::Int64),
                    ColumnDefinition::new("account_id", ColumnType::Int64),
                ],
            }],
        )
    }

    fn alter_orders_record(tx_id: u64, op: AlterTableOp) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::AlterTable {
                table: "orders".to_owned(),
                op,
            }],
        )
    }

    fn insert_order_record(tx_id: u64, id: i64, account_id: i64) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::InsertRow {
                table: "orders".to_owned(),
                key: id.to_string(),
                values: vec![
                    ColumnValue {
                        column: "id".to_owned(),
                        value: Value::Int64(id),
                    },
                    ColumnValue {
                        column: "account_id".to_owned(),
                        value: Value::Int64(account_id),
                    },
                ],
            }],
        )
    }

    fn fk_constraint(name: &str, action: ReferentialAction) -> NamedConstraint {
        NamedConstraint {
            name: name.to_owned(),
            kind: crate::commit_stream::ConstraintKind::ForeignKey {
                columns: vec!["account_id".to_owned()],
                references_table: "accounts".to_owned(),
                references_columns: vec!["id".to_owned()],
                on_delete: action,
                on_update: ReferentialAction::NoAction,
            },
        }
    }

    #[test]
    fn fk_constraint_rejects_insert_without_parent() {
        let result = RowProjection::rebuild(&[
            create_accounts_record(1),
            create_orders_record(2),
            alter_orders_record(
                3,
                AlterTableOp::AddConstraint(fk_constraint(
                    "orders_account_fk",
                    ReferentialAction::NoAction,
                )),
            ),
            insert_order_record(4, 1, 99),
        ]);
        assert!(matches!(result, Err(OpenDbError::InvalidInput(_))));
    }

    fn delete_record(tx_id: u64, table: &str, key: &str) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::DeleteRow {
                table: table.to_owned(),
                key: key.to_owned(),
            }],
        )
    }

    #[test]
    fn delete_with_cascade_removes_children() {
        let projection = RowProjection::rebuild(&[
            create_accounts_record(1),
            create_orders_record(2),
            alter_orders_record(
                3,
                AlterTableOp::AddConstraint(fk_constraint(
                    "orders_account_fk",
                    ReferentialAction::Cascade,
                )),
            ),
            insert_account_record(4, "1"),
            insert_order_record(5, 10, 1),
            delete_record(6, "accounts", "1"),
        ])
        .expect("cascade delete");
        assert!(projection.table("accounts").unwrap().rows.is_empty());
        assert!(projection.table("orders").unwrap().rows.is_empty());
    }

    #[test]
    fn delete_with_no_action_rejects_when_children_exist() {
        let result = RowProjection::rebuild(&[
            create_accounts_record(1),
            create_orders_record(2),
            alter_orders_record(
                3,
                AlterTableOp::AddConstraint(fk_constraint(
                    "orders_account_fk",
                    ReferentialAction::NoAction,
                )),
            ),
            insert_account_record(4, "1"),
            insert_order_record(5, 10, 1),
            delete_record(6, "accounts", "1"),
        ]);
        assert!(matches!(result, Err(OpenDbError::InvalidInput(_))));
    }

    #[test]
    fn alter_table_add_constraint_and_index_record_metadata() {
        let projection = RowProjection::rebuild(&[
            create_accounts_record(1),
            alter_record(
                2,
                AlterTableOp::AddConstraint(NamedConstraint {
                    name: "accounts_unique_name".to_owned(),
                    kind: crate::commit_stream::ConstraintKind::Unique {
                        columns: vec!["name".to_owned()],
                    },
                }),
            ),
            alter_record(
                3,
                AlterTableOp::AddIndex(IndexDescriptor {
                    name: "accounts_name_idx".to_owned(),
                    columns: vec!["name".to_owned()],
                    unique: false,
                    if_not_exists: true,
                }),
            ),
        ])
        .expect("rebuild");
        let accounts = projection.table("accounts").expect("accounts");
        assert_eq!(accounts.constraints.len(), 1);
        assert_eq!(accounts.indexes.len(), 1);
        assert_eq!(accounts.indexes[0].name, "accounts_name_idx");
    }
}
