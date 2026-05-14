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
        if let Statement::DoBlock {
            inner,
            swallow_duplicate,
        } = statement
        {
            for inner_statement in inner {
                match self.execute(inner_statement) {
                    Ok(_) => {}
                    Err(error) => {
                        if swallow_duplicate && is_duplicate_object_error(&error) {
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
            Statement::SelectAll {
                table,
                predicate,
                order_by,
                limit,
                offset,
                columns,
            } => self
                .select_all(
                    &table,
                    &predicate,
                    order_by.as_ref(),
                    limit,
                    offset,
                    &columns,
                )
                .map(|(result, route)| PreparedQuery::Read { result, route }),
            Statement::SelectExpr { items } => {
                self.select_expr(items).map(|result| PreparedQuery::Read {
                    result,
                    route: RouteIntent::Root,
                })
            }
            Statement::AlterTable { table, op } => self.prepare_write(
                vec![Mutation::AlterTable { table, op }],
                "ALTER TABLE",
                RouteIntent::Root,
            ),
            Statement::CreateIndex { table, index } => self.prepare_write(
                vec![Mutation::AlterTable {
                    table,
                    op: opendb_storage::commit_stream::AlterTableOp::AddIndex(index),
                }],
                "CREATE INDEX",
                RouteIntent::Root,
            ),
            Statement::DoBlock { .. } => Err(OpenDbError::Sql(
                "DO blocks must be executed via SqlEngine::execute".to_owned(),
            )),
            Statement::DeleteRow { table, key } => {
                let route_key_value = route_key(&table, &key);
                self.prepare_write(
                    vec![Mutation::DeleteRow {
                        table: table.clone(),
                        key: key.clone(),
                    }],
                    "DELETE 1",
                    RouteIntent::Key {
                        table,
                        key: route_key_value,
                    },
                )
            }
            Statement::UpdateRow {
                table,
                key,
                assignments,
            } => {
                let route_key_value = route_key(&table, &key);
                // Coerce each assigned value against the declared column type
                // before persisting, so DEFAULT NOW() / TIMESTAMP literals etc.
                // behave consistently with INSERT.
                let table_state = self
                    .projection
                    .table(&table)
                    .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
                let mut coerced: Vec<ColumnValue> = Vec::with_capacity(assignments.len());
                for (column_name, value) in assignments {
                    let column = table_state
                        .columns
                        .iter()
                        .find(|c| c.name == column_name)
                        .ok_or_else(|| {
                            OpenDbError::InvalidInput(format!(
                                "unknown column {column_name} on table {table}"
                            ))
                        })?;
                    coerced.push(ColumnValue {
                        column: column_name,
                        value: coerce_value(value, &column.data_type),
                    });
                }
                self.prepare_write(
                    vec![Mutation::UpdateRow {
                        table: table.clone(),
                        key,
                        assignments: coerced,
                    }],
                    "UPDATE 1",
                    RouteIntent::Key {
                        table,
                        key: route_key_value,
                    },
                )
            }
            Statement::Select {
                left,
                join,
                where_clause,
                order_by,
                limit,
                offset,
            } => self
                .select_joined(left, join, where_clause, order_by, limit, offset)
                .map(|(result, route)| PreparedQuery::Read { result, route }),
            Statement::Begin => Ok(PreparedQuery::Read {
                result: QueryResult::Command {
                    tag: "BEGIN".to_owned(),
                },
                route: RouteIntent::Root,
            }),
            Statement::Commit => Ok(PreparedQuery::Read {
                result: QueryResult::Command {
                    tag: "COMMIT".to_owned(),
                },
                route: RouteIntent::Root,
            }),
            Statement::Rollback => Ok(PreparedQuery::Read {
                result: QueryResult::Command {
                    tag: "ROLLBACK".to_owned(),
                },
                route: RouteIntent::Root,
            }),
            Statement::DeleteWhere { table, predicate } => {
                self.prepare_delete_where(table, predicate)
            }
            Statement::UpdateWhere {
                table,
                predicate,
                assignments,
            } => self.prepare_update_where(table, predicate, assignments),
        }
    }

    /// Sprint 14.D: multi-row DELETE via full-table scan + filter.
    fn prepare_delete_where(
        &self,
        table: String,
        predicates: Vec<Predicate>,
    ) -> OpenDbResult<PreparedQuery> {
        let table_state = self
            .projection
            .table(&table)
            .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
        // Validate column references early.
        for p in &predicates {
            let column = table_state
                .columns
                .iter()
                .find(|c| c.name == p.column)
                .ok_or_else(|| {
                    OpenDbError::Sql(format!("column {} not in table {table}", p.column))
                })?;
            if !matches!(p.value, Value::Null) && !value_matches_type(&p.value, &column.data_type) {
                return Err(OpenDbError::Sql(format!(
                    "WHERE value for column {} does not match {:?}",
                    column.name, column.data_type
                )));
            }
        }
        let primary_key = table_state.primary_key_column().ok_or_else(|| {
            OpenDbError::InvalidInput(format!("table {table} has no primary key"))
        })?;
        let matching_keys: Vec<String> = table_state
            .rows
            .iter()
            .filter(|(_, row)| predicates.iter().all(|p| evaluate_predicate(row, p)))
            .map(|(key, _)| key.clone())
            .collect();
        let _ = primary_key;
        let row_count = matching_keys.len();
        let mutations: Vec<Mutation> = matching_keys
            .into_iter()
            .map(|key| Mutation::DeleteRow {
                table: table.clone(),
                key,
            })
            .collect();
        self.prepare_write(
            mutations,
            &format!("DELETE {row_count}"),
            RouteIntent::Scan {
                table: table.clone(),
            },
        )
    }

    /// Sprint 14.D: multi-row UPDATE via full-table scan + filter.
    fn prepare_update_where(
        &self,
        table: String,
        predicates: Vec<Predicate>,
        assignments: Vec<(String, Value)>,
    ) -> OpenDbResult<PreparedQuery> {
        let table_state = self
            .projection
            .table(&table)
            .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
        for p in &predicates {
            let column = table_state
                .columns
                .iter()
                .find(|c| c.name == p.column)
                .ok_or_else(|| {
                    OpenDbError::Sql(format!("column {} not in table {table}", p.column))
                })?;
            if !matches!(p.value, Value::Null) && !value_matches_type(&p.value, &column.data_type) {
                return Err(OpenDbError::Sql(format!(
                    "WHERE value for column {} does not match {:?}",
                    column.name, column.data_type
                )));
            }
        }
        let coerced_assignments: Vec<ColumnValue> = assignments
            .into_iter()
            .map(|(column_name, value)| {
                let column = table_state
                    .columns
                    .iter()
                    .find(|c| c.name == column_name)
                    .ok_or_else(|| {
                        OpenDbError::InvalidInput(format!(
                            "unknown column {column_name} on table {table}"
                        ))
                    })?;
                Ok(ColumnValue {
                    column: column_name,
                    value: coerce_value(value, &column.data_type),
                })
            })
            .collect::<OpenDbResult<Vec<_>>>()?;
        let matching_keys: Vec<String> = table_state
            .rows
            .iter()
            .filter(|(_, row)| predicates.iter().all(|p| evaluate_predicate(row, p)))
            .map(|(key, _)| key.clone())
            .collect();
        let row_count = matching_keys.len();
        let mutations: Vec<Mutation> = matching_keys
            .into_iter()
            .map(|key| Mutation::UpdateRow {
                table: table.clone(),
                key,
                assignments: coerced_assignments.clone(),
            })
            .collect();
        self.prepare_write(
            mutations,
            &format!("UPDATE {row_count}"),
            RouteIntent::Scan {
                table: table.clone(),
            },
        )
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
        predicates: &[Predicate],
        order_by: Option<&crate::ast::OrderBy>,
        limit: Option<u64>,
        offset: Option<u64>,
        columns: &crate::ast::SelectColumns,
    ) -> OpenDbResult<(QueryResult, RouteIntent)> {
        let table_state = self
            .projection
            .table(table)
            .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
        let all_columns = table_state.column_names();
        let column_names = match columns {
            crate::ast::SelectColumns::Star => all_columns.clone(),
            crate::ast::SelectColumns::Explicit(requested) => {
                for name in requested {
                    if !all_columns.iter().any(|column| column == name) {
                        return Err(OpenDbError::Sql(format!(
                            "column {name} not in table {table}"
                        )));
                    }
                }
                requested.clone()
            }
        };
        let (rows, route) = if predicates.is_empty() {
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
        } else {
            // Sprint 14: a conjunction of predicates is filtered with a full-table
            // scan unless one of the predicates is a PK-equality (then we fast-path
            // the routing intent while still running the conjunction).
            let primary_key = table_state.primary_key_column().ok_or_else(|| {
                OpenDbError::InvalidInput(format!("table {table} has no primary key"))
            })?;
            for predicate in predicates {
                let column = table_state
                    .columns
                    .iter()
                    .find(|c| c.name == predicate.column)
                    .ok_or_else(|| {
                        OpenDbError::Sql(format!(
                            "column {} not in table {table}",
                            predicate.column
                        ))
                    })?;
                if !matches!(predicate.value, Value::Null)
                    && !value_matches_type(&predicate.value, &column.data_type)
                {
                    return Err(OpenDbError::Sql(format!(
                        "WHERE value for column {} does not match {:?}",
                        column.name, column.data_type
                    )));
                }
            }
            let pk_eq = predicates
                .iter()
                .find(|p| p.column == primary_key.name && matches!(p.op, crate::ast::WhereOp::Eq));
            let route = match pk_eq {
                Some(p) => RouteIntent::Key {
                    table: table.to_owned(),
                    key: route_key(table, &value_to_key(&p.value)),
                },
                None => RouteIntent::Scan {
                    table: table.to_owned(),
                },
            };
            let rows = table_state
                .rows
                .values()
                .filter(|row| predicates.iter().all(|p| evaluate_predicate(row, p)))
                .map(|row| project_row(table, &column_names, row))
                .collect::<OpenDbResult<Vec<_>>>()?;
            (rows, route)
        };
        let mut rows = rows;
        if let Some(order_by) = order_by {
            let position = column_names
                .iter()
                .position(|name| name == &order_by.column)
                .ok_or_else(|| {
                    OpenDbError::Sql(format!(
                        "ORDER BY column {} not in projection",
                        order_by.column
                    ))
                })?;
            rows.sort_by(|left, right| {
                let l = left.get(position).cloned().unwrap_or(Value::Null);
                let r = right.get(position).cloned().unwrap_or(Value::Null);
                let ordering = compare_values(&l, &r);
                match order_by.direction {
                    crate::ast::OrderDirection::Asc => ordering,
                    crate::ast::OrderDirection::Desc => ordering.reverse(),
                }
            });
        }
        let offset_count = offset.unwrap_or(0) as usize;
        let limit_count = limit.unwrap_or(u64::MAX) as usize;
        let final_rows: Vec<_> = rows
            .into_iter()
            .skip(offset_count)
            .take(limit_count)
            .collect();
        Ok((
            QueryResult::Rows {
                columns: column_names,
                column_types: Vec::new(),
                rows: final_rows,
            },
            route,
        ))
    }
}

impl SqlEngine {
    fn select_expr(&self, items: Vec<crate::ast::SelectExprItem>) -> OpenDbResult<QueryResult> {
        use crate::ast::{SelectExpr, SelectFunction};
        let mut column_names = Vec::with_capacity(items.len());
        let mut row: Vec<Value> = Vec::with_capacity(items.len());
        for (index, item) in items.into_iter().enumerate() {
            let (value, default_name) = match item.expr {
                SelectExpr::Literal(value) => {
                    let name = match &value {
                        Value::Int64(_) => "?column?".to_string(),
                        Value::Text(_) => "?column?".to_string(),
                        Value::Bool(_) => "?column?".to_string(),
                        Value::Float64(_) => "?column?".to_string(),
                        Value::Timestamp(_) => "?column?".to_string(),
                        Value::Json(_) => "?column?".to_string(),
                        Value::Null => "?column?".to_string(),
                    };
                    (value, name)
                }
                SelectExpr::Function(SelectFunction::Version) => (
                    Value::Text("opendb-node 0.1.0 on PostgreSQL 16.0 compatible".to_owned()),
                    "version".to_owned(),
                ),
                SelectExpr::Function(SelectFunction::Now) => (
                    Value::Timestamp((self.next_tx + 1) as i64),
                    "now".to_owned(),
                ),
                SelectExpr::Function(SelectFunction::CurrentTimestamp) => (
                    Value::Timestamp((self.next_tx + 1) as i64),
                    "current_timestamp".to_owned(),
                ),
            };
            let name = item.alias.unwrap_or_else(|| {
                if default_name == "?column?" {
                    format!("?column?_{index}")
                } else {
                    default_name
                }
            });
            column_names.push(name);
            row.push(value);
        }
        Ok(QueryResult::Rows {
            columns: column_names,
            column_types: Vec::new(),
            rows: vec![row],
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn select_joined(
        &self,
        left_table: String,
        join: crate::ast::JoinClause,
        where_clause: Option<crate::ast::JoinedPredicate>,
        order_by: Option<crate::ast::JoinedOrderBy>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> OpenDbResult<(QueryResult, RouteIntent)> {
        let left_state = self
            .projection
            .table(&left_table)
            .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {left_table}")))?;
        let right_state = self
            .projection
            .table(&join.right)
            .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {}", join.right)))?;

        let mut output_columns: Vec<String> = left_state
            .columns
            .iter()
            .map(|column| format!("{left_table}.{}", column.name))
            .collect();
        output_columns.extend(
            right_state
                .columns
                .iter()
                .map(|column| format!("{}.{}", join.right, column.name)),
        );

        let left_columns = left_state.column_names();
        let right_columns = right_state.column_names();

        let mut joined_rows: Vec<Vec<Value>> = Vec::new();
        for (_left_key, left_row) in left_state.rows.iter() {
            let left_join_value = left_row
                .get(&join.left_column)
                .cloned()
                .unwrap_or(Value::Null);
            let mut matched = false;
            for (_right_key, right_row) in right_state.rows.iter() {
                let right_join_value = right_row
                    .get(&join.right_column)
                    .cloned()
                    .unwrap_or(Value::Null);
                if values_join_match(&left_join_value, &right_join_value) {
                    matched = true;
                    let projected = project_joined_row(
                        &left_columns,
                        left_row,
                        &right_columns,
                        Some(right_row),
                    );
                    joined_rows.push(projected);
                }
            }
            if !matched && matches!(join.kind, crate::ast::JoinKind::Left) {
                let projected = project_joined_row(&left_columns, left_row, &right_columns, None);
                joined_rows.push(projected);
            }
        }

        // WHERE
        if let Some(predicate) = &where_clause {
            let index = find_qualified_column_position(
                &output_columns,
                predicate.qualifier.as_deref(),
                &predicate.column,
            )?;
            joined_rows.retain(|row| {
                row.get(index)
                    .map(|value| values_join_match(value, &predicate.value))
                    .unwrap_or(false)
            });
        }

        // ORDER BY
        if let Some(order_by) = &order_by {
            let index = find_qualified_column_position(
                &output_columns,
                order_by.qualifier.as_deref(),
                &order_by.column,
            )?;
            joined_rows.sort_by(|left, right| {
                let l = left.get(index).cloned().unwrap_or(Value::Null);
                let r = right.get(index).cloned().unwrap_or(Value::Null);
                let ordering = compare_values(&l, &r);
                match order_by.direction {
                    crate::ast::OrderDirection::Asc => ordering,
                    crate::ast::OrderDirection::Desc => ordering.reverse(),
                }
            });
        }

        let offset_count = offset.unwrap_or(0) as usize;
        let limit_count = limit.unwrap_or(u64::MAX) as usize;
        let final_rows: Vec<_> = joined_rows
            .into_iter()
            .skip(offset_count)
            .take(limit_count)
            .collect();

        let route = RouteIntent::Scan { table: left_table };
        Ok((
            QueryResult::Rows {
                columns: output_columns,
                column_types: Vec::new(),
                rows: final_rows,
            },
            route,
        ))
    }
}

fn project_joined_row(
    left_columns: &[String],
    left_row: &std::collections::BTreeMap<String, Value>,
    right_columns: &[String],
    right_row: Option<&std::collections::BTreeMap<String, Value>>,
) -> Vec<Value> {
    let mut row = Vec::with_capacity(left_columns.len() + right_columns.len());
    for column in left_columns {
        row.push(left_row.get(column).cloned().unwrap_or(Value::Null));
    }
    for column in right_columns {
        let value = match right_row {
            Some(r) => r.get(column).cloned().unwrap_or(Value::Null),
            None => Value::Null,
        };
        row.push(value);
    }
    row
}

fn values_join_match(left: &Value, right: &Value) -> bool {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return false;
    }
    left == right
}

fn find_qualified_column_position(
    columns: &[String],
    qualifier: Option<&str>,
    column: &str,
) -> OpenDbResult<usize> {
    if let Some(qualifier) = qualifier {
        let target = format!("{qualifier}.{column}");
        return columns
            .iter()
            .position(|name| name == &target)
            .ok_or_else(|| OpenDbError::Sql(format!("column {target} not in projection")));
    }
    // Without qualifier: try suffix match.
    let suffix = format!(".{column}");
    let mut matches: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter(|(_, name)| name.ends_with(&suffix) || *name == &column.to_owned())
        .map(|(i, _)| i)
        .collect();
    if matches.is_empty() {
        return Err(OpenDbError::Sql(format!(
            "column {column} not in projection"
        )));
    }
    if matches.len() > 1 {
        return Err(OpenDbError::Sql(format!(
            "column {column} is ambiguous across joined tables (qualify with table.column)"
        )));
    }
    Ok(matches.remove(0))
}

fn compare_values(left: &Value, right: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
        (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (Value::Json(a), Value::Json(b)) => a.to_string().cmp(&b.to_string()),
        (a, b) => format!("{a:?}").cmp(&format!("{b:?}")),
    }
}

fn route_key(table: &str, row_key: &str) -> String {
    format!("{table}/{row_key}")
}

/// Sprint 14: evaluate a single-column predicate against a projected row.
fn evaluate_predicate(
    row: &std::collections::BTreeMap<String, Value>,
    predicate: &Predicate,
) -> bool {
    use crate::ast::WhereOp;
    let value = row.get(&predicate.column).cloned().unwrap_or(Value::Null);
    let ordering = compare_values(&value, &predicate.value);
    match predicate.op {
        WhereOp::Eq => value == predicate.value,
        WhereOp::NotEq => value != predicate.value,
        WhereOp::Lt => ordering == std::cmp::Ordering::Less,
        WhereOp::Lte => ordering != std::cmp::Ordering::Greater,
        WhereOp::Gt => ordering == std::cmp::Ordering::Greater,
        WhereOp::Gte => ordering != std::cmp::Ordering::Less,
    }
}

fn is_duplicate_object_error(error: &OpenDbError) -> bool {
    is_duplicate_object_error_for_do_block(error)
}

pub fn is_duplicate_object_error_for_do_block(error: &OpenDbError) -> bool {
    let message = match error {
        OpenDbError::InvalidInput(message) => message,
        OpenDbError::Sql(message) => message,
        _ => return false,
    };
    let lower = message.to_ascii_lowercase();
    lower.contains("already exists") || lower.contains("duplicate")
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
        (Value::Text(text), ColumnType::Timestamp) => match parse_text_to_timestamp(&text) {
            Some(micros) => Value::Timestamp(micros),
            None => Value::Text(text),
        },
        (Value::Text(text), ColumnType::Json) => match serde_json::from_str(&text) {
            Ok(json) => Value::Json(json),
            Err(_) => Value::Text(text),
        },
        (other, _) => other,
    }
}

/// Sprint 12.1: accept ISO-8601 (RFC 3339) and PostgreSQL
/// `YYYY-MM-DD HH:MM:SS[.uuuuuu]` text literals as `TIMESTAMP` values.
/// Returns microseconds since the Unix epoch (UTC, no timezone).
fn parse_text_to_timestamp(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(trimmed) {
        return Some(dt.with_timezone(&chrono::Utc).timestamp_micros());
    }
    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(trimmed, format) {
            return Some(naive.and_utc().timestamp_micros());
        }
    }
    None
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
            let coerced = coerce_value(value.clone(), &column.data_type);
            if !coerced.is_null() && !value_matches_type(&coerced, &column.data_type) {
                return Err(OpenDbError::InvalidInput(format!(
                    "DEFAULT for column {} on table {table} does not match {:?}",
                    column.name, column.data_type
                )));
            }
            Ok(coerced)
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
    fn select_where_non_primary_key_filters_via_scan() {
        // Sprint 14: WHERE on a non-PK column triggers a full-table scan
        // and returns matching rows. The legacy assertion that this should
        // fail was retired with the operator extension.
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        engine
            .execute(parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("parse"))
            .expect("insert");
        engine
            .execute(parse("INSERT INTO accounts VALUES (2, 'Grace')").expect("parse"))
            .expect("insert2");

        let result = engine
            .execute(parse("SELECT * FROM accounts WHERE name = 'Ada'").expect("parse"))
            .expect("scan");
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Text("Ada".to_owned()));
    }

    #[test]
    fn delete_where_multi_row_filters_via_scan() {
        let mut engine = SqlEngine::default();
        engine
            .execute(
                parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)").expect("parse"),
            )
            .expect("create");
        for (id, name, score) in [(1, "a", 10), (2, "a", 20), (3, "b", 30)] {
            engine
                .execute(
                    parse(&format!("INSERT INTO t VALUES ({id}, '{name}', {score})"))
                        .expect("parse"),
                )
                .expect("insert");
        }
        let tag = engine
            .execute(parse("DELETE FROM t WHERE name = 'a'").expect("parse"))
            .expect("delete multi");
        assert_eq!(
            tag,
            QueryResult::Command {
                tag: "DELETE 2".to_owned()
            }
        );
        let rows = engine
            .execute(parse("SELECT * FROM t").expect("parse"))
            .expect("select");
        let QueryResult::Rows { rows, .. } = rows else {
            panic!("expected Rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Value::Text("b".to_owned()));
    }

    #[test]
    fn update_where_multi_row_filters_via_scan() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, status TEXT)").expect("parse"))
            .expect("create");
        for (id, status) in [(1, "pending"), (2, "pending"), (3, "done")] {
            engine
                .execute(parse(&format!("INSERT INTO t VALUES ({id}, '{status}')")).expect("parse"))
                .expect("insert");
        }
        let tag = engine
            .execute(
                parse("UPDATE t SET status = 'active' WHERE status = 'pending'").expect("parse"),
            )
            .expect("update multi");
        assert_eq!(
            tag,
            QueryResult::Command {
                tag: "UPDATE 2".to_owned()
            }
        );
    }

    #[test]
    fn select_where_and_composes_predicates() {
        let mut engine = SqlEngine::default();
        engine
            .execute(
                parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)").expect("parse"),
            )
            .expect("create");
        for (id, name, score) in [(1, "a", 10), (2, "a", 20), (3, "b", 30)] {
            engine
                .execute(
                    parse(&format!("INSERT INTO t VALUES ({id}, '{name}', {score})"))
                        .expect("parse"),
                )
                .expect("insert");
        }
        let result = engine
            .execute(parse("SELECT * FROM t WHERE name = 'a' AND score > 10").expect("parse"))
            .expect("composite where");
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int64(2));
    }

    #[test]
    fn select_where_comparison_operators_filter_correctly() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, score INT)").expect("parse"))
            .expect("create");
        for (id, score) in [(1, 10), (2, 20), (3, 30), (4, 40)] {
            engine
                .execute(parse(&format!("INSERT INTO t VALUES ({id}, {score})")).expect("parse"))
                .expect("insert");
        }
        let gt = engine
            .execute(parse("SELECT * FROM t WHERE score > 20").expect("parse"))
            .expect("gt");
        let QueryResult::Rows { rows, .. } = gt else {
            panic!("expected Rows");
        };
        assert_eq!(rows.len(), 2);
        let neq = engine
            .execute(parse("SELECT * FROM t WHERE score != 20").expect("parse"))
            .expect("neq");
        let QueryResult::Rows { rows, .. } = neq else {
            panic!("expected Rows");
        };
        assert_eq!(rows.len(), 3);
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
    fn named_insert_coerces_text_literal_into_jsonb_value() {
        let mut engine = SqlEngine::default();
        engine
            .execute(
                parse(
                    "CREATE TABLE t (id INT PRIMARY KEY, data JSONB NOT NULL DEFAULT '{}'::jsonb)",
                )
                .expect("parse"),
            )
            .expect("create");
        engine
            .execute(
                parse("INSERT INTO t (id, data) VALUES (1, '{\"k\":\"v\"}'::jsonb)")
                    .expect("parse"),
            )
            .expect("insert");
        engine
            .execute(parse("INSERT INTO t (id) VALUES (2)").expect("parse"))
            .expect("insert default");

        let last_two = &engine.commits()[engine.commits().len() - 2..];
        let first_insert = &last_two[0];
        let second_insert = &last_two[1];
        let Mutation::InsertRow { values, .. } = &first_insert.mutations[0] else {
            panic!("expected InsertRow");
        };
        let data = values.iter().find(|cv| cv.column == "data").expect("data");
        match &data.value {
            Value::Json(value) => assert_eq!(value, &serde_json::json!({"k":"v"})),
            other => panic!("expected Json, got {other:?}"),
        }
        let Mutation::InsertRow { values: vals2, .. } = &second_insert.mutations[0] else {
            panic!("expected InsertRow");
        };
        let data2 = vals2.iter().find(|cv| cv.column == "data").expect("data");
        match &data2.value {
            Value::Json(value) => assert_eq!(value, &serde_json::json!({})),
            other => panic!("expected Json default, got {other:?}"),
        }
    }

    #[test]
    fn named_insert_rejects_invalid_jsonb_literal() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, data JSONB)").expect("parse"))
            .expect("create");

        let result = engine.execute(
            parse("INSERT INTO t (id, data) VALUES (1, '{not json}'::jsonb)").expect("parse"),
        );
        assert!(matches!(result, Err(OpenDbError::InvalidInput(_))));
    }

    #[test]
    fn select_literal_returns_single_row() {
        let mut engine = SqlEngine::default();
        let result = engine
            .execute(parse("SELECT 1").expect("parse"))
            .expect("select");
        let QueryResult::Rows { columns, rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int64(1));
        assert_eq!(columns.len(), 1);
    }

    #[test]
    fn select_version_returns_canonical_string() {
        let mut engine = SqlEngine::default();
        let result = engine
            .execute(parse("SELECT version() AS v").expect("parse"))
            .expect("select version");
        let QueryResult::Rows { columns, rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(columns, vec!["v"]);
        let Value::Text(text) = &rows[0][0] else {
            panic!("expected Text");
        };
        assert!(text.contains("opendb-node"));
    }

    #[test]
    fn select_current_timestamp_returns_timestamp() {
        let mut engine = SqlEngine::default();
        let result = engine
            .execute(parse("SELECT current_timestamp").expect("parse"))
            .expect("select");
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected Rows");
        };
        assert!(matches!(rows[0][0], Value::Timestamp(_)));
    }

    #[test]
    fn select_explicit_projection_returns_only_listed_columns() {
        let mut engine = SqlEngine::default();
        engine
            .execute(
                parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT, label TEXT)")
                    .expect("parse"),
            )
            .expect("create");
        engine
            .execute(
                parse("INSERT INTO accounts (id, name, label) VALUES (1, 'Ada', 'L')")
                    .expect("parse"),
            )
            .expect("insert");
        let result = engine
            .execute(parse("SELECT id, name FROM accounts").expect("parse"))
            .expect("select");
        let QueryResult::Rows { columns, rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(columns, vec!["id", "name"]);
        assert_eq!(rows[0].len(), 2);
    }

    #[test]
    fn insert_coerces_iso_8601_timestamp_literal() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP)").expect("parse"))
            .expect("create");
        engine
            .execute(
                parse("INSERT INTO t (id, ts) VALUES (1, '2026-05-13T00:00:00Z')").expect("parse"),
            )
            .expect("insert iso");
        let last = engine.commits().last().expect("commit");
        let Mutation::InsertRow { values, .. } = &last.mutations[0] else {
            panic!("expected InsertRow");
        };
        let ts = values.iter().find(|cv| cv.column == "ts").expect("ts");
        assert!(matches!(ts.value, Value::Timestamp(_)));
    }

    #[test]
    fn insert_coerces_postgres_timestamp_literal() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, ts TIMESTAMP)").expect("parse"))
            .expect("create");
        engine
            .execute(
                parse("INSERT INTO t (id, ts) VALUES (1, '2026-05-13 00:00:00')").expect("parse"),
            )
            .expect("insert pg");
        let last = engine.commits().last().expect("commit");
        let Mutation::InsertRow { values, .. } = &last.mutations[0] else {
            panic!("expected InsertRow");
        };
        let ts = values.iter().find(|cv| cv.column == "ts").expect("ts");
        assert!(matches!(ts.value, Value::Timestamp(_)));
    }

    #[test]
    fn begin_insert_commit_emits_tags_and_applies_insert() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY)").expect("parse"))
            .expect("create");

        let begin = engine
            .execute(parse("BEGIN").expect("parse"))
            .expect("begin");
        assert_eq!(
            begin,
            QueryResult::Command {
                tag: "BEGIN".to_owned()
            }
        );
        engine
            .execute(parse("INSERT INTO t (id) VALUES (1)").expect("parse"))
            .expect("insert");
        let commit = engine
            .execute(parse("COMMIT").expect("parse"))
            .expect("commit");
        assert_eq!(
            commit,
            QueryResult::Command {
                tag: "COMMIT".to_owned()
            }
        );
        let rows = engine
            .execute(parse("SELECT * FROM t").expect("parse"))
            .expect("select");
        let QueryResult::Rows { rows, .. } = rows else {
            panic!("expected Rows");
        };
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn select_inner_join_returns_matching_rows() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE a (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create a");
        engine
            .execute(parse("CREATE TABLE b (id INT PRIMARY KEY, a_id INT)").expect("parse"))
            .expect("create b");
        engine
            .execute(parse("INSERT INTO a (id, name) VALUES (1, 'one')").expect("parse"))
            .expect("insert a1");
        engine
            .execute(parse("INSERT INTO a (id, name) VALUES (2, 'two')").expect("parse"))
            .expect("insert a2");
        engine
            .execute(parse("INSERT INTO b (id, a_id) VALUES (10, 1)").expect("parse"))
            .expect("insert b");
        let result = engine
            .execute(parse("SELECT * FROM a INNER JOIN b ON a.id = b.a_id").expect("parse"))
            .expect("join");
        let QueryResult::Rows { columns, rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(columns, vec!["a.id", "a.name", "b.id", "b.a_id"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int64(1));
        assert_eq!(rows[0][2], Value::Int64(10));
    }

    #[test]
    fn select_left_join_pads_null_for_missing_right() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE a (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create a");
        engine
            .execute(parse("CREATE TABLE b (id INT PRIMARY KEY, a_id INT)").expect("parse"))
            .expect("create b");
        engine
            .execute(parse("INSERT INTO a (id, name) VALUES (1, 'one')").expect("parse"))
            .expect("insert a1");
        engine
            .execute(parse("INSERT INTO a (id, name) VALUES (2, 'two')").expect("parse"))
            .expect("insert a2");
        engine
            .execute(parse("INSERT INTO b (id, a_id) VALUES (10, 1)").expect("parse"))
            .expect("insert b");
        let result = engine
            .execute(
                parse("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id ORDER BY a.id ASC")
                    .expect("parse"),
            )
            .expect("left join");
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int64(1));
        assert_eq!(rows[0][2], Value::Int64(10));
        assert_eq!(rows[1][0], Value::Int64(2));
        assert_eq!(rows[1][2], Value::Null);
    }

    #[test]
    fn select_with_order_by_limit_offset_returns_sorted_slice() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        for i in 1..=5 {
            engine
                .execute(
                    parse(&format!("INSERT INTO t (id, name) VALUES ({i}, 'r{i}')"))
                        .expect("parse"),
                )
                .expect("insert");
        }
        let result = engine
            .execute(parse("SELECT * FROM t ORDER BY id DESC LIMIT 2 OFFSET 1").expect("parse"))
            .expect("select");
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(rows.len(), 2);
        // ORDER BY id DESC → 5, 4, 3, 2, 1; OFFSET 1 → 4, 3, 2, 1; LIMIT 2 → 4, 3
        let first_id = match rows[0][0] {
            Value::Int64(v) => v,
            _ => panic!("expected Int64 id"),
        };
        let second_id = match rows[1][0] {
            Value::Int64(v) => v,
            _ => panic!("expected Int64 id"),
        };
        assert_eq!(first_id, 4);
        assert_eq!(second_id, 3);
    }

    #[test]
    fn update_row_changes_assigned_columns() {
        let mut engine = SqlEngine::default();
        engine
            .execute(
                parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT, status TEXT)")
                    .expect("parse"),
            )
            .expect("create");
        engine
            .execute(
                parse("INSERT INTO accounts (id, name, status) VALUES (1, 'Ada', 'pending')")
                    .expect("parse"),
            )
            .expect("insert");
        let tag = engine
            .execute(
                parse("UPDATE accounts SET name = 'Bob', status = 'active' WHERE id = 1")
                    .expect("parse"),
            )
            .expect("update");
        assert_eq!(
            tag,
            QueryResult::Command {
                tag: "UPDATE 1".to_owned()
            }
        );
        let result = engine
            .execute(parse("SELECT * FROM accounts WHERE id = 1").expect("parse"))
            .expect("select");
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(rows[0][1], Value::Text("Bob".to_owned()));
        assert_eq!(rows[0][2], Value::Text("active".to_owned()));
    }

    #[test]
    fn update_rejects_change_to_primary_key() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        engine
            .execute(parse("INSERT INTO t (id, name) VALUES (1, 'a')").expect("parse"))
            .expect("insert");
        let result = engine.execute(parse("UPDATE t SET id = 2 WHERE id = 1").expect("parse"));
        assert!(matches!(result, Err(OpenDbError::InvalidInput(_))));
    }

    #[test]
    fn update_rejects_missing_row() {
        // Sprint 14.D: UPDATE WHERE pk = lit follows the scan + filter path,
        // which silently affects zero rows when none match (Postgres semantics).
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        let tag = engine
            .execute(parse("UPDATE t SET name = 'b' WHERE id = 99").expect("parse"))
            .expect("update");
        assert_eq!(
            tag,
            QueryResult::Command {
                tag: "UPDATE 0".to_owned()
            }
        );
    }

    #[test]
    fn delete_row_executes_end_to_end() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        engine
            .execute(parse("INSERT INTO t (id, name) VALUES (1, 'Ada')").expect("parse"))
            .expect("insert");
        let result = engine
            .execute(parse("DELETE FROM t WHERE id = 1").expect("parse"))
            .expect("delete");
        assert_eq!(
            result,
            QueryResult::Command {
                tag: "DELETE 1".to_owned()
            }
        );
        let select = engine
            .execute(parse("SELECT * FROM t").expect("parse"))
            .expect("select");
        let QueryResult::Rows { rows, .. } = select else {
            panic!("expected Rows");
        };
        assert!(rows.is_empty());
    }

    #[test]
    fn alter_table_add_column_then_named_insert_uses_default() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        engine
            .execute(parse("INSERT INTO t (id, name) VALUES (1, 'Ada')").expect("parse"))
            .expect("insert");
        engine
            .execute(
                parse("ALTER TABLE t ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")
                    .expect("parse"),
            )
            .expect("alter");
        engine
            .execute(parse("INSERT INTO t (id, name) VALUES (2, 'Grace')").expect("parse"))
            .expect("insert grace");

        let last = engine.commits().last().expect("commit");
        let Mutation::InsertRow { values, .. } = &last.mutations[0] else {
            panic!("expected InsertRow");
        };
        let status = values
            .iter()
            .find(|cv| cv.column == "status")
            .expect("status column");
        assert_eq!(status.value, Value::Text("active".to_owned()));
    }

    #[test]
    fn create_index_records_metadata_without_acceleration() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").expect("parse"))
            .expect("create");
        let result = engine
            .execute(
                parse("CREATE INDEX IF NOT EXISTS t_name_idx ON t USING btree (name)")
                    .expect("parse"),
            )
            .expect("create index");
        assert_eq!(
            result,
            QueryResult::Command {
                tag: "CREATE INDEX".to_owned()
            }
        );
    }

    #[test]
    fn do_block_swallows_duplicate_object_errors() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY)").expect("parse"))
            .expect("create");
        engine
            .execute(parse("ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'x'").expect("parse"))
            .expect("alter");
        // Second ALTER would fail with "already exists", but the DO block
        // marked WHEN duplicate_object swallows the error.
        let result = engine.execute(
            parse(
                "DO $$ BEGIN ALTER TABLE t ADD COLUMN tag TEXT DEFAULT 'x'; EXCEPTION WHEN duplicate_object THEN null; END $$",
            )
            .expect("parse"),
        );
        assert!(matches!(
            result,
            Ok(QueryResult::Command { ref tag }) if tag == "DO"
        ));
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
