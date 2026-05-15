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
        /// Sprint 16.A: optional RETURNING projection. When `Some`, the
        /// caller emits these `QueryResult::Rows` instead of the
        /// CommandComplete tag — matching Postgres semantics for
        /// `INSERT ... RETURNING ...`.
        returning_result: Option<QueryResult>,
    },
}

#[derive(Clone, Debug, Default)]
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
            PreparedQuery::Write {
                record,
                tag,
                returning_result,
                ..
            } => {
                self.apply_committed(record)?;
                if let Some(rows) = returning_result {
                    Ok(rows)
                } else {
                    Ok(QueryResult::Command { tag })
                }
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
                returning,
            } => {
                let table_state = self
                    .projection
                    .table(&table)
                    .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
                let columns = table_state.columns.clone();
                let now_microseconds = wall_clock_microseconds();
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
                // Sprint 16.A: build the RETURNING projection from the
                // freshly-materialized column values (defaults already
                // applied by `materialize_insert_values`).
                let returning_result = if let Some(spec) = &returning {
                    Some(project_returning(&columns, &column_values, spec)?)
                } else {
                    None
                };
                self.prepare_write_with_returning(
                    vec![Mutation::InsertRow {
                        table,
                        key: row_key,
                        values: column_values,
                    }],
                    "INSERT 0 1",
                    route,
                    returning_result,
                )
            }
            Statement::SelectAll {
                table,
                predicate,
                order_by,
                limit,
                offset,
                columns,
                group_by,
                having,
            } => self
                .select_all(
                    &table,
                    &predicate,
                    order_by.as_ref(),
                    limit,
                    offset,
                    &columns,
                    &group_by,
                    &having,
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
            Statement::DeleteRow {
                table,
                key,
                returning: _,
            } => {
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
                returning: _,
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
                columns,
                group_by,
                having,
            } => self
                .select_joined(
                    left,
                    join,
                    where_clause,
                    order_by,
                    limit,
                    offset,
                    columns,
                    group_by,
                    having,
                )
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
            Statement::DeleteWhere {
                table,
                predicate,
                returning,
            } => self.prepare_delete_where(table, predicate, returning),
            Statement::UpdateWhere {
                table,
                predicate,
                assignments,
                returning,
            } => self.prepare_update_where(table, predicate, assignments, returning),
        }
    }

    /// Sprint 14.D: multi-row DELETE via full-table scan + filter.
    fn prepare_delete_where(
        &self,
        table: String,
        predicates: Vec<Predicate>,
        returning: Option<crate::ast::ReturningClause>,
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
        // Sprint 16.B: capture the matching pre-mutation rows so DELETE
        // RETURNING can echo them. We materialize the lookup once and reuse
        // it for both the mutation list and the RETURNING projection.
        let matching: Vec<(String, std::collections::BTreeMap<String, Value>)> = table_state
            .rows
            .iter()
            .filter(|(_, row)| predicates.iter().all(|p| evaluate_predicate(row, p)))
            .map(|(key, row)| (key.clone(), row.clone()))
            .collect();
        let _ = primary_key;
        let row_count = matching.len();
        let returning_result = if let Some(spec) = &returning {
            let rows: Vec<_> = matching.iter().map(|(_, row)| row.clone()).collect();
            Some(project_returning_rows(&table_state.columns, &rows, spec)?)
        } else {
            None
        };
        // Sprint 16.B: a WHERE that matches zero rows is a valid no-op.
        // Storage rejects empty mutation lists, so short-circuit to a Read.
        if row_count == 0 {
            let result = match returning_result {
                Some(rows) => rows,
                None => QueryResult::Command {
                    tag: format!("DELETE {row_count}"),
                },
            };
            return Ok(PreparedQuery::Read {
                result,
                route: RouteIntent::Scan {
                    table: table.clone(),
                },
            });
        }
        let mutations: Vec<Mutation> = matching
            .into_iter()
            .map(|(key, _)| Mutation::DeleteRow {
                table: table.clone(),
                key,
            })
            .collect();
        self.prepare_write_with_returning(
            mutations,
            &format!("DELETE {row_count}"),
            RouteIntent::Scan {
                table: table.clone(),
            },
            returning_result,
        )
    }

    /// Sprint 14.D: multi-row UPDATE via full-table scan + filter.
    fn prepare_update_where(
        &self,
        table: String,
        predicates: Vec<Predicate>,
        assignments: Vec<(String, Value)>,
        returning: Option<crate::ast::ReturningClause>,
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
        let matching: Vec<(String, std::collections::BTreeMap<String, Value>)> = table_state
            .rows
            .iter()
            .filter(|(_, row)| predicates.iter().all(|p| evaluate_predicate(row, p)))
            .map(|(key, row)| (key.clone(), row.clone()))
            .collect();
        let row_count = matching.len();
        // Sprint 16.B: synthesize the post-update row by overlaying the
        // assignments on top of the captured pre-update row. UPDATE
        // RETURNING in Postgres returns the post-state.
        let returning_result = if let Some(spec) = &returning {
            let post_rows: Vec<std::collections::BTreeMap<String, Value>> = matching
                .iter()
                .map(|(_, row)| {
                    let mut next = row.clone();
                    for cv in &coerced_assignments {
                        next.insert(cv.column.clone(), cv.value.clone());
                    }
                    next
                })
                .collect();
            Some(project_returning_rows(
                &table_state.columns,
                &post_rows,
                spec,
            )?)
        } else {
            None
        };
        // Sprint 16.B: 0-row UPDATE is a valid no-op (same rationale as DELETE).
        if row_count == 0 {
            let result = match returning_result {
                Some(rows) => rows,
                None => QueryResult::Command {
                    tag: format!("UPDATE {row_count}"),
                },
            };
            return Ok(PreparedQuery::Read {
                result,
                route: RouteIntent::Scan {
                    table: table.clone(),
                },
            });
        }
        let mutations: Vec<Mutation> = matching
            .into_iter()
            .map(|(key, _)| Mutation::UpdateRow {
                table: table.clone(),
                key,
                assignments: coerced_assignments.clone(),
            })
            .collect();
        self.prepare_write_with_returning(
            mutations,
            &format!("UPDATE {row_count}"),
            RouteIntent::Scan {
                table: table.clone(),
            },
            returning_result,
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
        self.prepare_write_with_returning(mutations, tag, route, None)
    }

    /// Sprint 16.A: prepare a write whose result is the projected RETURNING
    /// rows. `returning_result` is wrapped into the `PreparedQuery::Write`
    /// payload and surfaces in `execute()` as `QueryResult::Rows` instead of
    /// the legacy CommandComplete tag.
    fn prepare_write_with_returning(
        &self,
        mutations: Vec<Mutation>,
        tag: &str,
        route: RouteIntent,
        returning_result: Option<QueryResult>,
    ) -> OpenDbResult<PreparedQuery> {
        let record = self.build_next_record(mutations);
        let mut validated_projection = self.projection.clone();
        validated_projection.apply(&record)?;
        Ok(PreparedQuery::Write {
            record,
            tag: tag.to_owned(),
            route,
            returning_result,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn select_all(
        &self,
        table: &str,
        predicates: &[Predicate],
        order_by: Option<&crate::ast::OrderBy>,
        limit: Option<u64>,
        offset: Option<u64>,
        columns: &crate::ast::SelectColumns,
        group_by: &[String],
        having: &[crate::ast::HavingPredicate],
    ) -> OpenDbResult<(QueryResult, RouteIntent)> {
        let table_state = self
            .projection
            .table(table)
            .ok_or_else(|| OpenDbError::NotFound(format!("table not found: {table}")))?;
        let all_columns = table_state.column_names();
        // Sprint 15: aggregated projection takes its own code path because the
        // row shape is determined by the GROUP BY partition, not the table
        // schema.
        if let crate::ast::SelectColumns::Aggregated(projection) = columns {
            return self.select_aggregated(
                table,
                table_state,
                predicates,
                projection,
                group_by,
                having,
                order_by,
                limit,
                offset,
            );
        }
        if !group_by.is_empty() {
            return Err(OpenDbError::Sql(
                "GROUP BY requires an aggregated projection".to_owned(),
            ));
        }
        if !having.is_empty() {
            return Err(OpenDbError::Sql(
                "HAVING requires an aggregated projection".to_owned(),
            ));
        }
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
            crate::ast::SelectColumns::Aggregated(_) => unreachable!("handled above"),
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

    /// Sprint 15: aggregated SELECT. Builds one logical group per distinct
    /// value combination of the `GROUP BY` columns (or a single global group
    /// when `group_by` is empty) and folds each matching row into the
    /// per-group aggregate state.
    #[allow(clippy::too_many_arguments)]
    fn select_aggregated(
        &self,
        table: &str,
        table_state: &opendb_storage::row_projection::Table,
        predicates: &[Predicate],
        projection: &crate::ast::AggregateProjection,
        group_by: &[String],
        having: &[crate::ast::HavingPredicate],
        order_by: Option<&crate::ast::OrderBy>,
        limit: Option<u64>,
        offset: Option<u64>,
    ) -> OpenDbResult<(QueryResult, RouteIntent)> {
        use crate::ast::{AggregateArg, AggregateOrColumn};
        // Sprint 15.C: collect the aggregate expressions referenced by HAVING
        // (deduplicated by structural identity) so we can fold them per group
        // alongside the projection aggregates.
        let having_aggs: Vec<crate::ast::AggregateExpr> = having
            .iter()
            .filter_map(|p| match &p.expr {
                AggregateOrColumn::Aggregate(e) => Some(e.clone()),
                AggregateOrColumn::Column(_) => None,
            })
            .collect();
        let all_columns = table_state.column_names();
        // Sprint 15.F: a qualified `qualifier.col` reference is valid as long
        // as `col` exists on the table; the qualifier check is only enforced
        // in the joined aggregator. We compare by basename here.
        let column_exists = |name: &str| -> bool {
            let basename = column_basename(name);
            all_columns.iter().any(|c| c == name || c == basename)
        };
        // Validate group_by columns + bare projection columns belong to the table
        // and are listed in GROUP BY.
        for g in group_by {
            if !column_exists(g) {
                return Err(OpenDbError::Sql(format!(
                    "GROUP BY column {g} not in table {table}"
                )));
            }
        }
        // Sprint 15.F: a projection column matches a GROUP BY entry by exact
        // name OR by bare-suffix equality (so `status` matches `job_queue.status`
        // and vice versa). Drizzle qualifies the GROUP BY but typically not
        // the projection, or vice versa.
        let in_group_by = |name: &str| -> bool {
            let bare = column_basename(name);
            group_by
                .iter()
                .any(|g| g == name || column_basename(g) == name || g == bare)
        };
        for item in &projection.items {
            if let AggregateOrColumn::Column(name) = &item.expr {
                if !in_group_by(name) {
                    return Err(OpenDbError::Sql(format!(
                        "column {name} must appear in GROUP BY"
                    )));
                }
            }
            if let AggregateOrColumn::Aggregate(expr) = &item.expr {
                if let AggregateArg::Column(name) = &expr.arg {
                    if !column_exists(name) {
                        return Err(OpenDbError::Sql(format!(
                            "aggregate column {name} not in table {table}"
                        )));
                    }
                }
            }
        }
        // Predicate type-check (same gating as scalar SelectAll).
        for predicate in predicates {
            let column = table_state
                .columns
                .iter()
                .find(|c| c.name == predicate.column)
                .ok_or_else(|| {
                    OpenDbError::Sql(format!("column {} not in table {table}", predicate.column))
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

        // Fold matching rows into per-group aggregate state. Empty group_by =>
        // a single global group keyed by an empty string. We key groups by a
        // serialized representation of the GROUP BY value tuple because
        // `Value` does not implement `Ord`. Each group carries two parallel
        // state vectors: one for the projection items, one for the HAVING
        // aggregate expressions (used by Sprint 15.C).
        let mut groups: std::collections::BTreeMap<
            String,
            (Vec<Value>, Vec<AggregateState>, Vec<AggregateState>),
        > = std::collections::BTreeMap::new();
        let mut matched_any = false;
        for row in table_state.rows.values() {
            if !predicates.iter().all(|p| evaluate_predicate(row, p)) {
                continue;
            }
            matched_any = true;
            let key_values: Vec<Value> = group_by.iter().map(|g| row_lookup(row, g)).collect();
            let key_str = group_key_string(&key_values);
            let (_, states, having_states) = groups.entry(key_str).or_insert_with(|| {
                (
                    key_values.clone(),
                    projection
                        .items
                        .iter()
                        .map(|_| AggregateState::new())
                        .collect(),
                    having_aggs.iter().map(|_| AggregateState::new()).collect(),
                )
            });
            for (slot, item) in states.iter_mut().zip(projection.items.iter()) {
                if let AggregateOrColumn::Aggregate(expr) = &item.expr {
                    let value = match &expr.arg {
                        AggregateArg::Star => Value::Int64(1),
                        AggregateArg::Column(name) => row_lookup(row, name),
                    };
                    slot.accumulate(expr.func, &value);
                }
            }
            for (slot, expr) in having_states.iter_mut().zip(having_aggs.iter()) {
                let value = match &expr.arg {
                    AggregateArg::Star => Value::Int64(1),
                    AggregateArg::Column(name) => row_lookup(row, name),
                };
                slot.accumulate(expr.func, &value);
            }
        }

        // Emit one row per group. If group_by is empty AND no rows match, we
        // still return a single row with the aggregate's empty-set semantics
        // (COUNT(*) = 0, others = NULL) — matches PostgreSQL behaviour.
        let mut output_rows: Vec<Vec<Value>> = Vec::with_capacity(groups.len());
        let mut column_names: Vec<String> = projection
            .items
            .iter()
            .enumerate()
            .map(|(idx, item)| aggregate_item_default_name(item, idx))
            .collect();
        let _ = &mut column_names;
        if groups.is_empty() && group_by.is_empty() {
            let states: Vec<AggregateState> = projection
                .items
                .iter()
                .map(|_| AggregateState::new())
                .collect();
            let row = projection
                .items
                .iter()
                .zip(states.iter())
                .map(|(item, state)| match &item.expr {
                    AggregateOrColumn::Column(_) => Value::Null,
                    AggregateOrColumn::Aggregate(expr) => state.finalize(expr.func),
                })
                .collect();
            output_rows.push(row);
            let _ = matched_any;
        } else {
            for (_, (key_values, states, _)) in &groups {
                let row: Vec<Value> = projection
                    .items
                    .iter()
                    .zip(states.iter())
                    .map(|(item, state)| match &item.expr {
                        AggregateOrColumn::Column(name) => {
                            let bare = column_basename(name);
                            let pos = group_by
                                .iter()
                                .position(|g| g == name || column_basename(g) == name || g == bare)
                                .expect("validated above");
                            key_values.get(pos).cloned().unwrap_or(Value::Null)
                        }
                        AggregateOrColumn::Aggregate(expr) => state.finalize(expr.func),
                    })
                    .collect();
                output_rows.push(row);
            }
        }

        // Sprint 15.C: HAVING filters AFTER aggregation. Each predicate's LHS
        // is evaluated against the per-group `having_states` slot computed
        // during the main fold (or against the GROUP BY key for bare-column
        // predicates).
        if !having.is_empty() {
            let groups_vec: Vec<(
                &String,
                &(Vec<Value>, Vec<AggregateState>, Vec<AggregateState>),
            )> = groups.iter().collect();
            let mut kept: Vec<Vec<Value>> = Vec::with_capacity(output_rows.len());
            if group_by.is_empty() && groups.is_empty() {
                let empty_having_states: Vec<AggregateState> =
                    having_aggs.iter().map(|_| AggregateState::new()).collect();
                if output_rows.len() == 1
                    && having_matches(having, &having_aggs, group_by, &[], &empty_having_states)
                {
                    kept.push(output_rows[0].clone());
                }
            } else {
                for (row, (_, (key_values, _, having_states))) in
                    output_rows.iter().zip(groups_vec.iter())
                {
                    if having_matches(having, &having_aggs, group_by, key_values, having_states) {
                        kept.push(row.clone());
                    }
                }
            }
            output_rows = kept;
        }

        // Optional ORDER BY against the aggregated column names.
        if let Some(ob) = order_by {
            let pos = column_names
                .iter()
                .position(|c| c == &ob.column)
                .ok_or_else(|| {
                    OpenDbError::Sql(format!(
                        "ORDER BY column {} not in aggregated projection",
                        ob.column
                    ))
                })?;
            output_rows.sort_by(|l, r| {
                let lv = l.get(pos).cloned().unwrap_or(Value::Null);
                let rv = r.get(pos).cloned().unwrap_or(Value::Null);
                let ord = compare_values(&lv, &rv);
                match ob.direction {
                    crate::ast::OrderDirection::Asc => ord,
                    crate::ast::OrderDirection::Desc => ord.reverse(),
                }
            });
        }
        let offset_count = offset.unwrap_or(0) as usize;
        let limit_count = limit.unwrap_or(u64::MAX) as usize;
        let final_rows: Vec<Vec<Value>> = output_rows
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
            RouteIntent::Scan {
                table: table.to_owned(),
            },
        ))
    }
}

/// Sprint 15: per-aggregate-slot running state. We keep a small Sum-of-Int /
/// Sum-of-Float / Count / Max / Min view; the right field is picked at
/// `finalize` time based on the aggregate function.
#[derive(Clone, Debug, Default)]
struct AggregateState {
    count: i64,
    sum_int: i128,
    sum_float: f64,
    sum_float_seen: bool,
    sum_int_seen: bool,
    max: Option<Value>,
    min: Option<Value>,
    any_non_null: bool,
}

impl AggregateState {
    fn new() -> Self {
        Self::default()
    }

    fn accumulate(&mut self, func: crate::ast::AggregateFunction, value: &Value) {
        use crate::ast::AggregateFunction as F;
        match func {
            F::Count => {
                if !matches!(value, Value::Null) {
                    self.count += 1;
                }
            }
            F::Sum | F::Avg => {
                match value {
                    Value::Int64(n) => {
                        self.sum_int += *n as i128;
                        self.sum_int_seen = true;
                        self.count += 1;
                        self.any_non_null = true;
                    }
                    Value::Float64(f) => {
                        self.sum_float += *f;
                        self.sum_float_seen = true;
                        self.count += 1;
                        self.any_non_null = true;
                    }
                    Value::Null => {}
                    _ => {
                        // Non-numeric: ignore (PG would error; we choose lenient
                        // semantics for now and emit Null at finalize).
                    }
                }
            }
            F::Max => {
                if !matches!(value, Value::Null) {
                    self.any_non_null = true;
                    self.max = Some(match &self.max {
                        None => value.clone(),
                        Some(current) => {
                            if compare_values(value, current).is_gt() {
                                value.clone()
                            } else {
                                current.clone()
                            }
                        }
                    });
                }
            }
            F::Min => {
                if !matches!(value, Value::Null) {
                    self.any_non_null = true;
                    self.min = Some(match &self.min {
                        None => value.clone(),
                        Some(current) => {
                            if compare_values(value, current).is_lt() {
                                value.clone()
                            } else {
                                current.clone()
                            }
                        }
                    });
                }
            }
        }
    }

    fn finalize(&self, func: crate::ast::AggregateFunction) -> Value {
        use crate::ast::AggregateFunction as F;
        match func {
            F::Count => Value::Int64(self.count),
            F::Sum => {
                if self.sum_float_seen {
                    Value::Float64(self.sum_float + self.sum_int as f64)
                } else if self.sum_int_seen {
                    Value::Int64(self.sum_int as i64)
                } else {
                    Value::Null
                }
            }
            F::Avg => {
                if self.count == 0 {
                    Value::Null
                } else {
                    let total = self.sum_float + self.sum_int as f64;
                    Value::Float64(total / self.count as f64)
                }
            }
            F::Max => self.max.clone().unwrap_or(Value::Null),
            F::Min => self.min.clone().unwrap_or(Value::Null),
        }
    }
}

/// Sprint 15.F: smart column lookup for non-joined rows. Tries the exact
/// name first, then strips a `qualifier.` prefix and retries. Lets the
/// aggregate fold accept Drizzle-style `"table"."col"` references against
/// bare-keyed row maps.
fn row_lookup(row: &std::collections::BTreeMap<String, Value>, name: &str) -> Value {
    if let Some(v) = row.get(name) {
        return v.clone();
    }
    if let Some(idx) = name.find('.') {
        let bare = &name[idx + 1..];
        if let Some(v) = row.get(bare) {
            return v.clone();
        }
    }
    Value::Null
}

/// Sprint 15.F: bare column name from a qualified `qualifier.column` form.
/// Returns the input unchanged if there's no `.` separator.
fn column_basename(qualified: &str) -> &str {
    if let Some(idx) = qualified.rfind('.') {
        &qualified[idx + 1..]
    } else {
        qualified
    }
}

/// Sprint 15.F: shared finalization for joined SELECT — applies ORDER BY,
/// LIMIT, OFFSET to a (columns, rows) pair and wraps in a `QueryResult`.
fn finish_joined(
    columns: Vec<String>,
    mut rows: Vec<Vec<Value>>,
    order_by: Option<&crate::ast::JoinedOrderBy>,
    limit: Option<u64>,
    offset: Option<u64>,
    left_table: String,
) -> OpenDbResult<(QueryResult, RouteIntent)> {
    if let Some(ob) = order_by {
        let pos = columns
            .iter()
            .position(|c| {
                c == &ob.column
                    || column_basename(c) == ob.column.as_str()
                    || ob
                        .qualifier
                        .as_deref()
                        .map(|q| format!("{}.{}", q, ob.column) == *c)
                        .unwrap_or(false)
            })
            .ok_or_else(|| {
                OpenDbError::Sql(format!(
                    "ORDER BY column {} not in joined projection",
                    ob.column
                ))
            })?;
        rows.sort_by(|l, r| {
            let lv = l.get(pos).cloned().unwrap_or(Value::Null);
            let rv = r.get(pos).cloned().unwrap_or(Value::Null);
            let ordering = compare_values(&lv, &rv);
            match ob.direction {
                crate::ast::OrderDirection::Asc => ordering,
                crate::ast::OrderDirection::Desc => ordering.reverse(),
            }
        });
    }
    let off = offset.unwrap_or(0) as usize;
    let lim = limit.unwrap_or(u64::MAX) as usize;
    let final_rows: Vec<_> = rows.into_iter().skip(off).take(lim).collect();
    Ok((
        QueryResult::Rows {
            columns,
            column_types: Vec::new(),
            rows: final_rows,
        },
        RouteIntent::Scan { table: left_table },
    ))
}

/// Sprint 15.F: per-group aggregation over already-joined rows. Reuses the
/// same `AggregateState` machinery as the simple-table aggregator. Column
/// references in the projection / GROUP BY / HAVING are resolved against the
/// joined output's qualified column names with bare-name fallback.
#[allow(clippy::too_many_arguments)]
fn aggregate_joined_rows(
    left_table: &str,
    output_columns: &[String],
    joined_rows: &[Vec<Value>],
    projection: &crate::ast::AggregateProjection,
    group_by: &[String],
    having: &[crate::ast::HavingPredicate],
    order_by: Option<&crate::ast::JoinedOrderBy>,
    limit: Option<u64>,
    offset: Option<u64>,
) -> OpenDbResult<(QueryResult, RouteIntent)> {
    use crate::ast::{AggregateArg, AggregateOrColumn};
    // Resolve a projection / group-by column name against the joined output:
    // try exact qualified match, then bare-suffix match.
    let resolve = |name: &str| -> Option<usize> {
        output_columns
            .iter()
            .position(|c| c == name || column_basename(c) == name)
    };
    // Validate group_by + projection columns exist.
    for g in group_by {
        if resolve(g).is_none() {
            return Err(OpenDbError::Sql(format!(
                "GROUP BY column {g} not found in joined projection"
            )));
        }
    }
    let in_group_by = |name: &str| -> bool {
        let bare = column_basename(name);
        group_by
            .iter()
            .any(|g| g == name || column_basename(g) == name || g == bare)
    };
    for item in &projection.items {
        match &item.expr {
            AggregateOrColumn::Column(name) => {
                if !in_group_by(name) {
                    return Err(OpenDbError::Sql(format!(
                        "column {name} must appear in GROUP BY"
                    )));
                }
                if resolve(name).is_none() {
                    return Err(OpenDbError::Sql(format!(
                        "column {name} not found in joined projection"
                    )));
                }
            }
            AggregateOrColumn::Aggregate(expr) => {
                if let AggregateArg::Column(name) = &expr.arg {
                    if resolve(name).is_none() {
                        return Err(OpenDbError::Sql(format!(
                            "aggregate column {name} not found in joined projection"
                        )));
                    }
                }
            }
        }
    }

    // Pre-resolve indices so the inner loop is O(rows * items).
    let group_by_idx: Vec<usize> = group_by.iter().map(|g| resolve(g).unwrap()).collect();
    let item_arg_idx: Vec<Option<usize>> = projection
        .items
        .iter()
        .map(|item| match &item.expr {
            AggregateOrColumn::Column(name) => resolve(name),
            AggregateOrColumn::Aggregate(expr) => match &expr.arg {
                AggregateArg::Star => None,
                AggregateArg::Column(name) => resolve(name),
            },
        })
        .collect();
    let having_aggs: Vec<crate::ast::AggregateExpr> = having
        .iter()
        .filter_map(|p| match &p.expr {
            AggregateOrColumn::Aggregate(e) => Some(e.clone()),
            AggregateOrColumn::Column(_) => None,
        })
        .collect();
    let having_arg_idx: Vec<Option<usize>> = having_aggs
        .iter()
        .map(|e| match &e.arg {
            AggregateArg::Star => None,
            AggregateArg::Column(name) => resolve(name),
        })
        .collect();

    let mut groups: std::collections::BTreeMap<
        String,
        (Vec<Value>, Vec<AggregateState>, Vec<AggregateState>),
    > = std::collections::BTreeMap::new();
    for row in joined_rows {
        let key_values: Vec<Value> = group_by_idx
            .iter()
            .map(|i| row.get(*i).cloned().unwrap_or(Value::Null))
            .collect();
        let key_str = group_key_string(&key_values);
        let (_, states, having_states) = groups.entry(key_str).or_insert_with(|| {
            (
                key_values.clone(),
                projection
                    .items
                    .iter()
                    .map(|_| AggregateState::new())
                    .collect(),
                having_aggs.iter().map(|_| AggregateState::new()).collect(),
            )
        });
        for (slot_idx, item) in projection.items.iter().enumerate() {
            if let AggregateOrColumn::Aggregate(expr) = &item.expr {
                let value = match (&expr.arg, item_arg_idx[slot_idx]) {
                    (AggregateArg::Star, _) => Value::Int64(1),
                    (AggregateArg::Column(_), Some(idx)) => {
                        row.get(idx).cloned().unwrap_or(Value::Null)
                    }
                    (AggregateArg::Column(_), None) => Value::Null,
                };
                states[slot_idx].accumulate(expr.func, &value);
            }
        }
        for (slot_idx, expr) in having_aggs.iter().enumerate() {
            let value = match (&expr.arg, having_arg_idx[slot_idx]) {
                (AggregateArg::Star, _) => Value::Int64(1),
                (AggregateArg::Column(_), Some(idx)) => {
                    row.get(idx).cloned().unwrap_or(Value::Null)
                }
                (AggregateArg::Column(_), None) => Value::Null,
            };
            having_states[slot_idx].accumulate(expr.func, &value);
        }
    }

    // Emit one row per group (or a single empty-set row when no GROUP BY).
    let mut output_rows: Vec<Vec<Value>> = Vec::with_capacity(groups.len());
    let column_names: Vec<String> = projection
        .items
        .iter()
        .enumerate()
        .map(|(idx, item)| aggregate_item_default_name(item, idx))
        .collect();
    if groups.is_empty() && group_by.is_empty() {
        let row = projection
            .items
            .iter()
            .map(|item| match &item.expr {
                AggregateOrColumn::Column(_) => Value::Null,
                AggregateOrColumn::Aggregate(expr) => AggregateState::new().finalize(expr.func),
            })
            .collect();
        output_rows.push(row);
    } else {
        for (_, (key_values, states, having_states)) in &groups {
            if !having_matches(having, &having_aggs, group_by, key_values, having_states) {
                continue;
            }
            let row: Vec<Value> = projection
                .items
                .iter()
                .zip(states.iter())
                .map(|(item, state)| match &item.expr {
                    AggregateOrColumn::Column(name) => {
                        let bare = column_basename(name);
                        let pos = group_by
                            .iter()
                            .position(|g| g == name || column_basename(g) == name || g == bare)
                            .expect("validated above");
                        key_values.get(pos).cloned().unwrap_or(Value::Null)
                    }
                    AggregateOrColumn::Aggregate(expr) => state.finalize(expr.func),
                })
                .collect();
            output_rows.push(row);
        }
    }

    finish_joined(
        column_names,
        output_rows,
        order_by,
        limit,
        offset,
        left_table.to_owned(),
    )
}

/// Sprint 15.C: evaluate the conjunction of HAVING predicates against a
/// single group. `having_aggs` and `having_states` are aligned: index `i` in
/// `having_aggs` matches state slot `i` (built during the main fold).
fn having_matches(
    having: &[crate::ast::HavingPredicate],
    having_aggs: &[crate::ast::AggregateExpr],
    group_by: &[String],
    key_values: &[Value],
    having_states: &[AggregateState],
) -> bool {
    use crate::ast::AggregateOrColumn;
    let mut agg_cursor = 0usize;
    for pred in having {
        let lhs_value = match &pred.expr {
            AggregateOrColumn::Column(name) => {
                let bare = column_basename(name);
                let Some(pos) = group_by
                    .iter()
                    .position(|g| g == name || column_basename(g) == name || g == bare)
                else {
                    return false;
                };
                key_values.get(pos).cloned().unwrap_or(Value::Null)
            }
            AggregateOrColumn::Aggregate(_expr) => {
                let slot = having_states
                    .get(agg_cursor)
                    .expect("having_states aligned with having_aggs by construction");
                let func = having_aggs
                    .get(agg_cursor)
                    .expect("having_aggs aligned with predicates")
                    .func;
                agg_cursor += 1;
                slot.finalize(func)
            }
        };
        if !compare_where_op(&lhs_value, pred.op.clone(), &pred.value) {
            return false;
        }
    }
    true
}

/// Sprint 15.C: thin wrapper that maps a `WhereOp` over the (LHS, RHS) value
/// pair. Subset of `evaluate_predicate` re-used for HAVING.
fn compare_where_op(lhs: &Value, op: crate::ast::WhereOp, rhs: &Value) -> bool {
    use crate::ast::WhereOp as W;
    match op {
        W::Eq => lhs == rhs,
        W::NotEq => lhs != rhs,
        W::Lt => compare_values(lhs, rhs).is_lt(),
        W::Lte => compare_values(lhs, rhs).is_le(),
        W::Gt => compare_values(lhs, rhs).is_gt(),
        W::Gte => compare_values(lhs, rhs).is_ge(),
        W::In(list) => list.iter().any(|v| v == lhs),
        W::IsNull => matches!(lhs, Value::Null),
        W::IsNotNull => !matches!(lhs, Value::Null),
    }
}

/// Sprint 15: build a deterministic ordering-safe string key for a tuple of
/// GROUP BY values (since `Value` doesn't implement `Ord`).
fn group_key_string(values: &[Value]) -> String {
    let mut out = String::new();
    for v in values {
        match v {
            Value::Null => out.push_str("N|"),
            Value::Bool(b) => {
                out.push('B');
                out.push(if *b { 'T' } else { 'F' });
                out.push('|');
            }
            Value::Int64(n) => {
                out.push('I');
                out.push_str(&n.to_string());
                out.push('|');
            }
            Value::Float64(f) => {
                out.push('F');
                out.push_str(&f.to_bits().to_string());
                out.push('|');
            }
            Value::Text(s) => {
                out.push('S');
                out.push_str(&s.len().to_string());
                out.push(':');
                out.push_str(s);
                out.push('|');
            }
            Value::Timestamp(n) => {
                out.push('T');
                out.push_str(&n.to_string());
                out.push('|');
            }
            Value::Json(s) => {
                let serialized = s.to_string();
                out.push('J');
                out.push_str(&serialized.len().to_string());
                out.push(':');
                out.push_str(&serialized);
                out.push('|');
            }
        }
    }
    out
}

/// Sprint 15: default column name for an aggregated projection slot.
fn aggregate_item_default_name(item: &crate::ast::AggregateSelectItem, index: usize) -> String {
    if let Some(alias) = &item.alias {
        return alias.clone();
    }
    use crate::ast::{AggregateArg, AggregateFunction as F, AggregateOrColumn};
    match &item.expr {
        AggregateOrColumn::Column(name) => name.clone(),
        AggregateOrColumn::Aggregate(expr) => {
            let func = match expr.func {
                F::Count => "count",
                F::Sum => "sum",
                F::Max => "max",
                F::Min => "min",
                F::Avg => "avg",
            };
            let _ = index;
            match &expr.arg {
                AggregateArg::Star => func.to_owned(),
                AggregateArg::Column(_) => func.to_owned(),
            }
        }
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
                    Value::Timestamp(wall_clock_microseconds()),
                    "now".to_owned(),
                ),
                SelectExpr::Function(SelectFunction::CurrentTimestamp) => (
                    Value::Timestamp(wall_clock_microseconds()),
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
        columns: crate::ast::SelectColumns,
        group_by: Vec<String>,
        having: Vec<crate::ast::HavingPredicate>,
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
                if !values_join_match(&left_join_value, &right_join_value) {
                    continue;
                }
                // Sprint 15.F: ON-clause extra predicates filter the right
                // row. They typically reference the right table only
                // (`right.col = literal`); LEFT JOIN semantics treat a
                // failed extra-pred match as a non-match (left row stays,
                // right side becomes NULL).
                if !join.extra.iter().all(|pred| {
                    let target = match pred.qualifier.as_deref() {
                        Some(q) if q == join.right => right_row,
                        Some(q) if q == left_table => left_row,
                        _ => right_row,
                    };
                    target
                        .get(&pred.column)
                        .map(|value| values_join_match(value, &pred.value))
                        .unwrap_or(false)
                }) {
                    continue;
                }
                matched = true;
                let projected =
                    project_joined_row(&left_columns, left_row, &right_columns, Some(right_row));
                joined_rows.push(projected);
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

        // Sprint 15.F: aggregated projection over the joined rows.
        if let crate::ast::SelectColumns::Aggregated(projection) = &columns {
            return aggregate_joined_rows(
                &left_table,
                &output_columns,
                &joined_rows,
                projection,
                &group_by,
                &having,
                order_by.as_ref(),
                limit,
                offset,
            );
        }
        if !group_by.is_empty() || !having.is_empty() {
            return Err(OpenDbError::Sql(
                "GROUP BY / HAVING require an aggregated projection".to_owned(),
            ));
        }

        // Sprint 15.F: explicit-column projection on a joined SELECT. The
        // returned columns and rows are pruned to only the requested set.
        if let crate::ast::SelectColumns::Explicit(requested) = &columns {
            let mut indices: Vec<usize> = Vec::with_capacity(requested.len());
            for name in requested {
                let pos = output_columns
                    .iter()
                    .position(|c| c == name || column_basename(c) == name.as_str())
                    .ok_or_else(|| {
                        OpenDbError::Sql(format!("column {name} not found in joined projection"))
                    })?;
                indices.push(pos);
            }
            let projected_columns: Vec<String> = indices
                .iter()
                .map(|i| column_basename(&output_columns[*i]).to_owned())
                .collect();
            joined_rows = joined_rows
                .into_iter()
                .map(|row| {
                    indices
                        .iter()
                        .map(|i| row.get(*i).cloned().unwrap_or(Value::Null))
                        .collect()
                })
                .collect();
            // Replace output_columns with the projected list for downstream
            // ORDER BY / LIMIT / OFFSET / serialization.
            let _ = output_columns;
            return finish_joined(
                projected_columns,
                joined_rows,
                order_by.as_ref(),
                limit,
                offset,
                left_table,
            );
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
    match &predicate.op {
        WhereOp::Eq => value == predicate.value,
        WhereOp::NotEq => value != predicate.value,
        WhereOp::Lt => ordering == std::cmp::Ordering::Less,
        WhereOp::Lte => ordering != std::cmp::Ordering::Greater,
        WhereOp::Gt => ordering == std::cmp::Ordering::Greater,
        WhereOp::Gte => ordering != std::cmp::Ordering::Less,
        WhereOp::In(values) => values.iter().any(|v| v == &value),
        WhereOp::IsNull => matches!(value, Value::Null),
        WhereOp::IsNotNull => !matches!(value, Value::Null),
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

/// Sprint 16.A: build a `QueryResult::Rows` for an INSERT-style RETURNING
/// from a single freshly-materialized row (`columns` + `column_values` are
/// aligned). For UPDATE/DELETE, see `project_returning_rows`.
fn project_returning(
    columns: &[ColumnDefinition],
    column_values: &[ColumnValue],
    spec: &crate::ast::ReturningClause,
) -> OpenDbResult<QueryResult> {
    let row: std::collections::BTreeMap<String, Value> = column_values
        .iter()
        .map(|cv| (cv.column.clone(), cv.value.clone()))
        .collect();
    let column_names = column_names_for_returning(columns, spec)?;
    let row_values: Vec<Value> = column_names
        .iter()
        .map(|name| row.get(name).cloned().unwrap_or(Value::Null))
        .collect();
    Ok(QueryResult::Rows {
        columns: column_names,
        column_types: Vec::new(),
        rows: vec![row_values],
    })
}

/// Sprint 16.B: build a `QueryResult::Rows` for UPDATE/DELETE RETURNING from
/// a vector of post-/pre-mutation row maps.
fn project_returning_rows(
    columns: &[ColumnDefinition],
    rows: &[std::collections::BTreeMap<String, Value>],
    spec: &crate::ast::ReturningClause,
) -> OpenDbResult<QueryResult> {
    let column_names = column_names_for_returning(columns, spec)?;
    let materialized: Vec<Vec<Value>> = rows
        .iter()
        .map(|row| {
            column_names
                .iter()
                .map(|name| row.get(name).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();
    Ok(QueryResult::Rows {
        columns: column_names,
        column_types: Vec::new(),
        rows: materialized,
    })
}

fn column_names_for_returning(
    columns: &[ColumnDefinition],
    spec: &crate::ast::ReturningClause,
) -> OpenDbResult<Vec<String>> {
    match spec {
        crate::ast::ReturningClause::Star => Ok(columns.iter().map(|c| c.name.clone()).collect()),
        crate::ast::ReturningClause::Columns(names) => {
            // Drizzle qualifies RETURNING entries (`"folders"."id"`); accept
            // bare-name suffix match against the table schema.
            let mut resolved = Vec::with_capacity(names.len());
            for name in names {
                let bare = column_basename(name);
                let exists = columns
                    .iter()
                    .any(|c| c.name == name.as_str() || c.name == bare);
                if !exists {
                    return Err(OpenDbError::Sql(format!(
                        "RETURNING column {name} not in table"
                    )));
                }
                resolved.push(bare.to_owned());
            }
            Ok(resolved)
        }
    }
}

/// Sprint 15.G: real wall-clock microseconds-since-epoch for `DEFAULT NOW()`
/// on omitted INSERT columns. Falls back to 0 if SystemTime panics, which is
/// only theoretical (e.g., a clock running before UNIX_EPOCH).
fn wall_clock_microseconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
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
    fn select_count_star_returns_total_rows() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, label TEXT)").expect("parse"))
            .expect("create");
        for (id, label) in [(1, "a"), (2, "b"), (3, "c")] {
            engine
                .execute(parse(&format!("INSERT INTO t VALUES ({id}, '{label}')")).expect("parse"))
                .expect("insert");
        }
        let result = engine
            .execute(parse("SELECT count(*) FROM t").expect("parse"))
            .expect("count");
        let QueryResult::Rows { columns, rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(columns, vec!["count".to_owned()]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int64(3));
    }

    #[test]
    fn select_count_star_with_where_filters_before_counting() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, status TEXT)").expect("parse"))
            .expect("create");
        for (id, status) in [(1, "open"), (2, "open"), (3, "closed")] {
            engine
                .execute(parse(&format!("INSERT INTO t VALUES ({id}, '{status}')")).expect("parse"))
                .expect("insert");
        }
        let result = engine
            .execute(parse("SELECT count(*) FROM t WHERE status = 'open'").expect("parse"))
            .expect("count");
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(rows[0][0], Value::Int64(2));
    }

    #[test]
    fn select_count_star_group_by_partitions() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, status TEXT)").expect("parse"))
            .expect("create");
        for (id, status) in [
            (1, "open"),
            (2, "open"),
            (3, "closed"),
            (4, "closed"),
            (5, "open"),
        ] {
            engine
                .execute(parse(&format!("INSERT INTO t VALUES ({id}, '{status}')")).expect("parse"))
                .expect("insert");
        }
        let result = engine
            .execute(parse("SELECT status, count(*) FROM t GROUP BY status").expect("parse"))
            .expect("count by status");
        let QueryResult::Rows { columns, rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(columns, vec!["status".to_owned(), "count".to_owned()]);
        assert_eq!(rows.len(), 2);
        // Sorted by group_key_string("S{n}:open" < "S{n}:closed" lexicographic).
        // Just verify the values regardless of order.
        let mut counts = std::collections::HashMap::new();
        for row in &rows {
            let Value::Text(status) = &row[0] else {
                panic!("expected text status");
            };
            let Value::Int64(c) = &row[1] else {
                panic!("expected int count");
            };
            counts.insert(status.clone(), *c);
        }
        assert_eq!(counts.get("open"), Some(&3));
        assert_eq!(counts.get("closed"), Some(&2));
    }

    #[test]
    fn select_group_by_multi_column() {
        let mut engine = SqlEngine::default();
        engine
            .execute(
                parse("CREATE TABLE t (id INT PRIMARY KEY, region TEXT, status TEXT)")
                    .expect("parse"),
            )
            .expect("create");
        for (id, region, status) in [
            (1, "EU", "open"),
            (2, "EU", "open"),
            (3, "EU", "closed"),
            (4, "US", "open"),
            (5, "US", "closed"),
            (6, "US", "closed"),
        ] {
            engine
                .execute(
                    parse(&format!(
                        "INSERT INTO t VALUES ({id}, '{region}', '{status}')"
                    ))
                    .expect("parse"),
                )
                .expect("insert");
        }
        let result = engine
            .execute(
                parse("SELECT region, status, count(*) FROM t GROUP BY region, status")
                    .expect("parse"),
            )
            .expect("agg");
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(rows.len(), 4);
        let mut by_key: std::collections::HashMap<(String, String), i64> =
            std::collections::HashMap::new();
        for row in &rows {
            let Value::Text(region) = &row[0] else {
                panic!()
            };
            let Value::Text(status) = &row[1] else {
                panic!()
            };
            let Value::Int64(c) = &row[2] else { panic!() };
            by_key.insert((region.clone(), status.clone()), *c);
        }
        assert_eq!(by_key.get(&("EU".to_owned(), "open".to_owned())), Some(&2));
        assert_eq!(
            by_key.get(&("EU".to_owned(), "closed".to_owned())),
            Some(&1)
        );
        assert_eq!(by_key.get(&("US".to_owned(), "open".to_owned())), Some(&1));
        assert_eq!(
            by_key.get(&("US".to_owned(), "closed".to_owned())),
            Some(&2)
        );
    }

    #[test]
    fn select_having_filters_groups_post_aggregation() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, status TEXT)").expect("parse"))
            .expect("create");
        for (id, status) in [(1, "open"), (2, "open"), (3, "open"), (4, "closed")] {
            engine
                .execute(parse(&format!("INSERT INTO t VALUES ({id}, '{status}')")).expect("parse"))
                .expect("insert");
        }
        let result = engine
            .execute(
                parse("SELECT status, count(*) FROM t GROUP BY status HAVING count(*) > 1")
                    .expect("parse"),
            )
            .expect("having");
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Text("open".to_owned()));
        assert_eq!(rows[0][1], Value::Int64(3));
    }

    #[test]
    fn select_sum_max_min_avg_global() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE orders (id INT PRIMARY KEY, amount INT)").expect("parse"))
            .expect("create");
        for (id, amount) in [(1, 10), (2, 20), (3, 30), (4, 40)] {
            engine
                .execute(
                    parse(&format!("INSERT INTO orders VALUES ({id}, {amount})")).expect("parse"),
                )
                .expect("insert");
        }
        let result = engine
            .execute(
                parse("SELECT sum(amount), max(amount), min(amount), avg(amount) FROM orders")
                    .expect("parse"),
            )
            .expect("agg");
        let QueryResult::Rows { columns, rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(
            columns,
            vec![
                "sum".to_owned(),
                "max".to_owned(),
                "min".to_owned(),
                "avg".to_owned()
            ]
        );
        assert_eq!(rows[0][0], Value::Int64(100));
        assert_eq!(rows[0][1], Value::Int64(40));
        assert_eq!(rows[0][2], Value::Int64(10));
        assert_eq!(rows[0][3], Value::Float64(25.0));
    }

    #[test]
    fn select_sum_group_by_with_alias() {
        let mut engine = SqlEngine::default();
        engine
            .execute(
                parse("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")
                    .expect("parse"),
            )
            .expect("create");
        for (id, uid, amount) in [(1, 1, 10), (2, 1, 5), (3, 2, 100)] {
            engine
                .execute(
                    parse(&format!(
                        "INSERT INTO orders VALUES ({id}, {uid}, {amount})"
                    ))
                    .expect("parse"),
                )
                .expect("insert");
        }
        let result = engine
            .execute(
                parse("SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id")
                    .expect("parse"),
            )
            .expect("agg");
        let QueryResult::Rows { columns, rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(columns, vec!["user_id".to_owned(), "total".to_owned()]);
        assert_eq!(rows.len(), 2);
        let mut by_uid = std::collections::HashMap::new();
        for row in &rows {
            let Value::Int64(uid) = row[0] else { panic!() };
            let Value::Int64(t) = row[1] else { panic!() };
            by_uid.insert(uid, t);
        }
        assert_eq!(by_uid.get(&1), Some(&15));
        assert_eq!(by_uid.get(&2), Some(&100));
    }

    #[test]
    fn select_count_star_on_empty_returns_zero() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY)").expect("parse"))
            .expect("create");
        let result = engine
            .execute(parse("SELECT count(*) FROM t").expect("parse"))
            .expect("count");
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(rows, vec![vec![Value::Int64(0)]]);
    }

    #[test]
    fn select_where_in_and_is_null() {
        let mut engine = SqlEngine::default();
        engine
            .execute(parse("CREATE TABLE t (id INT PRIMARY KEY, label TEXT)").expect("parse"))
            .expect("create");
        for (id, label) in [(1, "a"), (2, "b"), (3, "c")] {
            engine
                .execute(parse(&format!("INSERT INTO t VALUES ({id}, '{label}')")).expect("parse"))
                .expect("insert");
        }
        engine
            .execute(parse("INSERT INTO t (id, label) VALUES (4, NULL)").expect("parse"))
            .expect("insert null");
        let result = engine
            .execute(parse("SELECT * FROM t WHERE id IN (1, 3)").expect("parse"))
            .expect("in");
        let QueryResult::Rows { rows, .. } = result else {
            panic!("expected Rows");
        };
        assert_eq!(rows.len(), 2);
        let null_result = engine
            .execute(parse("SELECT * FROM t WHERE label IS NULL").expect("parse"))
            .expect("is null");
        let QueryResult::Rows { rows, .. } = null_result else {
            panic!("expected Rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int64(4));
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

        let PreparedQuery::Write {
            record, tag, route, ..
        } = prepared_create
        else {
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
    fn named_insert_applies_default_now_as_wall_clock_timestamp() {
        // Sprint 15.G: DEFAULT NOW() now resolves to a real wall-clock
        // microseconds-since-epoch (was previously the tx counter, which
        // showed up as 1970-01-01 in client outputs). Assert the value is
        // within a reasonable window of "right now".
        let before_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
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
        let after_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);

        let last = engine.commits().last().expect("commit");
        let Mutation::InsertRow { values, .. } = &last.mutations[0] else {
            panic!("expected InsertRow");
        };
        let created_at = values
            .iter()
            .find(|cv| cv.column == "created_at")
            .expect("created_at column");
        let Value::Timestamp(t) = created_at.value else {
            panic!("expected Timestamp value, got {:?}", created_at.value);
        };
        assert!(
            t >= before_micros && t <= after_micros,
            "DEFAULT NOW() value {t} not in [{before_micros}, {after_micros}]"
        );
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
