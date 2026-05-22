use opendb_storage::commit_stream::{
    AlterTableOp, ColumnDefinition, ColumnType, IndexDescriptor, Value,
};

// `column_types` is currently always emitted as `Vec::new()` by the engine;
// pgwire derives the row-description OIDs from the first row instead. This
// will become non-empty in a later sprint when the SQL layer carries column
// types end to end.
//
// Keeping the field on `QueryResult::Rows` (not behind an `Option`) avoids
// churn on every test that pattern-matches the variant.

#[derive(Clone, Debug, PartialEq)]
pub struct Predicate {
    pub column: String,
    pub value: Value,
    /// Sprint 14: comparison operator. Defaults to `Eq` to keep legacy
    /// `WHERE col = lit` callers unchanged.
    #[doc(hidden)]
    pub op: WhereOp,
}

impl Predicate {
    pub fn eq(column: String, value: Value) -> Self {
        Self {
            column,
            value,
            op: WhereOp::Eq,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum WhereOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    /// Sprint 14.C: `col IN (v1, v2, ...)`. The literal list lives inline.
    In(Vec<Value>),
    /// Sprint 14.C: `col IS NULL`.
    IsNull,
    /// Sprint 14.C: `col IS NOT NULL`.
    IsNotNull,
}

/// Sprint 14: composite WHERE clause joined by AND. A single-element vector
/// is the common case (legacy `WHERE col = lit`).
#[derive(Clone, Debug, PartialEq, Default)]
pub struct WhereClause {
    pub conjunction: Vec<Predicate>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OrderBy {
    pub column: String,
    pub direction: OrderDirection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    CreateTable {
        table: String,
        columns: Vec<ColumnDefinition>,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Value>,
        /// Sprint 16.A: optional `RETURNING ...` clause. `None` keeps the
        /// legacy command-tag-only behavior.
        returning: Option<ReturningClause>,
    },
    SelectAll {
        table: String,
        /// Sprint 14: conjunctive WHERE clause. Empty vector = no WHERE.
        /// `predicate.first()` keeps a friendly accessor for the legacy
        /// single-predicate path.
        predicate: Vec<Predicate>,
        #[doc = "Sprint 10: optional ORDER BY clause."]
        order_by: Option<OrderBy>,
        #[doc = "Sprint 10: optional LIMIT clause."]
        limit: Option<u64>,
        #[doc = "Sprint 10: optional OFFSET clause."]
        offset: Option<u64>,
        #[doc = "Sprint 12.1: explicit column projection (`SELECT a, b FROM t`)."]
        columns: SelectColumns,
        #[doc = "Sprint 15: GROUP BY <col1>[, ...]. Empty vector = no GROUP BY."]
        group_by: Vec<String>,
        #[doc = "Sprint 15.C: HAVING <agg-predicate> [AND ...]. Empty = no HAVING."]
        having: Vec<HavingPredicate>,
    },
    SelectExpr {
        items: Vec<SelectExprItem>,
    },
    AlterTable {
        table: String,
        op: AlterTableOp,
    },
    CreateIndex {
        index: IndexDescriptor,
        table: String,
    },
    DoBlock {
        inner: Vec<Statement>,
        swallow_duplicate: bool,
    },
    DeleteRow {
        table: String,
        key: String,
        returning: Option<ReturningClause>,
    },
    UpdateRow {
        table: String,
        key: String,
        assignments: Vec<(String, Value)>,
        returning: Option<ReturningClause>,
    },
    /// Sprint 14.D: multi-row DELETE with a conjunctive WHERE clause.
    DeleteWhere {
        table: String,
        predicate: Vec<Predicate>,
        returning: Option<ReturningClause>,
    },
    /// Sprint 14.D: multi-row UPDATE with a conjunctive WHERE clause.
    UpdateWhere {
        table: String,
        predicate: Vec<Predicate>,
        assignments: Vec<(String, Value)>,
        returning: Option<ReturningClause>,
    },
    Select {
        left: String,
        /// Sprint 18.C.1: a chain of one or more JOINs applied left-to-right.
        /// Index 0 joins `left` with `join.right`; index 1 joins the result
        /// with `joins[1].right`; etc. Drizzle migrations and route handlers
        /// frequently emit `T1 LEFT JOIN T2 ... LEFT JOIN T3 ...`.
        joins: Vec<JoinClause>,
        /// Sprint 18.C: joined-SELECT WHERE accepts a conjunction of
        /// `qualifier.col = literal` predicates. Empty = no WHERE.
        where_clause: Vec<JoinedPredicate>,
        order_by: Option<JoinedOrderBy>,
        limit: Option<u64>,
        offset: Option<u64>,
        /// Sprint 15.F: explicit projection. `Star` keeps the legacy
        /// `SELECT *` joined behavior; `Explicit` selects qualified columns;
        /// `Aggregated` triggers per-group aggregation over the joined rows.
        columns: SelectColumns,
        /// Sprint 15.F: optional `GROUP BY` projecting onto the joined rows.
        /// Empty = no grouping. Required when `columns` is `Aggregated` and
        /// the projection includes any non-aggregate column.
        group_by: Vec<String>,
        /// Sprint 15.F: optional `HAVING` filter applied post-aggregation.
        having: Vec<HavingPredicate>,
    },
    Begin,
    Commit,
    Rollback,
    /// `DROP TABLE [IF EXISTS] <table>` (single table). Multi-table
    /// `DROP TABLE [IF EXISTS] t1, t2, ..` is exploded by the parser into
    /// a DoBlock of N DropTable statements (same shape as multi-row INSERT
    /// VALUES) so the executor only has to handle the single-table case.
    DropTable {
        table: String,
        if_exists: bool,
    },
    /// Phase A 2026-05-22: `TRUNCATE [TABLE] <table>` (single table).
    /// Multi-table list is exploded into a DoBlock by the parser.
    TruncateTable {
        table: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinClause {
    pub kind: JoinKind,
    pub right: String,
    pub left_column: String,
    pub right_column: String,
    /// Sprint 15.F: extra `col = literal` predicates that appear inside
    /// `ON (... AND ...)`. Drizzle emits these for tenant-scoped joins
    /// (`ON (a.fk = b.pk AND b.workspace_id = $1)`), so we filter the right
    /// side during the join. Empty = legacy `ON a.x = b.y` only.
    #[doc(hidden)]
    pub extra: Vec<JoinedPredicate>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinedPredicate {
    pub qualifier: Option<String>,
    pub column: String,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinedOrderBy {
    pub qualifier: Option<String>,
    pub column: String,
    pub direction: OrderDirection,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SelectColumns {
    Star,
    Explicit(Vec<String>),
    /// Sprint 15: aggregated projection with optional `GROUP BY` partitioning.
    /// Each item is either a raw column (which must appear in `group_by`) or an
    /// aggregate expression like `COUNT(*)` / `SUM(amount)`.
    Aggregated(AggregateProjection),
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateProjection {
    pub items: Vec<AggregateSelectItem>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateSelectItem {
    pub expr: AggregateOrColumn,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AggregateOrColumn {
    /// A bare column reference. Must be present in the enclosing
    /// `SelectAll.group_by` list (or the query has no aggregates at all).
    Column(String),
    Aggregate(AggregateExpr),
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateExpr {
    pub func: AggregateFunction,
    pub arg: AggregateArg,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AggregateArg {
    /// `COUNT(*)` only.
    Star,
    Column(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Max,
    Min,
    Avg,
}

/// Sprint 15.C: a single HAVING-clause predicate. The LHS is either an
/// aggregate expression or a bare column that participates in `GROUP BY`.
#[derive(Clone, Debug, PartialEq)]
pub struct HavingPredicate {
    pub expr: AggregateOrColumn,
    pub op: WhereOp,
    pub value: Value,
}

/// Sprint 16.A: parsed `RETURNING` clause for INSERT / UPDATE / DELETE.
/// `Star` mirrors `RETURNING *` (Drizzle `.returning()` no-arg) and
/// `Columns(...)` mirrors `.returning({col: t.col, ...})`.
#[derive(Clone, Debug, PartialEq)]
pub enum ReturningClause {
    Star,
    Columns(Vec<String>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SelectExprItem {
    pub expr: SelectExpr,
    pub alias: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SelectExpr {
    Literal(Value),
    Function(SelectFunction),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SelectFunction {
    Version,
    Now,
    CurrentTimestamp,
}

impl Statement {
    pub fn is_read(&self) -> bool {
        matches!(
            self,
            Self::SelectAll { .. }
                | Self::Select { .. }
                | Self::SelectExpr { .. }
                | Self::Begin
                | Self::Commit
                | Self::Rollback
        )
    }

    /// Sprint 10: convenience constructor that mirrors the pre-Sprint-10
    /// `SelectAll { table, predicate }` shape and fills the new optional
    /// clauses with `None`. Keeps the test suites readable.
    pub fn select_all_legacy(table: String, predicate: Option<Predicate>) -> Self {
        Self::SelectAll {
            table,
            predicate: predicate.into_iter().collect(),
            order_by: None,
            limit: None,
            offset: None,
            columns: SelectColumns::Star,
            group_by: Vec::new(),
            having: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryResult {
    Command {
        tag: String,
    },
    Rows {
        columns: Vec<String>,
        #[doc = "Per-column SQL type. Empty vector means \"unknown\" (legacy callers)."]
        column_types: Vec<ColumnType>,
        rows: Vec<Vec<Value>>,
    },
}
