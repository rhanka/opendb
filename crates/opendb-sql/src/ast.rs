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
    },
    SelectAll {
        table: String,
        predicate: Option<Predicate>,
        #[doc = "Sprint 10: optional ORDER BY clause."]
        order_by: Option<OrderBy>,
        #[doc = "Sprint 10: optional LIMIT clause."]
        limit: Option<u64>,
        #[doc = "Sprint 10: optional OFFSET clause."]
        offset: Option<u64>,
        #[doc = "Sprint 12.1: explicit column projection (`SELECT a, b FROM t`)."]
        columns: SelectColumns,
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
    },
    Select {
        left: String,
        join: JoinClause,
        where_clause: Option<JoinedPredicate>,
        order_by: Option<JoinedOrderBy>,
        limit: Option<u64>,
        offset: Option<u64>,
    },
    Begin,
    Commit,
    Rollback,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JoinClause {
    pub kind: JoinKind,
    pub right: String,
    pub left_column: String,
    pub right_column: String,
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
            predicate,
            order_by: None,
            limit: None,
            offset: None,
            columns: SelectColumns::Star,
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
