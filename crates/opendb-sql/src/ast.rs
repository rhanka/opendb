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
}

impl Statement {
    pub fn is_read(&self) -> bool {
        matches!(self, Self::SelectAll { .. })
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
