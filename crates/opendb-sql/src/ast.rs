use opendb_storage::commit_stream::{ColumnDefinition, Value};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Predicate {
    pub column: String,
    pub value: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Statement {
    CreateTable {
        table: String,
        columns: Vec<ColumnDefinition>,
    },
    Insert {
        table: String,
        values: Vec<Value>,
    },
    SelectAll {
        table: String,
        predicate: Option<Predicate>,
    },
}

impl Statement {
    pub fn is_read(&self) -> bool {
        matches!(self, Self::SelectAll { .. })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryResult {
    Command {
        tag: String,
    },
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
}
