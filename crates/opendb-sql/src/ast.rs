use opendb_storage::commit_stream::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Statement {
    CreateTable { table: String, columns: Vec<String> },
    Insert { table: String, values: Vec<Value> },
    SelectAll { table: String },
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
