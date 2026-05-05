use opendb_common::{LogicalTimestamp, RangeId, TransactionId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Int64(i64),
    Text(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColumnValue {
    pub column: String,
    pub value: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Mutation {
    CreateTable {
        table: String,
        columns: Vec<String>,
    },
    InsertRow {
        table: String,
        key: String,
        values: Vec<ColumnValue>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitRecord {
    pub version: u16,
    pub tx_id: TransactionId,
    pub range_id: RangeId,
    pub ts: LogicalTimestamp,
    pub actor: String,
    pub mutations: Vec<Mutation>,
}

impl CommitRecord {
    pub const VERSION: u16 = 1;

    pub fn new(tx_id: TransactionId, ts: LogicalTimestamp, mutations: Vec<Mutation>) -> Self {
        Self {
            version: Self::VERSION,
            tx_id,
            range_id: RangeId::ROOT,
            ts,
            actor: "system".to_string(),
            mutations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendb_common::{LogicalTimestamp, RangeId, TransactionId};

    #[test]
    fn commit_record_has_stable_version_and_root_range() {
        let record = CommitRecord::new(
            TransactionId(42),
            LogicalTimestamp(7),
            vec![Mutation::CreateTable {
                table: "users".to_string(),
                columns: vec!["id".to_string(), "name".to_string()],
            }],
        );

        assert_eq!(CommitRecord::VERSION, 1);
        assert_eq!(record.version, CommitRecord::VERSION);
        assert_eq!(record.tx_id, TransactionId(42));
        assert_eq!(record.range_id, RangeId::ROOT);
        assert_eq!(record.ts, LogicalTimestamp(7));
        assert_eq!(record.actor, "system");
        assert_eq!(record.mutations.len(), 1);
    }
}
