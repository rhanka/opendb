use crate::range_catalog::RangeDescriptor;
use opendb_common::{LogicalTimestamp, RangeId, TransactionId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Int64(i64),
    Text(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ColumnType {
    Int64,
    Text,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: ColumnType,
    pub primary_key: bool,
}

impl ColumnDefinition {
    pub fn new(name: impl Into<String>, data_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            data_type,
            primary_key: false,
        }
    }

    pub fn primary_key(name: impl Into<String>, data_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            data_type,
            primary_key: true,
        }
    }
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
        columns: Vec<ColumnDefinition>,
    },
    InsertRow {
        table: String,
        key: String,
        values: Vec<ColumnValue>,
    },
    PutRangeDescriptor {
        descriptor: RangeDescriptor,
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
    pub const VERSION: u16 = 2;

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
                columns: vec![
                    ColumnDefinition::primary_key("id", ColumnType::Int64),
                    ColumnDefinition::new("name", ColumnType::Text),
                ],
            }],
        );

        assert_eq!(CommitRecord::VERSION, 2);
        assert_eq!(record.version, CommitRecord::VERSION);
        assert_eq!(record.tx_id, TransactionId(42));
        assert_eq!(record.range_id, RangeId::ROOT);
        assert_eq!(record.ts, LogicalTimestamp(7));
        assert_eq!(record.actor, "system");
        assert_eq!(record.mutations.len(), 1);
    }

    #[test]
    fn commit_record_serializes_range_descriptor_metadata_mutation() {
        let descriptor = RangeDescriptor {
            range_id: RangeId::ROOT,
            parent_range_id: None,
            key_start: None,
            key_end: None,
            replica_node_ids: vec![0, 1, 2],
        };
        let record = CommitRecord::new(
            TransactionId(43),
            LogicalTimestamp(8),
            vec![Mutation::PutRangeDescriptor {
                descriptor: descriptor.clone(),
            }],
        );
        let encoded = serde_json::to_string(&record).expect("serialize range descriptor record");
        let decoded: CommitRecord =
            serde_json::from_str(&encoded).expect("deserialize range descriptor record");

        assert_eq!(decoded, record);
        assert_eq!(record.version, CommitRecord::VERSION);
        assert_eq!(CommitRecord::VERSION, 2);
        assert_eq!(
            record.mutations,
            vec![Mutation::PutRangeDescriptor { descriptor }]
        );
    }
}
