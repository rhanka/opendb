use crate::archive_manifest::{ArchiveObjectPointer, RecoveryArtifactPointer};
use crate::range_catalog::RangeDescriptor;
use opendb_common::{LogicalTimestamp, RangeId, TransactionId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Value {
    Int64(i64),
    Text(String),
    Bool(bool),
    Float64(f64),
    Timestamp(i64),
    Json(serde_json::Value),
    Null,
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ColumnType {
    Int64,
    Text,
    Bool,
    Float64,
    Timestamp,
    Json,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum DefaultExpr {
    Const(Value),
    Now,
}

fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: ColumnType,
    pub primary_key: bool,
    #[serde(default = "default_true")]
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<DefaultExpr>,
}

impl ColumnDefinition {
    pub fn new(name: impl Into<String>, data_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            data_type,
            primary_key: false,
            nullable: true,
            default: None,
        }
    }

    pub fn primary_key(name: impl Into<String>, data_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            data_type,
            primary_key: true,
            nullable: false,
            default: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn with_default(mut self, expr: DefaultExpr) -> Self {
        self.default = Some(expr);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnValue {
    pub column: String,
    pub value: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeSplit {
    pub source_range_id: RangeId,
    pub split_key: String,
    pub left: RangeDescriptor,
    pub right: RangeDescriptor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeMerge {
    pub source_range_ids: Vec<RangeId>,
    pub merged: RangeDescriptor,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    SplitRange {
        split: RangeSplit,
    },
    MergeRanges {
        merge: RangeMerge,
    },
    PutArchiveObjectPointer {
        pointer: ArchiveObjectPointer,
    },
    PutRecoveryArtifactPointer {
        artifact: RecoveryArtifactPointer,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    pub const BOOTSTRAP_ACTOR: &'static str = "system";

    pub fn new(tx_id: TransactionId, ts: LogicalTimestamp, mutations: Vec<Mutation>) -> Self {
        Self::new_with_actor(tx_id, ts, Self::BOOTSTRAP_ACTOR, mutations)
    }

    pub fn new_with_actor(
        tx_id: TransactionId,
        ts: LogicalTimestamp,
        actor: impl Into<String>,
        mutations: Vec<Mutation>,
    ) -> Self {
        Self::new_for_range(RangeId::ROOT, tx_id, ts, actor, mutations)
    }

    pub fn new_for_range(
        range_id: RangeId,
        tx_id: TransactionId,
        ts: LogicalTimestamp,
        actor: impl Into<String>,
        mutations: Vec<Mutation>,
    ) -> Self {
        Self {
            version: Self::VERSION,
            tx_id,
            range_id,
            ts,
            actor: actor.into(),
            mutations,
        }
    }

    pub fn root_bootstrap(replica_node_ids: Vec<u64>) -> Self {
        let mut replica_node_ids = replica_node_ids;
        replica_node_ids.sort_unstable();
        replica_node_ids.dedup();
        Self::new_with_actor(
            TransactionId(0),
            LogicalTimestamp(0),
            Self::BOOTSTRAP_ACTOR,
            vec![Mutation::PutRangeDescriptor {
                descriptor: RangeDescriptor {
                    range_id: RangeId::ROOT,
                    parent_range_id: None,
                    key_start: None,
                    key_end: None,
                    replica_node_ids,
                },
            }],
        )
    }

    pub fn is_root_bootstrap(&self) -> bool {
        self.tx_id == TransactionId(0)
            && self.ts == LogicalTimestamp(0)
            && self.range_id == RangeId::ROOT
            && self.actor == Self::BOOTSTRAP_ACTOR
            && matches!(
                self.mutations.as_slice(),
                [Mutation::PutRangeDescriptor { descriptor }]
                    if descriptor.range_id == RangeId::ROOT
                        && descriptor.parent_range_id.is_none()
                        && descriptor.key_start.is_none()
                        && descriptor.key_end.is_none()
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive_manifest::{
        ArchiveBackendKind, ArchiveObjectPointer, CompressionKind, RecoveryArtifactKind,
        RecoveryArtifactPointer,
    };
    use opendb_common::{LogicalTimestamp, RangeId, TransactionId};

    #[test]
    fn value_round_trips_through_serde_for_every_variant() {
        for value in [
            Value::Int64(42),
            Value::Text("hello".to_owned()),
            Value::Bool(true),
            Value::Bool(false),
            Value::Float64(3.5),
            Value::Timestamp(1_700_000_000_000_000),
            Value::Json(serde_json::json!({"k": "v", "n": 7, "arr": [1, 2, 3]})),
            Value::Json(serde_json::json!([])),
            Value::Json(serde_json::Value::Null),
            Value::Null,
        ] {
            let json = serde_json::to_string(&value).expect("serialize");
            let parsed: Value = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(parsed, value);
        }
    }

    #[test]
    fn column_definition_serializes_nullable_and_default() {
        let definition = ColumnDefinition::new("created_at", ColumnType::Timestamp)
            .not_null()
            .with_default(DefaultExpr::Now);
        let json = serde_json::to_value(&definition).expect("serialize");

        assert_eq!(json["name"], "created_at");
        assert_eq!(json["data_type"], "Timestamp");
        assert_eq!(json["primary_key"], false);
        assert_eq!(json["nullable"], false);
        assert_eq!(json["default"], "Now");
    }

    #[test]
    fn column_definition_reads_legacy_payload_without_nullable_and_default() {
        let legacy = serde_json::json!({
            "name": "id",
            "data_type": "Int64",
            "primary_key": true,
        });
        let parsed: ColumnDefinition = serde_json::from_value(legacy).expect("legacy parse");
        assert!(parsed.nullable);
        assert!(parsed.default.is_none());
    }

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

    #[test]
    fn commit_record_serializes_archive_object_pointer_metadata_mutation() {
        let pointer = ArchiveObjectPointer {
            backend: ArchiveBackendKind::GoogleCloudStorage,
            bucket: "opendb-archives".to_owned(),
            key: "root-range/00000002.wal".to_owned(),
            content_sha256: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
                .to_owned(),
        };
        let record = CommitRecord::new(
            TransactionId(49),
            LogicalTimestamp(14),
            vec![Mutation::PutArchiveObjectPointer {
                pointer: pointer.clone(),
            }],
        );
        let encoded =
            serde_json::to_string(&record).expect("serialize archive object pointer record");
        let decoded: CommitRecord =
            serde_json::from_str(&encoded).expect("deserialize archive object pointer record");

        assert_eq!(decoded, record);
        assert_eq!(record.version, CommitRecord::VERSION);
        assert_eq!(CommitRecord::VERSION, 2);
        assert_eq!(
            record.mutations,
            vec![Mutation::PutArchiveObjectPointer { pointer }]
        );
    }

    #[test]
    fn commit_record_serializes_recovery_artifact_pointer_metadata_mutation() {
        let artifact = RecoveryArtifactPointer {
            artifact_kind: RecoveryArtifactKind::WalSegment,
            range_id: RangeId::ROOT,
            object: ArchiveObjectPointer {
                backend: ArchiveBackendKind::GoogleCloudStorage,
                bucket: "opendb-archives".to_owned(),
                key: "root-range/00000005.wal".to_owned(),
                content_sha256: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
                    .to_owned(),
            },
            format_version: 1,
            tx_id_start: TransactionId(0),
            tx_id_end: TransactionId(10),
            ts_start: LogicalTimestamp(0),
            ts_end: LogicalTimestamp(10),
            record_count: 11,
            byte_len: 4096,
            compression: CompressionKind::None,
        };
        let record = CommitRecord::new(
            TransactionId(50),
            LogicalTimestamp(15),
            vec![Mutation::PutRecoveryArtifactPointer {
                artifact: artifact.clone(),
            }],
        );
        let encoded =
            serde_json::to_string(&record).expect("serialize recovery artifact pointer record");
        let decoded: CommitRecord =
            serde_json::from_str(&encoded).expect("deserialize recovery artifact pointer record");

        assert_eq!(decoded, record);
        assert_eq!(record.version, CommitRecord::VERSION);
        assert_eq!(CommitRecord::VERSION, 2);
        assert_eq!(
            encoded,
            r#"{"version":2,"tx_id":50,"range_id":1,"ts":15,"actor":"system","mutations":[{"PutRecoveryArtifactPointer":{"artifact":{"artifact_kind":"wal_segment","range_id":1,"object":{"backend":"google_cloud_storage","bucket":"opendb-archives","key":"root-range/00000005.wal","content_sha256":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},"format_version":1,"tx_id_start":0,"tx_id_end":10,"ts_start":0,"ts_end":10,"record_count":11,"byte_len":4096,"compression":"none"}}}]}"#
        );
        assert_eq!(
            record.mutations,
            vec![Mutation::PutRecoveryArtifactPointer { artifact }]
        );
    }

    #[test]
    fn commit_record_serializes_range_split_metadata_mutation() {
        let split = RangeSplit {
            source_range_id: RangeId::ROOT,
            split_key: "orders/".to_owned(),
            left: RangeDescriptor {
                range_id: RangeId(2),
                parent_range_id: Some(RangeId::ROOT),
                key_start: None,
                key_end: Some("orders/".to_owned()),
                replica_node_ids: vec![0, 1, 2],
            },
            right: RangeDescriptor {
                range_id: RangeId(3),
                parent_range_id: Some(RangeId::ROOT),
                key_start: Some("orders/".to_owned()),
                key_end: None,
                replica_node_ids: vec![0, 1, 2],
            },
        };
        let record = CommitRecord::new(
            TransactionId(51),
            LogicalTimestamp(16),
            vec![Mutation::SplitRange {
                split: split.clone(),
            }],
        );

        let encoded = serde_json::to_string(&record).expect("serialize split record");
        let decoded: CommitRecord = serde_json::from_str(&encoded).expect("decode split record");

        assert_eq!(decoded, record);
        assert_eq!(record.version, CommitRecord::VERSION);
        assert_eq!(CommitRecord::VERSION, 2);
        assert_eq!(record.mutations, vec![Mutation::SplitRange { split }]);
    }

    #[test]
    fn commit_record_serializes_range_merge_metadata_mutation() {
        let merge = RangeMerge {
            source_range_ids: vec![RangeId(2), RangeId(3)],
            merged: RangeDescriptor {
                range_id: RangeId(4),
                parent_range_id: Some(RangeId::ROOT),
                key_start: None,
                key_end: None,
                replica_node_ids: vec![0, 1, 2],
            },
        };
        let record = CommitRecord::new(
            TransactionId(52),
            LogicalTimestamp(17),
            vec![Mutation::MergeRanges {
                merge: merge.clone(),
            }],
        );

        let encoded = serde_json::to_string(&record).expect("serialize merge record");
        let decoded: CommitRecord = serde_json::from_str(&encoded).expect("decode merge record");

        assert_eq!(decoded, record);
        assert_eq!(record.version, CommitRecord::VERSION);
        assert_eq!(CommitRecord::VERSION, 2);
        assert_eq!(record.mutations, vec![Mutation::MergeRanges { merge }]);
    }

    #[test]
    fn commit_record_builds_record_for_logical_range() {
        let record = CommitRecord::new_for_range(
            RangeId(2),
            TransactionId(53),
            LogicalTimestamp(18),
            CommitRecord::BOOTSTRAP_ACTOR,
            vec![Mutation::InsertRow {
                table: "accounts".to_owned(),
                key: "1".to_owned(),
                values: vec![ColumnValue {
                    column: "id".to_owned(),
                    value: Value::Int64(1),
                }],
            }],
        );

        assert_eq!(record.range_id, RangeId(2));
        assert_eq!(record.tx_id, TransactionId(53));
        assert_eq!(record.ts, LogicalTimestamp(18));
    }

    #[test]
    fn commit_record_builds_stable_root_bootstrap_record() {
        let record = CommitRecord::root_bootstrap(vec![2, 0, 1]);

        assert_eq!(record.version, CommitRecord::VERSION);
        assert_eq!(record.tx_id, TransactionId(0));
        assert_eq!(record.ts, LogicalTimestamp(0));
        assert_eq!(record.range_id, RangeId::ROOT);
        assert_eq!(record.actor, "system");
        assert_eq!(
            record.mutations,
            vec![Mutation::PutRangeDescriptor {
                descriptor: RangeDescriptor {
                    range_id: RangeId::ROOT,
                    parent_range_id: None,
                    key_start: None,
                    key_end: None,
                    replica_node_ids: vec![0, 1, 2],
                },
            }]
        );
        assert!(record.is_root_bootstrap());
    }
}
