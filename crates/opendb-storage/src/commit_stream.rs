use crate::archive_manifest::{ArchiveObjectPointer, RecoveryArtifactPointer};
use crate::range_catalog::RangeDescriptor;
use opendb_common::{LogicalTimestamp, RangeId, TransactionId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Value {
    Int64(i64),
    Text(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ColumnType {
    Int64,
    Text,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct ColumnValue {
    pub column: String,
    pub value: Value,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    PutArchiveObjectPointer {
        pointer: ArchiveObjectPointer,
    },
    PutRecoveryArtifactPointer {
        artifact: RecoveryArtifactPointer,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
        Self {
            version: Self::VERSION,
            tx_id,
            range_id: RangeId::ROOT,
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
