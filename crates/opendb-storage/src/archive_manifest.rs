use crate::commit_stream::{CommitRecord, Mutation};
use opendb_common::{LogicalTimestamp, OpenDbError, OpenDbResult, RangeId, TransactionId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveBackendKind {
    S3Compatible,
    GoogleCloudStorage,
    AzureBlobCompatible,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveObjectPointer {
    pub backend: ArchiveBackendKind,
    pub bucket: String,
    pub key: String,
    pub content_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryArtifactKind {
    WalSegment,
    Snapshot,
    ProjectionCheckpoint,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionKind {
    None,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct RecoveryArtifactPointer {
    pub artifact_kind: RecoveryArtifactKind,
    pub range_id: RangeId,
    pub object: ArchiveObjectPointer,
    pub format_version: u16,
    pub tx_id_start: TransactionId,
    pub tx_id_end: TransactionId,
    pub ts_start: LogicalTimestamp,
    pub ts_end: LogicalTimestamp,
    pub record_count: u64,
    pub byte_len: u64,
    pub compression: CompressionKind,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArchiveManifest {
    object_pointers: Vec<ArchiveObjectPointer>,
    recovery_artifacts: Vec<RecoveryArtifactPointer>,
}

impl ArchiveManifest {
    pub fn apply(&mut self, record: &CommitRecord) -> OpenDbResult<()> {
        let mut next = self.clone();
        next.apply_inner(record)?;
        *self = next;
        Ok(())
    }

    fn apply_inner(&mut self, record: &CommitRecord) -> OpenDbResult<()> {
        for mutation in &record.mutations {
            match mutation {
                Mutation::PutArchiveObjectPointer { pointer } => {
                    validate_object_pointer(pointer)?;
                    if let Some(existing) = self
                        .recovery_artifacts
                        .iter()
                        .find(|existing| same_object_location(&existing.object, pointer))
                        && existing.object.content_sha256 != pointer.content_sha256
                    {
                        return Err(OpenDbError::InvalidInput(format!(
                            "conflicting recovery artifact object {}/{}/{} has content_sha256 {}, got {}",
                            backend_name(&pointer.backend),
                            pointer.bucket,
                            pointer.key,
                            existing.object.content_sha256,
                            pointer.content_sha256
                        )));
                    }
                    match self
                        .object_pointers
                        .iter()
                        .find(|existing| same_object_location(existing, pointer))
                    {
                        Some(existing) if existing.content_sha256 == pointer.content_sha256 => {}
                        Some(_) => {
                            return Err(OpenDbError::InvalidInput(format!(
                                "archive object pointer {}/{}/{} has conflicting content_sha256",
                                backend_name(&pointer.backend),
                                pointer.bucket,
                                pointer.key
                            )));
                        }
                        None => self.object_pointers.push(pointer.clone()),
                    }
                }
                Mutation::PutRecoveryArtifactPointer { artifact } => {
                    self.apply_recovery_artifact(artifact)?;
                }
                Mutation::CreateTable { .. }
                | Mutation::InsertRow { .. }
                | Mutation::DeleteRow { .. }
                | Mutation::PutRangeDescriptor { .. }
                | Mutation::SplitRange { .. }
                | Mutation::MergeRanges { .. }
                | Mutation::AlterTable { .. } => {}
            }
        }
        Ok(())
    }

    fn apply_recovery_artifact(&mut self, artifact: &RecoveryArtifactPointer) -> OpenDbResult<()> {
        validate_recovery_artifact(artifact)?;
        if self
            .recovery_artifacts
            .iter()
            .any(|existing| existing == artifact)
        {
            return Ok(());
        }
        if let Some(existing) = self
            .object_pointers
            .iter()
            .find(|existing| same_object_location(existing, &artifact.object))
            && existing.content_sha256 != artifact.object.content_sha256
        {
            return Err(OpenDbError::InvalidInput(format!(
                "conflicting recovery artifact object {}/{}/{} has content_sha256 {}, got {}",
                backend_name(&artifact.object.backend),
                artifact.object.bucket,
                artifact.object.key,
                existing.content_sha256,
                artifact.object.content_sha256
            )));
        }
        if let Some(existing) = self
            .recovery_artifacts
            .iter()
            .find(|existing| same_object_location(&existing.object, &artifact.object))
        {
            return Err(OpenDbError::InvalidInput(format!(
                "conflicting recovery artifact object {}/{}/{}: existing {:?}, new {:?}",
                backend_name(&artifact.object.backend),
                artifact.object.bucket,
                artifact.object.key,
                existing,
                artifact
            )));
        }
        if let Some(existing) = self.recovery_artifacts.iter().find(|existing| {
            existing.range_id == artifact.range_id
                && existing.artifact_kind == artifact.artifact_kind
                && tx_ranges_overlap(
                    existing.tx_id_start,
                    existing.tx_id_end,
                    artifact.tx_id_start,
                    artifact.tx_id_end,
                )
        }) {
            return Err(OpenDbError::InvalidInput(format!(
                "conflicting recovery artifact coverage for range {:?} {:?}: existing {:?}, new {:?}",
                artifact.range_id, artifact.artifact_kind, existing, artifact
            )));
        }

        self.recovery_artifacts.push(artifact.clone());
        Ok(())
    }

    pub fn rebuild(records: &[CommitRecord]) -> OpenDbResult<Self> {
        let mut manifest = Self::default();
        for record in records {
            manifest.apply(record)?;
        }
        Ok(manifest)
    }

    pub fn object_pointers(&self) -> &[ArchiveObjectPointer] {
        &self.object_pointers
    }

    pub fn recovery_artifacts(&self) -> &[RecoveryArtifactPointer] {
        &self.recovery_artifacts
    }
}

fn validate_object_pointer(pointer: &ArchiveObjectPointer) -> OpenDbResult<()> {
    if pointer.bucket.trim().is_empty() || pointer.bucket != pointer.bucket.trim() {
        return Err(OpenDbError::InvalidInput(
            "archive object pointer bucket must not be empty or padded".to_string(),
        ));
    }
    if pointer.key.trim().is_empty() || pointer.key != pointer.key.trim() {
        return Err(OpenDbError::InvalidInput(
            "archive object pointer key must not be empty or padded".to_string(),
        ));
    }
    if !is_lowercase_sha256_hex(&pointer.content_sha256) {
        return Err(OpenDbError::InvalidInput(
            "archive object pointer content_sha256 must be 64 lowercase hex characters".to_string(),
        ));
    }
    Ok(())
}

fn is_lowercase_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_recovery_artifact(artifact: &RecoveryArtifactPointer) -> OpenDbResult<()> {
    validate_object_pointer(&artifact.object)?;
    if artifact.format_version == 0 {
        return Err(OpenDbError::InvalidInput(
            "recovery artifact format_version must be greater than zero".to_string(),
        ));
    }
    if artifact.record_count == 0 {
        return Err(OpenDbError::InvalidInput(
            "recovery artifact record_count must be greater than zero".to_string(),
        ));
    }
    if artifact.byte_len == 0 {
        return Err(OpenDbError::InvalidInput(
            "recovery artifact byte_len must be greater than zero".to_string(),
        ));
    }
    if artifact.tx_id_start > artifact.tx_id_end {
        return Err(OpenDbError::InvalidInput(
            "recovery artifact tx_id_start must be <= tx_id_end".to_string(),
        ));
    }
    if artifact.ts_start > artifact.ts_end {
        return Err(OpenDbError::InvalidInput(
            "recovery artifact ts_start must be <= ts_end".to_string(),
        ));
    }
    Ok(())
}

fn same_object_location(left: &ArchiveObjectPointer, right: &ArchiveObjectPointer) -> bool {
    left.backend == right.backend && left.bucket == right.bucket && left.key == right.key
}

fn tx_ranges_overlap(
    left_start: TransactionId,
    left_end: TransactionId,
    right_start: TransactionId,
    right_end: TransactionId,
) -> bool {
    left_start <= right_end && right_start <= left_end
}

fn backend_name(backend: &ArchiveBackendKind) -> &'static str {
    match backend {
        ArchiveBackendKind::S3Compatible => "s3_compatible",
        ArchiveBackendKind::GoogleCloudStorage => "google_cloud_storage",
        ArchiveBackendKind::AzureBlobCompatible => "azure_blob_compatible",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_stream::{CommitRecord, Mutation, RangeMerge, RangeSplit};
    use crate::range_catalog::RangeDescriptor;
    use opendb_common::{LogicalTimestamp, RangeId, TransactionId};

    fn pointer() -> ArchiveObjectPointer {
        ArchiveObjectPointer {
            backend: ArchiveBackendKind::S3Compatible,
            bucket: "opendb-archives".to_owned(),
            key: "root-range/00000001.wal".to_owned(),
            content_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_owned(),
        }
    }

    fn recovery_artifact(
        key: impl Into<String>,
        tx_id_start: u64,
        tx_id_end: u64,
    ) -> RecoveryArtifactPointer {
        RecoveryArtifactPointer {
            artifact_kind: RecoveryArtifactKind::WalSegment,
            range_id: RangeId::ROOT,
            object: ArchiveObjectPointer {
                key: key.into(),
                ..pointer()
            },
            format_version: 1,
            tx_id_start: TransactionId(tx_id_start),
            tx_id_end: TransactionId(tx_id_end),
            ts_start: LogicalTimestamp(tx_id_start),
            ts_end: LogicalTimestamp(tx_id_end),
            record_count: tx_id_end - tx_id_start + 1,
            byte_len: 4096,
            compression: CompressionKind::None,
        }
    }

    fn record_for_recovery_artifact(tx_id: u64, artifact: RecoveryArtifactPointer) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::PutRecoveryArtifactPointer { artifact }],
        )
    }

    fn split_range_record(tx_id: u64) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::SplitRange {
                split: RangeSplit {
                    source_range_id: RangeId::ROOT,
                    split_key: "orders/".to_owned(),
                    left: RangeDescriptor {
                        range_id: RangeId(2),
                        parent_range_id: Some(RangeId::ROOT),
                        key_start: None,
                        key_end: Some("orders/".to_owned()),
                        replica_node_ids: vec![0],
                    },
                    right: RangeDescriptor {
                        range_id: RangeId(3),
                        parent_range_id: Some(RangeId::ROOT),
                        key_start: Some("orders/".to_owned()),
                        key_end: None,
                        replica_node_ids: vec![0],
                    },
                },
            }],
        )
    }

    fn merge_ranges_record(tx_id: u64) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::MergeRanges {
                merge: RangeMerge {
                    source_range_ids: vec![RangeId(2), RangeId(3)],
                    merged: RangeDescriptor {
                        range_id: RangeId(4),
                        parent_range_id: Some(RangeId::ROOT),
                        key_start: None,
                        key_end: None,
                        replica_node_ids: vec![0],
                    },
                },
            }],
        )
    }

    #[test]
    fn archive_manifest_rebuilds_object_pointers_from_commit_stream() {
        let pointer = pointer();
        let record = CommitRecord::new(
            TransactionId(47),
            LogicalTimestamp(12),
            vec![Mutation::PutArchiveObjectPointer {
                pointer: pointer.clone(),
            }],
        );

        let manifest = ArchiveManifest::rebuild(&[record]).expect("rebuild archive manifest");

        assert_eq!(manifest.object_pointers(), &[pointer]);
    }

    #[test]
    fn archive_manifest_rebuilds_recovery_artifacts() {
        let artifact = recovery_artifact("root-range/00000001.wal", 0, 10);
        let record = CommitRecord::new(
            TransactionId(55),
            LogicalTimestamp(20),
            vec![Mutation::PutRecoveryArtifactPointer {
                artifact: artifact.clone(),
            }],
        );

        let manifest = ArchiveManifest::rebuild(&[record]).expect("rebuild manifest");

        assert_eq!(manifest.recovery_artifacts(), &[artifact]);
    }

    #[test]
    fn archive_manifest_ignores_range_split_merge_metadata() {
        let pointer = pointer();
        let pointer_record = CommitRecord::new(
            TransactionId(49),
            LogicalTimestamp(14),
            vec![Mutation::PutArchiveObjectPointer {
                pointer: pointer.clone(),
            }],
        );

        let manifest = ArchiveManifest::rebuild(&[
            split_range_record(47),
            pointer_record,
            merge_ranges_record(50),
        ])
        .expect("rebuild archive manifest");

        assert_eq!(manifest.object_pointers(), &[pointer]);
        assert!(manifest.recovery_artifacts().is_empty());
    }

    #[test]
    fn archive_manifest_rejects_conflicting_recovery_artifact_object_metadata() {
        let first = recovery_artifact("root-range/00000001.wal", 0, 10);
        let conflicting = RecoveryArtifactPointer {
            object: ArchiveObjectPointer {
                content_sha256: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    .to_owned(),
                ..first.object.clone()
            },
            ..first.clone()
        };
        let records = vec![
            CommitRecord::new(
                TransactionId(55),
                LogicalTimestamp(20),
                vec![Mutation::PutRecoveryArtifactPointer { artifact: first }],
            ),
            CommitRecord::new(
                TransactionId(56),
                LogicalTimestamp(21),
                vec![Mutation::PutRecoveryArtifactPointer {
                    artifact: conflicting,
                }],
            ),
        ];

        let error = ArchiveManifest::rebuild(&records).expect_err("reject conflict");
        assert!(
            error.to_string().contains("conflicting recovery artifact"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn archive_manifest_rejects_invalid_recovery_artifacts() {
        let cases = vec![
            (
                RecoveryArtifactPointer {
                    format_version: 0,
                    ..recovery_artifact("root-range/00000001.wal", 0, 10)
                },
                "format_version",
            ),
            (
                RecoveryArtifactPointer {
                    record_count: 0,
                    ..recovery_artifact("root-range/00000001.wal", 0, 10)
                },
                "record_count",
            ),
            (
                RecoveryArtifactPointer {
                    byte_len: 0,
                    ..recovery_artifact("root-range/00000001.wal", 0, 10)
                },
                "byte_len",
            ),
            (
                RecoveryArtifactPointer {
                    tx_id_start: TransactionId(11),
                    tx_id_end: TransactionId(10),
                    ..recovery_artifact("root-range/00000001.wal", 0, 10)
                },
                "tx_id_start",
            ),
            (
                RecoveryArtifactPointer {
                    ts_start: LogicalTimestamp(11),
                    ts_end: LogicalTimestamp(10),
                    ..recovery_artifact("root-range/00000001.wal", 0, 10)
                },
                "ts_start",
            ),
            (
                RecoveryArtifactPointer {
                    object: ArchiveObjectPointer {
                        content_sha256: "not-a-sha".to_owned(),
                        ..pointer()
                    },
                    ..recovery_artifact("root-range/00000001.wal", 0, 10)
                },
                "content_sha256",
            ),
        ];

        for (artifact, expected_message) in cases {
            let error = ArchiveManifest::rebuild(&[record_for_recovery_artifact(55, artifact)])
                .expect_err("reject invalid recovery artifact");

            assert!(
                error.to_string().contains(expected_message),
                "expected {expected_message:?}, got {error}"
            );
        }
    }

    #[test]
    fn archive_manifest_deduplicates_identical_recovery_artifacts() {
        let artifact = recovery_artifact("root-range/00000001.wal", 0, 10);
        let records = vec![
            record_for_recovery_artifact(55, artifact.clone()),
            record_for_recovery_artifact(56, artifact.clone()),
        ];

        let manifest = ArchiveManifest::rebuild(&records).expect("rebuild manifest");

        assert_eq!(manifest.recovery_artifacts(), &[artifact]);
    }

    #[test]
    fn archive_manifest_rejects_overlapping_recovery_artifact_coverage() {
        let records = vec![
            record_for_recovery_artifact(55, recovery_artifact("root-range/00000001.wal", 0, 10)),
            record_for_recovery_artifact(56, recovery_artifact("root-range/00000002.wal", 10, 20)),
        ];

        let error = ArchiveManifest::rebuild(&records).expect_err("reject overlap");

        assert!(
            error
                .to_string()
                .contains("conflicting recovery artifact coverage"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn archive_manifest_accepts_adjacent_recovery_artifact_coverage() {
        let first = recovery_artifact("root-range/00000001.wal", 0, 10);
        let second = recovery_artifact("root-range/00000002.wal", 11, 20);
        let records = vec![
            record_for_recovery_artifact(55, first.clone()),
            record_for_recovery_artifact(56, second.clone()),
        ];

        let manifest = ArchiveManifest::rebuild(&records).expect("rebuild manifest");

        assert_eq!(manifest.recovery_artifacts(), &[first, second]);
    }

    #[test]
    fn archive_manifest_apply_rejects_invalid_recovery_artifact_atomically() {
        let mut manifest = ArchiveManifest::default();
        let valid = recovery_artifact("root-range/00000001.wal", 0, 10);
        let invalid = RecoveryArtifactPointer {
            record_count: 0,
            ..recovery_artifact("root-range/00000002.wal", 11, 20)
        };
        let record = CommitRecord::new(
            TransactionId(55),
            LogicalTimestamp(55),
            vec![
                Mutation::PutRecoveryArtifactPointer { artifact: valid },
                Mutation::PutRecoveryArtifactPointer { artifact: invalid },
            ],
        );

        let error = manifest
            .apply(&record)
            .expect_err("reject invalid artifact");

        assert!(
            error.to_string().contains("record_count"),
            "unexpected error: {error}"
        );
        assert!(manifest.recovery_artifacts().is_empty());
    }

    #[test]
    fn archive_manifest_rejects_recovery_artifact_object_with_different_coverage() {
        let first = recovery_artifact("root-range/00000001.wal", 0, 10);
        let conflicting = RecoveryArtifactPointer {
            tx_id_start: TransactionId(11),
            tx_id_end: TransactionId(20),
            ts_start: LogicalTimestamp(11),
            ts_end: LogicalTimestamp(20),
            ..first.clone()
        };
        let records = vec![
            record_for_recovery_artifact(55, first),
            record_for_recovery_artifact(56, conflicting),
        ];

        let error = ArchiveManifest::rebuild(&records).expect_err("reject object reuse");

        assert!(
            error
                .to_string()
                .contains("conflicting recovery artifact object"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn archive_manifest_rejects_archive_pointer_recovery_artifact_hash_conflicts() {
        let pointer = pointer();
        let conflicting_artifact = RecoveryArtifactPointer {
            object: ArchiveObjectPointer {
                content_sha256: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    .to_owned(),
                ..pointer.clone()
            },
            ..recovery_artifact(pointer.key.clone(), 0, 10)
        };

        for records in [
            vec![
                CommitRecord::new(
                    TransactionId(55),
                    LogicalTimestamp(55),
                    vec![Mutation::PutArchiveObjectPointer {
                        pointer: pointer.clone(),
                    }],
                ),
                record_for_recovery_artifact(56, conflicting_artifact.clone()),
            ],
            vec![
                record_for_recovery_artifact(55, conflicting_artifact.clone()),
                CommitRecord::new(
                    TransactionId(56),
                    LogicalTimestamp(56),
                    vec![Mutation::PutArchiveObjectPointer {
                        pointer: pointer.clone(),
                    }],
                ),
            ],
        ] {
            let error = ArchiveManifest::rebuild(&records).expect_err("reject hash conflict");

            assert!(
                error
                    .to_string()
                    .contains("conflicting recovery artifact object"),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn archive_manifest_rejects_invalid_object_pointers() {
        let cases = vec![
            (
                ArchiveObjectPointer {
                    bucket: String::new(),
                    ..pointer()
                },
                "bucket",
            ),
            (
                ArchiveObjectPointer {
                    bucket: " padded".to_owned(),
                    ..pointer()
                },
                "bucket",
            ),
            (
                ArchiveObjectPointer {
                    key: String::new(),
                    ..pointer()
                },
                "key",
            ),
            (
                ArchiveObjectPointer {
                    key: "padded ".to_owned(),
                    ..pointer()
                },
                "key",
            ),
            (
                ArchiveObjectPointer {
                    content_sha256: "not-a-sha".to_owned(),
                    ..pointer()
                },
                "content_sha256",
            ),
        ];

        for (pointer, expected_message) in cases {
            let record = CommitRecord::new(
                TransactionId(48),
                LogicalTimestamp(13),
                vec![Mutation::PutArchiveObjectPointer { pointer }],
            );

            let error = ArchiveManifest::rebuild(&[record]).expect_err("reject invalid pointer");

            assert!(
                error.to_string().contains(expected_message),
                "expected {expected_message:?}, got {error}"
            );
        }
    }

    #[test]
    fn archive_manifest_rejects_conflicting_object_pointer_hashes() {
        let first = pointer();
        let conflicting = ArchiveObjectPointer {
            content_sha256: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                .to_owned(),
            ..first.clone()
        };
        let records = vec![
            CommitRecord::new(
                TransactionId(49),
                LogicalTimestamp(14),
                vec![Mutation::PutArchiveObjectPointer { pointer: first }],
            ),
            CommitRecord::new(
                TransactionId(50),
                LogicalTimestamp(15),
                vec![Mutation::PutArchiveObjectPointer {
                    pointer: conflicting,
                }],
            ),
        ];

        let error =
            ArchiveManifest::rebuild(&records).expect_err("reject conflicting archive pointer");

        assert!(
            error.to_string().contains("conflicting content_sha256"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn archive_manifest_deduplicates_identical_object_pointers() {
        let pointer = pointer();
        let records = vec![
            CommitRecord::new(
                TransactionId(49),
                LogicalTimestamp(14),
                vec![Mutation::PutArchiveObjectPointer {
                    pointer: pointer.clone(),
                }],
            ),
            CommitRecord::new(
                TransactionId(50),
                LogicalTimestamp(15),
                vec![Mutation::PutArchiveObjectPointer {
                    pointer: pointer.clone(),
                }],
            ),
        ];

        let manifest = ArchiveManifest::rebuild(&records).expect("rebuild archive manifest");

        assert_eq!(manifest.object_pointers(), &[pointer]);
    }
}
