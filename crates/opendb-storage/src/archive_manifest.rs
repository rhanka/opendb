use crate::commit_stream::{CommitRecord, Mutation};
use opendb_common::{OpenDbError, OpenDbResult};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveBackendKind {
    S3Compatible,
    GoogleCloudStorage,
    AzureBlobCompatible,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArchiveObjectPointer {
    pub backend: ArchiveBackendKind,
    pub bucket: String,
    pub key: String,
    pub content_sha256: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArchiveManifest {
    object_pointers: Vec<ArchiveObjectPointer>,
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
                    match self.object_pointers.iter().find(|existing| {
                        existing.backend == pointer.backend
                            && existing.bucket == pointer.bucket
                            && existing.key == pointer.key
                    }) {
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
                Mutation::CreateTable { .. }
                | Mutation::InsertRow { .. }
                | Mutation::PutRangeDescriptor { .. } => {}
            }
        }
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
    use crate::commit_stream::{CommitRecord, Mutation};
    use opendb_common::{LogicalTimestamp, TransactionId};

    fn pointer() -> ArchiveObjectPointer {
        ArchiveObjectPointer {
            backend: ArchiveBackendKind::S3Compatible,
            bucket: "opendb-archives".to_owned(),
            key: "root-range/00000001.wal".to_owned(),
            content_sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_owned(),
        }
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
