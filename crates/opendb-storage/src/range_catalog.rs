use crate::commit_stream::{CommitRecord, Mutation};
use opendb_common::{OpenDbError, OpenDbResult, RangeId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RangeDescriptor {
    pub range_id: RangeId,
    pub parent_range_id: Option<RangeId>,
    pub key_start: Option<String>,
    pub key_end: Option<String>,
    pub replica_node_ids: Vec<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RangeCatalog {
    descriptors: BTreeMap<RangeId, RangeDescriptor>,
}

impl RangeCatalog {
    pub fn apply(&mut self, record: &CommitRecord) -> OpenDbResult<()> {
        let mut next = self.clone();
        next.apply_inner(record)?;
        *self = next;
        Ok(())
    }

    fn apply_inner(&mut self, record: &CommitRecord) -> OpenDbResult<()> {
        let mut candidate = self.descriptors.clone();
        for mutation in &record.mutations {
            match mutation {
                Mutation::PutRangeDescriptor { descriptor } => {
                    validate_descriptor_shape(descriptor)?;
                    match candidate.get(&descriptor.range_id) {
                        Some(existing) if existing == descriptor => {}
                        Some(existing) => {
                            return Err(OpenDbError::InvalidInput(format!(
                                "range {:?} has conflicting descriptor update: existing {:?}, new {:?}",
                                descriptor.range_id, existing, descriptor
                            )));
                        }
                        None => {
                            candidate.insert(descriptor.range_id, descriptor.clone());
                        }
                    }
                }
                Mutation::CreateTable { .. }
                | Mutation::InsertRow { .. }
                | Mutation::PutArchiveObjectPointer { .. }
                | Mutation::PutRecoveryArtifactPointer { .. } => {}
            }
        }
        validate_parent_graph(&candidate)?;
        validate_root_descriptor(&candidate)?;
        validate_sibling_ranges(&candidate)?;
        self.descriptors = candidate;
        Ok(())
    }

    pub fn rebuild(records: &[CommitRecord]) -> OpenDbResult<Self> {
        let mut catalog = Self::default();
        for record in records {
            catalog.apply(record)?;
        }
        Ok(catalog)
    }

    pub fn descriptor(&self, range_id: RangeId) -> Option<&RangeDescriptor> {
        self.descriptors.get(&range_id)
    }
}

fn validate_descriptor_shape(descriptor: &RangeDescriptor) -> OpenDbResult<()> {
    if descriptor.replica_node_ids.is_empty() {
        return Err(OpenDbError::InvalidInput(format!(
            "range {:?} requires at least one replica node",
            descriptor.range_id
        )));
    }
    let mut seen_replicas = BTreeSet::new();
    for node_id in &descriptor.replica_node_ids {
        if !seen_replicas.insert(node_id) {
            return Err(OpenDbError::InvalidInput(format!(
                "range {:?} has duplicate replica node id {node_id}",
                descriptor.range_id
            )));
        }
    }
    if let (Some(key_start), Some(key_end)) = (&descriptor.key_start, &descriptor.key_end)
        && key_start >= key_end
    {
        return Err(OpenDbError::InvalidInput(format!(
            "range {:?} requires key_start < key_end",
            descriptor.range_id
        )));
    }
    if descriptor.parent_range_id == Some(descriptor.range_id) {
        return Err(OpenDbError::InvalidInput(format!(
            "range {:?} cannot be its own parent",
            descriptor.range_id
        )));
    }
    if descriptor.range_id == RangeId::ROOT
        && (descriptor.parent_range_id.is_some()
            || descriptor.key_start.is_some()
            || descriptor.key_end.is_some())
    {
        return Err(OpenDbError::InvalidInput(
            "root range descriptor must not have parent or key bounds".to_string(),
        ));
    }
    Ok(())
}

fn validate_root_descriptor(descriptors: &BTreeMap<RangeId, RangeDescriptor>) -> OpenDbResult<()> {
    if !descriptors.is_empty() && !descriptors.contains_key(&RangeId::ROOT) {
        return Err(OpenDbError::InvalidInput(
            "range catalog requires exactly one root descriptor".to_string(),
        ));
    }
    if let Some(root_descriptor) = descriptors.get(&RangeId::ROOT)
        && (root_descriptor.parent_range_id.is_some()
            || root_descriptor.key_start.is_some()
            || root_descriptor.key_end.is_some())
    {
        return Err(OpenDbError::InvalidInput(
            "root range descriptor must not have parent or key bounds".to_string(),
        ));
    }
    Ok(())
}

fn validate_parent_graph(descriptors: &BTreeMap<RangeId, RangeDescriptor>) -> OpenDbResult<()> {
    for descriptor in descriptors.values() {
        if descriptor.range_id == RangeId::ROOT {
            continue;
        }
        validate_parent_chain(descriptor, descriptors)?;
        validate_child_bounds(descriptor, descriptors)?;
    }
    Ok(())
}

fn validate_parent_chain(
    descriptor: &RangeDescriptor,
    descriptors: &BTreeMap<RangeId, RangeDescriptor>,
) -> OpenDbResult<()> {
    let mut visited = BTreeSet::new();
    let mut current_range_id = descriptor.range_id;

    loop {
        if current_range_id == RangeId::ROOT {
            return if descriptors.contains_key(&RangeId::ROOT) {
                Ok(())
            } else {
                Err(OpenDbError::InvalidInput(format!(
                    "range {:?} references missing parent range {:?}",
                    descriptor.range_id,
                    RangeId::ROOT
                )))
            };
        }
        if !visited.insert(current_range_id) {
            return Err(OpenDbError::InvalidInput(format!(
                "range {:?} has parent cycle through range {:?}",
                descriptor.range_id, current_range_id
            )));
        }
        let current = descriptors.get(&current_range_id).ok_or_else(|| {
            OpenDbError::InvalidInput(format!(
                "range {:?} references missing parent range {:?}",
                descriptor.range_id, current_range_id
            ))
        })?;
        current_range_id = current.parent_range_id.ok_or_else(|| {
            OpenDbError::InvalidInput(format!(
                "range {:?} requires a parent range",
                current.range_id
            ))
        })?;
    }
}

fn validate_child_bounds(
    descriptor: &RangeDescriptor,
    descriptors: &BTreeMap<RangeId, RangeDescriptor>,
) -> OpenDbResult<()> {
    let parent_range_id = descriptor.parent_range_id.expect("parent graph validated");
    let parent = descriptors
        .get(&parent_range_id)
        .expect("parent graph validated");

    if descriptor.key_start.is_none() && parent.key_start.is_some() {
        return Err(OpenDbError::InvalidInput(format!(
            "range {:?} has unbounded key_start outside parent {:?}",
            descriptor.range_id, parent_range_id
        )));
    }
    if let (Some(child_start), Some(parent_start)) = (&descriptor.key_start, &parent.key_start)
        && child_start < parent_start
    {
        return Err(OpenDbError::InvalidInput(format!(
            "range {:?} starts before parent {:?}",
            descriptor.range_id, parent_range_id
        )));
    }
    if descriptor.key_end.is_none() && parent.key_end.is_some() {
        return Err(OpenDbError::InvalidInput(format!(
            "range {:?} has unbounded key_end outside parent {:?}",
            descriptor.range_id, parent_range_id
        )));
    }
    if let (Some(child_end), Some(parent_end)) = (&descriptor.key_end, &parent.key_end)
        && child_end > parent_end
    {
        return Err(OpenDbError::InvalidInput(format!(
            "range {:?} ends after parent {:?}",
            descriptor.range_id, parent_range_id
        )));
    }
    Ok(())
}

fn validate_sibling_ranges(descriptors: &BTreeMap<RangeId, RangeDescriptor>) -> OpenDbResult<()> {
    let mut siblings_by_parent: BTreeMap<RangeId, Vec<&RangeDescriptor>> = BTreeMap::new();
    for descriptor in descriptors.values() {
        if let Some(parent_range_id) = descriptor.parent_range_id {
            siblings_by_parent
                .entry(parent_range_id)
                .or_default()
                .push(descriptor);
        }
    }

    for (parent_range_id, siblings) in siblings_by_parent {
        let mut sorted = siblings;
        sorted.sort_by(|left, right| {
            left.key_start
                .cmp(&right.key_start)
                .then_with(|| left.key_end.cmp(&right.key_end))
                .then_with(|| left.range_id.cmp(&right.range_id))
        });
        for pair in sorted.windows(2) {
            let previous = pair[0];
            let next = pair[1];
            if sibling_ranges_overlap(previous, next) {
                return Err(OpenDbError::InvalidInput(format!(
                    "range {:?} overlaps sibling {:?} under parent {:?}",
                    previous.range_id, next.range_id, parent_range_id
                )));
            }
        }
    }
    Ok(())
}

fn sibling_ranges_overlap(previous: &RangeDescriptor, next: &RangeDescriptor) -> bool {
    match (&previous.key_end, &next.key_start) {
        (Some(previous_end), Some(next_start)) => previous_end > next_start,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive_manifest::{
        ArchiveBackendKind, ArchiveObjectPointer, CompressionKind, RecoveryArtifactKind,
        RecoveryArtifactPointer,
    };
    use crate::commit_stream::{CommitRecord, Mutation};
    use opendb_common::{LogicalTimestamp, RangeId, TransactionId};

    fn root_descriptor() -> RangeDescriptor {
        RangeDescriptor {
            range_id: RangeId::ROOT,
            parent_range_id: None,
            key_start: None,
            key_end: None,
            replica_node_ids: vec![0],
        }
    }

    fn child_descriptor(
        range_id: RangeId,
        key_start: Option<&str>,
        key_end: Option<&str>,
    ) -> RangeDescriptor {
        RangeDescriptor {
            range_id,
            parent_range_id: Some(RangeId::ROOT),
            key_start: key_start.map(str::to_owned),
            key_end: key_end.map(str::to_owned),
            replica_node_ids: vec![0],
        }
    }

    fn root_and_child_record(
        range_id: RangeId,
        key_start: Option<&str>,
        key_end: Option<&str>,
        replica_node_ids: Vec<u64>,
    ) -> CommitRecord {
        CommitRecord::new(
            TransactionId(50),
            LogicalTimestamp(15),
            vec![
                Mutation::PutRangeDescriptor {
                    descriptor: root_descriptor(),
                },
                Mutation::PutRangeDescriptor {
                    descriptor: RangeDescriptor {
                        replica_node_ids,
                        ..child_descriptor(range_id, key_start, key_end)
                    },
                },
            ],
        )
    }

    fn recovery_artifact_record(tx_id: u64) -> CommitRecord {
        CommitRecord::new(
            TransactionId(tx_id),
            LogicalTimestamp(tx_id),
            vec![Mutation::PutRecoveryArtifactPointer {
                artifact: RecoveryArtifactPointer {
                    artifact_kind: RecoveryArtifactKind::WalSegment,
                    range_id: RangeId::ROOT,
                    object: ArchiveObjectPointer {
                        backend: ArchiveBackendKind::S3Compatible,
                        bucket: "opendb-archives".to_owned(),
                        key: "root-range/00000005.wal".to_owned(),
                        content_sha256: "not-validated-by-range-catalog".to_owned(),
                    },
                    format_version: 0,
                    tx_id_start: TransactionId(0),
                    tx_id_end: TransactionId(10),
                    ts_start: LogicalTimestamp(0),
                    ts_end: LogicalTimestamp(10),
                    record_count: 0,
                    byte_len: 0,
                    compression: CompressionKind::None,
                },
            }],
        )
    }

    #[test]
    fn range_catalog_rebuilds_descriptors_from_committed_metadata() {
        let descriptor = RangeDescriptor {
            range_id: RangeId::ROOT,
            parent_range_id: None,
            key_start: None,
            key_end: None,
            replica_node_ids: vec![0, 1, 2],
        };
        let record = CommitRecord::new(
            TransactionId(44),
            LogicalTimestamp(9),
            vec![Mutation::PutRangeDescriptor {
                descriptor: descriptor.clone(),
            }],
        );

        let catalog = RangeCatalog::rebuild(&[record]).expect("rebuild range catalog");

        assert_eq!(catalog.descriptor(RangeId::ROOT), Some(&descriptor));
    }

    #[test]
    fn range_catalog_rejects_child_descriptor_with_missing_parent() {
        let record = CommitRecord::new(
            TransactionId(45),
            LogicalTimestamp(10),
            vec![Mutation::PutRangeDescriptor {
                descriptor: RangeDescriptor {
                    range_id: RangeId(2),
                    parent_range_id: Some(RangeId(404)),
                    key_start: Some("accounts/".to_owned()),
                    key_end: Some("orders/".to_owned()),
                    replica_node_ids: vec![0, 1, 2],
                },
            }],
        );

        let error = RangeCatalog::rebuild(&[record]).expect_err("reject missing parent");

        assert!(
            error.to_string().contains("missing parent range"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn range_catalog_validates_parent_and_child_in_one_commit_atomically() {
        let child = RangeDescriptor {
            range_id: RangeId(2),
            parent_range_id: Some(RangeId::ROOT),
            key_start: Some("accounts/".to_owned()),
            key_end: Some("orders/".to_owned()),
            replica_node_ids: vec![0, 1, 2],
        };
        let root = RangeDescriptor {
            range_id: RangeId::ROOT,
            parent_range_id: None,
            key_start: None,
            key_end: None,
            replica_node_ids: vec![0, 1, 2],
        };
        let record = CommitRecord::new(
            TransactionId(46),
            LogicalTimestamp(11),
            vec![
                Mutation::PutRangeDescriptor {
                    descriptor: child.clone(),
                },
                Mutation::PutRangeDescriptor {
                    descriptor: root.clone(),
                },
            ],
        );

        let catalog = RangeCatalog::rebuild(&[record]).expect("rebuild parent and child");

        assert_eq!(catalog.descriptor(RangeId::ROOT), Some(&root));
        assert_eq!(catalog.descriptor(RangeId(2)), Some(&child));
    }

    #[test]
    fn range_catalog_rejects_invalid_descriptor_shape() {
        let cases = vec![
            (
                RangeDescriptor {
                    range_id: RangeId::ROOT,
                    parent_range_id: Some(RangeId(2)),
                    key_start: None,
                    key_end: None,
                    replica_node_ids: vec![0],
                },
                "root range descriptor must not have parent or key bounds",
            ),
            (
                RangeDescriptor {
                    range_id: RangeId::ROOT,
                    parent_range_id: None,
                    key_start: None,
                    key_end: None,
                    replica_node_ids: Vec::new(),
                },
                "requires at least one replica node",
            ),
            (
                RangeDescriptor {
                    range_id: RangeId::ROOT,
                    parent_range_id: None,
                    key_start: None,
                    key_end: None,
                    replica_node_ids: vec![0, 0],
                },
                "duplicate replica node id",
            ),
            (
                RangeDescriptor {
                    range_id: RangeId(2),
                    parent_range_id: Some(RangeId::ROOT),
                    key_start: Some("z".to_owned()),
                    key_end: Some("a".to_owned()),
                    replica_node_ids: vec![0],
                },
                "requires key_start < key_end",
            ),
            (
                RangeDescriptor {
                    range_id: RangeId(2),
                    parent_range_id: Some(RangeId(2)),
                    key_start: Some("a".to_owned()),
                    key_end: Some("z".to_owned()),
                    replica_node_ids: vec![0],
                },
                "cannot be its own parent",
            ),
        ];

        for (descriptor, expected_message) in cases {
            let record = CommitRecord::new(
                TransactionId(46),
                LogicalTimestamp(11),
                vec![Mutation::PutRangeDescriptor { descriptor }],
            );
            let error = RangeCatalog::rebuild(&[record]).expect_err("reject invalid descriptor");

            assert!(
                error.to_string().contains(expected_message),
                "expected {expected_message:?}, got {error}"
            );
        }
    }

    #[test]
    fn range_catalog_rejects_parent_cycle_in_one_commit() {
        let record = CommitRecord::new(
            TransactionId(51),
            LogicalTimestamp(16),
            vec![
                Mutation::PutRangeDescriptor {
                    descriptor: root_descriptor(),
                },
                Mutation::PutRangeDescriptor {
                    descriptor: RangeDescriptor {
                        range_id: RangeId(2),
                        parent_range_id: Some(RangeId(3)),
                        key_start: Some("a".to_owned()),
                        key_end: Some("m".to_owned()),
                        replica_node_ids: vec![0],
                    },
                },
                Mutation::PutRangeDescriptor {
                    descriptor: RangeDescriptor {
                        range_id: RangeId(3),
                        parent_range_id: Some(RangeId(2)),
                        key_start: Some("m".to_owned()),
                        key_end: Some("z".to_owned()),
                        replica_node_ids: vec![0],
                    },
                },
            ],
        );

        let error = RangeCatalog::rebuild(&[record]).expect_err("reject cycle");

        assert!(
            error.to_string().contains("parent cycle"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn range_catalog_accepts_idempotent_descriptor_update() {
        let first = root_and_child_record(RangeId(2), Some("a"), Some("m"), vec![0]);
        let update = CommitRecord::new(
            TransactionId(52),
            LogicalTimestamp(17),
            vec![Mutation::PutRangeDescriptor {
                descriptor: child_descriptor(RangeId(2), Some("a"), Some("m")),
            }],
        );

        let catalog = RangeCatalog::rebuild(&[first, update]).expect("accept idempotent update");

        assert!(catalog.descriptor(RangeId(2)).is_some());
    }

    #[test]
    fn range_catalog_rejects_conflicting_descriptor_update() {
        let first = root_and_child_record(RangeId(2), Some("a"), Some("m"), vec![0]);
        let update = CommitRecord::new(
            TransactionId(52),
            LogicalTimestamp(17),
            vec![Mutation::PutRangeDescriptor {
                descriptor: RangeDescriptor {
                    range_id: RangeId(2),
                    parent_range_id: Some(RangeId::ROOT),
                    key_start: Some("a".to_owned()),
                    key_end: Some("z".to_owned()),
                    replica_node_ids: vec![0],
                },
            }],
        );

        let error = RangeCatalog::rebuild(&[first, update]).expect_err("reject update");

        assert!(
            error.to_string().contains("conflicting descriptor"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn range_catalog_rejects_overlapping_sibling_ranges() {
        let record = CommitRecord::new(
            TransactionId(53),
            LogicalTimestamp(18),
            vec![
                Mutation::PutRangeDescriptor {
                    descriptor: root_descriptor(),
                },
                Mutation::PutRangeDescriptor {
                    descriptor: child_descriptor(RangeId(2), Some("a"), Some("m")),
                },
                Mutation::PutRangeDescriptor {
                    descriptor: child_descriptor(RangeId(3), Some("k"), Some("z")),
                },
            ],
        );

        let error = RangeCatalog::rebuild(&[record]).expect_err("reject overlap");

        assert!(
            error.to_string().contains("overlap"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn range_catalog_rejects_unbounded_child_edge_inside_bounded_parent() {
        let cases = vec![
            (
                RangeDescriptor {
                    range_id: RangeId(3),
                    parent_range_id: Some(RangeId(2)),
                    key_start: None,
                    key_end: Some("m".to_owned()),
                    replica_node_ids: vec![0],
                },
                "unbounded key_start",
            ),
            (
                RangeDescriptor {
                    range_id: RangeId(3),
                    parent_range_id: Some(RangeId(2)),
                    key_start: Some("m".to_owned()),
                    key_end: None,
                    replica_node_ids: vec![0],
                },
                "unbounded key_end",
            ),
        ];

        for (child, expected_message) in cases {
            let record = CommitRecord::new(
                TransactionId(54),
                LogicalTimestamp(19),
                vec![
                    Mutation::PutRangeDescriptor {
                        descriptor: root_descriptor(),
                    },
                    Mutation::PutRangeDescriptor {
                        descriptor: child_descriptor(RangeId(2), Some("a"), Some("z")),
                    },
                    Mutation::PutRangeDescriptor { descriptor: child },
                ],
            );

            let error =
                RangeCatalog::rebuild(&[record]).expect_err("reject child edge outside parent");

            assert!(
                error.to_string().contains(expected_message),
                "expected {expected_message:?}, got {error}"
            );
        }
    }

    #[test]
    fn range_catalog_accepts_adjacent_siblings_and_gaps() {
        let record = CommitRecord::new(
            TransactionId(55),
            LogicalTimestamp(20),
            vec![
                Mutation::PutRangeDescriptor {
                    descriptor: root_descriptor(),
                },
                Mutation::PutRangeDescriptor {
                    descriptor: child_descriptor(RangeId(2), None, Some("accounts/")),
                },
                Mutation::PutRangeDescriptor {
                    descriptor: child_descriptor(RangeId(3), Some("orders/"), None),
                },
            ],
        );

        let catalog = RangeCatalog::rebuild(&[record]).expect("gaps are allowed in sprint 2");

        assert!(catalog.descriptor(RangeId(2)).is_some());
        assert!(catalog.descriptor(RangeId(3)).is_some());
    }

    #[test]
    fn range_catalog_ignores_recovery_artifact_metadata() {
        let root = root_descriptor();
        let root_record = CommitRecord::new(
            TransactionId(1),
            LogicalTimestamp(1),
            vec![Mutation::PutRangeDescriptor {
                descriptor: root.clone(),
            }],
        );

        let catalog =
            RangeCatalog::rebuild(&[root_record, recovery_artifact_record(2)]).expect("rebuild");

        assert_eq!(catalog.descriptor(RangeId::ROOT), Some(&root));
    }
}
