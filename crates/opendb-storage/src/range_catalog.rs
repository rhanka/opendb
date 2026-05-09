use crate::commit_stream::{CommitRecord, Mutation};
use opendb_common::{OpenDbError, OpenDbResult, RangeId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
                    candidate.insert(descriptor.range_id, descriptor.clone());
                }
                Mutation::CreateTable { .. } | Mutation::InsertRow { .. } => {}
            }
        }
        for mutation in &record.mutations {
            if let Mutation::PutRangeDescriptor { descriptor } = mutation {
                validate_descriptor_parent(descriptor, &candidate)?;
            }
        }
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

fn validate_descriptor_parent(
    descriptor: &RangeDescriptor,
    descriptors: &BTreeMap<RangeId, RangeDescriptor>,
) -> OpenDbResult<()> {
    if descriptor.range_id == RangeId::ROOT {
        return Ok(());
    }
    let parent_range_id = descriptor.parent_range_id.ok_or_else(|| {
        OpenDbError::InvalidInput(format!(
            "range {:?} requires a parent range",
            descriptor.range_id
        ))
    })?;
    if !descriptors.contains_key(&parent_range_id) {
        return Err(OpenDbError::InvalidInput(format!(
            "range {:?} references missing parent range {:?}",
            descriptor.range_id, parent_range_id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_stream::{CommitRecord, Mutation};
    use opendb_common::{LogicalTimestamp, RangeId, TransactionId};

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
}
