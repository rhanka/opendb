use crate::commit_stream::{CommitRecord, Mutation, RangeMerge, RangeSplit};
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
    active_range_ids: BTreeSet<RangeId>,
    split_history: Vec<RangeSplit>,
    merge_history: Vec<RangeMerge>,
}

impl RangeCatalog {
    pub fn apply(&mut self, record: &CommitRecord) -> OpenDbResult<()> {
        let mut next = self.clone();
        next.apply_inner(record)?;
        *self = next;
        Ok(())
    }

    fn apply_inner(&mut self, record: &CommitRecord) -> OpenDbResult<()> {
        let mut candidate_descriptors = self.descriptors.clone();
        let mut candidate_active_range_ids = self.active_range_ids.clone();
        let mut candidate_split_history = self.split_history.clone();
        let mut candidate_merge_history = self.merge_history.clone();
        for mutation in &record.mutations {
            match mutation {
                Mutation::PutRangeDescriptor { descriptor } => {
                    apply_descriptor(&mut candidate_descriptors, descriptor)?;
                    candidate_active_range_ids.insert(descriptor.range_id);
                }
                Mutation::SplitRange { split } => {
                    apply_split(
                        &mut candidate_descriptors,
                        &mut candidate_active_range_ids,
                        split,
                    )?;
                    candidate_split_history.push(split.clone());
                }
                Mutation::MergeRanges { merge } => {
                    apply_merge(
                        &mut candidate_descriptors,
                        &mut candidate_active_range_ids,
                        merge,
                    )?;
                    candidate_merge_history.push(merge.clone());
                }
                Mutation::CreateTable { .. }
                | Mutation::InsertRow { .. }
                | Mutation::PutArchiveObjectPointer { .. }
                | Mutation::PutRecoveryArtifactPointer { .. } => {}
            }
        }
        validate_parent_graph(&candidate_descriptors)?;
        validate_root_descriptor(&candidate_descriptors)?;
        validate_active_sibling_ranges(&candidate_descriptors, &candidate_active_range_ids)?;
        self.descriptors = candidate_descriptors;
        self.active_range_ids = candidate_active_range_ids;
        self.split_history = candidate_split_history;
        self.merge_history = candidate_merge_history;
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

    pub fn route_key(&self, key: &str) -> Option<&RangeDescriptor> {
        self.active_range_ids
            .iter()
            .filter_map(|range_id| self.descriptors.get(range_id))
            .filter(|descriptor| descriptor_contains_key(descriptor, key))
            .max_by(|left, right| {
                descriptor_depth(left.range_id, &self.descriptors)
                    .cmp(&descriptor_depth(right.range_id, &self.descriptors))
                    .then_with(|| left.range_id.cmp(&right.range_id))
            })
            .or_else(|| self.descriptors.get(&RangeId::ROOT))
    }

    pub fn split_history(&self) -> &[RangeSplit] {
        &self.split_history
    }

    pub fn merge_history(&self) -> &[RangeMerge] {
        &self.merge_history
    }
}

fn apply_descriptor(
    descriptors: &mut BTreeMap<RangeId, RangeDescriptor>,
    descriptor: &RangeDescriptor,
) -> OpenDbResult<()> {
    validate_descriptor_shape(descriptor)?;
    match descriptors.get(&descriptor.range_id) {
        Some(existing) if existing == descriptor => {}
        Some(existing) => {
            return Err(OpenDbError::InvalidInput(format!(
                "range {:?} has conflicting descriptor update: existing {:?}, new {:?}",
                descriptor.range_id, existing, descriptor
            )));
        }
        None => {
            descriptors.insert(descriptor.range_id, descriptor.clone());
        }
    }
    Ok(())
}

fn apply_split(
    descriptors: &mut BTreeMap<RangeId, RangeDescriptor>,
    active_range_ids: &mut BTreeSet<RangeId>,
    split: &RangeSplit,
) -> OpenDbResult<()> {
    let source = descriptors
        .get(&split.source_range_id)
        .cloned()
        .ok_or_else(|| {
            OpenDbError::InvalidInput(format!(
                "split source range {:?} does not exist",
                split.source_range_id
            ))
        })?;
    if !active_range_ids.contains(&split.source_range_id) {
        return Err(OpenDbError::InvalidInput(format!(
            "split source range {:?} is not active",
            split.source_range_id
        )));
    }
    validate_split_shape(&source, split)?;
    if descriptors.contains_key(&split.left.range_id) {
        return Err(OpenDbError::InvalidInput(format!(
            "split child range {:?} already exists",
            split.left.range_id
        )));
    }
    if descriptors.contains_key(&split.right.range_id) {
        return Err(OpenDbError::InvalidInput(format!(
            "split child range {:?} already exists",
            split.right.range_id
        )));
    }
    apply_descriptor(descriptors, &split.left)?;
    apply_descriptor(descriptors, &split.right)?;
    active_range_ids.remove(&split.source_range_id);
    active_range_ids.insert(split.left.range_id);
    active_range_ids.insert(split.right.range_id);
    Ok(())
}

fn validate_split_shape(source: &RangeDescriptor, split: &RangeSplit) -> OpenDbResult<()> {
    if split.split_key.is_empty() {
        return Err(OpenDbError::InvalidInput(
            "split_key must not be empty".to_string(),
        ));
    }
    if let Some(source_start) = &source.key_start
        && split.split_key.as_str() <= source_start.as_str()
    {
        return Err(OpenDbError::InvalidInput(format!(
            "split_key {:?} must be strictly inside source range {:?}",
            split.split_key, source.range_id
        )));
    }
    if let Some(source_end) = &source.key_end
        && split.split_key.as_str() >= source_end.as_str()
    {
        return Err(OpenDbError::InvalidInput(format!(
            "split_key {:?} must be strictly inside source range {:?}",
            split.split_key, source.range_id
        )));
    }
    if split.left.range_id == split.right.range_id
        || split.left.range_id == split.source_range_id
        || split.right.range_id == split.source_range_id
    {
        return Err(OpenDbError::InvalidInput(format!(
            "split children for range {:?} must use distinct new range ids",
            split.source_range_id
        )));
    }
    if split.left.parent_range_id != Some(split.source_range_id)
        || split.right.parent_range_id != Some(split.source_range_id)
    {
        return Err(OpenDbError::InvalidInput(format!(
            "split children for range {:?} must reference the source as parent",
            split.source_range_id
        )));
    }
    let split_key_bound = Some(split.split_key.clone());
    if split.left.key_start != source.key_start || split.left.key_end != split_key_bound {
        return Err(OpenDbError::InvalidInput(format!(
            "split left child range {:?} must cover the source lower bound to split_key",
            split.left.range_id
        )));
    }
    if split.right.key_start != split_key_bound || split.right.key_end != source.key_end {
        return Err(OpenDbError::InvalidInput(format!(
            "split right child range {:?} must cover split_key to the source upper bound",
            split.right.range_id
        )));
    }
    validate_descriptor_shape(&split.left)?;
    validate_descriptor_shape(&split.right)?;
    Ok(())
}

fn apply_merge(
    descriptors: &mut BTreeMap<RangeId, RangeDescriptor>,
    active_range_ids: &mut BTreeSet<RangeId>,
    merge: &RangeMerge,
) -> OpenDbResult<()> {
    validate_merge_shape(descriptors, active_range_ids, merge)?;
    apply_descriptor(descriptors, &merge.merged)?;
    for source_range_id in &merge.source_range_ids {
        active_range_ids.remove(source_range_id);
    }
    active_range_ids.insert(merge.merged.range_id);
    Ok(())
}

fn validate_merge_shape(
    descriptors: &BTreeMap<RangeId, RangeDescriptor>,
    active_range_ids: &BTreeSet<RangeId>,
    merge: &RangeMerge,
) -> OpenDbResult<()> {
    if merge.source_range_ids.len() < 2 {
        return Err(OpenDbError::InvalidInput(
            "range merge requires at least two source ranges".to_string(),
        ));
    }
    if descriptors.contains_key(&merge.merged.range_id) {
        return Err(OpenDbError::InvalidInput(format!(
            "merged range {:?} already exists",
            merge.merged.range_id
        )));
    }
    validate_descriptor_shape(&merge.merged)?;

    let mut seen_source_ids = BTreeSet::new();
    let mut sources = Vec::new();
    for source_range_id in &merge.source_range_ids {
        if !seen_source_ids.insert(*source_range_id) {
            return Err(OpenDbError::InvalidInput(format!(
                "range merge lists source range {:?} more than once",
                source_range_id
            )));
        }
        if *source_range_id == RangeId::ROOT {
            return Err(OpenDbError::InvalidInput(
                "range merge cannot merge the root range".to_string(),
            ));
        }
        if !active_range_ids.contains(source_range_id) {
            return Err(OpenDbError::InvalidInput(format!(
                "merge source range {:?} is not active",
                source_range_id
            )));
        }
        let source = descriptors.get(source_range_id).cloned().ok_or_else(|| {
            OpenDbError::InvalidInput(format!(
                "merge source range {:?} does not exist",
                source_range_id
            ))
        })?;
        sources.push(source);
    }

    let shared_parent = sources
        .first()
        .and_then(|source| source.parent_range_id)
        .ok_or_else(|| {
            OpenDbError::InvalidInput("range merge sources require a parent range".to_string())
        })?;
    for source in &sources {
        if source.parent_range_id != Some(shared_parent) {
            return Err(OpenDbError::InvalidInput(
                "range merge sources must share one parent range".to_string(),
            ));
        }
    }

    sources.sort_by(|left, right| {
        left.key_start
            .cmp(&right.key_start)
            .then_with(|| left.key_end.cmp(&right.key_end))
            .then_with(|| left.range_id.cmp(&right.range_id))
    });
    for pair in sources.windows(2) {
        let previous = &pair[0];
        let next = &pair[1];
        if previous.key_end.is_none() || previous.key_end != next.key_start {
            return Err(OpenDbError::InvalidInput(
                "range merge sources must be contiguous".to_string(),
            ));
        }
    }

    let first = sources.first().expect("non-empty merge sources");
    let last = sources.last().expect("non-empty merge sources");
    if merge.merged.parent_range_id != Some(shared_parent) {
        return Err(OpenDbError::InvalidInput(format!(
            "merged range {:?} must reference the source parent",
            merge.merged.range_id
        )));
    }
    if merge.merged.key_start != first.key_start || merge.merged.key_end != last.key_end {
        return Err(OpenDbError::InvalidInput(format!(
            "merged range {:?} must cover the outer source bounds",
            merge.merged.range_id
        )));
    }
    Ok(())
}

fn descriptor_contains_key(descriptor: &RangeDescriptor, key: &str) -> bool {
    let starts_after_left = match &descriptor.key_start {
        Some(start) => key >= start.as_str(),
        None => true,
    };
    let ends_before_right = match &descriptor.key_end {
        Some(end) => key < end.as_str(),
        None => true,
    };
    starts_after_left && ends_before_right
}

fn descriptor_depth(range_id: RangeId, descriptors: &BTreeMap<RangeId, RangeDescriptor>) -> usize {
    let mut depth = 0;
    let mut current = descriptors.get(&range_id);
    while let Some(descriptor) = current {
        let Some(parent_range_id) = descriptor.parent_range_id else {
            break;
        };
        depth += 1;
        current = descriptors.get(&parent_range_id);
    }
    depth
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

fn validate_active_sibling_ranges(
    descriptors: &BTreeMap<RangeId, RangeDescriptor>,
    active_range_ids: &BTreeSet<RangeId>,
) -> OpenDbResult<()> {
    let mut siblings_by_parent: BTreeMap<RangeId, Vec<&RangeDescriptor>> = BTreeMap::new();
    for range_id in active_range_ids {
        let descriptor = descriptors.get(range_id).ok_or_else(|| {
            OpenDbError::InvalidInput(format!(
                "active range {:?} does not have a descriptor",
                range_id
            ))
        })?;
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
    use crate::commit_stream::{CommitRecord, Mutation, RangeMerge, RangeSplit};
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

    fn split_root_record() -> CommitRecord {
        CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(2),
            vec![Mutation::SplitRange {
                split: RangeSplit {
                    source_range_id: RangeId::ROOT,
                    split_key: "orders/".to_owned(),
                    left: child_descriptor(RangeId(2), None, Some("orders/")),
                    right: child_descriptor(RangeId(3), Some("orders/"), None),
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
                    merged: child_descriptor(RangeId(4), None, None),
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

    #[test]
    fn range_catalog_routes_to_deepest_active_split_child() {
        let catalog = RangeCatalog::rebuild(&[
            CommitRecord::root_bootstrap(vec![0, 1, 2]),
            split_root_record(),
        ])
        .expect("rebuild split catalog");

        assert_eq!(
            catalog
                .route_key("accounts/1")
                .expect("route accounts")
                .range_id,
            RangeId(2)
        );
        assert_eq!(
            catalog
                .route_key("orders/1")
                .expect("route orders")
                .range_id,
            RangeId(3)
        );
    }

    #[test]
    fn range_catalog_rejects_split_with_boundary_outside_source() {
        let record = CommitRecord::new(
            TransactionId(2),
            LogicalTimestamp(2),
            vec![Mutation::SplitRange {
                split: RangeSplit {
                    source_range_id: RangeId(2),
                    split_key: "z".to_owned(),
                    left: RangeDescriptor {
                        range_id: RangeId(3),
                        parent_range_id: Some(RangeId(2)),
                        key_start: Some("a".to_owned()),
                        key_end: Some("z".to_owned()),
                        replica_node_ids: vec![0],
                    },
                    right: RangeDescriptor {
                        range_id: RangeId(4),
                        parent_range_id: Some(RangeId(2)),
                        key_start: Some("z".to_owned()),
                        key_end: Some("m".to_owned()),
                        replica_node_ids: vec![0],
                    },
                },
            }],
        );

        let error = RangeCatalog::rebuild(&[
            root_and_child_record(RangeId(2), Some("a"), Some("m"), vec![0]),
            record,
        ])
        .expect_err("reject bad split");

        assert!(error.to_string().contains("split_key"));
    }

    #[test]
    fn range_catalog_merges_active_contiguous_siblings() {
        let catalog = RangeCatalog::rebuild(&[
            CommitRecord::root_bootstrap(vec![0, 1, 2]),
            split_root_record(),
            merge_ranges_record(3),
        ])
        .expect("rebuild merged catalog");

        assert_eq!(
            catalog
                .route_key("accounts/1")
                .expect("route accounts")
                .range_id,
            RangeId(4)
        );
        assert_eq!(
            catalog
                .route_key("orders/1")
                .expect("route orders")
                .range_id,
            RangeId(4)
        );
    }

    #[test]
    fn range_catalog_rejects_merge_with_gap() {
        let record = CommitRecord::new(
            TransactionId(3),
            LogicalTimestamp(3),
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
        );

        let error = RangeCatalog::rebuild(&[
            CommitRecord::root_bootstrap(vec![0]),
            CommitRecord::new(
                TransactionId(2),
                LogicalTimestamp(2),
                vec![
                    Mutation::PutRangeDescriptor {
                        descriptor: child_descriptor(RangeId(2), None, Some("accounts/")),
                    },
                    Mutation::PutRangeDescriptor {
                        descriptor: child_descriptor(RangeId(3), Some("orders/"), None),
                    },
                ],
            ),
            record,
        ])
        .expect_err("reject gapped merge");

        assert!(error.to_string().contains("contiguous"));
    }
}
