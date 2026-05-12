# OpenDB Milestone 2 Sprint 4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add typed split/merge metadata and catalog-derived logical routing while keeping one canonical commit stream and the existing pgwire edge.

**Architecture:** The root OpenRaft stream remains the only physical commit stream. `RangeCatalog` becomes the derived routing projection for logical `RangeId` ownership, `SqlEngine` emits route intent, and `Database` resolves that intent before submitting row records. No physical range WALs, no object storage, no Kubernetes correctness logic.

**Tech Stack:** Rust, Tokio, Serde, serde_json, OpenRaft-backed root range, TypeScript, Vitest, Kubernetes/k3s static manifests. No Python.

---

## Source Spec

Implement the approved design:

- `docs/superpowers/specs/2026-05-11-opendb-milestone-2-sprint-4-design.md`

## File Structure

- `crates/opendb-storage/src/commit_stream.rs`
  Owns `RangeSplit`, `RangeMerge`, new mutation variants, and helper
  constructors for committing records to a logical range.
- `crates/opendb-storage/src/range_catalog.rs`
  Owns active range projection, split/merge validation, and key-to-range route
  lookup.
- `crates/opendb-storage/src/row_projection.rs`
  Explicitly ignores split/merge metadata while preserving row projection
  replay.
- `crates/opendb-storage/src/archive_manifest.rs`
  Explicitly ignores split/merge metadata while preserving archive metadata
  replay.
- `crates/opendb-storage/src/wal.rs`
  Adds WAL append/read and strict-decode tests for split/merge records.
- `crates/opendb-storage/tests/wal_golden.rs`
  Adds a second golden WAL frame fixture for split metadata.
- `crates/opendb-storage/tests/fixtures/wal/frame-v1-record-v2-range-split.hex`
  New full-frame WAL fixture generated only by Rust or TypeScript tooling.
- `crates/opendb-sql/src/executor.rs`
  Adds `RouteIntent` to `PreparedQuery` and computes route keys from primary
  keys.
- `crates/opendb-node/src/database.rs`
  Rebuilds and stores the catalog from WAL, resolves route intent, stamps write
  records with the selected range, and tests end-to-end logical routing.
- `crates/opendb-consensus/src/root_range.rs`
  Replaces root-only `range_id` validation with canonical-stream validation:
  metadata records stay root; row records may target catalog-known logical
  ranges.
- `tools/k3s-smoke.ts`, `tests/cluster/*.test.ts`, `deploy/k8s/base/*.yaml`
  No planned changes. If implementation changes smoke output or manifests, add
  explicit TypeScript tests in the same commit.

## Task 1: Commit Stream Split/Merge Metadata

**Ownership:** One worker owns `crates/opendb-storage/src/commit_stream.rs`,
the compile fixes for exhaustive mutation matches in storage crates, and the
serialization tests for this task. Other workers should not edit
`range_catalog.rs` until Task 1 lands.

**Files:**
- Modify: `crates/opendb-storage/src/commit_stream.rs`
- Modify: `crates/opendb-storage/src/row_projection.rs`
- Modify: `crates/opendb-storage/src/archive_manifest.rs`

- [ ] **Step 1: Add failing commit-stream tests**

Add tests near the existing metadata serialization tests in
`crates/opendb-storage/src/commit_stream.rs`:

```rust
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
```

Run:

```bash
cargo test -p opendb-storage commit_record_serializes_range_split_metadata_mutation
```

Expected before implementation: compile failure because `RangeSplit`,
`RangeMerge`, `Mutation::SplitRange`, `Mutation::MergeRanges`, and
`CommitRecord::new_for_range` do not exist.

- [ ] **Step 2: Implement metadata structs and mutation variants**

Add to `crates/opendb-storage/src/commit_stream.rs` after `ColumnValue`:

```rust
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
```

Extend `Mutation`:

```rust
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
```

Add a range-aware constructor:

```rust
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
```

Then change `new_with_actor` to call `new_for_range(RangeId::ROOT, ...)`.

- [ ] **Step 3: Update exhaustive metadata ignores**

In `crates/opendb-storage/src/row_projection.rs`, extend the ignored metadata
match arm:

```rust
Mutation::PutRangeDescriptor { .. }
| Mutation::SplitRange { .. }
| Mutation::MergeRanges { .. }
| Mutation::PutArchiveObjectPointer { .. }
| Mutation::PutRecoveryArtifactPointer { .. } => {}
```

In `crates/opendb-storage/src/archive_manifest.rs`, keep archive handling as-is
and extend non-archive metadata ignores with `SplitRange` and `MergeRanges`.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cargo test -p opendb-storage commit_record_serializes_range_split_metadata_mutation
cargo test -p opendb-storage commit_record_serializes_range_merge_metadata_mutation
cargo test -p opendb-storage commit_record_builds_record_for_logical_range
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/opendb-storage/src/commit_stream.rs crates/opendb-storage/src/row_projection.rs crates/opendb-storage/src/archive_manifest.rs
git commit -m "feat: add typed range split merge metadata"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

Expected grep count: `0`.

## Task 2: Active Range Catalog And Routing

**Ownership:** One worker owns `crates/opendb-storage/src/range_catalog.rs`.
Do not edit SQL, node, or consensus files in this task.

**Files:**
- Modify: `crates/opendb-storage/src/range_catalog.rs`

- [ ] **Step 1: Add failing range catalog tests**

Add tests at the end of `crates/opendb-storage/src/range_catalog.rs`:

```rust
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

#[test]
fn range_catalog_routes_to_deepest_active_split_child() {
    let catalog = RangeCatalog::rebuild(&[
        CommitRecord::root_bootstrap(vec![0, 1, 2]),
        split_root_record(),
    ])
    .expect("rebuild split catalog");

    assert_eq!(
        catalog.route_key("accounts/1").expect("route accounts").range_id,
        RangeId(2)
    );
    assert_eq!(
        catalog.route_key("orders/1").expect("route orders").range_id,
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
    let merge = CommitRecord::new(
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
                    replica_node_ids: vec![0, 1, 2],
                },
            },
        }],
    );
    let catalog = RangeCatalog::rebuild(&[
        CommitRecord::root_bootstrap(vec![0, 1, 2]),
        split_root_record(),
        merge,
    ])
    .expect("rebuild merged catalog");

    assert_eq!(
        catalog.route_key("accounts/1").expect("route accounts").range_id,
        RangeId(4)
    );
    assert_eq!(
        catalog.route_key("orders/1").expect("route orders").range_id,
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
```

Run:

```bash
cargo test -p opendb-storage range_catalog_routes_to_deepest_active_split_child
```

Expected before implementation: compile failure or failing assertions.

- [ ] **Step 2: Extend imports and state**

Update imports:

```rust
use crate::commit_stream::{CommitRecord, Mutation, RangeMerge, RangeSplit};
```

Change `RangeCatalog`:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeCatalog {
    descriptors: BTreeMap<RangeId, RangeDescriptor>,
    active_range_ids: BTreeSet<RangeId>,
    split_history: Vec<RangeSplit>,
    merge_history: Vec<RangeMerge>,
}
```

Implement `Default` manually so `active_range_ids`, `split_history`, and
`merge_history` start empty.

- [ ] **Step 3: Route lookup helper**

Add:

```rust
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
```

Add helper functions:

```rust
fn descriptor_contains_key(descriptor: &RangeDescriptor, key: &str) -> bool {
    let starts_after_left = descriptor
        .key_start
        .as_ref()
        .is_none_or(|start| key >= start);
    let ends_before_right = descriptor
        .key_end
        .as_ref()
        .is_none_or(|end| key < end);
    starts_after_left && ends_before_right
}

fn descriptor_depth(
    range_id: RangeId,
    descriptors: &BTreeMap<RangeId, RangeDescriptor>,
) -> usize {
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
```

If the MSRV rejects `Option::is_none_or`, replace it with explicit `match`.

- [ ] **Step 4: Apply split/merge metadata atomically**

Refactor `apply_inner` to clone all catalog state into candidates, then apply
mutations to the candidates. Add match arms:

```rust
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
```

Keep non-range mutations ignored in this projection.

- [ ] **Step 5: Split validation helpers**

Implement:

```rust
fn apply_split(
    descriptors: &mut BTreeMap<RangeId, RangeDescriptor>,
    active_range_ids: &mut BTreeSet<RangeId>,
    split: &RangeSplit,
) -> OpenDbResult<()> {
    let source = descriptors
        .get(&split.source_range_id)
        .cloned()
        .ok_or_else(|| OpenDbError::InvalidInput(format!(
            "split source range {:?} does not exist",
            split.source_range_id
        )))?;
    if !active_range_ids.contains(&split.source_range_id) {
        return Err(OpenDbError::InvalidInput(format!(
            "split source range {:?} is not active",
            split.source_range_id
        )));
    }
    validate_split_shape(&source, split)?;
    apply_descriptor(descriptors, &split.left)?;
    apply_descriptor(descriptors, &split.right)?;
    active_range_ids.remove(&split.source_range_id);
    active_range_ids.insert(split.left.range_id);
    active_range_ids.insert(split.right.range_id);
    Ok(())
}
```

`validate_split_shape` must check source bounds, child parent ids, child
bounds, child ids are different from each other and from the source, and
`split_key` is strictly inside the source bounds.

- [ ] **Step 6: Merge validation helpers**

Implement:

```rust
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
```

`validate_merge_shape` must sort source descriptors by `key_start`, require one
shared parent, require every source active, require adjacent bounds
(`previous.key_end == next.key_start` with `None` allowed only at outer edges),
and require merged bounds to match the outer source bounds.

- [ ] **Step 7: Validate active overlaps**

Keep existing parent graph/root/shape validation. Change sibling overlap
validation to operate on active descriptors:

```rust
validate_active_sibling_ranges(&candidate_descriptors, &candidate_active_range_ids)?;
```

The function should ignore historical inactive descriptors, but active siblings
under the same parent must remain non-overlapping.

- [ ] **Step 8: Run focused tests**

Run:

```bash
cargo test -p opendb-storage range_catalog_
```

Expected: all range catalog tests pass.

- [ ] **Step 9: Commit**

```bash
git add crates/opendb-storage/src/range_catalog.rs
git commit -m "feat: derive active range routing from catalog metadata"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

Expected grep count: `0`.

## Task 3: WAL Compatibility Coverage

**Ownership:** One worker owns WAL tests and fixtures only. Do not change
production code except imports required by tests.

**Files:**
- Modify: `crates/opendb-storage/src/wal.rs`
- Modify: `crates/opendb-storage/tests/wal_golden.rs`
- Create: `crates/opendb-storage/tests/fixtures/wal/frame-v1-record-v2-range-split.hex`

- [ ] **Step 1: Add WAL append/read tests**

In `crates/opendb-storage/src/wal.rs`, add tests near the existing metadata WAL
tests:

```rust
#[tokio::test]
async fn wal_appends_and_reads_range_split_record() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let wal = Wal::new(temp_dir.path().join("root-range").join("commit.wal"));
    let record = CommitRecord::new(
        TransactionId(6),
        LogicalTimestamp(15),
        vec![Mutation::SplitRange {
            split: RangeSplit {
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
            },
        }],
    );

    wal.append(&record).await.expect("append split record");

    assert_eq!(wal.read_all().await.expect("read split record"), vec![record]);
}

#[tokio::test]
async fn wal_rejects_unknown_field_in_range_split_record() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let wal_path = temp_dir.path().join("commit.wal");
    let payload = br#"{"version":2,"tx_id":1,"range_id":1,"ts":1,"actor":"system","mutations":[{"SplitRange":{"split":{"source_range_id":1,"split_key":"orders/","left":{"range_id":2,"parent_range_id":1,"key_start":null,"key_end":"orders/","replica_node_ids":[0]},"right":{"range_id":3,"parent_range_id":1,"key_start":"orders/","key_end":null,"replica_node_ids":[0]},"unexpected":true}}}]}"#;
    fs::write(&wal_path, encode_raw_payload_frame(payload))
        .await
        .expect("write fixture");

    let error = Wal::new(&wal_path)
        .read_all()
        .await
        .expect_err("reject unknown split field");

    assert!(error.to_string().contains("unknown field"));
}
```

Use the existing test helpers in `wal.rs`. Adjust helper names if they differ.

- [ ] **Step 2: Generate the split golden fixture without Python**

Use a temporary Rust test or TypeScript snippet to print a full WAL frame as
hex for this exact record:

```rust
CommitRecord::new(
    TransactionId(6),
    LogicalTimestamp(15),
    vec![Mutation::SplitRange {
        split: RangeSplit {
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
        },
    }],
)
```

Commit only the fixture, not the temporary generator.

- [ ] **Step 3: Add golden fixture test**

Refactor `crates/opendb-storage/tests/wal_golden.rs` to avoid global length
constants. Add helper:

```rust
fn assert_frame(bytes: &[u8], expected_payload_len: usize) {
    assert_eq!(&bytes[0..4], b"ODW1");
    assert_eq!(u16::from_le_bytes([bytes[4], bytes[5]]), FRAME_VERSION);
    assert_eq!(u16::from_le_bytes([bytes[6], bytes[7]]), FRAME_RESERVED);
    let payload_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
    assert_eq!(payload_len, expected_payload_len);
    assert_eq!(FRAME_HEADER_LEN + payload_len, bytes.len());
    let expected_checksum = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    assert_eq!(
        expected_checksum,
        frame_checksum(&bytes[4..8], &bytes[8..12], &bytes[FRAME_HEADER_LEN..])
    );
}
```

Then add:

```rust
#[tokio::test]
async fn wal_reads_frame_v1_record_v2_range_split_fixture() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let path = temp_dir.path().join("commit.wal");
    let bytes = decode_hex(include_str!(
        "fixtures/wal/frame-v1-record-v2-range-split.hex"
    ));
    assert_frame(&bytes, bytes.len() - FRAME_HEADER_LEN);
    tokio::fs::write(&path, bytes).await.expect("write wal fixture");

    let records = Wal::new(&path).read_all().await.expect("read fixture");

    assert_eq!(records.len(), 1);
    assert!(matches!(
        records[0].mutations.as_slice(),
        [Mutation::SplitRange { .. }]
    ));
}
```

Import `Mutation` from `opendb_storage::commit_stream`.

- [ ] **Step 4: Run focused tests**

```bash
cargo test -p opendb-storage wal_appends_and_reads_range_split_record
cargo test -p opendb-storage wal_rejects_unknown_field_in_range_split_record
cargo test -p opendb-storage --test wal_golden
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/opendb-storage/src/wal.rs crates/opendb-storage/tests/wal_golden.rs crates/opendb-storage/tests/fixtures/wal/frame-v1-record-v2-range-split.hex
git commit -m "test: cover range split wal compatibility"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

Expected grep count: `0`.

## Task 4: SQL Route Intent

**Ownership:** One worker owns `crates/opendb-sql/src/executor.rs`. Do not edit
node or consensus files in this task.

**Files:**
- Modify: `crates/opendb-sql/src/executor.rs`

- [ ] **Step 1: Add failing route intent tests**

Add tests near existing executor tests:

```rust
#[test]
fn insert_prepare_returns_primary_key_route_intent() {
    let mut engine = SqlEngine::default();
    engine
        .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
        .expect("create");

    let prepared = engine
        .prepare(parse("INSERT INTO accounts VALUES (7, 'Ada')").expect("parse"))
        .expect("prepare insert");

    assert!(matches!(
        prepared,
        PreparedQuery::Write {
            route: RouteIntent::Key { ref table, ref key },
            ..
        } if table == "accounts" && key == "accounts/7"
    ));
}

#[test]
fn select_where_prepare_returns_primary_key_route_intent() {
    let mut engine = SqlEngine::default();
    engine
        .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
        .expect("create");

    let prepared = engine
        .prepare(parse("SELECT * FROM accounts WHERE id = 7").expect("parse"))
        .expect("prepare select");

    assert!(matches!(
        prepared,
        PreparedQuery::Read {
            route: RouteIntent::Key { ref table, ref key },
            ..
        } if table == "accounts" && key == "accounts/7"
    ));
}

#[test]
fn create_table_prepare_returns_root_route_intent() {
    let engine = SqlEngine::default();

    let prepared = engine
        .prepare(parse("CREATE TABLE accounts (id INT PRIMARY KEY)").expect("parse"))
        .expect("prepare create");

    assert!(matches!(
        prepared,
        PreparedQuery::Write {
            route: RouteIntent::Root,
            ..
        }
    ));
}

#[test]
fn select_scan_prepare_returns_scan_route_intent() {
    let mut engine = SqlEngine::default();
    engine
        .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
        .expect("create");

    let prepared = engine
        .prepare(parse("SELECT * FROM accounts").expect("parse"))
        .expect("prepare scan");

    assert!(matches!(
        prepared,
        PreparedQuery::Read {
            route: RouteIntent::Scan { ref table },
            ..
        } if table == "accounts"
    ));
}
```

Run:

```bash
cargo test -p opendb-sql route_intent
```

Expected before implementation: compile failure.

- [ ] **Step 2: Add route intent types**

Change `PreparedQuery`:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RouteIntent {
    Root,
    Key { table: String, key: String },
    Scan { table: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreparedQuery {
    Read {
        result: QueryResult,
        route: RouteIntent,
    },
    Write {
        record: CommitRecord,
        tag: String,
        route: RouteIntent,
    },
}
```

Update `execute` and all tests that match `PreparedQuery::Read(result)` or
`PreparedQuery::Write { record, tag }`.

- [ ] **Step 3: Build canonical route keys**

Add helper:

```rust
fn route_key(table: &str, row_key: &str) -> String {
    format!("{table}/{row_key}")
}
```

In insert preparation, after `row_key` is computed, return:

```rust
self.prepare_write(
    vec![Mutation::InsertRow {
        table: table.clone(),
        key: row_key.clone(),
        values: column_values,
    }],
    "INSERT 0 1",
    RouteIntent::Key {
        table,
        key: route_key(&table, &row_key),
    },
)
```

Avoid moving `table` before building the route key by cloning once if needed.

- [ ] **Step 4: Attach read route intent**

Refactor `select_all` to return `(QueryResult, RouteIntent)` or add a helper
that computes route intent next to the result.

For primary-key predicates:

```rust
RouteIntent::Key {
    table: table.to_owned(),
    key: route_key(table, &row_key),
}
```

For scans:

```rust
RouteIntent::Scan {
    table: table.to_owned(),
}
```

- [ ] **Step 5: Run focused tests**

```bash
cargo test -p opendb-sql insert_prepare_returns_primary_key_route_intent
cargo test -p opendb-sql select_where_prepare_returns_primary_key_route_intent
cargo test -p opendb-sql create_table_prepare_returns_root_route_intent
cargo test -p opendb-sql select_scan_prepare_returns_scan_route_intent
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add crates/opendb-sql/src/executor.rs
git commit -m "feat: expose sql route intent"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

Expected grep count: `0`.

## Task 5: Database Catalog Routing

**Ownership:** One worker owns `crates/opendb-node/src/database.rs`. Coordinate
with Task 6 because consensus validation will initially reject non-root row
records until Task 6 lands.

**Files:**
- Modify: `crates/opendb-node/src/database.rs`

- [ ] **Step 1: Add failing database routing test**

Add test in `crates/opendb-node/src/database.rs`:

```rust
#[tokio::test]
async fn execute_stamps_insert_with_catalog_routed_range() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let root_range = RootRange::new(temp_dir.path());
    let mut database = Database::open_with_root_range(root_range.clone())
        .await
        .expect("open database");

    database
        .execute(parse("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)").expect("parse"))
        .await
        .expect("create");

    root_range
        .apply_committed(&CommitRecord::new(
            opendb_common::TransactionId(2),
            opendb_common::LogicalTimestamp(2),
            vec![opendb_storage::commit_stream::Mutation::SplitRange {
                split: opendb_storage::commit_stream::RangeSplit {
                    source_range_id: opendb_common::RangeId::ROOT,
                    split_key: "orders/".to_owned(),
                    left: opendb_storage::range_catalog::RangeDescriptor {
                        range_id: opendb_common::RangeId(2),
                        parent_range_id: Some(opendb_common::RangeId::ROOT),
                        key_start: None,
                        key_end: Some("orders/".to_owned()),
                        replica_node_ids: vec![0],
                    },
                    right: opendb_storage::range_catalog::RangeDescriptor {
                        range_id: opendb_common::RangeId(3),
                        parent_range_id: Some(opendb_common::RangeId::ROOT),
                        key_start: Some("orders/".to_owned()),
                        key_end: None,
                        replica_node_ids: vec![0],
                    },
                },
            }],
        ))
        .await
        .expect("append split metadata");

    database
        .execute(parse("INSERT INTO accounts VALUES (1, 'Ada')").expect("parse"))
        .await
        .expect("insert");

    let records = root_range.replay().await.expect("replay");
    assert_eq!(records.last().expect("last record").range_id, opendb_common::RangeId(2));
}
```

This test may fail until Task 6 relaxes root-only validation. Keep it in this
task and coordinate merge order.

- [ ] **Step 2: Store rebuilt catalog in Database**

Import:

```rust
use opendb_sql::executor::{PreparedQuery, RouteIntent, SqlEngine};
use opendb_storage::{commit_stream::CommitRecord, range_catalog::RangeCatalog};
```

Add field:

```rust
range_catalog: RangeCatalog,
```

In both open constructors:

```rust
let range_catalog = RangeCatalog::rebuild(&records)?;
let engine = SqlEngine::from_commits(records)?;
```

- [ ] **Step 3: Refresh catalog with engine**

Change `refresh_engine_from_wal`:

```rust
async fn refresh_engine_from_wal(&mut self) -> OpenDbResult<()> {
    let records = self.root_range.replay().await?;
    self.range_catalog = RangeCatalog::rebuild(&records)?;
    self.engine = SqlEngine::from_commits(records)?;
    Ok(())
}
```

- [ ] **Step 4: Resolve route intent**

Add:

```rust
fn resolve_route(&self, route: &RouteIntent) -> OpenDbResult<opendb_common::RangeId> {
    match route {
        RouteIntent::Root | RouteIntent::Scan { .. } => Ok(opendb_common::RangeId::ROOT),
        RouteIntent::Key { key, .. } => self
            .range_catalog
            .route_key(key)
            .map(|descriptor| descriptor.range_id)
            .ok_or_else(|| OpenDbError::Storage(format!("no range route for key {key}"))),
    }
}
```

- [ ] **Step 5: Stamp writes with resolved range**

Update `execute`:

```rust
match self.engine.prepare(statement)? {
    PreparedQuery::Read { result, route } => {
        let _target_range_id = self.resolve_route(&route)?;
        Ok(result)
    }
    PreparedQuery::Write {
        mut record,
        tag,
        route,
    } => {
        self.ensure_leader_for_client_query().await?;
        record.range_id = self.resolve_route(&route)?;
        self.root_range
            .submit(RootRangeCommand {
                record: record.clone(),
            })
            .await?;
        self.engine.apply_committed(record)?;
        Ok(QueryResult::Command { tag })
    }
}
```

Keep the existing read leadership behavior before the match.

- [ ] **Step 6: Run focused tests**

```bash
cargo test -p opendb-node execute_stamps_insert_with_catalog_routed_range
```

Expected after Task 6: pass. If run before Task 6, expected failure is root-only
validation rejecting `RangeId(2)`.

- [ ] **Step 7: Commit**

```bash
git add crates/opendb-node/src/database.rs
git commit -m "feat: resolve database operations through range catalog"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

Expected grep count: `0`.

## Task 6: Root Stream Logical Range Validation

**Ownership:** One worker owns `crates/opendb-consensus/src/root_range.rs`.
Coordinate with Task 5 for the database integration test.

**Files:**
- Modify: `crates/opendb-consensus/src/root_range.rs`

- [ ] **Step 1: Add failing consensus validation tests**

Update the old non-root rejection tests. Replace
`replay_rejects_forged_non_root_records_in_root_wal` with:

```rust
#[tokio::test]
async fn replay_accepts_known_routed_non_root_row_record() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let wal = Wal::new(temp_dir.path().join("root-range").join("commit.wal"));
    wal.append(&CommitRecord::root_bootstrap(vec![0]))
        .await
        .expect("append bootstrap");
    wal.append(&CommitRecord::new(
        TransactionId(1),
        LogicalTimestamp(1),
        vec![Mutation::CreateTable {
            table: "accounts".to_owned(),
            columns: account_columns(),
        }],
    ))
    .await
    .expect("append create");
    wal.append(&CommitRecord::new(
        TransactionId(2),
        LogicalTimestamp(2),
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
    ))
    .await
    .expect("append split");
    wal.append(&CommitRecord::new_for_range(
        RangeId(2),
        TransactionId(3),
        LogicalTimestamp(3),
        CommitRecord::BOOTSTRAP_ACTOR,
        vec![Mutation::InsertRow {
            table: "accounts".to_owned(),
            key: "1".to_owned(),
            values: vec![
                ColumnValue {
                    column: "id".to_owned(),
                    value: Value::Int64(1),
                },
                ColumnValue {
                    column: "name".to_owned(),
                    value: Value::Text("Ada".to_owned()),
                },
            ],
        }],
    ))
    .await
    .expect("append routed insert");

    let records = RootRange::new(temp_dir.path())
        .replay()
        .await
        .expect("replay routed WAL");

    assert_eq!(records.last().expect("last").range_id, RangeId(2));
}

#[tokio::test]
async fn replay_rejects_unknown_non_root_row_range() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let wal = Wal::new(temp_dir.path().join("root-range").join("commit.wal"));
    wal.append(&CommitRecord::root_bootstrap(vec![0]))
        .await
        .expect("append bootstrap");
    wal.append(&CommitRecord::new(
        TransactionId(1),
        LogicalTimestamp(1),
        vec![Mutation::CreateTable {
            table: "accounts".to_owned(),
            columns: vec![ColumnDefinition::primary_key("id", ColumnType::Int64)],
        }],
    ))
    .await
    .expect("append create");
    wal.append(&CommitRecord::new_for_range(
        RangeId(404),
        TransactionId(2),
        LogicalTimestamp(2),
        CommitRecord::BOOTSTRAP_ACTOR,
        vec![Mutation::InsertRow {
            table: "accounts".to_owned(),
            key: "1".to_owned(),
            values: vec![ColumnValue {
                column: "id".to_owned(),
                value: Value::Int64(1),
            }],
        }],
    ))
    .await
    .expect("append forged insert");

    let error = RootRange::new(temp_dir.path())
        .replay()
        .await
        .expect_err("reject unknown range");

    assert!(error.to_string().contains("range route"));
}
```

Import `ColumnDefinition`, `ColumnType`, `ColumnValue`, `RangeSplit`, and
`Value`.

- [ ] **Step 2: Remove unconditional root range-id rejection**

In `validate_apply_record` and `validate_replayed_record`, keep version
validation but remove the unconditional `record.range_id != self.range_id`
error. Root bootstrap validation still protects the first record.

- [ ] **Step 3: Add semantic route validation**

After `RangeCatalog::rebuild(&records)?` in `validate_semantic_append` and
`replay`, validate row routes:

```rust
validate_record_routes(&records)?;
```

Implement:

```rust
fn validate_record_routes(records: &[CommitRecord]) -> OpenDbResult<()> {
    let mut catalog = RangeCatalog::default();
    for record in records {
        validate_metadata_record_range(record)?;
        catalog.apply(record)?;
        for mutation in &record.mutations {
            if let Mutation::InsertRow { table, key, .. } = mutation {
                let route_key = format!("{table}/{key}");
                let expected = catalog
                    .route_key(&route_key)
                    .map(|descriptor| descriptor.range_id)
                    .ok_or_else(|| OpenDbError::Storage(format!(
                        "no range route for key {route_key}"
                    )))?;
                if record.range_id != expected {
                    return Err(OpenDbError::Storage(format!(
                        "row route key {route_key} expected range {:?}, got {:?}",
                        expected, record.range_id
                    )));
                }
            }
        }
    }
    Ok(())
}
```

`validate_metadata_record_range` must require root range id for records that
contain `CreateTable`, `PutRangeDescriptor`, `SplitRange`, `MergeRanges`,
`PutArchiveObjectPointer`, or `PutRecoveryArtifactPointer`. If a record mixes
metadata and row mutations, reject it with `InvalidInput` on append and
`Storage` on replay by mapping append errors through
`sequence_validation_error_for_append`.

- [ ] **Step 4: Preserve append error mapping**

Ensure invalid route decisions during append return `OpenDbError::InvalidInput`
to callers by reusing `sequence_validation_error_for_append` for route
validation errors in `validate_semantic_append`.

- [ ] **Step 5: Run focused tests**

```bash
cargo test -p opendb-consensus replay_accepts_known_routed_non_root_row_record
cargo test -p opendb-consensus replay_rejects_unknown_non_root_row_range
cargo test -p opendb-node execute_stamps_insert_with_catalog_routed_range
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add crates/opendb-consensus/src/root_range.rs crates/opendb-node/src/database.rs
git commit -m "feat: validate logical range routes in root stream"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

Expected grep count: `0`.

## Task 7: TypeScript And Documentation Guardrails

**Ownership:** One worker owns docs/tests guardrails only. Do not change Rust
logic in this task.

**Files:**
- Modify only if needed: `docs/k3s-uat.md`
- Modify only if smoke output changes: `tests/cluster/k3s-smoke.test.ts`
- Modify only if manifests change: `tests/cluster/manifests.test.ts`

- [ ] **Step 1: Check whether docs need a user-visible update**

If Sprint 4 does not change pgwire behavior, smoke behavior, manifests,
operator status, or UAT commands, leave `docs/k3s-uat.md` unchanged.

If a user-visible note is needed, add one sentence under the recovery contract
section:

```markdown
Sprint 4 range split/merge metadata is replayed from the canonical WAL only; it does not add object storage, extra pods, or a destructive smoke default.
```

- [ ] **Step 2: Preserve non-destructive smoke tests**

Run:

```bash
npm run test:cluster
```

Expected: tests pass and existing assertions still prove that default
`npm run smoke:k3s` does not contain `kubectl delete pod`.

- [ ] **Step 3: Preserve static manifest checks**

Run:

```bash
npm run check:manifests
```

Expected:

```text
Kubernetes manifests passed static checks.
```

- [ ] **Step 4: Commit only if files changed**

If no docs, tests, or manifests changed, do not create an empty commit.

If files changed:

```bash
git add docs/k3s-uat.md tests/cluster/k3s-smoke.test.ts tests/cluster/manifests.test.ts
git commit -m "docs: note range metadata smoke guardrails"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

Expected grep count: `0`.

## Final Verification

Run before declaring Sprint 4 implementation complete:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm run check:ts
npm run check:no-python
npm run check:manifests
npm test
git diff --check HEAD
```

If `cargo test --workspace` fails because OpenRaft tests need unsandboxed local
ports, rerun the same command outside the sandbox with approval instead of
weakening tests.

If k3d is available and images are current, run:

```bash
npm run smoke:k3s
npm run smoke:k3s -- --with-restart-recovery
```

The first command must remain non-destructive. The second command is the
explicit restart-recovery opt-in.

After every push, verify the strict no-AI-attribution rule:

```bash
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

Expected:

```text
0
```

## Review Checklist

- [ ] Commit stream remains the only source of truth.
- [ ] Split/merge are typed metadata, not separate subsystems.
- [ ] `CommitRecord::VERSION` remains 2 with the compatibility rationale from
      the spec.
- [ ] Root bootstrap remains the first record.
- [ ] Metadata records use `RangeId::ROOT`.
- [ ] Row records may use non-root logical range ids only when catalog routing
      derives the same id.
- [ ] pgwire remains an edge compatibility layer only.
- [ ] No object-storage client or service is introduced.
- [ ] No Python files or scripts are introduced.
- [ ] `npm run smoke:k3s` remains non-destructive by default.
- [ ] `OpenDbCluster.status.phase` remains kube readiness only.
