# OpenDB Milestone 2 Sprint 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make root-range recovery explicit and deterministic before OpenDB adds multi-range routing, split/merge, snapshots, or object-storage clients.

**Architecture:** The canonical commit stream remains the only source of truth. Sprint 2 adds a root bootstrap record, strict catalog replay, strict persistent decode rules, metadata-only recovery artifacts, and a kube-visible local recovery surface without adding external storage services.

**Tech Stack:** Rust, Tokio, Serde, serde_json, OpenRaft-backed root range, TypeScript, Vitest, Kubernetes/k3s manifests. No Python.

---

## Source Spec

Implement the approved design:

- `docs/superpowers/specs/2026-05-09-opendb-milestone-2-sprint-2-design.md`

## File Structure

- `crates/opendb-storage/src/commit_stream.rs`
  Owns persisted commit record types, bootstrap record helpers, strict serde attributes, and the new recovery artifact mutation.
- `crates/opendb-storage/src/range_catalog.rs`
  Owns root descriptor, descriptor immutability, parent graph, bounds, and sibling overlap validation.
- `crates/opendb-storage/src/archive_manifest.rs`
  Owns archive object pointer validation and recovery artifact projection validation.
- `crates/opendb-storage/src/wal.rs`
  Owns frame compatibility, corruption/truncation behavior, and WAL byte golden fixture tests.
- `crates/opendb-storage/tests/wal_golden.rs`
  Integration tests that decode committed golden WAL byte fixtures.
- `crates/opendb-storage/tests/fixtures/wal/*.hex`
  ASCII hex fixtures representing full WAL bytes. They are committed text fixtures, not generated at test time.
- `crates/opendb-consensus/src/root_range.rs`
  Owns bootstrap enforcement, record ordering validation, semantic replay, and OpenRaft peer-to-root-descriptor mapping.
- `crates/opendb-node/src/database.rs`
  Opens the database only after root bootstrap and recovery replay.
- `crates/opendb-node/src/health.rs`
  Exposes kube-readable local recovery status without changing database correctness ownership.
- `crates/opendb-node/src/main.rs`
  Wires recovery status from database open into health state.
- `crates/opendb-node/Cargo.toml`
  Adds `serde` and `serde_json` workspace dependencies for JSON health output.
- `docs/k3s-uat.md`
  Documents the restart-based recovery UAT for k3s/local-path.
- `tools/k3s-smoke.ts`
  Extends the k3s smoke test with a restart-recovery plan and optional execution path.

## Task 1: Root Descriptor Bootstrap And Commit Ordering

**Ownership:** Main session or one worker owns `commit_stream.rs`, `root_range.rs`, `database.rs`, and related tests for this task. Other workers must not edit these files until Task 1 lands.

**Files:**
- Modify: `crates/opendb-storage/src/commit_stream.rs`
- Modify: `crates/opendb-consensus/src/root_range.rs`
- Modify: `crates/opendb-node/src/database.rs`

- [ ] **Step 1: Add failing storage tests for the bootstrap record**

Add tests in `crates/opendb-storage/src/commit_stream.rs`:

```rust
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
}
```

Run:

```bash
cargo test -p opendb-storage commit_record_builds_stable_root_bootstrap_record
```

Expected before implementation: fail because `CommitRecord::root_bootstrap` does not exist.

- [ ] **Step 2: Implement bootstrap helpers in commit stream**

Add this implementation shape to `CommitRecord` in `crates/opendb-storage/src/commit_stream.rs`:

```rust
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
```

Run:

```bash
cargo test -p opendb-storage commit_record_builds_stable_root_bootstrap_record
```

Expected: pass.

- [ ] **Step 3: Add failing root-range bootstrap tests**

Add tests in `crates/opendb-consensus/src/root_range.rs`:

```rust
#[tokio::test]
async fn root_range_bootstraps_root_descriptor_once() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let root_range = RootRange::new(temp_dir.path());

    root_range.ensure_bootstrapped().await.expect("bootstrap");
    root_range.ensure_bootstrapped().await.expect("idempotent bootstrap");

    let records = root_range.replay().await.expect("replay");
    assert_eq!(records, vec![CommitRecord::root_bootstrap(vec![0])]);
}

#[tokio::test]
async fn user_record_before_bootstrap_is_rejected() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let root_range = RootRange::new(temp_dir.path());
    let record = CommitRecord::new(
        TransactionId(1),
        LogicalTimestamp(1),
        vec![Mutation::CreateTable {
            table: "accounts".to_string(),
            columns: account_columns(),
        }],
    );

    let error = root_range
        .apply_committed(&record)
        .await
        .expect_err("reject missing bootstrap");

    assert!(
        error.to_string().contains("root descriptor bootstrap"),
        "unexpected error: {error}"
    );
    assert_eq!(root_range.replay().await.expect("replay"), Vec::new());
}

#[tokio::test]
async fn replay_rejects_non_monotonic_user_tx_id() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let wal = Wal::new(temp_dir.path().join("root-range").join("commit.wal"));
    wal.append(&CommitRecord::root_bootstrap(vec![0]))
        .await
        .expect("append bootstrap");
    wal.append(&CommitRecord::new(
        TransactionId(2),
        LogicalTimestamp(2),
        vec![Mutation::CreateTable {
            table: "accounts".to_string(),
            columns: account_columns(),
        }],
    ))
    .await
    .expect("append first user record");
    wal.append(&CommitRecord::new(
        TransactionId(2),
        LogicalTimestamp(3),
        vec![Mutation::CreateTable {
            table: "orders".to_string(),
            columns: id_columns(),
        }],
    ))
    .await
    .expect("append forged duplicate tx");

    let error = RootRange::new(temp_dir.path())
        .replay()
        .await
        .expect_err("reject duplicate tx");

    assert!(
        error.to_string().contains("strictly increasing tx_id"),
        "unexpected error: {error}"
    );
}
```

Run:

```bash
cargo test -p opendb-consensus bootstrap
cargo test -p opendb-consensus non_monotonic
```

Expected before implementation: fail because `ensure_bootstrapped` and ordering validation do not exist.

- [ ] **Step 4: Implement root bootstrap state**

Update `RootRange` in `crates/opendb-consensus/src/root_range.rs`:

```rust
#[derive(Clone, Debug)]
pub struct RootRange {
    range_id: RangeId,
    wal: Wal,
    proposal_path: RootRangeProposalPath,
    semantic_append_lock: Arc<Mutex<()>>,
    openraft_submit_lock: Arc<Mutex<()>>,
    bootstrap_replica_node_ids: Vec<OpenDbRaftNodeId>,
}
```

Set `bootstrap_replica_node_ids`:

- `RootRange::new_with_authority`: `vec![0]`;
- `RootRange::new_raft_backed`: sorted keys from the `members` map.

Add:

```rust
pub async fn ensure_bootstrapped(&self) -> OpenDbResult<()> {
    let _guard = self.semantic_append_lock.lock().await;
    let records = self.wal.read_all().await?;
    let expected = CommitRecord::root_bootstrap(self.bootstrap_replica_node_ids.clone());

    match records.first() {
        None => {
            self.validate_apply_record(&expected)?;
            self.wal.append(&expected).await
        }
        Some(first) if first == &expected => {
            self.validate_replay_sequence(&records)?;
            Ok(())
        }
        Some(first) if first.is_root_bootstrap() => Err(OpenDbError::Storage(format!(
            "root descriptor bootstrap does not match expected replicas {:?}: got {:?}",
            self.bootstrap_replica_node_ids, first
        ))),
        Some(_) => Err(OpenDbError::Storage(
            "root descriptor bootstrap is missing from the first WAL record".to_string(),
        )),
    }
}
```

Add a private `validate_replay_sequence(&self, records: &[CommitRecord])` that enforces:

- empty WAL is accepted only by `replay()`, not by append of a user record;
- first non-empty record must equal the expected bootstrap;
- only the first record may use `tx_id = 0` or `ts = 0`;
- user mutations are non-empty;
- user actor is non-empty and trimmed;
- user `tx_id` and `ts` are strictly increasing.

- [ ] **Step 5: Enforce bootstrap before user append and submit**

In `apply_committed()` and the OpenRaft/local write path in `submit()`, keep the semantic append lock behavior and reject non-bootstrap records when the WAL is empty.

The intended check inside `validate_semantic_append()` is:

```rust
let mut records = self.replay().await?;
if records.is_empty() && !record.is_root_bootstrap() {
    return Err(OpenDbError::InvalidInput(
        "root descriptor bootstrap must be committed before user records".to_string(),
    ));
}
records.push(record.clone());
self.validate_replay_sequence(&records)?;
RowProjection::rebuild(&records)?;
RangeCatalog::rebuild(&records)?;
ArchiveManifest::rebuild(&records)?;
```

- [ ] **Step 6: Wire database open through bootstrap**

In `crates/opendb-node/src/database.rs`, call bootstrap before replay:

```rust
pub async fn open_with_root_range(root_range: RootRange) -> OpenDbResult<Self> {
    root_range.ensure_bootstrapped().await?;
    let records = root_range.replay().await?;
    let engine = SqlEngine::from_commits(records)?;

    Ok(Self {
        root_range,
        engine,
        peer_server: None,
    })
}
```

`RowProjection` and `SqlEngine` already ignore `PutRangeDescriptor`, and `SqlEngine::apply_committed` already sets `next_tx` with `max`, so the bootstrap record leaves the next user transaction at `1`.

- [ ] **Step 7: Run focused verification and commit**

Run:

```bash
cargo test -p opendb-storage root_bootstrap
cargo test -p opendb-consensus bootstrap
cargo test -p opendb-consensus non_monotonic
cargo test -p opendb-node execute_persists_writes_through_root_range_and_replays_on_reopen
cargo fmt --all -- --check
```

Expected: all listed tests pass.

Commit:

```bash
git add crates/opendb-storage/src/commit_stream.rs crates/opendb-consensus/src/root_range.rs crates/opendb-node/src/database.rs
git commit -m "feat: bootstrap root descriptor"
git push origin HEAD:main
```

## Task 2: Strict Range Catalog Invariants

**Ownership:** A worker may own only `crates/opendb-storage/src/range_catalog.rs` for this task. Do not edit `commit_stream.rs`, `root_range.rs`, or archive files in this task.

**Files:**
- Modify: `crates/opendb-storage/src/range_catalog.rs`

- [ ] **Step 1: Add failing catalog invariant tests**

Add tests to `crates/opendb-storage/src/range_catalog.rs`:

```rust
#[test]
fn range_catalog_rejects_parent_cycle_in_one_commit() {
    let record = CommitRecord::new(
        TransactionId(51),
        LogicalTimestamp(16),
        vec![
            Mutation::PutRangeDescriptor {
                descriptor: RangeDescriptor {
                    range_id: RangeId::ROOT,
                    parent_range_id: None,
                    key_start: None,
                    key_end: None,
                    replica_node_ids: vec![0],
                },
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
    assert!(error.to_string().contains("parent cycle"), "unexpected error: {error}");
}

#[test]
fn range_catalog_rejects_conflicting_descriptor_update() {
    let first = root_and_child_record(
        RangeId(2),
        Some("a"),
        Some("m"),
        vec![0],
    );
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
```

Add local test helper functions `root_descriptor`, `child_descriptor`, and `root_and_child_record` in the test module.

Run:

```bash
cargo test -p opendb-storage range_catalog_rejects_parent_cycle_in_one_commit
cargo test -p opendb-storage range_catalog_rejects_conflicting_descriptor_update
cargo test -p opendb-storage range_catalog_rejects_overlapping_sibling_ranges
```

Expected before implementation: fail because cycles, conflicting updates, and sibling overlaps are not rejected.

- [ ] **Step 2: Implement descriptor immutability**

In `apply_inner`, when applying `PutRangeDescriptor`, compare against an existing descriptor:

```rust
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
```

This preserves idempotent metadata replay while rejecting parent, bounds, and replica changes until typed mutations exist.

- [ ] **Step 3: Implement graph and sibling validation**

After building `candidate`, validate the entire catalog:

```rust
validate_root_descriptor(&candidate)?;
validate_parent_graph(&candidate)?;
validate_sibling_ranges(&candidate)?;
```

Rules:

- exactly one root descriptor exists at `RangeId::ROOT`;
- non-root parent ids exist;
- walking parent ids must terminate at root;
- a repeated visited id is a parent cycle;
- siblings are grouped by `parent_range_id`;
- sorted sibling intervals must satisfy `previous.key_end <= next.key_start`;
- exact adjacency is valid;
- gaps are valid in Sprint 2.

- [ ] **Step 4: Add positive tests for adjacency and gaps**

Add:

```rust
#[test]
fn range_catalog_accepts_adjacent_siblings_and_gaps() {
    let record = CommitRecord::new(
        TransactionId(54),
        LogicalTimestamp(19),
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
```

Run:

```bash
cargo test -p opendb-storage range_catalog
```

Expected: all range catalog tests pass.

- [ ] **Step 5: Commit**

Run:

```bash
cargo fmt --all -- --check
cargo test -p opendb-storage range_catalog
```

Commit:

```bash
git add crates/opendb-storage/src/range_catalog.rs
git commit -m "feat: enforce range catalog invariants"
git push origin HEAD:main
```

## Task 3: Strict Persistent Compatibility And WAL Goldens

**Ownership:** One worker owns `crates/opendb-storage/src/commit_stream.rs`, `crates/opendb-storage/src/wal.rs`, and `crates/opendb-storage/tests/fixtures/wal/*` for this task after Task 1 is merged.

**Files:**
- Modify: `crates/opendb-storage/src/commit_stream.rs`
- Modify: `crates/opendb-storage/src/range_catalog.rs`
- Modify: `crates/opendb-storage/src/archive_manifest.rs`
- Modify: `crates/opendb-storage/src/wal.rs`
- Create: `crates/opendb-storage/tests/wal_golden.rs`
- Create: `crates/opendb-storage/tests/fixtures/wal/frame-v1-record-v2-bootstrap.hex`

- [ ] **Step 1: Add strict serde decode tests**

Add tests in `crates/opendb-storage/src/wal.rs`:

```rust
#[tokio::test]
async fn wal_rejects_known_record_with_unknown_field() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let wal_path = temp_dir.path().join("commit.wal");
    let payload = br#"{"version":2,"tx_id":1,"range_id":1,"ts":1,"actor":"system","unexpected":true,"mutations":[]}"#;
    let frame = encode_payload_for_test(payload);
    tokio::fs::write(&wal_path, frame).await.expect("write fixture");

    let error = Wal::new(&wal_path)
        .read_all()
        .await
        .expect_err("reject unknown field");

    assert!(
        error.to_string().contains("unknown field"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn wal_rejects_unknown_mutation_variant() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let wal_path = temp_dir.path().join("commit.wal");
    let payload = br#"{"version":2,"tx_id":1,"range_id":1,"ts":1,"actor":"system","mutations":[{"DropEverything":{}}]}"#;
    let frame = encode_payload_for_test(payload);
    tokio::fs::write(&wal_path, frame).await.expect("write fixture");

    let error = Wal::new(&wal_path)
        .read_all()
        .await
        .expect_err("reject unknown mutation");

    assert!(
        error.to_string().contains("unknown variant"),
        "unexpected error: {error}"
    );
}
```

Add a test-only helper near existing WAL tests:

```rust
fn encode_payload_for_test(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.extend_from_slice(WAL_MAGIC);
    frame.extend_from_slice(&WAL_FRAME_VERSION.to_le_bytes());
    frame.extend_from_slice(&FRAME_RESERVED.to_le_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&0_u32.to_le_bytes());
    frame.extend_from_slice(payload);
    let checksum = frame_checksum(&frame[4..8], &frame[8..12], payload);
    frame[12..16].copy_from_slice(&checksum.to_le_bytes());
    frame
}
```

Run:

```bash
cargo test -p opendb-storage wal_rejects_known_record_with_unknown_field
cargo test -p opendb-storage wal_rejects_unknown_mutation_variant
```

Expected before implementation: at least the unknown field test fails because persisted structs do not yet deny unknown fields.

- [ ] **Step 2: Add strict serde attributes**

Add `#[serde(deny_unknown_fields)]` to persisted structs and enums where Serde can ignore named fields:

```rust
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnDefinition { ... }

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnValue { ... }

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Mutation { ... }

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitRecord { ... }
```

Add the same attribute to `RangeDescriptor`, `ArchiveObjectPointer`, and new Task 4 persisted artifact structs when that task lands.

- [ ] **Step 3: Add a committed golden byte fixture**

Create `crates/opendb-storage/tests/fixtures/wal/frame-v1-record-v2-bootstrap.hex` as a lowercase hex string representing a full WAL frame for:

```rust
CommitRecord::root_bootstrap(vec![0, 1, 2])
```

Generate the hex only with Rust or TypeScript. One acceptable temporary Rust path is adding a local ignored test during development, printing the bytes with `cargo test -- --nocapture`, then committing only the fixture and the real tests. Do not add generator Python.

- [ ] **Step 4: Add integration fixture tests**

Create `crates/opendb-storage/tests/wal_golden.rs`:

```rust
use opendb_common::{LogicalTimestamp, RangeId, TransactionId};
use opendb_storage::{
    commit_stream::{CommitRecord, Mutation},
    range_catalog::RangeDescriptor,
    wal::Wal,
};

fn decode_hex(input: &str) -> Vec<u8> {
    let hex = input.chars().filter(|ch| !ch.is_whitespace()).collect::<String>();
    assert_eq!(hex.len() % 2, 0, "hex fixture must have even length");
    hex.as_bytes()
        .chunks(2)
        .map(|chunk| {
            let value = std::str::from_utf8(chunk).expect("hex utf8");
            u8::from_str_radix(value, 16).expect("hex byte")
        })
        .collect()
}

#[tokio::test]
async fn wal_reads_frame_v1_record_v2_bootstrap_fixture() {
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let path = temp_dir.path().join("commit.wal");
    let bytes = decode_hex(include_str!("fixtures/wal/frame-v1-record-v2-bootstrap.hex"));
    tokio::fs::write(&path, bytes).await.expect("write wal fixture");

    let records = Wal::new(&path).read_all().await.expect("read fixture");

    assert_eq!(
        records,
        vec![CommitRecord::root_bootstrap(vec![0, 1, 2])]
    );
}
```

Run:

```bash
cargo test -p opendb-storage --test wal_golden
```

Expected: pass.

- [ ] **Step 5: Verify fatal compatibility behavior**

Add or update tests in `wal.rs` proving:

- future commit version is rejected;
- legacy v1 create-table shape is rejected;
- bad checksum is fatal;
- bad magic is fatal;
- append truncates only final torn frame;
- append does not truncate a frame with future commit version.

Run:

```bash
cargo test -p opendb-storage wal_rejects
cargo test -p opendb-storage wal_truncates
```

Expected: all WAL compatibility tests pass.

- [ ] **Step 6: Commit**

Run:

```bash
cargo fmt --all -- --check
cargo test -p opendb-storage wal
cargo test -p opendb-storage --test wal_golden
```

Commit:

```bash
git add crates/opendb-storage/src/commit_stream.rs crates/opendb-storage/src/range_catalog.rs crates/opendb-storage/src/archive_manifest.rs crates/opendb-storage/src/wal.rs crates/opendb-storage/tests/wal_golden.rs crates/opendb-storage/tests/fixtures/wal/frame-v1-record-v2-bootstrap.hex
git commit -m "feat: enforce strict wal compatibility"
git push origin HEAD:main
```

## Task 4: Metadata-Only Recovery Artifacts

**Ownership:** One worker owns `archive_manifest.rs` and the recovery artifact additions in `commit_stream.rs` after Task 3 lands.

**Files:**
- Modify: `crates/opendb-storage/src/archive_manifest.rs`
- Modify: `crates/opendb-storage/src/commit_stream.rs`
- Modify: `crates/opendb-consensus/src/root_range.rs`

- [ ] **Step 1: Add failing archive artifact tests**

Add tests in `crates/opendb-storage/src/archive_manifest.rs`:

```rust
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
fn archive_manifest_rejects_conflicting_recovery_artifact_object_metadata() {
    let first = recovery_artifact("root-range/00000001.wal", 0, 10);
    let conflicting = RecoveryArtifactPointer {
        object: ArchiveObjectPointer {
            content_sha256: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned(),
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
```

Run:

```bash
cargo test -p opendb-storage recovery_artifacts
```

Expected before implementation: fail because recovery artifact types and mutation do not exist.

- [ ] **Step 2: Add recovery artifact types**

Add to `crates/opendb-storage/src/archive_manifest.rs`:

```rust
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
#[serde(deny_unknown_fields)]
pub struct RecoveryArtifactPointer {
    pub artifact_kind: RecoveryArtifactKind,
    pub range_id: opendb_common::RangeId,
    pub object: ArchiveObjectPointer,
    pub format_version: u16,
    pub tx_id_start: opendb_common::TransactionId,
    pub tx_id_end: opendb_common::TransactionId,
    pub ts_start: opendb_common::LogicalTimestamp,
    pub ts_end: opendb_common::LogicalTimestamp,
    pub record_count: u64,
    pub byte_len: u64,
    pub compression: CompressionKind,
}
```

Add to `Mutation` in `commit_stream.rs`:

```rust
PutRecoveryArtifactPointer {
    artifact: RecoveryArtifactPointer,
},
```

- [ ] **Step 3: Implement recovery artifact validation**

In `ArchiveManifest`:

- add `recovery_artifacts: Vec<RecoveryArtifactPointer>`;
- validate `format_version > 0`;
- validate `record_count > 0`;
- validate `byte_len > 0`;
- validate `tx_id_start <= tx_id_end`;
- validate `ts_start <= ts_end`;
- reuse `validate_object_pointer(&artifact.object)`;
- deduplicate byte-identical artifacts;
- reject same object path with different hash or coverage;
- reject overlapping `(range_id, artifact_kind, tx_id_start..=tx_id_end)` unless the artifact is identical.

Expose:

```rust
pub fn recovery_artifacts(&self) -> &[RecoveryArtifactPointer] {
    &self.recovery_artifacts
}
```

- [ ] **Step 4: Update replay projections**

Update match arms in `RowProjection`, `RangeCatalog`, and `ArchiveManifest` so `PutRecoveryArtifactPointer` is ignored by non-archive projections and validated by archive replay.

Run:

```bash
cargo test -p opendb-storage archive_manifest
cargo test -p opendb-consensus archive
```

Expected: archive manifest and root-range replay tests pass.

- [ ] **Step 5: Commit**

Run:

```bash
cargo fmt --all -- --check
cargo test -p opendb-storage archive_manifest
cargo test -p opendb-consensus archive
```

Commit:

```bash
git add crates/opendb-storage/src/archive_manifest.rs crates/opendb-storage/src/commit_stream.rs crates/opendb-storage/src/range_catalog.rs crates/opendb-storage/src/row_projection.rs crates/opendb-consensus/src/root_range.rs
git commit -m "feat: add recovery artifact metadata"
git push origin HEAD:main
```

## Task 5: Kube-Visible Recovery Status And k3s Restart UAT

**Ownership:** One worker owns node health, node main/database wiring, k3s docs, and TypeScript smoke changes for this task after Tasks 1 and 4 land.

**Files:**
- Modify: `crates/opendb-node/Cargo.toml`
- Modify: `crates/opendb-node/src/health.rs`
- Modify: `crates/opendb-node/src/database.rs`
- Modify: `crates/opendb-node/src/main.rs`
- Modify: `docs/k3s-uat.md`
- Modify: `tools/k3s-smoke.ts`
- Modify: `tests/cluster/k3s-smoke.test.ts`

- [ ] **Step 1: Add failing health status tests**

Add to `crates/opendb-node/src/health.rs`:

```rust
#[test]
fn status_reports_recovery_watermark() {
    let state = HealthState::new(false);
    state.set_recovery_status(RecoveryStatus {
        root_descriptor_known: true,
        wal_replay_completed: true,
        last_replayed_tx_id: Some(2),
        last_replayed_ts: Some(2),
        archive_metadata_replayed: true,
        latest_recovery_artifact: None,
    });

    let response = response_for_path("/status", &state);

    assert_eq!(response.status_code, 200);
    assert!(response.body.contains("\"rootDescriptorKnown\":true"));
    assert!(response.body.contains("\"lastReplayedTxId\":2"));
}
```

Run:

```bash
cargo test -p opendb-node status_reports_recovery_watermark
```

Expected before implementation: fail because `/status` and `RecoveryStatus` do not exist.

- [ ] **Step 2: Implement recovery status in health**

Add `serde` and `serde_json` workspace dependencies to `crates/opendb-node/Cargo.toml` if using JSON.

Add:

```rust
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryStatus {
    pub root_descriptor_known: bool,
    pub wal_replay_completed: bool,
    pub last_replayed_tx_id: Option<u64>,
    pub last_replayed_ts: Option<u64>,
    pub archive_metadata_replayed: bool,
    pub latest_recovery_artifact: Option<String>,
}
```

Extend `HealthState` with `Arc<std::sync::RwLock<RecoveryStatus>>`, `set_recovery_status`, and `recovery_status`.

Change `HealthResponse.body` from `&'static str` to `String`, and return JSON for `/status`.

- [ ] **Step 3: Produce recovery status from database open**

In `crates/opendb-node/src/database.rs`, add:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseRecoveryStatus {
    pub root_descriptor_known: bool,
    pub wal_replay_completed: bool,
    pub last_replayed_tx_id: Option<u64>,
    pub last_replayed_ts: Option<u64>,
    pub archive_metadata_replayed: bool,
}
```

Store it in `Database` and expose `pub fn recovery_status(&self) -> &DatabaseRecoveryStatus`.

When opening:

- call `ensure_bootstrapped`;
- replay records;
- set root descriptor known if the first record is `is_root_bootstrap`;
- set last replayed tx/ts from the last record;
- set archive metadata replayed after `root_range.replay()` succeeds.

In `main.rs`, after opening the database and before serving health:

```rust
let recovery_status = {
    let database_guard = database.lock().await;
    database_guard.recovery_status().clone()
};
health_state.set_recovery_status(recovery_status.into());
```

Implement `From<DatabaseRecoveryStatus> for health::RecoveryStatus`.

- [ ] **Step 4: Extend k3s smoke planning and docs**

In `tools/k3s-smoke.ts`, add a printed plan section for restart recovery:

```text
8. create table and insert recovery smoke row through pgwire
9. delete the current leader pod
10. wait for OpenDbCluster/status Ready with a leader pod
11. query the recovery smoke row through pgwire
```

Keep execution optional if the current static smoke cannot safely delete pods in every local context. The `--print-plan` output must document the restart UAT.

In `docs/k3s-uat.md`, add a `Recovery Contract UAT` section with:

- PVC/local-path only;
- no MinIO or cloud object storage;
- leader restart/delete scenario;
- status endpoint expectations;
- current limitation that archive metadata is local replay metadata only.

- [ ] **Step 5: Update TypeScript cluster tests**

In `tests/cluster/k3s-smoke.test.ts`, assert the printed plan includes:

```typescript
expect(output).toContain("delete the current leader pod");
expect(output).toContain("query the recovery smoke row through pgwire");
expect(output).toContain("no object storage service is required");
```

Run:

```bash
npm run test:cluster
```

Expected: pass.

- [ ] **Step 6: Commit**

Run:

```bash
cargo fmt --all -- --check
cargo test -p opendb-node status
npm run check:ts
npm run test:cluster
npm run check:no-python
```

Commit:

```bash
git add crates/opendb-node/Cargo.toml crates/opendb-node/src/health.rs crates/opendb-node/src/database.rs crates/opendb-node/src/main.rs docs/k3s-uat.md tools/k3s-smoke.ts tests/cluster/k3s-smoke.test.ts
git commit -m "feat: expose recovery status for k3s"
git push origin HEAD:main
```

## Final Verification

No Sprint 2 implementation is complete unless these commands pass:

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

`cargo test --workspace` may require execution outside the sandbox because OpenRaft tests bind local loopback ports. `tsx` commands may also require execution outside the sandbox when the IPC pipe is blocked.

## Review Checklist

- [ ] The first WAL record is the root bootstrap descriptor.
- [ ] Bootstrap is idempotent and detects conflicting existing root descriptors.
- [ ] User records cannot appear before bootstrap.
- [ ] `tx_id` and `ts` are strictly increasing after bootstrap.
- [ ] Range catalog rejects cycles and sibling overlaps.
- [ ] Range catalog allows gaps until typed split/merge exists.
- [ ] Persisted decode rejects unknown fields and unknown mutation variants.
- [ ] Golden WAL fixture is full-frame bytes represented as committed ASCII hex.
- [ ] Recovery artifacts describe coverage but do not upload/download objects.
- [ ] k3s UAT documents restart recovery without object storage.
- [ ] No Python files or scripts are introduced.
