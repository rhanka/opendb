# OpenDB Milestone 2 Sprint 5 Implementation Plan

Date: 2026-05-12
Sprint: Milestone 2 Sprint 5
Workers: 1-2 (most tasks are independently parallelizable; tasks 4-5 must
share JSON contract review with the node owner).

## Source Spec

`docs/superpowers/specs/2026-05-12-opendb-milestone-2-sprint-5-design.md`.

## File Structure

- `crates/opendb-storage/src/range_catalog.rs` — allocator + tests.
- `crates/opendb-node/src/database.rs` — propose_range_split / merge.
- `crates/opendb-node/src/admin.rs` — new axum router for admin endpoints.
- `crates/opendb-node/src/health.rs` — `/status` block added, deserializer
  loosened for operator-side compatibility.
- `crates/opendb-node/src/main.rs` — wire the admin router behind the same
  port as `/live` / `/ready` / `/status`, or behind a separate port if we
  want kube NetworkPolicy isolation later (we go with same port + path
  prefix in this sprint).
- `crates/opendb-operator/src/recovery.rs` — extend status fetcher with the
  new optional `rangeCatalog` field.
- `crates/opendb-operator/src/crd.rs` — add `RangeCatalogStable` condition
  type, extend recovery summary block.
- `crates/opendb-operator/src/main.rs` — aggregate condition.
- `tools/k3s-smoke.ts` — new `--with-range-split` opt-in.
- `tests/cluster/k3s-smoke.test.ts` — guardrail for opt-in flag.
- `tests/cluster/manifests.test.ts` — no manifest change expected; assertion
  that no new Service was added.
- `docs/k3s-uat.md` — short paragraph on the new opt-in flag.

## Task 1: Range Allocator

**Ownership:** One worker owns `crates/opendb-storage/src/range_catalog.rs`.
No external file touched.

**Files:**
- Modify: `crates/opendb-storage/src/range_catalog.rs`.

- [ ] **Step 1: Failing allocator tests**

Add to the existing `mod tests`:

```rust
#[test]
fn allocate_range_id_returns_next_after_max_known_descriptor() {
    let catalog = RangeCatalog::rebuild(&[
        CommitRecord::root_bootstrap(vec![0, 1, 2]),
        split_root_record(),
    ])
    .expect("rebuild");

    assert_eq!(catalog.allocate_range_id(), RangeId(4));
}

#[test]
fn allocate_range_id_skips_ids_retired_by_merge() {
    let merged = CommitRecord::new(
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
        merged,
    ])
    .expect("rebuild");

    assert_eq!(catalog.allocate_range_id(), RangeId(5));
}

#[test]
fn allocate_range_id_on_empty_catalog_returns_root_plus_one() {
    let catalog = RangeCatalog::default();
    assert_eq!(catalog.allocate_range_id(), RangeId(RangeId::ROOT.0 + 1));
}
```

Run `cargo test -p opendb-storage allocate_range_id`. Expect compile failure.

- [ ] **Step 2: Implement the allocator**

Add to `RangeCatalog`:

```rust
pub fn allocate_range_id(&self) -> RangeId {
    let max_id = self
        .descriptors
        .keys()
        .copied()
        .max()
        .unwrap_or(RangeId::ROOT);
    RangeId(max_id.0 + 1)
}
```

The allocator uses `descriptors` (not `active_range_ids`): retired ids are
still in the descriptor map, so the allocator never reuses them.

- [ ] **Step 3: Focused run**

```bash
cargo test -p opendb-storage allocate_range_id
```

Expect: all green.

- [ ] **Step 4: Commit**

```bash
git add crates/opendb-storage/src/range_catalog.rs
git commit -m "feat: allocate range ids from catalog descriptors"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

Expected grep count: `0`.

## Task 2: Database Proposal Surface

**Ownership:** One worker owns `crates/opendb-node/src/database.rs`. Do not
edit consensus or storage files in this task.

**Files:**
- Modify: `crates/opendb-node/src/database.rs`.

- [ ] **Step 1: Add proposal types and methods**

Add public structs:

```rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposedRangeSplit {
    pub source_range_id: RangeId,
    pub split_key: String,
    pub left_range_id: Option<RangeId>,
    pub right_range_id: Option<RangeId>,
    pub left_replica_node_ids: Option<Vec<u64>>,
    pub right_replica_node_ids: Option<Vec<u64>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeSplitProposalResult {
    pub left_range_id: RangeId,
    pub right_range_id: RangeId,
    pub tx_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProposedRangeMerge {
    pub source_range_ids: Vec<RangeId>,
    pub merged_range_id: Option<RangeId>,
    pub replica_node_ids: Option<Vec<u64>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeMergeProposalResult {
    pub merged_range_id: RangeId,
    pub tx_id: u64,
}
```

Implementation outline for `propose_range_split`:

1. `self.refresh_engine_from_wal().await?;`
2. `self.ensure_leader_for_client_query().await?;`
3. Resolve source descriptor from `self.range_catalog`. Reject with
   `OpenDbError::InvalidInput` if it does not exist or is not active.
4. Resolve `left_range_id` and `right_range_id`. If absent, call
   `self.range_catalog.allocate_range_id()` for the first; allocate twice
   for the second using a temporary catalog clone that "absorbs" the
   first id.
5. Build `RangeDescriptor` for left and right by combining the source
   bounds and the split key (same logic as Sprint 4 spec).
6. Build the metadata `CommitRecord` with `range_id = RangeId::ROOT`,
   monotonic `tx_id` and `ts` (use `Database::next_tx_id_after_replay`).
7. Submit through `self.root_range.submit(RootRangeCommand { record })`.
8. Refresh the engine again so the local catalog observes the new
   descriptors.
9. Return `RangeSplitProposalResult { left_range_id, right_range_id, tx_id }`.

`propose_range_merge` follows the same pattern.

- [ ] **Step 2: Tests**

Add three tests at the end of the `mod tests`:

```rust
#[tokio::test]
async fn propose_range_split_assigns_ids_and_stamps_metadata_on_root() { /* … */ }

#[tokio::test]
async fn propose_range_split_rejected_when_source_inactive() { /* … */ }

#[tokio::test]
async fn propose_range_merge_round_trip() { /* … */ }
```

Each test opens a `Database` against a tempdir, performs the proposal,
re-reads the WAL through `root_range.replay()`, and asserts the resulting
descriptors and `range_id` of the final record.

- [ ] **Step 3: Focused run**

```bash
cargo test -p opendb-node propose_range
```

- [ ] **Step 4: Commit**

```bash
git add crates/opendb-node/src/database.rs
git commit -m "feat: propose range split and merge from database"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

## Task 3: Admin HTTP Endpoint

**Ownership:** One worker owns `crates/opendb-node/src/admin.rs` (new) and
`crates/opendb-node/src/main.rs` for the router wiring. Do not edit
`health.rs` here.

**Files:**
- Create: `crates/opendb-node/src/admin.rs`.
- Modify: `crates/opendb-node/src/main.rs`.
- Modify: `crates/opendb-node/src/lib.rs` (re-export `admin`).

- [ ] **Step 1: New module skeleton**

```rust
use axum::{Router, routing::post, http::StatusCode, Json, extract::State};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct AdminState {
    pub database: Arc<tokio::sync::Mutex<Database>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SplitRequest {
    pub source_range_id: u64,
    pub split_key: String,
    #[serde(default)]
    pub left_range_id: Option<u64>,
    #[serde(default)]
    pub right_range_id: Option<u64>,
    #[serde(default)]
    pub left_replica_node_ids: Option<Vec<u64>>,
    #[serde(default)]
    pub right_replica_node_ids: Option<Vec<u64>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SplitResponse {
    pub range_ids: [u64; 2],
    pub tx_id: u64,
}

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/admin/ranges/split", post(handle_split))
        .route("/admin/ranges/merge", post(handle_merge))
        .with_state(state)
}
```

Handler outline:

```rust
async fn handle_split(
    State(state): State<AdminState>,
    Json(req): Json<SplitRequest>,
) -> Result<(StatusCode, Json<SplitResponse>), (StatusCode, Json<ErrorBody>)> {
    let mut db = state.database.lock().await;
    match db.propose_range_split(req.into()).await {
        Ok(result) => Ok((StatusCode::ACCEPTED, Json(result.into()))),
        Err(OpenDbError::NotLeader { leader_id, leader_addr }) => Err(misdirected(leader_id, leader_addr)),
        Err(OpenDbError::InvalidInput(message)) => Err(bad_request(message)),
        Err(OpenDbError::NotFound(message)) => Err(bad_request(message)),
        Err(error) => Err(internal(error)),
    }
}
```

`misdirected` returns `421 Misdirected Request` with body
`{ "leaderId": …, "leaderAddr": … }`.

- [ ] **Step 2: Wire into main**

In `crates/opendb-node/src/main.rs`, when building the health router,
merge in the admin router:

```rust
let admin_router = opendb_node::admin::router(AdminState { database: database.clone() });
let app = health::router(...).merge(admin_router);
```

The existing `Database` must be wrapped in `Arc<Mutex<Database>>` if it
isn't already, so the admin handler can mutate it. If it is wrapped
behind another type, plumb the lock through.

- [ ] **Step 3: Tests**

Add `crates/opendb-node/src/admin.rs` tests using `axum::Router::oneshot`
or `axum::body::Body` directly. Three minimal cases:

```rust
#[tokio::test]
async fn split_endpoint_returns_202_with_allocated_ids() { /* … */ }

#[tokio::test]
async fn split_endpoint_returns_421_for_follower() { /* … */ }

#[tokio::test]
async fn split_endpoint_returns_400_for_invalid_boundary() { /* … */ }
```

- [ ] **Step 4: Focused run**

```bash
cargo test -p opendb-node split_endpoint merge_endpoint
```

- [ ] **Step 5: Commit**

```bash
git add crates/opendb-node/src/admin.rs crates/opendb-node/src/main.rs crates/opendb-node/src/lib.rs
git commit -m "feat: expose admin range split and merge endpoints"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

## Task 4: /status `rangeCatalog` Block

**Ownership:** One worker owns `crates/opendb-node/src/health.rs` and the
matching deserializer in `crates/opendb-operator/src/recovery.rs`. Keep
backward compatibility (operator must still parse old payloads).

**Files:**
- Modify: `crates/opendb-node/src/health.rs`.
- Modify: `crates/opendb-operator/src/recovery.rs`.

- [ ] **Step 1: Add new payload field**

Node side:

```rust
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusPayload {
    /* existing fields */
    range_catalog: RangeCatalogStatusPayload,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RangeCatalogStatusPayload {
    active_range_count: usize,
    last_split_tx_id: Option<u64>,
    last_merge_tx_id: Option<u64>,
}
```

The values come from a new accessor on `Database`:

```rust
pub fn range_catalog_status(&self) -> RangeCatalogStatusSnapshot;
```

backed by `self.range_catalog.active_range_ids().len()`, the highest
`tx_id` of any record containing a `Mutation::SplitRange`, and the highest
`tx_id` of any record containing a `Mutation::MergeRanges`.

Operator side:

```rust
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RangeCatalogStatusFetched {
    active_range_count: usize,
    #[serde(default)]
    last_split_tx_id: Option<u64>,
    #[serde(default)]
    last_merge_tx_id: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct StatusFetched {
    /* existing fields */
    #[serde(default)]
    range_catalog: Option<RangeCatalogStatusFetched>,
}
```

`#[serde(default)]` on the field is what keeps legacy payloads valid.

- [ ] **Step 2: Tests**

`opendb-node`: golden test on `range_catalog_status` after bootstrap +
split + merge.

`opendb-operator`: deserialize a fixture payload without the new field
(must default to `None`) and one with the field (must parse).

- [ ] **Step 3: Focused run**

```bash
cargo test -p opendb-node range_catalog_status
cargo test -p opendb-operator range_catalog_status
```

- [ ] **Step 4: Commit**

```bash
git add crates/opendb-node/src/health.rs crates/opendb-operator/src/recovery.rs
git commit -m "feat: report range catalog snapshot from node status"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

## Task 5: Operator Condition `RangeCatalogStable`

**Ownership:** One worker owns `crates/opendb-operator/src/main.rs` and
`crd.rs`. Coordinate with Task 4 for JSON payload shape.

**Files:**
- Modify: `crates/opendb-operator/src/crd.rs`.
- Modify: `crates/opendb-operator/src/main.rs`.
- Modify: `crates/opendb-operator/src/recovery.rs` (aggregator).

- [ ] **Step 1: CRD constants**

Add condition type constant:

```rust
pub const CONDITION_RANGE_CATALOG_STABLE: &str = "RangeCatalogStable";
```

Reasons:

```rust
pub const REASON_ACTIVE_RANGE_COUNT_AGREES: &str = "ActiveRangeCountAgrees";
pub const REASON_PENDING_SPLIT_MERGE: &str = "PendingSplitMerge";
pub const REASON_DIVERGENT_CATALOG: &str = "DivergentCatalog";
pub const REASON_RANGE_CATALOG_STATUS_UNKNOWN: &str = "StatusUnknown";
```

Extend `OpenDbClusterRecoverySummary`:

```rust
pub struct RangeCatalogSummary {
    pub active_range_count: Option<usize>,
    pub last_split_tx_id: Option<u64>,
    pub last_merge_tx_id: Option<u64>,
}
```

- [ ] **Step 2: Aggregator**

```rust
fn aggregate_range_catalog_condition(reports: &[StatusFetched]) -> Condition {
    if any_unreachable(reports) {
        return condition_unknown(...);
    }
    let active_counts: BTreeSet<_> = reports
        .iter()
        .filter_map(|r| r.range_catalog.as_ref())
        .map(|c| c.active_range_count)
        .collect();
    match active_counts.len() {
        0 => condition_unknown(REASON_RANGE_CATALOG_STATUS_UNKNOWN, "no node reported a catalog"),
        1 => condition_true(REASON_ACTIVE_RANGE_COUNT_AGREES, format!(
            "active ranges={}", active_counts.iter().next().unwrap()
        )),
        _ => condition_false(REASON_DIVERGENT_CATALOG, format!(
            "active_range_count diverges: {active_counts:?}"
        )),
    }
}
```

The reason `PENDING_SPLIT_MERGE` is for a future sprint (we observe
in-flight Raft proposals); leave it as a documented constant unused this
sprint to avoid churn later.

- [ ] **Step 3: Tests**

Three operator unit tests:

```rust
#[test]
fn range_catalog_condition_true_when_all_pods_agree() { /* … */ }

#[test]
fn range_catalog_condition_unknown_when_any_pod_unreachable() { /* … */ }

#[test]
fn range_catalog_condition_false_on_divergent_active_count() { /* … */ }
```

- [ ] **Step 4: Focused run**

```bash
cargo test -p opendb-operator range_catalog
```

- [ ] **Step 5: Commit**

```bash
git add crates/opendb-operator/src/crd.rs crates/opendb-operator/src/main.rs crates/opendb-operator/src/recovery.rs
git commit -m "feat: aggregate range catalog stable condition in operator"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

## Task 6: Cluster Smoke `--with-range-split`

**Ownership:** One worker owns `tools/k3s-smoke.ts` and
`tests/cluster/k3s-smoke.test.ts`. Do not change Rust code in this task.

**Files:**
- Modify: `tools/k3s-smoke.ts`.
- Modify: `tests/cluster/k3s-smoke.test.ts`.

- [ ] **Step 1: Add flag plumbing**

Parse `--with-range-split` and `OPENDB_K3S_WITH_RANGE_SPLIT` env var the
same way `--with-restart-recovery` is parsed. Default is off.

- [ ] **Step 2: Implement the steps**

Inside the smoke, after the existing "insert (1,'first')" step, if the
flag is on:

1. Use `kubectl port-forward` to the current leader pod's health port
   (`:health`).
2. `curl -X POST http://localhost:.../admin/ranges/split` with body
   `{ "sourceRangeId": 1, "splitKey": "accounts/2" }`.
3. Poll `kubectl get opendbcluster opendb -n opendb-system -o json` until
   `.status.conditions[type=RangeCatalogStable].status == "True"` and
   `.status.recoverySummary.rangeCatalog.activeRangeCount == 2`.
4. Insert `(2, 'second')` through pgwire.
5. `kubectl cp` the leader's `commit.wal`, parse it through a small
   reuse of the existing TS WAL parser used by the restart-recovery
   smoke, assert the second insert has `range_id` equal to the split
   response's `rangeIds[1]`.
6. SELECT both rows back and check the count.

- [ ] **Step 3: Update vitest guardrail**

In `tests/cluster/k3s-smoke.test.ts`, mirror the existing
`with-restart-recovery` guardrails for the new flag:

- Default smoke output never contains `/admin/ranges/split`.
- Opt-in flag enables the steps in the plan output.

- [ ] **Step 4: Run focused tests**

```bash
npm run test:cluster
npm run check:manifests
```

- [ ] **Step 5: Commit**

```bash
git add tools/k3s-smoke.ts tests/cluster/k3s-smoke.test.ts
git commit -m "feat: add opt-in range split smoke flag"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

## Task 7: TypeScript and Documentation Guardrails

**Ownership:** One worker owns docs/tests only.

**Files:**
- Modify: `docs/k3s-uat.md`.

- [ ] **Step 1: Append the new opt-in flag to the UAT**

Under the existing `--with-restart-recovery` paragraph, add:

```markdown
Sprint 5 adds an opt-in `--with-range-split` flag that exercises the runtime
split admin endpoint on the leader and verifies the new
`RangeCatalogStable` condition. The default smoke does not call any admin
endpoint and remains non-destructive.
```

- [ ] **Step 2: Final verification**

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

If k3d is available:

```bash
npm run smoke:k3s
npm run smoke:k3s -- --with-restart-recovery
npm run smoke:k3s -- --with-range-split
```

The first two must remain non-destructive (no `kubectl delete pod` in
either). The third is the explicit opt-in for split exercise.

- [ ] **Step 3: Commit**

```bash
git add docs/k3s-uat.md
git commit -m "docs: note range split smoke guardrails"
git push origin main
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

## Final Verification

Re-run the standard suite plus the new opt-in:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm run check:ts
npm run check:no-python
npm run check:manifests
npm test
npm run smoke:k3s
npm run smoke:k3s -- --with-restart-recovery
npm run smoke:k3s -- --with-range-split
git log origin/main --grep="anthropic\\|claude\\|🤖" -i --oneline | wc -l
```

Expected: all green, last `wc -l` returns `0`.

## Review Checklist

- [ ] Commit stream remains the only source of truth.
- [ ] Split/merge metadata still uses `RangeId::ROOT` on the record.
- [ ] Admin endpoint never proxied through pgwire.
- [ ] Followers reject admin writes before any catalog mutation.
- [ ] Allocator is deterministic and stateless.
- [ ] Operator `phase` unchanged; `Recovered` unchanged.
- [ ] No Python file introduced.
- [ ] No new persistent volume, no new container, no new external port.
- [ ] `npm run smoke:k3s` default is still non-destructive.
- [ ] Commit messages contain no AI attribution.
