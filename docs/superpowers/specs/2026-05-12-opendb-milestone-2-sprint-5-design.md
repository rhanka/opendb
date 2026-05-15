# OpenDB Milestone 2 Sprint 5 Design

Status: draft (2026-05-12)
Author: opendb maintainers

## Goal

Sprint 4 introduced typed split/merge metadata and active range routing as a
catalog projection. Sprint 5 turns those metadata records into a runtime
operation: an operator-visible mechanism to split a range, merge two adjacent
ranges, and let writes land on the resulting non-root range identifiers
without any new subsystem. Everything still flows through the single canonical
commit stream and the OpenRaft-backed root range.

The output of this sprint is the first interactive proof that the range
catalog is not just descriptive — it can be evolved at runtime on a live k3d
cluster, observed through `OpenDbCluster.status`, and exercised by a cluster
smoke test that does not require a process restart.

## Context

After Sprint 4:

- `Mutation::SplitRange { split }` and `Mutation::MergeRanges { merge }` are
  typed mutations in the commit stream and replay deterministically.
- `RangeCatalog` keeps `active_range_ids` and routes a `(table, key)` pair to
  the deepest active descriptor that contains it.
- `Database::execute` stamps every write with the catalog-resolved
  `range_id` before submitting to consensus.
- `RootRange::replay` and `validate_semantic_append` reject metadata records
  that do not target `RangeId::ROOT` and rows that target an unknown logical
  range.

Sprint 5 builds on top of that without changing any of those invariants. No
new mutation types, no second commit log, no separate replication channel for
the new ranges. The new ranges are logical view onto the same root commit
stream — the replicas hosting them are the replicas of the root range
themselves.

## Non-Goals

- No per-range Raft instance, no Raft snapshot install, no real
  physical sharding. Replicas are still the root range replicas.
- No object storage upload, no archive of pre-split data. Archive policy
  remains metadata-only.
- No automatic split trigger by row count, byte size, or latency. We add an
  optional advisory metric, but the act of splitting stays user-initiated in
  this sprint.
- No `ALTER TABLE`, no secondary index, no JSON, no transactions, no joins.
  Those belong to the sentropic-substitution path (sprints 6+).
- No change to the `phase` of `OpenDbCluster`: it stays kube-readiness only.
- No pgwire protocol changes. Admin operations go through a dedicated HTTP
  endpoint on opendb-node, not through pgwire.

## Design Summary

Three independent surfaces, all glued to the existing commit stream:

1. **Admin HTTP endpoint** on each opendb-node, served by the same axum
   router as `/live`, `/ready`, `/status`. Two new routes:
   - `POST /admin/ranges/split`
   - `POST /admin/ranges/merge`
   Both forward to `Database::propose_range_split` /
   `Database::propose_range_merge`, which build the corresponding metadata
   `CommitRecord` and submit it to the root range. Only the OpenRaft leader
   accepts the request; followers reply 421 with the current leader hint.

2. **Range allocator** in `opendb-storage`: deterministic monotonic
   `RangeId` generator based on the catalog's max known id. The allocator
   is a pure projection function — `next_range_id(catalog) -> RangeId(prev
   + 1)` — that callers run against the freshly replayed catalog before
   building the record. There is no allocator state on disk.

3. **Operator surface**: a new condition
   `RangeCatalogStable` on `OpenDbCluster.status.conditions` summarising the
   active range count and the last split/merge transaction id observed by
   the operator, plus the same fields on `OpenDbClusterRecoverySummary`.
   The condition is the operator's aggregation of node `/status` reports —
   it does not own catalog correctness; the database does.

The cluster smoke (`npm run smoke:k3s`) gains an opt-in
`--with-range-split` flag, mirroring `--with-restart-recovery`: default
non-destructive runs do not exercise splits; the opt-in run inserts a row,
triggers a split through the admin endpoint, inserts a second row that must
land on the new child range, and checks the `range_id` stamped on the
replayed WAL plus the operator status.

## Admin Endpoint Contract

`POST /admin/ranges/split`

Body (camelCase JSON, `deny_unknown_fields`):

```json
{
  "sourceRangeId": 1,
  "splitKey": "accounts/5",
  "leftRangeId": 2,
  "rightRangeId": 3,
  "leftReplicaNodeIds": [0, 1, 2],
  "rightReplicaNodeIds": [0, 1, 2]
}
```

Both `leftRangeId` and `rightRangeId` are optional. When omitted, the server
calls the allocator on the just-replayed catalog and assigns the next two
ids. When provided, the server still validates they are unused.

`leftReplicaNodeIds` / `rightReplicaNodeIds` default to the source
descriptor's `replica_node_ids` when omitted.

Responses:

- `202 Accepted` with `{ "rangeIds": [2, 3], "txId": 17 }` once the metadata
  record has been committed through OpenRaft and applied to the local WAL.
- `400 Bad Request` for catalog validation errors (gap, overlap,
  split_key outside source bounds, etc.) using the existing
  `OpenDbError::InvalidInput` text.
- `421 Misdirected Request` on follower nodes with a JSON body
  `{ "leaderId": 1, "leaderAddr": "opendb-1.opendb-peer:7000" }`.
- `409 Conflict` if the source range is already inactive (already split or
  merged away).

`POST /admin/ranges/merge`

Body:

```json
{
  "sourceRangeIds": [2, 3],
  "mergedRangeId": 4,
  "replicaNodeIds": [0, 1, 2]
}
```

Same response codes. `mergedRangeId` is also optional; it defaults to the
allocator output. `replicaNodeIds` defaults to the union of the source
descriptors' replicas.

## Range Allocator

A small function on `RangeCatalog`:

```rust
impl RangeCatalog {
    pub fn allocate_range_id(&self) -> RangeId {
        RangeId(self.max_range_id().0 + 1)
    }

    fn max_range_id(&self) -> RangeId {
        self.descriptors
            .keys()
            .copied()
            .max()
            .unwrap_or(RangeId::ROOT)
    }
}
```

No persistence. The OpenRaft path serialises proposals through the leader,
so two concurrent admin requests cannot allocate the same id.

The descriptor for the new range is built right after allocation. The
record proposed to consensus carries the full descriptor; the catalog
remains the source of truth.

## Database Proposal Surface

`Database` grows two methods:

```rust
impl Database {
    pub async fn propose_range_split(&mut self, request: ProposedRangeSplit) -> OpenDbResult<RangeSplitProposalResult>;
    pub async fn propose_range_merge(&mut self, request: ProposedRangeMerge) -> OpenDbResult<RangeMergeProposalResult>;
}
```

Both refresh the engine from the WAL first (same pattern as `execute`),
build the descriptor, validate it against the fresh catalog, submit to
`RootRange`, then return the result with the assigned ids and the
transaction id. Callers do not see consensus details.

## Operator Surface

The operator already aggregates `/status` reports. We extend the
`/status` JSON with two new fields:

```json
{
  "rootDescriptorKnown": true,
  ...,
  "rangeCatalog": {
    "activeRangeCount": 2,
    "lastSplitTxId": 17,
    "lastMergeTxId": null
  }
}
```

Backward compatibility: existing fields keep their semantics; the new
object is optional in the `deny_unknown_fields` deserializer on the
operator side (use `#[serde(default)]`).

The operator then aggregates across pods and writes one new condition:

```yaml
- type: RangeCatalogStable
  status: "True"   # or "Unknown" if any pod unreachable, "False" if any pod disagrees on active count
  reason: ActiveRangeCountAgrees | PendingSplitMerge | DivergentCatalog | StatusUnknown
  message: "active ranges=2 (lastSplit txId=17, lastMerge txId=none)"
  lastTransitionTime: ...
```

`OpenDbClusterRecoverySummary` gains a `rangeCatalog` block mirroring the
node payload.

This condition is informational only; it never blocks `Ready` or
`Recovered`.

## Cluster Smoke

`npm run smoke:k3s -- --with-range-split` extends the default smoke:

1. Wait for `Recovered=True` (existing).
2. Create a typed table through pgwire (existing).
3. Insert `(1, 'first')` (existing).
4. Call `POST /admin/ranges/split` via `kubectl port-forward` against the
   current leader, with body:

   ```json
   { "sourceRangeId": 1, "splitKey": "accounts/2" }
   ```

5. Poll the operator status until `RangeCatalogStable=True` and
   `activeRangeCount=2`.
6. Insert `(2, 'second')`. The smoke replays the WAL from the leader pod's
   filesystem (using the same `kubectl cp + jq` pattern used by the
   existing smoke) and asserts the second record has `range_id=3` (or
   whatever id the allocator assigned, surfaced in the split response).
7. Select both rows back through pgwire.

The default `npm run smoke:k3s` does not include any of these steps and
does not call any admin endpoint.

## Recovery And Archive Semantics

No change. The new mutations are already covered by Sprint 4's
`validate_record_routes`. The recovery contract still reports
`rootDescriptorKnown`, `walReplayCompleted`, `archiveMetadataReplayed`.
The new `rangeCatalog` block is additive.

After a leader restart, the new admin endpoint waits for the local replay
to complete before serving requests, otherwise the allocator could
allocate an id that is already taken by a record committed before the
restart but not yet replayed locally. This is the same guard already used
by `Database::execute`.

## pgwire Boundary

Unchanged. pgwire stays a client compatibility layer. We deliberately do
not expose split/merge through `ALTER RANGE` SQL in this sprint:

- Drizzle (the future sentropic client) does not need it.
- pgwire prepared-statement plumbing would expand the parser surface for a
  feature that admins can already reach over HTTP.
- Keeping admin off pgwire makes it trivial to ACL the admin endpoint
  separately later (kube NetworkPolicy + Kubernetes RBAC on a future
  custom subresource).

## Test Strategy

- `opendb-storage`: unit tests on the allocator (`allocate_range_id`,
  monotonicity, gap-after-merge behaviour).
- `opendb-storage`: `RangeCatalog::route_key` still correct after a
  sequence of split → merge → split records (golden trace).
- `opendb-node`: `Database::propose_range_split` happy path, follower path
  (`NotLeader`), invalid path (overlapping bounds, unknown source).
- `opendb-node`: admin HTTP endpoint returns the right HTTP codes and
  JSON shapes; integration test through `axum::Router::oneshot`.
- `opendb-operator`: `/status` deserializer accepts both legacy payloads
  (no `rangeCatalog` field) and new payloads; aggregator computes
  `RangeCatalogStable`.
- `vitest tests/cluster`: split admin endpoint manifest checks
  (`tools/check-manifests.ts` must not flag the new route as needing a
  Service); restart-recovery still passes; the new
  `--with-range-split` opt-in is reflected in `k3s-smoke.test.ts`.
- `vitest tests/parity`: existing pgwire smoke must keep passing after a
  split (the SELECT must still return both inserted rows).

## Review Points

- Allocator deterministic and stateless.
- Admin endpoint behind axum, never proxied through pgwire.
- Split/merge records always have `range_id=ROOT`; Sprint 4 invariant
  preserved.
- Follower writes still rejected before WAL touch.
- `OpenDbCluster.status.phase` unchanged.
- No new container, no new network port, no new persistent volume.
- No Python file introduced anywhere.
- Commit messages strip every AI attribution.

## Acceptance Criteria

- `cargo test --workspace`, `cargo clippy -D warnings`, `cargo fmt --check`
  green.
- `npm run check:ts`, `npm run check:no-python`, `npm run check:manifests`,
  `npm test` green.
- `npm run smoke:k3s` non-destructive default still passes.
- `npm run smoke:k3s -- --with-restart-recovery` still passes.
- `npm run smoke:k3s -- --with-range-split` passes end-to-end on k3d.
- `git log origin/main --grep="anthropic\|claude\|🤖" -i --oneline | wc -l`
  returns `0` after every commit.
