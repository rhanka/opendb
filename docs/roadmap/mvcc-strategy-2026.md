# Phase C — MVCC strategy decision (2026-06-11)

5-way agent consensus on whether to implement OpenDB's per-row versions
as **(A) a `VersionChain` wrapper around the existing `BTreeMap<key, Row>`**
or **(B) a packed heap-tuple replacement of `Row`** (PG-style heap files).

**User ratified Strategy A on 2026-06-11.**

## Decision: Strategy A — wrapper around BTreeMap

5/5 votes for A. Estimated effort: **3 weeks**. Single dominant risk
across all voters: **unbounded version chains degrade hot-row read
latency unless vacuum/HOT-pruning ships at the same time as MVCC**.

## Concrete shape

```rust
struct VersionChain {
    versions: Vec<TupleVersion>,
}
struct TupleVersion {
    xmin: TxnId,
    xmax: Option<TxnId>,
    row: BTreeMap<ColumnName, Value>,
}
// In RowProjection::Table:
rows: BTreeMap<KeyString, VersionChain>,
```

Visibility resolution under a `Snapshot { xmin_horizon, in_progress: HashSet<TxnId> }`:
walk the chain newest-to-oldest, pick the first version where
`xmin ≤ snapshot AND xmin ∉ snapshot.in_progress AND (xmax is None OR
xmax > snapshot OR xmax ∈ snapshot.in_progress)`.

## Consensus rationale (synthesis of 5 voter answers)

1. **Phase C goal is concurrency semantics, not storage efficiency.**
   Readers-don't-block-writers comes from xmin/xmax visibility, which
   Strategy A delivers natively. Strategy B is a different concern (cache
   layout) being conflated with MVCC.

2. **Blast radius matters before MVCC is proven correct.** Strategy B
   touches `RowProjection`, `SqlEngine` executor, WAL replay, and every
   column-value access site simultaneously. That's months of debugging
   on top of months of implementation. Strategy A keeps the change
   surface tight — only the projection layer's `BTreeMap` value type
   changes shape.

3. **Calendar leverage.** 2-3 weeks for A vs 6-12 weeks for B frees up
   8-9 weeks for the things that actually unblock users: vacuum / HOT-
   pruning (required regardless of A or B), snapshot isolation
   correctness tests, pgwire concurrency (Phase B), and the Projection
   trait (Phase F.1) that lets ColumnarProjection coexist.

4. **Optionality preserved.** Strategy A doesn't burn the bridge to B.
   Once MVCC visibility is proven against real workloads, a follow-up
   storage-format phase can rewrite `Row` to a heap-tuple layout for
   scan throughput — that work belongs in the **OLAP track (Phase F+)**
   anyway, where contiguous columnar layout is the actual goal.

5. **The Strategy B "win" is partly wasted today.** Cache-line-friendly
   tuple layout only helps when the outer container is also dense. As
   long as the per-table state is `BTreeMap<String, …>` (string keys,
   tree indirection), the bottleneck is elsewhere — premature
   optimization to address it now would be invisible in benchmarks.

## Acceptance criteria (must ship together — top risk mitigation)

- `VersionChain` wrapper in projection.
- `Snapshot` type + visibility resolver.
- `TxnId` allocator (atomic) + commit log entries `(txn_id, commit_ts, status)`.
- **Vacuum / HOT-pruning task** running in background — prunes versions
  where `xmax < oldest_active_snapshot`. **Non-negotiable** — without
  this, the unanimous top risk materializes immediately.
- Concurrent reader acceptance bench: long-running SELECT does not block
  concurrent UPDATEs; `pgbench -c 32` reaches ≥ 50 % of `single-client ×
  32` throughput.

## Out of scope for Phase C

- Heap-tuple layout (deferred to a dedicated storage-format phase,
  most naturally bundled with Phase F's ColumnarProjection work).
- Predicate locks (SSI / SERIALIZABLE isolation). Default isolation
  stays READ COMMITTED to match PG and what pgbench measures.
- Multi-version GC tunables (autovacuum-equivalent worker pool, cost
  limits). One vacuum task is enough for Phase C; pooling is later.

## Provenance

5 independent voter outputs, all converging on Strategy A with 3-week
effort and identical top-risk phrasing (unbounded version chains absent
vacuum). Voter transcripts in `/tmp/claude-0/.../tasks/ae5f54cf*,
af049e42*, a876d9f1*, a6b3ffac1*, aea53dc7*`. User ratification recorded
in `docs/roadmap/decisions-for-user-2026-06-11.md`.
