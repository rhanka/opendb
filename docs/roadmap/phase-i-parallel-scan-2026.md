# Phase I — morsel-driven parallel scan design (2026-06-15)

Direct design (no consensus run — morsel-driven parallelism is the
canonical pattern from Leis et al. 2014 and DuckDB's implementation;
no architectural contest).

## What

After Phase G (vectorized chunk execution) lands, each query
processes one chunk at a time on one CPU. Phase I parallelizes by
splitting the scan into **morsels** (fixed-size work units, ~10K rows
each) and dispatching them across a worker pool.

Reference: Leis et al. "Morsel-Driven Parallelism: A NUMA-Aware Query
Evaluation Framework for the Many-Core Age" (SIGMOD 2014).

## Architecture

```rust
// crates/opendb-sql/src/parallel.rs (new)

pub struct MorselScheduler {
    workers: Vec<JoinHandle<()>>,
    morsel_tx: mpsc::Sender<Morsel>,
    result_rx: mpsc::Receiver<MorselResult>,
}

pub struct Morsel {
    // Slice into the columnar projection — zero-copy reference into
    // the per-column Vec<T> from Phase F.2.
    table: TableId,
    chunk_offsets: Range<usize>,        // rows [start, end)
    pipeline: Arc<Pipeline>,            // the operator chain to run
}

pub enum MorselResult {
    Partial(PartialAggregate),
    Done,
    Error(OpenDbError),
}
```

The scheduler partitions a scan into morsels at planning time, pushes
them onto a shared work queue, and each worker:

1. Pops a morsel off the queue (`mpsc::Receiver::recv`).
2. Runs the per-chunk pipeline (Phase G kernels: filter mask, sum,
   count, groupby hash insertion, ...).
3. Pushes its partial result back via `result_rx`.

The coordinator merges partials into a final result.

## Default thread count

`max_threads = num_cpus::get()` (Rust's `std::thread::available_parallelism`).
Operator override: `OPENDB_MAX_PARALLEL_WORKERS` env var.

Per-query override at runtime: extend the parser with PG's
`SET max_parallel_workers_per_gather = N` syntax (no-op if planner
doesn't pick parallel for the query). For Phase I MVP, support the
GUC parse + storage, even if only `SELECT ... FROM big_table WHERE ...
GROUP BY ...` uses it.

## When to parallelize

Phase I MVP rules:
1. Table size ≥ `parallel_scan_min_rows` (default 100K). Below that,
   the morsel scheduling overhead dwarfs the gain.
2. ColumnarProjection table (`engine = 'columnar'`). RowProjection
   stays single-threaded — its workload is OLTP-shape, doesn't
   benefit from parallel scan and would force MVCC visibility checks
   per chunk into a cross-thread coordination problem.
3. Query is read-only (`SELECT` only). Phase I doesn't parallelize
   writes — that's a different problem (multi-leader / sharding).

Below the threshold OR on row store OR on writes: single-threaded
scan (Phase G's existing path).

## GROUP BY hash table

Two-level hash table per Leis et al.:

1. **Per-worker local hash**: each worker builds its own partial hash
   table for the morsels it processes. No locking. Cache-friendly.
2. **Merge phase**: when all morsels are processed, the coordinator
   merges per-worker partials by hash-partition (Radix-like). For each
   partition bucket, workers reconcile their entries.

For low-cardinality `GROUP BY` (e.g. `GROUP BY status` with 4 values),
the merge is trivial. For high-cardinality (`GROUP BY user_id`), the
two-level approach keeps memory bounded.

## NUMA awareness

**Out of scope** for Phase I MVP. Single-socket commodity hardware
typical for OpenDB deployments today. Revisit if a user runs on a
multi-socket box and we measure cross-socket bandwidth as the
bottleneck.

The morsel API is NUMA-ready (workers are tokio tasks; the OS
scheduler handles NUMA placement). Phase I.next can add explicit
pinning via `tokio::task::Builder::name` + `core_affinity` if needed.

## Acceptance criteria

- TPC-H Q1 over 1 M `lineitem` rows on the ColumnarProjection:
  - 1 worker: same as Phase G measurement (~120 ms warm).
  - 4 workers on a 4-core box: ≥ 3× speedup (≤ 40 ms warm).
  - 8 workers on an 8-core box: ≥ 6× speedup (≤ 20 ms warm).
- Linear-ish scaling up to `num_cpus`. Sub-linear past that (Amdahl)
  is acceptable.
- Single-threaded fallback for tables < 100K rows shows no regression
  vs Phase G baseline.
- Per-worker hash partial sizes measured + reported in
  `docs/bench/parallel-scan-<DATE>.md`.

## Effort

**4-6 weeks.** Breakdown:
- MorselScheduler + worker pool: 1 wk
- Per-chunk pipeline serialization (so a morsel carries everything it
  needs to run independently): 1 wk
- Two-level GROUP BY hash + merge phase: 1-2 wk
- Predicate filter morsel: 0.5 wk (reuse Phase G kernels)
- Test + bench infra: 1 wk
- TPC-H Q1 scaling measurement: 0.5 wk

## Out of scope (Phase I follow-ups)

- **Parallel joins** (hash join with partitioned build/probe). Phase
  I MVP parallelizes single-table scans + aggregations. Joins are
  Phase I.next or Phase J.
- **Window functions in parallel**. Same logic; defer.
- **Cross-NUMA pinning** (see above).
- **Adaptive parallelism** (start with N workers, scale down if
  contention). Static `max_threads` for MVP.

## Track items

Already specced at the WP level in track (I — morsel-driven parallel
scan). The breakdown above slots into that item.

## Dependencies

Hard prerequisites:
- Phase F.1 — `Projection` trait (so the morsel scheduler can target
  `ColumnarProjection` without dispatching through `&dyn`).
- Phase F.2 — `ColumnarProjection` materialization (so morsels are
  contiguous `&[T]` slices, not BTreeMap iterators).
- Phase G — vectorized chunk execution (the kernel that runs inside a
  morsel).

Phase I is the **last** OLAP track phase; depends on all of F + G + H
(H compression doesn't block but should land first to avoid
re-benchmarking).
