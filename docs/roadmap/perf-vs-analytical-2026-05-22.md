# OpenDB vs. ClickHouse / Vertica / DuckDB — analytical-perf gap — 2026-05-22

Scope: what it would take for OpenDB (Rust, pgwire, in-memory, row-store projection)
to compete on **analytical workloads** with ClickHouse, Vertica, and DuckDB. Current
architecture is the wrong shape for OLAP at scale; this is the strategic input for
the milestones after Milestone 1. Read-only research; no source changes.

---

## 0. Current OpenDB shape — recap

- Row-store projection rebuilt from a JSON WAL of `CommitRecord`s
  (`crates/opendb-storage/src/row_projection.rs:50` — `RowProjection { tables: BTreeMap<String, Table> }`,
  each `Table` holds `rows: BTreeMap<String, BTreeMap<String, Value>>` at
  `crates/opendb-storage/src/row_projection.rs:17-23`).
- Mutations typed (`Mutation::InsertRow { table, key, values: Vec<ColumnValue> }`,
  `crates/opendb-storage/src/commit_stream.rs:115`); runtime rep is
  `BTreeMap<String, Value>` per row — one heap alloc per cell, no contiguous storage.
- WAL frames are `serde_json::to_vec(record)` (`crates/opendb-storage/src/wal.rs:184-185`)
  — text JSON, no compression, no batching primitive.
- Executor scans `table_state.rows.values()` row-by-row, filters predicates, folds
  aggregates into `AggregateState` per group
  (`crates/opendb-sql/src/executor.rs:618-676` for `SELECT *`,
  `crates/opendb-sql/src/executor.rs:818-852` for `GROUP BY`).
  Single-threaded, no SIMD, no batching.
- Current bench: 500 rows, sub-ms reads because the working set fits in L1
  (`docs/bench/sentropic-bench-2026-05-21.md`). Meaningless for OLAP.

Fine for HTAP-leaning OLTP point reads. Wrong for
`SELECT sum(x), avg(y) FROM t WHERE z BETWEEN ... GROUP BY w` over 1B rows.

---

## 1. ClickHouse — what makes it fast (ref: 23.x / 24.x lineage)

ClickHouse is the reigning open-source OLAP heavyweight on ClickBench
(<https://benchmark.clickhouse.com/>). Five pillars:

### 1.1 MergeTree storage

- **Columnar files per part.** Each `MergeTree` table is a forest of immutable
  *parts*; each part stores one file per column (`column.bin` + `column.mrk`
  marks). Reads only touch the columns the query references.
- **Sorting key (ORDER BY clause).** Defines physical layout inside each part.
  The primary index is *sparse* (default one entry per 8192-row "granule"), so
  the index itself fits in RAM for tables with trillions of rows. Range scans
  on the sorting key become "find the granule range, sequential read".
- **Partitioning key** (usually month or day). Lets the optimizer prune entire
  parts before reading anything. Partitions are also the unit for `ALTER TABLE
  ... DROP PARTITION` (cheap mass delete).
- **Background merges.** Parts coalesce via leveled merges (à la LSM), keeping
  the per-query part fan-out bounded.

Refs: <https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree>.

### 1.2 Vectorized execution

- Block-based pipeline: each operator consumes/produces `Block`s of typically
  65 536 rows × N columns. Aggregations, filters, expressions are loops over a
  column array.
- Hand-written SIMD intrinsics for hot kernels (sum, count, filter mask
  application, common comparators on i32/i64/f64). Plus aggressive
  auto-vectorization friendly code.
- The same vectorized layout is reused for the network protocol (Native block
  format), avoiding row-by-row materialization on the way out.

### 1.3 Compression codecs

- Per-column codec stack. Defaults: `LZ4` (fast) or `ZSTD` (denser). Specialty
  codecs: `Delta` (good for monotonic IDs / timestamps), `DoubleDelta`,
  `Gorilla` (floats with high temporal correlation, e.g. metrics),
  `T64` (bit-packing for low-cardinality integers), `FPC` (floats).
- Codecs compose: `Delta, ZSTD` is the canonical timestamp recipe. Compression
  is computed once per granule, decompressed on read.
- Practical result: 5–20× compression on real telemetry / log workloads, and
  the decompressor often runs faster than the disk it reads from — compression
  becomes a *speedup*, not a tax.

### 1.4 Parallel execution

- Per-part parallelism (each part scans independently) + per-stream
  parallelism inside large parts. Default `max_threads = num_cpu_cores`.
- Aggregations use two-level hash tables and partial-aggregate merging across
  threads — almost linear scaling on `GROUP BY` until you hit memory bandwidth.
- Distributed extension via `Distributed` table engine (shards = remote
  clusters, query is fanned out and merged).

### 1.5 Materialized views & projections

- `MATERIALIZED VIEW` triggers on insert → maintains a derived MergeTree
  (typically pre-aggregated). Reads of the matching shape become "scan the MV".
- `ProjectionsMergeTree` (alternative physical layouts inside the same table)
  is ClickHouse's answer to Vertica projections — same data, multiple sort
  orders, optimizer picks the best for the query.

### 1.6 Trade-offs ClickHouse made

- No real multi-statement transactions on `MergeTree` (atomic batched insert
  only; transactional MergeTree is experimental at best).
- `UPDATE` and `DELETE` are async "mutations" that rewrite parts — fine for
  occasional GDPR-style erasures, useless as an OLTP primitive.
- Eventual consistency on replicated tables (ZooKeeper / ClickHouse-Keeper
  coordinated).
- Joins historically weak (hash join, build side must fit memory). Improving
  but still not the engine's strong suit — fact/dim layouts and pre-joined
  MVs are the idiomatic answer.

### 1.7 What this means for OpenDB

ClickHouse is fast *because* it gave up on OLTP. We can't copy that wholesale
(we want pgwire transactions). But the read-side stack — columnar layout,
vectorized scan, per-column compression, sparse granule index, per-part
parallelism — is additive: it doesn't break the OLTP write path.

---

## 2. Vertica — what to steal (ref: 12.x / 24.x)

Vertica is proprietary but the design is well-documented in the C-Store /
Vertica papers (Stonebraker et al., VLDB 2005; "The Vertica Analytic Database:
C-Store 7 Years Later", VLDB 2012).

### 2.1 ROS + WOS tuple mover

- **WOS** (Write-Optimized Store): in-memory row-ish staging area for fresh
  inserts. Cheap to write, not great to scan.
- **ROS** (Read-Optimized Store): on-disk, columnar, sorted, encoded,
  compressed. Built to be scanned fast.
- **Tuple mover** background process: drains WOS → ROS in batches; merges
  small ROS containers (mini-merges) and consolidates large ones (mergeout).
- Note: Vertica 9+ deprecated WOS in favour of direct-to-ROS with in-memory
  buffer; we cite the original because the *pattern* (write-side and
  read-side have different layouts, async mover between them) is what's
  worth borrowing. This maps cleanly onto OpenDB's "WAL is row-shaped, but
  the read-side projection can be columnar" idea.

### 2.2 Projections

- A *projection* in Vertica is a sorted, encoded, on-disk materialization
  of (a subset of) a table's columns. A table can have many projections;
  the optimizer picks the projection that best matches the query.
- The "super-projection" stores every column and is mandatory; auxiliary
  projections (e.g. sorted by a different key, or pre-joined with a
  dimension) accelerate specific queries.
- This is the conceptual ancestor of ClickHouse's `ProjectionsMergeTree`
  and Snowflake's clustering keys.

### 2.3 Encoding & compression

- Per-column encoding chosen via the *Database Designer* tool (or manual):
  RLE, delta, dictionary, block-dictionary, bit-packed, GZIP, LZO.
- Encodings compose with compression. The encoder is picked from a sample
  pass over the data (cardinality, sortedness, value distribution).

### 2.4 MPP execution

- Shared-nothing, shards by *segmentation expression* (hash by some
  column) across nodes.
- "K-safety": data is replicated K+1 times across segments so the cluster
  survives K node losses.
- Query plan is a tree of *operators* exchanged across nodes via the
  internal data transport. Joins use either local (same segmentation key
  on both sides) or broadcast / resegment moves.

### 2.5 What to steal

- **Multiple physical layouts per logical table.** Projections is the
  pattern. OpenDB's `RowProjection` already isolates rebuilt state behind
  the WAL → extend it so multiple `Projection` impls coexist (row,
  columnar-sorted-by-A, columnar-sorted-by-B).
- **Async mover semantics.** Row-side write commit; columnar read-side
  materialization; read columnar when caught up, fall back to row state
  for the not-yet-moved tail.

MPP-distributed Vertica is out of scope short-term — hard distributed-systems
problem, and ClickHouse + DuckDB Local both punted on it early.

---

## 3. DuckDB — the relevant comparison point (ref: 1.0 / 1.1)

DuckDB is the most informative reference for OpenDB because: single-node,
embeddable, no distributed story, MIT license, written in C++ but with a
clean architecture that's easy to mirror in Rust.

### 3.1 Vectorized execution (Vector + DataChunk)

- A `Vector` is a typed, fixed-capacity (default 2048 rows) column of
  values plus optional null mask + selection vector.
- A `DataChunk` is a tuple of `Vector`s — i.e. a row batch in columnar
  layout.
- Operators are pull-based pipelines; each `Execute(DataChunk& input,
  DataChunk& output)` call processes one chunk. Filters, expressions,
  hashing, aggregation all loop over the vector arrays — auto-vectorizes
  to SIMD on hot loops.

This is the single most important pattern to copy if we want any chance
on ClickBench. Per-row iteration is the wrong shape; per-vector is
roughly an order of magnitude faster on dense numeric work even
*without* hand-written SIMD.

### 3.2 Columnar in-memory format

- Even in pure-RAM use (the common DuckDB case), tables are stored
  column-major. Row groups (~120k rows) × columns.
- Compressed in-memory by default: bit-packing for integers, dictionary
  for low-cardinality strings, FSST for high-cardinality strings,
  Patas / Chimp for floats. Compression and vectorized scan compose:
  the scanner produces an already-decompressed `Vector` per chunk.

### 3.3 Parallel pipelines

- `morsel-driven` scheduling (paper: "Morsel-Driven Parallelism" —
  Leis et al., SIGMOD 2014). The scan is sliced into morsels
  (~100k rows); a thread pool pulls morsels, runs the pipeline, and
  contributes to per-thread partial aggregates that are merged at the
  end.
- Single-node scaling on `count/sum/group by` is close to linear up to
  the memory bandwidth wall.

### 3.4 Why it's competitive on a single node

ClickBench shows DuckDB within 1.5–3× of ClickHouse on most queries on a
single node, with a fraction of the operational footprint. The delta is
ClickHouse's larger SIMD-kernel investment and a more mature optimizer;
the fundamentals are the same. Confirms the hypothesis: **a well-architected
single-node columnar engine can match or beat distributed engines on
workloads that fit in one box's memory** — exactly OpenDB's target.

### 3.5 What's directly portable to Rust

- Vector / DataChunk: trivial — `Vec<T>` + `BitVec` null mask + optional
  `selection: Vec<u32>` for unmaterialized filters.
- Morsel scheduling: a `rayon` parallel iterator over morsels with
  thread-local hash maps merged at the end. Rust's borrow checker makes
  the per-thread aggregator pattern *cleaner* than C++.
- Operator interface: a `Source → Operator → Sink` trait family. We
  already have an `Executor` boundary at
  `crates/opendb-sql/src/executor.rs:41` to plug this in behind.

---

## 4. Standard analytical benchmarks — which one to target

### 4.1 ClickBench — <https://benchmark.clickhouse.com/>

- **What it tests.** 43 queries over a single denormalized 100M-row
  hits table (anonymized web analytics, ~14 GB compressed). Mix of
  point lookups, `WHERE` filters, GROUP BYs of varying cardinality,
  TOP-N, regex / `LIKE` patterns. Run cold + hot (memory cache after
  first run). Score = sum of normalized timings against a reference
  baseline, lower is better.
- **Why it's the right first target.** Single-table, no joins, dataset
  fits on a laptop (100M rows × ~80 columns). Every serious OLAP engine
  in 2024–2026 publishes ClickBench numbers. There's a public ranking
  with ClickHouse, DuckDB, Snowflake, StarRocks, Doris, Umbra, etc. —
  immediate apples-to-apples positioning.

### 4.2 TPC-H

- 22 queries, joins-heavy (LINEITEM × ORDERS × PARTSUPP × ...). Scale
  factor in GB (SF=1, 10, 100, 1000, 10000). The de-facto "real OLAP"
  benchmark for decades.
- Stresses *join planning* and *cardinality estimation* much more than
  ClickBench. Less interesting for OpenDB phase 1 — we don't have a
  join optimizer worth mentioning yet, and the scan layer is the
  bottleneck.
- Worth targeting at phase 3+ once we have columnar scan + reasonable
  hash join.

### 4.3 TPC-DS

- 99 queries, much more complex schema (snowflake), heavier on window
  functions, subqueries, multi-way joins. The "advanced" OLAP test.
- Not relevant for OpenDB for at least a year. We don't have window
  functions, the parser doesn't accept much of the TPC-DS dialect, and
  the optimizer would be the bottleneck long before storage.

### 4.4 Recommendation

**Target ClickBench first.** (1) Public scoreboard against every relevant
competitor — direct visibility on the user's actual metric. (2) Single-table,
so we sidestep join optimization (a separate multi-quarter project) and focus
on the layer where OpenDB is weakest: scan throughput. (3) Dataset fits in
memory on a 32 GB workstation — matches OpenDB's in-memory model. (4) The 43
queries cover the patterns that dominate real OLAP (filters, aggregates,
top-N).

After ClickBench parity → TPC-H SF=10 (joins) → TPC-H SF=100 (scaling).
TPC-DS only once we have a serious optimizer.

---

## 5. Architectural gaps in OpenDB (today → analytical-competitive)

In rough order of "how badly it hurts a 1M-row aggregation":

### 5.1 No columnar storage

`Table.rows: BTreeMap<String, BTreeMap<String, Value>>`
(`crates/opendb-storage/src/row_projection.rs:17-23`).
Every aggregate over `sum(x)` walks the outer BTreeMap (pointer chase,
log-n descent), then for each row walks the inner BTreeMap to find the
`x` column (another pointer chase). For a 1M-row × 50-column table, an
aggregate over one column touches all 50 columns' worth of cache
lines. Columnar storage would touch 1/50th of the memory bandwidth.
**Estimated impact alone: 10–50× on narrow aggregates.**

### 5.2 No vectorized execution

`select_aggregated` calls `slot.accumulate(expr.func, &value)` per row
per aggregate (`crates/opendb-sql/src/executor.rs:842-851`). Per-row
function dispatch, per-row `Value::Int64(_)` matching, per-row
heap-allocated `Value` cloning at `row_lookup`. Auto-vectorization is
impossible — the loop body is a chain of pointer chases and tagged
unions. **Estimated impact: 4–10× on top of columnar storage.**

### 5.3 No compression / encoding

Storage holds raw `Value::Int64(i64)` — 8 bytes payload + ~24 bytes of
enum tag overhead per cell. Compare ClickHouse's `T64` (bit-packed,
often 1–2 bytes per int) or DuckDB's bit-packing + RLE inline.
**Estimated impact: 2–5× on memory bandwidth-bound scans + 5–20× less
RAM for the same dataset (i.e. we can hold 5–20× more data in the same
box).**

### 5.4 No parallelism

`table_state.rows.values()` is a single-threaded iterator
(`crates/opendb-sql/src/executor.rs:818`). On a 16-core box we leave
15 cores idle. **Estimated impact: 8–12× on cores-bound queries.**

### 5.5 JSON-encoded WAL

`serde_json::to_vec(record)` per CommitRecord
(`crates/opendb-storage/src/wal.rs:184-185`), `Value`s as JSON tagged enums. A
`(1, 'Ada', 42.5)` insert becomes ~80 B JSON vs. ~24 B binary. At OLAP ingest
rates (millions of rows/s) this dominates fsync bandwidth. Mitigation:
bincode / postcard / a custom binary frame keyed off the `Mutation`
discriminant — same enum, different encoder. The *only* write-path gap;
everything else above is read-side.

### 5.6 Per-cell `String` column keys

`BTreeMap<String, Value>` keys columns by textual name. For a typed columnar
engine the column position is known at plan time — *zero* per-row lookup
cost. Disappears for free with columnar storage.

### 5.7 No statistics / no optimizer

`select_all` picks scan vs. pk-lookup by a syntactic check
(`crates/opendb-sql/src/executor.rs:657-668`). No cost model, no histograms.
Fine at 500 rows; useless picking between scan orders on 100M.
**Impact: large but unbounded — "wrong plan picked", not a constant factor.**

### 5.8 No zone maps / min-max indexes

`WHERE created_at BETWEEN x AND y` on 100M rows scans 100M rows.
ClickHouse's sparse primary index + per-granule min/max often reads <1% of
the data. **Impact: 10–1000× on selective range queries.**

### 5.9 No expression JIT

ClickHouse and DuckDB push hot expressions through LLVM JIT or hand-rolled
specialisations; OpenDB interprets the AST. Long-tail concern — 5.1–5.4
dominate first.

---

## 6. Phased plan to competitive analytical perf

Effort estimates are calendar-time for a single engineer working
roughly full-time on opendb. Confidence intervals are wide.

### Phase 1 — hybrid row+columnar projection (Q3 2026, ~6–8 weeks)

**Goal:** add a read-side columnar materialization next to
`RowProjection`, kept up to date from the same WAL stream. No write
path changes; columnar side is rebuilt by the same `apply()` mechanism.

- New trait `Projection { fn apply(&mut self, rec: &CommitRecord); }`
  with two implementations: `RowProjection` (existing) and
  `ColumnarProjection` (new).
- `ColumnarProjection` stores one `Vec<Value-of-T>` per column (typed,
  no enum wrapper for the common path — `Vec<i64>`, `Vec<f64>`,
  `Vec<Option<String>>`). Row groups of ~64k for memory locality.
- Executor learns a `scan_columnar(table, columns, filter) → ChunkStream`
  path. Aggregates and filters that don't need joins use the columnar
  scan; everything else falls back to the row path.
- Acceptance: 1M-row aggregation goes from "doesn't fit in a sane
  bench" to "matches PostgreSQL". Probably **5–15× faster than
  today's row scan** on narrow aggregates.
- **Risk:** keeping two projections in sync. Mitigation: same
  `apply()` callsite, deterministic from the WAL, single-writer →
  no race.

### Phase 2 — vectorized execution (Q4 2026, ~8–10 weeks)

**Goal:** rewrite the scan/filter/aggregate executor in batches of
~2k–8k rows in columnar layout.

- Introduce `DataChunk { columns: Vec<Vector>, len: usize }` and
  `Vector` (typed array + null bitmap + optional selection vector).
- Replace `select_aggregated`'s per-row loop
  (`crates/opendb-sql/src/executor.rs:818-852`) with a per-chunk
  loop. Aggregators expose `accumulate_chunk(&mut self, vec:
  &Vector, sel: &SelectionVector)`.
- Hot kernels (sum/count/min/max on i64/f64, eq/lt/gt comparators)
  written as straight loops over typed slices — let LLVM
  auto-vectorize first; hand-roll SIMD only where the codegen is
  bad. Use `std::simd` (portable_simd) when it stabilizes; until
  then `wide` or manual `core::arch::x86_64`.
- Acceptance: another **3–6× over Phase 1** on aggregate-heavy
  queries. ClickBench-style queries become testable end-to-end.
- **Risk:** the abstraction layer between AST and chunk-execution is
  the part we don't have yet. Probably 30% of phase 2's cost.

### Phase 3 — compression + encoding (Q1 2027, ~6–8 weeks)

**Goal:** the columnar projection stores compressed data, decompressed
into `Vector`s at scan time.

- Per-column encoders: bit-packing for low-range integers; dictionary
  for low-cardinality strings (one `Vec<String> dict` + `Vec<u32>
  codes`); delta for monotonic columns; FSST or LZ4 for arbitrary
  strings.
- Scan-time decoder produces a `Vector` per chunk. Filters can run
  *on the encoded representation* for some codecs (dictionary
  filters: compare against dictionary codes, not strings — order of
  magnitude faster).
- Acceptance: **2–4× memory reduction** (= 2–4× more data per box);
  on memory-bandwidth-bound queries, **1.5–3× faster** (less RAM to
  scan).
- **Risk:** dictionary encoding adds a per-batch state. Filter
  pushdown to encoded form is fiddly. Start with bit-packing + LZ4
  only and add codecs over time.

### Phase 4 — parallel scan (Q2 2027, ~4–6 weeks)

**Goal:** use all the cores.

- Morsel-driven scheduling via `rayon` over row-group boundaries.
- Per-thread partial aggregates merged at the sink.
- Two-level hash table for `GROUP BY` (cardinality-adaptive).
- Acceptance: **near-linear scaling to physical core count** on
  aggregate workloads. A 16-core box gets close to 12–14× over
  Phase 3.
- **Risk:** the BTreeMap/HashMap merge step on `GROUP BY` is the
  classic Amdahl tail. Two-level hashing is the standard fix.

### Phase 5 — distributed/MPP (optional, late 2027+)

**Goal:** multi-node shared-nothing.

- Hash-segmentation across nodes (Vertica-style), gossip via the
  existing consensus layer (`crates/opendb-consensus/`).
- Exchange operator for resegment / broadcast.
- This is a multi-quarter, possibly multi-engineer project. Only do
  it if (a) single-node OpenDB is genuinely at ClickHouse parity *and*
  (b) we have customers asking for it.

### Rough overall timeline

```
2026-Q3 ── Phase 1 (hybrid row+columnar)  ── parity with PG, 5–15× over today
2026-Q4 ── Phase 2 (vectorized exec)      ── within 5× of ClickHouse on narrow agg
2027-Q1 ── Phase 3 (compression)          ── within 3×, fit 10× more data
2027-Q2 ── Phase 4 (parallel scan)        ── within 2× single-node, beat DuckDB on some
2027-Q3+── Phase 5 (distributed, optional)
```

Confidence: Phase 1 is high (8 wks ± 2). Phase 2–4 calibrated against DuckDB
git history + Velox / Photon postmortems — credible but treat ±50% as the
realistic band.

---

## 7. Smallest demo that shows OpenDB ≥ ClickHouse on something

The user asks: "1M-row aggregation with cold cache and warm cache, on
a simple TPC-H Q1-like query. What architectural minimum?"

### 7.1 The query

TPC-H Q1 simplified:

```sql
SELECT
  l_returnflag,
  l_linestatus,
  sum(l_quantity),
  sum(l_extendedprice),
  avg(l_discount),
  count(*)
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

- 1M rows of lineitem (= TPC-H SF≈0.17). Fits easily in RAM (≈170 MB
  raw).
- 4 aggregates, 2 grouping columns, 1 range filter, small final
  result (≤ 4 groups). Classic ClickBench-shaped.

### 7.2 What ClickHouse does on this (reference)

On a modern 8-core laptop, ClickHouse 24.x finishes this in
**~30–80 ms warm**, ~150–250 ms cold. Memory bandwidth-bound — it
reads ~7 columns × 8 bytes × 1M = ~56 MB of decoded data, sums them
in vectorized loops, finishes.

### 7.3 What OpenDB would need to match

To plausibly hit 30–80 ms warm we need, at minimum:

1. **Columnar storage** (Phase 1). Touching only the 7 referenced
   columns instead of all of them is a 5–10× cut on memory bandwidth.
2. **Vectorized scan with typed kernels** (Phase 2). Per-row dispatch
   over 1M rows would burn ~50 ms in instruction overhead alone; per-
   chunk dispatch over 500 chunks burns sub-ms.
3. **Single-threaded is fine for 1M rows.** ClickHouse doesn't need
   to parallelise for queries this small; neither do we. Parallel
   scan (Phase 4) is not on the critical path for this demo.
4. **Compression nice-to-have, not required at 1M rows.** Phase 3
   would help fit more data, but the demo dataset is small enough
   that raw `Vec<i64>` works.
5. **JSON WAL is irrelevant for this demo** (read-only query). For
   the *ingest* part of the demo (loading 1M rows), JSON encoding
   would make load slow but not query slow.

### 7.4 Minimum-viable architecture for the demo

**Phase 1 + Phase 2, single-threaded, no compression.** Skip everything else.

- Columnar `Vec<i64>` for `l_quantity`, `l_extendedprice`, `l_discount`,
  `l_shipdate`; `Vec<u8>` dict code for `l_returnflag`, `l_linestatus`.
- Vectorized aggregator: per 2k-row chunk, eval the filter to a selection
  mask, run `sum_with_mask` / `count_with_mask` / `groupby_with_mask` over
  typed slices.
- Hash table for 2-col group-by: small, in cache, no merge step.

Plausible: **40–120 ms warm, 80–250 ms cold** on a modern laptop. Parity
with ClickHouse on this specific query, single node, 1M rows. Winnable.

### 7.5 What it costs

Phase 1 + Phase 2 minimum slice, just enough for this demo: **~10–14 weeks**
(vs. 14–18 for the fully-general implementations). Cuts: only the columns
the demo needs (i64, u8 dict, no nulls in hot cols); only the operators the
demo needs (scan, filter-by-mask, hash-aggregate, sort, project) — skip
joins, windows, subqueries. Tracer bullet — once it flies, every extension
(more types, ops, parallelism, compression) plugs into the same foundation.

---

## 8. Honest uncertainty register

- **Multipliers in §5 and §6 are calibrated from published DuckDB /
  ClickHouse engineering posts, not measured on OpenDB.** Directionally
  right; constants on Rust + our data shapes could vary 2×.
- **Phase 1 is highest-leverage and lowest-risk.** If only one phase gets
  funded, fund Phase 1 — the `Projection` trait + columnar materialization
  is what enables everything later.
- **pgwire limits some optimizations.** Returning results as columnar
  chunks (ClickHouse Native trick) isn't possible — pgwire is row-oriented.
  Row-materialization cost is paid only on the final aggregated rows, so
  negligible for OLAP shapes.
- **We haven't measured wire vs. scan share of current latency.** The
  2026-05-21 bench is 500 rows where wire dominates. Need a 100k+ row
  baseline before Phase 1.
- **Joins are not addressed here.** Phase 1–4 give a great single-table
  engine. Beating ClickHouse / Vertica on join-heavy queries (TPC-H beyond
  Q1/Q6) needs a separate hash-join + cardinality-estimator track,
  deliberately out of scope.

---

## 9. References

External:
- ClickHouse MergeTree: <https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree>
- ClickHouse architecture: <https://clickhouse.com/docs/en/development/architecture>
- ClickBench: <https://benchmark.clickhouse.com/>
- DuckDB internals: <https://duckdb.org/docs/internals/overview>
- "How DuckDB executes queries": <https://duckdb.org/2021/06/25/querying-parquet.html>
- "Morsel-Driven Parallelism" (Leis et al., SIGMOD 2014): <https://15721.courses.cs.cmu.edu/spring2016/papers/p743-leis.pdf>
- "C-Store" (Stonebraker et al., VLDB 2005): <https://www.vldb.org/archives/website/2005/program/paper/thu/p553-stonebraker.pdf>
- "Vertica: C-Store 7 Years Later" (Lamb et al., VLDB 2012): <https://vldb.org/pvldb/vol5/p1790_andrewlamb_vldb2012.pdf>
- TPC-H: <https://www.tpc.org/tpch/> · TPC-DS: <https://www.tpc.org/tpcds/>

OpenDB code anchors:
- `crates/opendb-storage/src/row_projection.rs:17` — `Table.rows`
- `crates/opendb-storage/src/row_projection.rs:50` — `RowProjection`
- `crates/opendb-storage/src/commit_stream.rs:8` — `Value` enum
- `crates/opendb-storage/src/wal.rs:184` — `encode_frame` (JSON)
- `crates/opendb-sql/src/executor.rs:41` — `execute()` entry
- `crates/opendb-sql/src/executor.rs:618` — `select_all` scan path
- `crates/opendb-sql/src/executor.rs:720` — `select_aggregated`
- `docs/bench/sentropic-bench-2026-05-21.md` — current bench baseline
