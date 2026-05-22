# OpenDB performance vision — beat PG transactional AND beat ClickHouse/Vertica analytical

Status: 2026-05-22 synthesis. Companion research notes:
- `docs/roadmap/perf-vs-transactional-2026-05-22.md` — OLTP gap vs PostgreSQL
- `docs/roadmap/perf-vs-analytical-2026-05-22.md` — OLAP gap vs ClickHouse/Vertica/DuckDB

This doc is the **product-level synthesis** — what's the destination, what's the sequence, and what we are *not* doing.

---

## 1. The two mandates

OpenDB has to be credibly faster than **PostgreSQL** on transactional workloads *and* credibly faster than **ClickHouse / Vertica / DuckDB** on analytical workloads. Today we are roughly:

| Axis | Reference | Current OpenDB | Gap |
|------|-----------|----------------|-----|
| OLTP point reads (sentropic POC, single client) | PG 16, sub-ms | OpenDB sub-ms (≥ PG on B3/B4) | parity ✅ |
| OLTP writes (sentropic POC, single client) | PG 16, 0.087 s seed | OpenDB 1.99 s seed | 23× slower ❌ |
| OLTP concurrent writes | PG 16, scales to ~32 cores | not measured (Database `Mutex` serializes all sessions) | unknown, structurally bad |
| OLAP single-table aggregate (1 M+ rows) | ClickHouse, ~30-80 ms | not measured (row-store, single-threaded scan, no vectorization) | ~100-1000× slower estimated |

The user's instinct that "we won't get there without a real study and plan" is correct. The two companion docs are that study. This doc is the plan.

---

## 2. The shared architectural backbone

Both mandates require the same five pieces of infrastructure. Building any of them once buys progress on both axes:

1. **Drop the global `Database` Mutex** — both OLTP scaling and concurrent OLAP scans require it (`docs/roadmap/perf-vs-transactional-2026-05-22.md` §3.1).
2. **MVCC with per-row versions** — OLTP needs readers not to block writers; OLAP needs long-running scans not to block ingest (OLTP §3.3, OLAP §6 cross-ref).
3. **Group-commit WAL writer** — OLTP commit throughput AND OLAP `COPY FROM` bulk ingest both ride this (OLTP §1.2, OLAP §6.2).
4. **Binary WAL framing** (replacing `serde_json::to_vec` at `crates/opendb-storage/src/wal.rs:184`) — both tracks want a compact, fast-to-decode format.
5. **A `Projection` trait** so the same WAL can materialize into multiple physical layouts (row-store for OLTP, columnar for OLAP) without changing the durability layer (OLAP §3, §7).

These are the things to build *first*, because each one unlocks two axes of progress.

---

## 3. Sequencing: OLTP first, then OLAP, then both

We do the transactional roadmap before the analytical roadmap, for three reasons:

1. **MVCC is the hardest single piece and must work**. Doing it in the OLTP context (small rows, frequent updates, fast feedback) surfaces correctness bugs before they hide inside a vectorized 1B-row scan.
2. **PostgreSQL is a sharper competitor on OLTP than DuckDB/ClickHouse are on OLAP.** Beating PG on `pgbench` is a cleaner proof point than beating ClickHouse on TPC-H, where the comparison is genuinely against a category-defining product.
3. **Sentropic (today's primary consumer of OpenDB) is INSERT-heavy autocommit.** That's an OLTP shape. The "show me all matches across N years" use case is analytical, but it's later than milestone 1. Aligning the order of work with the user's today-use-case is the safe call.

**Caveat for the user**: if sentropic's killer demo flips to "show me all matches across N years" before Milestone 1 ships, this sequencing is wrong and we should run both tracks in parallel from Phase B onward.

---

## 4. Phased roadmap (single integrated view)

Sequenced calendar order. Effort is one senior engineer including tests + benchmarks; cumulative is calendar weeks.

| # | Phase | Track | Effort | Cumul. | Headline unlock |
|---|-------|-------|--------|--------|------------------|
| **A** | pgbench baseline against OpenDB | OLTP | 1 wk | 1 wk | Make the cliff visible; surface parser/protocol gaps |
| **B** | Lock-narrowing on `Database` + binary WAL framing | shared (OLTP + OLAP) | 1-2 wk | 3 wk | Concurrent reads stop blocking each other; smaller/faster WAL frames |
| **C** | MVCC with per-row versions | shared | 3-6 wk | 9 wk | Readers stop blocking writers; the foundation under everything else |
| **D** | Secondary B-tree + hash indexes | OLTP | 2-3 wk | 12 wk | Non-PK queries stop full-scanning; HammerDB and sysbench OLTP become realistic |
| **E** | Dedicated WAL writer task + group commit | shared (OLTP commit throughput + OLAP bulk ingest) | 2-4 wk | 16 wk | Match PG write throughput on `pgbench -c 32`; enable `COPY FROM` at high rate |
| **F** | `Projection` trait + Phase 1 columnar materialization | OLAP | 5-7 wk | 21 wk | First credible 1M-row OLAP scan; tracer-bullet ClickBench demo |
| **G** | Vectorized chunk execution (sum/count/groupby on typed slices) | OLAP | 5-7 wk | 27 wk | Per-row dispatch overhead gone; OLAP scans approach memory bandwidth |
| **H** | Per-column compression (LZ4 / ZSTD / Delta / DoubleDelta / dict) | OLAP | 3-5 wk | 31 wk | 5-20× compression on real workloads, often *speeds up* scans |
| **I** | Morsel-driven parallel scan (all cores on one query) | OLAP | 4-6 wk | 36 wk | All-cores scaling for analytical workloads |
| **J** | Multi-leader writes / distributed shards | both | quarters | quarters | Scale-out, optional, not on the single-node critical path |

Conservative read: **~4 months to be PG-competitive on a single-box OLTP**, **~9 months from today to be ClickHouse-credible on single-node OLAP**, **same code, same WAL, two physical projections.**

The two companion docs go into per-phase detail.

---

## 5. The two acceptance demos

These are the two demos that prove we're done with the respective tracks.

### 5.1 OLTP acceptance demo
**`pgbench -i -s 10 ; pgbench -c 16 -j 4 -T 60 -M prepared --no-vacuum`**

OpenDB TPS ≥ PG 16 TPS on the same NVMe with `synchronous_commit=on`. Architectural minimum: Phases A + B + C + E (Phase D is required for HammerDB but not for pgbench).

Source: `docs/roadmap/perf-vs-transactional-2026-05-22.md` §5.

### 5.2 OLAP acceptance demo
**TPC-H Q1-shape over 1M `lineitem` rows** — single table, 4 aggregates, 2 grouping columns, 1 range filter.

OpenDB cold ≤ 250 ms, warm ≤ 120 ms — parity with ClickHouse on the same query. Architectural minimum: Phase F + Phase G (columnar + vectorized), single-threaded, no compression.

Source: `docs/roadmap/perf-vs-analytical-2026-05-22.md` §7.

Both demos are deliberately *small* — they're tracer bullets. Once each fires, every subsequent phase is an extension of the same infrastructure, not a new architecture.

---

## 6. What we are explicitly NOT doing in Milestone 1

To avoid scope creep on this roadmap:

- **No joins beyond hash-join-everything-fits-in-memory.** TPC-H beyond Q1/Q6 is out of scope; ClickHouse / Vertica beat each other on joins via different strategies and that's a separate ~quarter of work.
- **No SERIALIZABLE isolation.** PG default is READ COMMITTED; we offer that. Predicate locks (SSI) are post-MVP.
- **No window functions.** They're nice but not on the critical path of either acceptance demo.
- **No multi-leader / leaderless writes.** Phase J is optional and depends on the deployment story.
- **No connection pooler in-process.** We do not embed a PgBouncer-equivalent; if operators want it they front us with PgBouncer like they do PG.
- **No on-disk OLAP storage.** Phase F's columnar projection lives in memory, materialized from WAL. Phase F+ (years out) might add segment files; not now.

---

## 7. Risks and unknowns

Surfaced from both research docs, ranked by how much they would change the plan:

1. **MVCC effort (Phase C) could be 8-12 weeks instead of 3-6.** Wrapping the existing BTreeMap values with version chains is fast; replacing the row representation with packed heap tuples (`xmin`/`xmax` inline) is slow. The estimate assumes the wrapper approach; if it has hot-path costs we need the latter, the schedule slips ~6 weeks.
2. **Phase E group commit may be cheaper as a "single committer drains the queue under append_lock" prototype.** Less clean architecturally, ~80% of the win, ~30% of the work. Worth prototyping first before committing to the dedicated-task design.
3. **OLAP multipliers are not measured against OpenDB yet.** §5 of the analytical doc projects 100-1000× gaps based on published DuckDB/ClickHouse engineering posts. The first concrete measurement (a 1M-row scan on current OpenDB) might surface a bigger or smaller gap that changes priorities.
4. **OpenRaft fsync coordination.** If Milestone 1 ships 3-node by default, every commit fsyncs locally AND waits for raft majority — Phase E group commit then has to coordinate with raft batching (call it Phase E.5).
5. **Wire format constraints.** pgwire is row-oriented; returning OLAP results as columnar chunks like ClickHouse's Native protocol isn't possible. We pay row-materialization on the final result set only — negligible for OLAP shapes, but worth noting.

---

## 8. Immediate next action

Phase A this week: stand up `pgbench -i -s 1` against OpenDB and run `-c 1, 4, 16, 32 -T 60 -M simple` against both OpenDB and PG 16 on the same hardware. Output a single comparison doc in `docs/bench/pgbench-2026-MM-DD.md`.

Two outcomes both useful:
- If `pgbench -c 1` already runs end-to-end against opendb, we have a real measurement we can plot Phase B/C/E against.
- If it fails on parser or protocol gaps, those gaps are surfaced and prioritized immediately.

Until that number exists, every phase below is being prioritized blind.

---

## 9. How this connects to the sentropic north star

Sentropic today seeds 100 folders × 5 initiatives = 500 rows, then runs sub-ms reads. Post 2026-05-20 cache fix the seed is 1.99 s; post Track B (group-commit, shipping today) it should land near 0.9 s; post Phase E (real WAL writer + group commit) it should be sub-100 ms.

The OLTP track buys sentropic the write throughput it needs to seed in <1 s on real workloads. The OLAP track buys sentropic the analytical scans it will need when its "show me all matches across N years" query lands. The shared backbone (drop the Mutex, MVCC, binary WAL, Projection trait) is the difference between "we built a fast OLTP database and bolted on OLAP later" and "we built a database where both projections share the same durable truth from day one."

That last sentence is the product positioning the roadmap is structured to deliver.
