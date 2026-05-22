# Sentropic bench — opendb-node vs PostgreSQL 16 — 2026-05-22

Same schema (workspaces / organizations / folders / initiatives), same seed (100 folders × 5 initiatives = 500 initiatives), same node-postgres client. **50 repetitions per query, 5-rep warm-up dropped.** Latency in milliseconds.

## Run parameters

| Parameter | Value |
|-----------|-------|
| folders | 100 |
| initiatives per folder | 5 |
| total initiatives | 500 |
| repetitions per query | 50 |
| folder insert batch size | 25 |
| initiative insert batch size | 10 |

## Seed timing

| Engine | Duration |
|--------|----------|
| opendb-node | 4.6s |
| PostgreSQL 16 | 0.6s |

### Seed breakdown

| Phase | OpenDB (ms) | PostgreSQL (ms) | Ratio (OpenDB / PG) |
|-------|-------------|------------------|----------------------|
| DDL | 26.6 | 115.4 | 0.2× |
| Roots | 8.9 | 12.6 | 0.7× |
| Folders | 223.9 | 38.6 | 5.8× |
| Initiatives | 4375.6 | 399.3 | 11.0× |
| Refresh (none) | 0.2 | 0.0 | 10.1× |
| TOTAL | 4635.2 | 566.0 | 8.2× |

## Latency matrix

| Query | Desc | opendb p50 / p95 / p99 / mean (ms) | PG p50 / p95 / p99 / mean (ms) | opendb÷PG mean |
|------|------|------------------------------------|---------------------------------|-----------------|
| B1 | count(*) FROM folders | 0.86 / 1.24 / 2.62 / 18.01 | 1.06 / 1.44 / 1.56 / 1.18 | 15.32× |
| B2 | count(*) FROM initiatives WHERE status = 'completed' | 1.15 / 1.50 / 2.35 / 1.23 | 1.47 / 1.84 / 1.99 / 1.51 | 0.82× |
| B3 | GROUP BY status sur initiatives | 1.35 / 1.87 / 2.28 / 1.39 | 1.69 / 2.11 / 2.17 / 1.69 | 0.82× |
| B4 | SELECT folder par PK (id = 'fld-42') | 1.03 / 1.41 / 3.47 / 1.12 | 1.19 / 1.72 / 2.85 / 1.28 | 0.88× |
| B5 | WHERE workspace_id (full scan, FOLDERS rows) | 1.34 / 4.00 / 6.25 / 12.96 | 1.29 / 3.63 / 5.26 / 1.58 | 8.20× |

## Reading the numbers

- `opendb÷PG mean < 1` means opendb is faster on that query; `> 1` means PG is faster.
- opendb-node runs in-memory (the bench data dir is wiped at the end); PG runs with its default storage on disk inside the container. Both are localhost over TCP. This is **not** a fair production comparison — both have the same wire overhead but very different durability stories. Treat the numbers as a *worst case* for opendb (cold cache, no query plan) and a *best case* for PG (warm cache, mature optimizer).
- The dominant bottleneck in this run is ingestion: opendb-node needed 4.6s to seed 100 folders and 500 initiatives, versus 0.6s for PostgreSQL 16 (8.2× ratio, down from 23× on the 2026-05-21 single-row run). **Caveat: this run was machine-noisy** — PG itself ran 6× slower than its 2026-05-21 baseline (566 ms vs 87 ms), so absolute numbers are not directly comparable across days. The structural signal is in the per-span counts below: `wal.sync_data` went from **600 calls to 60** (10× reduction in fsyncs), `wal.open+seek+set_len` similarly 600→61, confirming the multi-row INSERT batch path is engaged. On a clean machine, projected seed lands ~1.2 s vs ~87 ms PG ≈ 14× ratio.


## Per-span timing (`OPENDB_PERF_TIMING=1`)

| Span | total_ms | calls | mean_us |
|------|----------|-------|---------|
| root_range.apply_committed | 1158.38 | 59 | 19633.47 |
| root_range.validate_semantic_append | 634.59 | 60 | 10576.47 |
| wal.append_with_len | 408.07 | 60 | 6801.10 |
| wal.sync_data | 267.20 | 61 | 4380.36 |
| root_range.commit_semantic_append_snapshot | 52.91 | 59 | 896.83 |
| wal.open+seek+set_len | 51.91 | 61 | 851.02 |
| wal.encode_frame_serde_json | 13.02 | 61 | 213.40 |
| wal.write_all | 8.17 | 61 | 133.94 |
| root_range.semantic_append_lock.acquire | 0.14 | 60 | 2.42 |
| wal.durable_prefix_len_cold | 0.04 | 1 | 42.35 |

### Track B (group-commit) signal vs noise

Comparing per-span call counts on this run vs the 2026-05-21 single-row run:

| Span | 2026-05-21 calls | 2026-05-22 calls | reduction |
|------|------------------|------------------|-----------|
| `root_range.apply_committed` (now batch) | 598 | 59 | 10.1× |
| `wal.append_with_len` | 599 | 60 | 10.0× |
| `wal.sync_data` | 600 | 60 | 10.0× |
| `root_range.commit_semantic_append_snapshot` | 598 | 59 | 10.1× |
| `wal.open+seek+set_len` | 600 | 61 | 9.8× |
| `root_range.validate_semantic_append` | 599 | 60 (10 records/call) | per-call ↑, per-record ≈ same |

The structural win is in syscalls (fsync, file open, fs::metadata-equivalent) and lock-acquire counts, **not** in per-record validation cost — `validate_semantic_append` still does O(N) work inside each batch because each record's mutation must be applied to the working snapshot. Estimated clean-machine seed savings: ~30-40 % vs single-row mode (not the 54 % projected pre-implementation, which incorrectly assumed validate batched too).

### Earlier-day progression

| Date | Optim | Seed (ms) | Notes |
|------|-------|-----------|-------|
| 2026-05-16 | baseline | 277 300 | Sprint 20 entry point |
| 2026-05-18 | WAL semantic cache + pgwire batch + TCP_NODELAY | 4 400 | reads sub-ms |
| 2026-05-20 | WAL durable_prefix_len O(N²) → O(1) cache | 1 870 | reads stay sub-ms |
| 2026-05-21 | timing instrumentation (no perf delta) | 1 990 | for attribution |
| 2026-05-22 | **group-commit DoBlock (Track B)** | 4 635 (noisy) | per-span counts confirm 10× syscall reduction; rerun on quiet machine pending |
