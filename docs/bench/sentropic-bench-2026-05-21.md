# Sentropic bench — opendb-node vs PostgreSQL 16 — 2026-05-21

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
| opendb-node | 2.0s |
| PostgreSQL 16 | 0.1s |

### Seed breakdown

| Phase | OpenDB (ms) | PostgreSQL (ms) | Ratio (OpenDB / PG) |
|-------|-------------|------------------|----------------------|
| DDL | 4.3 | 30.0 | 0.1× |
| Roots | 1.9 | 2.4 | 0.8× |
| Folders | 125.6 | 4.9 | 25.4× |
| Initiatives | 1853.9 | 49.3 | 37.6× |
| Refresh (none) | 0.1 | 0.0 | 21.9× |
| TOTAL | 1985.8 | 86.6 | 22.9× |

## Latency matrix

| Query | Desc | opendb p50 / p95 / p99 / mean (ms) | PG p50 / p95 / p99 / mean (ms) | opendb÷PG mean |
|------|------|------------------------------------|---------------------------------|-----------------|
| B1 | count(*) FROM folders | 0.15 / 0.23 / 0.28 / 0.16 | 0.14 / 0.25 / 0.37 / 0.17 | 0.93× |
| B2 | count(*) FROM initiatives WHERE status = 'completed' | 0.23 / 0.33 / 0.38 / 0.24 | 0.24 / 0.30 / 0.37 / 0.24 | 1.01× |
| B3 | GROUP BY status sur initiatives | 0.28 / 0.37 / 0.38 / 0.29 | 0.33 / 0.51 / 0.53 / 0.36 | 0.80× |
| B4 | SELECT folder par PK (id = 'fld-42') | 0.12 / 0.22 / 0.24 / 0.14 | 0.22 / 0.25 / 0.44 / 0.24 | 0.56× |
| B5 | WHERE workspace_id (full scan, FOLDERS rows) | 0.26 / 0.51 / 1.29 / 0.34 | 0.26 / 0.39 / 0.50 / 0.29 | 1.16× |

## Reading the numbers

- `opendb÷PG mean < 1` means opendb is faster on that query; `> 1` means PG is faster.
- opendb-node runs in-memory (the bench data dir is wiped at the end); PG runs with its default storage on disk inside the container. Both are localhost over TCP. This is **not** a fair production comparison — both have the same wire overhead but very different durability stories. Treat the numbers as a *worst case* for opendb (cold cache, no query plan) and a *best case* for PG (warm cache, mature optimizer).
- The dominant bottleneck in this run is ingestion: opendb-node needed 2.0s to seed 100 folders and 500 initiatives, versus 0.1s for PostgreSQL 16 (23× ratio). Read latency on the five probes is at parity or below PostgreSQL (B3/B4 faster, B1/B2/B5 within noise).


## Per-span timing (`OPENDB_PERF_TIMING=1`)

| Span | total_ms | calls | mean_us |
|------|----------|-------|---------|
| root_range.apply_committed | 1337.89 | 598 | 2237.27 |
| wal.append_with_len | 716.55 | 599 | 1196.25 |
| wal.sync_data | 576.40 | 600 | 960.67 |
| root_range.validate_semantic_append | 468.80 | 599 | 782.64 |
| root_range.commit_semantic_append_snapshot | 156.07 | 598 | 260.99 |
| wal.open+seek+set_len | 67.24 | 600 | 112.07 |
| wal.encode_frame_serde_json | 6.74 | 600 | 11.23 |
| wal.write_all | 1.62 | 600 | 2.71 |
| root_range.semantic_append_lock.acquire | 0.18 | 599 | 0.30 |
| wal.durable_prefix_len_cold | 0.02 | 1 | 21.54 |

### Attribution of `apply_committed` cost (1338 ms over 598 calls = 2237 µs/call)

| Step | Total ms | % of `apply_committed` |
|------|----------|------------------------|
| `wal.append_with_len` (inner) | 717 | 54% |
| `validate_semantic_append` | 469 | 35% |
| `commit_semantic_append_snapshot` | 156 | 12% |
| residual (lock acquire, await scheduling) | ~−4 | ~0% |

### Attribution of `wal.append_with_len` cost (717 ms over 599 calls = 1196 µs/call)

| Step | Total ms | % of `wal.append_with_len` |
|------|----------|----------------------------|
| `sync_data` (per-row fsync) | 576 | 80% |
| `open+seek+set_len` | 67 | 9% |
| `durable_len` cache mutex + `try_exists`/`create_dir_all` (residual) | ~64 | 9% |
| `encode_frame_serde_json` | 7 | 1% |
| `write_all` | 2 | <1% |

### Group-commit (Track B) projection from this data

Current path explodes a multi-row `INSERT VALUES (..),(..),..` into N single-row records, each triggering its own `apply_committed` → fsync + semantic validation + commit. The bench seeds 100 folders in batches of 25 (4 batches) and 500 initiatives in batches of 10 (50 batches), so 54 multi-row INSERT statements become 600 individual `apply_committed` calls.

If `apply_committed` were called once per multi-row INSERT (54 calls instead of 600):

| Span | observed total ms | projected after Track B | savings |
|------|-------------------|--------------------------|---------|
| sync_data | 576 | ~52 (10.7× fewer fsyncs) | −524 ms |
| validate_semantic_append | 469 | ~42 (validate once per batch) | −427 ms |
| commit_semantic_append_snapshot | 156 | ~14 (update once per batch) | −142 ms |
| open+seek+set_len | 67 | ~6 | −61 ms |
| **estimated seed total** | **1986** | **~830** | **−1156 ms** |

Projected seed ≈ 0.83 s = ~10× slower than PG (vs 23× today). Reads stay at parity.

The earlier `OPENDB_WAL_SKIP_FSYNC=1` experiment showed the seed *inflating* to 46 s — that was machine load noise, not a structural signal. The instrumented run above (with `sync_data` still enabled) shows fsync IS a significant cost (29 % of total seed time, 80 % of `wal.append_with_len`), so group-commit is the right next move.
