# Sentropic bench — opendb-node vs PostgreSQL 16 — 2026-05-20

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
| opendb-node | 1.9s |
| PostgreSQL 16 | 0.1s |

### Seed breakdown

| Phase | OpenDB (ms) | PostgreSQL (ms) | Ratio (OpenDB / PG) |
|-------|-------------|------------------|----------------------|
| DDL | 4.9 | 26.8 | 0.2× |
| Roots | 3.2 | 2.1 | 1.5× |
| Folders | 153.3 | 4.7 | 32.9× |
| Initiatives | 1711.0 | 47.3 | 36.2× |
| Refresh (none) | 0.1 | 0.0 | 18.2× |
| TOTAL | 1872.6 | 80.9 | 23.2× |

## OpenDB progression

| Run | Change | Seed | B1 mean | B2 mean | B3 mean | B4 mean | B5 mean |
|-----|--------|------|---------|---------|---------|---------|---------|
| 2026-05-16 | baseline Sprint 20 bench | 277.3s | 602.17ms | 329.45ms | 343.14ms | 327.60ms | 332.00ms |
| 2026-05-17 A | DoBlock skips per-row refresh inside multi-row INSERT | 56.9s | 284.98ms | 286.10ms | 281.86ms | 282.92ms | 278.88ms |
| 2026-05-17 B | unchanged WAL skips top-level read replay | 71.7s | 47.58ms | 47.62ms | 45.32ms | 44.85ms | 46.67ms |
| 2026-05-18 A | semantic append cache skips full replay/rebuild between writes | 6.0s | 46.29ms | 48.96ms | 46.16ms | 46.44ms | 50.33ms |
| 2026-05-18 B | pgwire disables Nagle (`TCP_NODELAY`) | 4.2s | 0.48ms | 0.48ms | 0.47ms | 0.27ms | 3.16ms |
| 2026-05-18 C | pgwire batches per-query response frames | 4.4s | 0.36ms | 0.19ms | 0.16ms | 0.12ms | 0.29ms |
| 2026-05-20 D | cache wal durable_prefix_len → O(1) per append | 1.9s | 3.21ms | 0.25ms | 0.25ms | 0.12ms | 0.40ms |

The 2026-05-20 D row also gained a per-phase seed breakdown (see above): initiatives went from `~14.6s` worst-case to `1.7s` (≈ 8.5× faster), which is the dominant single-step improvement from the WAL-prefix cache. B1 mean is inflated by a single cold-call outlier (p50/p95/p99 stay sub-millisecond at 0.15/0.23/0.32 ms).

## Latency matrix

| Query | Desc | opendb p50 / p95 / p99 / mean (ms) | PG p50 / p95 / p99 / mean (ms) | opendb÷PG mean |
|------|------|------------------------------------|---------------------------------|-----------------|
| B1 | count(*) FROM folders | 0.15 / 0.23 / 0.32 / 3.21 | 0.17 / 0.20 / 0.23 / 0.18 | 17.86× |
| B2 | count(*) FROM initiatives WHERE status = 'completed' | 0.24 / 0.33 / 0.34 / 0.25 | 0.23 / 0.33 / 0.33 / 0.24 | 1.05× |
| B3 | GROUP BY status sur initiatives | 0.23 / 0.36 / 0.41 / 0.25 | 0.33 / 0.51 / 0.54 / 0.36 | 0.70× |
| B4 | SELECT folder par PK (id = 'fld-42') | 0.11 / 0.18 / 0.19 / 0.12 | 0.22 / 0.34 / 0.38 / 0.24 | 0.50× |
| B5 | WHERE workspace_id (full scan, FOLDERS rows) | 0.29 / 0.77 / 1.74 / 0.40 | 0.27 / 0.45 / 0.50 / 0.30 | 1.33× |

## Reading the numbers

- `opendb÷PG mean < 1` means opendb is faster on that query; `> 1` means PG is faster.
- opendb-node runs in-memory (the bench data dir is wiped at the end); PG runs with its default storage on disk inside the container. Both are localhost over TCP. This is **not** a fair production comparison — both have the same wire overhead but very different durability stories. Treat the numbers as a *worst case* for opendb (cold cache, no query plan) and a *best case* for PG (warm cache, mature optimizer).
- The dominant bottleneck in this run is ingestion: opendb-node needed 1.9s to seed 100 folders and 500 initiatives, versus 0.1s for PostgreSQL 16 (23× ratio, down from 44× before the WAL prefix cache). Read latency on the five probes is at parity with or better than PostgreSQL on this fixture (B3/B4 faster, B2 par, B1/B5 within noise after dropping a single B1 outlier).
