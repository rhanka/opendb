# Sentropic bench — opendb-node vs PostgreSQL 16 — 2026-05-17

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
| opendb-node | 71.7s |
| PostgreSQL 16 | 0.1s |

## OpenDB progression

| Run | Change | Seed | B1 mean | B2 mean | B3 mean | B4 mean | B5 mean |
|-----|--------|------|---------|---------|---------|---------|---------|
| 2026-05-16 | baseline Sprint 20 bench | 277.3s | 602.17ms | 329.45ms | 343.14ms | 327.60ms | 332.00ms |
| 2026-05-17 A | DoBlock skips per-row refresh inside multi-row INSERT | 56.9s | 284.98ms | 286.10ms | 281.86ms | 282.92ms | 278.88ms |
| 2026-05-17 B | unchanged WAL skips top-level read replay | 71.7s | 47.58ms | 47.62ms | 45.32ms | 44.85ms | 46.67ms |

## Latency matrix

| Query | Desc | opendb p50 / p95 / p99 / mean (ms) | PG p50 / p95 / p99 / mean (ms) | opendb÷PG mean |
|------|------|------------------------------------|---------------------------------|-----------------|
| B1 | count(*) FROM folders | 41.03 / 44.09 / 152.95 / 47.58 | 0.08 / 0.24 / 0.47 / 0.11 | 420.98× |
| B2 | count(*) FROM initiatives WHERE status = 'completed' | 41.85 / 43.93 / 134.00 / 47.62 | 0.15 / 0.27 / 0.43 / 0.17 | 276.27× |
| B3 | GROUP BY status sur initiatives | 41.69 / 43.06 / 127.08 / 45.32 | 0.20 / 0.28 / 0.34 / 0.21 | 213.80× |
| B4 | SELECT folder par PK (id = 'fld-42') | 41.02 / 42.00 / 127.17 / 44.85 | 0.10 / 0.19 / 0.21 / 0.12 | 382.90× |
| B5 | WHERE workspace_id (full scan, FOLDERS rows) | 41.15 / 42.99 / 155.28 / 46.67 | 0.20 / 0.53 / 0.75 / 0.24 | 194.62× |

## Reading the numbers

- `opendb÷PG mean < 1` means opendb is faster on that query; `> 1` means PG is faster.
- opendb-node runs in-memory (the bench data dir is wiped at the end); PG runs with its default storage on disk inside the container. Both are localhost over TCP. This is **not** a fair production comparison — both have the same wire overhead but very different durability stories. Treat the numbers as a *worst case* for opendb (cold cache, no query plan) and a *best case* for PG (warm cache, mature optimizer).
- The read replay cache drops OpenDB query means from about 280ms to 45-48ms on this dataset. B1 improved 12.7x versus the baseline and 6.0x versus the DoBlock-only run.
- The dominant remaining bottleneck is ingestion: opendb-node needed 71.7s to seed 100 folders and 500 initiatives, versus 0.1s for PostgreSQL 16. Seed timing varies run-to-run, but remains tens of seconds after the read-cache fix.
