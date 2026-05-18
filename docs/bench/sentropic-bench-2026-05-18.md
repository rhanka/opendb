# Sentropic bench — opendb-node vs PostgreSQL 16 — 2026-05-18

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
| opendb-node | 6.0s |
| PostgreSQL 16 | 0.1s |

## OpenDB progression

| Run | Change | Seed | B1 mean | B2 mean | B3 mean | B4 mean | B5 mean |
|-----|--------|------|---------|---------|---------|---------|---------|
| 2026-05-16 | baseline Sprint 20 bench | 277.3s | 602.17ms | 329.45ms | 343.14ms | 327.60ms | 332.00ms |
| 2026-05-17 A | DoBlock skips per-row refresh inside multi-row INSERT | 56.9s | 284.98ms | 286.10ms | 281.86ms | 282.92ms | 278.88ms |
| 2026-05-17 B | unchanged WAL skips top-level read replay | 71.7s | 47.58ms | 47.62ms | 45.32ms | 44.85ms | 46.67ms |
| 2026-05-18 | semantic append cache skips full replay/rebuild between writes | 6.0s | 46.29ms | 48.96ms | 46.16ms | 46.44ms | 50.33ms |

## Latency matrix

| Query | Desc | opendb p50 / p95 / p99 / mean (ms) | PG p50 / p95 / p99 / mean (ms) | opendb÷PG mean |
|------|------|------------------------------------|---------------------------------|-----------------|
| B1 | count(*) FROM folders | 41.06 / 42.15 / 149.28 / 46.29 | 0.13 / 0.24 / 0.32 / 0.15 | 316.36× |
| B2 | count(*) FROM initiatives WHERE status = 'completed' | 41.10 / 43.05 / 172.07 / 48.96 | 0.16 / 0.40 / 0.47 / 0.20 | 244.47× |
| B3 | GROUP BY status sur initiatives | 41.15 / 42.99 / 152.08 / 46.16 | 0.23 / 0.45 / 0.48 / 0.27 | 173.66× |
| B4 | SELECT folder par PK (id = 'fld-42') | 41.07 / 42.65 / 159.71 / 46.44 | 0.22 / 0.36 / 0.52 / 0.26 | 179.64× |
| B5 | WHERE workspace_id (full scan, FOLDERS rows) | 41.08 / 42.25 / 205.75 / 50.33 | 0.21 / 0.38 / 0.64 / 0.25 | 204.84× |

## Reading the numbers

- `opendb÷PG mean < 1` means opendb is faster on that query; `> 1` means PG is faster.
- opendb-node runs in-memory (the bench data dir is wiped at the end); PG runs with its default storage on disk inside the container. Both are localhost over TCP. This is **not** a fair production comparison — both have the same wire overhead but very different durability stories. Treat the numbers as a *worst case* for opendb (cold cache, no query plan) and a *best case* for PG (warm cache, mature optimizer).
- The semantic append cache drops OpenDB seed time from 71.7s to 6.0s on this dataset (about 12x faster than the previous run, 46x faster than the baseline).
- Query latency is essentially unchanged from the read-cache run: OpenDB still spends about 46-50ms per probe, so the next bottleneck is the read/query path rather than ingestion.
- PostgreSQL still seeds this tiny fixture much faster (0.1s), so OpenDB ingestion remains about 60x slower on this POC despite the large internal improvement.
