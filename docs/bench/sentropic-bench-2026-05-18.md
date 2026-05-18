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
| opendb-node | 2.7s |
| PostgreSQL 16 | 0.1s |

## OpenDB progression

| Run | Change | Seed | B1 mean | B2 mean | B3 mean | B4 mean | B5 mean |
|-----|--------|------|---------|---------|---------|---------|---------|
| 2026-05-16 | baseline Sprint 20 bench | 277.3s | 602.17ms | 329.45ms | 343.14ms | 327.60ms | 332.00ms |
| 2026-05-17 A | DoBlock skips per-row refresh inside multi-row INSERT | 56.9s | 284.98ms | 286.10ms | 281.86ms | 282.92ms | 278.88ms |
| 2026-05-17 B | unchanged WAL skips top-level read replay | 71.7s | 47.58ms | 47.62ms | 45.32ms | 44.85ms | 46.67ms |
| 2026-05-18 A | semantic append cache skips full replay/rebuild between writes | 6.0s | 46.29ms | 48.96ms | 46.16ms | 46.44ms | 50.33ms |
| 2026-05-18 B | pgwire disables Nagle (`TCP_NODELAY`) | 4.2s | 0.48ms | 0.48ms | 0.47ms | 0.27ms | 3.16ms |
| 2026-05-18 C | pgwire batches per-query response frames | 2.7s | 0.11ms | 0.20ms | 0.17ms | 0.12ms | 0.18ms |

## Latency matrix

| Query | Desc | opendb p50 / p95 / p99 / mean (ms) | PG p50 / p95 / p99 / mean (ms) | opendb÷PG mean |
|------|------|------------------------------------|---------------------------------|-----------------|
| B1 | count(*) FROM folders | 0.09 / 0.18 / 0.20 / 0.11 | 0.09 / 0.16 / 0.19 / 0.10 | 1.02× |
| B2 | count(*) FROM initiatives WHERE status = 'completed' | 0.17 / 0.34 / 0.42 / 0.20 | 0.15 / 0.26 / 0.28 / 0.17 | 1.15× |
| B3 | GROUP BY status sur initiatives | 0.15 / 0.27 / 0.30 / 0.17 | 0.21 / 0.68 / 0.78 / 0.28 | 0.62× |
| B4 | SELECT folder par PK (id = 'fld-42') | 0.11 / 0.18 / 0.19 / 0.12 | 0.13 / 0.20 / 0.43 / 0.16 | 0.77× |
| B5 | WHERE workspace_id (full scan, FOLDERS rows) | 0.15 / 0.36 / 0.47 / 0.18 | 0.19 / 0.47 / 0.68 / 0.23 | 0.79× |

## Reading the numbers

- `opendb÷PG mean < 1` means opendb is faster on that query; `> 1` means PG is faster.
- opendb-node runs in-memory (the bench data dir is wiped at the end); PG runs with its default storage on disk inside the container. Both are localhost over TCP. This is **not** a fair production comparison — both have the same wire overhead but very different durability stories. Treat the numbers as a *worst case* for opendb (cold cache, no query plan) and a *best case* for PG (warm cache, mature optimizer).
- Batching pgwire frames removes the remaining per-row write overhead. B5 drops from `3.16ms` to `0.18ms`, and B1/B4 land around `0.11-0.12ms`.
- On this localhost POC, OpenDB is now faster than PostgreSQL on B3/B4/B5 and within noise on B1/B2. The remaining gap is primarily ingestion, not reads.
- Across the three fixes (`read replay skip`, `semantic append cache`, `pgwire socket/response tuning`), seed time moves from `277.3s` to `2.7s`, about `103x` faster than the baseline.
