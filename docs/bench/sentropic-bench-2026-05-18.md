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
| opendb-node | 4.2s |
| PostgreSQL 16 | 0.1s |

## OpenDB progression

| Run | Change | Seed | B1 mean | B2 mean | B3 mean | B4 mean | B5 mean |
|-----|--------|------|---------|---------|---------|---------|---------|
| 2026-05-16 | baseline Sprint 20 bench | 277.3s | 602.17ms | 329.45ms | 343.14ms | 327.60ms | 332.00ms |
| 2026-05-17 A | DoBlock skips per-row refresh inside multi-row INSERT | 56.9s | 284.98ms | 286.10ms | 281.86ms | 282.92ms | 278.88ms |
| 2026-05-17 B | unchanged WAL skips top-level read replay | 71.7s | 47.58ms | 47.62ms | 45.32ms | 44.85ms | 46.67ms |
| 2026-05-18 A | semantic append cache skips full replay/rebuild between writes | 6.0s | 46.29ms | 48.96ms | 46.16ms | 46.44ms | 50.33ms |
| 2026-05-18 B | pgwire disables Nagle (`TCP_NODELAY`) | 4.2s | 0.48ms | 0.48ms | 0.47ms | 0.27ms | 3.16ms |

## Latency matrix

| Query | Desc | opendb p50 / p95 / p99 / mean (ms) | PG p50 / p95 / p99 / mean (ms) | opendb÷PG mean |
|------|------|------------------------------------|---------------------------------|-----------------|
| B1 | count(*) FROM folders | 0.36 / 0.81 / 1.60 / 0.48 | 0.30 / 0.58 / 0.96 / 0.38 | 1.28× |
| B2 | count(*) FROM initiatives WHERE status = 'completed' | 0.45 / 0.61 / 0.73 / 0.48 | 0.38 / 0.51 / 0.54 / 0.38 | 1.26× |
| B3 | GROUP BY status sur initiatives | 0.43 / 0.67 / 0.88 / 0.47 | 0.51 / 0.65 / 0.75 / 0.50 | 0.93× |
| B4 | SELECT folder par PK (id = 'fld-42') | 0.25 / 0.35 / 0.36 / 0.27 | 0.34 / 0.49 / 0.61 / 0.37 | 0.75× |
| B5 | WHERE workspace_id (full scan, FOLDERS rows) | 3.01 / 4.78 / 5.16 / 3.16 | 0.65 / 1.50 / 2.69 / 0.82 | 3.85× |

## Reading the numbers

- `opendb÷PG mean < 1` means opendb is faster on that query; `> 1` means PG is faster.
- opendb-node runs in-memory (the bench data dir is wiped at the end); PG runs with its default storage on disk inside the container. Both are localhost over TCP. This is **not** a fair production comparison — both have the same wire overhead but very different durability stories. Treat the numbers as a *worst case* for opendb (cold cache, no query plan) and a *best case* for PG (warm cache, mature optimizer).
- `TCP_NODELAY` removes the 40ms delayed-ACK plateau from pgwire. B1-B4 now run in 0.27-0.48ms mean on this fixture; B3 and B4 are faster than PostgreSQL in this local POC.
- B5 remains slower than PostgreSQL because it returns/scans 100 folder rows over pgwire; this is now the main read-side target.
- The semantic append cache plus pgwire fix drops OpenDB seed time from 71.7s to 4.2s on this dataset (about 17x faster than the previous committed bench, 66x faster than the baseline). PostgreSQL still seeds the fixture much faster at 0.1s.
