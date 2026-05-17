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
| opendb-node | 56.9s |
| PostgreSQL 16 | 0.1s |

Previous run on 2026-05-16 measured opendb-node seed at 277.3s on the same dataset and batch settings. The DoBlock refresh fix reduces this seed phase by about 4.9x.

## Latency matrix

| Query | Desc | opendb p50 / p95 / p99 / mean (ms) | PG p50 / p95 / p99 / mean (ms) | opendb÷PG mean |
|------|------|------------------------------------|---------------------------------|-----------------|
| B1 | count(*) FROM folders | 264.42 / 337.12 / 353.18 / 284.98 | 0.10 / 0.42 / 0.59 / 0.15 | 1929.66× |
| B2 | count(*) FROM initiatives WHERE status = 'completed' | 266.92 / 342.06 / 349.01 / 286.10 | 0.12 / 0.57 / 0.62 / 0.20 | 1447.61× |
| B3 | GROUP BY status sur initiatives | 263.58 / 329.17 / 335.08 / 281.86 | 0.19 / 0.32 / 0.41 / 0.22 | 1269.38× |
| B4 | SELECT folder par PK (id = 'fld-42') | 264.06 / 335.96 / 357.35 / 282.92 | 0.11 / 0.33 / 0.40 / 0.16 | 1722.12× |
| B5 | WHERE workspace_id (full scan, FOLDERS rows) | 263.62 / 323.12 / 327.89 / 278.88 | 0.23 / 0.42 / 0.82 / 0.31 | 888.05× |

## Reading the numbers

- `opendb÷PG mean < 1` means opendb is faster on that query; `> 1` means PG is faster.
- opendb-node runs in-memory (the bench data dir is wiped at the end); PG runs with its default storage on disk inside the container. Both are localhost over TCP. This is **not** a fair production comparison — both have the same wire overhead but very different durability stories. Treat the numbers as a *worst case* for opendb (cold cache, no query plan) and a *best case* for PG (warm cache, mature optimizer).
- The dominant bottleneck in this run is ingestion: opendb-node needed 56.9s to seed 100 folders and 500 initiatives, versus 0.1s for PostgreSQL 16. Query latency is also orders of magnitude higher on all five probes.
- Remaining next target: each top-level read still refreshes/rebuilds from the WAL, so query latency stays near 280ms even after the ingestion fix.
