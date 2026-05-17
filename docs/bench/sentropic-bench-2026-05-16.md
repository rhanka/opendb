# Sentropic bench — opendb-node vs PostgreSQL 16 — 2026-05-16

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
| opendb-node | 277.3s |
| PostgreSQL 16 | 2.9s |

## Latency matrix

| Query | Desc | opendb p50 / p95 / p99 / mean (ms) | PG p50 / p95 / p99 / mean (ms) | opendb÷PG mean |
|------|------|------------------------------------|---------------------------------|-----------------|
| B1 | count(*) FROM folders | 371.16 / 1601.06 / 2456.32 / 602.17 | 0.11 / 0.20 / 0.28 / 0.14 | 4355.76× |
| B2 | count(*) FROM initiatives WHERE status = 'completed' | 305.00 / 410.22 / 418.33 / 329.45 | 0.18 / 0.24 / 0.24 / 0.19 | 1754.56× |
| B3 | GROUP BY status sur initiatives | 310.01 / 437.07 / 464.01 / 343.14 | 0.41 / 0.54 / 0.55 / 0.38 | 897.05× |
| B4 | SELECT folder par PK (id = 'fld-42') | 310.16 / 401.05 / 408.00 / 327.60 | 0.14 / 0.22 / 0.32 / 0.14 | 2279.25× |
| B5 | WHERE workspace_id (full scan, FOLDERS rows) | 303.74 / 414.25 / 445.16 / 332.00 | 0.19 / 0.47 / 0.54 / 0.23 | 1429.30× |

## Reading the numbers

- `opendb÷PG mean < 1` means opendb is faster on that query; `> 1` means PG is faster.
- opendb-node runs in-memory (the bench data dir is wiped at the end); PG runs with its default storage on disk inside the container. Both are localhost over TCP. This is **not** a fair production comparison — both have the same wire overhead but very different durability stories. Treat the numbers as a *worst case* for opendb (cold cache, no query plan) and a *best case* for PG (warm cache, mature optimizer).
- The dominant bottleneck in this run is ingestion: opendb-node needed 277.3s to seed 100 folders and 500 initiatives, versus 2.9s for PostgreSQL 16. Query latency is also orders of magnitude higher on all five probes.
