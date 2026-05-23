# Concurrent bench — opendb-node vs PostgreSQL 16 — 2026-05-23

Each client opens its own pg.Client, runs **200 autocommit (INSERT + SELECT) pairs** on a shared `bench_kv (id BIGINT PRIMARY KEY, payload TEXT)` table seeded with 100 rows. Clients use disjoint INSERT key ranges so they do not collide on the PK.

## Run parameters
| Parameter | Value |
|-----------|-------|
| clients | 4 |
| iterations per client | 200 |
| seed rows | 100 |

## Aggregate
| Engine | TPS | wall ms | mean ms | p50 | p95 | p99 |
|--------|-----|---------|---------|-----|-----|-----|
| opendb-node | 55.6 | 14400 | 71.75 | 43.77 | 244.56 | 539.47 |
| PostgreSQL 16 | 173.1 | 4621 | 22.79 | 3.03 | 84.61 | 161.52 |

**PG / OpenDB TPS ratio: 3.12×** (1.0 = parity; >1.0 = PG faster)
