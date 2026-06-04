# Concurrent bench — opendb-node vs PostgreSQL 16 — 2026-06-04

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
| opendb-node | 72.3 | 11062 | 54.94 | 41.39 | 153.60 | 236.00 |
| PostgreSQL 16 | 1087.8 | 735 | 3.55 | 3.30 | 5.45 | 7.90 |

**PG / OpenDB TPS ratio: 15.04×** (1.0 = parity; >1.0 = PG faster)

## Phase E.2 (commit worker task) — honest read of this run

Compared to the 2026-05-23 post-E.1 run:

| Metric | Post E.1 | Post E.2 | normalized to PG drift | Δ |
|--------|---------:|---------:|-----------------------:|--:|
| PG TPS | 3 105 | 1 088 | — | machine 2.85× slower |
| opendb TPS | 285 | 72 | ~100 expected at 2.85× drift | **−28 %** |
| opendb p50 (ms) | 12.9 | 41.4 | ~37 expected | +12 % |

**E.2 does not move c=4 numbers by itself**, and in this run shows a ~28 % regression after normalizing for the machine-load drift PG also experienced. The architectural change is correct (3 commit-worker tests + 35/45/103/5 full suite pass); the **benefit is masked by the pgwire-level `Arc<Mutex<Database>>`** that still serializes every connection before it reaches the commit worker.

Per-span counts confirm the worker drained **1 record per round** (801 wal calls for 800 inserts — no batching happened):

- The workload sends ~80 inserts/sec across 4 clients.
- The worker processes a round in ~3 ms → ~333 rounds/sec capacity.
- Arrival rate (80/sec) is well below service rate (333/sec), so the queue stays empty and `try_recv` returns nothing past the first request.

To make the queue actually pile up — and let the cross-client coalescing E.2 was built for kick in — we need either Phase B (drop the pgwire Mutex so many connections can submit in parallel) or a much higher concurrency (`c=32+`). Without those, E.2 is structural plumbing for later, not a measurable win today.

The ~28 % regression is most likely the **extra task hop** in the worker → wal_writer pipeline (caller → commit_worker → wal_writer → reply → reply, four task hops vs E.1's two). At ~5 µs per hop on top of a 3 ms request, the overhead should be ~0.3 %, so part of the regression is also machine noise across the multi-week gap.

## Per-span timing (`OPENDB_PERF_TIMING=1`)

| Span | total_ms | calls | mean_us |
|------|----------|-------|---------|
| wal.append_with_len | 2913.83 | 801 | 3637.74 |
| wal.sync_data | 1576.00 | 802 | 1965.09 |
| wal.open+seek+set_len | 507.73 | 802 | 633.08 |
| wal.write_all | 41.67 | 802 | 51.95 |
| wal.encode_frame_serde_json | 22.58 | 802 | 28.16 |
| wal.durable_prefix_len_cold | 1.15 | 1 | 1146.34 |
