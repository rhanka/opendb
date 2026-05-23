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
| opendb-node | 206.4 | 3877 | 19.33 | 17.76 | 25.20 | 85.56 |
| PostgreSQL 16 | 1743.6 | 459 | 2.17 | 1.73 | 4.31 | 13.00 |

**PG / OpenDB TPS ratio: 8.45×** (1.0 = parity; >1.0 = PG faster)

> The earlier 3.12× ratio (TPS 55.6 / 173.1) on this same fixture came from a machine-loaded run that throttled both engines proportionally. The clean-machine ratio above is the real OLTP gap to close.

## Where the time goes (clean machine, c=4)

Per insert, mean per call from `OPENDB_PERF_TIMING=1`:

| Stage | µs/call | % of `apply_committed` |
|-------|---------|------------------------|
| `wal.append_with_len` outer | 1477 | 49.5 % |
| ↳ `wal.sync_data` (fsync) | 1149 | 38.5 % |
| ↳ `wal.open+seek+set_len` | 166 | 5.6 % |
| ↳ `wal.encode_frame_serde_json` | 15 | 0.5 % |
| ↳ `wal.write_all` | 5 | 0.2 % |
| `validate_semantic_append` | 1112 | 37.3 % |
| `commit_semantic_append_snapshot` | 395 | 13.3 % |
| `semantic_append_lock.acquire` | 0.6 | <0.1 % |

**Reading the breakdown**
- Per-insert wall cost at c=4: `apply_committed = 2982 µs ≈ 3 ms`. With four clients pipelining INSERT+SELECT pairs, the global `Database` Mutex serializes these 3 ms inserts into a queue, giving the observed `p50 = 18 ms` (each client waits ~3 turns for its INSERT slot, then does the cheap SELECT).
- **fsync dominates**: 78 % of `wal.append_with_len` is `sync_data`. With one fsync per record across N concurrent clients, writes are serialized at the WAL `append_lock` regardless of what the outer Database Mutex does. Lock-narrowing (Phase B.2/B.3) helps reads pipeline behind writers but won't close the write-throughput gap.
- The **real lever** for c=4 write throughput is Phase E (dedicated WAL writer + group commit across clients) — coalesce multiple committers into one fsync. That's where the 8.45× ratio will come down.
- Phase B (lock-narrowing) is still worth doing for **read concurrency**: an idle SELECT today blocks behind a 3 ms INSERT on the Database Mutex. With per-engine RwLock, the SELECT runs in parallel with the WAL submit.

## Per-span timing (`OPENDB_PERF_TIMING=1`)

| Span | total_ms | calls | mean_us |
|------|----------|-------|---------|
| root_range.apply_committed | 2385.71 | 800 | 2982.14 |
| wal.append_with_len | 1182.99 | 801 | 1476.89 |
| wal.sync_data | 921.76 | 802 | 1149.33 |
| root_range.validate_semantic_append | 890.60 | 801 | 1111.86 |
| root_range.commit_semantic_append_snapshot | 316.00 | 800 | 395.00 |
| wal.open+seek+set_len | 133.24 | 802 | 166.14 |
| wal.encode_frame_serde_json | 12.07 | 802 | 15.06 |
| wal.write_all | 4.03 | 802 | 5.03 |
| root_range.semantic_append_lock.acquire | 0.51 | 801 | 0.63 |
| wal.durable_prefix_len_cold | 0.06 | 1 | 55.19 |
