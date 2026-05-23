# Concurrent bench — opendb-node vs PostgreSQL 16 — 2026-05-23

Each client opens its own pg.Client, runs **200 autocommit (INSERT + SELECT) pairs** on a shared `bench_kv (id BIGINT PRIMARY KEY, payload TEXT)` table seeded with 100 rows. Clients use disjoint INSERT key ranges so they do not collide on the PK.

## Run parameters
| Parameter | Value |
|-----------|-------|
| clients | 4 |
| iterations per client | 200 |
| seed rows | 100 |

## Aggregate (post Phase E.1 — single-writer WAL task)
| Engine | TPS | wall ms | mean ms | p50 | p95 | p99 |
|--------|-----|---------|---------|-----|-----|-----|
| opendb-node | 285.3 | 2804 | 14.00 | 12.94 | 19.04 | 26.54 |
| PostgreSQL 16 | 3105.0 | 258 | 1.20 | 1.11 | 1.81 | 2.12 |

**PG / OpenDB TPS ratio: 10.88×** (1.0 = parity; >1.0 = PG faster)

## Phase E.1 delta vs the pre-WalWriter run earlier today

| Metric | Pre E.1 | Post E.1 | Δ |
|--------|--------:|---------:|--:|
| opendb TPS | 206.4 | 285.3 | **+38.2 %** |
| opendb p50 (ms) | 17.76 | 12.94 | **−27.1 %** |
| opendb mean (ms) | 19.33 | 14.00 | **−27.6 %** |
| apply_committed (µs/call) | 2 982 | 2 205 | **−26.1 %** |
| wal.sync_data (µs/call) | 1 149 | 961 | −16.4 % |
| wal.append_with_len (µs/call) | 1 477 | 1 160 | −21.5 % |
| wal.open+seek+set_len (µs/call) | 166 | 93 | **−44.0 %** |
| validate_semantic_append (µs/call) | 1 112 | 765 | −31.2 % |

The PG-side numbers also fluctuated (1 743 → 3 105 TPS) so the cross-engine ratio drifted from 8.45 → 10.88×; treat the absolute ratio with caution. **The opendb-side delta is the structural signal.**

### Why E.1 helped even with `semantic_append_lock` still serializing

The writer task does not yet drain the queue under contention — `try_recv` returned 0–1 records per round because the `RootRange::semantic_append_lock` still serializes the validate + append + commit triple, so by the time client B sends to the writer, client A has already been replied to. The +38 % is **not** group-commit. It comes from three smaller effects:

1. **Warm file handle.** The writer task keeps the WAL open across appends, so the kernel inode cache + buffered-mode fd stays hot. `open+seek+set_len` mean dropped 166 → 93 µs (−44 %).
2. **Sequential I/O pattern.** A single task writing back-to-back lets the kernel's writeback scheduler coalesce sectors better than N tokio tasks racing the same `Wal::append_lock`. fsync mean dropped 1 149 → 961 µs (−16 %).
3. **Less lock thrash.** The `Wal::append_lock` (per-path async Mutex) is now uncontended on the hot path — only the writer task ever takes it. The previous N-client run paid context-switch cost every time a client got the lock; that cost is gone.

The fsync count is still **1 per record** (writer drains 0–1 per round). The real cross-client coalescing win is gated on Phase E.2: move the `semantic_append_*` triple inside the writer task so multiple clients can queue real work while the writer is mid-fsync. Projection from data: if the writer drained 4 records per round on average at c=4, we'd cut another ~3× on the WAL portion and the gap-to-PG would drop from 10.88× to ~4× before any further optimization.

## Per-span timing (`OPENDB_PERF_TIMING=1`)

| Span | total_ms | calls | mean_us |
|------|----------|-------|---------|
| root_range.apply_committed | 1764.37 | 800 | 2205.46 |
| wal.append_with_len | 929.35 | 801 | 1160.23 |
| wal.sync_data | 770.97 | 802 | 961.30 |
| root_range.validate_semantic_append | 612.51 | 801 | 764.69 |
| root_range.commit_semantic_append_snapshot | 216.24 | 800 | 270.30 |
| wal.open+seek+set_len | 74.65 | 802 | 93.08 |
| wal.encode_frame_serde_json | 8.16 | 802 | 10.17 |
| wal.write_all | 1.97 | 802 | 2.45 |
| root_range.semantic_append_lock.acquire | 0.33 | 801 | 0.41 |
| wal.durable_prefix_len_cold | 0.02 | 1 | 22.82 |
