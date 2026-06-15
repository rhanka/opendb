# Phase B sequencing decision (2026-06-11)

5-way agent consensus on which of B.2 / B.3 / B.4 to ship FIRST to
unlock the biggest observable win at c=4-and-up.

**User ratified the B.4 → B.3 → B.2 order on 2026-06-11.**

## Decision: B.4 first — per-session transaction buffer

5/5 votes for B.4. Estimated effort: **2 weeks**. Top risk converged
across voters: **pgwire connection-task refactor leaks transaction
state or breaks BEGIN/COMMIT semantics under concurrent sessions**.

## Recommended order

1. **B.4** — per-session txn buffer (drop the per-Database `transaction` field). **2 weeks.**
2. **B.3** — `Arc<RwLock<Database>>` for the engine. **1 week.** Trivial once B.4 has moved txn state out.
3. **B.2** — parse + prepare outside the lock. **1 week.** Additive perf polish; the planning slice is sub-millisecond and only becomes visible once the write path stops being the binding constraint.

Total Phase B remaining: **~4 weeks**.

## Consensus rationale

1. **B.4 is the only step that unblocks E.2's already-built win.** The
   commit worker (Phase E.2) was instrumented and confirmed at 2026-06-04
   to drain 1 record per round at c=4 because the upstream pgwire
   Mutex serializes all sessions before they reach the worker. The
   queue stays empty → no cross-client coalescing → the +50 % TPS
   projection from E.2 is invisible. **B.4 is the unlock**.

2. **B.2 and B.3 optimize around a still-binding constraint.**
   - B.2 saves ~1 ms of planning per query. At c=4 with the Mutex
     serializing execute, the planning saving is invisible.
   - B.3 lets concurrent SELECTs run truly parallel, but the
     bench-concurrent.ts workload is INSERT-heavy and writers still
     queue. Writer-bound benchmark = read parallelism doesn't move
     the needle.

3. **B.4 unblocks B.3 anyway.** Once per-session txn state lives in
   the pgwire connection task, B.3's RwLock split becomes trivial
   (no shared mutable txn buffer to coordinate). Doing B.4 first
   means B.3 takes ~1 week instead of ~2 with the entangled refactor.

4. **B.4 makes B.2's gain additive instead of masked.** Planning
   savings only show up once writes don't serialize the whole session
   pool. B.4 first removes the masking layer.

5. **Concurrent BEGIN is the most user-visible cliff today.** Two
   pgbench clients with `-c 2` will outright fail today's opendb on
   concurrent BEGIN (the second's BEGIN gets "transaction is already
   open" because `Database::transaction` is per-Database). That's a
   correctness regression, not a perf one — and B.4 fixes it.

## Acceptance criteria for B.4

- `Database::transaction: Option<TransactionBuffer>` field is removed.
- Transaction buffer state lives in the pgwire connection task
  (`crates/opendb-node/src/pgwire.rs` per-connection state struct).
- `Database::execute` takes an optional `&mut TransactionBuffer`
  parameter (or the txn boundary moves entirely into pgwire-side
  helpers that call `Database` only for prepare/submit).
- `Database` becomes `Arc<Database>` at the pgwire boundary (no
  outer `Mutex` for non-txn paths; commit_worker handles its own
  internal serialization).
- Test acceptance: `bench-concurrent.ts` with `BENCH_CLIENTS=8` and
  each client running `BEGIN; INSERT; INSERT; COMMIT;` succeeds
  end-to-end with no "transaction already open" errors.
- Perf acceptance: at c=8, the commit worker's `try_recv` drain
  finally returns ≥ 2 records per round on average (verified via
  `OPENDB_PERF_TIMING=1`), and per-record fsync count drops
  proportionally.

## Out of scope for B.4

- True per-session MVCC isolation (that's Phase C).
- pgwire connection pooling (out-of-process). Stay 1:1 task:connection.
- Removing the global `Arc<Mutex<Database>>` itself (that's B.3 — but
  B.4 makes most of the Mutex's locking go away, so B.3 is mostly a
  cleanup pass).

## Provenance

5 independent voter outputs converging on B.4-first with 2 weeks
effort and identical top-risk phrasing (pgwire connection refactor
risk to BEGIN/COMMIT semantics). Voter transcripts in
`/tmp/claude-0/.../tasks/aa6278db7*, a05a65f57*, a9a24fb9c*, a19718edc*,
a8ff395a07*`. User ratification recorded in
`docs/roadmap/decisions-for-user-2026-06-11.md`.
