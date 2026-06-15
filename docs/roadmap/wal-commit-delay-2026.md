# Phase E.4 — WAL `commit_delay` knob design (2026-06-15)

Direct design (no consensus run — this is a single tuning knob with a
narrow optimization curve, no architectural decision contested).

## What

A configurable micro-delay inserted at the start of the
`wal_writer::writer_loop` between receiving the first request and
calling `try_recv` to drain the queue. Lets more concurrent senders
queue up before the writer commits the fsync, increasing the effective
batch size at low arrival rates.

```rust
// crates/opendb-storage/src/wal_writer.rs
async fn writer_loop(wal: Wal, mut rx: mpsc::Receiver<WalAppendRequest>, commit_delay: Duration) {
    while let Some(first) = rx.recv().await {
        let mut batch: Vec<WalAppendRequest> = vec![first];
        if !commit_delay.is_zero() {
            // Phase E.4 — Nagle-for-fsync: brief sleep before drain so
            // siblings that arrived within commit_delay coalesce.
            tokio::time::sleep(commit_delay).await;
        }
        while let Ok(req) = rx.try_recv() {
            batch.push(req);
        }
        // ... existing processing ...
    }
}
```

## Why

The 2026-06-04 c=4 bench showed `try_recv` returned **1 record per round**
because the arrival rate (~80 ops/sec) was well below the writer's
service rate (~333 rounds/sec). The queue stayed empty; no cross-
client coalescing happened. After Phase B.4 lifts the pgwire Mutex,
the arrival rate at the writer queue will jump, but on bursty
workloads (sentropic-style: short bursts of inserts, then idle) the
queue can still be empty most of the time.

PostgreSQL has the exact same knob: `commit_delay` (in microseconds)
and `commit_siblings` (minimum concurrent committers to engage the
delay). The combination amortizes one fsync over N committers when N is
large enough to make the delay worthwhile.

## Default value: **50 µs**, configurable

Reasoning:
- fsync on commodity NVMe = 500-1500 µs. 50 µs delay is ~5% of an
  fsync; the worst-case wait penalty is negligible if a sibling
  doesn't arrive.
- Inter-arrival time at c=4 was ~12 ms (from the bench). At 50 µs we
  catch effectively zero extra siblings unless the client task is
  pipelining tightly.
- At c=16+ with B.4 unlocked, inter-arrival drops to ~750 µs. A 50 µs
  window catches roughly 1 sibling per round on average. At c=32+
  it catches 2-3.
- The "right" value scales with client concurrency. 50 µs is a
  conservative default that doesn't hurt low concurrency and helps
  high.
- PG's default is `commit_delay = 0` (off). They put the burden of
  enabling it on the operator because their typical workload has
  enough concurrent committers naturally. We default ON because the
  commit worker is the explicit serialization point.

Operator override via env var: `OPENDB_WAL_COMMIT_DELAY_MICROS`
(default 50). Set to 0 to disable.

## Acceptance criteria

- `OPENDB_WAL_COMMIT_DELAY_MICROS=0` reproduces the pre-E.4 behavior
  (no delay; existing tests stay green).
- `OPENDB_WAL_COMMIT_DELAY_MICROS=50` (the default) shows ≥ 1.5x
  improvement on batch size at c=16 with B.4 unlocked.
- `OPENDB_WAL_COMMIT_DELAY_MICROS=500` (5× the default) shows
  observable but diminishing returns at c=16 (mostly catches siblings
  the default would have caught) AND a measurable latency penalty
  at c=4 (single-record workload pays the 500 µs tax).
- Sweep doc: `docs/bench/wal-commit-delay-sweep-<DATE>.md` with
  `delay ∈ {0, 25, 50, 100, 250, 500, 1000} µs × c ∈ {4, 8, 16, 32}`.

## Out of scope

- Adaptive delay (auto-tune based on observed sibling arrival rate).
  Could be a follow-up; PG's `commit_delay` is also static.
- `commit_siblings` equivalent (only engage the delay above N pending).
  The simple version is fine for milestone 1; revisit if profiling
  shows the delay hurts low-concurrency workloads.
- Per-table commit_delay. Single global setting suffices.

## Effort

**3-4 days.** Tiny code change, the cost is mostly the bench sweep
matrix. Can be bundled into the same milestone as B.4 since they
are co-beneficiaries (B.4 makes commit_delay matter; commit_delay
doubles the win of B.4 at high concurrency).

## Track item

To be added under WP5 as a leaf under E (the existing "E.4 —
commit_delay knob" placeholder).
