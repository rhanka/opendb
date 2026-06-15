# OpenDB — decisions log (2026-06-11)

Consolidated record of architectural decisions taken during the
2026-06-11 planning sprint. Each decision was reached by 5-way
independent agent consensus (5/5 unanimous on every one) and ratified
by the user. Implementation work follows.

## 1. Phase C MVCC — Strategy A (BTreeMap wrapper) ✅ ratified

- Vote: 5/5 voters chose Strategy A independently.
- Effort: 3 weeks, with vacuum/HOT-pruning **non-negotiable** in the
  same milestone.
- Full design: `docs/roadmap/mvcc-strategy-2026.md`.

## 2. Phase E.3 WAL framing — Option X (postcard via WalCodec trait) ✅ ratified

- Vote: 5/5 voters chose Option X independently.
- Effort: 1 week. Codec wrapped behind a `WalCodec` trait for future
  swappability.
- Full design: `docs/roadmap/wal-framing-2026.md`.

## 3. Phase F.1 Projection trait — Option P (static trait, per-table opt-in) ✅ ratified

- Vote: 5/5 voters chose Option P independently.
- Effort: 4-6 weeks (median 6).
- Use `enum ProjectionRef<'a>` for static dispatch (no vtable
  overhead).
- Per-table opt-in via `CREATE TABLE t (...) WITH (engine = 'columnar')`.
- Full design: `docs/roadmap/projection-trait-2026.md`.

## 4. Phase B sequencing — B.4 first ✅ ratified

- Vote: 5/5 voters chose B.4 first independently.
- Order: B.4 (per-session txn buffer, 2 wk) → B.3 (RwLock split, 1 wk) →
  B.2 (parse outside lock, 1 wk). Total ~4 weeks.
- Rationale: B.4 is the only step that unblocks the already-built
  Phase E.2 cross-client coalescing — currently invisible at c=4
  because of the upstream pgwire Mutex.
- Full design: `docs/roadmap/phase-b-sequencing-2026.md`.

## 5. Phase A cliffs — Option α (ship COPY, defer INSERT-FROM-SELECT) ✅ consensus

- Vote: 5/5 voters (using Claude Opus) chose Option α independently.
- Effort: 2 weeks (5-8 days for COPY proper, plus edge-case buffer).
- Defers `INSERT-FROM-SELECT + generate_series` until a real workload
  demands it.
- Full design: `docs/roadmap/phase-a-cliffs-2026.md`.
- User-ratification still pending (see open items below).

## Open operational items (user input still needed)

These were excluded from consensus because they are operational /
product choices, not architectural ones.

### A. Workspace recovery model

Previous workspace at `/home/antoinefa/src/opendb/.worktrees/feat-milestone-1`
has disappeared TWICE this session. This commit is being pushed from a
fresh `git clone` at `/tmp/opendb-fresh`. Pick a durable convention:

- (a) **Continue cloning on demand into `/tmp/opendb-fresh`.** Simple,
  ephemeral. Risk: lose uncommitted state on every env shift.
- (b) **Mount a persistent volume** at a known path (e.g.
  `/workspace/opendb`). Survives env shifts. Requires infra setup.
- (c) **Status quo** — accept periodic re-clones.

Recommended: **(b)** — eliminates the recurring "where is my code?"
class of problem this session hit three times.

### B. h2a stack reconnect

The `mcp__h2a__*` server returned `ENOENT` for most of the session and
reconnected briefly. We didn't use the conductor / blockage / NHI tools
because the `Agent` tool + simple 5-way prompt convergence worked.

- (a) **Keep h2a connected** — for inter-agent workspace coordination
  in future multi-instance sessions.
- (b) **Drop h2a from this project** — track + Agent + AskUserQuestion
  is sufficient.

Recommended: **(a)** if multi-instance work is planned, **(b)** otherwise.

### C. Cron / loop re-arming

The 5-minute autonomous status loop is **off**. Re-arm with
`/loop 5min status+poursuite si pas de blocage` if you want autonomous
progress between check-ins.

### D. Acceptance bar — explicit acknowledgement

`docs/roadmap/perf-vision-2026.md` already documents two acceptance
demos:
- OLTP: `pgbench -c 16 -j 4 -T 60 -M prepared` at scale 10, opendb TPS
  ≥ PG 16.
- OLAP: TPC-H Q1 over 1 M lineitem rows, opendb cold ≤ 250 ms /
  warm ≤ 120 ms.

These are inherited from the prior sprint. Please confirm they remain
the bar, or propose alternatives.

## Track snapshot

Track CLI was available earlier in the session, used to migrate the
plan to `.track/events.jsonl` with 45 items (18 DONE, 3 in-progress
with decisions specified, 24 TO-DO). The encrypted home that hosted
`.track/` disappeared mid-session — the state is lost. The plan
itself lives entirely in `docs/roadmap/*.md` now, version-controlled
via this commit. If you want the live track view back, run
`track init` in a stable workspace and we re-import.

## Summary

- 5 architectural decisions ratified by 5/5 consensus (4 user-confirmed,
  1 pending user confirmation).
- 5 design docs in `docs/roadmap/`.
- ~4 weeks of implementation queued (Phase B at 4 wk including unlock
  of E.2 coalescing).
- Phase C MVCC, Phase E.3 WAL framing, Phase F.1 Projection trait, and
  Phase A.11 COPY all have specs ready.
- 4 operational items above need your input when convenient.
