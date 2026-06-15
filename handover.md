# Handover — OpenDB / 2026-06-15 (for codex)

Reads the current state of the OpenDB project so a fresh agent (codex)
can pick up implementation work without re-discovering decisions.

## Repository state

- Remote: `https://github.com/rhanka/opendb.git`
- Default branch: `main`
- Tip of `main`: `912a841` (this commit). Push log of the last week:
  ```text
  912a841 security: bump tsx to 4.22.4 to pull esbuild >= 0.28.1
  2d78215 security: bump hono to 4.12.25 for 4 medium-severity fixes
  4e37cab docs: 5-way agent consensus design decisions for Phases A/B/C/E.3/F.1
  c258467 security: bump vitest to 4.1.8 for arbitrary file read/exec fix
  2668bb6 docs: c=4 bench post Phase E.2 with honest read of the regression
  1b01149 refactor: Phase E.2 — CommitWorker task with cross-client coalescing
  cbb82c5 docs+bench: clean-machine c=4 baseline + per-span attribution
  c4ca113 feat: Phase B prep — multi-client bench harness + baseline
  59e3485 perf: Phase E.1 — single-writer WAL task funnels all appends through one fd
  ```
- Open Dependabot alerts at commit time: 0 (the most recent push closed
  esbuild #8/#9; re-scan latency may show 2 still open briefly).

## Workspace volatility — read this first

The local workspace has been **wiped twice this session**. Both the
`/home/antoinefa/src/opendb/.worktrees/feat-milestone-1` worktree and a
later `/home/antoinefa/src/opendb` shrunken view disappeared without
warning. Treat any local state as ephemeral. Always re-clone:

```bash
cd /tmp && git clone https://github.com/rhanka/opendb.git opendb-fresh
cd opendb-fresh
git config user.email "<your email>"
git config user.name "<your handle>"
gh auth setup-git   # if pushing back
```

The `.track/` plan state from earlier in the session is **lost**. The
plan is now fully recoverable from `docs/roadmap/*.md` only.

Build setup once cloned: `npm install --include=dev` (the cluster sets
`NODE_ENV=production` by default which strips devDeps — explicit flag
required). Rust: standard `cargo build --release -p opendb-node`. POC
bench: `make poc-bench-concurrent` (env knobs: `BENCH_CLIENTS`,
`BENCH_ITERATIONS`, `BENCH_SEED_ROWS`, `OPENDB_PERF_TIMING=1`).

## Roadmap state (work-package %)

Computed from the design docs + commit history. Two columns:
**code-done** (shipped to main) and **specified** (consensus ratified,
ready to code). Effort estimates from the consensus voters.

| WP  | Phase                                | Code-done | Specified | Effort to finish | Status |
|-----|--------------------------------------|----------:|----------:|------------------|--------|
| WP1 | Phase A — pgbench surface            | 8/10 = **80 %**  | 9/10 (with A.11 COPY) | +2 wk (A.11) | DECIDED |
| WP2 | Phase B — drop pgwire Mutex          | 2/5 = **40 %**   | 3/5 (B.4 spec'd) | +4 wk (B.4 → B.3 → B.2) | UNBLOCKING-E.2 |
| WP3 | Phase C — MVCC per-row versions      | 0/7 = **0 %**    | 7/7 (full spec) | 3 wk + vacuum | READY |
| WP4 | Phase D — secondary B-tree + hash    | 0/5 = **0 %**    | 5/5 (specced at WP level) | 2-3 wk | READY |
| WP5 | Phase E — WAL writer + group commit  | 2/5 = **40 %**   | 3/5 (E.3 spec'd) | +1 wk (E.3) + E.4 | DECIDED |
| WP6 | Phase F-I — OLAP track               | 0/6 = **0 %**    | 6/6 (F.1 spec'd; F.2/G/H/I outlined) | 14-20 wk | DECIDED |
| WP7 | Track B (foundation perf wins)       | 6/6 = **100 %**  | 6/6      | 0 | DONE ✅ |

**Aggregate**: 18/44 items code-shipped = **41 %**. After Phase B and
Phase A.11 land, the spec'd-but-unshipped buffer collapses into
end-to-end demos for both acceptance bars.

## Ratified architectural decisions (5/5 consensus)

Each was reached by 5 independent agent voters converging unanimously
and ratified by the user. Implementation must follow the spec.

| # | Phase | Decision | Effort | Design doc |
|---|-------|----------|-------:|------------|
| 1 | C MVCC | **Strategy A** — VersionChain wrapper around BTreeMap, vacuum non-negotiable in same milestone | 3 wk | `docs/roadmap/mvcc-strategy-2026.md` |
| 2 | E.3 WAL framing | **Option X** — postcard behind a `WalCodec` trait | 1 wk | `docs/roadmap/wal-framing-2026.md` |
| 3 | F.1 Projection trait | **Option P** — static trait + `enum ProjectionRef` dispatch, per-table opt-in via `CREATE TABLE WITH (engine='columnar')` | 4-6 wk | `docs/roadmap/projection-trait-2026.md` |
| 4 | B sequencing | **B.4 first** → B.3 → B.2 (B.4 is the only step that unblocks the already-built but currently-invisible Phase E.2 coalescing) | 4 wk | `docs/roadmap/phase-b-sequencing-2026.md` |
| 5 | A cliffs | **Option α** — ship COPY FROM STDIN, defer INSERT-FROM-SELECT (pending explicit user ratification) | 2 wk | `docs/roadmap/phase-a-cliffs-2026.md` |

Recap doc with open ops items: `docs/roadmap/decisions-for-user-2026-06-11.md`.

## Recommended implementation order (next ~10-13 weeks)

Sequenced for maximum observable wins.

1. **B.4 — per-session txn buffer** (2 wk). The single highest-leverage
   item. Currently the pgwire `Arc<Mutex<Database>>` serializes every
   connection before requests reach the commit worker (Phase E.2 task)
   — so E.2's cross-client coalescing drained 1 record per round at
   c=4 in the 2026-06-04 bench. B.4 moves `transaction:
   Option<TransactionBuffer>` out of `Database` and into per-pgwire-
   connection task state. Concurrent BEGIN/COMMIT finally works.
   Acceptance: `bench-concurrent.ts` with `BENCH_CLIENTS=8` running
   `BEGIN; INSERT; INSERT; COMMIT;` per client succeeds; the commit
   worker `try_recv` finally drains ≥ 2 records per round.

2. **B.3 — `Arc<RwLock<Database>>`** (1 wk). Trivial once B.4 has
   moved txn state out. Reads truly parallel.

3. **B.2 — parse + prepare outside the lock** (1 wk). Polish; planning
   was sub-millisecond, the win is only visible past B.4.

4. **C.1-C.7 — MVCC + vacuum** (3 wk). Critical: ship vacuum/HOT-
   pruning **in the same milestone** as the version chains, else
   unbounded chains degrade hot-row reads (the unanimous top-risk).

5. **E.3 — WalCodec trait + PostcardCodec** (1 wk). Bump
   `WAL_FRAME_VERSION` 1 → 2, dual-decode for one milestone before
   deprecating v1.

6. **A.11 — COPY FROM STDIN** (2 wk). Unblocks pgbench-default-init at
   any scale + Phase F OLAP TPC-H load. Reuses the existing
   `wal_writer.append_many` primitive.

7. **F.1 — Projection trait** (4-6 wk). Static dispatch via
   `enum ProjectionRef<'a>`. `RowProjection` refactored as a trait
   impl, `ColumnarProjection` MVP per `docs/roadmap/perf-vs-analytical-2026-05-22.md`.

8. **F.2 — ColumnarProjection** + **G — vectorized chunk exec** +
   **H — compression codecs** + **I — morsel-driven parallel scan**.
   Total ~14 wk. Target: TPC-H Q1 over 1M lineitem rows, cold ≤ 250 ms
   / warm ≤ 120 ms.

9. **D — secondary indexes** (2-3 wk). Required for HammerDB and
   sysbench `oltp_read_write`. Not on the critical path for pgbench
   (whose UPDATEs are PK-keyed).

## Acceptance bars (inherited from `docs/roadmap/perf-vision-2026.md`)

- **OLTP**: `pgbench -c 16 -j 4 -T 60 -M prepared` at scale 10,
  opendb TPS ≥ PG 16 on the same NVMe with `synchronous_commit=on`.
- **OLAP**: TPC-H Q1 over 1 M `lineitem` rows, opendb cold ≤ 250 ms /
  warm ≤ 120 ms (parity with ClickHouse).

Both are still pending user confirmation per
`docs/roadmap/decisions-for-user-2026-06-11.md` §D. Treat them as the
bar unless the user proposes otherwise.

## Key code anchors (existing implementations)

These are the load-bearing files codex will touch first.

- `crates/opendb-node/src/database.rs` — `Database` struct + execute path.
  Holds `transaction: Option<TransactionBuffer>` field that B.4 must
  move out.
- `crates/opendb-node/src/pgwire.rs` — `Arc<Mutex<Database>>` boundary
  + per-connection task. B.4's new home for txn state.
- `crates/opendb-consensus/src/commit_worker.rs` — Phase E.2 commit
  worker task. Already wired through `RootRange`; drains 1 record per
  round at c=4 until B.4 lands.
- `crates/opendb-consensus/src/root_range.rs` — `RootRange` is now
  `Arc<Self>` (Phase E.2 prereq). Validate helpers are `pub(crate)`
  for the commit worker to call directly.
- `crates/opendb-storage/src/wal.rs` — `Wal::append_many_with_len`
  primitive. Has a per-path `durable_len: Arc<Mutex<Option<u64>>>`
  cache that fixed the O(N²) re-decode bug (Track B foundation work).
- `crates/opendb-storage/src/wal_writer.rs` — Phase E.1 single-writer
  task. E.3 will wire the new `WalCodec` trait at the
  `encode_frame_serde_json` site.
- `crates/opendb-storage/src/row_projection.rs` — `RowProjection`. F.1
  will refactor this as a `Projection` trait impl.
- `crates/opendb-storage/src/commit_stream.rs` — `CommitRecord` and
  `Mutation` enum. C.1 must NOT change this layer (per the consensus
  doc); MVCC lives in `row_projection.rs`.
- `tools/sentropic-poc/bench-concurrent.ts` — the multi-client harness
  to measure B.4's win. Has `OPENDB_PERF_TIMING=1` stderr capture.
- `tools/pgbench-runner.sh` — pgbench runner with the `-I dtGvp`
  workaround. After A.11 COPY ships, switch back to default `-i`.

## Open user-decision items

These were intentionally NOT consensus-resolved because they're
operational/product choices, not architectural.

1. **Workspace recovery model** — durable mount vs. clone-on-demand.
2. **h2a stack reconnect** — keep or drop for this project.
3. **Cron re-arm** — `/loop 5min status+poursuite si pas de blocage`.
4. **Acceptance bars** — confirm the two inherited demos or propose
   alternatives.
5. **Phase A cliffs (Option α)** — user ratification of the 5/5 Opus
   consensus to ship COPY and defer INSERT-FROM-SELECT.

See `docs/roadmap/decisions-for-user-2026-06-11.md` for full context
on each. Don't block implementation on these — proceed with the
ratified items.

## What codex should NOT do

- **Do not re-debate** the 5 ratified decisions. The voters were
  unanimous on each. The design docs include explicit rationale,
  acceptance criteria, and out-of-scope sections.
- **Do not bundle** Phase C MVCC with the heap-tuple rewrite
  (Strategy B was explicitly rejected — wait for a dedicated
  storage-format phase).
- **Do not use `&dyn Projection`** for Phase F.1; the consensus
  explicitly mandates `enum ProjectionRef<'a>` to avoid vtable
  overhead on the OLTP hot path.
- **Do not ship MVCC without vacuum.** It's the unanimous top-risk
  mitigation and is non-negotiable per the spec.
- **Do not bypass `commit_worker`** when wiring B.4. The commit
  worker is the canonical write entry point now; B.4 unlocks its
  cross-client batching.

## Status as of this commit

Code: 18/44 backlog items shipped (41 %). 5 architectural decisions
ratified (4 by user, 1 by Opus consensus pending user ratification).
0 open Dependabot alerts (post the 3 security bumps this session).
All tests green at the last bench run (32 + 45 + 103 + 5 across
consensus / node / storage / wal_golden). Phase E.1 + E.2 measured
at c=4 with `OPENDB_PERF_TIMING=1`; cross-client coalescing is
plumbed but invisible until B.4.

Read `docs/roadmap/perf-vision-2026.md` for the strategic story
(beat PG transactional **and** beat ClickHouse / Vertica analytical)
and the two acceptance demos that define "done" for this milestone.
