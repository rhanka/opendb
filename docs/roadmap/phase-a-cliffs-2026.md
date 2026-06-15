# Phase A — cliffs decision: COPY FROM STDIN vs INSERT-FROM-SELECT (2026-06-11)

5-way agent consensus (Claude Opus voters) on whether to ship one, both,
or neither of the two remaining Phase A cliffs identified in
`docs/bench/pgbench-phase-a-2026-05-22.md`.

## Decision: Option α — ship COPY FROM STDIN, defer INSERT-FROM-SELECT

5/5 votes for α. Estimated effort: **2 weeks** (median; floor 1 wk for
COPY itself, ceiling 2 wk including edge-case handling for binary mode
+ error-mid-stream + large rows). Top risk converged across voters: **COPY
edge cases inflate the 5-8 day estimate beyond one sprint, OR a
downstream OLAP pattern in TPC-H Q1 surfaces a need for
INSERT-from-SELECT we under-scoped today**.

## Consensus rationale

1. **Two acceptance demos unlocked for one week of work.** The OLTP demo
   (`pgbench -c 16 -T 60 -M prepared` at scale 10 = 1 M accounts) and
   the OLAP demo (`COPY lineitem FROM 'lineitem.tbl'` for TPC-H Q1 over
   1 M rows) both *need* a bulk-ingest path. Custom loaders (γ) are
   throwaway scripts that must be rewritten per demo. COPY is the
   reusable primitive that earns its keep twice.

2. **Architectural alignment with the existing `wal_writer` task.**
   COPY's natural shape is: buffer N rows from the client into one
   `wal_writer.append_many` batch. That's a perfect fit for the Phase
   E.1 primitive shipped 2026-05-23. Low risk, low surprise.

3. **INSERT-from-SELECT has zero current user demand.** Sentropic
   doesn't use it. Neither acceptance demo needs it. Building a
   3-4 week architectural feature (query-to-write pipeline + set-
   returning-function framework for `generate_series`) speculatively
   is gold-plating a Phase A that's already 80 % shipped.

4. **Phase A closes cleanly at 9/10 with α.** The remaining cliff is
   documented as a deferred feature with a clear scope; not a
   half-finished implementation. Phase A's purpose was surface gaps,
   prioritize fixes, and ship — α completes that loop.

5. **Defer-and-revisit is cheap because we built the surface map.**
   `docs/bench/pgbench-phase-a-2026-05-22.md` already catalogs the
   INSERT-from-SELECT cliff with its full scope and rationale. If a
   real user later complains, we have the spec ready and can resume
   without re-discovery cost.

## Implementation order (within the 2-week budget)

1. **Day 1-2** — pgwire protocol-level CopyInResponse / CopyData /
   CopyDone message handlers. State machine for "in COPY mode" on a
   pgwire connection task. Reject mid-stream non-COPY frames.
2. **Day 3-4** — parser side: `COPY <table> FROM STDIN [WITH (...)]`.
   Recognize the FREEZE / DELIMITER / FORMAT modifiers as no-ops or
   bounded behaviors. Decoder per format (CSV default, binary later).
3. **Day 5-7** — buffer rows into a `Vec<CommitRecord>` until the
   batch hits N rows (configurable, default 1024) or a CopyDone
   arrives; flush via `commit_worker.commit(records)`. Reuses the
   existing per-session txn-buffer pathway once B.4 lands.
4. **Day 8-9** — tests: round-trip via `psql \copy`, pgbench `-i`
   default mode, golden text-mode CSV file.
5. **Day 10** — perf bench. `pgbench -i -s 10` against opendb should
   complete in seconds (one fsync per ~1024 rows = ~1000 fsyncs for
   1 M accounts).

## Acceptance criteria

- `psql -c "COPY pgbench_accounts FROM STDIN" < accounts.tsv` succeeds
  against opendb-node.
- `pgbench -i -s 10` against opendb-node completes without
  `-I dtGvp` workaround.
- Per-record cost: ≤ 50 µs for the COPY ingest path at scale 10
  (vs the per-INSERT ~2 ms baseline = 40× faster bulk).
- WAL writer fsync count ≤ ⌈row_count / 1024⌉ + epsilon.
- Reuses `commit_worker` for atomicity guarantees; partial-batch
  errors roll back the in-progress batch and the connection task
  reports the error to the client.

## Out of scope

- **COPY TO** (export). Read-side; bench demos don't need it.
- **Binary COPY format**. Text mode covers the bench needs; binary
  is a follow-up.
- **`COPY ... FROM 'file'`** (server-side file read). Security
  considerations; only support `STDIN` for now.
- INSERT-from-SELECT + generate_series + set-returning-function
  framework. Deferred until a real workload requires it.

## Provenance

5 independent voter outputs, all converging on α (ship COPY only).
Top-risk phrasing diverged slightly (COPY edge cases vs deferred
INSERT-from-SELECT surfacing later), but rationale was unanimous on
"COPY = 1 week unlock with reusable primitive; INSERT-from-SELECT =
3-4 weeks for zero current demand". Voter transcripts in
`/scratch/tmp/claude-0/.../tasks/a21a2a5a7*, a029fa41c*, af57da48a*,
a1a9ced27*, a6baf9332*`. All five voters used Claude Opus per user
request ("avec un claude 4.8max" → highest available model =
claude-opus-4-7).

## Track item

Will be added: under WP1 (Phase A), as a new feature item
"A.11 — COPY FROM STDIN (pgwire protocol + commit_worker batching)"
with the 2-week estimate and acceptance criteria above. Defers cliff
A.12 (INSERT-FROM-SELECT) explicitly as "won't-do until user demand".
