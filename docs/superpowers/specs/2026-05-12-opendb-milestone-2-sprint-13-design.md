# OpenDB Milestone 2 Sprint 13 Design — Entropiq POC read-only

Status: **blocked on Sprint 12 + Sprint 12.1** (2026-05-13). Smoke
ran against opendb-node and produced a complete decision matrix —
see `docs/bench/entropiq-poc-2026-05-13.md`. Reproduce with
`npm run poc:entropiq:smoke`.

## Goal

Connect a real Drizzle-backed entropiq slice to opendb-node and prove
the read path end to end. The target is one realistic table (`folders`
or `initiatives` — both top-5 in entropiq's query frequency analysis)
served from opendb-node, read by the entropiq HTTP API in a docker
compose harness.

## Concrete check-list

1. **Driver compatibility**: configure Drizzle / `pg` to issue simple
   queries (no Extended protocol) for this POC. If Drizzle insists on
   Extended even after pool tweaks, fall back to Sprint 12 (Extended
   pgwire) before continuing.
2. **Schema subset**: pick 1-5 entropiq tables that have only types
   OpenDB supports (TEXT, INT, JSONB, TIMESTAMP, BOOL). The first
   candidates are `folders`, `business_config`, `magic_links`,
   `email_verification_codes`.
3. **Migration run**: apply the matching Drizzle migration files
   against opendb-node via `npm run bench:fk`-style harness (extract
   only the SQL and run through pgwire).
4. **Seed data**: replay the first ~100 rows of each chosen table from
   a one-shot SQL dump.
5. **HTTP smoke**: stand up `entropiq/api` against `DATABASE_URL`
   pointing at the opendb-node pgwire endpoint. Hit one or two
   read-only HTTP routes (the ones that only `SELECT`).
6. **Report**: capture the trace of every SQL OpenDB sees, surface
   gaps (unsupported statement, type mismatch, missing index) under
   `docs/bench/entropiq-poc-YYYY-MM-DD.md`.

## Non-Goals

- No write traffic from entropiq (Sprint 14).
- No coverage of the chat / queue tables (heavy join paths, deferred).
- No multi-pod opendb cluster — single-node only, the kube path stays
  Sprint 5+ work.

## Likely follow-ups

- Sprint 14: write path on the same tables, including bulk INSERT and
  cascade DELETE.
- Sprint 15: full migration replay + perf-vs-PG benchmark report.

## User checkpoints

The user wanted to be back in the loop from Sprint 13 onwards.
Defaults accepted on 2026-05-13:

- Table subset: `business_config` (trivial control) + `folders` (FK +
  default + timestamp coverage). `initiatives` deferred to Sprint 14.
- Drizzle mode: try simple-query first via the smoke
  (`tools/entropiq-poc/smoke.ts`) — Sprint 12 reactivated if the smoke
  proves Extended is unavoidable.
- Compose harness: local `docker-compose.yml` under
  `tools/entropiq-poc/` (single opendb-node + entropiq-api service);
  k3d kept for the Sprint 5+ cluster track.

## Findings 2026-05-13 (smoke run)

The probe matrix is in `docs/bench/entropiq-poc-2026-05-13.md`. Summary:

- Simple-query baseline only partially works: `CREATE TABLE` and
  `SELECT *` pass, but `SELECT 1` (driver-level health probe),
  explicit column projection (`SELECT a, b FROM t`), and the
  ISO/Postgres TIMESTAMP literal in `INSERT … VALUES` all fail.
- Extended-protocol path is fully missing: every `Parse` / `Bind` /
  `Describe` / `Execute` / `Sync` message returns `unsupported
  message tag`.
- Conclusion: Sprint 13 cannot start until Sprint 12 (Extended) and a
  small SQL-surface sprint (proposed **Sprint 12.1**) land. After
  that, the smoke under `npm run poc:entropiq:smoke` is the green-
  light gate before adding the entropiq compose harness and HTTP
  smoke.

## Proposed Sprint 12.1 scope (new)

Triaged from probes A1, A3, A5:

- `SELECT <expr>` without `FROM` for `SELECT 1`, `SELECT version()`,
  `SELECT current_timestamp` (minimum surface drivers ping at
  connect).
- Explicit column projection in `SELECT col1, col2 FROM t`.
- TIMESTAMP literal grammar in `INSERT … VALUES` accepting both
  Postgres-style `'2026-05-13 00:00:00'` and ISO-8601
  `'2026-05-13T00:00:00Z'`.

Sprint 12.1 lands together with Sprint 12 in a single PR; the smoke
re-run is the acceptance criterion.
