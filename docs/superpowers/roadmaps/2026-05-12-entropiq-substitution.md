# OpenDB → Entropiq Substitution Roadmap

Date: 2026-05-12
Target product: `/home/antoinefa/src/entropiq` (Drizzle ORM + PostgreSQL via
`pg` Pool, 50 tables, 28 migrations).

## Why this exists

We want OpenDB to be able to host an existing production-shaped TypeScript
service. Entropiq is the chosen pilot: it is a real workload we control end
to end, it uses Drizzle over pgwire (no Supabase-specific dependency), and
its schema is representative (heavy JSONB, plenty of joins, transactions,
indexes, foreign keys).

This document is the user-visible plan. Each row maps to one or two
sprints. Sprints are executed in order; each sprint ends with a `cargo
test --workspace`, `npm test`, `npm run smoke:k3s` green run.

## Inventory snapshot (2026-05-12)

- Stack: `pg` Pool + `drizzle-orm/node-postgres`; `drizzle-kit generate`
  produces SQL migrations under `api/drizzle/0000_…sql` to `0027_…sql`.
- 50 `pgTable` calls; column types observed:
  - 394 `text`, 104 `timestamp`, 75 `jsonb`, 21 `integer`, 12 `boolean`,
    3 `json`.
  - 118 `index`, 99 `references`, 42 `unique`, 49 `primaryKey`.
- Query verbs: 16 `innerJoin`, 12 `leftJoin`, 15 `transaction`, 109 raw
  `` sql`…` ``, 4 `groupBy`, 2 `distinct`, 1 `union`.
- Defaults: `defaultNow()`, JSONB defaults (`'{}'::jsonb`), and a few
  enumerated string defaults via `default('completed')`.
- DDL patterns: `DO $$ … EXCEPTION WHEN duplicate_object` blocks,
  `CREATE INDEX IF NOT EXISTS`, `ALTER TABLE ADD CONSTRAINT … FOREIGN
  KEY … ON DELETE`.
- No PG extension (no `pgcrypto`, no `postgis`, no `tsvector`).
- No JSONB SQL operators (`->`, `->>`, `@>`) in source — JSONB is read
  raw and parsed in TS.

## Current OpenDB capabilities (end of Sprint 4)

Supported: `CREATE TABLE` with primary key on a single typed column
(`INT`/`TEXT`), `INSERT VALUES (…)`, `SELECT *` and `SELECT * WHERE
pk = …`. pgwire minimal. Range catalog with split/merge metadata typed,
routing actif, root-stream logical range validation. Recovery contract +
operator conditions live.

The gap to entropiq is large enough that we plan it as nine sprints
post Sprint 5 (which closes the range-catalog runtime story, not the
SQL surface).

## Sprint sequence

Estimates below are in **active workdays** at the Sprint 4 pace (roughly
one sprint = 2-5 active days of paired work).

| # | Sprint                                                                 | Effort | Unlocks                                                       |
|---|-----------------------------------------------------------------------|--------|---------------------------------------------------------------|
| 5 | Range split/merge runtime (admin endpoint, allocator, condition)       | 2-3 d  | live split/merge on a running cluster                         |
| 6 | Types: `BOOLEAN`, `TIMESTAMP`, `FLOAT64`, `NOT NULL`, `DEFAULT`, `DEFAULT now()` | 2-3 d  | enough scalar coverage to seed entropiq's metadata rows       |
| 7 | `JSONB` storage + parser + pgwire serialization                        | 4-5 d  | 75 jsonb columns become representable; entropiq core unlocked |
| 8 | `ALTER TABLE` (add/drop/rename column, add constraint) + `CREATE INDEX` + `DO $$` idempotency | 4-5 d  | drizzle-kit migrations apply unchanged                        |
| 9 | `UNIQUE` + foreign keys + `ON DELETE CASCADE`                          | 3-4 d  | referential integrity matches drizzle assumptions             |
| 10 | `SELECT` with `INNER`/`LEFT JOIN`, `WHERE` composé, `GROUP BY`, `ORDER BY`, `LIMIT`, `OFFSET` | 5-7 d  | 28 join sites and 4 groupBy use cases pass                    |
| 11 | Transactions (`BEGIN`/`COMMIT`/`ROLLBACK`, snapshot isolation)         | 5-7 d  | 15 transaction sites compatible                               |
| 12 | pgwire prepared-statement protocol + parameter binding + Drizzle compat | 4-5 d  | Drizzle client connects and runs unmodified queries           |
| 12.5 | Benchmarks: OpenDB vs PostgreSQL (`tools/bench-pg.ts` ts-only via `pg`) — INSERT/SELECT throughput, JSONB roundtrip, named INSERT, transaction overhead. Numbers committed under `docs/bench/` | 3-4 d  | first quantitative comparison and regression baseline         |
| 13 | POC entropiq read-only on 1-5 tables (e.g. `folders`, `initiatives`)   | 2-3 d  | first end-to-end proof against entropiq's HTTP API            |
| 14 | POC élargi: 10-20 tables, writes + simple joins, no complex tx         | 3-5 d  | covers majority of entropiq's hot paths                       |
| 15 | Substitution complète: full migrations replay + UAT entropiq full + perf-vs-PG report | 5-7 d  | drop-in DATABASE_URL swap with documented perf delta          |

The bench harness lands in Sprint 12.5 but is **incrementally seeded from
Sprint 7 onwards**: each sprint that introduces a SQL feature also adds a
matching micro-benchmark fixture under `tools/bench/`, so by the time
Sprint 12.5 runs the full comparison the input set is already non-trivial.

Totals (in active workdays):

- POC partiel (Sprint 9 done): **~15-20 d** ≈ 3 weeks of active work.
- POC élargi (Sprint 11 done): **~25-30 d** ≈ 5-6 weeks.
- Bench baseline (Sprint 12.5 done): **~32-39 d** ≈ 6-8 weeks.
- Substitution complète et stable (Sprint 15 done): **~42-55 d**
  ≈ 8-11 weeks.

In calendar weeks at a 4-6 active-hours-per-day cadence with the user,
those numbers translate to roughly:

- POC partiel : **~3 semaines** calendrier.
- POC élargi : **~5-6 semaines** calendrier.
- Substitution complète : **~7-9 semaines** calendrier.

## Decisions captured along the way

- **No Python**: any tooling for JSONB tests, prepared-statement smoke,
  and entropiq parity harness is TypeScript only (vitest), enforced by
  `npm run check:no-python`.
- **No object storage during this roadmap**: archive metadata stays
  metadata-only until the substitution UAT is green.
- **Commit stream remains canonical**: transactions in Sprint 11 ride
  on top of the commit stream + snapshot reads; no parallel WAL is
  introduced.
- **pgwire stays a compatibility layer**: any extension to the SQL
  surface is implemented inside OpenDB and exposed through pgwire, not
  through a Drizzle-specific shim.
- **No AI attribution in commits**: every push must keep
  `git log origin/main --grep="anthropic\|claude\|🤖" -i --oneline | wc -l`
  at `0`.

## Tracking

Per-sprint spec + plan files land under
`docs/superpowers/specs/YYYY-MM-DD-opendb-milestone-2-sprint-N-design.md`
and
`docs/superpowers/plans/YYYY-MM-DD-opendb-milestone-2-sprint-N.md`,
following the format used since Sprint 1. The first entropiq-parity
vitest file will live at `tests/parity/entropiq-mini.test.ts` and grow
across sprints 13-15.
