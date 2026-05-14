# Entropiq POC smoke — 2026-05-13

Empirical decision: can opendb-node's pgwire surface serve a Drizzle-backed read path against an entropiq-shaped table?

Reproduce: `npm run poc:entropiq:smoke`.

## Probe matrix

| Probe | Protocol | Outcome | Details |
|------|----------|---------|---------|
| A1 | simple | FAIL | error: sql error: unsupported SQL: SELECT 1 AS one |
| A2 | simple | PASS | command=CREATE |
| A3 | simple | FAIL | error: invalid input: value for column created_at on table folders_smoke does not match Timestamp |
| A4 | simple | PASS | rows=[] |
| A5 | simple | FAIL | error: sql error: unsupported SQL: SELECT id, name FROM folders_smoke |
| B1 | extended | FAIL | error: unsupported message tag 80 |
| C1 | drizzle | FAIL | error: unsupported message tag 80 |
| C2 | drizzle | FAIL | error: unsupported message tag 80 |

## Gaps

- **No-FROM `SELECT <expr>` not supported** (probe A1). Drivers and pgwire clients commonly issue `SELECT 1` / `SELECT version()` as health/probe queries on connect.
- **Explicit column projection `SELECT a, b FROM t` not supported** (probe A5). Only `SELECT *` returns rows; Drizzle and most ORMs emit explicit lists.
- **TIMESTAMP literal coercion gap** (probe A3): `error: invalid input: value for column created_at on table folders_smoke does not match Timestamp`. Need to align the accepted literal grammar with Postgres ISO-8601 forms.
- **Extended protocol entirely missing** (probes B1, C1, C2): every `Parse`/`Bind`/`Describe`/`Execute`/`Sync` message returns "unsupported message tag". Drizzle always issues Extended-protocol queries via `pg`, so the simple-query fallback path is unreachable from Drizzle.

## Verdict

Sprint 13 cannot proceed as a single read-only POC — three orthogonal gaps must close first, in order:

1. **Sprint 12 (Extended pgwire)** — hard prerequisite for any Drizzle client. Without it `db.select().from(folders).limit(1)` cannot return a row.
2. **SQL surface micro-sprint (proposed Sprint 12.1)** — add no-FROM `SELECT <expr>` literals (and at minimum `SELECT 1`, `SELECT version()`) plus explicit column projection in `SELECT`. These are both pre-handshake probes pg/Drizzle emit unconditionally.
3. **TIMESTAMP literal grammar** — accept Postgres-style `'2026-05-13 00:00:00'` and ISO-8601 `'2026-05-13T00:00:00Z'` in `INSERT` text values. Without this no realistic entropiq seed can be replayed.

## Next action (proposed)

- Promote Sprint 12 out of the parked state and design Sprint 12.1 (SQL surface gaps surfaced here) at the same time. Land them together as a single PR before re-running this smoke; the smoke is the green-light gate for the table-level read-only POC (entropiq seed + HTTP route).
