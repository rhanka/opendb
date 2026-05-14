# Entropiq POC smoke — 2026-05-13

Empirical decision: can opendb-node's pgwire surface serve a Drizzle-backed read path against an entropiq-shaped table?

Reproduce: `npm run poc:entropiq:smoke`.

## Probe matrix

| Probe | Protocol | Outcome | Details |
|------|----------|---------|---------|
| A1 | simple | PASS | rows=[{"one":"1"}] |
| A2 | simple | PASS | command=CREATE |
| A3 | simple | PASS | command=INSERT |
| A4 | simple | PASS | rows=[{"id":"f1","workspace_id":"admin","name":"root","status":"completed","created_at":"2026-05-13T04:00:00.000Z"}] |
| A5 | simple | PASS | rows=[{"id":"f1","name":"root"}] |
| B1 | extended | PASS | rows=[{"id":"f1"}] |
| D1 | simple | PASS | rows=[{"count":"1"}] |
| D2 | simple | PASS | rows=[{"status":"completed","count":"1"}] |
| D3 | simple | PASS | rows=[{"status":"completed","count":"1"}] |
| D4 | extended | PASS | rows=[{"count":"0"}] |
| C1 | drizzle | PASS | rows=[{"id":"f1","workspaceId":"admin","name":"root","status":"completed","createdAt":"2026-05-13T00:00:00.000Z"}] |
| C2 | drizzle | PASS | rows=[{"id":"f1","workspaceId":"admin","name":"root","status":"completed","createdAt":"2026-05-13T00:00:00.000Z"}] |

## Gaps


## Verdict

Sprint 13 cannot proceed as a single read-only POC — three orthogonal gaps must close first, in order:

1. **Sprint 12 (Extended pgwire)** — hard prerequisite for any Drizzle client. Without it `db.select().from(folders).limit(1)` cannot return a row.
2. **SQL surface micro-sprint (proposed Sprint 12.1)** — add no-FROM `SELECT <expr>` literals (and at minimum `SELECT 1`, `SELECT version()`) plus explicit column projection in `SELECT`. These are both pre-handshake probes pg/Drizzle emit unconditionally.
3. **TIMESTAMP literal grammar** — accept Postgres-style `'2026-05-13 00:00:00'` and ISO-8601 `'2026-05-13T00:00:00Z'` in `INSERT` text values. Without this no realistic entropiq seed can be replayed.

## Next action (proposed)

- Promote Sprint 12 out of the parked state and design Sprint 12.1 (SQL surface gaps surfaced here) at the same time. Land them together as a single PR before re-running this smoke; the smoke is the green-light gate for the table-level read-only POC (entropiq seed + HTTP route).
