# OpenDB Milestone 2 Sprint 12.1 Design — SQL surface gaps for POC

Status: active (2026-05-14). Triaged from
`docs/bench/entropiq-poc-2026-05-13.md` probes A1, A3, A5.

## Goal

Close the three SQL-surface gaps surfaced by the entropiq POC smoke so
the simple-query handshake path returns sensible responses, and so the
TIMESTAMP literal grammar accepts Postgres + ISO-8601 forms.

Probe → fix mapping:

- A1 `SELECT 1 AS one` → support `SELECT <expr> [AS <alias>] [, …]`
  without `FROM`. Drivers and Drizzle's `pg` issue
  `SELECT 1`, `SELECT version()`, `SELECT current_timestamp` as
  connect-time pings.
- A5 `SELECT id, name FROM folders_smoke` → support explicit column
  projection alongside the existing `SELECT *`. Same parse path; the
  executor projects only the listed columns.
- A3 `INSERT … VALUES (… , '2026-05-13T00:00:00.000Z')` → coerce both
  ISO-8601 (`YYYY-MM-DDTHH:MM:SS[.uuu][Z|±HH:MM]`) and Postgres-style
  (`YYYY-MM-DD HH:MM:SS[.uuuuuu]`) `Text` literals into
  `Value::Timestamp(microseconds_since_epoch)` when the target column
  is `Timestamp`.

## Non-Goals

- No expression evaluation beyond integer/text/bool/null/version()/
  current_timestamp/now() function literals. No arithmetic, no string
  concat.
- No multi-table projection beyond what Sprint 10.5 already exposes
  (single FROM, optional JOIN). `SELECT col1, col2 FROM a JOIN b ON
  …` remains out of Sprint 12.1.
- No `INTERVAL` literal, no `TIMESTAMPTZ` semantics beyond stripping
  the trailing `Z`/offset and treating the value as naive UTC.

## Design

1. **AST**
   - `SelectColumns::Star` | `SelectColumns::Explicit(Vec<String>)` for
     the explicit projection. Replaces the implicit `*` semantics in
     `Statement::SelectAll`.
   - `Statement::SelectExpr { items: Vec<SelectExprItem> }` for the
     no-FROM form.
   - `SelectExprItem { expr: SelectExpr, alias: Option<String> }`.
   - `SelectExpr::{Literal(Value), Function(String)}`. Sprint 12.1
     only recognises three function names: `VERSION`, `NOW`,
     `CURRENT_TIMESTAMP`. Anything else returns `OpenDbError::Sql`.

2. **Parser**
   - Detect `SELECT <list>` where `<list>` does not start with `*`. If
     followed by ` FROM `, route to the existing `SelectAll` path
     with `SelectColumns::Explicit`. Otherwise route to
     `SelectExpr`.
   - Handle the `AS` keyword for aliases in both paths.

3. **Executor**
   - For `Statement::SelectAll` with `SelectColumns::Explicit`,
     project only the listed columns (qualified or unqualified).
     Reject unknown columns with `OpenDbError::Sql`.
   - For `Statement::SelectExpr`, return a single-row result. Each
     literal value renders verbatim; functions render to:
     - `VERSION()` → `"opendb-node 0.1.0 on PostgreSQL 16.0
       compatible"`.
     - `NOW()` / `CURRENT_TIMESTAMP` → `Value::Timestamp` based on the
       engine's monotonic `LogicalTimestamp`. Pgwire serialises with
       the existing `format_timestamp_micros`.

4. **TIMESTAMP coercion**
   - `coerce_value(Value::Text(s), ColumnType::Timestamp)` tries:
     - ISO-8601 `chrono::DateTime::parse_from_rfc3339`.
     - Postgres-style `chrono::NaiveDateTime::parse_from_str(s,
       "%Y-%m-%d %H:%M:%S%.f")`.
     - On success: `Value::Timestamp(microseconds_since_epoch)`.
     - On failure: leave `Value::Text(s)` for the existing type-
       mismatch error to surface.

## Test Strategy

- Parser tests for the three new forms.
- Executor tests:
  - `SELECT 1` returns a single row with `Value::Int64(1)`.
  - `SELECT version()` returns the canonical string.
  - `SELECT current_timestamp` returns a `Timestamp`.
  - `SELECT id, name FROM t` projects only those columns.
  - `INSERT INTO t (id, ts) VALUES (1, '2026-05-13T00:00:00Z')`
    succeeds when `ts` is `TIMESTAMP`.
- POC smoke re-run: A1, A3, A5 must move from FAIL to PASS once
  Sprint 12 also lands.

## Acceptance

Standard verifications green plus `npm run poc:entropiq:smoke` shows
A1/A3/A5 = PASS (B1/C1/C2 still FAIL pending Sprint 12).
