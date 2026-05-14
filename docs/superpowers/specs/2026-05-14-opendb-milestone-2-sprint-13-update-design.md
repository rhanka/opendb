# OpenDB Milestone 2 Sprint 13 Design — UPDATE

Status: active (2026-05-14). Triaged from
`docs/superpowers/audits/2026-05-14-entropiq-substitution-gap.md`
(126 `.update(...)` sites in entropiq, blocks ~25 % of HTTP routes).

## Goal

Support Drizzle's `db.update(table).set({...}).where(eq(pk, x))` for
the single-row-by-primary-key shape. WHERE on non-PK columns lands in
Sprint 14 with composite predicates.

Statement: `UPDATE <table> SET <col1> = <lit1> [, <col2> = <lit2>, ...]
WHERE <pk> = <literal>`.

## Non-Goals

- No WHERE on non-PK columns (Sprint 14).
- No `RETURNING` (Sprint 16).
- No multi-row update.
- No `UPDATE FROM ...` (PostgreSQL-specific join in UPDATE).
- No expression on RHS (`SET counter = counter + 1`); only literals.

## Design

1. **Storage** (`opendb-storage`)
   - `Mutation::UpdateRow { table, key, assignments: Vec<ColumnValue> }`.
   - `RowProjection::apply` for `UpdateRow`:
     - resolve `(table, key)` — error `NotFound` if absent.
     - validate every assigned column exists, type-check the value
       (with `coerce_value`), reject NULL on NOT NULL.
     - re-run UNIQUE / FK enforcement on the resulting row (an UPDATE
       can violate a UNIQUE constraint or break a FK target).
     - apply in-place.

2. **AST + Parser** (`opendb-sql`)
   - `Statement::UpdateRow { table, key, assignments: Vec<(String, Value)> }`.
   - Parser:
     - strip `UPDATE <ident>`.
     - read `SET <col> = <literal> [, <col> = <literal>]*`.
     - read `WHERE <pk> = <literal>`.
     - unquote identifiers (`"users"."name"` → `name`).

3. **Executor**
   - `Statement::UpdateRow` → builds `Mutation::UpdateRow`, route
     `Key { table, key: route_key(&table, &key) }`, tag `"UPDATE 1"`.

4. **pgwire**: nothing new; the existing `Q` and `P/B/E` paths route
   through `Database::execute`.

## Tests

- Parser: `UPDATE accounts SET name = 'Bob', status = 'active' WHERE id = 1`
  parses to expected AST.
- Storage projection: round-trip insert + update + select returns
  updated row.
- UNIQUE constraint violation on update is rejected.
- FK target update without breaking constraints succeeds.
- Drizzle smoke probe `db.update(folders).set({ name: 'new' }).where(eq(folders.id, 'f1'))`
  succeeds end-to-end through pgwire Extended.

## Acceptance

Standard verifications green plus a new probe in
`tools/entropiq-poc/smoke.ts` (D1) passes.
