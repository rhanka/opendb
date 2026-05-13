# OpenDB Milestone 2 Sprint 10 Design — JOINs + ORDER BY + LIMIT

Status: draft (2026-05-12)

## Goal

Sprint 10 extends the SELECT surface with `INNER JOIN`, `LEFT JOIN`,
`ORDER BY`, `LIMIT`, and `OFFSET`. Drizzle issues 28 join sites in the
entropiq codebase plus heavy `orderBy` / `limit` usage. With this sprint,
the substitution pilot can read its main relational paths.

`GROUP BY` and aggregates land in Sprint 10.5 if entropiq usage forces
them; otherwise they stay deferred.

## Non-Goals

- No CROSS JOIN, RIGHT JOIN, FULL OUTER JOIN.
- No subqueries (`SELECT ... FROM (SELECT ...)`).
- No correlated subqueries.
- No `HAVING`, no `WINDOW` functions.
- No view materialization.
- No index usage; joins are nested-loop on the in-memory projection.
- `WHERE` extends from PK-only equality to: simple `col = literal` AND
  `col = col` (joined columns), nothing more in Sprint 10.

## Design Summary

1. **AST extensions** (`opendb-sql`):
   - `Statement::Select { from: SelectSource, columns: SelectColumns,
     order_by: Vec<OrderBy>, limit: Option<u64>, offset: Option<u64> }`
   - `SelectSource::Table { name }`
   - `SelectSource::Join { left: Box<SelectSource>, right: SelectSource,
     kind: JoinKind, on: JoinPredicate }`
   - `SelectColumns::All` (`SELECT *`) | `SelectColumns::List(Vec<...>)`
     (Sprint 10.5).
   - Sprint 10 keeps `SelectColumns::All`. The `*` projects all columns
     from every joined source (qualified as `<table>.<column>`).
   - `JoinKind::{Inner, Left}`.
   - `JoinPredicate::Equality { left_table, left_column, right_table,
     right_column }`.
   - `OrderBy { table_qualifier: Option<String>, column: String,
     direction: OrderDirection }`.
   - Existing `Statement::SelectAll` is gradually replaced; we keep it
     temporarily for backward compatibility and have the parser route
     unqualified bare `SELECT * FROM t [WHERE pk=?]` through the legacy
     path.

2. **Parser**:
   - Extend `parse_select_all` to detect ` JOIN `, ` ORDER BY `,
     ` LIMIT `, ` OFFSET ` and emit the new `Statement::Select` when any
     of them appear. Otherwise fall back to the legacy `SelectAll`.
   - Parse table aliases `<table> AS <alias>` and `<table> <alias>`.
   - Parse join clauses `INNER JOIN <table> ON <table.col> = <table.col>`
     and `LEFT JOIN ... ON ...`.

3. **Executor**:
   - For `Statement::Select`:
     - Build the materialized join: nested loop. For LEFT JOIN, emit
       NULL-padded right rows when no match.
     - Apply ORDER BY using stable sort.
     - Apply LIMIT and OFFSET by slicing.
     - Project all columns; column names follow `<table>.<column>` (or
       just `<column>` when the source is single-table for backwards
       compatibility).

4. **pgwire**:
   - Multi-table column names in RowDescription are
     `qualifier.column` strings.
   - No OID logic change.

## Test Strategy

- `opendb-sql` parser: parses each new keyword combination.
- `opendb-sql` executor:
  - INNER JOIN returns intersection rows.
  - LEFT JOIN returns all left rows with NULL-padded right when no
    match.
  - ORDER BY ASC / DESC sorts deterministically.
  - LIMIT / OFFSET slice correctly.
  - Combined: WHERE on the join result (joined-column predicate).
- `tests/parity`: pgwire smoke gains a join + order-by + limit
  scenario.
- `tools/bench/join.ts`: join throughput micro-bench.

## Acceptance Criteria

- `cargo test --workspace`, `cargo clippy -D warnings`, `cargo fmt
  --check` green.
- Parity vitest green.
- bench:join runs end-to-end.
