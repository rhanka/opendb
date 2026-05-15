# OpenDB Milestone 2 Sprint 10.5 Design — INNER / LEFT JOIN

Status: draft (2026-05-12)

## Goal

Sprint 10 added ORDER BY / LIMIT / OFFSET. Sprint 10.5 closes the
remaining JOIN gap from the original Sprint 10 scope. Sentropic uses 16
`INNER JOIN` and 12 `LEFT JOIN` sites in its TypeScript source; this
sprint is the last big SELECT-surface piece before transactions
(Sprint 11) and prepared statements (Sprint 12).

## Non-Goals

- No RIGHT / FULL / CROSS JOIN.
- No subqueries.
- No alias projection — projection stays `SELECT *` only; column
  names in the result come back as `<table>.<column>`.
- No GROUP BY or aggregates (kept deferred — sentropic does not need
  them in hot paths).
- No multiple JOINs in one query — Sprint 10.5 supports a single JOIN
  clause (left table JOIN right table). Multi-table joins go to Sprint
  10.6 if needed.

## Design

`Statement::Select` (a new variant alongside `SelectAll`):

```rust
Statement::Select {
    left: String,
    join: JoinClause,
    where_clause: Option<JoinedPredicate>,
    order_by: Option<JoinedOrderBy>,
    limit: Option<u64>,
    offset: Option<u64>,
}

pub struct JoinClause {
    pub kind: JoinKind,
    pub right: String,
    pub left_column: String,
    pub right_column: String,
}

pub enum JoinKind { Inner, Left }

pub struct JoinedPredicate {
    pub qualifier: Option<String>,
    pub column: String,
    pub value: Value,
}

pub struct JoinedOrderBy {
    pub qualifier: Option<String>,
    pub column: String,
    pub direction: OrderDirection,
}
```

Parser recognises:

```sql
SELECT * FROM <left> INNER JOIN <right> ON <left>.<col> = <right>.<col>
SELECT * FROM <left> LEFT JOIN <right> ON ...
```

(`JOIN` is treated as `INNER JOIN`.)

Executor: nested-loop join over the projection. LEFT pads with
`Value::Null` when no match. The projection emits the columns of the
left table first, then the right table, with `table.column` names.

WHERE / ORDER BY / LIMIT / OFFSET apply to the joined row set.

## Test Strategy

- Parser: `SELECT * FROM a INNER JOIN b ON a.id = b.a_id` parses to
  expected AST; same with `LEFT JOIN`.
- Executor: INNER returns intersection; LEFT returns all left rows with
  NULL padding; combined with WHERE / ORDER BY / LIMIT.
- pgwire parity: a join smoke that creates two tables, inserts rows,
  and verifies the join row count + column names.
- bench seed: `tools/bench/join.ts` — nested-loop join over 100/100
  rows.

## Acceptance Criteria

- `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test
  --workspace` green.
- Parity test green.
- bench:join runs.
