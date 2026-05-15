# OpenDB Milestone 2 Sprint 9 Design — UNIQUE / FK enforcement / DELETE

Status: draft (2026-05-12)

## Goal

Sprint 8 recorded UNIQUE and FOREIGN KEY constraints as metadata. Sprint
9 makes them enforce-able at INSERT time, and introduces a minimal
`DELETE FROM t WHERE pk = ?` statement so referential integrity has a
write path other than INSERT. This unblocks any sentropic INSERT/DELETE
flow that depends on cascades or unique columns.

## Non-Goals

- No `UPDATE` statement (Sprint 10).
- No cascading `ON DELETE` action chains across multi-level graphs;
  Sprint 9 walks one hop. Multi-hop cascades land in Sprint 10 with the
  same FK metadata.
- No `ON UPDATE` action enforcement (we still record the metadata).
- No deferred constraint evaluation; every check fires inline.
- No secondary-index acceleration for UNIQUE / FK lookups; a full row
  scan per check is acceptable for the volumes Sprint 9 targets.

## Design Summary

1. **`RowProjection` extensions** (`opendb-storage`):
   - `RowProjection::apply` walks the new constraint list at every
     `InsertRow` mutation:
     - `Unique { columns }` → reject if any existing row shares the
       same tuple values.
     - `ForeignKey { columns, references_table, references_columns,
       … }` → reject unless a matching row exists in the referenced
       table (referenced_columns must form a primary key — Sprint 9
       restriction).
   - New `Mutation::DeleteRow { table, key }`. Apply removes the row;
     before removal, if any other table holds a ForeignKey pointing
     here:
       - `Cascade` → enqueue dependent deletes (one hop) and apply them
         in the same record.
       - `NoAction` / `Restrict` → reject the delete with
         `InvalidInput`.
       - `SetNull` → set the matching columns on dependent rows to
         `Value::Null` (rejected if column is `NOT NULL`).
       - `SetDefault` → use the column default if present, otherwise
         reject.
   - The dependent walk is intentionally single-hop in this sprint.

2. **Parser**:
   - `DELETE FROM <table> WHERE <pk> = <literal>`.
   - Reject other `DELETE` forms with a clear error.

3. **AST + Executor**:
   - `Statement::DeleteRow { table, key }`.
   - The executor builds `Mutation::DeleteRow` and submits as a Write
     with route intent `Key { table, key: route_key(table, &row_key) }`.
   - Command tag: `"DELETE 1"`.

4. **pgwire**: no protocol-level change; command tag follows the
   executor.

5. **Bench seed**: extend `tools/bench/` with `fk-insert-delete.ts` —
   N inserts under a FK relationship followed by deletes that cascade.
   Same opt-in `--with-pg` flag.

## Test Strategy

- `opendb-storage`: `RowProjection` rejects duplicate UNIQUE rows;
  rejects FK INSERT without parent; accepts DELETE with cascade; rejects
  DELETE with NoAction when child rows exist; tests for `SetNull` and
  `SetDefault`.
- `opendb-storage`: WAL fixture for `Mutation::DeleteRow`.
- `opendb-sql`: parser test for `DELETE FROM …`.
- `opendb-sql`: executor test for an end-to-end UNIQUE rejection;
  end-to-end FK insertion rejection; end-to-end DELETE with cascade.
- `tests/parity`: pgwire smoke gains a UNIQUE + FK + DELETE scenario.

## Review Points

- Constraint enforcement happens during projection apply, not later in
  the pipeline — so replays remain deterministic.
- Single-hop cascade only; multi-hop is Sprint 10 work.
- No new mutation versioning required.
- No Python introduced.

## Acceptance Criteria

- `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test
  --workspace` green.
- `npm run check:ts`, `npm run check:no-python`, `npm run
  check:manifests`, `npm test` green.
- `npm run bench:fk -- --rows 200` returns a JSON summary.
