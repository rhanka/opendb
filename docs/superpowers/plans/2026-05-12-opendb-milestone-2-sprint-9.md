# OpenDB Milestone 2 Sprint 9 Implementation Plan — UNIQUE / FK / DELETE

Date: 2026-05-12

## Source Spec

`docs/superpowers/specs/2026-05-12-opendb-milestone-2-sprint-9-design.md`.

## Tasks

### Task 1 — Storage Mutation::DeleteRow + RowProjection enforcement

- `commit_stream.rs`: add `Mutation::DeleteRow { table, key }`.
- `row_projection.rs`: enforce UNIQUE / FK at InsertRow; apply
  DeleteRow with one-hop cascade actions.
- Update sibling files (range_catalog, archive_manifest) with the
  catch-all branch.
- Tests:
  - duplicate UNIQUE → InvalidInput
  - INSERT without parent FK row → InvalidInput
  - DELETE with cascade triggers dependent removal
  - DELETE with `NoAction` blocked when child exists
  - DELETE with `SetNull` clears dependent values
- Commit `feat: enforce unique and fk constraints on insert and delete`.

### Task 2 — WAL fixture

- Append a `Mutation::DeleteRow` golden fixture under
  `tests/fixtures/wal/frame-v1-record-v2-delete-row.hex`.
- Commit `test: cover delete row wal compatibility`.

### Task 3 — SQL parser DELETE

- `parser.rs`: parse `DELETE FROM <t> WHERE <pk> = <literal>` only.
- `ast.rs`: new `Statement::DeleteRow { table, key }`.
- Tests cover the supported form and reject the rest.
- Commit `feat: parse delete from where pk equality`.

### Task 4 — Executor DELETE

- `executor.rs`: route DELETE through `Mutation::DeleteRow`, route
  intent `Key`, tag `DELETE 1`.
- Tests: end-to-end DELETE, UNIQUE rejection, FK rejection.
- Commit `feat: execute delete row mutations through executor`.

### Task 5 — pgwire parity + bench seed + docs

- pgwire smoke gains a UNIQUE + FK + DELETE scenario.
- `tools/bench/fk-insert-delete.ts` — N parent/child inserts then
  cascade deletes; opt-in `--with-pg`.
- `package.json` adds `bench:fk`.
- `docs/k3s-uat.md` gains a Sprint 9 paragraph.
- Commit `test+bench+docs: pgwire fk delete parity and bench seed`.

## Final Verification

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm run check:ts
npm run check:no-python
npm run check:manifests
npm test
npm run bench:fk -- --rows 100
```
