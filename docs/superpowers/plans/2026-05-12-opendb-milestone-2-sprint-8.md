# OpenDB Milestone 2 Sprint 8 Implementation Plan — ALTER TABLE / CREATE INDEX / DO $$

Date: 2026-05-12
Sprint: Milestone 2 Sprint 8

## Source Spec

`docs/superpowers/specs/2026-05-12-opendb-milestone-2-sprint-8-design.md`.

## Tasks

### Task 1 — Storage AlterTable / ConstraintKind / ReferentialAction

- `crates/opendb-storage/src/commit_stream.rs`: add `Mutation::AlterTable`,
  `AlterTableOp`, `ConstraintKind`, `ReferentialAction`, `IndexDescriptor`.
- Update `value_to_key` / `value_matches_type` paths if needed.
- Tests: round-trip each variant through serde.
- Commit `feat: add alter table mutation surface`.

### Task 2 — RowProjection ALTER application

- `crates/opendb-storage/src/row_projection.rs`: apply each
  `AlterTableOp` and reject inconsistent forms.
- `Table` gains `constraints: Vec<NamedConstraint>` and
  `indexes: Vec<IndexDescriptor>` slots.
- Tests: ADD COLUMN backfills with Null/Default; DROP COLUMN removes
  values; RENAME COLUMN updates schema and row maps;
  AddConstraint/AddIndex record metadata.
- Commit `feat: apply alter table mutations to row projection`.

### Task 3 — WAL fixture

- Add `wal_appends_and_reads_alter_table_record` test + golden hex
  fixture.
- Commit `test: cover alter table wal compatibility`.

### Task 4 — SQL parser

- `crates/opendb-sql/src/parser.rs`: recognize the new statement forms,
  emit new `Statement` variants `AlterTable`, `CreateIndex`, `DoBlock`.
- `crates/opendb-sql/src/ast.rs`: declare the new variants.
- Tests cover each surface form (parser only; executor is Task 5).
- Commit `feat: parse alter table create index and do blocks`.

### Task 5 — Executor

- `crates/opendb-sql/src/executor.rs`: dispatch the new statements;
  command tags `ALTER TABLE`, `CREATE INDEX`, `DO`. `DoBlock` runs each
  inner statement and swallows `duplicate_object`-style errors when the
  surrounding block carries an `EXCEPTION WHEN duplicate_object` clause.
- Tests: ADD COLUMN then INSERT covers the new column; CREATE INDEX
  records the index; DO block idempotence; FK constraint metadata
  recorded but not enforced.
- Commit `feat: apply alter table and do blocks in executor`.

### Task 6 — pgwire + parity

- pgwire tags align (`ALTER TABLE`, `CREATE INDEX`, `DO`).
- Extend `tools/pgwire-smoke.ts` with an ALTER TABLE + CREATE INDEX
  scenario.
- Commit `test: pgwire parity for alter and create index`.

### Task 7 — Bench seed + docs

- `tools/bench/alter-then-select.ts` — N alters + M selects with
  optional `--with-pg`.
- `package.json` adds `"bench:alter": "tsx tools/bench/alter-then-select.ts"`.
- `docs/k3s-uat.md`: paragraph for Sprint 8.
- Commit `bench+docs: alter-then-select seed and sprint 8 notes`.

## Final Verification

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm run check:ts
npm run check:no-python
npm run check:manifests
npm test
npm run bench:alter -- --rows 200
```

All green expected.
