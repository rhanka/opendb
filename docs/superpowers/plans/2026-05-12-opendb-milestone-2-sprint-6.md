# OpenDB Milestone 2 Sprint 6 Implementation Plan

Date: 2026-05-12
Sprint: Milestone 2 Sprint 6 — first sentropic-path sprint
Workers: 1-2

## Source Spec

`docs/superpowers/specs/2026-05-12-opendb-milestone-2-sprint-6-design.md`.

## File Structure

- `crates/opendb-storage/src/commit_stream.rs` — `Value`, `ColumnType`,
  `ColumnDefinition` extensions; `DefaultExpr`.
- `crates/opendb-storage/src/row_projection.rs` — apply `nullable` /
  `default` semantics.
- `crates/opendb-storage/tests/fixtures/wal/frame-v1-record-v2-typed-defaults.hex`
  — new golden fixture.
- `crates/opendb-sql/src/parser.rs` — new type tokens, `NOT NULL`,
  `DEFAULT`, named-column INSERT.
- `crates/opendb-sql/src/ast.rs` — `Insert` variant + types.
- `crates/opendb-sql/src/executor.rs` — apply defaults + type checking.
- `crates/opendb-node/src/pgwire.rs` — OIDs / text-mode encoding for
  new types.
- `tests/parity/sql-smoke.test.ts` — round-trip pgwire test.

## Task 1: Storage Types And Column Metadata

**Files:**
- `crates/opendb-storage/src/commit_stream.rs`
- `crates/opendb-storage/src/row_projection.rs`

- [ ] Add to `Value`: `Bool(bool)`, `Float64(f64)`, `Timestamp(i64)`,
  `Null`. Use `#[serde(rename_all = "snake_case")]` consistently.
- [ ] Add to `ColumnType`: `Bool`, `Float64`, `Timestamp`.
- [ ] Extend `ColumnDefinition`:
  ```rust
  pub struct ColumnDefinition {
      pub name: String,
      pub data_type: ColumnType,
      pub is_primary_key: bool,
      #[serde(default = "default_true")]
      pub nullable: bool,
      #[serde(default, skip_serializing_if = "Option::is_none")]
      pub default: Option<DefaultExpr>,
  }
  ```
  with `fn default_true() -> bool { true }` and:
  ```rust
  pub enum DefaultExpr {
      Const(Value),
      Now,
  }
  ```
- [ ] Update `ColumnDefinition::primary_key(name, ty)` to set
  `nullable=false`.
- [ ] Add `ColumnDefinition::with_default(self, expr)`.
- [ ] Add `Value::is_null()` helper.
- [ ] `RowProjection::apply` keeps current semantics (rows stay strict).
- [ ] Tests:
  - Round-trip `ColumnDefinition` with default through serde.
  - `Value::Bool(true)` / `Float64(3.14)` / `Timestamp(0)` /
    `Null` round-trip.
- [ ] Commit `feat: extend value and column types with bool float timestamp default`.

## Task 2: WAL Compatibility

**Files:**
- `crates/opendb-storage/tests/wal_golden.rs`
- `crates/opendb-storage/tests/fixtures/wal/frame-v1-record-v2-typed-defaults.hex`
- `crates/opendb-storage/src/wal.rs` (test additions only)

- [ ] Add WAL tests:
  - `wal_appends_and_reads_typed_column_definition_record` (CreateTable
    with bool/float/timestamp + DEFAULT NOW()).
  - `wal_rejects_unknown_field_in_default_expr` (negative).
- [ ] Generate the golden fixture hex via a temporary `panic!`-print
  test, then remove the temp test.
- [ ] Add a golden assertion in `wal_golden.rs` that decodes the new
  fixture and confirms `ColumnDefinition.default == Some(Now)`.
- [ ] Commit `test: cover typed defaults wal compatibility`.

## Task 3: SQL Parser

**Files:**
- `crates/opendb-sql/src/parser.rs`
- `crates/opendb-sql/src/ast.rs`

- [ ] Extend the tokenizer / parser to accept type tokens:
  `INT`, `INTEGER`, `BIGINT`, `TEXT`, `BOOL`, `BOOLEAN`, `FLOAT8`,
  `FLOAT64`, `DOUBLE PRECISION`, `TIMESTAMP`.
- [ ] Accept `NOT NULL` after the type token.
- [ ] Accept `DEFAULT <literal>` and `DEFAULT NOW()` after the type
  (and after `NOT NULL` if present).
- [ ] Accept named-column INSERT form
  `INSERT INTO t (a, b) VALUES (1, 'x')`.
- [ ] Change `Statement::Insert` to carry an optional `columns:
  Option<Vec<String>>` field.
- [ ] Parser tests:
  - `CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL,
    completed BOOL DEFAULT false, created_at TIMESTAMP NOT NULL DEFAULT
    NOW())` parses to expected AST.
  - `INSERT INTO t (id, name) VALUES (1, 'Ada')` parses with the named
    columns.
  - Negative: `DEFAULT NOW()` on non-`TIMESTAMP` column is rejected at
    executor time, not parser time (we keep the parser permissive).
- [ ] Commit `feat: parse extended column types and named insert columns`.

## Task 4: Executor And Defaults

**Files:**
- `crates/opendb-sql/src/executor.rs`

- [ ] In `prepare_insert`:
  - When `columns` is `None`, keep positional behavior.
  - When `columns` is `Some(names)`:
    - Validate every name exists in the table and no duplicates.
    - Build a value map; for every missing column, apply its
      `DefaultExpr`:
      - `Const(value)` → use it as-is.
      - `Now` → require `ColumnType::Timestamp`, substitute the next
        `LogicalTimestamp` as microseconds.
    - If a missing column has neither `default` nor is `nullable`,
      reject with `OpenDbError::InvalidInput`.
- [ ] Extend `value_matches_type` for `Bool`, `Float64`, `Timestamp`.
- [ ] `Value::Null` matches a `nullable` column for any
  `ColumnType`.
- [ ] Tests:
  - Named INSERT applies `DEFAULT 'completed'`.
  - Named INSERT applies `DEFAULT NOW()` → row has a `Timestamp`
    value equal to the engine's `next_tx` timestamp.
  - Named INSERT missing a `NOT NULL` column without `DEFAULT` is
    rejected.
  - Named INSERT with an unknown column is rejected.
  - INSERT with a `null` literal into a `NOT NULL` column is rejected.
- [ ] Commit `feat: apply column defaults and named insert columns`.

## Task 5: pgwire OIDs And Text-Mode Encoding

**Files:**
- `crates/opendb-node/src/pgwire.rs`

- [ ] Map `ColumnType` to PG OIDs in `describe`:
  - `Int64` → 23 (INT4) — kept for Drizzle compat (Drizzle declares
    `integer()` and reads INT4); test confirms.
  - `Text` → 25 (TEXT).
  - `Bool` → 16.
  - `Float64` → 701 (FLOAT8).
  - `Timestamp` → 1114 (TIMESTAMP).
- [ ] Text-mode DataRow encoding:
  - `Bool(true)` → `"t"`, `Bool(false)` → `"f"`.
  - `Float64(v)` → Rust `{:?}` format, then strip trailing zero (matches
    PG enough for Drizzle).
  - `Timestamp(us)` → `YYYY-MM-DD HH:MM:SS.uuuuuu` (no timezone).
  - `Null` → `0xFFFFFFFF` length prefix (PG null sentinel).
- [ ] Parse parameter values back from the wire for the same types
  (`BindParam`).
- [ ] Tests:
  - Encode/decode each type round-trip.
  - `Null` round-trip in both directions.
- [ ] Commit `feat: serialize bool float timestamp over pgwire text mode`.

## Task 6: TS Parity And Manifests

**Files:**
- `tests/parity/sql-smoke.test.ts`

- [ ] Extend the existing pgwire parity test:
  - `CREATE TABLE typed_smoke (id INT PRIMARY KEY, name TEXT NOT NULL,
    completed BOOL DEFAULT false, ratio FLOAT8, created_at TIMESTAMP NOT
    NULL DEFAULT NOW())`.
  - `INSERT INTO typed_smoke (id, name, ratio) VALUES (1, 'Ada', 0.5)`
    — verify defaults filled in on read.
  - `SELECT * FROM typed_smoke WHERE id = 1` — verify the row shape.
- [ ] No manifest change in this sprint; `tests/cluster/manifests.test.ts`
  must still pass unchanged.
- [ ] Commit `test: pgwire parity for extended column types and defaults`.

## Task 7: Documentation

**Files:**
- `docs/k3s-uat.md`

- [ ] Append a paragraph under the existing Sprint 5 note:
  > Sprint 6 adds typed columns (`BOOL`, `FLOAT8`, `TIMESTAMP`),
  > `NOT NULL`, `DEFAULT <literal>`, `DEFAULT NOW()`, and the
  > named-column `INSERT` form. `DEFAULT NOW()` is resolved to the
  > engine's monotonic `LogicalTimestamp`, not wall-clock time.
- [ ] Commit `docs: note sprint 6 extended type surface`.

## Final Verification

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm run check:ts
npm run check:no-python
npm run check:manifests
npm test
npm run smoke:k3s
npm run smoke:k3s -- --with-restart-recovery
npm run smoke:k3s -- --with-range-split
```

All green expected. Per-commit grep:

```bash
git log origin/main --grep="anthropic\|claude\|🤖" -i --oneline | wc -l
```

Must not increase on top of the pre-existing baseline (1, from
`c39522d` — to be addressed separately).

## Review Checklist

- [ ] Commit stream remains the only source of truth.
- [ ] `ColumnDefinition` reads old fixtures unchanged.
- [ ] `DEFAULT NOW()` uses `LogicalTimestamp`, not wall-clock.
- [ ] pgwire OIDs match Drizzle/`pg` expectations for the new types.
- [ ] `npm run smoke:k3s` default stays non-destructive.
- [ ] No Python file introduced.
- [ ] Commit messages contain no AI attribution.
