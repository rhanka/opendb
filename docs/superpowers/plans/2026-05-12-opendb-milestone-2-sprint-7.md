# OpenDB Milestone 2 Sprint 7 Implementation Plan — JSONB

Date: 2026-05-12
Sprint: Milestone 2 Sprint 7
Workers: 1-2

## Source Spec

`docs/superpowers/specs/2026-05-12-opendb-milestone-2-sprint-7-design.md`.

## File Structure

- `crates/opendb-storage/src/commit_stream.rs` — `Value::Json`,
  `ColumnType::Json`.
- `crates/opendb-storage/Cargo.toml` — `serde_json.workspace = true`
  (already present transitively).
- `crates/opendb-storage/tests/fixtures/wal/frame-v1-record-v2-jsonb-insert.hex`
  — golden fixture.
- `crates/opendb-sql/src/parser.rs` — `JSON`, `JSONB` type tokens; cast
  suffix stripping.
- `crates/opendb-sql/src/executor.rs` — coerce `Text → Json`; extend
  `value_matches_type`; `default_value_for_column` Coalesces.
- `crates/opendb-node/src/pgwire.rs` — OID 3802; encoding;
  `resolve_row_description_types` covers `Value::Json`.
- `tools/pgwire-smoke.ts` — typed smoke gains a JSON column.
- `tools/bench/jsonb.ts` — first bench fixture (TypeScript only).

## Task 1: Storage Json Variant

**Files:**
- `crates/opendb-storage/src/commit_stream.rs`
- `crates/opendb-storage/src/row_projection.rs`

- [ ] Add `Value::Json(serde_json::Value)`. Update derives where needed
  (`PartialEq` already on `serde_json::Value`).
- [ ] Add `ColumnType::Json`.
- [ ] Update `value_to_key` (debug-only path), `value_matches_type` to
  cover Json.
- [ ] Tests: round-trip `Value::Json(serde_json::json!({...}))` through
  serde; serialize then deserialize; assert object equality.
- [ ] Commit `feat: add jsonb value and column type`.

## Task 2: WAL Fixture

**Files:**
- `crates/opendb-storage/src/wal.rs`
- `crates/opendb-storage/tests/wal_golden.rs`
- `crates/opendb-storage/tests/fixtures/wal/frame-v1-record-v2-jsonb-insert.hex`

- [ ] Add WAL test `wal_appends_and_reads_jsonb_insert_record`.
- [ ] Generate fixture via temporary print test (panic-and-strip).
- [ ] Add `wal_reads_frame_v1_record_v2_jsonb_insert_fixture` in
  `wal_golden.rs`.
- [ ] Commit `test: cover jsonb wal compatibility`.

## Task 3: SQL Parser JSON Tokens

**Files:**
- `crates/opendb-sql/src/parser.rs`

- [ ] Map `JSON` and `JSONB` to `ColumnType::Json` in
  `parse_column_type_tokens`.
- [ ] In `parse_value`, accept the optional cast suffix `::jsonb` and
  `::json` and strip it before further parsing (string literal stays a
  `Value::Text`; coercion happens in the executor).
- [ ] Tests: `CREATE TABLE t (id INT PRIMARY KEY, data JSONB DEFAULT
  '{}'::jsonb)` parses to expected AST.
- [ ] Commit `feat: parse jsonb type and cast literal`.

## Task 4: Executor Coercion

**Files:**
- `crates/opendb-sql/src/executor.rs`

- [ ] Extend `coerce_value` with arm `(Value::Text(s), ColumnType::Json)`
  → parse `serde_json::from_str(&s)`; on Ok wrap in `Value::Json`; on
  Err return original (downstream `value_matches_type` will reject).
- [ ] Tests: INSERT a `'{"k":"v"}'` literal into a `JSONB` column →
  stored as `Value::Json`; SELECT it back → JSON object.
- [ ] DEFAULT `'{}'::jsonb` coerces to `Value::Json(serde_json::json!({}))`.
- [ ] Negative: invalid JSON literal into JSONB column → `InvalidInput`.
- [ ] Commit `feat: coerce text literals into jsonb values`.

## Task 5: pgwire JSONB

**Files:**
- `crates/opendb-node/src/pgwire.rs`

- [ ] Map `ColumnType::Json` → OID 3802 in `oid_for_column_type`.
- [ ] Extend `value_to_text` for `Value::Json` →
  `serde_json::to_string(&v).unwrap_or("null")`.
- [ ] Extend `resolve_row_description_types` heuristic to learn
  `Value::Json` → `ColumnType::Json`.
- [ ] Commit `feat: serialize jsonb over pgwire text mode`.

## Task 6: Parity TS

**Files:**
- `tools/pgwire-smoke.ts`

- [ ] Extend the existing `typed_smoke_*` block with a `JSONB` column
  and an INSERT/SELECT round-trip; assert the SELECT row contains the
  serialized JSON string.
- [ ] Commit `test: pgwire parity for jsonb roundtrip`.

## Task 7: Bench Seed

**Files:**
- `tools/bench/jsonb.ts` (new)
- `package.json` (npm script `bench:jsonb`)

- [ ] Spawn a fresh `opendb-node` (mirroring the parity helper) on a
  random local pgwire port.
- [ ] Run N (default 1000) INSERTs of a 1 KB JSON blob via `pg`
  text-protocol; record wall-clock per op.
- [ ] Run M (default 1000) SELECTs by primary key; record per-op time.
- [ ] Print a single JSON line: `{ "engine": "opendb", "insert_p50_ms":
  …, "insert_p95_ms": …, "select_p50_ms": …, "select_p95_ms": …,
  "rows": N }`.
- [ ] Optional `--with-pg`: same workload against a local PostgreSQL
  reachable via `PGHOST`/`PGPORT` env vars (no docker compose required;
  just connect strings). Skip silently if vars unset.
- [ ] Add `"bench:jsonb": "tsx tools/bench/jsonb.ts"`.
- [ ] Commit `bench: seed jsonb throughput script`.

## Task 8: Documentation

**Files:**
- `docs/k3s-uat.md`

- [ ] Append a paragraph: `Sprint 7 adds JSONB columns. Use `JSON` or
  `JSONB` as a type token; default literals like `'{}'::jsonb` are
  stripped of their cast suffix and stored as parsed JSON. The pgwire
  RowDescription emits OID 3802. JSON operators (`->`, `->>`, `@>`)
  are out of scope until a consumer requires them.`
- [ ] Commit `docs: note sprint 7 jsonb surface`.

## Final Verification

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm run check:ts
npm run check:no-python
npm run check:manifests
npm test
npm run bench:jsonb -- --rows 200
```

All green expected.

## Review Checklist

- [ ] No new mutation type, only a new `Value` variant + `ColumnType`.
- [ ] Coercion lives in `coerce_value`, not scattered.
- [ ] pgwire emits OID 3802 for JSONB.
- [ ] Bench script is one self-contained TypeScript file.
- [ ] No Python introduced.
- [ ] Commit messages contain no AI attribution.
