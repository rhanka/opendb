# OpenDB Milestone 2 Sprint 7 Design — JSONB

Status: draft (2026-05-12)
Author: opendb maintainers

## Goal

Sprint 7 adds the `JSONB` type to OpenDB. Entropiq has 75 `jsonb` columns
in its 50-table schema; without JSONB the substitution pilot is blocked.
This sprint targets storage + serde round-trip + parser literals + pgwire
emit/parse, sufficient to ingest and emit JSONB blobs verbatim. JSON
operators (`->`, `->>`, `@>`) are not in scope; entropiq does not use
them in its TypeScript source — JSONB is read raw and parsed in JS.

A first-bench fixture for JSONB INSERT/SELECT throughput lands under
`tools/bench/jsonb.ts` (one file, no harness yet — that comes in Sprint
12.5). The fixture is a vitest-runnable script that records numbers for
later regression.

## Context

After Sprint 6, OpenDB supports `INT/INTEGER/BIGINT/TEXT/BOOL/FLOAT8/
DOUBLE PRECISION/TIMESTAMP`, `NOT NULL`, `DEFAULT <literal>`, `DEFAULT
NOW()`, named-column INSERT. pgwire emits the right OIDs, including a
text-mode fallback for empty result sets.

JSONB is the last big scalar gap in entropiq's schema. Drizzle declares
`jsonb('data').default(sql\`'{}'::jsonb\`)` and reads it as
`Record<string, unknown>`. The TypeScript layer never issues `data->>'key'`
queries — it parses the entire blob in Node.

## Non-Goals

- No JSON operator: `->`, `->>`, `@>`, `?`, `#>`, `jsonb_set`,
  `jsonb_build_object`, `jsonb_agg`, etc. Reserved for later if a
  consumer needs them.
- No path expressions, no GIN index, no JSON path query.
- No distinction between `JSON` and `JSONB` semantics — we normalize
  every literal to JSONB internally and emit OID 3802.
- No size-limit policy on JSON blobs in this sprint; the existing WAL
  frame cap (`MAX_FRAME_LEN`) is the de-facto limit.
- No streaming for big blobs; the entire value is in-memory.

## Design Summary

1. **Storage** (`opendb-storage`):
   - `Value::Json(serde_json::Value)`.
   - `ColumnType::Json` (the canonical name internally; both `JSON` and
     `JSONB` parser tokens map to it).
   - WAL serialization: nested under the existing serde envelope, no
     frame format change.

2. **Parser** (`opendb-sql`):
   - Accept `JSON`, `JSONB` as type tokens.
   - Accept JSON literals: `'{"k":"v"}'`, `'[]'`, `'null'` (the JSON
     literal `null`, distinct from SQL `NULL`), and the explicit cast
     suffix `'…'::jsonb` / `'…'::json` (we strip the cast).
   - The parser does **not** try to detect JSON shape from arbitrary
     literals; the column type drives interpretation. A bare `'{"k":"v"}'`
     remains a `Value::Text` until the executor coerces it against a
     `ColumnType::Json` column. That keeps the `'a=b'` text predicate
     test from regressing.
   - For INSERT into a JSON column, the executor coerces `Value::Text`
     to `Value::Json` by parsing the text as JSON; failure → typed
     error.

3. **Executor** (`opendb-sql`):
   - Extend `coerce_value` and `value_matches_type` for `Json`.
   - Coercion `Text → Json` parses with `serde_json::from_str` and
     returns `OpenDbError::InvalidInput` on parse failure.
   - `Value::Null` matches a nullable `Json` column.
   - `DEFAULT '{}'` works because the executor coerces the constant
     literal at default application time (already on the existing
     `default_value_for_column` path — we just route through
     `coerce_value`).

4. **pgwire** (`opendb-node`):
   - OID 3802 (JSONB).
   - Text-mode encoding: `serde_json::to_string(&value)`.
   - Heuristic OID derivation in `resolve_row_description_types` learns
     `Value::Json` → `ColumnType::Json`.

5. **Bench seed** (`tools/bench/jsonb.ts`):
   - Spawns one local opendb-node + (optionally) one local PostgreSQL
     container via `docker compose -f tools/bench/postgres.yml up`.
   - PostgreSQL is **opt-in**: the script skips the PG comparison if the
     compose file isn't running, so CI doesn't require docker. The
     opt-in flag `--with-pg` is the user-facing trigger; the default
     just exercises OpenDB and reports its own throughput.
   - Records to stdout in a stable JSON shape so a future Sprint 12.5
     harness can collate runs.

## Test Strategy

- `opendb-storage`: `Value::Json` round-trips through serde + a WAL
  fixture covering an INSERT with a JSON value.
- `opendb-sql`: parser tests for `JSON` and `JSONB` type tokens;
  parser tests for `'…'::jsonb` cast stripping; executor tests for
  text-to-json coercion (success, failure), DEFAULT `'{}'` on a
  `JSON NOT NULL` column.
- `opendb-node`: pgwire OID test (JSON column gets 3802); text-mode
  encoding round-trip.
- `tests/parity`: extend the typed-smoke pgwire test to insert a JSON
  blob, select it back, and assert the JSON deserializes to the same
  object.
- `tools/bench/jsonb.ts`: smoke-runs as a TypeScript script (no vitest
  wrapper in this sprint to keep it lightweight; vitest harness lands in
  Sprint 12.5).

## pgwire Boundary

Unchanged in scope. JSON is just another scalar; no protocol-level
changes.

## Recovery / Replay

No commit stream version change. `Value::Json` is just a new enum
variant under `deny_unknown_fields`; old WAL fixtures remain valid
because they don't reference it.

## Review Points

- `Value::Json` lives next to existing scalars; no separate JSONB
  subsystem.
- Coercion `Text → Json` is one place (`coerce_value`), not scattered.
- pgwire emits OID 3802 for JSONB columns, even on empty result sets
  when the column type is known via the heuristic.
- Bench script is opt-in PG comparison and never required for CI.
- No Python file introduced anywhere.
- Commit messages contain no AI attribution.

## Acceptance Criteria

- `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test
  --workspace` green.
- `npm run check:ts`, `npm run check:no-python`, `npm run check:manifests`,
  `npm test` green.
- `npm run smoke:k3s` default still non-destructive.
- The bench script runs end-to-end against a fresh opendb-node and emits
  a JSON report to stdout.
- New `git log origin/main` Sprint 7 commits do not increase the
  pre-existing AI-attribution baseline (still 1, from `c39522d`).
