# OpenDB Milestone 2 Sprint 6 Design

Status: draft (2026-05-12)
Author: opendb maintainers

## Goal

Sprint 6 is the first sprint of the sentropic-substitution path. It extends
the SQL type surface to cover the columns sentropic actually uses in its 28
Drizzle migrations: boolean, timestamp, float64 (a small minority — int,
text are already done), plus the `NOT NULL`, `DEFAULT`, and `DEFAULT
now()` modifiers, and the named-column form of `INSERT INTO t (a, b)
VALUES (…)`. After this sprint, OpenDB can ingest enough of an sentropic
schema to host a single read-mostly table end to end.

Out of scope: `JSONB` (Sprint 7), `ALTER TABLE` (Sprint 8), `UNIQUE`/FK
(Sprint 9), joins (Sprint 10), transactions (Sprint 11).

## Context

After Sprint 5, OpenDB supports:

- `CREATE TABLE t (id INT PRIMARY KEY, name TEXT)` — `Int64` and `Text`
  columns only, primary key on a single column, no nullability, no
  defaults.
- `INSERT INTO t VALUES (1, 'Ada')` — positional values only, every
  declared column must appear.
- `SELECT *` and `SELECT * WHERE pk = …`.
- Active range catalog with runtime split/merge through the admin
  endpoint, and a `RangeCatalogStable` condition.

Sentropic's actual schema uses (counts in the schema audit): 394 `text`,
104 `timestamp`, 75 `jsonb`, 21 `integer`, 12 `boolean`, plus
`defaultNow()` on most timestamps and `default('completed')`-style
literal defaults on a few text columns. The largest tables also use
`integer` for ordinals where 64-bit isn't necessary. We don't need any
PG extension.

## Non-Goals

- No `JSONB` type or operators (reserved for Sprint 7).
- No `INTEGER`/`BIGINT` distinction beyond what's needed to map Drizzle's
  `integer()` to `INT`. We map both `INTEGER` and `INT` to `Int64`. The
  pgwire serialization keeps INT4 OID semantics for compatibility.
- No `NUMERIC`, no `DECIMAL`, no `DATE`, no `TIME`, no `TIMESTAMPTZ`.
  Only `TIMESTAMP WITHOUT TIME ZONE` (matching what sentropic's schema
  emits) and as an alias `TIMESTAMP`.
- No `DEFAULT` for arbitrary expressions. We support:
  - `DEFAULT <literal>` (`'completed'`, `0`, `false`, `null`).
  - `DEFAULT NOW()` on `TIMESTAMP` columns (resolved at insert time to
    the current `LogicalTimestamp`).
- No `CHECK`, no `UNIQUE`, no `REFERENCES`, no `ON DELETE`.
- No `RETURNING` clause (Drizzle uses it; we accept it without erroring,
  but reject anything beyond returning the primary key — covered in
  Sprint 7 along with JSONB inserts).

## Design Summary

Five surfaces, all keyed on existing types:

1. **`Value` and `ColumnType` extension** (`opendb-storage`):
   - `Value::Bool(bool)`, `Value::Float64(f64)`, `Value::Timestamp(i64)`
     (microseconds since 1970-01-01, no timezone), `Value::Null`.
   - `ColumnType::{Bool, Float64, Timestamp}`.
   - Serialization stays serde-driven (`deny_unknown_fields` + golden
     fixture additions).

2. **`ColumnDefinition` extension** (`opendb-storage`):
   - Add `nullable: bool` (default `true` to keep existing schemas valid).
   - Add `default: Option<DefaultExpr>`.
   - `DefaultExpr = Const(Value) | Now`.
   - WAL frame format version stays `2`. The new fields are
     `#[serde(default)]` on read to keep old fixtures valid; new writes
     always emit them.

3. **Parser** (`opendb-sql`):
   - Accept `BOOL` / `BOOLEAN`, `FLOAT8` / `DOUBLE PRECISION` /
     `FLOAT64` (we standardize on `FLOAT64` internally), `TIMESTAMP`,
     `INTEGER` (alias of `INT`).
   - Accept `NOT NULL` after the type token.
   - Accept `DEFAULT <literal>` or `DEFAULT NOW()` after the type or
     `NOT NULL` token.
   - Accept the named-column INSERT form
     `INSERT INTO t (a, b) VALUES (1, 'x')`.

4. **Executor** (`opendb-sql`):
   - Maintain backward compat: unnamed INSERT keeps positional behavior.
   - For named INSERT: validate the column list against the schema; for
     each missing column, apply its default (or `NULL` if nullable
     without default; reject if `NOT NULL` without default).
   - For `DEFAULT NOW()`: replace by the engine's monotonic
     `LogicalTimestamp` at prepare time (so the same row replays
     identically). Document this in the design — there is no "system
     clock" notion in OpenDB.
   - Type checking: extend `value_matches_type` for the three new
     types, reject mismatches.

5. **pgwire** (`opendb-node/pgwire.rs`):
   - On the wire, send the new types as text-mode by default
     (DataRow/text format) using PG OIDs:
     - `BOOL` → 16
     - `FLOAT8` → 701
     - `TIMESTAMP` → 1114 (formatted `YYYY-MM-DD HH:MM:SS.uuuuuu`)
   - Accept the same OIDs on bind for the parameter side. Drizzle
     already issues prepared statements in text mode for non-blob
     fields when configured against `pg`, so this matches.

## Recovery / Replay

No commit stream semantic change. `CommitRecord::VERSION` stays `2`.
Existing WAL fixtures still decode because every new field on
`ColumnDefinition` is `#[serde(default)]`. The new mutation variants are
unchanged — only `Mutation::InsertRow` carries new `Value` enum
variants, and the existing serde format already handles enum extension
under `deny_unknown_fields` because new variants are additions, not
renames.

We do add a new golden WAL fixture
(`frame-v1-record-v2-typed-defaults.hex`) covering a record with a
boolean, a float, and a timestamp value, plus a `NOT NULL DEFAULT now()`
column descriptor.

## SQL Surface (informal grammar deltas)

```text
column_def ::= identifier type_token nullability? default?
type_token ::= "INT" | "INTEGER" | "BIGINT" | "TEXT" | "BOOL" | "BOOLEAN"
             | "FLOAT8" | "FLOAT64" | "DOUBLE" "PRECISION" | "TIMESTAMP"
nullability ::= "NOT" "NULL"
default ::= "DEFAULT" ( literal | "NOW" "(" ")" )
literal ::= "NULL" | int | float | text | "TRUE" | "FALSE"

insert_stmt ::= "INSERT" "INTO" identifier column_list? "VALUES" value_list
column_list ::= "(" identifier ( "," identifier )* ")"
value_list ::= "(" literal ( "," literal )* ")"
```

`BIGINT` is an alias of `INT` (both map to `Int64`).

## Test Strategy

- `opendb-storage`: unit tests on `Value`/`ColumnType` round-tripping
  through serde + WAL fixture for typed defaults.
- `opendb-sql`: parser tests for each new type, `NOT NULL`, each
  `DEFAULT` form, and the named-column INSERT. Executor tests for:
  - INSERT missing optional column → default applied.
  - INSERT missing required column → error.
  - INSERT extra column → error.
  - INSERT with mismatched type → error.
  - `DEFAULT NOW()` → row's `LogicalTimestamp` lands in the value.
- `opendb-node`: pgwire serializer/parser round-trip for booleans,
  doubles, timestamps over text mode.
- `tests/parity`: vitest case that issues a multi-typed INSERT through
  `pg` and reads back via the Drizzle-style text format.

## pgwire Boundary

Unchanged in scope: still a compatibility layer only. We do not expose
the new types via the admin endpoint.

## Review Points

- `ColumnDefinition` retains backward-compatible read.
- `DEFAULT NOW()` semantics documented: not wall-clock, the engine's
  monotonic timestamp.
- pgwire text-mode formatting matches PG's `YYYY-MM-DD HH:MM:SS.uuuuuu`
  for timestamps.
- No new mutation type added — Sprint 6 stays inside the existing commit
  stream surface.
- No Python file introduced anywhere.
- Commit messages strip every AI attribution.

## Acceptance Criteria

- `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test
  --workspace` green.
- `npm run check:ts`, `npm run check:no-python`, `npm run
  check:manifests`, `npm test` green.
- `npm run smoke:k3s` default still non-destructive.
- `npm run smoke:k3s -- --with-restart-recovery` still passes.
- `npm run smoke:k3s -- --with-range-split` still passes.
- `git log origin/main --grep="anthropic\|claude\|🤖" -i --oneline | wc -l`
  retourne `0` après chaque commit Sprint 6 (les faux positifs
  pré-existants restent hors scope).
