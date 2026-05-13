# OpenDB Milestone 2 Sprint 8 Design — ALTER TABLE / CREATE INDEX / DO $$

Status: draft (2026-05-12)

## Goal

Make `drizzle-kit generate` migrations apply unchanged. Entropiq's 28
migrations rely on `ALTER TABLE ADD COLUMN`, `ALTER TABLE ADD CONSTRAINT
… FOREIGN KEY … REFERENCES …`, `CREATE INDEX IF NOT EXISTS … USING btree
(…)`, and `DO $$ BEGIN … EXCEPTION WHEN duplicate_object THEN null;
END $$;` idempotence blocks. Sprint 8 adds the typed mutations and
parser routes so each statement is accepted, persisted, and replayable.

Foreign-key enforcement (deletes / inserts) is Sprint 9's job. Sprint 8
records the constraint metadata; it does not run the check. Same idea
for `CREATE INDEX`: we record the index for replay, but no acceleration
path is wired (a full-table scan remains the only read path).

## Non-Goals

- No foreign-key validation at INSERT/DELETE time (Sprint 9).
- No physical secondary-index data structure (Sprint 10+).
- No `ALTER TABLE … ALTER COLUMN SET DEFAULT/DROP DEFAULT` variants;
  entropiq does not use them.
- No `ALTER TABLE … VALIDATE CONSTRAINT`.

## Design Summary

1. **Storage** (`opendb-storage`):
   - New `Mutation::AlterTable { table: String, op: AlterTableOp }`.
   - `AlterTableOp` variants: `AddColumn(ColumnDefinition)`,
     `DropColumn(String)`, `RenameColumn { from: String, to: String }`,
     `AddConstraint { name: String, kind: ConstraintKind }`,
     `AddIndex { name: String, columns: Vec<String>, unique: bool,
     if_not_exists: bool }`.
   - `ConstraintKind` variants: `ForeignKey { columns: Vec<String>,
     references_table: String, references_columns: Vec<String>,
     on_delete: ReferentialAction, on_update: ReferentialAction }`,
     `Unique { columns: Vec<String> }`. We add an enum
     `ReferentialAction::{NoAction, Cascade, SetNull, SetDefault,
     Restrict}` — `NoAction` is the default.

2. **`RowProjection`** (`opendb-storage`):
   - Apply `AlterTable` against the in-memory schema:
     - `AddColumn`: push the descriptor onto the table; backfill
       existing rows with `Value::Null` or the column default.
     - `DropColumn`: remove the descriptor; drop the value from every
       existing row.
     - `RenameColumn`: update the descriptor name; rewrite the row maps.
     - `AddConstraint`: record under a new `Table.constraints` slot,
       no semantic enforcement.
     - `AddIndex`: record under `Table.indexes`, no semantic
       acceleration.
   - Reject inconsistent operations (drop unknown column, rename to
     existing name, add column with duplicate name).

3. **Parser** (`opendb-sql`):
   - `ALTER TABLE <ident> ADD COLUMN <name> <type> [NOT NULL]
     [DEFAULT …]`.
   - `ALTER TABLE <ident> DROP COLUMN <name>`.
   - `ALTER TABLE <ident> RENAME COLUMN <name> TO <name>`.
   - `ALTER TABLE <ident> ADD CONSTRAINT <name> FOREIGN KEY (<cols>)
     REFERENCES <table> (<cols>) [ON DELETE <action>] [ON UPDATE
     <action>]`.
   - `ALTER TABLE <ident> ADD CONSTRAINT <name> UNIQUE (<cols>)`.
   - `CREATE [UNIQUE] INDEX [IF NOT EXISTS] <name> ON <table> [USING
     btree] (<cols>)`.
   - `DO $$ BEGIN … END $$;` accepted as a "block statement"; the
     parser splits on `BEGIN`/`END` boundaries and runs each inner
     statement, swallowing `EXCEPTION` clauses and `duplicate_object`
     errors. Sprint 8 implements a minimal pattern matcher: anything
     inside the block that we know how to parse is forwarded; anything
     we cannot parse is rejected with a clear error.

4. **Executor** (`opendb-sql`):
   - `Statement::AlterTable { table, op }` → builds the corresponding
     `Mutation::AlterTable` and stamps the route on `RangeId::ROOT`.
   - `Statement::CreateIndex { … }` is also routed through
     `Mutation::AlterTable { op: AddIndex }`.
   - `Statement::DoBlock { inner }` runs each inner statement; if a
     statement returns `OpenDbError::InvalidInput` that mentions
     "already exists" / "duplicate" and the DO block has the
     `EXCEPTION WHEN duplicate_object` clause, swallow the error and
     return success.
   - Command tags: `"ALTER TABLE"`, `"CREATE INDEX"`, `"DO"`.

5. **pgwire** (`opendb-node`):
   - Command-complete tags follow the executor.
   - No OID or RowDescription change.

## Test Strategy

- `opendb-storage`: `RowProjection` tests on each `AlterTableOp`.
- `opendb-storage`: WAL fixture for an `AlterTable` record.
- `opendb-sql`: parser tests for each statement form, including the
  `DO $$` block with `EXCEPTION WHEN duplicate_object` (variant must
  parse without raising).
- `opendb-sql`: executor tests applying ALTER TABLE + querying the
  updated table; CREATE INDEX records the index in
  `RowProjection::indexes`; DO block idempotence.
- `tests/parity`: extend `pgwire-smoke.ts` with an ALTER TABLE ADD
  COLUMN + CREATE INDEX scenario and verify the command tags arrive in
  the right order.

## Bench

Extend `tools/bench/` with `alter-then-select.ts`: spawn opendb-node,
run N migrations (`ALTER TABLE … ADD COLUMN …`), then M selects, and
report timing. Same opt-in `--with-pg` toggle. Lands in the same
sprint to keep the bench surface growing.

## Review Points

- `Mutation::AlterTable` is one new mutation, no schema-system overhaul.
- Foreign keys are recorded but not enforced; the spec calls this out.
- `DO $$ BEGIN … END $$;` is best-effort; unknown statements inside
  still error.
- pgwire command tags match PostgreSQL.
- No Python introduced.
- Commit messages contain no AI attribution.

## Acceptance Criteria

- `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test
  --workspace` green.
- `npm run check:ts`, `npm run check:no-python`, `npm run check:manifests`,
  `npm test` green.
- A full entropiq Drizzle migration file (`0017_context_documents.sql`
  for the canary test) applies without error against opendb-node.
- New `git log origin/main` commits do not increase the AI-attribution
  baseline.
