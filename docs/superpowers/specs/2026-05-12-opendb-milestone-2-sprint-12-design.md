# OpenDB Milestone 2 Sprint 12 Design — pgwire Extended Protocol

Status: **active prerequisite for Sprint 13** (promoted 2026-05-13).
The Sprint 13 smoke (`tools/sentropic-poc/smoke.ts`,
`docs/bench/sentropic-poc-2026-05-13.md`) showed every Drizzle query
path goes through Extended (`Parse`/`Bind`/`Describe`/`Execute`/`Sync`)
and opendb-node rejects each tag with `unsupported message tag`.
Without Sprint 12, no Drizzle read can complete.

## Goal (when reactivated)

Implement the pgwire Extended Query protocol: Parse, Bind, Describe,
Execute, Sync, Close. This lets Drizzle send `INSERT INTO t (a, b)
VALUES ($1, $2)` with bind parameters instead of inlining them.

## Sketch

- `pgwire::handle_extended_message`: receive `P` (Parse) → store
  SQL + param OIDs.
- `B` (Bind) → substitute params into the SQL textually before
  reusing the simple-query executor. (Real implementation would
  binary-bind, but text substitution is enough for Sprint 12 minimum.)
- `D` (Describe) → re-run the prepared statement against an empty
  table (or use type inference from the parse) to return the
  RowDescription / ParameterDescription.
- `E` (Execute) → call the existing execute pipeline.
- `S` (Sync) → emit `ReadyForQuery`.

Sprint 12 keeps protocol scope minimal: text-mode params, no binary,
no portal management beyond one anonymous portal at a time.

## Non-Goals

- No binary parameter encoding.
- No multiple portals.
- No `CopyIn` / `CopyOut`.

## Notes

If Sprint 13 reveals Drizzle requires Extended at all, this spec
becomes Sprint 12; otherwise it stays parked and Sprint 12.5 starts
right after Sprint 11.
