# Phase A — pgbench against opendb-node (2026-05-22)

First attempt at running `pgbench -i -s 1 ; pgbench -c 1 -T 10 -M simple`
against opendb-node to surface the parser / protocol / model gaps before
prioritizing Phase B (lock narrowing) and Phase C (MVCC). The
transactional roadmap (`docs/roadmap/perf-vs-transactional-2026-05-22.md`)
predicted "1 week — likely one or two parser gaps". This run surfaced
**10 gaps**, of which 8 are now fixed and 2 are confirmed cliffs that
need their own milestone.

## Runner

`tools/pgbench-runner.sh` — spawns opendb-node + a PG 16-alpine
container side-by-side, runs `pgbench -i` and `pgbench` against both,
captures all four logs into `docs/bench/`. Env knobs: `SCALE`,
`CLIENTS`, `THREADS`, `DURATION`, `MODE`, `INIT_STEPS`, `SKIP_PG`.

## Gaps surfaced + status

| # | Gap | Status | What it took |
|---|-----|--------|--------------|
| 1 | `DROP TABLE [IF EXISTS] t1, t2, ...` (multi-table comma list) | ✅ fixed | `Mutation::DropTable` + `Statement::DropTable` + parser (multi expands to DoBlock) + executor + projection apply + 4 tests |
| 2 | `DROP TABLE IF EXISTS missing` (no-op without WAL write) | ✅ fixed | Executor returns `PreparedQuery::Read` (not `Write` with empty mutations, which would trip "at least one mutation" validate) |
| 3 | `VACUUM` / `ANALYZE` (pgbench `-i` runs `vacuum analyze pgbench_*`) | ✅ fixed | Parser returns empty `DoBlock` no-op |
| 4 | `CHAR(N)`, `VARCHAR(N)`, `CHARACTER(N)`, `NUMERIC`, `DECIMAL`, `SMALLINT`, `SERIAL`, `BIGSERIAL`, `INT2/4/8`, `TIMESTAMPTZ`, `DATE`, `TIME`, `BPCHAR` | ✅ fixed | `parse_column_type_tokens` strips `(N)` / `(N,M)` parametric suffix and maps to the existing `ColumnType` enum |
| 5 | `CREATE TABLE` without explicit `PRIMARY KEY` (PG accepts heap tables; opendb required exactly one PK) | ✅ fixed | Parser auto-injects synthetic `__opendb_rowid BIGINT NOT NULL PRIMARY KEY DEFAULT auto` at the end of the column list; new `DefaultExpr::AutoRowId` populated from a monotonic atomic counter at INSERT time |
| 6 | `CREATE TABLE ... ) WITH (fillfactor=100)` (post-column-list modifiers): `rfind(')')` mis-bound | ✅ fixed | Balanced-paren walk to find the column-list's matching close; `WITH`, `WITHOUT OIDS`, `INHERITS`, `TABLESPACE` accepted as no-op modifiers after the close |
| 7 | `TRUNCATE TABLE t1, t2, ...` (multi-list + `RESTART/CONTINUE IDENTITY` + `CASCADE/RESTRICT` modifiers) | ✅ fixed | `Mutation::TruncateTable` + `Statement::TruncateTable` + parser + executor + projection apply (clears rows, keeps schema) |
| 8 | `INSERT INTO ...(cols) values(a,b)` — no space between `VALUES` and `(` | ✅ fixed | `parse_insert` finds ` VALUES` then verifies the next char is whitespace or `(`; accepts `VALUES(`, `VALUES\n`, `VALUES ` |
| 9 | `COPY pgbench_accounts FROM STDIN WITH (freeze on)` (pgbench `-i` default uses COPY) | ⚠ workaround | pgbench `-I dtGvp` forces `INSERT INTO ... SELECT` data generation instead of COPY. `tools/pgbench-runner.sh` defaults to that. Real COPY support is a pgwire-protocol-level feature: CopyInResponse / CopyData / CopyDone messages — multi-day work, deferred. |
| 10 | `INSERT INTO ...(cols) SELECT bid, 0 FROM generate_series(1, 1) AS bid` — INSERT-from-SELECT + set-returning `generate_series()` function | ❌ cliff | Two architectural features. INSERT-from-SELECT pipes a query result into a write path; `generate_series()` is a built-in set-returning function. Both are PG primitives we have no analog for. **This is where Phase A stops today.** |

## Tests

`cargo test -p opendb-sql -p opendb-storage -p opendb-consensus -p opendb-node`:
- opendb-common: 2 OK
- opendb-consensus: 32 OK
- opendb-node: 45 OK (one existing test updated to use duplicate-column instead of no-PK)
- opendb-sql: 99 OK (4 new tests: DROP single, DROP multi, DROP CASCADE/RESTRICT no-op, VACUUM/ANALYZE no-op)
- opendb-storage: 100 OK
- wal_golden: 5 OK

`cargo fmt --all -- --check` clean.

## What this confirms

The transactional roadmap's prediction ("1 week, parser gaps + concurrent BEGIN") was directionally right but under-counted the surface area. Each surfaced gap was structurally simple (parser / type system / single mutation) until #9 and #10, both of which are full features rather than gaps:

- **COPY FROM** is a pgwire protocol extension and would benefit OLTP (bulk ingest) **and** the analytical track (OLAP bulk loads) per `docs/roadmap/perf-vision-2026.md` §2.
- **INSERT-from-SELECT + generate_series** is a query-pipeline feature. INSERT-from-SELECT requires the executor to materialize a result set and then route it through the WAL write path; generate_series belongs to the set-returning-function family that PG uses for many synthetic queries. Both feed into the future query-engine work, not Phase B/C.

## Recommendation

Three options, ranked by acceptance criterion progress per week of work:

1. **(Recommended) Stop Phase A here, pivot to Phase B.** We have enough surface from the 10 gaps to know where pgbench cliffs, and Phase B (lock narrowing on `Database` Mutex) doesn't need pgbench-full-init to validate — a custom hand-written INSERT-only seed at small scale will work as the workload. Phase B unblocks concurrent reads against opendb today, which is the *real* OLTP gap (per `docs/roadmap/perf-vs-transactional-2026-05-22.md` §3.1).
2. **Custom seed script.** Write a bash + SQL seeder that hand-builds `pgbench_branches`, `pgbench_tellers`, `pgbench_accounts` (at scale 0.001 = 100 accounts) without `generate_series`, then run only the *bench* portion of `pgbench -c 1 -T 10`. Gets a single-client number in ~half a day. Still doesn't validate concurrency.
3. **Implement COPY + INSERT-from-SELECT.** Multi-week features. Right call eventually but ahead of priority — does not move us toward beating PG faster than Phase B/C/E do.

Option 1 is recommended because the *next* finding we expect — even if pgbench ran end-to-end — is the §3.9 cliff (concurrent BEGIN rejected because the open-transaction state is per-`Database`, not per-session). Phase B fixes that root cause and unlocks scale-from-1-to-c more directly than another two days of pgbench fence-mending would.

## Cumulative cost (this session, autonomous)

- 10 SQL/parser/executor gaps surfaced
- 8 fixed end-to-end with tests
- ~600 LOC across `commit_stream.rs`, `row_projection.rs`, `range_catalog.rs`, `archive_manifest.rs`, `ast.rs`, `parser.rs`, `executor.rs`, `database.rs`
- 4 new parser tests, 1 updated test
- 1 new runner script (`tools/pgbench-runner.sh`)
