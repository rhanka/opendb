# OpenDB Milestone 2 Sprint 10 Implementation Plan — JOINs / ORDER BY / LIMIT

Date: 2026-05-12

## Source Spec

`docs/superpowers/specs/2026-05-12-opendb-milestone-2-sprint-10-design.md`.

## Tasks

### Task 1 — AST + Parser

- `ast.rs`: declare `Statement::Select`, `SelectSource`, `JoinKind`,
  `JoinPredicate`, `OrderBy`, `OrderDirection`.
- `parser.rs`: detect JOIN/ORDER BY/LIMIT/OFFSET. Fall back to the
  existing `SelectAll` when none are present. Parse aliases.
- Tests cover each clause and the alias form.
- Commit `feat: parse joins, order by, limit`.

### Task 2 — Executor

- `executor.rs`: `Statement::Select` path. Build the materialized join
  via nested loop, apply WHERE (joined-column equality + literal
  equality), ORDER BY (stable), LIMIT, OFFSET. Project columns.
- Tests cover INNER, LEFT, NULL padding, ORDER BY ASC/DESC, LIMIT,
  OFFSET, combined.
- Commit `feat: execute joins, order by, limit`.

### Task 3 — Parity TS + Bench seed + Docs

- `pgwire-smoke.ts`: scenario joining two tables, ordering by name,
  limit 1.
- `tools/bench/join.ts`: nested-loop join bench with `--with-pg`
  toggle.
- `package.json` adds `bench:join`.
- `docs/k3s-uat.md`: paragraph for Sprint 10.
- Commit `test+bench+docs: join order limit parity and bench seed`.

## Final Verification

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm run check:ts
npm run check:no-python
npm run check:manifests
npm test
npm run bench:join -- --rows 100
```
