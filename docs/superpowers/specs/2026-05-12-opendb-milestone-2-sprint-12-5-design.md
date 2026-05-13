# OpenDB Milestone 2 Sprint 12.5 Design — Bench harness

Status: draft (2026-05-12)

## Goal

Consolidate the four bench seeds (`jsonb`, `alter`, `fk`, plus a new
`join`) into a unified runner that emits a Markdown report comparing
OpenDB vs PostgreSQL (when `PGHOST`/`PGPORT` are set), and stamps the
result under `docs/bench/`.

## Design

- New `tools/bench/runner.ts`: discovers the four bench scripts, runs
  each one with the same `--rows` value, and collects the JSON
  summaries.
- Output:
  - JSON file under `docs/bench/run-YYYY-MM-DD.json` (one per run).
  - Markdown table under `docs/bench/run-YYYY-MM-DD.md` summarising
    p50 / p95 per engine per workload.
- `npm run bench:all -- --rows 200` triggers the runner.

## Non-Goals

- No CI integration (manual run only).
- No regression budget enforcement.
- No graphing — Markdown table only.

## Acceptance

- `npm run bench:all -- --rows 50 --with-pg` produces a JSON + MD
  report.
- Existing `bench:jsonb` / `bench:alter` / `bench:fk` scripts keep
  working standalone.
