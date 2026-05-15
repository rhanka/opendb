# OpenDB Milestone 2 Sprint 11 Design — Transactions (no-op skeleton)

Status: draft (2026-05-12)

## Goal

Drizzle emits `BEGIN` / `COMMIT` / `ROLLBACK` whenever the user runs
`db.transaction(async (tx) => …)`. Sprint 11 makes opendb-node accept
those statements and answer with the right command tags so a Drizzle
client does not error out. Mutations issued between BEGIN and COMMIT
are still applied immediately to the canonical commit stream (no
isolation, no rollback semantics).

True snapshot isolation + rollback semantics land in Sprint 11.5 once
we have a transactional buffer wired in front of the commit stream;
splitting the work here keeps the drumbeat short while unblocking
sentropic workloads that wrap small read-mostly batches in a
`db.transaction(...)` purely for connection grouping.

## Non-Goals (Sprint 11)

- No real snapshot isolation. Reads in a transaction see the latest
  committed data, same as outside a transaction.
- No rollback semantics. `ROLLBACK` succeeds and emits the right tag;
  any mutations already applied since `BEGIN` are NOT reversed.
- No `SAVEPOINT`, `RELEASE SAVEPOINT`, `ROLLBACK TO SAVEPOINT`.
- No transaction isolation levels (`READ COMMITTED`, etc.).

## Design

Parser:

```sql
BEGIN [TRANSACTION]
START TRANSACTION
COMMIT [TRANSACTION]
END
ROLLBACK [TRANSACTION]
ABORT
```

AST:

```rust
Statement::Begin
Statement::Commit
Statement::Rollback
```

Executor returns:

- `BEGIN` → command tag `"BEGIN"`
- `COMMIT` → `"COMMIT"`
- `ROLLBACK` → `"ROLLBACK"`

No commit-stream record is created for any of the three. Sprint 11.5
will replace this with a buffered transaction body that fans out to a
multi-mutation `CommitRecord` at COMMIT time.

## Test Strategy

- Parser: each of the six forms parses to the expected AST variant.
- Executor: a BEGIN/INSERT/COMMIT sequence applies the INSERT and emits
  the right tags.
- Parity: pgwire smoke runs `BEGIN; INSERT; COMMIT;` and `BEGIN; INSERT;
  ROLLBACK;` and checks the tag stream.

## Acceptance

Standard verifications green.
