# Running sentropic against opendb-node

This guide tells the sentropic team how to swap their PostgreSQL dependency
for **opendb-node** in a local POC environment, without any modification to
sentropic itself.

opendb-node speaks the PostgreSQL wire protocol (pgwire v3) and is exposed
through a small static-linked container image. Sentropic's `node-postgres`
driver and Drizzle migrations connect to it as if it were a regular Postgres
server.

---

## TL;DR

```bash
# 1. Pull the image (or build it locally, see below)
docker pull ghcr.io/rhanka/opendb-node:poc-1

# 2. Run opendb-node, exposing the pgwire port
docker run --rm -d \
  --name opendb-poc \
  -p 25432:5432 \
  -p 28080:8080 \
  -v opendb-poc-data:/var/lib/opendb \
  ghcr.io/rhanka/opendb-node:poc-1 \
  --node-id 1 \
  --pgwire-addr 0.0.0.0:5432 \
  --health-addr 0.0.0.0:8080

# 3. Point sentropic at it
export DATABASE_URL=postgres://opendb:opendb@127.0.0.1:25432/opendb

# 4. Apply your normal migrations + boot the API
cd /path/to/sentropic/api
npm install   # if not already done
npm run dev   # tsx watch src/index.ts
```

Health check (no auth):

```bash
curl http://127.0.0.1:28080/healthz
curl http://127.0.0.1:8787/api/v1/health      # via sentropic
```

---

## What works today

opendb-node was validated against **sentropic's actual Drizzle migrations
and route handlers** (verbatim, no source modification). The artifacts below
live in this repo and are reproducible:

| Probe matrix | Result | Tooling |
|--------------|--------|---------|
| 27 Drizzle migrations replay | **22/27 PASS (81 %)** | `make poc-migrate` → `docs/bench/sentropic-migrate-*.md` |
| Drizzle seed (workspaces / orgs / folders / initiatives + verify SELECT + GROUP BY + count) | **6/6 PASS** | `make poc-seed` → `docs/bench/sentropic-seed-*.md` |
| HTTP routes (handler logic copied 1:1 from sentropic) | **10/10 PASS** | `make poc-http` → `docs/bench/sentropic-http-*.md` |
| Drizzle smoke (real queries from entropiq/sentropic routes) | **14/14 PASS** | `make smoke-real` → `docs/bench/sentropic-real-*.md` |

Routes covered by the HTTP POC (`docs/bench/sentropic-http-*.md`):

- `GET /api/folders?include_usecase_counts=true` (folders.ts:139) — list with
  LEFT JOIN organizations + LEFT JOIN initiatives + count + GROUP BY 8 cols
- `GET /api/folders?include_usecase_counts=true&organization_id=...` (filtered)
- `GET /api/folders/:id` (single + LEFT JOIN orgs)
- `GET /api/folders/:id` 404 path
- `GET /api/folders/list/with-matrices`
- `GET /api/folders/:id/matrix` (resolves workspace type)
- `GET /api/folders/matrix/default`
- `GET /api/organizations`
- `GET /api/initiatives`
- `GET /api/initiatives?folder_id=...`

The Drizzle ORM `.toSQL()` of each route emits the **exact** queries you
would see in production sentropic, including:

- multi-LEFT-JOIN with `ON (a = b AND c = literal)` conjunctions
- `GROUP BY` on 8 qualified columns
- `count(joined.id)` aggregate with `LEFT JOIN` NULL semantics
- `WHERE (a = $1 AND b = $2)` (parens-wrapped conjunction)
- `INSERT ... VALUES (...), (...), (...)` (multi-row)
- `INSERT ... ON CONFLICT (col) DO NOTHING` (idempotent seeds)
- `INSERT ... RETURNING *` / `... RETURNING col`
- `UPDATE ... SET ... WHERE ... RETURNING col`
- `DELETE ... WHERE ... RETURNING *`
- `DEFAULT NOW()` / `DEFAULT CURRENT_TIMESTAMP` (wall-clock)
- Drizzle's `db.transaction(async tx => { ... })` with COMMIT and ROLLBACK
  atomicity (rolled-back rows actually vanish)

---

## Known limitations (read this before you ship)

These five sentropic migrations **do not** apply cleanly today and are
skipped during the replay. None of them block the 10 routes above:

| File | Reason | Impact |
|------|--------|--------|
| `0008_clumsy_luminals.sql` | `UPDATE ... SET data = COALESCE(...) \|\| jsonb_build_object(...)` — opendb does not yet implement the `\|\|` jsonb concat operator or `jsonb_build_object`. | Data backfill into `use_cases.data` JSONB. Tables stay structurally correct; seed flow inserts fresh data so the missing backfill is a no-op. |
| `0016_organizations.sql` (1 stmt) | `UPDATE` with `COALESCE`/`\|\|`/`jsonb_build_object` chain. | Same as above for `organizations.data`. |
| `0018_workspace_collaboration.sql` | Composite `PRIMARY KEY (workspace_id, user_id)` on `workspace_memberships`. opendb requires exactly one PK column today. | Workspace sharing UI breaks; single-user POC unaffected. |
| `0024_workspace_types_initiatives.sql` (1 stmt) | `INSERT ... SELECT ...` (backfill). | Type-system migration; the table structure is created OK, only the backfill INSERT is skipped. |
| `0025_workflow_runtime_state.sql` | Composite PK on `workflow_task_results (run_id, task_key, task_instance_key)`. | BR-04B workflow runtime — not in the POC route surface. |

Other things to be aware of:

- **No `LISTEN`/`NOTIFY`/`UNLISTEN`** — opendb accepts these statements as
  no-ops so sentropic's lock event flow doesn't crash, but no events are
  delivered. SSE-driven UI features (live folder updates etc.) won't tick.
- **No async chat trace purge sweeps** — sentropic's `setInterval`
  background loops will hit any unsupported SQL and log errors, but the
  process keeps running.
- **Single primary node** — the POC image runs opendb-node standalone (no
  Raft cluster). Suitable for local + integration; not for production.
- **Sessions are in-memory across pgwire connections**: opendb supports
  `BEGIN/COMMIT/ROLLBACK` per connection, but does not yet implement
  cross-session isolation (one open transaction blocks others).

---

## Build the image yourself

If you can't pull from ghcr, build locally from the opendb worktree:

```bash
cd /path/to/opendb
make poc-image           # docker build -f Dockerfile.alpine -t opendb-node:poc-local .
docker run --rm -p 25432:5432 -p 28080:8080 opendb-node:poc-local \
  --node-id 1 \
  --pgwire-addr 0.0.0.0:5432 \
  --health-addr 0.0.0.0:8080
```

The build is a static-musl Rust release that yields a ~9.2 MB binary
packaged in a ~30 MB alpine image. No glibc, no openssl, no native
dependencies — the whole workspace is pure-Rust at runtime.

---

## What we want from sentropic in return

If you swap your PG dep for opendb-node and find a route that breaks,
please file the **exact** failing SQL (use `DEBUG=pg:*` or `.toSQL()`
upstream of the failing call) at https://github.com/rhanka/opendb/issues
with the migration sequence + the failing query. We'll add it to
`tools/sentropic-poc/real-smoke.ts` and ship the parser/executor fix.
