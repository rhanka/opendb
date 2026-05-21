# Write-path audit — 2026-05-18

Trace of one `INSERT` from pgwire ingress to durable WAL flush, with `file:path:line` citations. Source paths are relative to `/home/antoinefa/src/opendb/.worktrees/feat-milestone-1/`.

Context: bench POC (500 inserts, workspaces/orgs/folders/initiatives) currently 4.4s vs Postgres 0.1s (~44x). Reads are sub-ms post `2026-05-18 C` (`docs/bench/sentropic-bench-2026-05-18.md:32`). Bench runs against a single standalone node — `RootRangeProposalPath::Local(Standalone)` — so the OpenRaft branch is dead code on the hot path.

## 1. pgwire ingress

- TCP accept: `crates/opendb-node/src/pgwire.rs:33` — `pub async fn serve(...)` spawns one task per accepted connection.
- Per-connection loop + `TCP_NODELAY`: `crates/opendb-node/src/pgwire.rs:50-99` — `handle_connection` reads tagged frames in a tight loop.
- Simple-query branch: `crates/opendb-node/src/pgwire.rs:79-82` — `b'Q'` extracts SQL via `cstring_payload(&payload)` then calls `execute_simple_query`.
- Simple-query dispatcher: `crates/opendb-node/src/pgwire.rs:374-388` — `execute_simple_query` (1) parses, (2) takes `database.lock().await` (line 381), (3) calls `database.execute(statement)`, (4) sends framed response.
- Response framing (batched in one `write_all`): `crates/opendb-node/src/pgwire.rs:662-690` — `write_query_result` builds the whole `RowDescription`+`DataRow*`+`CommandComplete`+`ReadyForQuery` buffer, then one `stream.write_all(&buffer).await` (line 688). This is the recent perf optim — do not flag.

## 2. SQL parse

- Crate boundary: `crates/opendb-sql/src/parser.rs` — pgwire imports via `crates/opendb-node/src/pgwire.rs:4` (`use opendb_sql::{ast::QueryResult, parser::parse};`).
- Entry: `crates/opendb-sql/src/parser.rs:13` — `pub fn parse(sql: &str) -> OpenDbResult<Statement>`. Synchronous, string-based; first strips comments (`strip_sql_comments`, line 18), uppercases to keyword-sniff, then dispatches.
- `INSERT INTO` dispatch: `crates/opendb-sql/src/parser.rs:66-67` → `parse_insert(normalized)`.
- DoBlock handling for multi-statement: `crates/opendb-sql/src/parser.rs:24-27, 57-60`.
- Called once per simple query inside `execute_simple_query` BEFORE the `database.lock().await` (line 379) — i.e. parse is not under the DB lock, but it is on the request critical path.

## 3. Engine prepare / plan

- `Database::execute` entry: `crates/opendb-node/src/database.rs:169-171` — async wrapper boxing into `execute_with_refresh`.
- Refresh gate (recent perf optim — skips when wal_len unchanged): `crates/opendb-node/src/database.rs:196-198, 532-538`.
- DoBlock loop (recent: passes `refresh_before_execute=false` to inner statements): `crates/opendb-node/src/database.rs:200-221`.
- Engine plan call: `crates/opendb-node/src/database.rs:227` — `self.engine.prepare(statement)?`. Synchronous.
- Prepare impl: `crates/opendb-sql/src/executor.rs:80-310` — `pub fn prepare(...)`. For `Statement::Insert` it (line 87-148) clones `table_state.columns`, materializes values, and emits a `PreparedQuery::Write { record, ... }` containing a fully-built `CommitRecord` and a `RouteIntent::Key { key, .. }`.
- Route resolution: `crates/opendb-node/src/database.rs:239` → `resolve_route` at `crates/opendb-node/src/database.rs:518-527` — looks up `range_catalog.route_key(key)` (in-memory BTreeMap-ish; cheap).

## 4. Submit to consensus

- Submit site (non-transactional path — bench POC autocommit): `crates/opendb-node/src/database.rs:248-256`.
  ```
  self.root_range.submit(RootRangeCommand { record: record.clone() }).await?;
  self.last_replayed_wal_len = self.root_range.wal_byte_len().await?;   // extra fs::metadata round-trip per write
  self.engine.apply_committed(record)?;
  ```
- `RootRange::submit`: `crates/opendb-consensus/src/root_range.rs:274-302`. Branches on `proposal_path`. Bench POC uses `RootRangeProposalPath::Local(Standalone)` → falls through to `self.apply_committed(&command.record).await` at line 291.
- `apply_committed`: `crates/opendb-consensus/src/root_range.rs:251-259` does, per row, in order:
  1. `validate_apply_record` (cheap version check) — line 252.
  2. `_guard = self.semantic_append_lock.lock().await` — line 253 (Mutex, async).
  3. `validate_semantic_append(record)` — line 254 → `crates/opendb-consensus/src/root_range.rs:401-421` which calls `semantic_snapshot_for_append` (line 371-389). The cache is keyed on `wal_len`; cache HIT path still does a `self.wal.byte_len().await` (`fs::metadata`) plus three `Mutex::lock().await` ops on the cache.
  4. `wal.append_with_len(record).await` — line 255 (see §5).
  5. `commit_semantic_append_snapshot(...)` — line 256/391-399 (re-locks cache mutex, stores new snapshot).
- Synchronous vs deferred: in the autocommit path everything above is synchronous before `submit` returns; `engine.apply_committed` then runs sync after (see §7). Transaction path defers WAL append until COMMIT (`crates/opendb-node/src/database.rs:286-312`) but Drizzle/seed runs autocommit one INSERT at a time.

## 5. WAL append

- Call site (single): `crates/opendb-consensus/src/root_range.rs:255` — exactly one `wal.append_with_len(record).await` per `apply_committed`. There is **no batching at this layer**: each INSERT row = one record = one `append_with_len`.
- Implementation: `crates/opendb-storage/src/wal.rs:33-74` — `pub async fn append_with_len`.
  - Line 34: `_guard = self.append_lock.lock().await` (per-path async Mutex; see §8).
  - Line 37-39: `fs::create_dir_all(&parent)` — every single call.
  - Line 41-44: `fs::try_exists(&self.path)` — every call (extra stat).
  - Line 46: `durable_prefix_len(&self.path).await` (`crates/opendb-storage/src/wal.rs:228-236`) — **reads and JSON-decodes the entire WAL file from disk every append**, just to find the torn-frame truncation offset.
  - Line 47-53: opens the file fresh (`OpenOptions::open(...).await`) per call.
  - Line 54: `encode_frame(record)` — `serde_json::to_vec` of the full record (`crates/opendb-storage/src/wal.rs:120-141`).
  - Line 56-58: `file.set_len(append_offset)` — truncate-to-prefix (no-op when there's no torn tail, but still a syscall).
  - Line 59-61: `seek(SeekFrom::Start(append_offset))`.
  - Line 62-64: `write_all(&frame)`.
  - Line 65-67: `file.sync_data().await` — fsync of the WAL file (see §6).
  - Line 69-71: directory `sync_all` only the first time the file is created (line 69 `if !existed_before`).

## 6. WAL fsync / durability

- Per-record fsync: `crates/opendb-storage/src/wal.rs:65-67` — `file.sync_data().await` runs **once per `append_with_len`**, i.e. **once per INSERT row**, both in autocommit and in transaction COMMIT (the COMMIT loop at `crates/opendb-node/src/database.rs:294-307` calls `submit` per record). There is no per-batch / per-transaction / per-DoBlock fsync coalescing.
- No group-commit primitive exists in the WAL — `grep -n "group_commit\|coalesce\|batch" crates/opendb-storage/src/wal.rs` returns nothing relevant.
- Raft-side state file fsync: `crates/opendb-consensus/src/raft.rs:1013` (`file.sync_data()` in `write_json_file_atomic`) + `crates/opendb-consensus/src/raft.rs:1046` (directory `sync_all`). Called inside `persist` (`crates/opendb-consensus/src/raft.rs:387-407`) on every `append_to_log` and every `apply_to_state_machine` entry — but this path is dormant in the standalone bench (`proposal_path` is `Local`).

## 7. Engine apply

- Apply site: `crates/opendb-node/src/database.rs:255` — `self.engine.apply_committed(record)?` runs synchronously **after** the WAL fsync returns, on the same task.
- Engine impl: `crates/opendb-sql/src/executor.rs:503-508` — `self.projection.apply(&record)?` plus push into `self.commits`. Pure in-memory.
- Note duplicate work: `prepare_write_with_returning` (`crates/opendb-sql/src/executor.rs:536-545`) **already applied** the record to a *cloned* `validated_projection` during `prepare`, so the post-WAL `apply_committed` re-applies the same mutations to the real `projection`. Not a perf bug for B-tree-size inserts but is double-work per row.

## 8. Locks / barriers on the hot insert path

In autocommit order, every INSERT acquires:

1. `database.lock().await` — `crates/opendb-node/src/pgwire.rs:381` (per simple query). Held for the entire parse-plan-WAL-fsync-apply path. Single connection bench => uncontended, but it serializes everything.
2. `RootRange.semantic_append_lock` — `crates/opendb-consensus/src/root_range.rs:253` (Mutex<()>; declared `crates/opendb-consensus/src/root_range.rs:81`). Held for the whole `apply_committed`. Uncontended in bench.
3. `RootRange.semantic_append_cache` — `crates/opendb-consensus/src/root_range.rs:374` and `:386` (read for hit-check, then re-lock for store), and again at `:397` in `commit_semantic_append_snapshot`. Three lock-unlock cycles per row on the cache mutex.
4. `Wal.append_lock` — `crates/opendb-storage/src/wal.rs:34` (per-path Mutex from `WAL_APPEND_LOCKS` registry at `crates/opendb-storage/src/wal.rs:100-112`). Held across the entire `read-decode-write-fsync` cycle. Single writer => uncontended but blocks any reader that tries to use `byte_len` concurrently — though `byte_len` itself uses `fs::metadata` so does not take this lock.

No `RwLock` on the hot insert path. No `tokio::sync::Notify`/`Semaphore` coordination — meaning no group-commit.

## Candidate instrumentation points (ranked by likely cost)

1. **`durable_prefix_len`** (`crates/opendb-storage/src/wal.rs:228-236`, called from `crates/opendb-storage/src/wal.rs:46`) — reads + JSON-decodes the **entire WAL** before every single append. After 500 inserts the WAL is ~500 records, so each append re-decodes O(N) records → O(N²) total. This is almost certainly the dominant single cost. Wrap with `Instant::now()` checkpoint `wal_durable_prefix_scan` and log `(records_scanned, bytes, elapsed)`.
2. **`Wal::append_with_len`** outer timing (`crates/opendb-storage/src/wal.rs:33`) — wraps the whole append including `sync_data`. Split into 4 sub-checkpoints: `prefix_scan`, `open+seek`, `write_all`, `sync_data`. The `sync_data` slice will isolate filesystem fsync latency vs. logical work.
3. **`tokio::fs::File::sync_data`** at `crates/opendb-storage/src/wal.rs:65` — per-row fsync. Combined with #1, on ext4 + non-NVMe this alone explains a sizable chunk; if the SSD is fast, #1 dominates.
4. **`semantic_snapshot_for_append`** (`crates/opendb-consensus/src/root_range.rs:371-389`) — even on cache HIT this hits `wal.byte_len` (a `fs::metadata` syscall, `crates/opendb-storage/src/wal.rs:91-97`) and three Mutex acquisitions. Measure `byte_len_metadata_elapsed` vs `cache_lock_elapsed` to confirm the cache is paying off.
5. **`validate_semantic_append`** (`crates/opendb-consensus/src/root_range.rs:401-421`) — clones the snapshot then applies `projection`, `range_catalog`, `archive_manifest` per row. Even with the cache, that's three `.apply(record)` calls on cloned structures per row. Wrap each `.apply` separately.
6. **`SqlEngine::prepare`** for `Statement::Insert` (`crates/opendb-sql/src/executor.rs:87-148`) — `prepare_write_with_returning` clones the projection and applies the mutation a *first* time (line 545). Confirms double application.
7. **`Database::execute_with_refresh` outer span** (`crates/opendb-node/src/database.rs:173-264`) — coarse total per query; the difference between this and `wal.append_with_len` is "everything else" (parse + plan + lock acquisition).
8. **`execute_simple_query` outer span** (`crates/opendb-node/src/pgwire.rs:374-388`) — includes pgwire framing on either side; the gap to `Database::execute` is pgwire transport.
9. **`Database.last_replayed_wal_len = self.root_range.wal_byte_len()`** at `crates/opendb-node/src/database.rs:254` — an extra `fs::metadata` syscall after every write (purely to update the read-refresh skip cache). Likely cheap individually but is per-row.
10. **`encode_frame` / `serde_json::to_vec`** at `crates/opendb-storage/src/wal.rs:121` — JSON-encoding of each record. With wide row payloads (workspaces/orgs/folders have a few text columns) this is non-trivial. Same applies to the decode path in `durable_prefix_len`.

## Most surprising finding

The semantic-append cache (the headline 2026-05-18-A optim) skips the high-level `replay()` rebuild — but `Wal::append_with_len` at `crates/opendb-storage/src/wal.rs:46` still calls `durable_prefix_len`, which **reads the entire WAL file from disk and JSON-decodes every record** on every single append, just to compute the torn-frame truncation offset. That is O(N) bytes + JSON parsing per INSERT (so O(N²) over the 500-row seed) and lives entirely *below* the semantic cache, so the cache cannot mask it. The cache turned the semantic layer into ~O(1) per write but the WAL layer is still effectively quadratic; that's the most likely reason the bench plateaued at 4.4s instead of approaching Postgres's 0.1s after the recent optimizations.
