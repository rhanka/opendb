# OpenDB vs PostgreSQL — transactional perf roadmap (2026-05-22)

Self-contained research note. Companion: `docs/roadmap/perf-vs-analytical-2026-05-22.md` (sibling doc on OLAP).

Question: what is the minimum architectural work required for OpenDB (Rust, pgwire, in-memory row store with WAL durability) to beat PostgreSQL on transactional workloads at non-trivial concurrency?

Today's reference point (sentropic POC seed, single client, autocommit, 500 rows, post 2026-05-20 `durable_prefix_len` O(N²)→O(1) cache fix): OpenDB 1.99 s vs PG 0.087 s ≈ 23× slower on writes; reads sub-ms ≥ PG parity. We have no concurrent-client write benchmark yet — that gap is itself the headline finding (§5).

---

## 1. PostgreSQL transactional architecture — what makes it fast under concurrency?

PG's transactional throughput is not the speed of any single operation; it's the orchestration of several specialized processes around a shared buffer pool. The pieces that matter for OLTP:

### 1.1 MVCC + lock manager
- **MVCC** via row tuple versions stored inline in heap pages (`xmin`/`xmax`/`cmin`/`cmax`). Readers never block writers and writers never block readers — they each pick the visible version under their snapshot (`pg_snapshot`). This is the single biggest concurrency win and the one OpenDB is furthest from (§3.3).
- **Row-level locks** are tuple-level write locks, held only in shared memory tables (`PROCLOCK`/`LOCK`). Lock-mode lattice: `FOR UPDATE` / `FOR NO KEY UPDATE` / `FOR SHARE` / `FOR KEY SHARE`. A reader needing the latest visible version doesn't go through the lock manager at all.
- **Predicate locks (SSI)** for SERIALIZABLE — only matter once you offer SERIALIZABLE isolation. PG's default is READ COMMITTED; most OLTP runs READ COMMITTED. OpenDB does not need SSI in phase 1.
- **Deadlock detector**: periodic scan of the lock wait graph. Costly but rarely on the hot path.

### 1.2 WAL writer + group commit
- A dedicated `walwriter` process flushes WAL buffers periodically (`wal_writer_delay`, default 200 ms) and on commit. Client backends append to in-memory WAL buffers; only one backend per commit group performs the `fdatasync`.
- **Group commit** (`commit_delay`, `commit_siblings`) coalesces fsyncs across concurrent committers. Under load this is the single most important reason PG scales: per-commit fsync amortizes to ~1 syscall per N committers.
- `synchronous_commit=off` trades durability for throughput; `synchronous_commit=local` is the usual high-perf production setting.

### 1.3 Background workers (the "stuff that isn't on the user's path")
- `bgwriter`: flushes dirty buffers to keep clean pages available.
- `checkpointer`: periodic checkpoint of dirty buffers + WAL recycling; smoothed over `checkpoint_completion_target`.
- `autovacuum` (multiple workers): reclaims dead tuples + updates `pg_statistics` for the planner. Key for steady-state OLTP — MVCC creates dead tuples on every UPDATE.
- `walwriter`, `archiver`, `logical replication workers`, `parallel workers`.
- Effect: the user-visible commit path is short. Everything that can be deferred, is.

### 1.4 Index types for OLTP
- **B-tree**: the default; covers PK lookups, range scans, equality. ~98% of OLTP index use. OpenDB needs this first.
- **Hash**: equality-only, smaller than B-tree on big keys, WAL-logged since PG 10.
- **BRIN**: cheap summary for huge naturally-sorted columns (time series). OLTP-marginal.
- **GIN**: inverted index for arrays, JSONB, full-text. OLTP only via JSONB.
- **GiST/SP-GiST**: geometric / specialized. Not OLTP.
- For OpenDB phase D, **B-tree + hash** is the right pair; everything else is later.

### 1.5 Connection model
- One backend **process** per connection (heavy: ~10 MB RSS, fork-per-connect). 200+ connections = real RAM pressure.
- PG itself does no pooling — operators front it with **PgBouncer** (transaction-mode pooling) or **pgcat**.
- OpenDB inherits an advantage here: one task per connection, single Rust process. We should not throw it away by holding a global Mutex on the hot path (§3.1).

### 1.6 PG 16/17 specific optimizations to anticipate
Targets shift over time; current benchmark numbers reflect:
- PG 16: parallel application of logical replication, improved `vacuum` cost-limit defaults, `pg_stat_io` (not on hot path, but observability).
- PG 16: SLRU cache size tunables (subtransactions, multixact), eased some long-standing OLTP cliffs.
- PG 16: faster `COPY FROM`, faster sort, allow_in_place_tablespaces.
- PG 17 (recently shipped): incremental backups, JSON_TABLE, improvements to VACUUM memory management, faster B-tree index scans for IN-lists, `pg_buffercache` evict.
- Implication for us: we are not chasing a static target. We have to beat what is, conservatively, PG 17 with `synchronous_commit=on`, `shared_buffers≈25% RAM`, B-tree PK + 1-2 secondaries, on commodity NVMe.

Uncertainty: PG 17 NUMA-related work is ongoing in PG 18 betas. If we ship phase E in 2026 H2 we should benchmark against whatever stable was in spring 2026.

---

## 2. Standard transactional benchmarks

### 2.1 pgbench (TPC-B-like, built-in)
- Schema: `pgbench_branches`, `pgbench_tellers`, `pgbench_accounts`, `pgbench_history`. Initialized via `pgbench -i -s <scale>` — scale 1 = 100 k accounts ≈ 16 MB.
- Default transaction (5 statements, one UPDATE per table + INSERT into history): the canonical short OLTP TX.
- Typical numbers on commodity NVMe (one author's recent runs, single 8-core box, PG 16, `-c 32 -j 8 -T 60 -M prepared`, scale 100):
  - `synchronous_commit=on`: 8–20 k TPS
  - `synchronous_commit=off`: 30–60 k TPS
  - `-S` (select-only): 150–400 k TPS
- It's biased toward UPDATE-heavy contention (4 row updates per TX, one of them on a 100-row `branches`). That contention is what we want to measure — but it is **also a worst case** for any naive serialized write path.

### 2.2 HammerDB (TPC-C-like)
- Five transactions, 23 rows touched per TX on average, multi-table joins. Much more realistic OLTP than pgbench.
- Tuned PG on big iron: ~1–2 M tpmC. Commodity boxes: 200–500 k tpmC.
- Requires more SQL surface (stored procs, prepared statements, complex selects). OpenDB does not have stored procs yet; we'd run the **non-stored-proc HammerDB driver** ("HammerDB without stored procedures") or pull TX logic into a thin Python/Go driver.

### 2.3 sysbench
- `oltp_read_write.lua`: 14 statements per TX, configurable read/write ratio. Lower ceremony than HammerDB. Widely cited.
- `oltp_read_only.lua`, `oltp_write_only.lua`, `oltp_point_select.lua` (the OLTP equivalent of a unit test).
- Numbers on PG 16 + 32 cores: typically 30–80 k TPS on `oltp_read_write` scale 100.

### 2.4 YCSB
- Workloads A/B/C/D/F (95/5, 50/50, 100% read, latest, RMW). Originally for key-value stores; works for SQL via JDBC.
- Good for "is opendb at least as fast as a KV store" sanity check, but the queries are too simple to reflect real OLTP — no joins, no transactions of >1 op.

### 2.5 Recommendation
**Start with `pgbench`**, then graduate to **sysbench `oltp_read_write`**, then `HammerDB`:

1. **pgbench** is built-in to PG, scriptable against any pgwire endpoint via `PGHOST`/`PGPORT`, ships with PG packages. Zero setup cost for us. It exercises UPDATE/SELECT/INSERT contention which is exactly where we'll cliff first (§3.1).
2. **sysbench oltp_read_write** once we've fixed phase B/C — broader query mix, well-known numbers across the industry, easy parametric scans of concurrency.
3. **HammerDB** is the headline benchmark we'd publish against, but it needs prepared statements + non-trivial planning. Save for phase D+.

Skip YCSB unless someone specifically asks "is opendb faster than Redis as a KV". That's not the goal.

---

## 3. OpenDB transactional perf gaps today

Concrete current-code findings (all `path:line` against this worktree). Each is a concurrency cliff we will hit before we get to interesting optimizations.

### 3.1 Global `Database` Mutex on the hot path
- `crates/opendb-node/src/pgwire.rs:33` — `serve(addr, database: Arc<Mutex<Database>>)`.
- `crates/opendb-node/src/pgwire.rs:231` and `:381` — every simple-query and every extended-query Execute message does `database.lock().await` and then `database.execute(statement).await` inside the lock.
- `crates/opendb-node/src/database.rs:169` — `Database::execute` is `async`, so the lock is held across an `.await` chain that includes SQL parsing, engine prepare, route resolution, the `root_range.submit` (which itself takes more locks and does WAL I/O + fsync), and the post-write `wal_byte_len()` (an extra `fs::metadata`).
- **Effect**: with N concurrent pgwire connections, **all** write traffic AND **all** read traffic serializes on a single async Mutex. The Rust+Tokio cost is small (no kernel mutex), but the held-time is huge because it spans an fsync. At N=10, throughput is ≤ 1/N of single-client throughput, minus context-switch overhead. We have no measurement of this yet — phase A's first job is to make the cliff visible.
- Even reads block because the Mutex is acquired before we know it's a read.

### 3.2 Per-WAL-path `append_lock` serializes all writers system-wide
- `crates/opendb-storage/src/wal.rs:29-30, 51` — `append_lock: Arc<Mutex<()>>`; `let _guard = self.append_lock.lock().await` is the first thing `append_with_len` does.
- `crates/opendb-storage/src/wal.rs:147-163` — the registry of locks is keyed by `PathBuf` so two `Wal::new(same_path)` share the lock (correct), and there is exactly one WAL path per range. **All writers on a range are serialized at this lock.**
- `crates/opendb-storage/src/wal.rs:101-104` — the fsync (`sync_data().await`) happens **inside** the held `append_lock`. So fsync latency × N writers is sequential.
- Even if we removed the `Database` Mutex (§3.1), every writer would still queue at this lock for the duration of one fsync (~50–500 µs on NVMe, much more on cloud-EBS).
- This is exactly the role of PG's `walwriter` + group commit (§1.2). The fix is structural, not a tuning knob.

### 3.3 No MVCC
- `crates/opendb-storage/src/row_projection.rs:18-22` — `Table { rows: BTreeMap<String, BTreeMap<String, Value>> }`. **One** version per row. No `xmin`/`xmax`. UPDATE mutates in place.
- `crates/opendb-sql/src/executor.rs:503-508` — `apply_committed` calls `projection.apply(&record)` which **overwrites** the row.
- Consequence: a SELECT that wants to see a consistent snapshot during a long INSERT batch has no mechanism to do so. We don't have a "snapshot" type. Today this is hidden because (a) the global Mutex serializes everything anyway, so consistency comes from serialization, not snapshots; (b) tests are single-client.
- When we remove the Mutex (phase B), this becomes immediately visible: readers and writers will race against the same `BTreeMap`. We'll need an `RwLock` as a stopgap, then real per-row versions (phase C).

### 3.4 Engine prepare cost included in the locked critical section
- `crates/opendb-sql/src/executor.rs:80-310` — `SqlEngine::prepare` is synchronous, takes `&self`, returns `PreparedQuery`. Today it runs inside the `Database` Mutex (`crates/opendb-node/src/database.rs:227`).
- It clones `table_state.columns`, validates the projection against a **cloned** projection (`crates/opendb-sql/src/executor.rs:544 — let mut validated_projection = self.projection.clone();`), then `apply()`s the record to the clone. For a 50-column table this clone is non-trivial.
- That clone runs while the Database lock is held — i.e. no other connection can do anything. Easy phase-B win: do the clone+validate outside the lock, then take the lock just for the WAL submit + apply step.

### 3.5 Double-apply per write
- `crates/opendb-sql/src/executor.rs:544-552` (`prepare_write_with_returning`): clones projection, applies record to the clone for validation, builds `PreparedQuery::Write`.
- After WAL commit, `crates/opendb-node/src/database.rs:255` calls `self.engine.apply_committed(record)`, which re-applies the same record to the real projection.
- We mutate (via clone) once during prepare, and once for real after submit. The first apply is purely defensive (validation). At ~500 rows it doesn't dominate, but at 100 k rows per TX it's measurable.

### 3.6 Per-write `fs::metadata` round-trip
- `crates/opendb-node/src/database.rs:254` — `self.last_replayed_wal_len = self.root_range.wal_byte_len().await?;` runs **after** every write.
- `crates/opendb-consensus/src/root_range.rs:364` — `wal_byte_len()` does an `fs::metadata` syscall.
- That's one extra stat per write, holding the Database Mutex. Cheap individually, brutal under contention.
- Why is it there? To update the refresh gate at `crates/opendb-node/src/database.rs:534`. We can replace it with the value `append_with_len` already returns (`crates/opendb-storage/src/wal.rs:120 — Ok(new_len)`); the plumbing exists, the wiring doesn't.

### 3.7 No background workers (architecturally)
- All "background" work today runs as tokio tasks inside the same process, sharing the same async runtime. There is no separate fsync writer, no checkpointer, no vacuum-equivalent.
- The async-task vs OS-process distinction is mostly academic for perf — what matters is whether the work is **off the user's critical path**. Right now it isn't, because there is no work to defer (the WAL is the only durable store and every commit fsyncs).
- Phase E (dedicated WAL writer task with group commit) is the equivalent of PG's `walwriter`.

### 3.8 No secondary indexes
- `crates/opendb-storage/src/row_projection.rs:20` — `rows: BTreeMap<String, BTreeMap<String, Value>>` keyed by **the primary key string only**.
- `crates/opendb-sql/src/executor.rs:345` and `:437` — DELETE WHERE and UPDATE WHERE do `table_state.rows.iter()`. Full table scan for every predicate that isn't a PK lookup.
- pgbench wouldn't immediately reveal this (its updates are PK-keyed) but `WHERE bid = ?` on `pgbench_tellers` is a 10-row table, scan is free. On HammerDB it becomes a cliff (e.g. NewOrder's `i_id` lookup).

### 3.9 Open-transaction state is per-`Database`, not per-session
- `crates/opendb-node/src/database.rs:57` + `:268-281` — `Database::transaction: Option<TransactionBuffer>`. The comment at `:60-62` notes: "Currently shared across all sessions because `Database` is wrapped in `Arc<Mutex<...>>` at the pgwire boundary; concurrent BEGIN from a second session is rejected."
- pgbench opens a transaction per connection. Today a second pgbench client's `BEGIN` would fail or block. We cannot even **run** pgbench with concurrency > 1 against the current code path without first fixing this.

### 3.10 WAL frame encoded as JSON, full file re-read on every cold open
- `crates/opendb-storage/src/wal.rs:184-205` — `encode_frame` uses `serde_json::to_vec`. JSON is ~3-5× larger than a binary encoding and ~10× slower to encode.
- `crates/opendb-storage/src/wal.rs:67-71` — first append after process start scans the entire WAL to find `durable_prefix_len`. After the 2026-05-20 cache, subsequent appends are O(1). Good.
- JSON encoding is the next bottleneck once the durable_prefix_len cache stops dominating.

---

## 4. Phased plan to reach competitive transactional perf

Effort estimates are calendar-week (one senior engineer, including tests and benchmarking).

### Phase A — bench-driven baseline (1 week)
- Stand up `pgbench -i -s 1` against opendb. Document what breaks (likely: §3.9 — concurrent BEGIN — and one or two parser gaps).
- Add a `bench/pgbench-runner.sh` and a `docs/bench/pgbench-2026-MM-DD.md` template.
- Run `-c 1, 4, 16, 32 -T 60 -M simple` against opendb and PG 16 on the same hardware. Publish the cliff.
- Output: a number to beat. **Acceptance**: `pgbench -c 1` runs to completion against opendb.
- Risk: low. Most likely outcome: opendb is 10–30× slower at `c=1`, gets **worse** at `c=16` (because of §3.1).

### Phase B — reduce lock-holding-time on Database (1–2 weeks)
- Move SQL parse + engine `prepare` outside the Database lock (`crates/opendb-node/src/pgwire.rs:381` and `crates/opendb-node/src/database.rs:227`). Take the lock only for the submit+apply pair.
- Replace `Mutex<Database>` with split locking: `RwLock` on the projection for reads, `Mutex` on the WAL submit path. Per-session transaction buffer (fixes §3.9).
- Use the `new_len` returned by `wal.append_with_len` to update `last_replayed_wal_len` instead of stat-ing the file (§3.6).
- Drop the double-apply (§3.5) by trusting the prepare-time validation under the per-session lock.
- **Acceptance**: `pgbench -c 16` linear-ish scaling on reads. Writes still bottleneck on §3.2 — that's expected, that's phase E.

### Phase C — real MVCC (3–6 weeks)
- Change `Table::rows` from `BTreeMap<key, row>` to `BTreeMap<key, VersionChain>` where each version carries `(xmin, xmax, row)`.
- Add a `TxnId` / snapshot type. Readers snapshot the current commit version and walk the chain. Writers append a new version under their own `xmin`.
- Vacuum equivalent: a periodic task that prunes versions whose `xmax < oldest_active_snapshot`.
- This is the biggest single piece of work and the one that unlocks everything else. Without MVCC, every concurrent reader needs a read-side lock, and we can't scale.
- **Acceptance**: `pgbench -c 32` reaches at least 50% of single-client × 32 throughput, with no read-side serialization. Long-running SELECT does not block concurrent UPDATEs.

### Phase D — indexing beyond primary key (2–3 weeks)
- Add a secondary B-tree index struct (`BTreeMap<value, Vec<key>>` initially; later, a real B+tree if hot).
- Add `CREATE INDEX` parsing + `DROP INDEX`.
- Wire the executor to consult indexes before falling back to full scan (`crates/opendb-sql/src/executor.rs:345, 437` and the equivalent SELECT path).
- Add hash indexes for equality-only lookups; gate behind explicit `USING hash` (PG syntax).
- **Acceptance**: HammerDB NewOrder's `customer_id` UPDATE WHERE no longer scans the table.

### Phase E — dedicated WAL writer + group commit (2–4 weeks)
- Move WAL append + fsync to a dedicated tokio task with a `mpsc` queue of records. Submit-side returns a oneshot that the WAL writer fulfils after its fsync.
- The WAL writer batches: it takes the queue prefix that has accumulated since its last fsync, writes them in one `write_all`, fsyncs once, then resolves all the oneshots. This is the **group commit** primitive.
- Optionally add `wal_writer_delay`-equivalent (force a small wait to collect more committers under heavy load).
- Replace JSON framing (§3.10) with `bincode` or a hand-rolled binary frame. Order-of-magnitude smaller payloads.
- **Acceptance**: `pgbench -c 32` write throughput scales as `min(per-commit-rows-per-fsync × NVMe fsync rate)`. On consumer NVMe (~5 k fsync/s), this is ~50–150 k TPS write — competitive with PG.

### Phase F — concurrent multi-leader writes (months, not weeks)
- Today we have one OpenRaft group per range. To get multi-leader writes we need either:
  - per-range leadership distributed across nodes (already the design intent — `crates/opendb-consensus/src/root_range.rs`), OR
  - leaderless writes with conflict resolution (CRDT-flavored — incompatible with strong SQL semantics).
- This is the cluster-scale story, not the single-node story. **Not required to beat PG on a single box.**

### Timeline summary
| Phase | Effort | Cumulative | Unlocks                                  |
|-------|--------|------------|------------------------------------------|
| A     | 1 wk   | 1 wk       | "where do we cliff?"                      |
| B     | 1–2 wk | 3 wk       | concurrent reads, concurrent transactions |
| C     | 3–6 wk | 9 wk       | real concurrency: readers don't block writers |
| D     | 2–3 wk | 12 wk      | non-PK queries don't full-scan            |
| E     | 2–4 wk | 16 wk      | the headline: matches PG write throughput |
| F     | months | quarters    | scale across nodes                        |

Conservative read: **~4 months of focused work to get to "competitive with PG on pgbench at c=32 on a single box."** That assumes no surprises from phase C, which is optimistic — MVCC is the hardest part of any database and our row projection is currently quite far from one.

---

## 5. The smallest demo that shows opendb ≥ PG on a transactional workload

**Target**: `pgbench -i -s 10 ; pgbench -c 16 -j 4 -T 60 -M prepared --no-vacuum`

Scale 10 = 1 M accounts ≈ 160 MB. Fits in RAM easily (PG's `shared_buffers` won't even need tuning). 16 concurrent clients is enough to exercise concurrency without saturating fsync on a single NVMe.

`-M prepared` matters: it amortizes parse + plan over the run. We claim parity on prepared throughput, not on per-statement parse.

`--no-vacuum` between runs: PG's autovacuum would otherwise penalize the "100 short transactions" pattern. We're not yet running vacuum equivalents so this is a fair comparison.

**Architectural minimum to attempt this**:

- Phase A (otherwise we can't even run pgbench against opendb).
- Phase B (otherwise `-c 16` performs worse than `-c 1`).
- Phase C (otherwise readers block on writer fsyncs, killing the read-50% of pgbench's mix).
- Phase E (otherwise write throughput is bounded by serial fsyncs at ~1–2 k TPS regardless of cores).

Phase D is **not** strictly required for pgbench (all its UPDATEs are PK-keyed). It is required for sysbench `oltp_read_write` and HammerDB.

**Concrete acceptance**: opendb TPS ≥ PG TPS on `pgbench -c 16 -T 60 -M prepared` at scale 10, both with `synchronous_commit=on` and on the same NVMe.

Aspirational stretch: same comparison at `-c 64`. PG starts to feel its per-backend RAM cost there; opendb's single-process model should win on memory and possibly on TPS too — but only after phase E, and only if our group-commit batch sizes hit the same fsync ceiling.

---

## 6. Cross-reference to the analytical track

Sibling: `docs/roadmap/perf-vs-analytical-2026-05-22.md`.

### 6.1 Where transactional and analytical conflict
- **Row store vs columnar**: pgbench/sysbench want a row store — `SELECT * FROM accounts WHERE aid = ?` is a single-row gather, cheap on rows, expensive on columns. Analytical scans want columnar — `SUM(amount)` across 1 B rows is bandwidth-bound and benefits massively from columnar compression. We will eventually need both, accessed via a unified plan layer. Most modern systems (DuckDB, ClickHouse, Singlestore) pick one and bolt the other on; the cost of bolting on the "other" half is high. **Recommendation**: stay row-store-only for milestone 1; the analytical doc proposes a separate columnar projection materialized from the same WAL.
- **MVCC vs append-only segments**: transactional MVCC wants per-row versions, frequent updates, vacuum. Analytical workloads want immutable segments + bulk rewrites. Phase C's MVCC design should not preclude later append-only segments — the projection layer can host both.
- **fsync per commit vs batch ingest**: phase E's group commit reduces fsync pressure but still fsyncs on every commit. Analytical bulk inserts (`COPY FROM`) want one fsync per million rows. The WAL writer task design should expose a "bulk" submit mode.
- **Background workers competing for CPU**: vacuum + checkpointer (OLTP) vs background segment compactor (OLAP) will fight for the same CPU. PG has cost-limits per worker; we'd need the same.

### 6.2 Where they reinforce each other
- **Binary WAL framing** (§3.10): phase E replaces JSON with binary. The analytical track wants the same — large encoded payloads are bad for both.
- **Removing the global Database mutex** (§3.1): phase B benefits analytical queries too. A long-running scan today blocks every other connection.
- **Lock-free reads via MVCC** (§3.3): phase C lets analytical scans run concurrently with OLTP writes. This is the canonical "HTAP" win and the reason single-binary HTAP systems exist.
- **Indexing infrastructure** (phase D): the same B-tree code can host primary, secondary, and the OLAP-side "zonemap" / min-max sketches. Build the abstraction once.
- **Group commit** (phase E): bulk INSERT/COPY for OLAP rides the same WAL writer queue. One implementation, two beneficiaries.
- **Background task framework**: vacuum, checkpointer, compactor, autostats all want the same primitive — a tokio task with a cost budget and a backoff. Build it once in phase C/E and reuse.

### 6.3 Sequencing recommendation
Do the transactional phases A→E first, because:
1. The hardest piece (MVCC, phase C) is needed for both; doing it in the OLTP context surfaces correctness issues sooner (fewer rows, simpler queries).
2. PG is a stronger competitor on OLTP than on OLAP — beating PG on pgbench is a sharper proof point than beating it on TPC-H, where DuckDB / ClickHouse are the real comparison.
3. The OLAP track can borrow infrastructure (group commit, MVCC, binary framing) instead of duplicating it.

Counter-argument: if the user's product (sentropic) is more analytical than transactional, this sequencing is wrong. **Flag for the user**: which track does the killer demo need first? Today's sentropic POC mix is INSERT-heavy autocommit — that's OLTP. Long-term, sentropic's "show me all matches across N years" is OLAP. The roadmap above assumes OLTP-first; revisit if that assumption breaks.

---

## 7. What I'm uncertain about

- **PG numbers cited above** are from memory and from a few public blog posts. We should re-measure on our actual hardware before quoting a TPS gap.
- **Phase C effort** (MVCC) could easily be 8–12 weeks rather than 3–6. The estimate assumes we keep the projection layer's BTreeMap and only wrap values; if we end up needing a real heap-tuple format (with `xmin`/`xmax` packed inline) it's much more work.
- **Phase E group commit** could be much faster if we punt on the dedicated-task design and just batch under `append_lock` (a single committer drains a queue while holding the lock). Less clean architecturally, fewer weeks of work, gets us 80% of the throughput win. Worth prototyping first.
- **OpenRaft path** (multi-node) is not on this roadmap and may dominate the fsync story in clustered deployments. If milestone 1 ships standalone, that's fine; if it ships 3-node, every commit fsyncs locally **and** waits for raft majority. Phase E group commit then has to coordinate with raft batching — call it phase E.5.
- **Connection model**: I've assumed we stay with "one tokio task per connection." If we ever switch to a connection-pool-aware model (a fixed worker pool that multiplexes connections), the analysis changes. PG with PgBouncer is what most production deployments actually look like; comparing opendb-direct vs PG-with-PgBouncer is the fairer apples-to-apples once we have phase B.

---

## 8. Recommended immediate next action

Phase A, this week: get `pgbench` running against opendb. The first failed run will probably surface one or two parser/protocol gaps (§3.9 at minimum). Fix those, publish the c=1 / c=4 / c=16 numbers in `docs/bench/pgbench-2026-MM-DD.md`. **Until we have those numbers, all the work in §4 is being prioritized blind.**
