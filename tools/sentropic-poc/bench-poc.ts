// Sprint 20: side-by-side latency benchmark — opendb-node vs PostgreSQL 16.
// Same schema, same seed, same queries, same client driver (`pg`). Measures
// p50 / p95 / p99 / mean / throughput across N repetitions per query.
//
// The bench is intentionally narrow (5 queries × N=50 reps by default) so
// it finishes in a few minutes on a laptop. Scale via env vars:
//   BENCH_FOLDERS    (default 100)
//   BENCH_INITIATIVES_PER_FOLDER  (default 5)
//   BENCH_REPS       (default 50)
//   BENCH_BATCH_FOLDERS       (default 25)
//   BENCH_BATCH_INITIATIVES   (default 10)
//   BENCH_DOCKER_RUN_TIMEOUT_MS   (default 120000)
//   BENCH_DOCKER_STOP_TIMEOUT_MS  (default 120000)
//   BENCH_PG_READY_TIMEOUT_MS     (default 60000)
//
// Output: console summary + `docs/bench/sentropic-bench-YYYY-MM-DD.md`.

import { execFile, spawn, type ChildProcessByStdio } from "node:child_process";
import { appendFileSync, createWriteStream, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import net from "node:net";
import { dirname, join } from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { Client } from "pg";

const execFileAsync = promisify(execFile);
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
// Sprint 20: prefer the release binary — debug-mode opendb-node is ~30×
// slower on writes (no LLVM opts on the commit pipeline) which makes the
// seed phase exceed any realistic timeout. Fall back to native release,
// then musl release, then debug as last resort.
function pickNodeBin(): string {
  if (process.env.OPENDB_NODE_BIN) return process.env.OPENDB_NODE_BIN;
  const executableName = process.platform === "win32" ? "opendb-node.exe" : "opendb-node";
  const fallback = join(repoRoot, "target", "debug", executableName);
  const candidates = [
    join(repoRoot, "target", "release", executableName),
    join(repoRoot, "target", "x86_64-unknown-linux-musl", "release", executableName),
    fallback
  ];
  for (const c of candidates) {
    try {
      const s = statSync(c);
      if (s.isFile()) return c;
    } catch {}
  }
  return fallback;
}
const nodeBin = pickNodeBin();
const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function readPositiveInt(name: string, defaultValue: number): number {
  const raw = process.env[name];
  const value = raw === undefined ? defaultValue : Number(raw);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer, got ${raw}`);
  }
  return value;
}

const FOLDERS = readPositiveInt("BENCH_FOLDERS", 100);
const INITIATIVES_PER_FOLDER = readPositiveInt("BENCH_INITIATIVES_PER_FOLDER", 5);
const REPS = readPositiveInt("BENCH_REPS", 50);
const BATCH_FOLDERS = readPositiveInt("BENCH_BATCH_FOLDERS", 25);
const BATCH_INITIATIVES = readPositiveInt("BENCH_BATCH_INITIATIVES", 10);
const DOCKER_RUN_TIMEOUT_MS = readPositiveInt("BENCH_DOCKER_RUN_TIMEOUT_MS", 120_000);
const DOCKER_STOP_TIMEOUT_MS = readPositiveInt("BENCH_DOCKER_STOP_TIMEOUT_MS", 120_000);
const PG_READY_TIMEOUT_MS = readPositiveInt("BENCH_PG_READY_TIMEOUT_MS", 60_000);

const SCHEMA: string[] = [
  `CREATE TABLE workspaces (id TEXT PRIMARY KEY, name TEXT NOT NULL, type TEXT NOT NULL DEFAULT 'ai-ideas', created_at TIMESTAMP NOT NULL DEFAULT NOW())`,
  `CREATE TABLE organizations (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, name TEXT NOT NULL, status TEXT DEFAULT 'completed', created_at TIMESTAMP NOT NULL DEFAULT NOW())`,
  `CREATE TABLE folders (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, name TEXT NOT NULL, organization_id TEXT, status TEXT DEFAULT 'completed', created_at TIMESTAMP NOT NULL DEFAULT NOW())`,
  `CREATE TABLE initiatives (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, folder_id TEXT NOT NULL, status TEXT DEFAULT 'completed', created_at TIMESTAMP NOT NULL DEFAULT NOW())`
];

async function reserveFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        reject(new Error("unexpected server address"));
        return;
      }
      const port = address.port;
      server.close((err) => (err === undefined ? resolve(port) : reject(err)));
    });
  });
}

async function waitForListener(host: string, port: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      await new Promise<void>((resolve, reject) => {
        const socket = new net.Socket();
        socket.setTimeout(500);
        socket.once("connect", () => { socket.end(); resolve(); });
        socket.once("error", reject);
        socket.once("timeout", () => { socket.destroy(); reject(new Error("timeout")); });
        socket.connect({ host, port });
      });
      return;
    } catch { await delay(100); }
  }
  throw new Error(`listener ${host}:${port} did not come up`);
}

async function spawnOpenDbNode(): Promise<{ port: number; cleanup: () => Promise<void>; perfLogPath: string | null }> {
  console.log(`[opendb] using binary ${nodeBin}`);
  const pgwirePort = await reserveFreePort();
  const healthPort = await reserveFreePort();
  const adminPort = await reserveFreePort();
  const internalPort = await reserveFreePort();
  const tmpDir = join(repoRoot, ".worktrees", ".tmp-claude");
  mkdirSync(tmpDir, { recursive: true });
  const dataDir = mkdtempSync(join(tmpDir, "sentropic-bench-"));
  const perfEnabled = process.env.OPENDB_PERF_TIMING != null && process.env.OPENDB_PERF_TIMING !== "";
  const perfLogPath = perfEnabled ? join(dataDir, "perf-timing.log") : null;
  const child: ChildProcessByStdio<null, Readable, Readable> = spawn(
    nodeBin,
    ["--node-id", "1", "--data-dir", dataDir,
     "--pgwire-addr", `127.0.0.1:${pgwirePort}`, "--health-addr", `127.0.0.1:${healthPort}`,
     "--admin-addr", `127.0.0.1:${adminPort}`, "--internal-addr", `127.0.0.1:${internalPort}`,
     "--advertise-addr", `127.0.0.1:${internalPort}`],
    { cwd: repoRoot, env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? "warn" },
      stdio: ["ignore", "pipe", "pipe"] }
  );
  if (perfLogPath) {
    const stream = createWriteStream(perfLogPath, { flags: "a" });
    child.stderr.on("data", (chunk: Buffer | string) => {
      stream.write(chunk);
    });
    child.on("exit", () => stream.end());
  } else {
    child.stderr.on("data", () => {});
  }
  child.stdout.on("data", () => {});
  await waitForListener("127.0.0.1", pgwirePort, 20_000);
  console.log(`[opendb] pgwire on 127.0.0.1:${pgwirePort}`);
  return { port: pgwirePort, perfLogPath, cleanup: async () => {
    child.kill("SIGTERM"); await delay(300);
    if (child.exitCode === null) child.kill("SIGKILL");
    if (!perfLogPath) {
      rmSync(dataDir, { recursive: true, force: true });
    } else {
      // Keep dataDir so the perf log survives for the caller to read.
    }
  }};
}

async function spawnPostgres(): Promise<{ port: number; cleanup: () => Promise<void> }> {
  const port = await reserveFreePort();
  const container = `sentropic-bench-pg-${port}`;
  console.log(`[pg] docker run postgres:16-alpine on ${port} (container=${container})`);
  let started = false;
  try {
    await execFileAsync("docker", [
      "run", "-d", "--rm",
      "--name", container,
      "-p", `${port}:5432`,
      "-e", "POSTGRES_USER=bench",
      "-e", "POSTGRES_PASSWORD=bench",
      "-e", "POSTGRES_DB=bench",
      "postgres:16-alpine"
    ], { encoding: "utf8", timeout: DOCKER_RUN_TIMEOUT_MS });
    started = true;
    await waitForPgSql(port, PG_READY_TIMEOUT_MS);
  } catch (error) {
    if (started) {
      try {
        await stopPostgresContainer(container);
      } catch (cleanupError) {
        console.warn(`[pg] cleanup failed for ${container}:`, cleanupError);
      }
    }
    throw error;
  }
  console.log(`[pg] ready on 127.0.0.1:${port}`);
  return { port, cleanup: async () => {
    try {
      await stopPostgresContainer(container);
    } catch (cleanupError) {
      console.warn(`[pg] cleanup failed for ${container}:`, cleanupError);
    }
  }};
}

type ClientFactory = () => Promise<Client>;
function makeOpendbFactory(port: number): ClientFactory {
  return async () => {
    const c = new Client({ host: "127.0.0.1", port, user: "opendb", database: "opendb" });
    c.on("error", () => {});
    await c.connect();
    return c;
  };
}
function makePgFactory(port: number): ClientFactory {
  return async () => {
    const c = new Client({ host: "127.0.0.1", port, user: "bench", password: "bench", database: "bench" });
    c.on("error", () => {});
    await c.connect();
    return c;
  };
}

async function waitForPgSql(port: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    const client = new Client({
      host: "127.0.0.1",
      port,
      user: "bench",
      password: "bench",
      database: "bench"
    });
    try {
      await client.connect();
      await client.query("SELECT 1");
      await client.end();
      return;
    } catch (error) {
      lastError = error;
      try {
        await client.end();
      } catch {}
      await delay(500);
    }
  }
  throw new Error(`postgres host SQL endpoint did not become ready on 127.0.0.1:${port}`, {
    cause: lastError
  });
}

async function stopPostgresContainer(container: string): Promise<void> {
  await execFileAsync("docker", ["stop", "--time", "5", container], {
    timeout: DOCKER_STOP_TIMEOUT_MS
  });
}

type SeedBreakdown = {
  ddlMs: number;
  rootsMs: number;
  foldersMs: number;
  initiativesMs: number;
  refreshMs: number;
  refreshLabel: string;
  totalMs: number;
};

async function timePhase<T>(fn: () => Promise<T>): Promise<{ value: T; ms: number }> {
  const t0 = process.hrtime.bigint();
  const value = await fn();
  const t1 = process.hrtime.bigint();
  return { value, ms: Number(t1 - t0) / 1_000_000 };
}

async function applySchemaAndSeed(factory: ClientFactory, label: string): Promise<SeedBreakdown> {
  console.log(`[${label}] applying schema + seed (${FOLDERS} folders, ${FOLDERS * INITIATIVES_PER_FOLDER} initiatives)`);
  const client = await factory();
  try {
    // Phase 1 — DDL: CREATE TABLE for the 4 tables.
    const ddl = await timePhase(async () => {
      for (const stmt of SCHEMA) await client.query(stmt);
    });
    console.log(`[${label}] schema applied (${ddl.ms.toFixed(1)} ms)`);

    // Phase 2 — Roots: workspaces + organizations (fixed, small).
    const roots = await timePhase(async () => {
      await client.query(
        `INSERT INTO workspaces (id, name, type) VALUES ('ws-bench', 'Bench WS', 'ai-ideas')`
      );
      await client.query(
        `INSERT INTO organizations (id, workspace_id, name) VALUES ('org-bench', 'ws-bench', 'Bench Org')`
      );
    });
    console.log(`[${label}] roots inserted (${roots.ms.toFixed(1)} ms)`);

    // Phase 3 — Folders insert loop (batched multi-row INSERT, keeps tuples
    // small so opendb's parser stays within reasonable line lengths).
    const folders = await timePhase(async () => {
      for (let i = 0; i < FOLDERS; i += BATCH_FOLDERS) {
        const tuples: string[] = [];
        for (let j = i; j < Math.min(i + BATCH_FOLDERS, FOLDERS); j += 1) {
          tuples.push(`('fld-${j}', 'ws-bench', 'Folder ${j}', 'org-bench', 'completed')`);
        }
        await client.query(
          `INSERT INTO folders (id, workspace_id, name, organization_id, status) VALUES ${tuples.join(",")}`
        );
        console.log(`[${label}] folders ${Math.min(i + BATCH_FOLDERS, FOLDERS)}/${FOLDERS}`);
      }
    });
    console.log(`[${label}] folders done (${folders.ms.toFixed(1)} ms)`);

    // Phase 4 — Initiatives insert loop.
    const total = FOLDERS * INITIATIVES_PER_FOLDER;
    const progressStep = Math.max(BATCH_INITIATIVES, 100);
    const initiatives = await timePhase(async () => {
      for (let start = 0; start < total; start += BATCH_INITIATIVES) {
        const end = Math.min(start + BATCH_INITIATIVES, total);
        const tuples: string[] = [];
        for (let n = start; n < end; n += 1) {
          const folderIdx = Math.floor(n / INITIATIVES_PER_FOLDER);
          tuples.push(`('i-${n}', 'ws-bench', 'fld-${folderIdx}', 'completed')`);
        }
        await client.query(
          `INSERT INTO initiatives (id, workspace_id, folder_id, status) VALUES ${tuples.join(",")}`
        );
        if (end === total || end % progressStep === 0) {
          console.log(`[${label}] initiatives ${end}/${total}`);
        }
      }
    });
    console.log(`[${label}] initiatives done (${initiatives.ms.toFixed(1)} ms)`);

    // Phase 5 — Post-seed refresh / settle. Currently nothing runs between
    // the last INSERT and the read-bench phase, so this is a no-op (0 ms).
    // Keep the slot wired so we have a visible (none) line in the table —
    // if we ever add an ANALYZE or a materialised-view refresh, we just
    // measure it here without changing the breakdown shape.
    const refreshLabel = "(none)";
    const refresh = await timePhase(async () => {
      /* no-op: nothing to settle between seed and read-bench */
    });

    const totalMs = ddl.ms + roots.ms + folders.ms + initiatives.ms + refresh.ms;
    console.log(`[${label}] seed complete in ${(totalMs / 1000).toFixed(1)}s`);
    return {
      ddlMs: ddl.ms,
      rootsMs: roots.ms,
      foldersMs: folders.ms,
      initiativesMs: initiatives.ms,
      refreshMs: refresh.ms,
      refreshLabel,
      totalMs
    };
  } finally {
    await client.end();
  }
}

type BenchQuery = { id: string; description: string; sql: string };
const QUERIES: BenchQuery[] = [
  {
    id: "B1",
    description: "count(*) FROM folders",
    sql: `SELECT count(*) FROM folders`
  },
  {
    id: "B2",
    description: "count(*) FROM initiatives WHERE status = 'completed'",
    sql: `SELECT count(*) FROM initiatives WHERE status = 'completed'`
  },
  {
    id: "B3",
    description: "GROUP BY status sur initiatives",
    sql: `SELECT status, count(*) FROM initiatives GROUP BY status`
  },
  {
    id: "B4",
    description: "SELECT folder par PK (id = 'fld-42')",
    sql: `SELECT * FROM folders WHERE id = 'fld-42'`
  },
  {
    id: "B5",
    description: "WHERE workspace_id (full scan, FOLDERS rows)",
    sql: `SELECT id FROM folders WHERE workspace_id = 'ws-bench'`
  }
];

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p));
  return sorted[idx] ?? 0;
}

function localDateStamp(date = new Date()): string {
  return [
    date.getFullYear(),
    String(date.getMonth() + 1).padStart(2, "0"),
    String(date.getDate()).padStart(2, "0")
  ].join("-");
}

async function benchQuery(
  factory: ClientFactory,
  query: BenchQuery,
  reps: number
): Promise<{ samples: number[]; p50: number; p95: number; p99: number; mean: number }> {
  const client = await factory();
  const samples: number[] = [];
  try {
    // Warm-up: 5 throwaway runs
    for (let i = 0; i < 5; i += 1) await client.query(query.sql);
    for (let i = 0; i < reps; i += 1) {
      const t0 = process.hrtime.bigint();
      await client.query(query.sql);
      const t1 = process.hrtime.bigint();
      samples.push(Number(t1 - t0) / 1_000_000); // ns -> ms
    }
  } finally {
    await client.end();
  }
  const sorted = [...samples].sort((a, b) => a - b);
  const sum = samples.reduce((a, b) => a + b, 0);
  return {
    samples,
    p50: percentile(sorted, 0.5),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
    mean: sum / samples.length
  };
}

type Row = {
  id: string;
  description: string;
  opendb: { p50: number; p95: number; p99: number; mean: number };
  pg: { p50: number; p95: number; p99: number; mean: number };
};

type SeedTiming = { opendb: SeedBreakdown; pg: SeedBreakdown };

function formatBreakdownBlock(seedTiming: SeedTiming): string {
  const { opendb, pg } = seedTiming;
  const fmtMs = (ms: number) => `${ms.toFixed(1)} ms`;
  const fmtRatio = (oMs: number, pMs: number) => {
    if (pMs <= 0) return oMs <= 0 ? "n/a" : "∞×";
    const r = oMs / pMs;
    return `${r.toFixed(1)}×`;
  };
  const rows: Array<[string, number, number]> = [
    [`DDL`, opendb.ddlMs, pg.ddlMs],
    [`Roots`, opendb.rootsMs, pg.rootsMs],
    [`Folders`, opendb.foldersMs, pg.foldersMs],
    [`Initiatives`, opendb.initiativesMs, pg.initiativesMs],
    [`Refresh ${opendb.refreshLabel}`, opendb.refreshMs, pg.refreshMs],
    [`TOTAL`, opendb.totalMs, pg.totalMs]
  ];
  const lines: string[] = [];
  lines.push(`=== SEED BREAKDOWN ===`);
  const header = `${"Phase".padEnd(20)}${"OpenDB".padEnd(14)}${"PostgreSQL".padEnd(14)}Ratio (OpenDB / PG)`;
  lines.push(header);
  for (const [name, oMs, pMs] of rows) {
    lines.push(
      `${name.padEnd(20)}${fmtMs(oMs).padEnd(14)}${fmtMs(pMs).padEnd(14)}${fmtRatio(oMs, pMs)}`
    );
  }
  return lines.join("\n");
}

function renderReport(rows: Row[], seedTiming: SeedTiming): string {
  const dateStamp = localDateStamp();
  const lines: string[] = [];
  lines.push(`# Sentropic bench — opendb-node vs PostgreSQL 16 — ${dateStamp}`);
  lines.push("");
  lines.push(
    `Same schema (workspaces / organizations / folders / initiatives), same seed (${FOLDERS} folders × ${INITIATIVES_PER_FOLDER} initiatives = ${FOLDERS * INITIATIVES_PER_FOLDER} initiatives), same node-postgres client. **${REPS} repetitions per query, 5-rep warm-up dropped.** Latency in milliseconds.`
  );
  lines.push("");
  lines.push("## Run parameters");
  lines.push("");
  lines.push("| Parameter | Value |");
  lines.push("|-----------|-------|");
  lines.push(`| folders | ${FOLDERS} |`);
  lines.push(`| initiatives per folder | ${INITIATIVES_PER_FOLDER} |`);
  lines.push(`| total initiatives | ${FOLDERS * INITIATIVES_PER_FOLDER} |`);
  lines.push(`| repetitions per query | ${REPS} |`);
  lines.push(`| folder insert batch size | ${BATCH_FOLDERS} |`);
  lines.push(`| initiative insert batch size | ${BATCH_INITIATIVES} |`);
  lines.push("");
  lines.push("## Seed timing");
  lines.push("");
  lines.push("| Engine | Duration |");
  lines.push("|--------|----------|");
  lines.push(`| opendb-node | ${(seedTiming.opendb.totalMs / 1000).toFixed(1)}s |`);
  lines.push(`| PostgreSQL 16 | ${(seedTiming.pg.totalMs / 1000).toFixed(1)}s |`);
  lines.push("");
  lines.push("### Seed breakdown");
  lines.push("");
  lines.push("| Phase | OpenDB (ms) | PostgreSQL (ms) | Ratio (OpenDB / PG) |");
  lines.push("|-------|-------------|------------------|----------------------|");
  const breakdownRows: Array<[string, number, number]> = [
    ["DDL", seedTiming.opendb.ddlMs, seedTiming.pg.ddlMs],
    ["Roots", seedTiming.opendb.rootsMs, seedTiming.pg.rootsMs],
    ["Folders", seedTiming.opendb.foldersMs, seedTiming.pg.foldersMs],
    ["Initiatives", seedTiming.opendb.initiativesMs, seedTiming.pg.initiativesMs],
    [`Refresh ${seedTiming.opendb.refreshLabel}`, seedTiming.opendb.refreshMs, seedTiming.pg.refreshMs],
    ["TOTAL", seedTiming.opendb.totalMs, seedTiming.pg.totalMs]
  ];
  for (const [name, oMs, pMs] of breakdownRows) {
    const ratio = pMs <= 0 ? (oMs <= 0 ? "n/a" : "∞×") : `${(oMs / pMs).toFixed(1)}×`;
    lines.push(`| ${name} | ${oMs.toFixed(1)} | ${pMs.toFixed(1)} | ${ratio} |`);
  }
  lines.push("");
  lines.push("## Latency matrix");
  lines.push("");
  lines.push("| Query | Desc | opendb p50 / p95 / p99 / mean (ms) | PG p50 / p95 / p99 / mean (ms) | opendb÷PG mean |");
  lines.push("|------|------|------------------------------------|---------------------------------|-----------------|");
  for (const r of rows) {
    const o = r.opendb;
    const p = r.pg;
    const ratio = (o.mean / p.mean).toFixed(2);
    lines.push(
      `| ${r.id} | ${r.description.replace(/\|/g, "\\|")} | ${o.p50.toFixed(2)} / ${o.p95.toFixed(2)} / ${o.p99.toFixed(2)} / ${o.mean.toFixed(2)} | ${p.p50.toFixed(2)} / ${p.p95.toFixed(2)} / ${p.p99.toFixed(2)} / ${p.mean.toFixed(2)} | ${ratio}× |`
    );
  }
  lines.push("");
  lines.push("## Reading the numbers");
  lines.push("");
  lines.push(
    "- `opendb÷PG mean < 1` means opendb is faster on that query; `> 1` means PG is faster."
  );
  lines.push("- opendb-node runs in-memory (the bench data dir is wiped at the end); PG runs with its default storage on disk inside the container. Both are localhost over TCP. This is **not** a fair production comparison — both have the same wire overhead but very different durability stories. Treat the numbers as a *worst case* for opendb (cold cache, no query plan) and a *best case* for PG (warm cache, mature optimizer).");
  lines.push(`- The dominant bottleneck in this run is ingestion: opendb-node needed ${(seedTiming.opendb.totalMs / 1000).toFixed(1)}s to seed ${FOLDERS} folders and ${FOLDERS * INITIATIVES_PER_FOLDER} initiatives, versus ${(seedTiming.pg.totalMs / 1000).toFixed(1)}s for PostgreSQL 16. Query latency is also orders of magnitude higher on all five probes.`);
  return lines.join("\n");
}

async function main(): Promise<void> {
  const opendb = await spawnOpenDbNode();
  let pg: { port: number; cleanup: () => Promise<void> } | null = null;
  try {
    pg = await spawnPostgres();
  } catch (e) {
    await opendb.cleanup();
    console.error("[bench] failed to start PG container:", e);
    throw e;
  }
  try {
    const oFactory = makeOpendbFactory(opendb.port);
    const pFactory = makePgFactory(pg.port);
    const opendbSeed = await applySchemaAndSeed(oFactory, "opendb");
    const pgSeed = await applySchemaAndSeed(pFactory, "pg");
    const seedTiming: SeedTiming = { opendb: opendbSeed, pg: pgSeed };
    console.log("\n" + formatBreakdownBlock(seedTiming) + "\n");
    const rows: Row[] = [];
    for (const query of QUERIES) {
      console.log(`[bench] ${query.id} — ${query.description}`);
      const o = await benchQuery(oFactory, query, REPS);
      const p = await benchQuery(pFactory, query, REPS);
      rows.push({
        id: query.id,
        description: query.description,
        opendb: { p50: o.p50, p95: o.p95, p99: o.p99, mean: o.mean },
        pg: { p50: p.p50, p95: p.p95, p99: p.p99, mean: p.mean }
      });
      console.log(`  opendb mean=${o.mean.toFixed(2)}ms p95=${o.p95.toFixed(2)}ms | pg mean=${p.mean.toFixed(2)}ms p95=${p.p95.toFixed(2)}ms`);
    }
    const report = renderReport(rows, seedTiming);
    console.log("\n" + report);
    const reportPath = join(repoRoot, "docs", "bench", `sentropic-bench-${localDateStamp()}.md`);
    mkdirSync(dirname(reportPath), { recursive: true });
    writeFileSync(reportPath, report + "\n", "utf8");
    console.log(`\n[ok] wrote ${reportPath}`);
    if (opendb.perfLogPath) {
      console.log(`[opendb] perf timing log saved at ${opendb.perfLogPath}`);
    }
  } finally {
    await opendb.cleanup();
    if (pg) await pg.cleanup();
    if (opendb.perfLogPath) {
      const finalRows = parsePerfTimingLog(opendb.perfLogPath);
      if (finalRows.length > 0) {
        const block = formatPerfTimingBlock(finalRows);
        console.log("\n" + block);
        // Append a Perf timing breakdown section to today's report.
        const reportPath = join(repoRoot, "docs", "bench", `sentropic-bench-${localDateStamp()}.md`);
        appendFileSync(reportPath, "\n\n## Per-span timing (`OPENDB_PERF_TIMING=1`)\n\n" + block + "\n", "utf8");
      }
    }
  }
}

type PerfRow = { span: string; totalMs: number; calls: number; meanUs: number };

function parsePerfTimingLog(path: string): PerfRow[] {
  let raw: string;
  try {
    raw = readFileSync(path, "utf8");
  } catch {
    return [];
  }
  // Each dump emits one block of OPENDB_PERF lines, separated by other log
  // lines (warn). The last block in the file is the freshest snapshot.
  const allLines = raw.split(/\n/).filter((l) => l.startsWith("OPENDB_PERF "));
  if (allLines.length === 0) return [];
  // Identify the last block: scan from end, grab consecutive OPENDB_PERF lines.
  const allParsed = allLines.map((l) => {
    const m = l.match(/^OPENDB_PERF span=(\S+) total_ms=([0-9.]+) calls=(\d+) mean_us=([0-9.]+)/);
    if (!m || m[1] == null || m[2] == null || m[3] == null || m[4] == null) return null;
    return { span: m[1], totalMs: parseFloat(m[2]), calls: parseInt(m[3], 10), meanUs: parseFloat(m[4]) };
  }).filter((x): x is PerfRow => x !== null);
  // De-duplicate by span name: each subsequent block re-emits all spans with
  // monotonically increasing totals. Keep only the last occurrence per span.
  const seen = new Map<string, PerfRow>();
  for (const row of allParsed) seen.set(row.span, row);
  return Array.from(seen.values()).sort((a, b) => b.totalMs - a.totalMs);
}

function formatPerfTimingBlock(rows: PerfRow[]): string {
  const lines: string[] = [];
  lines.push("| Span | total_ms | calls | mean_us |");
  lines.push("|------|----------|-------|---------|");
  for (const r of rows) {
    lines.push(`| ${r.span} | ${r.totalMs.toFixed(2)} | ${r.calls} | ${r.meanUs.toFixed(2)} |`);
  }
  return lines.join("\n");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
