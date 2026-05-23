// Phase B prep (2026-05-22) — multi-client benchmark for opendb-node.
//
// Spawns one opendb-node release binary, one PG 16-alpine container,
// seeds a small `bench_kv (id BIGINT PK, payload TEXT)` fixture into
// each, then opens N concurrent pg.Client connections and runs M
// iterations of (autocommit) INSERT random key + SELECT random key per
// client.
//
// Reports aggregate TPS + per-client latency p50/p95 + opendb/PG
// ratio. The goal is to measure Phase B.2 (lock narrowing on the
// global Database Mutex) wins without depending on pgbench-init —
// which is still blocked on the COPY pgwire protocol.
//
// Env knobs:
//   BENCH_CLIENTS         (default 4)   N concurrent clients
//   BENCH_ITERATIONS      (default 500) iterations per client
//   BENCH_SEED_ROWS       (default 200) rows pre-seeded in the table
//   BENCH_OPENDB_ONLY     (default 0)   if 1, skip PG side
//   OPENDB_NODE_BIN       override the binary path

import { spawn, type ChildProcessByStdio } from "node:child_process";
import { appendFileSync, createWriteStream, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import net from "node:net";
import { dirname, join } from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { Client } from "pg";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");

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
    } catch { /* keep going */ }
  }
  return fallback;
}
const nodeBin = pickNodeBin();

const CLIENTS = readPositiveInt("BENCH_CLIENTS", 4);
const ITERATIONS = readPositiveInt("BENCH_ITERATIONS", 500);
const SEED_ROWS = readPositiveInt("BENCH_SEED_ROWS", 200);
const OPENDB_ONLY = process.env.BENCH_OPENDB_ONLY === "1";

const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function readPositiveInt(name: string, defaultValue: number): number {
  const raw = process.env[name];
  const value = raw === undefined ? defaultValue : Number(raw);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer, got ${raw}`);
  }
  return value;
}

async function reserveFreePort(): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const server = net.createServer();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address && typeof address === "object") {
        const port = address.port;
        server.close(() => resolve(port));
      } else {
        server.close();
        reject(new Error("could not bind a free port"));
      }
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
  const dataDir = mkdtempSync(join(tmpDir, "bench-concurrent-"));
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
    child.stderr.on("data", (chunk: Buffer | string) => { stream.write(chunk); });
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
    }
  }};
}

type PerfRow = { span: string; totalMs: number; calls: number; meanUs: number };

function parsePerfTimingLog(path: string): PerfRow[] {
  let raw: string;
  try { raw = readFileSync(path, "utf8"); } catch { return []; }
  const lines = raw.split(/\n/).filter((l) => l.startsWith("OPENDB_PERF "));
  if (lines.length === 0) return [];
  const seen = new Map<string, PerfRow>();
  for (const l of lines) {
    const m = l.match(/^OPENDB_PERF span=(\S+) total_ms=([0-9.]+) calls=(\d+) mean_us=([0-9.]+)/);
    if (!m || m[1] == null || m[2] == null || m[3] == null || m[4] == null) continue;
    seen.set(m[1], { span: m[1], totalMs: parseFloat(m[2]), calls: parseInt(m[3], 10), meanUs: parseFloat(m[4]) });
  }
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

async function spawnPostgres(): Promise<{ port: number; cleanup: () => Promise<void> }> {
  const port = await reserveFreePort();
  const container = `opendb-bench-concurrent-pg-${port}`;
  console.log(`[pg] docker run postgres:16-alpine on ${port} (container=${container})`);
  await new Promise<void>((resolve, reject) => {
    const child = spawn("docker", ["run", "-d", "--rm", "--name", container,
      "-p", `${port}:5432`, "-e", "POSTGRES_PASSWORD=bench", "-e", "POSTGRES_USER=opendb",
      "postgres:16-alpine", "-c", "synchronous_commit=on"],
      { stdio: ["ignore", "ignore", "inherit"] });
    child.on("exit", (code) => code === 0 ? resolve() : reject(new Error(`docker run rc=${code}`)));
  });
  // wait for PG ready by polling pg_isready inside the container
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const code = await new Promise<number | null>((resolve) => {
      const child = spawn("docker", ["exec", container, "pg_isready", "-q", "-U", "opendb"],
        { stdio: "ignore" });
      child.on("exit", (c) => resolve(c));
    });
    if (code === 0) break;
    await delay(500);
  }
  console.log(`[pg] ready`);
  return { port, cleanup: async () => {
    await new Promise<void>((resolve) => {
      const child = spawn("docker", ["rm", "-f", container], { stdio: "ignore" });
      child.on("exit", () => resolve());
    });
  }};
}

function makeOpendbFactory(port: number): () => Promise<Client> {
  return async () => {
    const client = new Client({ host: "127.0.0.1", port, user: "opendb", database: "postgres" });
    await client.connect();
    return client;
  };
}

function makePgFactory(port: number): () => Promise<Client> {
  return async () => {
    const client = new Client({ host: "127.0.0.1", port, user: "opendb", password: "bench", database: "postgres" });
    await client.connect();
    return client;
  };
}

async function applySchemaAndSeed(factory: () => Promise<Client>, label: string): Promise<void> {
  const client = await factory();
  try {
    await client.query(`DROP TABLE IF EXISTS bench_kv`);
    await client.query(`CREATE TABLE bench_kv (id BIGINT NOT NULL PRIMARY KEY, payload TEXT)`);
    // Seed in batches of 50 via multi-row INSERT to keep the seed fast.
    const batch = 50;
    for (let i = 0; i < SEED_ROWS; i += batch) {
      const tuples: string[] = [];
      for (let j = i; j < Math.min(i + batch, SEED_ROWS); j += 1) {
        tuples.push(`(${j}, 'seed-${j}')`);
      }
      await client.query(`INSERT INTO bench_kv (id, payload) VALUES ${tuples.join(",")}`);
    }
    console.log(`[${label}] seeded ${SEED_ROWS} rows`);
  } finally {
    await client.end();
  }
}

function quantile(sorted: number[], q: number): number {
  if (sorted.length === 0) return 0;
  const pos = (sorted.length - 1) * q;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  if (lo === hi) return sorted[lo]!;
  const frac = pos - lo;
  return sorted[lo]! * (1 - frac) + sorted[hi]! * frac;
}

type ClientResult = { clientIndex: number; iterations: number; totalMs: number; latencies: number[] };

async function runClient(
  factory: () => Promise<Client>,
  clientIndex: number,
  label: string
): Promise<ClientResult> {
  const client = await factory();
  const latencies: number[] = new Array(ITERATIONS);
  // Each client picks a distinct key range for its INSERTs so concurrent
  // clients don't collide on the primary key.
  const insertBase = SEED_ROWS + clientIndex * ITERATIONS;
  const wallStart = process.hrtime.bigint();
  try {
    for (let i = 0; i < ITERATIONS; i += 1) {
      const insertId = insertBase + i;
      const selectId = Math.floor(Math.random() * SEED_ROWS);
      const t0 = process.hrtime.bigint();
      await client.query(`INSERT INTO bench_kv (id, payload) VALUES (${insertId}, 'client-${clientIndex}-iter-${i}')`);
      await client.query(`SELECT payload FROM bench_kv WHERE id = ${selectId}`);
      const t1 = process.hrtime.bigint();
      latencies[i] = Number(t1 - t0) / 1_000_000;
    }
  } catch (error) {
    console.error(`[${label}] client ${clientIndex} failed at iteration:`, error);
    throw error;
  } finally {
    await client.end();
  }
  const wallEnd = process.hrtime.bigint();
  return {
    clientIndex,
    iterations: ITERATIONS,
    totalMs: Number(wallEnd - wallStart) / 1_000_000,
    latencies
  };
}

type LabelStats = {
  label: string;
  clients: number;
  iterationsPerClient: number;
  totalIterations: number;
  totalWallMs: number;
  tps: number;
  meanLatencyMs: number;
  p50: number;
  p95: number;
  p99: number;
  maxClientWallMs: number;
};

function summarize(label: string, results: ClientResult[], wallMs: number): LabelStats {
  const allLatencies = results.flatMap((r) => r.latencies);
  allLatencies.sort((a, b) => a - b);
  const total = results.reduce((s, r) => s + r.iterations, 0);
  const meanLatency = allLatencies.reduce((s, v) => s + v, 0) / Math.max(allLatencies.length, 1);
  return {
    label,
    clients: results.length,
    iterationsPerClient: ITERATIONS,
    totalIterations: total,
    totalWallMs: wallMs,
    tps: (total / wallMs) * 1000,
    meanLatencyMs: meanLatency,
    p50: quantile(allLatencies, 0.5),
    p95: quantile(allLatencies, 0.95),
    p99: quantile(allLatencies, 0.99),
    maxClientWallMs: Math.max(...results.map((r) => r.totalMs))
  };
}

async function runEngine(factory: () => Promise<Client>, label: string): Promise<LabelStats> {
  await applySchemaAndSeed(factory, label);
  console.log(`[${label}] running ${CLIENTS} concurrent clients x ${ITERATIONS} iterations`);
  const wallStart = process.hrtime.bigint();
  const results = await Promise.all(
    Array.from({ length: CLIENTS }, (_, i) => runClient(factory, i, label))
  );
  const wallEnd = process.hrtime.bigint();
  const wallMs = Number(wallEnd - wallStart) / 1_000_000;
  const stats = summarize(label, results, wallMs);
  console.log(`[${label}] done: ${stats.totalIterations} iterations in ${stats.totalWallMs.toFixed(0)} ms = ${stats.tps.toFixed(1)} TPS  (mean ${stats.meanLatencyMs.toFixed(2)} ms, p50 ${stats.p50.toFixed(2)}, p95 ${stats.p95.toFixed(2)}, p99 ${stats.p99.toFixed(2)})`);
  return stats;
}

function formatReport(opendb: LabelStats, pg: LabelStats | null): string {
  const lines: string[] = [];
  lines.push(`# Concurrent bench — opendb-node vs PostgreSQL 16 — ${new Date().toISOString().slice(0, 10)}`);
  lines.push("");
  lines.push(`Each client opens its own pg.Client, runs **${ITERATIONS} autocommit (INSERT + SELECT) pairs** on a shared \`bench_kv (id BIGINT PRIMARY KEY, payload TEXT)\` table seeded with ${SEED_ROWS} rows. Clients use disjoint INSERT key ranges so they do not collide on the PK.`);
  lines.push("");
  lines.push("## Run parameters");
  lines.push("| Parameter | Value |");
  lines.push("|-----------|-------|");
  lines.push(`| clients | ${CLIENTS} |`);
  lines.push(`| iterations per client | ${ITERATIONS} |`);
  lines.push(`| seed rows | ${SEED_ROWS} |`);
  lines.push("");
  lines.push("## Aggregate");
  lines.push("| Engine | TPS | wall ms | mean ms | p50 | p95 | p99 |");
  lines.push("|--------|-----|---------|---------|-----|-----|-----|");
  lines.push(`| opendb-node | ${opendb.tps.toFixed(1)} | ${opendb.totalWallMs.toFixed(0)} | ${opendb.meanLatencyMs.toFixed(2)} | ${opendb.p50.toFixed(2)} | ${opendb.p95.toFixed(2)} | ${opendb.p99.toFixed(2)} |`);
  if (pg) {
    lines.push(`| PostgreSQL 16 | ${pg.tps.toFixed(1)} | ${pg.totalWallMs.toFixed(0)} | ${pg.meanLatencyMs.toFixed(2)} | ${pg.p50.toFixed(2)} | ${pg.p95.toFixed(2)} | ${pg.p99.toFixed(2)} |`);
    const ratio = pg.tps / Math.max(opendb.tps, 0.01);
    lines.push("");
    lines.push(`**PG / OpenDB TPS ratio: ${ratio.toFixed(2)}×** (1.0 = parity; >1.0 = PG faster)`);
  }
  return lines.join("\n");
}

async function main(): Promise<void> {
  const opendbNode = await spawnOpenDbNode();
  let pgNode: { port: number; cleanup: () => Promise<void> } | null = null;
  if (!OPENDB_ONLY) {
    try {
      pgNode = await spawnPostgres();
    } catch (error) {
      console.error("[pg] failed to start container, falling back to opendb-only mode:", error);
    }
  }
  try {
    const opendb = await runEngine(makeOpendbFactory(opendbNode.port), "opendb");
    let pg: LabelStats | null = null;
    if (pgNode) {
      pg = await runEngine(makePgFactory(pgNode.port), "pg");
    }
    const report = formatReport(opendb, pg);
    console.log("\n" + report);
    const dateStamp = new Date().toISOString().slice(0, 10);
    const reportPath = join(repoRoot, "docs", "bench", `bench-concurrent-${dateStamp}.md`);
    mkdirSync(dirname(reportPath), { recursive: true });
    const { writeFileSync } = await import("node:fs");
    writeFileSync(reportPath, report + "\n", "utf8");
    console.log(`\n[ok] wrote ${reportPath}`);
    if (opendbNode.perfLogPath) {
      console.log(`[opendb] perf timing log saved at ${opendbNode.perfLogPath}`);
    }
  } finally {
    await opendbNode.cleanup();
    if (pgNode) await pgNode.cleanup();
    if (opendbNode.perfLogPath) {
      const rows = parsePerfTimingLog(opendbNode.perfLogPath);
      if (rows.length > 0) {
        const block = formatPerfTimingBlock(rows);
        console.log("\n" + block);
        const dateStamp = new Date().toISOString().slice(0, 10);
        const reportPath = join(repoRoot, "docs", "bench", `bench-concurrent-${dateStamp}.md`);
        appendFileSync(reportPath, "\n\n## Per-span timing (`OPENDB_PERF_TIMING=1`)\n\n" + block + "\n", "utf8");
      }
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
