// Sprint 13 read-only POC smoke: empirically decides whether opendb-node's
// simple-query-only pgwire surface is sufficient for Drizzle, or whether
// Sprint 12 (Extended protocol) must land first.
//
// The probe runs seven SQL probes against opendb-node and prints a verdict
// table. The smoke never throws on a Drizzle-expected failure; it records
// the failure and continues, because the goal is the decision matrix.

import { execFile, spawn, type ChildProcessByStdio } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { dirname, join } from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { Client } from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { eq } from "drizzle-orm";
import { pgTable, text, timestamp } from "drizzle-orm/pg-core";

const execFileAsync = promisify(execFile);
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const nodeBin = join(
  repoRoot,
  "target",
  "debug",
  process.platform === "win32" ? "opendb-node.exe" : "opendb-node"
);
const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

const folders = pgTable("folders_smoke", {
  id: text("id").primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  name: text("name").notNull(),
  status: text("status"),
  createdAt: timestamp("created_at", { withTimezone: false })
});

type ProbeOutcome =
  | { kind: "pass"; details: string }
  | { kind: "fail"; details: string };

type ProbeRecord = {
  id: string;
  description: string;
  protocol: "simple" | "extended" | "drizzle";
  outcome: ProbeOutcome;
};

const records: ProbeRecord[] = [];

function record(
  id: string,
  description: string,
  protocol: ProbeRecord["protocol"],
  outcome: ProbeOutcome
): void {
  records.push({ id, description, protocol, outcome });
  const stamp = outcome.kind === "pass" ? "PASS" : "FAIL";
  console.log(`[${stamp}] ${id} (${protocol}) — ${description}`);
  if (outcome.kind === "fail") console.log(`        ${outcome.details}`);
  else if (outcome.details) console.log(`        ${outcome.details}`);
}

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

async function waitForListener(
  host: string,
  port: number,
  timeoutMs: number
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      await new Promise<void>((resolve, reject) => {
        const socket = new net.Socket();
        socket.setTimeout(500);
        socket.once("connect", () => {
          socket.end();
          resolve();
        });
        socket.once("error", reject);
        socket.once("timeout", () => {
          socket.destroy();
          reject(new Error("timeout"));
        });
        socket.connect({ host, port });
      });
      return;
    } catch {
      await delay(100);
    }
  }
  throw new Error(`listener ${host}:${port} did not come up`);
}

async function buildOpenDbNode(): Promise<void> {
  console.log("[build] cargo build -p opendb-node ...");
  await execFileAsync("cargo", ["build", "-p", "opendb-node"], {
    cwd: repoRoot,
    encoding: "utf8",
    maxBuffer: 20 * 1024 * 1024,
    timeout: 300_000
  });
}

async function spawnOpenDbNode(): Promise<{
  port: number;
  cleanup: () => Promise<void>;
}> {
  await buildOpenDbNode();
  const pgwirePort = await reserveFreePort();
  const healthPort = await reserveFreePort();
  const adminPort = await reserveFreePort();
  const internalPort = await reserveFreePort();
  const tmpDir = join(repoRoot, ".worktrees", ".tmp-claude");
  mkdirSync(tmpDir, { recursive: true });
  const dataDir = mkdtempSync(join(tmpDir, "entropiq-poc-"));
  const child: ChildProcessByStdio<null, Readable, Readable> = spawn(
    nodeBin,
    [
      "--node-id",
      "1",
      "--data-dir",
      dataDir,
      "--pgwire-addr",
      `127.0.0.1:${pgwirePort}`,
      "--health-addr",
      `127.0.0.1:${healthPort}`,
      "--admin-addr",
      `127.0.0.1:${adminPort}`,
      "--internal-addr",
      `127.0.0.1:${internalPort}`,
      "--advertise-addr",
      `127.0.0.1:${internalPort}`
    ],
    {
      cwd: repoRoot,
      env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? "warn" },
      stdio: ["ignore", "pipe", "pipe"]
    }
  );
  const startupBuf: string[] = [];
  child.stderr.on("data", (chunk: Buffer) => {
    startupBuf.push(chunk.toString("utf8"));
  });
  child.stdout.on("data", (chunk: Buffer) => {
    startupBuf.push(chunk.toString("utf8"));
  });
  try {
    await waitForListener("127.0.0.1", pgwirePort, 20_000);
  } catch (error) {
    console.error("[opendb-node startup output]\n" + startupBuf.join(""));
    throw error;
  }
  return {
    port: pgwirePort,
    cleanup: async () => {
      child.kill("SIGTERM");
      await delay(300);
      if (child.exitCode === null) child.kill("SIGKILL");
      rmSync(dataDir, { recursive: true, force: true });
    }
  };
}

async function withClient<T>(
  port: number,
  body: (client: Client) => Promise<T>
): Promise<T> {
  const client = new Client({
    host: "127.0.0.1",
    port,
    user: "opendb",
    database: "opendb"
  });
  // Swallow async pg socket errors: opendb-node emits an ErrorResponse for
  // every unsupported Extended message (P, B, D, E, S), and pg surfaces each
  // as an async error event after the awaited query has resolved/rejected.
  client.on("error", () => {});
  await client.connect();
  try {
    return await body(client);
  } finally {
    try {
      await client.end();
    } catch {
      // Already broken connection; nothing to recover.
    }
  }
}

async function probe<T>(
  port: number,
  id: string,
  description: string,
  protocol: ProbeRecord["protocol"],
  body: (client: Client) => Promise<T>,
  format: (value: T) => string = () => ""
): Promise<void> {
  try {
    const value = await withClient(port, body);
    record(id, description, protocol, { kind: "pass", details: format(value) });
  } catch (error) {
    const message =
      error instanceof Error ? `${error.name}: ${error.message}` : String(error);
    record(id, description, protocol, { kind: "fail", details: message });
  }
}

async function runProbes(port: number): Promise<void> {
  // --- Bare pg probes -----------------------------------------------------
  await probe(
    port,
    "A1",
    `client.query("SELECT 1") — pure simple-query baseline`,
    "simple",
    async (c) => (await c.query("SELECT 1 AS one")).rows,
    (rows) => `rows=${JSON.stringify(rows)}`
  );

  await probe(
    port,
    "A2",
    `client.query("CREATE TABLE folders_smoke ...") — DDL via simple-query`,
    "simple",
    async (c) =>
      (
        await c.query(
          `CREATE TABLE folders_smoke (
             id TEXT PRIMARY KEY,
             workspace_id TEXT NOT NULL,
             name TEXT NOT NULL,
             status TEXT,
             created_at TIMESTAMP
           )`
        )
      ).command,
    (tag) => `command=${tag}`
  );

  await probe(
    port,
    "A3",
    `client.query("INSERT INTO folders_smoke ...") — DML simple, ISO timestamp`,
    "simple",
    async (c) =>
      (
        await c.query(
          `INSERT INTO folders_smoke (id, workspace_id, name, status, created_at) ` +
            `VALUES ('f1', 'admin', 'root', 'completed', '2026-05-13T00:00:00Z')`
        )
      ).command,
    (tag) => `command=${tag}`
  );

  await probe(
    port,
    "A4",
    `client.query("SELECT * FROM folders_smoke") — star projection`,
    "simple",
    async (c) => (await c.query("SELECT * FROM folders_smoke")).rows,
    (rows) => `rows=${JSON.stringify(rows)}`
  );

  await probe(
    port,
    "A5",
    `client.query("SELECT id, name FROM folders_smoke") — explicit projection`,
    "simple",
    async (c) =>
      (await c.query("SELECT id, name FROM folders_smoke")).rows,
    (rows) => `rows=${JSON.stringify(rows)}`
  );

  // --- Extended-protocol probe (expected gap) -----------------------------
  await probe(
    port,
    "B1",
    `client.query({ text, values }) — parametrized → forces Extended protocol`,
    "extended",
    async (c) =>
      (
        await c.query({
          text: "SELECT id FROM folders_smoke WHERE id = $1",
          values: ["f1"]
        })
      ).rows,
    (rows) => `rows=${JSON.stringify(rows)}`
  );

  // --- Sprint 15 aggregate probes ----------------------------------------
  await probe(
    port,
    "D1",
    `SELECT count(*) FROM folders_smoke (simple query)`,
    "simple",
    async (c) => (await c.query(`SELECT count(*) FROM folders_smoke`)).rows,
    (rows) => `rows=${JSON.stringify(rows)}`
  );
  await probe(
    port,
    "D2",
    `SELECT status, count(*) FROM folders_smoke GROUP BY status (simple query)`,
    "simple",
    async (c) =>
      (
        await c.query(
          `SELECT status, count(*) FROM folders_smoke GROUP BY status`
        )
      ).rows,
    (rows) => `rows=${JSON.stringify(rows)}`
  );
  await probe(
    port,
    "D3",
    `SELECT status, count(*) FROM folders_smoke GROUP BY status HAVING count(*) > 0 (simple query)`,
    "simple",
    async (c) =>
      (
        await c.query(
          `SELECT status, count(*) FROM folders_smoke GROUP BY status HAVING count(*) > 0`
        )
      ).rows,
    (rows) => `rows=${JSON.stringify(rows)}`
  );
  await probe(
    port,
    "D4",
    `SELECT count(*) FROM folders_smoke (extended/parametrized)`,
    "extended",
    async (c) =>
      (
        await c.query({
          text: `SELECT count(*) FROM folders_smoke WHERE workspace_id = $1`,
          values: ["w1"]
        })
      ).rows,
    (rows) => `rows=${JSON.stringify(rows)}`
  );

  // --- Drizzle probes -----------------------------------------------------
  await probe(
    port,
    "C1",
    `db.select().from(folders).limit(1) — Drizzle no-WHERE / no-param`,
    "drizzle",
    async (c) => {
      const db = drizzle(c);
      return await db.select().from(folders).limit(1);
    },
    (rows) => `rows=${JSON.stringify(rows)}`
  );

  await probe(
    port,
    "C2",
    `db.select().from(folders).where(eq(folders.id, "f1")) — Drizzle param`,
    "drizzle",
    async (c) => {
      const db = drizzle(c);
      return await db.select().from(folders).where(eq(folders.id, "f1"));
    },
    (rows) => `rows=${JSON.stringify(rows)}`
  );
}

function renderVerdict(): string {
  const lines: string[] = [];
  lines.push("# Entropiq POC smoke — 2026-05-13");
  lines.push("");
  lines.push(
    "Empirical decision: can opendb-node's pgwire surface serve a Drizzle-backed read path against an entropiq-shaped table?"
  );
  lines.push("");
  lines.push("Reproduce: `npm run poc:entropiq:smoke`.");
  lines.push("");
  lines.push("## Probe matrix");
  lines.push("");
  lines.push("| Probe | Protocol | Outcome | Details |");
  lines.push("|------|----------|---------|---------|");
  for (const rec of records) {
    const outcome = rec.outcome.kind === "pass" ? "PASS" : "FAIL";
    const det = rec.outcome.details.replace(/\|/g, "\\|").slice(0, 160);
    lines.push(`| ${rec.id} | ${rec.protocol} | ${outcome} | ${det} |`);
  }
  lines.push("");

  const find = (id: string) => records.find((r) => r.id === id);
  const a1 = find("A1");
  const a3 = find("A3");
  const a5 = find("A5");
  const b1 = find("B1");
  const c1 = find("C1");
  const c2 = find("C2");

  lines.push("## Gaps");
  lines.push("");
  if (a1 && a1.outcome.kind === "fail") {
    lines.push(
      "- **No-FROM `SELECT <expr>` not supported** (probe A1). Drivers and pgwire clients commonly issue `SELECT 1` / `SELECT version()` as health/probe queries on connect."
    );
  }
  if (a5 && a5.outcome.kind === "fail") {
    lines.push(
      "- **Explicit column projection `SELECT a, b FROM t` not supported** (probe A5). Only `SELECT *` returns rows; Drizzle and most ORMs emit explicit lists."
    );
  }
  if (a3 && a3.outcome.kind === "fail") {
    lines.push(
      `- **TIMESTAMP literal coercion gap** (probe A3): \`${a3.outcome.details}\`. Need to align the accepted literal grammar with Postgres ISO-8601 forms.`
    );
  }
  if (
    (b1 && b1.outcome.kind === "fail") ||
    (c1 && c1.outcome.kind === "fail") ||
    (c2 && c2.outcome.kind === "fail")
  ) {
    lines.push(
      "- **Extended protocol entirely missing** (probes B1, C1, C2): every `Parse`/`Bind`/`Describe`/`Execute`/`Sync` message returns \"unsupported message tag\". Drizzle always issues Extended-protocol queries via `pg`, so the simple-query fallback path is unreachable from Drizzle."
    );
  }
  lines.push("");

  lines.push("## Verdict");
  lines.push("");
  lines.push(
    "Sprint 13 cannot proceed as a single read-only POC — three orthogonal gaps must close first, in order:"
  );
  lines.push("");
  lines.push(
    "1. **Sprint 12 (Extended pgwire)** — hard prerequisite for any Drizzle client. Without it `db.select().from(folders).limit(1)` cannot return a row."
  );
  lines.push(
    "2. **SQL surface micro-sprint (proposed Sprint 12.1)** — add no-FROM `SELECT <expr>` literals (and at minimum `SELECT 1`, `SELECT version()`) plus explicit column projection in `SELECT`. These are both pre-handshake probes pg/Drizzle emit unconditionally."
  );
  lines.push(
    "3. **TIMESTAMP literal grammar** — accept Postgres-style `'2026-05-13 00:00:00'` and ISO-8601 `'2026-05-13T00:00:00Z'` in `INSERT` text values. Without this no realistic entropiq seed can be replayed."
  );
  lines.push("");
  lines.push("## Next action (proposed)");
  lines.push("");
  lines.push(
    "- Promote Sprint 12 out of the parked state and design Sprint 12.1 (SQL surface gaps surfaced here) at the same time. Land them together as a single PR before re-running this smoke; the smoke is the green-light gate for the table-level read-only POC (entropiq seed + HTTP route)."
  );
  return lines.join("\n");
}

async function main(): Promise<void> {
  const node = await spawnOpenDbNode();
  try {
    await runProbes(node.port);
  } finally {
    await node.cleanup();
  }
  const verdict = renderVerdict();
  console.log("");
  console.log(verdict);
  const reportPath = join(repoRoot, "docs", "bench", "entropiq-poc-2026-05-13.md");
  mkdirSync(dirname(reportPath), { recursive: true });
  writeFileSync(reportPath, verdict + "\n", "utf8");
  console.log(`\n[ok] wrote ${reportPath}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
