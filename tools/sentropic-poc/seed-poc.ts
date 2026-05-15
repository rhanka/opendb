// Sprint 18.B: end-to-end seed POC.
//   1. Spawn a fresh opendb-node.
//   2. Replay sentropic's Drizzle migrations (the 21/27 that PASS; the 6
//      data-backfill / composite-PK fails are non-blocking for the
//      `/api/folders` route under test).
//   3. Insert minimal seed via Drizzle (`db.insert(...).values(...)`) into
//      workspaces / organizations / folders / initiatives — the exact code
//      pattern sentropic uses in `db/seed-*.ts`.
//   4. Verify via `db.select(...)` that rows are visible.
//
// This is the gate for Sprint 18.C (HTTP route). If we can seed without any
// modification to sentropic, 18.C is unblocked.

import { execFile, spawn, type ChildProcessByStdio } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { dirname, join } from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { Client, Pool } from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { eq } from "drizzle-orm";
import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

const execFileAsync = promisify(execFile);
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const nodeBin = join(
  repoRoot,
  "target",
  "debug",
  process.platform === "win32" ? "opendb-node.exe" : "opendb-node"
);
const SENTROPIC_MIGRATIONS_DIR = "/home/antoinefa/src/entropiq/api/drizzle";
const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

// --- Drizzle schema mirror — exact subset of api/src/db/schema.ts used by
// `/api/folders`. Loose-typed (we don't enforce server-side anyway).
const workspaces = pgTable("workspaces", {
  id: text("id").primaryKey(),
  ownerUserId: text("owner_user_id"),
  name: text("name").notNull(),
  type: text("type").notNull().default("ai-ideas"),
  gateConfig: jsonb("gate_config"),
  hiddenAt: timestamp("hidden_at", { withTimezone: false }),
  createdAt: timestamp("created_at", { withTimezone: false }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: false }).defaultNow()
});

const organizations = pgTable("organizations", {
  id: text("id").primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  name: text("name").notNull(),
  status: text("status").default("completed"),
  createdAt: timestamp("created_at", { withTimezone: false }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: false }).defaultNow(),
  data: jsonb("data").notNull()
});

const folders = pgTable("folders", {
  id: text("id").primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  name: text("name").notNull(),
  description: text("description"),
  organizationId: text("organization_id"),
  matrixConfig: text("matrix_config"),
  executiveSummary: text("executive_summary"),
  status: text("status").default("completed"),
  createdAt: timestamp("created_at", { withTimezone: false }).notNull().defaultNow()
});

const initiatives = pgTable("initiatives", {
  id: text("id").primaryKey(),
  workspaceId: text("workspace_id").notNull(),
  folderId: text("folder_id").notNull(),
  organizationId: text("organization_id"),
  status: text("status").default("completed"),
  model: text("model"),
  antecedentId: text("antecedent_id"),
  maturityStage: text("maturity_stage"),
  gateStatus: text("gate_status"),
  templateSnapshotId: text("template_snapshot_id"),
  createdAt: timestamp("created_at", { withTimezone: false }).notNull().defaultNow(),
  data: jsonb("data").notNull()
});

// --- Probe framework -----------------------------------------------------
type ProbeOutcome =
  | { kind: "pass"; details: string }
  | { kind: "fail"; details: string };
type ProbeRecord = { id: string; description: string; outcome: ProbeOutcome };
const records: ProbeRecord[] = [];

function record(id: string, description: string, outcome: ProbeOutcome): void {
  records.push({ id, description, outcome });
  const stamp = outcome.kind === "pass" ? "PASS" : "FAIL";
  console.log(`[${stamp}] ${id} — ${description}`);
  console.log(`        ${outcome.details}`);
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

async function waitForListener(host: string, port: number, timeoutMs: number): Promise<void> {
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

async function spawnOpenDbNode(): Promise<{ port: number; cleanup: () => Promise<void> }> {
  console.log("[build] cargo build -p opendb-node ...");
  await execFileAsync("cargo", ["build", "-p", "opendb-node"], {
    cwd: repoRoot,
    encoding: "utf8",
    maxBuffer: 20 * 1024 * 1024,
    timeout: 300_000
  });
  const pgwirePort = await reserveFreePort();
  const healthPort = await reserveFreePort();
  const adminPort = await reserveFreePort();
  const internalPort = await reserveFreePort();
  const tmpDir = join(repoRoot, ".worktrees", ".tmp-claude");
  mkdirSync(tmpDir, { recursive: true });
  const dataDir = mkdtempSync(join(tmpDir, "sentropic-seed-"));
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
  child.stderr.on("data", (c: Buffer) => startupBuf.push(c.toString("utf8")));
  child.stdout.on("data", (c: Buffer) => startupBuf.push(c.toString("utf8")));
  try {
    await waitForListener("127.0.0.1", pgwirePort, 20_000);
  } catch (e) {
    console.error("[opendb-node startup output]\n" + startupBuf.join(""));
    throw e;
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

// Same statement splitter as migrate-poc.ts (split on `--> statement-breakpoint`
// then on top-level `;` outside quotes / dollar bodies / comments).
function splitDrizzleStatements(sql: string): string[] {
  const primary = sql
    .split("--> statement-breakpoint")
    .map((chunk) => chunk.trim())
    .filter((chunk) => chunk.length > 0);
  const out: string[] = [];
  for (const chunk of primary) {
    out.push(...splitOnTopLevelSemicolons(chunk));
  }
  return out;
}

function splitOnTopLevelSemicolons(input: string): string[] {
  const result: string[] = [];
  let buf = "";
  let i = 0;
  let inSingleQuote = false;
  let inDollarBody = false;
  while (i < input.length) {
    const c = input[i];
    if (!inSingleQuote && !inDollarBody) {
      if (c === "-" && input[i + 1] === "-") {
        while (i < input.length && input[i] !== "\n") {
          buf += input[i];
          i += 1;
        }
        continue;
      }
      if (c === "/" && input[i + 1] === "*") {
        buf += "/*";
        i += 2;
        while (i + 1 < input.length && !(input[i] === "*" && input[i + 1] === "/")) {
          buf += input[i];
          i += 1;
        }
        if (i + 1 < input.length) {
          buf += "*/";
          i += 2;
        }
        continue;
      }
      if (c === "$" && input[i + 1] === "$") {
        inDollarBody = true;
        buf += "$$";
        i += 2;
        continue;
      }
    } else if (inDollarBody && c === "$" && input[i + 1] === "$") {
      inDollarBody = false;
      buf += "$$";
      i += 2;
      continue;
    }
    if (!inDollarBody && c === "'") inSingleQuote = !inSingleQuote;
    if (c === ";" && !inSingleQuote && !inDollarBody) {
      const trimmed = buf.trim();
      if (trimmed.length > 0) result.push(trimmed);
      buf = "";
      i += 1;
      continue;
    }
    buf += c;
    i += 1;
  }
  const trailing = buf.trim();
  if (trailing.length > 0) result.push(trailing);
  return result;
}

async function replayMigrations(
  port: number
): Promise<{ ok: number; skipped_files: string[] }> {
  // Replay every migration; record per-file ok/skip but continue past failures
  // so the seed POC isn't blocked by the 6 known-bad data-backfill files.
  const files = readdirSync(SENTROPIC_MIGRATIONS_DIR)
    .filter((n) => n.endsWith(".sql"))
    .sort();
  const client = new Client({
    host: "127.0.0.1",
    port,
    user: "opendb",
    database: "opendb"
  });
  client.on("error", () => {});
  await client.connect();
  let ok = 0;
  const skipped_files: string[] = [];
  try {
    for (const file of files) {
      const sql = readFileSync(join(SENTROPIC_MIGRATIONS_DIR, file), "utf8");
      // Sprint 18.B: best-effort replay — continue past per-statement
      // failures within a single migration file so a stray data-backfill
      // UPDATE doesn't block the rename/alter statements that follow it.
      // We track success at the file level: a file counts as PASS only when
      // every statement ran. Skipped files surface the gap to the user.
      let total = 0;
      let passed = 0;
      for (const statement of splitDrizzleStatements(sql)) {
        total += 1;
        try {
          await client.query(statement);
          passed += 1;
        } catch {
          // continue past the failing statement
        }
      }
      if (passed === total) ok += 1;
      else skipped_files.push(`${file} (${passed}/${total})`);
    }
  } finally {
    await client.end();
  }
  return { ok, skipped_files };
}

async function runSeed(port: number): Promise<void> {
  const pool = new Pool({
    host: "127.0.0.1",
    port,
    user: "opendb",
    database: "opendb"
  });
  pool.on("error", () => {});
  const db = drizzle(pool);

  // S1: workspace
  await probe("S1", "insert workspace", async () => {
    await db.insert(workspaces).values({
      id: "ws-poc",
      ownerUserId: "user-poc",
      name: "POC Workspace",
      type: "ai-ideas"
    });
  });

  // S2: organization (FK → workspace)
  await probe("S2", "insert organization", async () => {
    await db.insert(organizations).values({
      id: "org-poc",
      workspaceId: "ws-poc",
      name: "POC Org",
      status: "completed",
      data: { industry: "tech" }
    });
  });

  // S3: 3 folders
  await probe("S3", "insert 3 folders", async () => {
    await db.insert(folders).values([
      {
        id: "fld-1",
        workspaceId: "ws-poc",
        name: "Folder Alpha",
        organizationId: "org-poc",
        status: "completed"
      },
      {
        id: "fld-2",
        workspaceId: "ws-poc",
        name: "Folder Beta",
        organizationId: "org-poc",
        status: "completed"
      },
      {
        id: "fld-3",
        workspaceId: "ws-poc",
        name: "Folder Gamma",
        organizationId: "org-poc",
        status: "generating"
      }
    ]);
  });

  // S4: 5 initiatives across folders
  await probe("S4", "insert 5 initiatives", async () => {
    await db.insert(initiatives).values([
      { id: "i-1", workspaceId: "ws-poc", folderId: "fld-1", status: "completed", data: {} },
      { id: "i-2", workspaceId: "ws-poc", folderId: "fld-1", status: "completed", data: {} },
      { id: "i-3", workspaceId: "ws-poc", folderId: "fld-1", status: "draft", data: {} },
      { id: "i-4", workspaceId: "ws-poc", folderId: "fld-2", status: "completed", data: {} },
      { id: "i-5", workspaceId: "ws-poc", folderId: "fld-3", status: "completed", data: {} }
    ]);
  });

  // S5: verify all 3 folders are queryable by workspace_id
  await probe("S5", "SELECT folders WHERE workspace_id = 'ws-poc'", async () => {
    const rows = await db
      .select({ id: folders.id, name: folders.name })
      .from(folders)
      .where(eq(folders.workspaceId, "ws-poc"));
    if (rows.length !== 3) {
      throw new Error(`expected 3 folders, got ${rows.length}`);
    }
  });

  // S6: verify initiatives count per folder via aggregation (the route's
  // critical query — same shape as Q5 in real-smoke).
  await probe("S6", "GROUP BY folder + count(initiatives.id)", async () => {
    const { sql: aggSql } = await import("drizzle-orm");
    const rows = await db
      .select({
        id: folders.id,
        initiativeCount: aggSql<number>`count(${initiatives.id})`.as("initiative_count")
      })
      .from(folders)
      .leftJoin(initiatives, eq(initiatives.folderId, folders.id))
      .where(eq(folders.workspaceId, "ws-poc"))
      .groupBy(folders.id);
    const counts = new Map(rows.map((r) => [r.id, Number(r.initiativeCount)]));
    if (counts.get("fld-1") !== 3 || counts.get("fld-2") !== 1 || counts.get("fld-3") !== 1) {
      throw new Error(`unexpected counts: ${JSON.stringify(Object.fromEntries(counts))}`);
    }
  });

  await pool.end().catch(() => {});
}

async function probe(id: string, description: string, body: () => Promise<void>): Promise<void> {
  try {
    await body();
    record(id, description, { kind: "pass", details: "OK" });
  } catch (e) {
    const message = e instanceof Error ? `${e.name}: ${e.message}` : String(e);
    record(id, description, { kind: "fail", details: message });
  }
}

function renderVerdict(migrate: { ok: number; skipped_files: string[] }): string {
  const lines: string[] = [];
  lines.push(`# Sentropic seed POC (Sprint 18.B) — ${new Date().toISOString().slice(0, 10)}`);
  lines.push("");
  lines.push(
    `Replayed ${migrate.ok}/27 migrations (skipped: ${migrate.skipped_files.join(", ") || "none"}), then seeded via Drizzle into workspaces/organizations/folders/initiatives.`
  );
  lines.push("");
  lines.push("## Seed verdict");
  lines.push("");
  lines.push("| Probe | Outcome | Details |");
  lines.push("|------|---------|---------|");
  for (const r of records) {
    const o = r.outcome.kind === "pass" ? "PASS" : "FAIL";
    const det = r.outcome.details.replace(/\|/g, "\\|").slice(0, 200);
    lines.push(`| ${r.id} | ${o} | ${det} |`);
  }
  const passes = records.filter((r) => r.outcome.kind === "pass").length;
  const total = records.length;
  lines.push("");
  lines.push(`**Verdict: ${passes}/${total} seed probes PASS**`);
  lines.push("");
  if (passes === total) {
    lines.push("→ Sprint 18.C (HTTP route) unblocked.");
  } else {
    lines.push("→ Investigate the first failing seed probe before Sprint 18.C.");
  }
  return lines.join("\n");
}

async function main(): Promise<void> {
  const node = await spawnOpenDbNode();
  let migrate = { ok: 0, skipped_files: [] as string[] };
  try {
    migrate = await replayMigrations(node.port);
    console.log(`[migrations] ${migrate.ok}/27 PASS`);
    await runSeed(node.port);
  } finally {
    await node.cleanup();
  }
  const verdict = renderVerdict(migrate);
  console.log("");
  console.log(verdict);
  const reportPath = join(
    repoRoot,
    "docs",
    "bench",
    `sentropic-seed-${new Date().toISOString().slice(0, 10)}.md`
  );
  mkdirSync(dirname(reportPath), { recursive: true });
  writeFileSync(reportPath, verdict + "\n", "utf8");
  console.log(`\n[ok] wrote ${reportPath}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
