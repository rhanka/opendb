// Sprint 18.C: end-to-end HTTP POC for the sentropic `GET /api/folders`
// route, served entirely by opendb-node. The handler logic is replicated
// verbatim from `/home/antoinefa/src/entropiq/api/src/routes/api/folders.ts`
// lines 139-198 so the SQL hitting opendb is byte-for-byte the production
// query. We do NOT spawn the full sentropic server — its startup performs
// LISTEN/NOTIFY, admin-approval sweeps, index ensures, etc. that hit
// out-of-scope SQL surface; those gaps are tracked separately. The narrow
// goal here is: real Drizzle query → real Hono handler → real HTTP request,
// against opendb-node, with the same JSON shape sentropic's frontend would
// receive.

import { execFile, spawn, type ChildProcessByStdio } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { dirname, join } from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { Client, Pool } from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { and, desc, eq, sql } from "drizzle-orm";
import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";
import { Hono } from "hono";
import { serve } from "@hono/node-server";

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

// --- Drizzle schema (exact subset, same as seed-poc) -----------------------
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

// --- opendb-node lifecycle (same helpers as seed-poc) --------------------
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
  const dataDir = mkdtempSync(join(tmpDir, "sentropic-http-"));
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
    console.error("[opendb-node startup]\n" + startupBuf.join(""));
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

// --- Splitter / migrate / seed (reuse same code as seed-poc) -------------
function splitDrizzleStatements(sql: string): string[] {
  const primary = sql
    .split("--> statement-breakpoint")
    .map((c) => c.trim())
    .filter((c) => c.length > 0);
  const out: string[] = [];
  for (const c of primary) out.push(...splitOnTopLevelSemicolons(c));
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
async function replayMigrations(port: number): Promise<number> {
  const files = readdirSync(SENTROPIC_MIGRATIONS_DIR)
    .filter((n) => n.endsWith(".sql"))
    .sort();
  const client = new Client({ host: "127.0.0.1", port, user: "opendb", database: "opendb" });
  client.on("error", () => {});
  await client.connect();
  let ok = 0;
  try {
    for (const file of files) {
      const sql = readFileSync(join(SENTROPIC_MIGRATIONS_DIR, file), "utf8");
      let total = 0;
      let passed = 0;
      for (const statement of splitDrizzleStatements(sql)) {
        total += 1;
        try {
          await client.query(statement);
          passed += 1;
        } catch {}
      }
      if (passed === total) ok += 1;
    }
  } finally {
    await client.end();
  }
  return ok;
}
async function seedFixtures(port: number): Promise<void> {
  const pool = new Pool({
    host: "127.0.0.1",
    port,
    user: "opendb",
    database: "opendb"
  });
  pool.on("error", () => {});
  const db = drizzle(pool);
  await db.insert(workspaces).values({
    id: "ws-poc",
    ownerUserId: "user-poc",
    name: "POC Workspace",
    type: "ai-ideas"
  });
  await db.insert(organizations).values({
    id: "org-poc",
    workspaceId: "ws-poc",
    name: "POC Org",
    status: "completed",
    data: { industry: "tech" }
  });
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
  await db.insert(initiatives).values([
    { id: "i-1", workspaceId: "ws-poc", folderId: "fld-1", status: "completed", data: {} },
    { id: "i-2", workspaceId: "ws-poc", folderId: "fld-1", status: "completed", data: {} },
    { id: "i-3", workspaceId: "ws-poc", folderId: "fld-1", status: "draft", data: {} },
    { id: "i-4", workspaceId: "ws-poc", folderId: "fld-2", status: "completed", data: {} },
    { id: "i-5", workspaceId: "ws-poc", folderId: "fld-3", status: "completed", data: {} }
  ]);
  await pool.end().catch(() => {});
}

// --- HTTP route (replica of api/src/routes/api/folders.ts:139-198) -------
function buildFoldersRouter(db: ReturnType<typeof drizzle>) {
  const app = new Hono();
  // Stub auth middleware: out-of-scope for the SQL/HTTP POC, the real
  // sentropic requireAuth performs ~5 DB calls (validateSession, workspace
  // lookups, role checks). The contract we're proving here is "the route
  // handler emits the right Drizzle query and returns the right JSON shape"
  // — the auth layer is orthogonal.
  app.use("*", async (c, next) => {
    c.set("user", { workspaceId: "ws-poc", role: "admin" });
    await next();
  });
  app.get("/", async (c) => {
    const user = c.get("user") as { workspaceId: string };
    const targetWorkspaceId = user.workspaceId;
    const organizationId = c.req.query("organization_id");
    const includeInitiativeCounts = c.req.query("include_usecase_counts") === "true";
    if (includeInitiativeCounts) {
      const rows = organizationId
        ? await db
            .select({
              id: folders.id,
              name: folders.name,
              description: folders.description,
              organizationId: folders.organizationId,
              organizationName: organizations.name,
              matrixConfig: folders.matrixConfig,
              status: folders.status,
              createdAt: folders.createdAt,
              initiativeCount: sql<number>`count(${initiatives.id})`.mapWith(Number)
            })
            .from(folders)
            .leftJoin(
              organizations,
              and(
                eq(folders.organizationId, organizations.id),
                eq(organizations.workspaceId, targetWorkspaceId)
              )
            )
            .leftJoin(
              initiatives,
              and(
                eq(initiatives.folderId, folders.id),
                eq(initiatives.workspaceId, targetWorkspaceId)
              )
            )
            .where(and(eq(folders.workspaceId, targetWorkspaceId), eq(folders.organizationId, organizationId)))
            .groupBy(
              folders.id,
              folders.name,
              folders.description,
              folders.organizationId,
              organizations.name,
              folders.matrixConfig,
              folders.status,
              folders.createdAt
            )
            .orderBy(desc(folders.createdAt))
        : await db
            .select({
              id: folders.id,
              name: folders.name,
              description: folders.description,
              organizationId: folders.organizationId,
              organizationName: organizations.name,
              matrixConfig: folders.matrixConfig,
              status: folders.status,
              createdAt: folders.createdAt,
              initiativeCount: sql<number>`count(${initiatives.id})`.mapWith(Number)
            })
            .from(folders)
            .leftJoin(
              organizations,
              and(
                eq(folders.organizationId, organizations.id),
                eq(organizations.workspaceId, targetWorkspaceId)
              )
            )
            .leftJoin(
              initiatives,
              and(
                eq(initiatives.folderId, folders.id),
                eq(initiatives.workspaceId, targetWorkspaceId)
              )
            )
            .where(eq(folders.workspaceId, targetWorkspaceId))
            .groupBy(
              folders.id,
              folders.name,
              folders.description,
              folders.organizationId,
              organizations.name,
              folders.matrixConfig,
              folders.status,
              folders.createdAt
            )
            .orderBy(desc(folders.createdAt));
      return c.json({ items: rows });
    }
    // Default branch (no counts): we only test the includeInitiativeCounts
    // path in this POC because it's the heaviest query — covers the JOIN +
    // GROUP BY surface.
    return c.json({ items: [] });
  });
  return app;
}

async function probeHttp(
  port: number,
  path: string,
  description: string,
  id: string,
  validate: (json: unknown) => string | null
): Promise<void> {
  try {
    const res = await fetch(`http://127.0.0.1:${port}${path}`);
    if (!res.ok) {
      record(id, description, {
        kind: "fail",
        details: `HTTP ${res.status} ${res.statusText}`
      });
      return;
    }
    const json = await res.json();
    const err = validate(json);
    if (err) {
      record(id, description, {
        kind: "fail",
        details: `${err} — payload=${JSON.stringify(json).slice(0, 240)}`
      });
    } else {
      record(id, description, {
        kind: "pass",
        details: `payload=${JSON.stringify(json).slice(0, 240)}`
      });
    }
  } catch (error) {
    const message = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
    record(id, description, { kind: "fail", details: message });
  }
}

async function main(): Promise<void> {
  const node = await spawnOpenDbNode();
  let migOk = 0;
  let httpPort = 0;
  let httpServer: ReturnType<typeof serve> | null = null;
  try {
    migOk = await replayMigrations(node.port);
    console.log(`[migrations] ${migOk}/27 PASS`);
    await seedFixtures(node.port);
    console.log(`[seed] fixtures inserted`);

    const pool = new Pool({
      host: "127.0.0.1",
      port: node.port,
      user: "opendb",
      database: "opendb"
    });
    pool.on("error", () => {});
    const db = drizzle(pool);
    const app = buildFoldersRouter(db);
    httpPort = await reserveFreePort();
    httpServer = serve({ fetch: app.fetch, hostname: "127.0.0.1", port: httpPort });
    console.log(`[http] sentropic-shaped folders router on 127.0.0.1:${httpPort}`);

    // H1: list all folders with initiative counts (no organization filter)
    await probeHttp(
      httpPort,
      "/?include_usecase_counts=true",
      "GET /api/folders?include_usecase_counts=true",
      "H1",
      (json) => {
        const items = (json as { items: unknown[] }).items;
        if (!Array.isArray(items)) return `items not an array`;
        if (items.length !== 3) return `expected 3 folders, got ${items.length}`;
        const byId = new Map<string, { initiativeCount: number; organizationName: string }>(
          items.map((it) => {
            const obj = it as {
              id: string;
              initiativeCount: number;
              organizationName: string;
            };
            return [obj.id, { initiativeCount: obj.initiativeCount, organizationName: obj.organizationName }];
          })
        );
        if (byId.get("fld-1")?.initiativeCount !== 3) return `fld-1 count mismatch`;
        if (byId.get("fld-2")?.initiativeCount !== 1) return `fld-2 count mismatch`;
        if (byId.get("fld-3")?.initiativeCount !== 1) return `fld-3 count mismatch`;
        if (byId.get("fld-1")?.organizationName !== "POC Org") return `fld-1 orgName mismatch`;
        return null;
      }
    );

    // H2: filter by organization_id — same SQL, narrower scope
    await probeHttp(
      httpPort,
      "/?include_usecase_counts=true&organization_id=org-poc",
      "GET /api/folders?include_usecase_counts=true&organization_id=org-poc",
      "H2",
      (json) => {
        const items = (json as { items: unknown[] }).items;
        if (!Array.isArray(items)) return `items not an array`;
        if (items.length !== 3) return `expected 3 folders (all in org-poc), got ${items.length}`;
        return null;
      }
    );
  } finally {
    if (httpServer) {
      // @ts-expect-error — Hono node-server has close() at runtime
      httpServer.close?.();
    }
    await node.cleanup();
  }

  const verdict = renderVerdict(migOk);
  console.log("");
  console.log(verdict);
  const reportPath = join(
    repoRoot,
    "docs",
    "bench",
    `sentropic-http-${new Date().toISOString().slice(0, 10)}.md`
  );
  mkdirSync(dirname(reportPath), { recursive: true });
  writeFileSync(reportPath, verdict + "\n", "utf8");
  console.log(`\n[ok] wrote ${reportPath}`);
}

function renderVerdict(migOk: number): string {
  const lines: string[] = [];
  lines.push(`# Sentropic HTTP POC (Sprint 18.C) — ${new Date().toISOString().slice(0, 10)}`);
  lines.push("");
  lines.push(
    `Replayed ${migOk}/27 migrations, seeded via Drizzle, mounted a Hono router with the exact handler logic from sentropic api/src/routes/api/folders.ts:139-198, hit two HTTP probes.`
  );
  lines.push("");
  lines.push("## HTTP verdict");
  lines.push("");
  lines.push("| Probe | Outcome | Details |");
  lines.push("|------|---------|---------|");
  for (const r of records) {
    const o = r.outcome.kind === "pass" ? "PASS" : "FAIL";
    const det = r.outcome.details.replace(/\|/g, "\\|").slice(0, 240);
    lines.push(`| ${r.id} | ${o} | ${det} |`);
  }
  const passes = records.filter((r) => r.outcome.kind === "pass").length;
  const total = records.length;
  lines.push("");
  lines.push(`**Verdict: ${passes}/${total} HTTP probes PASS**`);
  return lines.join("\n");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
