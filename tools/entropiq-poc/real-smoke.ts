// Sprint 15.E corrective probe: rejoue des requêtes Drizzle/SQL réelles
// extraites des routes entropiq contre opendb-node. Le but est de prouver
// (ou réfuter) que le moteur SQL valide aux smoke A/B/C/D fonctionne aussi
// sur le SQL que l'API entropiq émet en production, sans rien modifier côté
// entropiq.
//
// Les requêtes ci-dessous sont copiées 1:1 depuis :
//   - api/src/routes/api/admin.ts (counts simples)
//   - api/src/scripts/queue-status.ts (GROUP BY status)
//   - api/src/routes/api/folders.ts (LEFT JOIN + count + multi-col GROUP BY)
//   - api/src/routes/api/admin.ts (sélection par PK)
// Le schéma est miroir exact de api/src/db/schema.ts pour les 5 tables
// touchées par ces requêtes. Aucun fichier entropiq n'est lu à l'exécution.

import { execFile, spawn, type ChildProcessByStdio } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import net from "node:net";
import { dirname, join } from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { Client } from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import { and, desc, eq, sql } from "drizzle-orm";
import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";

const execFileAsync = promisify(execFile);
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const nodeBin = join(
  repoRoot,
  "target",
  "debug",
  process.platform === "win32" ? "opendb-node.exe" : "opendb-node"
);
const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

// --- Schema mirror (entropiq api/src/db/schema.ts) ------------------------
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

const jobQueue = pgTable("job_queue", {
  id: text("id").primaryKey(),
  type: text("type").notNull(),
  status: text("status").notNull().default("pending"),
  workspaceId: text("workspace_id").notNull(),
  createdAt: timestamp("created_at", { withTimezone: false }).notNull().defaultNow()
});

// --- Probe machinery ------------------------------------------------------
type ProbeOutcome =
  | { kind: "pass"; details: string }
  | { kind: "fail"; details: string };

type ProbeRecord = {
  id: string;
  description: string;
  sql: string;
  outcome: ProbeOutcome;
};

const records: ProbeRecord[] = [];

function record(
  id: string,
  description: string,
  sqlText: string,
  outcome: ProbeOutcome
): void {
  records.push({ id, description, sql: sqlText, outcome });
  const stamp = outcome.kind === "pass" ? "PASS" : "FAIL";
  console.log(`[${stamp}] ${id} — ${description}`);
  console.log(`        sql=${sqlText}`);
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

async function spawnOpenDbNode(): Promise<{
  port: number;
  cleanup: () => Promise<void>;
}> {
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
  const dataDir = mkdtempSync(join(tmpDir, "entropiq-real-"));
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

// --- DDL + seed -----------------------------------------------------------
// CREATE TABLE statements match the migrations entropiq runs in prod.
// Kept minimal (FK omitted) so seed is order-independent in this smoke; the
// goal is SQL surface validation, not constraint replay.
const CREATE_STATEMENTS: string[] = [
  `CREATE TABLE workspaces (
     id TEXT PRIMARY KEY,
     owner_user_id TEXT,
     name TEXT NOT NULL,
     type TEXT NOT NULL DEFAULT 'ai-ideas',
     gate_config JSONB,
     hidden_at TIMESTAMP,
     created_at TIMESTAMP NOT NULL DEFAULT NOW(),
     updated_at TIMESTAMP DEFAULT NOW()
   )`,
  `CREATE TABLE organizations (
     id TEXT PRIMARY KEY,
     workspace_id TEXT NOT NULL,
     name TEXT NOT NULL,
     status TEXT DEFAULT 'completed',
     created_at TIMESTAMP NOT NULL DEFAULT NOW(),
     updated_at TIMESTAMP DEFAULT NOW(),
     data JSONB NOT NULL
   )`,
  `CREATE TABLE folders (
     id TEXT PRIMARY KEY,
     workspace_id TEXT NOT NULL,
     name TEXT NOT NULL,
     description TEXT,
     organization_id TEXT,
     matrix_config TEXT,
     executive_summary TEXT,
     status TEXT DEFAULT 'completed',
     created_at TIMESTAMP NOT NULL DEFAULT NOW()
   )`,
  `CREATE TABLE initiatives (
     id TEXT PRIMARY KEY,
     workspace_id TEXT NOT NULL,
     folder_id TEXT NOT NULL,
     organization_id TEXT,
     status TEXT DEFAULT 'completed',
     model TEXT,
     antecedent_id TEXT,
     maturity_stage TEXT,
     gate_status TEXT,
     template_snapshot_id TEXT,
     created_at TIMESTAMP NOT NULL DEFAULT NOW(),
     data JSONB NOT NULL
   )`,
  `CREATE TABLE job_queue (
     id TEXT PRIMARY KEY,
     type TEXT NOT NULL,
     status TEXT NOT NULL DEFAULT 'pending',
     workspace_id TEXT NOT NULL,
     created_at TIMESTAMP NOT NULL DEFAULT NOW()
   )`
];

const SEED_STATEMENTS: string[] = [
  // Workspaces
  `INSERT INTO workspaces (id, owner_user_id, name, type, created_at) VALUES ('w1', 'u1', 'Acme', 'ai-ideas', '2026-05-01T00:00:00Z')`,
  `INSERT INTO workspaces (id, owner_user_id, name, type, created_at) VALUES ('w2', 'u2', 'Beta', 'opportunity', '2026-05-02T00:00:00Z')`,
  // Organizations
  `INSERT INTO organizations (id, workspace_id, name, status, created_at, data) VALUES ('org1', 'w1', 'Org A', 'completed', '2026-05-03T00:00:00Z', '{}')`,
  `INSERT INTO organizations (id, workspace_id, name, status, created_at, data) VALUES ('org2', 'w1', 'Org B', 'completed', '2026-05-03T00:00:00Z', '{}')`,
  // Folders
  `INSERT INTO folders (id, workspace_id, name, organization_id, status, created_at) VALUES ('f1', 'w1', 'Folder A', 'org1', 'completed', '2026-05-04T00:00:00Z')`,
  `INSERT INTO folders (id, workspace_id, name, organization_id, status, created_at) VALUES ('f2', 'w1', 'Folder B', 'org1', 'completed', '2026-05-04T01:00:00Z')`,
  `INSERT INTO folders (id, workspace_id, name, organization_id, status, created_at) VALUES ('f3', 'w1', 'Folder C', 'org2', 'generating', '2026-05-04T02:00:00Z')`,
  // Initiatives
  `INSERT INTO initiatives (id, workspace_id, folder_id, status, created_at, data) VALUES ('i1', 'w1', 'f1', 'completed', '2026-05-05T00:00:00Z', '{}')`,
  `INSERT INTO initiatives (id, workspace_id, folder_id, status, created_at, data) VALUES ('i2', 'w1', 'f1', 'draft', '2026-05-05T01:00:00Z', '{}')`,
  `INSERT INTO initiatives (id, workspace_id, folder_id, status, created_at, data) VALUES ('i3', 'w1', 'f2', 'completed', '2026-05-05T02:00:00Z', '{}')`,
  // Job queue
  `INSERT INTO job_queue (id, type, status, workspace_id, created_at) VALUES ('j1', 'use_case_list', 'pending', 'w1', '2026-05-06T00:00:00Z')`,
  `INSERT INTO job_queue (id, type, status, workspace_id, created_at) VALUES ('j2', 'use_case_list', 'pending', 'w1', '2026-05-06T01:00:00Z')`,
  `INSERT INTO job_queue (id, type, status, workspace_id, created_at) VALUES ('j3', 'use_case_detail', 'completed', 'w1', '2026-05-06T02:00:00Z')`
];

async function setupSchema(port: number): Promise<void> {
  const client = new Client({
    host: "127.0.0.1",
    port,
    user: "opendb",
    database: "opendb"
  });
  client.on("error", () => {});
  await client.connect();
  try {
    for (const stmt of CREATE_STATEMENTS) {
      await client.query(stmt);
    }
    for (const stmt of SEED_STATEMENTS) {
      await client.query(stmt);
    }
  } finally {
    await client.end();
  }
}

// --- Real queries (verbatim from entropiq routes) -------------------------
async function runRealQueries(port: number): Promise<void> {
  // Drizzle wires a pg Pool; we use it to also surface the SQL text via
  // `.toSQL()` for the verdict report.
  const pool = new Pool({
    host: "127.0.0.1",
    port,
    user: "opendb",
    database: "opendb"
  });
  pool.on("error", () => {});
  const db = drizzle(pool);

  type QueryDef = {
    id: string;
    description: string;
    build: () => { sql: string; rows: () => Promise<unknown[]> };
  };

  const queries: QueryDef[] = [
    {
      id: "Q1",
      description: "admin.ts:55 — count(*) sur organizations",
      build: () => {
        const q = db
          .select({ count: sql<number>`count(*)` })
          .from(organizations);
        return { sql: q.toSQL().sql, rows: () => q };
      }
    },
    {
      id: "Q2",
      description: "admin.ts:56 — count(*) sur folders",
      build: () => {
        const q = db.select({ count: sql<number>`count(*)` }).from(folders);
        return { sql: q.toSQL().sql, rows: () => q };
      }
    },
    {
      id: "Q3",
      description: "queue-status.ts:10 — count(*) GROUP BY status sur job_queue",
      build: () => {
        const q = db
          .select({
            status: jobQueue.status,
            count: sql<number>`COUNT(*)`.as("count")
          })
          .from(jobQueue)
          .groupBy(jobQueue.status);
        return { sql: q.toSQL().sql, rows: () => q };
      }
    },
    {
      id: "Q4",
      description: "admin.ts:108 — select id FROM folders WHERE workspaceId = 'w1'",
      build: () => {
        const q = db
          .select({ id: folders.id })
          .from(folders)
          .where(eq(folders.workspaceId, "w1"));
        return { sql: q.toSQL().sql, rows: () => q };
      }
    },
    {
      id: "Q5",
      description:
        "folders.ts:188 — folders LEFT JOIN initiatives, count + multi-col GROUP BY",
      build: () => {
        const q = db
          .select({
            id: folders.id,
            name: folders.name,
            status: folders.status,
            initiativeCount: sql<number>`count(${initiatives.id})`
          })
          .from(folders)
          .leftJoin(
            initiatives,
            and(
              eq(initiatives.folderId, folders.id),
              eq(initiatives.workspaceId, "w1")
            )
          )
          .where(eq(folders.workspaceId, "w1"))
          .groupBy(folders.id, folders.name, folders.status);
        return { sql: q.toSQL().sql, rows: () => q };
      }
    },
    {
      id: "Q6",
      description: "queue-status.ts:23 — recent jobs ORDER BY created_at DESC LIMIT 10",
      build: () => {
        const q = db
          .select()
          .from(jobQueue)
          .orderBy(desc(jobQueue.createdAt))
          .limit(10);
        return { sql: q.toSQL().sql, rows: () => q };
      }
    },
    {
      id: "Q7",
      description: "workspaces — select par PK eq",
      build: () => {
        const q = db.select().from(workspaces).where(eq(workspaces.id, "w1"));
        return { sql: q.toSQL().sql, rows: () => q };
      }
    },
    {
      id: "Q8",
      description: "initiatives — count(*) WHERE status = 'completed'",
      build: () => {
        const q = db
          .select({ n: sql<number>`count(*)` })
          .from(initiatives)
          .where(eq(initiatives.status, "completed"));
        return { sql: q.toSQL().sql, rows: () => q };
      }
    },
    // Sprint 16.A — INSERT .returning() (no projection: returns all cols)
    {
      id: "Q9",
      description: "folders.ts — db.insert(folders).values(...).returning()",
      build: () => {
        const q = db
          .insert(folders)
          .values({
            id: "f-q9",
            workspaceId: "w1",
            name: "Q9 folder",
            status: "completed",
            createdAt: new Date("2026-05-20T00:00:00Z")
          })
          .returning();
        return { sql: q.toSQL().sql, rows: () => q };
      }
    },
    // Sprint 16.A — INSERT .returning({ id: t.id }) — projection-only
    {
      id: "Q10",
      description:
        "challenge-manager.ts — db.insert(t).values(...).returning({ id: t.id })",
      build: () => {
        const q = db
          .insert(folders)
          .values({
            id: "f-q10",
            workspaceId: "w1",
            name: "Q10 folder"
          })
          .returning({ id: folders.id });
        return { sql: q.toSQL().sql, rows: () => q };
      }
    },
    // Sprint 16.B — UPDATE .returning({ matrixConfig: t.matrixConfig })
    // verbatim from folders.ts: matrix config swap returning the new value.
    {
      id: "Q11",
      description: "folders.ts — db.update(t).set(...).where(...).returning(...)",
      build: () => {
        const q = db
          .update(folders)
          .set({ matrixConfig: '{"x":1}' })
          .where(eq(folders.id, "f-q9"))
          .returning({ matrixConfig: folders.matrixConfig });
        return { sql: q.toSQL().sql, rows: () => q };
      }
    },
    // Sprint 16.B — DELETE .returning() (entropiq queue-clear pattern)
    {
      id: "Q12",
      description: "queue-clear.ts — db.delete(t).returning()",
      build: () => {
        const q = db.delete(folders).where(eq(folders.id, "f-q10")).returning();
        return { sql: q.toSQL().sql, rows: () => q };
      }
    }
  ];

  for (const { id, description, build } of queries) {
    const { sql: text, rows } = build();
    try {
      const result = await rows();
      record(id, description, text, {
        kind: "pass",
        details: `rows=${JSON.stringify(result).slice(0, 240)}`
      });
    } catch (error) {
      const message =
        error instanceof Error ? `${error.name}: ${error.message}` : String(error);
      record(id, description, text, { kind: "fail", details: message });
    }
  }

  await pool.end().catch(() => {});
}

function renderVerdict(): string {
  const lines: string[] = [];
  lines.push("# Entropiq corrective probe (Sprint 15.E) — " + new Date().toISOString().slice(0, 10));
  lines.push("");
  lines.push(
    "Rejeu de requêtes Drizzle copiées 1:1 depuis les routes entropiq, contre opendb-node, sans modification entropiq."
  );
  lines.push("");
  lines.push("## Matrix");
  lines.push("");
  lines.push("| Probe | Outcome | SQL | Details |");
  lines.push("|------|---------|-----|---------|");
  for (const r of records) {
    const o = r.outcome.kind === "pass" ? "PASS" : "FAIL";
    const det = r.outcome.details.replace(/\|/g, "\\|").slice(0, 180);
    const sqlOneLine = r.sql.replace(/\s+/g, " ").replace(/\|/g, "\\|").slice(0, 200);
    lines.push(`| ${r.id} | ${o} | \`${sqlOneLine}\` | ${det} |`);
  }
  const passes = records.filter((r) => r.outcome.kind === "pass").length;
  const total = records.length;
  const ratio = total === 0 ? 0 : Math.round((passes / total) * 100);
  lines.push("");
  lines.push(`**Verdict: ${passes}/${total} PASS (${ratio}%)**`);
  lines.push("");
  if (ratio >= 70) {
    lines.push("→ Décision : poursuivre Sprint 16 (`.returning()` + PK composite). Gaps résiduels tracés en TODO.");
  } else if (ratio <= 30) {
    lines.push("→ Décision : STOP Sprint 16. Audit chiffré des gaps réels avant tout sprint additionnel.");
  } else {
    lines.push("→ Décision : ping user pour arbitrage. Le moteur SQL valide ~50% des requêtes entropiq mais reste insuffisant pour POC HTTP.");
  }
  return lines.join("\n");
}

async function main(): Promise<void> {
  const node = await spawnOpenDbNode();
  try {
    await setupSchema(node.port);
    await runRealQueries(node.port);
  } finally {
    await node.cleanup();
  }
  const verdict = renderVerdict();
  console.log("");
  console.log(verdict);
  const reportPath = join(
    repoRoot,
    "docs",
    "bench",
    `entropiq-real-${new Date().toISOString().slice(0, 10)}.md`
  );
  mkdirSync(dirname(reportPath), { recursive: true });
  const { writeFileSync } = await import("node:fs");
  writeFileSync(reportPath, verdict + "\n", "utf8");
  console.log(`\n[ok] wrote ${reportPath}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
