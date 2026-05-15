// Sprint 18.A: replay sentropic's actual Drizzle migrations against
// opendb-node and produce a per-migration verdict. Reads the SQL files
// directly from /home/antoinefa/src/sentropic/api/drizzle/ (no modification
// to the sentropic repo).
//
// Each migration file is a sequence of statements separated by Drizzle's
// `--> statement-breakpoint` marker. We execute statements one at a time and
// fail-fast within a single migration but continue across migrations so the
// verdict reports as many gaps as possible in one pass.

import { execFile, spawn, type ChildProcessByStdio } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { dirname, join } from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { Client } from "pg";

const execFileAsync = promisify(execFile);
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const nodeBin = join(
  repoRoot,
  "target",
  "debug",
  process.platform === "win32" ? "opendb-node.exe" : "opendb-node"
);
const SENTROPIC_MIGRATIONS_DIR = "/home/antoinefa/src/sentropic/api/drizzle";

const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

type MigrationOutcome =
  | { kind: "pass"; statements: number }
  | { kind: "fail"; statements_ran: number; failed_statement: string; error: string };

type MigrationRecord = {
  file: string;
  outcome: MigrationOutcome;
};

const records: MigrationRecord[] = [];

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
  const dataDir = mkdtempSync(join(tmpDir, "sentropic-migrate-"));
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

function splitDrizzleStatements(sql: string): string[] {
  // First-pass: split on Drizzle's explicit `--> statement-breakpoint`
  // marker (used by `drizzle-kit generate`).
  const primary = sql
    .split("--> statement-breakpoint")
    .map((chunk) => chunk.trim())
    .filter((chunk) => chunk.length > 0);
  // Second-pass: hand-written migrations (0020, 0021) skip the marker and
  // rely on raw `;` to separate statements. Re-split each primary chunk on
  // top-level semicolons (outside single-quoted strings, $$-delimited
  // bodies, and `--`/`/* */` comments). DO blocks contain semicolons inside
  // their `$$ ... $$` bodies so that delimiter must be respected.
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
      // `--` line comment skip
      if (c === "-" && input[i + 1] === "-") {
        while (i < input.length && input[i] !== "\n") {
          buf += input[i];
          i += 1;
        }
        continue;
      }
      // `/* */` block comment skip
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
      // `$$` enter dollar-quoted body
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
    if (!inDollarBody && c === "'") {
      inSingleQuote = !inSingleQuote;
    }
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

async function runMigrations(port: number): Promise<void> {
  const files = readdirSync(SENTROPIC_MIGRATIONS_DIR)
    .filter((name) => name.endsWith(".sql"))
    .sort();
  const client = new Client({
    host: "127.0.0.1",
    port,
    user: "opendb",
    database: "opendb"
  });
  client.on("error", () => {});
  await client.connect();
  try {
    for (const file of files) {
      const sql = readFileSync(join(SENTROPIC_MIGRATIONS_DIR, file), "utf8");
      const statements = splitDrizzleStatements(sql);
      let ranCount = 0;
      let failed: { statement: string; error: string } | null = null;
      for (const statement of statements) {
        try {
          await client.query(statement);
          ranCount += 1;
        } catch (error) {
          const message =
            error instanceof Error ? `${error.name}: ${error.message}` : String(error);
          failed = { statement, error: message };
          break;
        }
      }
      if (failed) {
        records.push({
          file,
          outcome: {
            kind: "fail",
            statements_ran: ranCount,
            failed_statement: failed.statement.slice(0, 240),
            error: failed.error
          }
        });
        console.log(
          `[FAIL] ${file} — ${ranCount}/${statements.length} statements ran, then: ${failed.error}`
        );
      } else {
        records.push({
          file,
          outcome: { kind: "pass", statements: statements.length }
        });
        console.log(`[PASS] ${file} — ${statements.length} statements`);
      }
    }
  } finally {
    await client.end();
  }
}

function renderVerdict(): string {
  const lines: string[] = [];
  lines.push(`# Sentropic migration replay (Sprint 18.A) — ${new Date().toISOString().slice(0, 10)}`);
  lines.push("");
  lines.push(
    `Source: \`${SENTROPIC_MIGRATIONS_DIR}\` (read-only, no modification to sentropic repo).`
  );
  lines.push("");
  lines.push("## Per-migration verdict");
  lines.push("");
  lines.push("| Migration | Outcome | Statements | Detail |");
  lines.push("|-----------|---------|------------|--------|");
  for (const r of records) {
    if (r.outcome.kind === "pass") {
      lines.push(`| ${r.file} | PASS | ${r.outcome.statements} | — |`);
    } else {
      const stmt = r.outcome.failed_statement.replace(/\s+/g, " ").replace(/\|/g, "\\|");
      const err = r.outcome.error.replace(/\|/g, "\\|").slice(0, 200);
      lines.push(
        `| ${r.file} | FAIL | ${r.outcome.statements_ran} ran | \`${stmt.slice(0, 100)}\` → ${err} |`
      );
    }
  }
  const passes = records.filter((r) => r.outcome.kind === "pass").length;
  const total = records.length;
  const ratio = total === 0 ? 0 : Math.round((passes / total) * 100);
  lines.push("");
  lines.push(`**Verdict: ${passes}/${total} migrations PASS (${ratio}%)**`);
  lines.push("");
  if (ratio < 50) {
    lines.push(
      "→ Décision : STOP Sprint 18. Audit chiffré des verbes SQL manquants nécessaire avant de continuer."
    );
  } else if (ratio < 100) {
    lines.push(
      "→ Décision : ping user pour arbitrage. Une partie des migrations passe ; la suite (seed, route HTTP) sera bloquée par les gaps restants."
    );
  } else {
    lines.push("→ Décision : 100 % des migrations PASS, on enchaîne Sprint 18.B (seed minimal).");
  }
  return lines.join("\n");
}

async function main(): Promise<void> {
  const node = await spawnOpenDbNode();
  try {
    await runMigrations(node.port);
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
    `sentropic-migrate-${new Date().toISOString().slice(0, 10)}.md`
  );
  mkdirSync(dirname(reportPath), { recursive: true });
  writeFileSync(reportPath, verdict + "\n", "utf8");
  console.log(`\n[ok] wrote ${reportPath}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
