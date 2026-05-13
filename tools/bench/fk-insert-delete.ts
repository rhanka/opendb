// Parent/child INSERT + cascade DELETE micro-benchmark.

import { execFile, spawn, type ChildProcessByStdio } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync } from "node:fs";
import net from "node:net";
import { dirname, join } from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const nodeBin = join(repoRoot, "target", "debug", process.platform === "win32" ? "opendb-node.exe" : "opendb-node");

type Options = {
  rows: number;
  withPg: boolean;
  pgHost: string;
  pgPort: number;
  pgUser: string;
  pgDatabase: string;
};

type SocketState = { socket: net.Socket; buffer: Buffer };

function parseOptions(argv: string[]): Options {
  const options: Options = {
    rows: 100,
    withPg: false,
    pgHost: process.env.PGHOST ?? "127.0.0.1",
    pgPort: Number.parseInt(process.env.PGPORT ?? "5432", 10),
    pgUser: process.env.PGUSER ?? "postgres",
    pgDatabase: process.env.PGDATABASE ?? "postgres"
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--rows") {
      const next = argv[index + 1];
      if (next === undefined) throw new Error("--rows requires a value");
      options.rows = Number.parseInt(next, 10);
      index += 1;
    } else if (arg === "--with-pg") {
      options.withPg = true;
    } else if (arg === "--help" || arg === "-h") {
      console.log("usage: bench/fk-insert-delete.ts [--rows N] [--with-pg]");
      process.exit(0);
    } else {
      throw new Error(`unknown bench flag: ${arg}`);
    }
  }
  return options;
}

function reserveFreePort(): Promise<number> {
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

async function buildOpenDbNode(): Promise<void> {
  await execFileAsync("cargo", ["build", "-p", "opendb-node"], {
    cwd: repoRoot,
    encoding: "utf8",
    maxBuffer: 20 * 1024 * 1024,
    timeout: 180_000
  });
}

const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

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

function startupMessage(user: string, database: string): Buffer {
  const params: Array<[string, string]> = [
    ["user", user],
    ["database", database]
  ];
  let payloadLength = 4;
  for (const [k, v] of params) payloadLength += k.length + 1 + v.length + 1;
  payloadLength += 1;
  const length = 4 + payloadLength;
  const buffer = Buffer.alloc(length);
  let offset = 0;
  buffer.writeUInt32BE(length, offset);
  offset += 4;
  buffer.writeUInt32BE(0x00030000, offset);
  offset += 4;
  for (const [k, v] of params) {
    offset += buffer.write(k, offset);
    buffer.writeUInt8(0, offset);
    offset += 1;
    offset += buffer.write(v, offset);
    buffer.writeUInt8(0, offset);
    offset += 1;
  }
  buffer.writeUInt8(0, offset);
  return buffer;
}

function queryMessage(sql: string): Buffer {
  const payload = Buffer.from(`${sql}\0`, "utf8");
  const buffer = Buffer.alloc(1 + 4 + payload.length);
  buffer.writeUInt8(0x51, 0);
  buffer.writeUInt32BE(4 + payload.length, 1);
  payload.copy(buffer, 5);
  return buffer;
}

function connectSocket(host: string, port: number): Promise<SocketState> {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    socket.setTimeout(15_000);
    socket.once("connect", () => resolve({ socket, buffer: Buffer.alloc(0) }));
    socket.once("error", reject);
    socket.once("timeout", () => {
      socket.destroy();
      reject(new Error("socket connect timeout"));
    });
    socket.connect({ host, port });
  });
}

function readBytes(state: SocketState, count: number): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const tryConsume = () => {
      if (state.buffer.length >= count) {
        const chunk = state.buffer.subarray(0, count);
        state.buffer = state.buffer.subarray(count);
        state.socket.off("data", onData);
        state.socket.off("error", onError);
        state.socket.off("close", onClose);
        resolve(chunk);
      }
    };
    const onData = (chunk: Buffer) => {
      state.buffer = Buffer.concat([state.buffer, chunk]);
      tryConsume();
    };
    const onError = (error: Error) => {
      state.socket.off("data", onData);
      state.socket.off("error", onError);
      state.socket.off("close", onClose);
      reject(error);
    };
    const onClose = () => {
      state.socket.off("data", onData);
      state.socket.off("error", onError);
      state.socket.off("close", onClose);
      reject(new Error("closed"));
    };
    state.socket.on("data", onData);
    state.socket.on("error", onError);
    state.socket.on("close", onClose);
    tryConsume();
  });
}

async function readUntilReady(state: SocketState): Promise<void> {
  for (;;) {
    const tagFrame = await readBytes(state, 5);
    const tag = tagFrame.readUInt8(0);
    const length = tagFrame.readUInt32BE(1);
    if (length < 4) throw new Error(`malformed pgwire frame length ${length}`);
    if (length > 4) await readBytes(state, length - 4);
    if (tag === 0x5a) return;
    if (tag === 0x45) throw new Error("pgwire error response");
  }
}

async function performStartup(state: SocketState, user: string, database: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    state.socket.write(startupMessage(user, database), (e) =>
      e === undefined || e === null ? resolve() : reject(e)
    );
  });
  await readUntilReady(state);
}

async function runQuery(state: SocketState, sql: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    state.socket.write(queryMessage(sql), (e) =>
      e === undefined || e === null ? resolve() : reject(e)
    );
  });
  await readUntilReady(state);
}

function percentile(samples: number[], p: number): number {
  if (samples.length === 0) return Number.NaN;
  const sorted = [...samples].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.floor(p * sorted.length)))] ?? Number.NaN;
}

type Summary = {
  engine: string;
  rows: number;
  parent_insert_p50_ms: number;
  child_insert_p50_ms: number;
  cascade_delete_total_ms: number;
};

async function runEngineBench(
  engine: string,
  host: string,
  port: number,
  user: string,
  database: string,
  rows: number
): Promise<Summary> {
  const state = await connectSocket(host, port);
  try {
    await performStartup(state, user, database);
    const suffix = `${Date.now()}_${process.pid}`;
    const parents = `bench_parents_${suffix}`;
    const children = `bench_children_${suffix}`;
    await runQuery(state, `DROP TABLE IF EXISTS ${children}`).catch(() => {});
    await runQuery(state, `DROP TABLE IF EXISTS ${parents}`).catch(() => {});
    await runQuery(state, `CREATE TABLE ${parents} (id INT PRIMARY KEY)`);
    await runQuery(state, `CREATE TABLE ${children} (id INT PRIMARY KEY, parent_id INT)`);
    await runQuery(
      state,
      `ALTER TABLE ${children} ADD CONSTRAINT fk FOREIGN KEY (parent_id) REFERENCES ${parents} (id) ON DELETE CASCADE`
    );

    const parentSamples: number[] = [];
    for (let i = 0; i < rows; i += 1) {
      const t0 = performance.now();
      await runQuery(state, `INSERT INTO ${parents} (id) VALUES (${i})`);
      parentSamples.push(performance.now() - t0);
    }

    const childSamples: number[] = [];
    for (let i = 0; i < rows; i += 1) {
      const t0 = performance.now();
      await runQuery(state, `INSERT INTO ${children} (id, parent_id) VALUES (${i}, ${i})`);
      childSamples.push(performance.now() - t0);
    }

    const deleteStart = performance.now();
    for (let i = 0; i < rows; i += 1) {
      await runQuery(state, `DELETE FROM ${parents} WHERE id = ${i}`);
    }
    const cascadeDelete = performance.now() - deleteStart;

    return {
      engine,
      rows,
      parent_insert_p50_ms: percentile(parentSamples, 0.5),
      child_insert_p50_ms: percentile(childSamples, 0.5),
      cascade_delete_total_ms: cascadeDelete
    };
  } finally {
    state.socket.end();
    state.socket.destroy();
  }
}

async function spawnOpenDbNode(): Promise<{
  cleanup: () => Promise<void>;
  port: number;
}> {
  await buildOpenDbNode();
  const pgwirePort = await reserveFreePort();
  const healthPort = await reserveFreePort();
  const dataDir = mkdtempSync(join(repoRoot, "tmp", "bench-fk-"));
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
      `127.0.0.1:${healthPort}`
    ],
    {
      cwd: repoRoot,
      env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? "warn" },
      stdio: ["ignore", "pipe", "pipe"]
    }
  );
  child.stderr.on("data", () => {});
  child.stdout.on("data", () => {});
  await waitForListener("127.0.0.1", pgwirePort, 15_000);
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

async function main(): Promise<void> {
  const options = parseOptions(process.argv.slice(2));
  mkdirSync(join(repoRoot, "tmp"), { recursive: true });
  const node = await spawnOpenDbNode();
  let openDb: Summary;
  try {
    openDb = await runEngineBench(
      "opendb",
      "127.0.0.1",
      node.port,
      "bench",
      "bench",
      options.rows
    );
  } finally {
    await node.cleanup();
  }
  const summaries: Summary[] = [openDb];
  if (options.withPg) {
    try {
      const pg = await runEngineBench(
        "postgres",
        options.pgHost,
        options.pgPort,
        options.pgUser,
        options.pgDatabase,
        options.rows
      );
      summaries.push(pg);
    } catch (error) {
      process.stderr.write(
        `bench[--with-pg] skipped: ${(error as Error).message ?? String(error)}\n`
      );
    }
  }
  process.stdout.write(`${JSON.stringify({ run: "fk-insert-delete", summaries })}\n`);
}

main().catch((error) => {
  process.stderr.write(`${(error as Error).stack ?? String(error)}\n`);
  process.exit(1);
});
