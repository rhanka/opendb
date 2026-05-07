import { execFile, spawn, type ChildProcessByStdio } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import http from "node:http";
import net from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { expect, test } from "vitest";

const execFileAsync = promisify(execFile);
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const tsxBin = join(repoRoot, "node_modules", ".bin", process.platform === "win32" ? "tsx.cmd" : "tsx");
const nodeBin = join(repoRoot, "target", "debug", process.platform === "win32" ? "opendb-node.exe" : "opendb-node");
const pgwireSmokePath = join(repoRoot, "tools", "pgwire-smoke.ts");
const pollIntervalMs = 100;
const readinessTimeoutMs = 10_000;
const childExitTimeoutMs = 2_000;
const maxCapturedOutputLength = 64 * 1024;

test("opendb-node accepts create, insert, and select over pgwire", async () => {
  const output = await runSqlSmokeParity();

  expect(output).toContain("pgwire smoke passed");
}, 180_000);

async function runSqlSmokeParity(): Promise<string> {
  await buildOpenDbNode();

  const ports = await reserveFreeLocalPorts(2);
  const pgwirePort = ports[0];
  const healthPort = ports[1];
  if (pgwirePort === undefined || healthPort === undefined) {
    throw new Error(`expected two reserved ports, got ${ports.length}`);
  }

  const dataDir = mkdtempSync(join(tmpdir(), "opendb-sql-smoke-"));
  const nodeProcess = spawn(
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
      env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? "info" },
      stdio: ["ignore", "pipe", "pipe"]
    }
  );
  const nodeOutput = captureOutput(nodeProcess);
  const nodeExit = trackExit(nodeProcess);

  try {
    await waitForReady("health endpoint", () => requestHealth(healthPort), nodeExit, nodeOutput);
    await waitForReady("pgwire listener", () => requestTcp(pgwirePort), nodeExit, nodeOutput);
    return await runPgwireSmoke(pgwirePort);
  } catch (error) {
    throw new Error(`${formatUnknownError(error)}\n\nopendb-node output:\n${nodeOutput.text()}`);
  } finally {
    await stopChild(nodeProcess, nodeExit);
    rmSync(dataDir, { recursive: true, force: true });
  }
}

async function buildOpenDbNode(): Promise<void> {
  try {
    await execFileAsync("cargo", ["build", "-p", "opendb-node"], {
      cwd: repoRoot,
      encoding: "utf8",
      maxBuffer: 20 * 1024 * 1024,
      timeout: 120_000
    });
  } catch (error) {
    throw new Error(`cargo build -p opendb-node failed\n${formatExecError(error)}`);
  }
}

async function runPgwireSmoke(port: number): Promise<string> {
  try {
    const { stdout, stderr } = await execFileAsync(tsxBin, [pgwireSmokePath], {
      cwd: repoRoot,
      encoding: "utf8",
      env: {
        ...process.env,
        OPENDB_PGWIRE_HOST: "127.0.0.1",
        OPENDB_PGWIRE_PORT: String(port)
      },
      maxBuffer: 1024 * 1024,
      timeout: 15_000
    });

    return [stdout, stderr].filter(Boolean).join("\n");
  } catch (error) {
    throw new Error(`pgwire smoke failed\n${formatExecError(error)}`);
  }
}

async function reserveFreeLocalPorts(count: number): Promise<number[]> {
  const servers: net.Server[] = [];

  try {
    for (let index = 0; index < count; index += 1) {
      const server = net.createServer();
      await listenOnEphemeralLocalPort(server);
      servers.push(server);
    }

    return servers.map((server) => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        throw new Error(`unexpected server address ${String(address)}`);
      }
      return address.port;
    });
  } finally {
    await Promise.all(servers.map(closeServer));
  }
}

function listenOnEphemeralLocalPort(server: net.Server): Promise<void> {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      server.off("error", onError);
      server.off("listening", onListening);
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const onListening = () => {
      cleanup();
      resolve();
    };

    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(0, "127.0.0.1");
  });
}

function closeServer(server: net.Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error === undefined) {
        resolve();
      } else {
        reject(error);
      }
    });
  });
}

async function waitForReady(
  name: string,
  probe: () => Promise<void>,
  nodeExit: ExitTracker,
  nodeOutput: CapturedOutput
): Promise<void> {
  const deadline = Date.now() + readinessTimeoutMs;
  let lastError: unknown;

  while (Date.now() < deadline) {
    if (nodeExit.result !== undefined) {
      throw new Error(`opendb-node exited before ${name} was ready: ${formatExit(nodeExit.result)}`);
    }

    try {
      await probe();
      return;
    } catch (error) {
      lastError = error;
    }

    await delay(pollIntervalMs);
  }

  throw new Error(
    `timed out waiting for ${name}: ${formatUnknownError(lastError)}\n\nlatest opendb-node output:\n${nodeOutput.text()}`
  );
}

function requestHealth(port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = http.request(
      {
        host: "127.0.0.1",
        method: "GET",
        path: "/healthz",
        port,
        timeout: 500
      },
      (response) => {
        const chunks: Buffer[] = [];

        response.on("data", (chunk: Buffer) => {
          chunks.push(chunk);
        });
        response.on("end", () => {
          const body = Buffer.concat(chunks).toString("utf8");
          if (response.statusCode === 200 && body === "ok\n") {
            resolve();
            return;
          }
          reject(new Error(`health returned ${response.statusCode ?? "unknown"} with body ${JSON.stringify(body)}`));
        });
      }
    );

    request.on("timeout", () => {
      request.destroy(new Error(`health request timed out on port ${port}`));
    });
    request.on("error", reject);
    request.end();
  });
}

function requestTcp(port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: "127.0.0.1", port });
    const cleanup = () => {
      socket.off("connect", onConnect);
      socket.off("error", onError);
      socket.off("timeout", onTimeout);
    };
    const onConnect = () => {
      cleanup();
      socket.end();
      resolve();
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const onTimeout = () => {
      cleanup();
      socket.destroy();
      reject(new Error(`tcp connection timed out on port ${port}`));
    };

    socket.setTimeout(500);
    socket.once("connect", onConnect);
    socket.once("error", onError);
    socket.once("timeout", onTimeout);
  });
}

function captureOutput(child: OpenDbNodeProcess): CapturedOutput {
  let stdout = "";
  let stderr = "";

  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk: string) => {
    stdout = appendCapturedOutput(stdout, chunk);
  });
  child.stderr.on("data", (chunk: string) => {
    stderr = appendCapturedOutput(stderr, chunk);
  });

  return {
    text: () => [section("stdout", stdout), section("stderr", stderr)].filter(Boolean).join("\n")
  };
}

function appendCapturedOutput(current: string, chunk: string): string {
  return (current + chunk).slice(-maxCapturedOutputLength);
}

function section(name: string, value: string): string {
  return value.length > 0 ? `${name}:\n${value}` : "";
}

function trackExit(child: OpenDbNodeProcess): ExitTracker {
  let result: ProcessExit | undefined;
  const promise = new Promise<ProcessExit>((resolve) => {
    child.once("exit", (code, signal) => {
      result = { code, signal };
      resolve(result);
    });
    child.once("error", (error) => {
      result = { code: null, error, signal: null };
      resolve(result);
    });
  });

  return {
    get result() {
      return result;
    },
    promise
  };
}

async function stopChild(child: OpenDbNodeProcess, exit: ExitTracker): Promise<void> {
  if (exit.result !== undefined) {
    return;
  }

  child.kill("SIGTERM");
  if (await waitForExit(exit.promise, childExitTimeoutMs)) {
    return;
  }

  child.kill("SIGKILL");
  await waitForExit(exit.promise, childExitTimeoutMs);
}

async function waitForExit(promise: Promise<ProcessExit>, timeoutMs: number): Promise<boolean> {
  const result = await Promise.race([promise, delay(timeoutMs).then(() => undefined)]);
  return result !== undefined;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function formatExecError(error: unknown): string {
  const execError = error as {
    code?: unknown;
    message?: string;
    signal?: unknown;
    stderr?: unknown;
    stdout?: unknown;
  };

  return [
    execError.message ?? String(error),
    execError.code !== undefined ? `exit code: ${String(execError.code)}` : "",
    execError.signal !== undefined ? `signal: ${String(execError.signal)}` : "",
    section("stdout", outputText(execError.stdout)),
    section("stderr", outputText(execError.stderr))
  ]
    .filter(Boolean)
    .join("\n");
}

function formatUnknownError(error: unknown): string {
  return error instanceof Error ? error.stack ?? error.message : String(error);
}

function outputText(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (Buffer.isBuffer(value)) {
    return value.toString("utf8");
  }
  return "";
}

function formatExit(exit: ProcessExit): string {
  const details = [`code=${String(exit.code)}`, `signal=${String(exit.signal)}`];
  if (exit.error !== undefined) {
    details.push(`error=${exit.error.message}`);
  }
  return details.join(" ");
}

type CapturedOutput = {
  text: () => string;
};

type OpenDbNodeProcess = ChildProcessByStdio<null, Readable, Readable>;

type ProcessExit = {
  code: number | null;
  error?: Error;
  signal: NodeJS.Signals | null;
};

type ExitTracker = {
  readonly promise: Promise<ProcessExit>;
  readonly result: ProcessExit | undefined;
};
