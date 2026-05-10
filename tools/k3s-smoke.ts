import { execFile, spawn, type ChildProcessByStdio } from "node:child_process";
import net from "node:net";
import { dirname, join } from "node:path";
import type { Readable } from "node:stream";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const tsxBin = join(repoRoot, "node_modules", ".bin", process.platform === "win32" ? "tsx.cmd" : "tsx");
const pgwireSmokePath = join(repoRoot, "tools", "pgwire-smoke.ts");
const defaultNamespace = "opendb-system";
const defaultClusterName = "opendb";
const defaultExpectedReplicas = 3;
const defaultTimeoutMs = 120_000;
const pollIntervalMs = 1_000;
const maxCapturedOutputLength = 64 * 1024;

export type CommandSpec = {
  command: string;
  args: string[];
};

export type K3sSmokePlanStep = {
  command?: CommandSpec;
  description: string;
};

export type K3sSmokePlanOptions = {
  clusterName: string;
  expectedReplicas: number;
  kubectl: string;
  localPgwirePort: number;
  namespace: string;
};

export type PodSummary = {
  leaderReadyNames: string[];
  runningCount: number;
  runningNames: string[];
};

type SmokeOptions = {
  allowNonLocalContext: boolean;
  clusterName: string;
  expectedReplicas: number;
  kubectl: string;
  localPgwirePort: number;
  namespace: string;
  printPlan: boolean;
  timeoutMs: number;
};

type PortForwardProcess = ChildProcessByStdio<null, Readable, Readable>;

export type ExecOptions = {
  env?: NodeJS.ProcessEnv;
  input?: string;
  timeoutMs?: number;
};

export function buildK3sSmokePlan(options: K3sSmokePlanOptions): K3sSmokePlanStep[] {
  const selector = openDbPodSelector(options.clusterName);

  return [
    {
      description: "Verify kube context is explicitly local or allowed",
      command: { command: options.kubectl, args: ["config", "current-context"] }
    },
    {
      description: "Ensure namespace exists",
      command: { command: options.kubectl, args: ["apply", "-f", "-"] }
    },
    {
      description: "Generate OpenDbCluster CRD from Rust",
      command: { command: "cargo", args: ["run", "-p", "opendb-operator", "--", "print-crd"] }
    },
    {
      description: "Apply generated CRD",
      command: { command: options.kubectl, args: ["apply", "-f", "-"] }
    },
    {
      description: "Wait for OpenDbCluster CRD establishment",
      command: {
        command: options.kubectl,
        args: ["wait", "--for=condition=Established", "crd/opendbclusters.db.opendb.dev", "--timeout=120s"]
      }
    },
    {
      description: "Apply k3s base manifests",
      command: { command: options.kubectl, args: ["apply", "-f", "deploy/k8s/base/"] }
    },
    {
      description: "Wait for operator Deployment availability",
      command: {
        command: options.kubectl,
        args: [
          "wait",
          "--for=condition=Available",
          "deployment/opendb-operator",
          "-n",
          options.namespace,
          "--timeout=120s"
        ]
      }
    },
    {
      description: `Poll until ${options.expectedReplicas} OpenDB node processes are running`,
      command: { command: options.kubectl, args: ["get", "pods", "-n", options.namespace, "-l", selector, "-o", "json"] }
    },
    {
      description: "Poll until OpenDbCluster status is Ready with a leader pod",
      command: {
        command: options.kubectl,
        args: ["get", "opendbcluster", options.clusterName, "-n", options.namespace, "-o", "json"]
      }
    },
    {
      description: "Port-forward pgwire Service",
      command: {
        command: options.kubectl,
        args: ["port-forward", "service/opendb-pgwire", `${options.localPgwirePort}:5432`, "-n", options.namespace]
      }
    },
    {
      description: "Run pgwire SQL smoke through the Kubernetes Service",
      command: { command: "tsx", args: ["tools/pgwire-smoke.ts"] }
    },
    {
      description: "create table and insert recovery smoke row through pgwire"
    },
    {
      description: "delete the current leader pod"
    },
    {
      description: "wait for OpenDbCluster/status Ready with a leader pod"
    },
    {
      description: "query the recovery smoke row through pgwire"
    },
    {
      description: "no object storage service is required"
    }
  ];
}

export function commandText(command: CommandSpec): string {
  return [command.command, ...command.args].join(" ");
}

export function summarizeOpenDbPods(value: unknown): PodSummary {
  const root = requireRecord(value, "pod list");
  const items = requireArray(root.items, "pod list.items");
  const runningNames: string[] = [];
  const leaderReadyNames: string[] = [];

  for (const item of items) {
    const pod = requireRecord(item, "pod list.items[]");
    const metadata = requireRecord(pod.metadata, "pod.metadata");
    if (metadata.deletionTimestamp !== undefined) {
      continue;
    }

    const name = requireString(metadata.name, "pod.metadata.name");
    const status = requireRecord(pod.status, "pod.status");
    const containerStatuses = optionalArray(status.containerStatuses);
    const conditions = optionalArray(status.conditions);
    const nodeRunning =
      status.phase === "Running" &&
      containerStatuses.some((containerValue) => {
        const container = requireRecord(containerValue, "pod.status.containerStatuses[]");
        const state = requireRecord(container.state, "pod.status.containerStatuses[].state");
        return container.name === "opendb-node" && isRecord(state.running);
      });
    const leaderReady = conditions.some((conditionValue) => {
      const condition = requireRecord(conditionValue, "pod.status.conditions[]");
      return condition.type === "Ready" && condition.status === "True";
    });

    if (nodeRunning) {
      runningNames.push(name);
    }
    if (leaderReady) {
      leaderReadyNames.push(name);
    }
  }

  return {
    leaderReadyNames,
    runningCount: runningNames.length,
    runningNames
  };
}

export function clusterStatusIsReady(value: unknown, expectedReplicas: number): boolean {
  if (!isRecord(value) || !isRecord(value.status)) {
    return false;
  }
  const status = value.status;
  return (
    status.phase === "Ready" &&
    typeof status.readyReplicas === "number" &&
    status.readyReplicas === expectedReplicas &&
    typeof status.leaderPod === "string" &&
    status.leaderPod.trim().length > 0
  );
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));
  const plan = buildK3sSmokePlan(options);

  if (options.printPlan) {
    printPlan(plan);
    return;
  }

  await runK3sSmoke(options);
}

async function runK3sSmoke(options: SmokeOptions): Promise<void> {
  await ensureSafeKubeContext(options);
  await ensureNamespace(options);
  const crdYaml = await captureCommand("cargo", ["run", "-p", "opendb-operator", "--", "print-crd"], {
    timeoutMs: options.timeoutMs
  });
  await run(options.kubectl, ["apply", "-f", "-"], { input: crdYaml.stdout, timeoutMs: options.timeoutMs });
  await run(
    options.kubectl,
    ["wait", "--for=condition=Established", "crd/opendbclusters.db.opendb.dev", "--timeout=120s"],
    { timeoutMs: options.timeoutMs }
  );
  await run(options.kubectl, ["apply", "-f", "deploy/k8s/base/"], { timeoutMs: options.timeoutMs });
  await run(
    options.kubectl,
    [
      "wait",
      "--for=condition=Available",
      "deployment/opendb-operator",
      "-n",
      options.namespace,
      "--timeout=120s"
    ],
    { timeoutMs: options.timeoutMs }
  );
  await waitForOpenDbPods(options);
  await waitForClusterStatus(options);

  const portForward = startPortForward(options);
  try {
    await waitForPortForwardReady(portForward, options.localPgwirePort, options.timeoutMs);
    await waitForTcp(options.localPgwirePort, options.timeoutMs);
    await run(tsxBin, [pgwireSmokePath], {
      env: {
        ...process.env,
        OPENDB_PGWIRE_HOST: "127.0.0.1",
        OPENDB_PGWIRE_PORT: String(options.localPgwirePort)
      },
      timeoutMs: options.timeoutMs
    });
  } finally {
    await stopPortForward(portForward);
  }

  console.log("k3s smoke passed");
}

async function ensureNamespace(options: SmokeOptions): Promise<void> {
  const manifest = [
    "apiVersion: v1",
    "kind: Namespace",
    "metadata:",
    `  name: ${options.namespace}`,
    ""
  ].join("\n");

  await run(options.kubectl, ["apply", "-f", "-"], { input: manifest, timeoutMs: options.timeoutMs });
}

async function waitForOpenDbPods(options: SmokeOptions): Promise<void> {
  await poll(`OpenDB node processes running in namespace ${options.namespace}`, options.timeoutMs, async () => {
    const output = await captureCommand(
      options.kubectl,
      [
        "get",
        "pods",
        "-n",
        options.namespace,
        "-l",
        openDbPodSelector(options.clusterName),
        "-o",
        "json"
      ],
      { timeoutMs: options.timeoutMs }
    );
    const summary = summarizeOpenDbPods(JSON.parse(output.stdout));
    if (summary.runningCount < options.expectedReplicas) {
      throw new Error(
        `expected ${options.expectedReplicas} running OpenDB pods, got ${summary.runningCount}: ${summary.runningNames.join(",")}`
      );
    }
    if (summary.leaderReadyNames.length !== 1) {
      throw new Error(`expected exactly one leader-ready pod, got ${summary.leaderReadyNames.join(",")}`);
    }
  });
}

async function waitForClusterStatus(options: SmokeOptions): Promise<void> {
  await poll(`OpenDbCluster/${options.clusterName} Ready status`, options.timeoutMs, async () => {
    const output = await captureCommand(
      options.kubectl,
      ["get", "opendbcluster", options.clusterName, "-n", options.namespace, "-o", "json"],
      { timeoutMs: options.timeoutMs }
    );
    const cluster = JSON.parse(output.stdout);
    if (!clusterStatusIsReady(cluster, options.expectedReplicas)) {
      throw new Error(`OpenDbCluster is not Ready: ${JSON.stringify(cluster.status ?? null)}`);
    }
  });
}

function startPortForward(options: SmokeOptions): PortForwardProcess {
  return spawn(
    options.kubectl,
    ["port-forward", "service/opendb-pgwire", `${options.localPgwirePort}:5432`, "-n", options.namespace],
    { cwd: repoRoot, env: process.env, stdio: ["ignore", "pipe", "pipe"] }
  );
}

async function stopPortForward(child: PortForwardProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  child.kill("SIGTERM");
  await new Promise<void>((resolve) => {
    const timeout = setTimeout(() => {
      child.kill("SIGKILL");
      resolve();
    }, 2_000);
    child.once("exit", () => {
      clearTimeout(timeout);
      resolve();
    });
  });
}

async function waitForPortForwardReady(child: PortForwardProcess, port: number, timeoutMs: number): Promise<void> {
  let output = "";

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`timed out waiting for kubectl port-forward to expose local port ${port}\n${output}`));
    }, timeoutMs);
    const cleanup = () => {
      clearTimeout(timeout);
      child.stdout.off("data", onData);
      child.stderr.off("data", onData);
      child.off("exit", onExit);
      child.off("error", onError);
    };
    const onData = (chunk: Buffer | string) => {
      output = appendCapturedOutput(output, chunk.toString());
      if (portForwardOutputShowsReady(output, port)) {
        cleanup();
        resolve();
      }
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      reject(
        new Error(
          `kubectl port-forward exited before exposing local port ${port}: code=${String(code)} signal=${String(signal)}\n${output}`
        )
      );
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };

    child.stdout.on("data", onData);
    child.stderr.on("data", onData);
    child.once("exit", onExit);
    child.once("error", onError);
  });
}

export function portForwardOutputShowsReady(output: string, port: number): boolean {
  const escapedPort = String(port).replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  const forwardingPattern = new RegExp(`Forwarding from (?:127\\.0\\.0\\.1|localhost|\\[::1\\]):${escapedPort} ->`, "u");
  return forwardingPattern.test(output);
}

async function waitForTcp(port: number, timeoutMs: number): Promise<void> {
  await poll(`local pgwire port ${port}`, timeoutMs, async () => {
    await new Promise<void>((resolve, reject) => {
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
  });
}

async function poll(name: string, timeoutMs: number, probe: () => Promise<void>): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      await probe();
      return;
    } catch (error) {
      lastError = error;
      await delay(pollIntervalMs);
    }
  }
  throw new Error(`timed out waiting for ${name}: ${formatUnknownError(lastError)}`);
}

export async function captureCommand(
  command: string,
  args: string[],
  options: ExecOptions = {}
): Promise<{ stdout: string; stderr: string }> {
  if (options.input !== undefined) {
    return await captureWithStdin(command, args, options);
  }

  try {
    return await execFileAsync(command, args, {
      cwd: repoRoot,
      encoding: "utf8",
      env: options.env ?? process.env,
      maxBuffer: 20 * 1024 * 1024,
      timeout: options.timeoutMs ?? defaultTimeoutMs
    });
  } catch (error) {
    throw new Error(`${commandText({ command, args })} failed\n${formatExecError(error)}`);
  }
}

async function run(command: string, args: string[], options: ExecOptions = {}): Promise<void> {
  await captureCommand(command, args, options);
}

function captureWithStdin(
  command: string,
  args: string[],
  options: ExecOptions
): Promise<{ stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: repoRoot,
      env: options.env ?? process.env,
      stdio: ["pipe", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    let finished = false;
    const timeout = setTimeout(() => {
      if (finished) {
        return;
      }
      finished = true;
      child.kill("SIGKILL");
      reject(new Error(`${commandText({ command, args })} timed out after ${options.timeoutMs ?? defaultTimeoutMs}ms`));
    }, options.timeoutMs ?? defaultTimeoutMs);
    const finish = (result: () => void) => {
      if (finished) {
        return;
      }
      finished = true;
      clearTimeout(timeout);
      result();
    };

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout = appendCapturedOutput(stdout, chunk);
    });
    child.stderr.on("data", (chunk: string) => {
      stderr = appendCapturedOutput(stderr, chunk);
    });
    child.once("error", (error) => {
      finish(() => reject(error));
    });
    child.once("close", (code, signal) => {
      if (code === 0) {
        finish(() => resolve({ stdout, stderr }));
        return;
      }
      finish(() =>
        reject(
          new Error(
            `${commandText({ command, args })} failed\n${[
              `exit code: ${String(code)}`,
              `signal: ${String(signal)}`,
              section("stdout", stdout),
              section("stderr", stderr)
            ]
              .filter(Boolean)
              .join("\n")}`
          )
        )
      );
    });
    child.stdin.write(options.input);
    child.stdin.end();
  });
}

export function parseSmokeOptions(args: string[]): SmokeOptions {
  const options: SmokeOptions = {
    allowNonLocalContext: process.env.OPENDB_K3S_ALLOW_NONLOCAL_CONTEXT === "1",
    clusterName: defaultClusterName,
    expectedReplicas: defaultExpectedReplicas,
    kubectl: envString("KUBECTL", "kubectl"),
    localPgwirePort: envNumber("OPENDB_LOCAL_PGWIRE_PORT", 15432),
    namespace: defaultNamespace,
    printPlan: false,
    timeoutMs: envNumber("OPENDB_K3S_SMOKE_TIMEOUT_MS", defaultTimeoutMs)
  };
  rejectStaticManifestEnvOverride("OPENDB_CLUSTER_NAME", defaultClusterName);
  rejectStaticManifestEnvOverride("OPENDB_NAMESPACE", defaultNamespace);
  rejectStaticManifestEnvOverride("OPENDB_EXPECTED_REPLICAS", String(defaultExpectedReplicas));

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    switch (arg) {
      case "--allow-nonlocal-context":
        options.allowNonLocalContext = true;
        break;
      case "--cluster-name":
        throw new Error("cluster-name override is not supported while deploy/k8s/base manifests are static");
      case "--expected-replicas":
        throw new Error("expected-replicas override is not supported while deploy/k8s/base manifests are static");
      case "--kubectl":
        options.kubectl = requireArgValue(args, ++index, arg);
        break;
      case "--local-pgwire-port":
        options.localPgwirePort = parsePositiveInt(requireArgValue(args, ++index, arg), arg);
        break;
      case "--namespace":
        throw new Error("namespace override is not supported while deploy/k8s/base manifests are static");
      case "--print-plan":
        options.printPlan = true;
        break;
      case "--timeout-ms":
        options.timeoutMs = parsePositiveInt(requireArgValue(args, ++index, arg), arg);
        break;
      default:
        throw new Error(`unknown argument: ${arg}`);
    }
  }

  return options;
}

function rejectStaticManifestEnvOverride(name: string, expectedValue: string): void {
  const value = process.env[name];
  if (value !== undefined && value !== expectedValue) {
    throw new Error(`${name} override is not supported while deploy/k8s/base manifests are static`);
  }
}

function parseArgs(args: string[]): SmokeOptions {
  return parseSmokeOptions(args);
}

async function ensureSafeKubeContext(options: SmokeOptions): Promise<void> {
  const output = await captureCommand(options.kubectl, ["config", "current-context"], {
    timeoutMs: options.timeoutMs
  });
  const context = output.stdout.trim();
  if (!kubeContextIsAllowed(context, options.allowNonLocalContext)) {
    throw new Error(
      [
        `refusing to run k3s smoke against kube context ${JSON.stringify(context)}.`,
        "Use a local k3s/k3d/kind/minikube/docker-desktop/rancher-desktop context,",
        "or set OPENDB_K3S_ALLOW_NONLOCAL_CONTEXT=1 / pass --allow-nonlocal-context explicitly."
      ].join(" ")
    );
  }
}

export function kubeContextIsAllowed(context: string, allowNonLocalContext: boolean): boolean {
  if (allowNonLocalContext) {
    return true;
  }
  const normalized = context.toLowerCase();
  if (["k3s", "minikube", "docker-desktop", "rancher-desktop"].includes(normalized)) {
    return true;
  }
  return normalized === "k3d" || normalized === "kind" || normalized.startsWith("k3d-") || normalized.startsWith("kind-");
}

function printPlan(plan: K3sSmokePlanStep[]): void {
  for (const [index, step] of plan.entries()) {
    const command = step.command === undefined ? "" : `: ${commandText(step.command)}`;
    console.log(`${index + 1}. ${step.description}${command}`);
  }
}

function openDbPodSelector(clusterName: string): string {
  return `app.kubernetes.io/name=opendb,app.kubernetes.io/instance=${clusterName}`;
}

function envString(name: string, fallback: string): string {
  return process.env[name] ?? fallback;
}

function envNumber(name: string, fallback: number): number {
  const value = process.env[name];
  return value === undefined ? fallback : parsePositiveInt(value, name);
}

function parsePositiveInt(value: string, context: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${context} must be a positive integer, got ${JSON.stringify(value)}`);
  }
  return parsed;
}

function requireArgValue(args: string[], index: number, option: string): string {
  const value = args[index];
  if (value === undefined) {
    throw new Error(`${option} requires a value`);
  }
  return value;
}

function requireRecord(value: unknown, context: string): Record<string, unknown> {
  if (!isRecord(value)) {
    throw new Error(`${context} must be an object`);
  }
  return value;
}

function requireArray(value: unknown, context: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new Error(`${context} must be an array`);
  }
  return value;
}

function optionalArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function requireString(value: unknown, context: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${context} must be a non-empty string`);
  }
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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

function appendCapturedOutput(current: string, chunk: string): string {
  return (current + chunk).slice(-maxCapturedOutputLength);
}

function section(name: string, value: string): string {
  return value.length > 0 ? `${name}:\n${value}` : "";
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(formatUnknownError(error));
    process.exit(1);
  });
}
