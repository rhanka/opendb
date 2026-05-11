import { execFile, spawn } from "node:child_process";
import { access, chmod, mkdir } from "node:fs/promises";
import net from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import os from "node:os";

const execFileAsync = promisify(execFile);
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const defaultClusterName = "opendb-dev";
const defaultK3dVersion = "v5.7.4";
const defaultK3dBinDir = join(os.homedir(), ".local", "bin");
const nodeImageTag = "opendb-node:dev";
const operatorImageTag = "opendb-operator:dev";
const releaseDir = join(repoRoot, "target", "release");

export type K3dUpOptions = {
  clusterName: string;
  k3dVersion: string;
  k3dBin: string | undefined;
  k3dBinDir: string;
  skipBuild: boolean;
  apiPort: number | undefined;
};

export function parseK3dUpOptions(args: string[]): K3dUpOptions {
  const options: K3dUpOptions = {
    clusterName: process.env.OPENDB_K3D_CLUSTER ?? defaultClusterName,
    k3dVersion: process.env.OPENDB_K3D_VERSION ?? defaultK3dVersion,
    k3dBin: process.env.OPENDB_K3D_BIN,
    k3dBinDir: process.env.OPENDB_K3D_BIN_DIR ?? defaultK3dBinDir,
    skipBuild: process.env.OPENDB_K3D_SKIP_BUILD === "1",
    apiPort: parseOptionalPort(process.env.OPENDB_K3D_API_PORT)
  };

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    switch (arg) {
      case "--cluster-name":
        options.clusterName = requireArgValue(args, ++index, arg);
        break;
      case "--k3d-version":
        options.k3dVersion = requireArgValue(args, ++index, arg);
        break;
      case "--k3d-bin":
        options.k3dBin = requireArgValue(args, ++index, arg);
        break;
      case "--k3d-bin-dir":
        options.k3dBinDir = requireArgValue(args, ++index, arg);
        break;
      case "--skip-build":
        options.skipBuild = true;
        break;
      case "--api-port":
        options.apiPort = parsePositiveInt(requireArgValue(args, ++index, arg), arg);
        break;
      default:
        throw new Error(`unknown argument: ${arg}`);
    }
  }

  return options;
}

function parseOptionalPort(value: string | undefined): number | undefined {
  return value === undefined ? undefined : parsePositiveInt(value, "OPENDB_K3D_API_PORT");
}

function requireArgValue(args: string[], index: number, option: string): string {
  const value = args[index];
  if (value === undefined) {
    throw new Error(`${option} requires a value`);
  }
  return value;
}

function parsePositiveInt(value: string, context: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${context} must be a positive integer, got ${JSON.stringify(value)}`);
  }
  return parsed;
}

async function which(binary: string): Promise<string | undefined> {
  try {
    const result = await execFileAsync(process.platform === "win32" ? "where" : "which", [binary]);
    const first = result.stdout.split(/\r?\n/u).find((line) => line.length > 0);
    return first?.trim();
  } catch {
    return undefined;
  }
}

async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

async function ensureDocker(): Promise<void> {
  try {
    await execFileAsync("docker", ["info", "--format", "{{.ServerVersion}}"], { timeout: 10_000 });
  } catch (error) {
    throw new Error(
      `docker daemon is not accessible. Start Docker first.\n${formatExecError(error)}`
    );
  }
}

export function k3dBinaryAssetName(platform: NodeJS.Platform, arch: string): string {
  const platformLabel = platform === "darwin" ? "darwin" : platform === "win32" ? "windows" : "linux";
  const archLabel = arch === "x64" ? "amd64" : arch === "arm64" ? "arm64" : arch;
  const suffix = platform === "win32" ? ".exe" : "";
  return `k3d-${platformLabel}-${archLabel}${suffix}`;
}

async function ensureK3d(options: K3dUpOptions): Promise<string> {
  if (options.k3dBin !== undefined) {
    if (!(await fileExists(options.k3dBin))) {
      throw new Error(`OPENDB_K3D_BIN points to ${options.k3dBin}, but the file does not exist`);
    }
    return options.k3dBin;
  }

  const existing = await which("k3d");
  if (existing !== undefined) {
    return existing;
  }

  const target = join(options.k3dBinDir, "k3d");
  if (await fileExists(target)) {
    return target;
  }

  console.log(`k3d binary not found; installing ${options.k3dVersion} into ${target}`);
  await mkdir(options.k3dBinDir, { recursive: true });
  const asset = k3dBinaryAssetName(process.platform, process.arch);
  const url = `https://github.com/k3d-io/k3d/releases/download/${options.k3dVersion}/${asset}`;
  console.log(`downloading ${url}`);
  await downloadBinary(url, target);
  await chmod(target, 0o755);
  console.log(`k3d installed at ${target}`);
  return target;
}

async function downloadBinary(url: string, target: string): Promise<void> {
  const response = await fetch(url, { redirect: "follow" });
  if (!response.ok || response.body === null) {
    throw new Error(`failed to download ${url}: ${response.status} ${response.statusText}`);
  }
  await pipeline(response.body as unknown as NodeJS.ReadableStream, createWriteStream(target));
}

async function buildBinaries(options: K3dUpOptions): Promise<void> {
  if (options.skipBuild) {
    console.log("skipping cargo build (OPENDB_K3D_SKIP_BUILD=1 / --skip-build)");
    return;
  }
  console.log("building release binaries (cargo build --release -p opendb-node -p opendb-operator)…");
  await runStreaming(
    "cargo",
    ["build", "--release", "-p", "opendb-node", "-p", "opendb-operator"],
    {}
  );
  const required = [
    join(releaseDir, "opendb-node"),
    join(releaseDir, "opendb-operator")
  ];
  for (const path of required) {
    if (!(await fileExists(path))) {
      throw new Error(`expected built binary at ${path} but it is missing`);
    }
  }
}

async function buildImages(): Promise<void> {
  console.log(`docker build ${nodeImageTag} from deploy/docker/Dockerfile.node`);
  await runStreaming(
    "docker",
    ["build", "-t", nodeImageTag, "-f", join("deploy", "docker", "Dockerfile.node"), "."],
    {}
  );
  console.log(`docker build ${operatorImageTag} from deploy/docker/Dockerfile.operator`);
  await runStreaming(
    "docker",
    ["build", "-t", operatorImageTag, "-f", join("deploy", "docker", "Dockerfile.operator"), "."],
    {}
  );
}

async function clusterExists(k3d: string, clusterName: string): Promise<boolean> {
  try {
    await execFileAsync(k3d, ["cluster", "get", clusterName], { timeout: 10_000 });
    return true;
  } catch {
    return false;
  }
}

async function ensureCluster(k3d: string, options: K3dUpOptions): Promise<void> {
  if (await clusterExists(k3d, options.clusterName)) {
    console.log(`k3d cluster '${options.clusterName}' already exists; reusing`);
    return;
  }
  console.log(`creating k3d cluster '${options.clusterName}'`);
  const args = ["cluster", "create", options.clusterName, "--wait"];
  if (options.apiPort !== undefined) {
    args.push("--api-port", `0.0.0.0:${options.apiPort}`);
  }
  await runStreaming(k3d, args, {});
}

async function importImages(k3d: string, options: K3dUpOptions): Promise<void> {
  console.log(`importing images into k3d cluster '${options.clusterName}'`);
  await runStreaming(
    k3d,
    ["image", "import", nodeImageTag, operatorImageTag, "-c", options.clusterName],
    {}
  );
}

async function runStreaming(
  command: string,
  args: string[],
  env: NodeJS.ProcessEnv
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: repoRoot,
      env: { ...process.env, ...env },
      stdio: ["ignore", "inherit", "inherit"]
    });
    child.once("error", reject);
    child.once("close", (code, signal) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${command} ${args.join(" ")} exited code=${String(code)} signal=${String(signal)}`));
    });
  });
}

function formatExecError(error: unknown): string {
  const execError = error as {
    code?: unknown;
    message?: string;
    stderr?: unknown;
    stdout?: unknown;
  };
  return [
    execError.message ?? String(error),
    execError.code !== undefined ? `exit code: ${String(execError.code)}` : "",
    typeof execError.stdout === "string" && execError.stdout.length > 0 ? `stdout:\n${execError.stdout}` : "",
    typeof execError.stderr === "string" && execError.stderr.length > 0 ? `stderr:\n${execError.stderr}` : ""
  ]
    .filter(Boolean)
    .join("\n");
}

async function main(): Promise<void> {
  const options = parseK3dUpOptions(process.argv.slice(2));
  await ensureDocker();
  const k3d = await ensureK3d(options);
  await buildBinaries(options);
  await buildImages();
  await ensureCluster(k3d, options);
  await importImages(k3d, options);
  console.log(`✓ k3d cluster '${options.clusterName}' ready with images ${nodeImageTag} and ${operatorImageTag}`);
  console.log("  kube context: k3d-" + options.clusterName);
  console.log("  next: npm run smoke:k3s   (or)   npm run smoke:k3s -- --with-restart-recovery");
}

// Avoid unused warnings when imported by tests.
export const _internals = {
  defaultClusterName,
  defaultK3dVersion,
  nodeImageTag,
  operatorImageTag,
  k3dBinaryAssetName
};

// Avoid unused import warnings when net is left over.
export const _net = net;

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.stack ?? error.message : String(error));
    process.exit(1);
  });
}
