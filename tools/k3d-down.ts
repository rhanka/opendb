import { execFile } from "node:child_process";
import { access } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";
import os from "node:os";

const execFileAsync = promisify(execFile);
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const defaultClusterName = "opendb-dev";

export type K3dDownOptions = {
  clusterName: string;
  k3dBin: string | undefined;
};

export function parseK3dDownOptions(args: string[]): K3dDownOptions {
  const options: K3dDownOptions = {
    clusterName: process.env.OPENDB_K3D_CLUSTER ?? defaultClusterName,
    k3dBin: process.env.OPENDB_K3D_BIN
  };

  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    switch (arg) {
      case "--cluster-name":
        options.clusterName = requireArgValue(args, ++index, arg);
        break;
      case "--k3d-bin":
        options.k3dBin = requireArgValue(args, ++index, arg);
        break;
      default:
        throw new Error(`unknown argument: ${arg}`);
    }
  }

  return options;
}

function requireArgValue(args: string[], index: number, option: string): string {
  const value = args[index];
  if (value === undefined) {
    throw new Error(`${option} requires a value`);
  }
  return value;
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

async function resolveK3d(options: K3dDownOptions): Promise<string> {
  if (options.k3dBin !== undefined) {
    if (!(await fileExists(options.k3dBin))) {
      throw new Error(`OPENDB_K3D_BIN points to ${options.k3dBin}, but the file does not exist`);
    }
    return options.k3dBin;
  }
  const fromPath = await which("k3d");
  if (fromPath !== undefined) {
    return fromPath;
  }
  const local = join(os.homedir(), ".local", "bin", "k3d");
  if (await fileExists(local)) {
    return local;
  }
  throw new Error(
    "k3d binary not found. Install it first (e.g. `npm run k3s:up` will install it), or set OPENDB_K3D_BIN."
  );
}

async function main(): Promise<void> {
  const options = parseK3dDownOptions(process.argv.slice(2));
  const k3d = await resolveK3d(options);
  try {
    await execFileAsync(k3d, ["cluster", "delete", options.clusterName], {
      cwd: repoRoot,
      timeout: 60_000
    });
    console.log(`✓ k3d cluster '${options.clusterName}' deleted`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.includes("No nodes found") || message.includes("no clusters found")) {
      console.log(`k3d cluster '${options.clusterName}' was not running; nothing to delete`);
      return;
    }
    throw error;
  }
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.stack ?? error.message : String(error));
    process.exit(1);
  });
}
