import { execFile } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { afterEach, expect, test } from "vitest";

const execFileAsync = promisify(execFile);
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const tsxBin = join(repoRoot, "node_modules", ".bin", process.platform === "win32" ? "tsx.cmd" : "tsx");
const checkManifestsPath = join(repoRoot, "tools", "check-manifests.ts");
const tempDirs: string[] = [];

test("kubernetes manifests pass static checks", async () => {
  const { stdout } = await execFileAsync("npm", ["run", "check:manifests"], { cwd: repoRoot });

  expect(stdout).toContain("Kubernetes manifests passed static checks.");
});

test("manifest checker rejects a missing operator ServiceAccount", async () => {
  const fixtureRoot = mkdtempSync(join(tmpdir(), "opendb-manifests-"));
  tempDirs.push(fixtureRoot);
  const baseDir = join(fixtureRoot, "deploy", "k8s", "base");
  mkdirSync(baseDir, { recursive: true });
  writeFileSync(join(baseDir, "manifests.yaml"), shallowPassingManifests);

  await expect(execFileAsync(tsxBin, [checkManifestsPath], { cwd: fixtureRoot })).rejects.toMatchObject({
    stderr: expect.stringContaining("Missing required manifest resource: ServiceAccount/opendb-operator")
  });
});

afterEach(() => {
  for (const dir of tempDirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

const shallowPassingManifests = `apiVersion: db.opendb.dev/v1alpha1
kind: OpenDbCluster
metadata:
  name: opendb
  namespace: opendb-system
spec:
  replicas: 3
---
apiVersion: v1
kind: Service
metadata:
  name: opendb-peer
  namespace: opendb-system
spec:
  ports: []
---
apiVersion: v1
kind: Service
metadata:
  name: opendb-pgwire
  namespace: opendb-system
spec:
  ports: []
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: opendb
  namespace: opendb-system
spec:
  replicas: 3
  volumeClaimTemplates:
    - spec:
        storageClassName: local-path
`;
