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

test("manifest checker rejects a peer Service without publishNotReadyAddresses", async () => {
  const fixtureRoot = mkdtempSync(join(tmpdir(), "opendb-manifests-"));
  tempDirs.push(fixtureRoot);
  const baseDir = join(fixtureRoot, "deploy", "k8s", "base");
  mkdirSync(baseDir, { recursive: true });
  writeFileSync(join(baseDir, "manifests.yaml"), manifestsWithoutPeerPublishNotReadyAddresses);

  await expect(execFileAsync(tsxBin, [checkManifestsPath], { cwd: fixtureRoot })).rejects.toMatchObject({
    stderr: expect.stringContaining("Service/opendb-peer.spec.publishNotReadyAddresses must be true")
  });
});

test("manifest checker rejects a StatefulSet without initial peer addresses", async () => {
  const fixtureRoot = mkdtempSync(join(tmpdir(), "opendb-manifests-"));
  tempDirs.push(fixtureRoot);
  const baseDir = join(fixtureRoot, "deploy", "k8s", "base");
  mkdirSync(baseDir, { recursive: true });
  writeFileSync(join(baseDir, "manifests.yaml"), manifestsWithoutInitialPeerAddresses);

  await expect(execFileAsync(tsxBin, [checkManifestsPath], { cwd: fixtureRoot })).rejects.toMatchObject({
    stderr: expect.stringContaining(
      'StatefulSet/opendb container opendb-node.env must include an item named "OPENDB_INITIAL_PEERS"'
    )
  });
});

test("manifest checker rejects a pgwire Service with a static leader pod selector", async () => {
  const fixtureRoot = mkdtempSync(join(tmpdir(), "opendb-manifests-"));
  tempDirs.push(fixtureRoot);
  const baseDir = join(fixtureRoot, "deploy", "k8s", "base");
  mkdirSync(baseDir, { recursive: true });
  writeFileSync(join(baseDir, "manifests.yaml"), manifestsWithStaticPgwireLeaderSelector);

  await expect(execFileAsync(tsxBin, [checkManifestsPath], { cwd: fixtureRoot })).rejects.toMatchObject({
    stderr: expect.stringContaining(
      "Service/opendb-pgwire.spec.selector must not include a static pod selector"
    )
  });
});

test("manifest checker rejects a StatefulSet without parallel pod management", async () => {
  const fixtureRoot = mkdtempSync(join(tmpdir(), "opendb-manifests-"));
  tempDirs.push(fixtureRoot);
  const baseDir = join(fixtureRoot, "deploy", "k8s", "base");
  mkdirSync(baseDir, { recursive: true });
  writeFileSync(join(baseDir, "manifests.yaml"), manifestsWithoutParallelPodManagement);

  await expect(execFileAsync(tsxBin, [checkManifestsPath], { cwd: fixtureRoot })).rejects.toMatchObject({
    stderr: expect.stringContaining("StatefulSet/opendb.spec.podManagementPolicy must be \"Parallel\"")
  });
});

test("manifest checker rejects extra pgwire selectors", async () => {
  const fixtureRoot = mkdtempSync(join(tmpdir(), "opendb-manifests-"));
  tempDirs.push(fixtureRoot);
  const baseDir = join(fixtureRoot, "deploy", "k8s", "base");
  mkdirSync(baseDir, { recursive: true });
  writeFileSync(join(baseDir, "manifests.yaml"), manifestsWithExtraPgwireSelector);

  await expect(execFileAsync(tsxBin, [checkManifestsPath], { cwd: fixtureRoot })).rejects.toMatchObject({
    stderr: expect.stringContaining(
      'Service/opendb-pgwire.spec.selector must select exactly ["app.kubernetes.io/instance","app.kubernetes.io/name"]'
    )
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

const operatorDeploymentManifest = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: opendb-operator
  namespace: opendb-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: opendb-operator
  template:
    metadata:
      labels:
        app.kubernetes.io/name: opendb-operator
    spec:
      serviceAccountName: opendb-operator
      containers:
        - name: opendb-operator
          image: opendb-operator:dev
          imagePullPolicy: IfNotPresent
          args:
            - run
          env:
            - name: OPENDB_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
`;

const manifestsWithoutPeerPublishNotReadyAddresses = `apiVersion: db.opendb.dev/v1alpha1
kind: OpenDbCluster
metadata:
  name: opendb
  namespace: opendb-system
spec:
  replicas: 3
  image: opendb-node:dev
  storageClassName: local-path
  storageSize: 1Gi
  pgwirePort: 5432
  healthPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: opendb-peer
  namespace: opendb-system
spec:
  clusterIP: None
  selector:
    app.kubernetes.io/name: opendb
    app.kubernetes.io/instance: opendb
  ports:
    - name: internal
      port: 7000
      targetPort: 7000
---
apiVersion: v1
kind: Service
metadata:
  name: opendb-pgwire
  namespace: opendb-system
spec:
  selector:
    app.kubernetes.io/name: opendb
    app.kubernetes.io/instance: opendb
  ports:
    - name: pgwire
      port: 5432
      targetPort: 5432
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: opendb
  namespace: opendb-system
spec:
  serviceName: opendb-peer
  podManagementPolicy: Parallel
  replicas: 3
  selector:
    matchLabels:
      app.kubernetes.io/name: opendb
      app.kubernetes.io/instance: opendb
  template:
    metadata:
      labels:
        app.kubernetes.io/name: opendb
        app.kubernetes.io/instance: opendb
    spec:
      terminationGracePeriodSeconds: 30
      containers:
        - name: opendb-node
          image: opendb-node:dev
          imagePullPolicy: IfNotPresent
          args:
            - "--node-id=$(OPENDB_ORDINAL)"
          env:
            - name: OPENDB_ORDINAL
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: OPENDB_DATA_DIR
              value: /var/lib/opendb
          ports:
            - name: pgwire
              containerPort: 5432
            - name: health
              containerPort: 8080
            - name: internal
              containerPort: 7000
          readinessProbe:
            httpGet:
              path: /ready
              port: health
            initialDelaySeconds: 2
            periodSeconds: 1
            failureThreshold: 1
          livenessProbe:
            httpGet:
              path: /live
              port: health
            initialDelaySeconds: 10
            periodSeconds: 10
          volumeMounts:
            - name: data
              mountPath: /var/lib/opendb
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes:
          - ReadWriteOnce
        storageClassName: local-path
        resources:
          requests:
            storage: 1Gi
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: opendb-operator
  namespace: opendb-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: opendb-operator
  namespace: opendb-system
rules:
  - apiGroups:
      - ""
    resources:
      - pods
    verbs:
      - get
      - list
      - watch
  - apiGroups:
      - db.opendb.dev
    resources:
      - opendbclusters
      - opendbclusters/status
    verbs:
      - get
      - list
      - watch
      - update
      - patch
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: opendb-operator
  namespace: opendb-system
subjects:
  - kind: ServiceAccount
    name: opendb-operator
    namespace: opendb-system
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: opendb-operator
---
${operatorDeploymentManifest}
`;

const manifestsWithoutInitialPeerAddresses = manifestsWithoutPeerPublishNotReadyAddresses.replace(
  "  clusterIP: None\n  selector:",
  "  clusterIP: None\n  publishNotReadyAddresses: true\n  selector:"
).replace(
  `          args:
            - "--node-id=$(OPENDB_ORDINAL)"
          env:
            - name: OPENDB_ORDINAL
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: OPENDB_DATA_DIR
              value: /var/lib/opendb`,
  `          args:
            - "--node-id=$(OPENDB_ORDINAL)"
            - "--node-id=$(OPENDB_POD_NAME)"
            - "--internal-addr=$(OPENDB_INTERNAL_ADDR)"
            - "--advertise-addr=$(OPENDB_ADVERTISE_ADDR)"
            - "--initial-peers=$(OPENDB_INITIAL_PEERS)"
            - "--bootstrap-node-id=$(OPENDB_BOOTSTRAP_NODE_ID)"
          env:
            - name: OPENDB_ORDINAL
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: OPENDB_POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: OPENDB_POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: OPENDB_INTERNAL_ADDR
              value: 0.0.0.0:7000
            - name: OPENDB_ADVERTISE_ADDR
              value: $(OPENDB_POD_NAME).opendb-peer.$(OPENDB_POD_NAMESPACE).svc.cluster.local:7000
            - name: OPENDB_BOOTSTRAP_NODE_ID
              value: "0"
            - name: OPENDB_DATA_DIR
              value: /var/lib/opendb`
);

const manifestsWithStaticPgwireLeaderSelector = manifestsWithoutPeerPublishNotReadyAddresses.replace(
  "  clusterIP: None\n  selector:",
  "  clusterIP: None\n  publishNotReadyAddresses: true\n  selector:"
).replace(
  `metadata:
  name: opendb-pgwire
  namespace: opendb-system
spec:
  selector:
    app.kubernetes.io/name: opendb
    app.kubernetes.io/instance: opendb`,
  `metadata:
  name: opendb-pgwire
  namespace: opendb-system
spec:
  selector:
    app.kubernetes.io/name: opendb
    app.kubernetes.io/instance: opendb
    statefulset.kubernetes.io/pod-name: opendb-0`
).replace(
  `          args:
            - "--node-id=$(OPENDB_ORDINAL)"
          env:
            - name: OPENDB_ORDINAL
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: OPENDB_DATA_DIR
              value: /var/lib/opendb`,
  `          args:
            - "--node-id=$(OPENDB_POD_NAME)"
            - "--internal-addr=$(OPENDB_INTERNAL_ADDR)"
            - "--advertise-addr=$(OPENDB_ADVERTISE_ADDR)"
            - "--initial-peers=$(OPENDB_INITIAL_PEERS)"
            - "--bootstrap-node-id=$(OPENDB_BOOTSTRAP_NODE_ID)"
          env:
            - name: OPENDB_POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: OPENDB_POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: OPENDB_INTERNAL_ADDR
              value: "0.0.0.0:7000"
            - name: OPENDB_ADVERTISE_ADDR
              value: "$(OPENDB_POD_NAME).opendb-peer.$(OPENDB_POD_NAMESPACE).svc.cluster.local:7000"
            - name: OPENDB_INITIAL_PEERS
              value: "0=opendb-0.opendb-peer.opendb-system.svc.cluster.local:7000,1=opendb-1.opendb-peer.opendb-system.svc.cluster.local:7000,2=opendb-2.opendb-peer.opendb-system.svc.cluster.local:7000"
            - name: OPENDB_BOOTSTRAP_NODE_ID
              value: "0"
            - name: OPENDB_DATA_DIR
              value: /var/lib/opendb`
);

const manifestsWithValidOpenraftCluster = manifestsWithStaticPgwireLeaderSelector.replace(
  "    statefulset.kubernetes.io/pod-name: opendb-0\n",
  ""
);

const manifestsWithoutParallelPodManagement = manifestsWithValidOpenraftCluster.replace(
  "  podManagementPolicy: Parallel\n",
  ""
);

const manifestsWithExtraPgwireSelector = manifestsWithValidOpenraftCluster.replace(
  `metadata:
  name: opendb-pgwire
  namespace: opendb-system
spec:
  selector:
    app.kubernetes.io/name: opendb
    app.kubernetes.io/instance: opendb`,
  `metadata:
  name: opendb-pgwire
  namespace: opendb-system
spec:
  selector:
    app.kubernetes.io/name: opendb
    app.kubernetes.io/instance: opendb
    opendb.dev/static-role: writer`
);
