import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import { parseAllDocuments } from "yaml";

type Manifest = Record<string, unknown>;

const expectedNamespace = "opendb-system";
const baseDir = join(process.cwd(), "deploy/k8s/base");
const expectedAdvertiseAddr = "$(OPENDB_POD_NAME).opendb-peer.$(OPENDB_POD_NAMESPACE).svc.cluster.local:7000";
const expectedInitialPeers = [
  "0=opendb-0.opendb-peer.opendb-system.svc.cluster.local:7000",
  "1=opendb-1.opendb-peer.opendb-system.svc.cluster.local:7000",
  "2=opendb-2.opendb-peer.opendb-system.svc.cluster.local:7000"
].join(",");

function fail(message: string): never {
  console.error(message);
  process.exit(1);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function formatValue(value: unknown): string {
  return JSON.stringify(value);
}

function requireRecord(value: unknown, context: string): Record<string, unknown> {
  if (!isRecord(value)) {
    fail(`${context} must be a mapping`);
  }
  return value;
}

function requireArray(value: unknown, context: string): unknown[] {
  if (!Array.isArray(value)) {
    fail(`${context} must be a list`);
  }
  return value;
}

function requireRecordField(
  record: Record<string, unknown>,
  field: string,
  context: string
): Record<string, unknown> {
  return requireRecord(record[field], `${context}.${field}`);
}

function requireArrayField(
  record: Record<string, unknown>,
  field: string,
  context: string
): unknown[] {
  return requireArray(record[field], `${context}.${field}`);
}

function requireStringArray(value: unknown, context: string): string[] {
  const values = requireArray(value, context);
  const strings: string[] = [];

  for (const value of values) {
    if (typeof value !== "string") {
      fail(`${context} must contain only strings`);
    }
    strings.push(value);
  }

  return strings;
}

function requireStringArrayField(
  record: Record<string, unknown>,
  field: string,
  context: string
): string[] {
  return requireStringArray(record[field], `${context}.${field}`);
}

function expectEqual(actual: unknown, expected: unknown, context: string): void {
  if (actual !== expected) {
    fail(`${context} must be ${formatValue(expected)}`);
  }
}

function expectIncludes(values: string[], expected: string, context: string): void {
  if (!values.includes(expected)) {
    fail(`${context} must include ${formatValue(expected)}`);
  }
}

function expectIncludesAll(values: string[], expectedValues: string[], context: string): void {
  for (const expected of expectedValues) {
    expectIncludes(values, expected, context);
  }
}

function requireNamedRecord(values: unknown[], name: string, context: string): Record<string, unknown> {
  const value = values.find((item) => isRecord(item) && item.name === name);
  if (value === undefined) {
    fail(`${context} must include an item named ${formatValue(name)}`);
  }
  return requireRecord(value, `${context}[name=${name}]`);
}

function metadataName(manifest: Manifest): string | undefined {
  if (!isRecord(manifest.metadata)) return undefined;
  return typeof manifest.metadata.name === "string" ? manifest.metadata.name : undefined;
}

function metadataNamespace(manifest: Manifest): string | undefined {
  if (!isRecord(manifest.metadata)) return undefined;
  return typeof manifest.metadata.namespace === "string" ? manifest.metadata.namespace : undefined;
}

function manifestKey(manifest: Manifest): string | undefined {
  if (typeof manifest.kind !== "string") return undefined;
  const name = metadataName(manifest);
  return name === undefined ? undefined : `${manifest.kind}/${name}`;
}

function readManifests(): Manifest[] {
  if (!existsSync(baseDir)) {
    fail(`Kubernetes manifest directory not found: ${relative(process.cwd(), baseDir)}`);
  }

  const files = readdirSync(baseDir, { withFileTypes: true })
    .filter((entry) => entry.isFile() && /\.ya?ml$/u.test(entry.name))
    .map((entry) => join(baseDir, entry.name))
    .sort();

  if (files.length === 0) {
    fail(`No YAML manifest files found in ${relative(process.cwd(), baseDir)}`);
  }

  const manifests: Manifest[] = [];

  for (const file of files) {
    const source = readFileSync(file, "utf8");
    const documents = parseAllDocuments(source);

    for (const document of documents) {
      if (document.errors.length > 0) {
        const messages = document.errors.map((error) => error.message).join("; ");
        fail(`Invalid YAML in ${relative(process.cwd(), file)}: ${messages}`);
      }

      const manifest = document.toJSON();
      if (manifest === null) continue;
      if (!isRecord(manifest)) {
        fail(`YAML document in ${relative(process.cwd(), file)} must be a mapping`);
      }
      manifests.push(manifest);
    }
  }

  return manifests;
}

const manifestsByKey = new Map<string, Manifest>();
for (const manifest of readManifests()) {
  const key = manifestKey(manifest);
  if (key === undefined) continue;
  if (manifestsByKey.has(key)) {
    fail(`Duplicate manifest resource: ${key}`);
  }
  manifestsByKey.set(key, manifest);
}

function requireManifest(kind: string, name: string, namespace = expectedNamespace): Manifest {
  const key = `${kind}/${name}`;
  const manifest = manifestsByKey.get(key);
  if (manifest === undefined) {
    fail(`Missing required manifest resource: ${key}`);
  }
  expectEqual(metadataNamespace(manifest), namespace, `${key} metadata.namespace`);
  return manifest;
}

function requireSpec(manifest: Manifest, key: string): Record<string, unknown> {
  return requireRecord(manifest.spec, `${key}.spec`);
}

function assertAppSelector(selector: Record<string, unknown>, context: string): void {
  expectEqual(selector["app.kubernetes.io/name"], "opendb", `${context}.app.kubernetes.io/name`);
}

function assertNoStaticPodSelector(selector: Record<string, unknown>, context: string): void {
  if (Object.prototype.hasOwnProperty.call(selector, "statefulset.kubernetes.io/pod-name")) {
    fail(
      `${context} must not include a static pod selector; remove statefulset.kubernetes.io/pod-name so readiness selects the leader`
    );
  }
}

function assertOnlyAppSelector(selector: Record<string, unknown>, context: string): void {
  assertAppSelector(selector, context);
  assertNoStaticPodSelector(selector, context);
  const keys = Object.keys(selector).sort();
  const expectedKeys = ["app.kubernetes.io/name"];
  if (keys.length !== expectedKeys.length || keys.some((key, index) => key !== expectedKeys[index])) {
    fail(`${context} must select exactly ${formatValue(expectedKeys)} so readiness can expose the elected leader`);
  }
}

function assertServicePort(
  ports: unknown[],
  name: string,
  port: number,
  targetPort: number,
  context: string
): void {
  const servicePort = requireNamedRecord(ports, name, context);
  expectEqual(servicePort.port, port, `${context}[name=${name}].port`);
  expectEqual(servicePort.targetPort, targetPort, `${context}[name=${name}].targetPort`);
}

function assertContainerPort(
  ports: unknown[],
  name: string,
  containerPort: number,
  context: string
): void {
  const port = requireNamedRecord(ports, name, context);
  expectEqual(port.containerPort, containerPort, `${context}[name=${name}].containerPort`);
}

function assertFieldRefEnv(env: unknown[], name: string, fieldPath: string, context: string): void {
  const envVar = requireNamedRecord(env, name, context);
  const valueFrom = requireRecordField(envVar, "valueFrom", `${context}[name=${name}]`);
  const fieldRef = requireRecordField(valueFrom, "fieldRef", `${context}[name=${name}].valueFrom`);
  expectEqual(fieldRef.fieldPath, fieldPath, `${context}[name=${name}].valueFrom.fieldRef.fieldPath`);
}

function assertValueEnv(env: unknown[], name: string, value: string, context: string): void {
  const envVar = requireNamedRecord(env, name, context);
  expectEqual(envVar.value, value, `${context}[name=${name}].value`);
}

function assertHttpProbe(
  container: Record<string, unknown>,
  probeName: "readinessProbe" | "livenessProbe",
  path: string,
  initialDelaySeconds: number,
  periodSeconds: number,
  failureThreshold?: number
): void {
  const probe = requireRecordField(container, probeName, "StatefulSet/opendb container opendb-node");
  const httpGet = requireRecordField(probe, "httpGet", `StatefulSet/opendb container opendb-node.${probeName}`);
  expectEqual(httpGet.path, path, `StatefulSet/opendb container opendb-node.${probeName}.httpGet.path`);
  expectEqual(httpGet.port, "health", `StatefulSet/opendb container opendb-node.${probeName}.httpGet.port`);
  expectEqual(
    probe.initialDelaySeconds,
    initialDelaySeconds,
    `StatefulSet/opendb container opendb-node.${probeName}.initialDelaySeconds`
  );
  expectEqual(probe.periodSeconds, periodSeconds, `StatefulSet/opendb container opendb-node.${probeName}.periodSeconds`);
  if (failureThreshold !== undefined) {
    expectEqual(
      probe.failureThreshold,
      failureThreshold,
      `StatefulSet/opendb container opendb-node.${probeName}.failureThreshold`
    );
  }
}

function assertRoleRules(role: Manifest): void {
  const rules = requireArray(role.rules, "Role/opendb-operator.rules");

  for (const [index, ruleValue] of rules.entries()) {
    const rule = requireRecord(ruleValue, `Role/opendb-operator.rules[${index}]`);
    const apiGroups = requireStringArrayField(rule, "apiGroups", `Role/opendb-operator.rules[${index}]`);
    const resources = requireStringArrayField(rule, "resources", `Role/opendb-operator.rules[${index}]`);
    const verbs = requireStringArrayField(rule, "verbs", `Role/opendb-operator.rules[${index}]`);

    if (
      apiGroups.includes("db.opendb.dev") &&
      resources.includes("opendbclusters") &&
      resources.includes("opendbclusters/status")
    ) {
      expectIncludesAll(
        verbs,
        ["get", "list", "watch", "update", "patch"],
        `Role/opendb-operator.rules[${index}].verbs`
      );
      return;
    }
  }

  fail("Role/opendb-operator.rules must include db.opendb.dev resources opendbclusters and opendbclusters/status");
}

function assertOpenDbCluster(): void {
  const cluster = requireManifest("OpenDbCluster", "opendb");
  const spec = requireSpec(cluster, "OpenDbCluster/opendb");
  expectEqual(spec.replicas, 3, "OpenDbCluster/opendb.spec.replicas");
  expectEqual(spec.image, "opendb-node:dev", "OpenDbCluster/opendb.spec.image");
  expectEqual(spec.storageClassName, "local-path", "OpenDbCluster/opendb.spec.storageClassName");
  expectEqual(spec.storageSize, "1Gi", "OpenDbCluster/opendb.spec.storageSize");
  expectEqual(spec.pgwirePort, 5432, "OpenDbCluster/opendb.spec.pgwirePort");
  expectEqual(spec.healthPort, 8080, "OpenDbCluster/opendb.spec.healthPort");
}

function assertService(
  name: "opendb-peer" | "opendb-pgwire",
  portName: "internal" | "pgwire",
  port: number,
  targetPort: number,
  options: { headless?: boolean; exactAppSelector?: boolean } = {}
): void {
  const key = `Service/${name}`;
  const service = requireManifest("Service", name);
  const spec = requireSpec(service, key);
  if (options.headless === true) {
    expectEqual(spec.clusterIP, "None", `${key}.spec.clusterIP`);
    expectEqual(spec.publishNotReadyAddresses, true, `${key}.spec.publishNotReadyAddresses`);
  }

  const selector = requireRecordField(spec, "selector", `${key}.spec`);
  if (options.exactAppSelector === true) {
    assertOnlyAppSelector(selector, `${key}.spec.selector`);
  } else {
    assertAppSelector(selector, `${key}.spec.selector`);
  }

  assertServicePort(requireArrayField(spec, "ports", `${key}.spec`), portName, port, targetPort, `${key}.spec.ports`);
}

function assertStatefulSet(): void {
  const statefulSet = requireManifest("StatefulSet", "opendb");
  const spec = requireSpec(statefulSet, "StatefulSet/opendb");
  expectEqual(spec.replicas, 3, "StatefulSet/opendb.spec.replicas");
  expectEqual(spec.serviceName, "opendb-peer", "StatefulSet/opendb.spec.serviceName");
  expectEqual(spec.podManagementPolicy, "Parallel", "StatefulSet/opendb.spec.podManagementPolicy");

  const selector = requireRecordField(spec, "selector", "StatefulSet/opendb.spec");
  const matchLabels = requireRecordField(selector, "matchLabels", "StatefulSet/opendb.spec.selector");
  assertAppSelector(matchLabels, "StatefulSet/opendb.spec.selector.matchLabels");

  const template = requireRecordField(spec, "template", "StatefulSet/opendb.spec");
  const templateMetadata = requireRecordField(template, "metadata", "StatefulSet/opendb.spec.template");
  const templateLabels = requireRecordField(templateMetadata, "labels", "StatefulSet/opendb.spec.template.metadata");
  assertAppSelector(templateLabels, "StatefulSet/opendb.spec.template.metadata.labels");

  const templateSpec = requireRecordField(template, "spec", "StatefulSet/opendb.spec.template");
  expectEqual(templateSpec.terminationGracePeriodSeconds, 30, "StatefulSet/opendb.spec.template.spec.terminationGracePeriodSeconds");

  const containers = requireArrayField(templateSpec, "containers", "StatefulSet/opendb.spec.template.spec");
  const nodeContainer = requireNamedRecord(containers, "opendb-node", "StatefulSet/opendb.spec.template.spec.containers");
  expectEqual(nodeContainer.image, "opendb-node:dev", "StatefulSet/opendb container opendb-node.image");
  expectEqual(nodeContainer.imagePullPolicy, "IfNotPresent", "StatefulSet/opendb container opendb-node.imagePullPolicy");

  const args = requireStringArrayField(nodeContainer, "args", "StatefulSet/opendb container opendb-node");
  expectIncludes(args, "--node-id=$(OPENDB_POD_NAME)", "StatefulSet/opendb container opendb-node.args");
  expectIncludes(args, "--internal-addr=$(OPENDB_INTERNAL_ADDR)", "StatefulSet/opendb container opendb-node.args");
  expectIncludes(args, "--advertise-addr=$(OPENDB_ADVERTISE_ADDR)", "StatefulSet/opendb container opendb-node.args");
  expectIncludes(args, "--initial-peers=$(OPENDB_INITIAL_PEERS)", "StatefulSet/opendb container opendb-node.args");
  expectIncludes(args, "--bootstrap-node-id=$(OPENDB_BOOTSTRAP_NODE_ID)", "StatefulSet/opendb container opendb-node.args");

  const env = requireArrayField(nodeContainer, "env", "StatefulSet/opendb container opendb-node");
  const envContext = "StatefulSet/opendb container opendb-node.env";
  assertFieldRefEnv(env, "OPENDB_POD_NAME", "metadata.name", envContext);
  assertFieldRefEnv(env, "OPENDB_POD_NAMESPACE", "metadata.namespace", envContext);
  assertValueEnv(env, "OPENDB_INTERNAL_ADDR", "0.0.0.0:7000", envContext);
  assertValueEnv(env, "OPENDB_ADVERTISE_ADDR", expectedAdvertiseAddr, envContext);
  assertValueEnv(env, "OPENDB_INITIAL_PEERS", expectedInitialPeers, envContext);
  assertValueEnv(env, "OPENDB_BOOTSTRAP_NODE_ID", "0", envContext);
  assertValueEnv(env, "OPENDB_DATA_DIR", "/var/lib/opendb", envContext);

  const ports = requireArrayField(nodeContainer, "ports", "StatefulSet/opendb container opendb-node");
  assertContainerPort(ports, "pgwire", 5432, "StatefulSet/opendb container opendb-node.ports");
  assertContainerPort(ports, "health", 8080, "StatefulSet/opendb container opendb-node.ports");
  assertContainerPort(ports, "internal", 7000, "StatefulSet/opendb container opendb-node.ports");

  assertHttpProbe(nodeContainer, "readinessProbe", "/ready", 2, 1, 1);
  assertHttpProbe(nodeContainer, "livenessProbe", "/live", 10, 10);

  const volumeMounts = requireArrayField(nodeContainer, "volumeMounts", "StatefulSet/opendb container opendb-node");
  const dataMount = requireNamedRecord(volumeMounts, "data", "StatefulSet/opendb container opendb-node.volumeMounts");
  expectEqual(dataMount.mountPath, "/var/lib/opendb", "StatefulSet/opendb container opendb-node.volumeMounts[name=data].mountPath");

  const volumeClaimTemplates = requireArrayField(spec, "volumeClaimTemplates", "StatefulSet/opendb.spec");
  const firstVolumeClaimTemplate = requireRecord(volumeClaimTemplates[0], "StatefulSet/opendb.spec.volumeClaimTemplates[0]");
  const metadata = requireRecordField(firstVolumeClaimTemplate, "metadata", "StatefulSet/opendb.spec.volumeClaimTemplates[0]");
  expectEqual(metadata.name, "data", "StatefulSet/opendb.spec.volumeClaimTemplates[0].metadata.name");

  const volumeSpec = requireRecordField(firstVolumeClaimTemplate, "spec", "StatefulSet/opendb.spec.volumeClaimTemplates[0]");
  expectIncludes(
    requireStringArrayField(volumeSpec, "accessModes", "StatefulSet/opendb.spec.volumeClaimTemplates[0].spec"),
    "ReadWriteOnce",
    "StatefulSet/opendb.spec.volumeClaimTemplates[0].spec.accessModes"
  );
  expectEqual(volumeSpec.storageClassName, "local-path", "StatefulSet/opendb.spec.volumeClaimTemplates[0].spec.storageClassName");

  const resources = requireRecordField(volumeSpec, "resources", "StatefulSet/opendb.spec.volumeClaimTemplates[0].spec");
  const requests = requireRecordField(resources, "requests", "StatefulSet/opendb.spec.volumeClaimTemplates[0].spec.resources");
  expectEqual(requests.storage, "1Gi", "StatefulSet/opendb.spec.volumeClaimTemplates[0].spec.resources.requests.storage");
}

const requiredResources: Array<[kind: string, name: string]> = [
  ["OpenDbCluster", "opendb"],
  ["Service", "opendb-peer"],
  ["Service", "opendb-pgwire"],
  ["StatefulSet", "opendb"],
  ["ServiceAccount", "opendb-operator"],
  ["Role", "opendb-operator"],
  ["RoleBinding", "opendb-operator"]
];

for (const [kind, name] of requiredResources) {
  requireManifest(kind, name);
}

assertRoleRules(requireManifest("Role", "opendb-operator"));
assertOpenDbCluster();
assertService("opendb-peer", "internal", 7000, 7000, { headless: true });
assertService("opendb-pgwire", "pgwire", 5432, 5432, { exactAppSelector: true });
assertStatefulSet();

console.log("Kubernetes manifests passed static checks.");
