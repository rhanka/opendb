import { expect, test } from "vitest";
import {
  buildK3sSmokePlan,
  clusterStatusIsReady,
  commandText,
  captureCommand,
  kubeContextIsAllowed,
  parseSmokeOptions,
  portForwardOutputShowsReady,
  summarizeOpenDbPods
} from "../../tools/k3s-smoke.js";

test("k3s smoke plan waits for DB process readiness instead of StatefulSet rollout", () => {
  const plan = buildK3sSmokePlan(parseSmokeOptions([]));
  const commands = plan.map((step) => step.command === undefined ? step.description : commandText(step.command));

  expect(commands).toContain("cargo run -p opendb-operator -- print-crd");
  expect(commands).toContain("kubectl wait --for=condition=Available deployment/opendb-operator -n opendb-system --timeout=120s");
  expect(commands).toContain("kubectl get pods -n opendb-system -l app.kubernetes.io/name=opendb,app.kubernetes.io/instance=opendb -o json");
  expect(commands).toContain("kubectl get opendbcluster opendb -n opendb-system -o json");
  expect(commands).toContain("kubectl port-forward service/opendb-pgwire 15432:5432 -n opendb-system");
  expect(commands).not.toContain("kubectl rollout status statefulset/opendb -n opendb-system --timeout=120s");
});

test("k3s smoke plan uses the configured kubectl command and static milestone manifests", () => {
  const plan = buildK3sSmokePlan(parseSmokeOptions(["--kubectl", "/tmp/kubectl-custom", "--local-pgwire-port", "15433"]));
  const commands = plan.map((step) => step.command === undefined ? step.description : commandText(step.command));

  expect(commands).toContain("/tmp/kubectl-custom config current-context");
  expect(commands).toContain("/tmp/kubectl-custom get pods -n opendb-system -l app.kubernetes.io/name=opendb,app.kubernetes.io/instance=opendb -o json");
  expect(commands).toContain("/tmp/kubectl-custom get opendbcluster opendb -n opendb-system -o json");
  expect(commands).toContain("/tmp/kubectl-custom port-forward service/opendb-pgwire 15433:5432 -n opendb-system");
});

test("k3s smoke plan documents restart recovery without destructive default commands", () => {
  const plan = buildK3sSmokePlan(parseSmokeOptions([]));
  const output = plan
    .map((step, index) => `${index + 1}. ${step.description}${step.command === undefined ? "" : `: ${commandText(step.command)}`}`)
    .join("\n");
  const deleteStep = plan.find((step) => step.description === "delete the current leader pod");

  expect(output).toContain("delete the current leader pod");
  expect(output).toContain("query the recovery smoke row through pgwire");
  expect(output).toContain("no object storage service is required");
  expect(output).toContain("non-destructive default");
  expect(output).not.toContain("kubectl delete pod");
  expect(deleteStep?.command).toBeUndefined();
});

test("k3s smoke plan with --with-restart-recovery attaches kubectl delete pod and Recovered wait", () => {
  const plan = buildK3sSmokePlan(parseSmokeOptions(["--with-restart-recovery"]));
  const output = plan
    .map((step, index) => `${index + 1}. ${step.description}${step.command === undefined ? "" : `: ${commandText(step.command)}`}`)
    .join("\n");
  const deleteStep = plan.find((step) => step.description === "delete the current leader pod");

  expect(output).toContain("kubectl delete pod");
  expect(output).toContain("Recovered");
  expect(deleteStep?.command).toBeDefined();
  expect(deleteStep?.command?.command).toBe("kubectl");
  expect(deleteStep?.command?.args).toEqual(["delete", "pod", "<leader>", "-n", "opendb-system"]);
});

test("pod summary counts running DB containers and ignores terminating pods", () => {
  const summary = summarizeOpenDbPods({
    items: [
      pod("opendb-0", true, true),
      pod("opendb-1", true, false),
      pod("opendb-2", true, false),
      { ...pod("opendb-3", true, true), metadata: { name: "opendb-3", deletionTimestamp: "2026-05-06T00:00:00Z" } }
    ]
  });

  expect(summary.runningNames).toEqual(["opendb-0", "opendb-1", "opendb-2"]);
  expect(summary.leaderReadyNames).toEqual(["opendb-0"]);
  expect(summary.runningCount).toBe(3);
});

test("cluster status readiness requires phase, replica count, and leader pod", () => {
  expect(
    clusterStatusIsReady(
      {
        status: {
          phase: "Ready",
          readyReplicas: 3,
          leaderPod: "opendb-0"
        }
      },
      3
    )
  ).toBe(true);

  expect(
    clusterStatusIsReady(
      {
        status: {
          phase: "Ready",
          readyReplicas: 4,
          leaderPod: "opendb-0"
        }
      },
      3
    )
  ).toBe(false);
});

test("capture command writes stdin to child processes", async () => {
  const output = await captureCommand("cat", [], {
    input: "stdin-payload",
    timeoutMs: 5_000
  });

  expect(output.stdout).toBe("stdin-payload");
});

test("k3s smoke refuses non-local kube contexts unless explicitly allowed", () => {
  expect(kubeContextIsAllowed("k3d", false)).toBe(true);
  expect(kubeContextIsAllowed("k3d-opendb", false)).toBe(true);
  expect(kubeContextIsAllowed("kind", false)).toBe(true);
  expect(kubeContextIsAllowed("kind-opendb", false)).toBe(true);
  expect(kubeContextIsAllowed("prod-kind-admin", false)).toBe(false);
  expect(kubeContextIsAllowed("company-k3s-prod", false)).toBe(false);
  expect(kubeContextIsAllowed("admin@k8s-dataiku", false)).toBe(false);
  expect(kubeContextIsAllowed("admin@k8s-dataiku", true)).toBe(true);
});

test("k3s smoke rejects namespace and cluster overrides while manifests are static", () => {
  expect(() => parseSmokeOptions(["--namespace", "analytics-system"])).toThrow(
    "namespace override is not supported"
  );
  expect(() => parseSmokeOptions(["--cluster-name", "analytics"])).toThrow(
    "cluster-name override is not supported"
  );
  expect(() => parseSmokeOptions(["--expected-replicas", "5"])).toThrow(
    "expected-replicas override is not supported"
  );
});

test("k3s smoke rejects namespace and cluster env overrides while manifests are static", () => {
  withEnv("OPENDB_NAMESPACE", "analytics-system", () => {
    expect(() => parseSmokeOptions([])).toThrow("OPENDB_NAMESPACE override is not supported");
  });
  withEnv("OPENDB_CLUSTER_NAME", "analytics", () => {
    expect(() => parseSmokeOptions([])).toThrow("OPENDB_CLUSTER_NAME override is not supported");
  });
  withEnv("OPENDB_EXPECTED_REPLICAS", "5", () => {
    expect(() => parseSmokeOptions([])).toThrow("OPENDB_EXPECTED_REPLICAS override is not supported");
  });
});

test("port-forward readiness is tied to the launched kubectl output and expected port", () => {
  expect(portForwardOutputShowsReady("Forwarding from 127.0.0.1:15432 -> 5432\n", 15432)).toBe(true);
  expect(portForwardOutputShowsReady("Forwarding from [::1]:15432 -> 5432\n", 15432)).toBe(true);
  expect(portForwardOutputShowsReady("Forwarding from 127.0.0.1:15433 -> 5432\n", 15432)).toBe(false);
  expect(portForwardOutputShowsReady("error: unable to listen on any requested ports\n", 15432)).toBe(false);
});

function pod(name: string, nodeRunning: boolean, podReady: boolean): Record<string, unknown> {
  return {
    metadata: { name },
    status: {
      phase: nodeRunning ? "Running" : "Pending",
      containerStatuses: [
        {
          name: "opendb-node",
          state: nodeRunning ? { running: {} } : { waiting: {} }
        }
      ],
      conditions: [
        {
          type: "Ready",
          status: podReady ? "True" : "False"
        }
      ]
    }
  };
}

function withEnv(name: string, value: string, testFn: () => void): void {
  const previous = process.env[name];
  process.env[name] = value;
  try {
    testFn();
  } finally {
    if (previous === undefined) {
      delete process.env[name];
    } else {
      process.env[name] = previous;
    }
  }
}
