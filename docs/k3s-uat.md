# k3s UAT

The smoke UAT validates the milestone-1 Kubernetes path against a real k3s-compatible cluster:

1. apply the generated `OpenDbCluster` CRD;
2. apply `deploy/k8s/base/`;
3. wait for the operator Deployment;
4. wait for three OpenDB node processes to be running;
5. wait for `OpenDbCluster/status` to report `Ready` with a leader pod;
6. port-forward `service/opendb-pgwire`;
7. run the pgwire SQL smoke test through the Kubernetes Service.

Do not wait on `rollout status statefulset/opendb`: follower Pods intentionally fail the `/ready` probe while the leader owns the pgwire Service.

```bash
npm run smoke:k3s:plan
npm run smoke:k3s
```

Equivalent UAT aliases:

```bash
npm run uat:k3s:plan
npm run uat:k3s
```

The manifests expect `opendb-node:dev` and `opendb-operator:dev` to be available to k3s with `imagePullPolicy: IfNotPresent`.
The smoke command refuses non-local kube contexts by default. Accepted context names are `k3s`, `k3d`, `kind`, `minikube`, `docker-desktop`, `rancher-desktop`, plus `k3d-*` and `kind-*`; use `--allow-nonlocal-context` only when the target context is explicitly intended.

```bash
npm run smoke:k3s -- --allow-nonlocal-context
```

Useful overrides:

```bash
OPENDB_LOCAL_PGWIRE_PORT=15432 \
OPENDB_K3S_SMOKE_TIMEOUT_MS=120000 \
npm run uat:k3s
```

Namespace, cluster name, and expected replica count are intentionally fixed while `deploy/k8s/base/` is static.
