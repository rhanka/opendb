# k3s UAT

## Provisioning a local cluster

`npm run k3s:up` provisions a local k3d-backed k3s cluster end-to-end:

1. installs the `k3d` binary into `~/.local/bin/k3d` if missing (override with `OPENDB_K3D_BIN` / `--k3d-bin`);
2. runs `cargo build --release -p opendb-node -p opendb-operator` (skip with `--skip-build` / `OPENDB_K3D_SKIP_BUILD=1`);
3. builds the `opendb-node:dev` and `opendb-operator:dev` images from `deploy/docker/Dockerfile.node` and `deploy/docker/Dockerfile.operator`;
4. creates a k3d cluster named `opendb-dev` (override with `--cluster-name` / `OPENDB_K3D_CLUSTER`);
5. imports both images into the cluster.

After it finishes, `kubectl config current-context` reports `k3d-opendb-dev` and the smoke UAT below can be run.

```bash
npm run k3s:up                       # provision (cargo + docker + k3d)
npm run smoke:k3s                    # default non-destructive UAT
npm run smoke:k3s -- --with-restart-recovery   # opt-in restart recovery UAT
npm run k3s:down                     # tear down the local cluster
```

The provisioning script never touches non-local kube contexts: it only creates and imports into the locally-managed k3d cluster. The smoke command continues to refuse non-`k3d-*` / `kind-*` / `k3s` / `minikube` / `docker-desktop` / `rancher-desktop` contexts unless `--allow-nonlocal-context` is also passed.

Docker must be running before `npm run k3s:up`; the script exits with a clear error if the daemon is not reachable.

## Smoke UAT

The smoke UAT validates the milestone-1 Kubernetes path against a real k3s-compatible cluster:

1. apply the generated `OpenDbCluster` CRD;
2. apply `deploy/k8s/base/`;
3. wait for the operator Deployment;
4. wait for three OpenDB node processes to be running;
5. wait for `OpenDbCluster/status` to report `Ready` with a leader pod;
6. port-forward `service/opendb-pgwire`;
7. run the pgwire SQL smoke test through the Kubernetes Service.

Do not wait on `rollout status statefulset/opendb`: follower Pods intentionally fail the `/ready` probe while the leader owns the pgwire Service.

## Recovery Contract UAT

The restart recovery UAT remains PVC/local-path only; no object storage service is required. It does not require MinIO, S3, GCS, Azure Blob, or any cloud archive endpoint.

The documented recovery scenario is:

1. create a table and insert a recovery smoke row through pgwire;
2. identify and delete the current leader pod;
3. wait for `OpenDbCluster/status` to report `Ready` with a leader pod again and `conditions[type=Recovered].status=True`;
4. query the recovery smoke row through pgwire.

`npm run smoke:k3s` is non-destructive by default: it documents the scenario in the printed plan but does not delete pods or insert recovery rows. To execute the restart recovery scenario end-to-end, pass `--with-restart-recovery`:

```bash
npm run smoke:k3s -- --with-restart-recovery
```

This creates a temporary `recovery_smoke_*` table through pgwire, deletes the current leader pod, polls `OpenDbCluster.status.conditions[type=Recovered].status` until it flips to `True` with a fresh leader, and re-queries the inserted row. The flag still respects the kube-context allow-list and requires `--allow-nonlocal-context` for non-local clusters. The same behavior can be enabled via `OPENDB_K3S_WITH_RESTART_RECOVERY=1`.

The operator-lite consumes each node's `/status` endpoint and reflects the recovery contract as standard Kubernetes conditions on `OpenDbCluster.status.conditions`:

- `RootDescriptorKnown`
- `WalReplayCompleted`
- `ArchiveMetadataKnown`
- `Recovered` (True iff `Ready` is True and the three above are True)

A condition is `Unknown` while any running pod is unreachable, `False` only when at least one running pod explicitly reports it as false. `OpenDbCluster.status.phase` semantics are unchanged: `phase=Ready` continues to report kube readiness, not database recovery.

Sprint 4 range split/merge metadata is replayed from the canonical WAL only; it does not add object storage, extra pods, or a destructive smoke default.

Sprint 5 adds an opt-in `--with-range-split` flag that exercises the runtime split admin endpoint on the leader (`POST /admin/ranges/split` on container port 7300, reached through `kubectl port-forward`) and verifies the new `RangeCatalogStable` condition flips to `True` with `activeRangeCount=2` after the split. The default smoke does not call any admin endpoint and remains non-destructive. Set `OPENDB_K3S_WITH_RANGE_SPLIT=1` or pass `--with-range-split` to enable it.

Sprint 6 extends the SQL surface to cover the columns used by Drizzle-style schemas: `BOOL` / `BOOLEAN`, `FLOAT8` / `FLOAT64` / `DOUBLE PRECISION`, `TIMESTAMP`, `INTEGER` (alias of `INT`); the `NOT NULL` modifier; `DEFAULT <literal>` (`'completed'`, `0`, `false`, `null`); `DEFAULT NOW()` for `TIMESTAMP` columns; and the named-column `INSERT INTO t (a, b) VALUES (…)` form. `DEFAULT NOW()` resolves to the engine's monotonic `LogicalTimestamp` (microseconds), not wall-clock time — replays are therefore deterministic. pgwire emits PG OIDs (16 BOOL, 701 FLOAT8, 1114 TIMESTAMP, 25 TEXT, 20 INT8) on RowDescription; for empty result sets the OIDs default to TEXT until Sprint 7 propagates the column type end to end.

### Known limitation surfaced by `--with-restart-recovery`

After a leader-pod delete, the existing recovery contract does not yet propagate the root bootstrap descriptor to a follower that comes back with an empty local WAL. Such a follower replays zero records, reports `rootDescriptorKnown=false`, and the cluster-wide `Recovered` condition stays `False` with reason `RootDescriptorMissing`. This is a true reflection of cluster state, not a Sprint 3 regression: Sprint 3 only adds visibility. The propagation gap will be addressed in a later sprint (split/merge metadata or Raft snapshot install). Use `npm run smoke:k3s -- --with-restart-recovery` as a diagnostic in the meantime — it will fail loudly on this open issue rather than masking it.

The node `/status` endpoint reports root descriptor known, WAL replay completed, last replayed `tx_id` / `ts`, archive metadata replayed, and the latest known recovery artifact when any exists. Archive metadata is local replay metadata only in this sprint; recovery artifacts describe coverage but no upload or download happens.

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
