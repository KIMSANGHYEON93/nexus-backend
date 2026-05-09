# NEXUS OS Backend — Kubernetes deployment

Initial scaffold for deploying the FastAPI + TimescaleDB + Redis stack
to any Kubernetes cluster (kind, minikube, AKS, GKE, EKS — agnostic).

## Architecture

```
            ┌─────────────────────────────────────────────────┐
            │                Namespace: nexus-os               │
            │                                                  │
            │   ┌──────────────┐   pubsub    ┌─────────────┐  │
            │   │  redis       │◄────────────│  nexus-api  │  │
            │   │  Deployment  │             │ Deployment  │  │
            │   │  ClusterIP   │             │ 2 replicas  │  │
            │   └──────────────┘             └──────┬──────┘  │
            │                                       │ asyncpg │
            │                                       ▼          │
            │                                ┌──────────────┐ │
            │                                │ timescaledb  │ │
            │                                │ StatefulSet  │ │
            │                                │ Headless Svc │ │
            │                                │ + 50Gi PVC   │ │
            │                                └──────────────┘ │
            └─────────────────────────────────────────────────┘
                              ▲
                              │  HTTPS / WebSocket
                              │  (terminated by your Ingress / Gateway API)
                          External traffic
```

## Files in this directory

| File | Purpose |
|------|---------|
| `namespace.yaml`              | Wraps every other resource for clean teardown. |
| `configmap.yaml`              | Non-sensitive runtime env (APP_ENV, LOG_LEVEL, CORS_ORIGINS, KIS_ENV). |
| `secret.yaml`                 | TEMPLATE — placeholder for KIS keys, Entra IDs, and Postgres root credentials. |
| `timescaledb-statefulset.yaml`| Single-replica Postgres + TimescaleDB extension with a 50 Gi PVC. |
| `redis-deployment.yaml`       | Single-replica Redis 7. No persistence — pub/sub is ephemeral. |
| `api-deployment.yaml`         | FastAPI app, 2 replicas, three probes (startup / liveness / readiness). |
| `services.yaml`               | One ClusterIP per workload + headless service for the StatefulSet. |
| `hpa.yaml`                    | HorizontalPodAutoscaler — scales nexus-api 2..10 on CPU 70% / Mem 80%. |
| `pdb.yaml`                    | PodDisruptionBudget for nexus-api + timescaledb (minAvailable: 1). |
| `networkpolicy.yaml`          | Default-deny ingress + selective allows (api↔db, api↔redis, ingress→api). |

## First-time deployment (operator playbook)

### 1. Build and push the API image

`.github/workflows/backend.yml` does this automatically — every push
to `main` and every `v*.*.*` tag pushes a multi-tagged image to GHCR
under `ghcr.io/<owner>/nexus-os-design-system-backend`.

For a manual one-off (e.g. before CI is wired up):

```bash
# Log in to GHCR — the GitHub CLI handles credentials.
echo $GITHUB_TOKEN | docker login ghcr.io -u <your-username> --password-stdin

# From the nexus-backend repo root:
docker build -t ghcr.io/<owner>/nexus-os-design-system-backend:0.1.0 .
docker push    ghcr.io/<owner>/nexus-os-design-system-backend:0.1.0
```

Then edit the image reference in `api-deployment.yaml` to replace the
`OWNER` placeholder with the actual GitHub organization or username.

For production deploys, **pin to a semver tag or commit sha**, not
`:latest`. A rolled-back code change in `main` would otherwise
silently roll forward the next time a pod restarts.

```yaml
# Recommended in api-deployment.yaml for production:
image: ghcr.io/your-org/nexus-os-design-system-backend:v1.2.3
# or:
image: ghcr.io/your-org/nexus-os-design-system-backend:sha-abc1234
```

If your GHCR repo is **private**, also create an `imagePullSecret`:

```bash
kubectl -n nexus-os create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io \
  --docker-username=<your-username> \
  --docker-password=$GITHUB_TOKEN
```

…and reference it in `api-deployment.yaml`:

```yaml
spec:
  template:
    spec:
      imagePullSecrets:
        - name: ghcr-pull
```

### 2. Populate secrets

`secret.yaml` ships with empty placeholders. **Never commit real values
here.** Two recommended workflows:

- **Sealed Secrets** (recommended for git-ops):
  ```bash
  # Edit secret.yaml with real values, then encrypt:
  kubeseal -f secret.yaml -w secret-sealed.yaml
  # Commit secret-sealed.yaml — the unsealed form stays out of git.
  ```

- **External Secrets Operator + Azure Key Vault / AWS Secrets Manager**:
  delete `secret.yaml` and replace with an `ExternalSecret` resource
  that pulls the same keys from your vault.

For a one-off bootstrap (NOT for production), edit `secret.yaml` in place,
apply, and rotate immediately afterward.

### 3. Apply in order

```bash
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml          # or secret-sealed.yaml
kubectl apply -f services.yaml
kubectl apply -f timescaledb-statefulset.yaml
kubectl apply -f redis-deployment.yaml

# Wait for TimescaleDB to be ready before the API tries to connect:
kubectl -n nexus-os rollout status statefulset/timescaledb

kubectl apply -f api-deployment.yaml
kubectl -n nexus-os rollout status deployment/nexus-api

# Policy resources — apply LAST so workloads exist for the selectors
# to bind to. Order within this group does not matter.
kubectl apply -f hpa.yaml             # autoscale 2..10 on CPU/Mem
kubectl apply -f pdb.yaml             # minAvailable: 1 during drains
kubectl apply -f networkpolicy.yaml   # default-deny + selective allows
```

**Cluster prerequisites for the policy resources:**
- `hpa.yaml` requires a metrics-server (`kubectl top pod` must work).
- `networkpolicy.yaml` requires a CNI that enforces NetworkPolicy
  (Calico, Cilium, Antrea — NOT default kindnet).

### 4. Apply the database schema + seed (one-time)

```bash
# Schema (idempotent migration runner):
kubectl -n nexus-os exec deploy/nexus-api -- python -m db.migrate

# Optional: dev seed data — refuses to run unless APP_ENV=development.
# For a real production deploy, skip this and let the KIS adapter
# populate the entity table from live ticks.
kubectl -n nexus-os exec deploy/nexus-api -- python -m db.seed
```

### 5. Smoke test

```bash
# Port-forward and hit /v1/readyz from your laptop:
kubectl -n nexus-os port-forward svc/nexus-api 8000:8000 &
curl -s http://localhost:8000/v1/readyz | jq

# Expected:
#   {
#     "ok": true,
#     "database": true,
#     "redis": true,
#     "migration": { "applied": 1, "expected": 1, "ok": true }
#   }
```

If `migration.ok` is false, see step 4 — migrations weren't applied yet.

### 6. Wire external traffic

Out of scope for this folder (it's cluster-specific), but the typical
shape is:

```
Ingress / Gateway API   →   svc/nexus-api:8000   →   pods
        ▲
        │
   TLS termination, host routing, rate limiting
```

## Probe semantics — why three of them

| Probe | Endpoint | Purpose | Action on failure |
|-------|----------|---------|-------------------|
| `startupProbe`   | `/v1/health` | "Is the app finished booting?" | Suspend liveness / readiness for up to 60 s. |
| `livenessProbe`  | `/v1/health` | "Is the process wedged?" Doesn't touch DB / Redis. | Kill the pod. |
| `readinessProbe` | `/v1/readyz` | "Is the pod ready for traffic?" Pings DB + Redis + verifies schema_version. | Remove from Service endpoints (no kill). |

This separation matters: a pod whose downstream DB is briefly unreachable
should be **drained from the LB** (readiness fails) but not **killed**
(liveness still passes), because killing it doesn't restore the DB and
the restart penalty would just lengthen the outage.

A schema mismatch (`migration.ok=false`) is treated identically to a DB
outage from the load balancer's perspective — the pod stays up reporting
the problem, an operator runs `python -m db.migrate`, and the next
readiness probe goes green without any restart.

## Scaling

- **API**: bump `replicas` in `api-deployment.yaml`. The app is stateless —
  the only shared state is Redis and Postgres, both already shared across
  replicas. A HorizontalPodAutoscaler keyed off CPU + request rate is the
  next step (not in this scaffold).
- **Redis**: do NOT scale beyond 1 replica without switching to Redis
  Cluster mode. Pub/sub does not fan out across non-clustered replicas.
- **TimescaleDB**: scale READS by adding read-replicas (separate
  StatefulSet). WRITES stay on the single primary; multi-master needs
  Patroni or pgpool — explicitly out of scope here.

## Rollback

```bash
kubectl -n nexus-os rollout undo deployment/nexus-api
```

The DB schema is forward-compatible by migration policy — older API
versions read newer schemas without breaking — so a code rollback never
needs a paired schema rollback.

## Tear down

```bash
kubectl delete namespace nexus-os
```

This releases the 50 Gi PVC. To preserve data, set the PVC's
`persistentVolumeReclaimPolicy: Retain` on the PV before deleting the
namespace.
