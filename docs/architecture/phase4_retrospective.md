# NEXUS OS — Phase 4 Retrospective

> **Closed:** Sprint 4t · 2026-05-09
> **Span:** Sprint 4a (frontend production build) → Sprint 4s (k8s policy resources)
> **Scope:** Backend basecamp through enterprise-ready production deploy.
> **Authors:** sanghyun6467 + Claude Opus 4.7 (paired).

Phase 4 took the project from a feature-complete frontend
(`d477e67 — NEXUS OS Core Engine Complete`) through a fully-typed,
fully-tested, fully-deployable backend service plus the entire CI/CD
and Kubernetes substrate underneath it.

This retrospective is organized around the four architectural decisions
that shaped the phase, not chronologically — the sprint chronicle is at
the end for reference.

---

## 1. By the numbers

| Metric | Backend (`nexus-backend`) | Frontend (`repo`) |
|---|---:|---:|
| Phase 4 commits | 11 | 6 |
| Source files (py / ts+tsx) | 39 | 21 |
| Python LOC (src + tests + db) | 3,505 | — |
| Tests at end of Phase 4 | 50 local / 72 CI | tsc + ESLint gate |
| Type-checker | `mypy --strict` (0 errors / 40 files) | `tsc -b --noEmit` (0 errors), `eslint --max-warnings 0` |
| Container image | multi-stage, non-root UID 10001, ~75 MB smaller than v1 | (frontend deploy is Phase 5) |
| K8s manifests | 10 (3 workloads + ConfigMap + 2 Secrets + Services + HPA + PDB + 5 NetworkPolicies) | — |
| CI gates | mypy → pytest → compose validate → docker buildx → GHCR publish | eslint → tsc → vite build → artifact upload |

---

## 2. Four architectural pillars

### 2.1. Frontend ↔ Backend decoupling via Redis Pub/Sub

The single most important decision in Phase 4 was **never letting the
frontend talk to a single producer**. From day one of Sprint 4j, the
data path was:

```
[ producer ]  →  Redis "nexus.market.tick"  →  N × WebSocket sessions
```

The producer is interchangeable: today it's `MockPublisher` (Sprint 4j,
synthetic random walk); tomorrow it's `KisClient` (Phase 5, real KIS
WebSocket). Subscribers don't care — same channel, same payload shape.

**Why this matters:**
- A second producer (e.g. an analysis module computing anomaly scores)
  can publish to its own channel without touching transport code.
- Horizontal API scaling (Sprint 4s HPA → 10 replicas) works because
  Redis fans out to whichever pod the browser connected to. There is
  no per-pod state.
- The mock-to-live cutover in Phase 5 is a one-line change in lifespan
  gating (`if not has_kis_creds`) — the channel name, payload shape,
  and the WebSocket consumer don't change.

The `BackendStreamer.ts` on the frontend implements `IMarketStreamer`
in full, so it slots into `useMarketData(streamer)` exactly the same
way as `MockStreamer`/`MomentumStreamer`/`HanTooStreamer`. Sprint 4k's
`BACKEND·LIVE` source toggle is just a fourth radio button — zero
changes to the canvas, the diff engine, or the HUD layer.

### 2.2. Strict type safety, end-to-end

`mypy --strict` (Sprint 4l) and `tsc` with `noUncheckedIndexedAccess` +
`exactOptionalPropertyTypes` + `noImplicitOverride` (Sprint 4m) are
enforced at three places:

1. **Pre-commit hook** (Sprint 4p) — fails the local `git commit`.
2. **CI** (Sprints 4f / 4l / 4m) — fails the GitHub Actions run.
3. **CI publish gate** (Sprint 4r) — `image-publish` job has
   `needs: [test, compose-validate]`, so a type regression cannot
   produce a published image.

**Numbers from Sprint 4l/4m:**
- Backend mypy strict: 52 → 29 → 3 → 0 errors over three passes.
- Frontend tsc strict: 93 → 73 → 17 → 0 errors over three passes.
- ESLint after the strict pass: **0 errors, 0 warnings** on every
  module in `src/`.

The `# type: ignore` count in the entire backend is **3** — all
documented as third-party stub gaps (`redis-py`'s `from_url` classmethod,
Starlette's `add_exception_handler` over-narrow handler signature, the
pure-ASGI vs `_MiddlewareClass` protocol mismatch). `warn_unused_ignores`
is on, so the moment any of those upstream stubs is fixed we get an
automatic notification.

### 2.3. Error boundaries — RFC 7807 + state machines

Phase 4 systematically replaced ad-hoc error returns with explicit
**discriminated envelopes** that the frontend Error Boundary can
switch on without parsing English:

- **Backend → frontend HTTP errors** (Sprint 4d-alt) ride
  `application/problem+json` per RFC 7807. Three handlers cover the
  full surface (`StarletteHTTPException` → 4xx, `RequestValidationError`
  → 422 with `errors[]`, generic `Exception` → 500 sanitized). Stable
  problem-type URIs (`/problems/auth-failed`, `/problems/validation-error`,
  `/problems/internal-error`) double as switch keys for the frontend.
- **Auth failures** (Sprint 4b) emit one of 12 structured `auth_failed`
  log lines per branch (`malformed_header`, `missing_kid`, `unknown_kid`,
  `jwks_lookup`, `decode`, `tenant_mismatch`, `no_subject`,
  `missing_token`, `wrong_scheme`, etc.). The 401 detail stays generic
  for the client; the operator sees the precise reason in the logs
  correlated by `request_id`.
- **Streamer states** are 7-state machines on both the backend
  (`KisConnectionState`) and frontend (`ConnectionState`):
  `disconnected → authenticating → connecting → connected →
  reconnecting → failed | replay`. The TopBar status pill renders
  directly from this enum.
- **Schema mismatches** (Sprint 4g) are first-class: `/v1/readyz` returns
  a `MigrationStatusDTO` with `applied / expected / ok / reason`, and
  the body literally tells the operator `"applied=0 < expected=1: run
  db/migrate.py"`.
- **Request correlation** (Sprint 4c-alt) is a single `X-Request-ID`
  that flows: browser → ASGI middleware → ContextVar → every log line
  in the request lifecycle → response header → response body
  (under `request_id`). One ID lets an operator grep across every layer.

### 2.4. The DevOps / SRE pipeline

The four sprints 4o → 4q → 4r → 4s are a **deploy quad** that only
works as a unit. Each closes a cross-file integration gap that none
of the per-file gates (mypy, tsc, eslint, pytest) could ever see:

| Sprint | Gap closed |
|---|---|
| 4o | k8s manifests (probes / volumes / securityContext) — but no image to pull, and Dockerfile didn't satisfy securityContext. |
| 4q | Dockerfile multi-stage + UID 10001 → satisfies k8s `runAsUser: 10001 / readOnlyRootFilesystem: true`. |
| 4r | CI publishes to GHCR; k8s manifest references `ghcr.io/...` instead of unreachable `nexus-backend:latest`. |
| 4s | HPA (auto-scale) + PDB (no-zero-capacity guarantee) + NetworkPolicy (default-deny + selective allows) → fully governed namespace. |

Two of those gaps would have produced **`CrashLoopBackOff` / `ImagePullBackOff`** at first `kubectl apply`. The bugs were caught
only by reading the manifests against the Dockerfile against the
workflow against the image reference together — a class of integration
issue that pre-commit and CI inherently cannot see (each tool only
inspects one file at a time).

The pre-commit hooks (Sprint 4p) layer on top: every gate that ran in
CI now also runs at `git commit` so the round-trip is seconds, not
minutes.

The 6 main supply-chain choices:

- **Multi-stage Docker build** (4q): builder stage carries gcc /
  libpq-dev / build-essential and is discarded; the runtime image is
  ~75 MB smaller and free of build-time CVEs.
- **Non-root by construction** (4q): Dockerfile creates UID 10001
  matching k8s `securityContext.runAsUser`. `USER 10001:10001`
  switches before `EXPOSE`. `tini` is PID 1 for proper SIGTERM
  propagation.
- **Read-only rootfs** (4q + 4o): `PYTHONDONTWRITEBYTECODE=1` so
  Python doesn't try to write `.pyc` files; tmpfs `/tmp` mount gives
  Python the writable scratch space `httpx` / `asyncio` need.
- **Provenance + SBOM** (4r): `provenance: true` and `sbom: true` on
  `docker/build-push-action@v5`. `cosign verify-attestation` confirms
  any image came from this repo's CI, not a tampered local build.
- **Default-deny NetworkPolicy** (4s): even another compromised pod
  in `nexus-os` can't talk to the database. The only allowlist hops
  are `ingress-controller → api`, `api → timescaledb`, `api → redis`,
  and `* → kube-dns`.
- **PodDisruptionBudget on the DB** (4s): the *only* DB pod can't be
  drained by mistake. An operator who runs `kubectl drain` on the
  node hosting it gets an explicit refusal and has to scale a read
  replica up first.

---

## 3. Sprint chronicle

```
Backend                         | Frontend                       | Cross-cutting
================================+================================+========================
                                | 4a — production build (d477e67)|
4b — Entra JWT validation       |                                |
4c-alt — JSON logs + request_id |                                |
4d — TimescaleDB + repository   |                                |
4d-alt — RFC 7807 problem+json  |                                |
4e — pytest harness (28 tests)  |                                |
4f — backend CI workflow        | 4f — frontend CI workflow      |
4g — schema verify + readyz     |                                |
4h — dev seed data (KRX)        |                                |
4j — mock publisher + WS        | 4i — typed API contract        |
                                | 4j — BackendStreamer           |
                                | 4k — BACKEND·LIVE source toggle|
4l — mypy --strict (0 errors)   |                                |
                                | 4m — strict tsc + ESLint       |
4n — API integration tests      |                                |
4o — k8s manifests (7 files)    |                                |
4p — pre-commit hooks (backend) | 4p — pre-commit hooks (frontend)|
4q — Dockerfile hardening       |                                |
4r — GHCR publish               |                                |
4s — HPA + PDB + NetworkPolicy  |                                |
                                |                                | 4t — THIS DOCUMENT
```

---

## 4. Lessons learned

### 4.1. Per-file gates can't catch cross-file integration bugs

mypy, tsc, eslint, pytest all inspect ONE file at a time (or one
module graph). They won't catch:

- Dockerfile says root, k8s requires UID 10001 (4q).
- k8s says `image: foo:latest`, CI says `push: false` (4r).
- DTO field name `schema` shadows `BaseModel.schema()` and only
  trips `filterwarnings = ["error"]` at runtime (4g).

The fix isn't more linters — it's reading the surfaces against each
other on every architectural change. Half of Phase 4's "polish"
sprints (4q / 4r / 4s) were debt accumulated from earlier
non-coordinated changes.

### 4.2. HMR is helpful right up until it isn't

Three times in Phase 4 (3e, 4m, 4o earlier) we hit the React HMR
"useEffect dep array changed size between renders" trap. Vite caches
modified module graphs across edits, and when a developer changes a
hook's dep array mid-session, the running app sees a different
fingerprint than the bundler. The fix is always the same:
`preview_stop` → `preview_start`.

It's documented now (Sprint 4m commit message) so the next iteration
won't re-debug it from scratch.

### 4.3. Strict mode pays for itself in hours, not weeks

Adding `noUncheckedIndexedAccess` to a 21-file frontend produced 93
errors. Fixing them took one sprint (4m). The same flags would have
caught the canvas's `tx[i].x` dereference bugs that we previously
debugged at runtime. Going forward, every new module starts strict.

### 4.4. The test pyramid was upside-down at first

Sprints 4e (28 unit tests) → 4n (14 integration tests) found a
genuine architectural assumption mismatch: `get_pool()` and
`get_client()` aren't FastAPI `Depends()`, they're module-level
singletons. `dependency_overrides` won't reach them; tests need to
patch `src.infrastructure.database._pool` directly. That insight was
hidden in the unit tests; it surfaced the moment we tried to drive
the actual ASGI cycle.

The 50 / 72-test split (local / CI) is the right shape going forward:
fast feedback on every commit, full integration on every push.

---

## 5. What "production-ready" means at the end of Phase 4

After 4t closes, an operator can:

```bash
# 1. Frontend — already shipping
cd repo
npm run lint && npx tsc -b --noEmit && npm run build
# → 0 errors / 0 warnings, dist/ ready for any static host

# 2. Backend — full stack, fully governed
cd nexus-backend
docker compose up -d                     # local dev
python -m db.migrate && python -m db.seed
curl localhost:8000/v1/readyz            # → ok=true

# 3. Production deploy
git push origin v1.0.0                   # → CI builds & pushes to GHCR
kubectl apply -f deploy/k8s/             # → 10 manifests, fully governed namespace
kubectl -n nexus-os exec deploy/nexus-api -- python -m db.migrate
curl https://nexus-api.example.com/v1/readyz
# → { "ok": true, "database": true, "redis": true,
#     "migration": { "applied": 1, "expected": 1, "ok": true } }
```

What's still mock — and what Phase 5 fixes — is **what's on the
channel**. The mock publisher emits synthetic random-walk ticks for
the 12 seeded KRX symbols. The frontend renders them correctly, the
state machines transition correctly, the database schema accepts them
correctly. But they aren't real market data. See
[`phase5_roadmap.md`](phase5_roadmap.md) for the cutover plan.

---

## 6. Repository state (Phase 4 close)

```
nexus-backend/
├── .dockerignore                          (4q)
├── .env.example                            (basecamp)
├── .github/workflows/backend.yml          (4f, 4l, 4r)
├── .gitignore
├── .pre-commit-config.yaml                (4p)
├── Dockerfile                              (basecamp → 4q)
├── README.md
├── db/
│   ├── migrations/001_init.sql            (4d)
│   ├── seeds/dev.sql                      (4h)
│   ├── migrate.py                         (4d)
│   └── seed.py                            (4h)
├── deploy/k8s/                             (4o + 4q + 4r + 4s)
│   ├── README.md
│   ├── namespace.yaml
│   ├── configmap.yaml
│   ├── secret.yaml
│   ├── timescaledb-statefulset.yaml
│   ├── redis-deployment.yaml
│   ├── api-deployment.yaml                (probes / securityContext)
│   ├── services.yaml
│   ├── hpa.yaml                           (4s)
│   ├── pdb.yaml                           (4s)
│   └── networkpolicy.yaml                 (4s)
├── docker-compose.yml
├── docs/architecture/
│   ├── phase4_retrospective.md            (4t — this file)
│   └── phase5_roadmap.md                  (4t)
├── pyproject.toml                          (4e mypy / pytest config)
├── requirements.txt
├── requirements-dev.txt                    (4l, 4e)
└── src/
    ├── api/v1/                            (router, dto, exception_handlers via core)
    ├── api/websockets/stream.py           (4j /v1/stream)
    ├── core/                              (config, security, logging, errors,
    │                                        exception_handlers, middleware)
    ├── domain/market/                     (models, repository — 4d)
    ├── domain/analysis/                   (anomaly z-score baseline)
    ├── infrastructure/                    (database, redis_pubsub, kis_client stub,
    │                                        mock_publisher)
    └── main.py
```
