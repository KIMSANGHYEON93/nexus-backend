# Cross-PC Session Handoff

> **Snapshot:** 2026-05-09
> **From:** Phase 4 closed (Sprint 4t) + KIS paper credentials issued + ready for Sprint 5c
> **Audience:** the operator booting the project on a fresh machine

This doc is the minimum-viable resume kit. After 5 minutes of setup
on a new PC the project should be in the same operational state as
the source machine.

---

## 1. Snapshot of Where We Were

| Phase | Status |
|---|---|
| Phase 4 (basecamp → enterprise deploy) | **CLOSED** by Sprint 4t — see [phase4_retrospective.md](phase4_retrospective.md) |
| Phase 5 G5.1 (KIS paper credentials issued) | **PASSED** — credentials populated on source PC |
| Phase 5 Sprint 5a–5g | **NOT YET STARTED** |
| Open Q1 (LLM provider — OpenAI vs Ollama) | **UNRESOLVED** — see [phase5_roadmap.md](phase5_roadmap.md) §7 |
| GitHub repos | Both **public** under `KIMSANGHYEON93/` (CI green, GHCR image live) |

**Git history (Phase 4 final HEADs at handoff time):**

| Repo | URL | HEAD | Total commits |
|---|---|---|---|
| Frontend | https://github.com/KIMSANGHYEON93/nexus-os | `6d15dea` | 7 |
| Backend  | https://github.com/KIMSANGHYEON93/nexus-backend | `25e26c1` | 14 |

**GHCR image:**
- `ghcr.io/KIMSANGHYEON93/nexus-backend:latest`
- `ghcr.io/KIMSANGHYEON93/nexus-backend:main`
- `ghcr.io/KIMSANGHYEON93/nexus-backend:sha-25e26c1`

---

## 2. Resume Kit — New PC Setup (5 minutes)

### 2.1. Clone both repos

```bash
mkdir -p ~/nexus-os && cd ~/nexus-os
git clone https://github.com/KIMSANGHYEON93/nexus-os.git frontend
git clone https://github.com/KIMSANGHYEON93/nexus-backend.git backend
```

### 2.2. Install dev tooling

```bash
# Frontend
cd ~/nexus-os/frontend
npm ci                     # the .npmrc legacy-peer-deps=true is already in repo
npm run lint               # confirm baseline: 0 errors / 0 warnings
npx tsc -b --noEmit        # confirm baseline: 0 errors

# Backend
cd ~/nexus-os/backend
python -m pip install -r requirements-dev.txt
mypy                       # confirm baseline: 0 errors / 40 source files
pytest -q                  # confirm baseline: ~50 passed (some skip without docker)
```

### 2.3. Populate `.env` ⚠️ ROTATE FIRST

The `.env` file is **not** in git. It must be populated by hand on the
new PC. **Before doing so:** the KIS keys generated on the source PC
were exposed in a chat session — treat them as compromised and ROTATE.

```
1. Visit https://apiportal.koreainvestment.com/
2. 모의투자 → 앱 발급 정보 → 'APP Secret 재발급' (re-issue secret)
   (You don't need to re-issue APP Key, just the secret.)
3. Copy the new APP Key + new APP Secret
4. Edit ~/nexus-os/backend/.env directly. Slot shape:

   DATABASE_URL=postgresql://nexus_admin:strongpassword123@timescaledb:5432/nexus_os
   REDIS_URL=redis://redis:6379/0

   KIS_APP_KEY=<new app key>
   KIS_APP_SECRET=<new app secret>
   KIS_ACCOUNT_NUMBER=50186971-01
   KIS_ENV=paper

   APP_ENV=development
   LOG_LEVEL=INFO
   CORS_ORIGINS=http://localhost:5173,http://localhost:3000
```

`.env.example` shows the full key shape if you need a template. Never
commit `.env` (already gitignored — `git status` should never show it).

### 2.4. Start the local stack

```bash
cd ~/nexus-os/backend
docker compose up -d                               # postgres + redis + api
python -m db.migrate                                # apply schema
python -m db.seed                                   # 12 KRX entities + edges
curl localhost:8000/v1/readyz | jq                 # → ok=true expected

cd ~/nexus-os/frontend
npm run dev                                         # http://localhost:5173
# In the harness panel pick BACKEND·LIVE source — canvas wires to /v1/stream
```

### 2.5. Verify Phase 5 readiness

```bash
cd ~/nexus-os/backend
grep -c "^KIS_APP_KEY=." .env       # → 1 (set)
grep -c "^KIS_APP_SECRET=." .env    # → 1 (set)
grep -c "^KIS_ACCOUNT_NUMBER=." .env # → 1 (set)
```

If all three show `1`, gate G5.1 is satisfied and Sprint 5c can begin.

---

## 3. Where to Resume

The next sprint is **Sprint 5c — KIS REST OAuth + WebSocket**:

- Implementation lives in `src/infrastructure/kis_client.py` (currently a
  stub — `KisConnectionState` enum and method shells are already
  defined; the connect / authenticate / subscribe bodies are TODO).
- `src/infrastructure/mock_publisher.py` keeps running until KIS is
  wired in — the lifespan gating (`main.py`) flips the producer
  automatically once `KIS_APP_KEY` + `KIS_APP_SECRET` are populated.
- Sprint 5d (lifespan cutover with 24-hour dual-channel) will land in
  the same commit pair as 5c.
- Tests use recorded KIS frame fixtures (none yet — the first paper
  session creates them).

The full Sprint 5c spec is in
[phase5_roadmap.md](phase5_roadmap.md#2-goal-2--live-kis-openapi-cutover).

---

## 4. Critical Reminders

1. **Rotate KIS APP Secret on the new PC** before any first run.
   Source-PC keys are in chat history → assume compromised.
2. **Do NOT commit `.env`.** `.gitignore` blocks it but `git add -f`
   can override — never use `-f` on `.env`.
3. **Never paste secrets in chat / Slack / commit messages.** The
   right path is: KIS portal → `.env` directly. Even the AI agent
   should not see the values; it can write code that READS them via
   `Settings`.
4. **Live trading toggle (`ALLOW_LIVE_ORDERS=true`) is intentionally
   absent** from `.env.example`. Phase 5 Sprint 5g paper-soaks 30
   days before that flag can even be considered.

---

## 5. Dependency Versions Pinned at Handoff Time

For any "but the new PC version is different" debugging:

| Tool | Version |
|---|---|
| Python | 3.11 (CI), 3.14 acceptable for local dev (asyncpg won't compile, tests skip) |
| Node | 20 LTS |
| pytest | 8.4.2 |
| pytest-asyncio | 1.3.0 |
| mypy | 2.0.0 |
| eslint | 9.39.4 |
| @typescript-eslint/parser | 5.62.0 (cached, peer-conflict accepted via .npmrc) |
| TypeScript | 5.9.3 |
| Vite | 8.0.5 |
| FastAPI | 0.109.2 |
| Pydantic | (transitive via pydantic-settings 2.1.0) — 2.12.x at runtime |
| asyncpg | 0.29.0 |
| redis-py | 5.0.1 |
| python-jose | 3.3.0 |
| httpx | 0.26.0 |

---

## 6. Open Questions (still unresolved)

From [phase5_roadmap.md](phase5_roadmap.md#7-open-questions-to-resolve-before-sprint-5a):

1. LLM provider: OpenAI `gpt-4o-mini` vs local Ollama?
2. KIS paper-vs-live cutover criterion: 30-day soak, or canary live orders?
3. Audit retention storage tier: same TimescaleDB hypertable vs separate cold storage?
4. n8n integration sprint ownership: 5b (Macro agent) or later?

These can defer to the start of each relevant sprint; just shouldn't
be answered DURING the sprint.
