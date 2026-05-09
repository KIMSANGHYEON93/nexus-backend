# NEXUS OS — Phase 5 Roadmap: Trading Agents & Live Market

> **Opens:** 2026-05 (Phase 4 closed by Sprint 4t)
> **Theme:** Replace synthetic data with live market intelligence;
> introduce LLM-driven agents under hard safety guardrails.
> **Sequencing:** Goals are deliberately ordered. Goal 2 (live KIS)
> blocks meaningful evaluation of Goal 1; Goal 3 cannot ship without
> both Goal 1 and Goal 2 in place.

---

## 0. Pre-conditions inherited from Phase 4

Phase 5 sprints inherit a substrate that is already enterprise-grade:

- Mock-vs-live cutover is a **single-line flag** in `main.py` lifespan
  gating (`if dev AND not has_kis_creds`). Replacing the publisher
  doesn't touch the channel name, the payload schema, the WebSocket
  consumer, or the frontend `BackendStreamer`.
- Type safety covers every new module from day one (`mypy --strict` +
  `tsc` strict + ESLint, all gated at pre-commit and CI).
- The error envelope (`application/problem+json`) already covers
  401 / 422 / 500. Phase 5 only adds **new problem-type URIs** — the
  shape stays stable.
- `ContextVar` request_id propagation (Sprint 4c-alt) propagates into
  every async task spawned during Phase 5 work — agent calls, KIS
  websocket frames, analysis pipeline — without any plumbing.
- The k8s namespace already enforces NetworkPolicy default-deny.
  Phase 5 services must add explicit allows; nothing escapes by
  default.

---

## 1. Goal 1 — Taurik TradingAgents v0.2.4 integration

### What we're integrating

[Taurik TradingAgents v0.2.4](https://github.com/Taurik-Capital/TradingAgents)
is a multi-agent framework where specialized LLM agents (Quant /
Macro / Sentiment / Risk) deliberate on a trade thesis. We
integrate two agents first; Sentiment + Risk land in Goal 3.

### Sprint 5a — Quant Agent vertical slice

- New module `src/domain/agents/quant.py`. Pure function over the
  same `Tick` / `Quote` types the analysis baseline uses, so the
  z-score detector and the LLM-based detector can A/B test on
  identical inputs.
- Inference path: feature extraction (rolling stats over a 5 min
  window from `market_tick_1m`) → LLM call → structured JSON output
  validated by a Pydantic model → write to a new `agent_signal`
  TimescaleDB table.
- Pinned to `gpt-4o-mini` initially; structured output schema fixed
  by Pydantic so a model swap is one config change.
- Cost budget: **per-agent token/min cap** in `core/config.py`.
  Hitting the cap moves the agent to `cooling_down` state (returns
  cached prior signal); never burns through the budget silently.

### Sprint 5b — Macro LLM Agent

- Same Pydantic-output / `agent_signal` table contract. Different
  prompt, different feature set (FRED macro indicators, KOSPI futures
  basis, USDKRX). The Quant agent's vertical slice is the template;
  Macro is mostly data plumbing.
- New table `macro_indicator` (Timescale hypertable like
  `market_tick`) so a future agent doesn't have to re-fetch FRED on
  every inference.

### What we're NOT doing in Goal 1

- No order placement. No portfolio state. Agents emit *signals*
  only — read-only against market data, write-only against
  `agent_signal`. The Coordinator (Goal 3) decides whether a signal
  becomes an action.

### Verification gates

- `agent_signal` rows must validate against the Pydantic schema on
  insert (a malformed LLM response is rejected at the boundary, not
  poisoned into the database).
- Per-agent integration tests using a recorded LLM response (vcrpy
  cassette or equivalent), so unit tests stay deterministic and
  cheap.
- Token cost dashboards: `/v1/agents/cost` endpoint, surfaced in the
  TopBar somewhere reasonable.

---

## 2. Goal 2 — Live KIS OpenAPI cutover

### Where we are

`src/infrastructure/kis_client.py` is a stub from Sprint 3g/4a-era.
It owns the documented state machine (`disconnected →
authenticating → connecting → connected → reconnecting → failed`)
and the wire format (`0|H0STCNT0|len|csv-data`) but doesn't connect.
`src/infrastructure/mock_publisher.py` (Sprint 4j) drives the same
Redis channel with synthetic ticks.

### Sprint 5c — REST OAuth + WebSocket connect

- Implement `KisClient.authenticate()`: POST `/oauth2/tokenP` with
  `appkey` / `appsecret`, store the 24 h `access_token`, refresh on
  expiry. Test with the paper-trading endpoint
  (`openapivts.koreainvestment.com:31000`) before pointing at live.
- Implement `KisClient.connect()` using the `websockets` library
  (already in requirements.txt-adjacent). Send the approval
  handshake; subscribe to `H0STCNT0` (체결) for the seeded
  12-symbol set.
- Implement the KIS frame parser as documented in the stub:
  pipe-delimited `0|TR|len|CSV` → normalize CSV positions per
  TR-id → emit `Tick` / `Quote` matching the existing domain types.

### Sprint 5d — Lifespan cutover + dual-write safety

- `main.py` lifespan: when `KIS_APP_KEY` and `KIS_APP_SECRET` are
  both populated AND `KIS_ENV` ∈ {`paper`, `live`}, spawn the real
  `KisClient` task and **do not** spawn `MockPublisher`. The
  existing `mock_publisher_armed` log line becomes
  `kis_client_armed` with the env tag.
- For the first 24 hours of paper-trading, run BOTH publishers but
  on different Redis channels (`nexus.market.tick` for KIS,
  `nexus.market.tick.mock` for mock). The frontend stays on the
  KIS channel; the mock channel is for backstop testing only. Drop
  the mock channel after the first clean 24 h.
- Failure handling: on KIS reconnect-failure, the existing
  `connectionState: failed` propagates through Redis → WebSocket →
  TopBar amber pill. **No automatic fallback to MockPublisher** —
  silently degrading to fake data in production would be a
  catastrophic UX bug.

### Verification gates

- Paper-trading session **must** persist 1 hour without a single
  reconnect → recorded in `connection_event` audit table.
- Frame parser unit tests using real KIS frame fixtures (recorded
  during paper session).
- `/v1/readyz` extends `MigrationStatusDTO` with a peer
  `MarketLinkStatusDTO`: `{ provider: "KIS", env: "paper",
  state: "connected", last_tick_age_s: 0.4 }`.

### Risk matrix — Goal 2

| Risk | Mitigation |
|---|---|
| KIS keys leaked via logs | `auth_failed` log lines never print the token; pre-commit `detect-private-key` blocks accidental commits. |
| Frontend split-brain across two publishers during cutover | 24 h dual-channel period explicitly documented; only one channel is the production read. |
| KIS rate limits trip during reconnect storms | Exponential backoff with full jitter (already in `HanTooStreamer.ts` design — port to backend). |
| Paper-vs-live confusion | `KIS_ENV` is loud in every log line + `/v1/readyz` body; TopBar shows env explicitly. |

---

## 3. Goal 3 — Multi-agent coordinator + safety guardrails

### Sprint 5e — Coordinator (read-only)

- `src/domain/agents/coordinator.py` — orchestrates the agent
  ensemble. Inputs: latest signals from `agent_signal`, latest
  market state from `market_tick_1m`, latest macro indicators.
  Output: a single `Decision` with `(action, confidence, reasoning,
  contributing_signals[])`.
- Action enum: `HOLD | BUY_SUGGEST | SELL_SUGGEST | NO_TRADE`.
  Note **`SUGGEST`** — at this stage, the coordinator emits
  recommendations only; no order is placed.
- Decisions persisted to `decision_log` (audit trail) and emitted
  on a new Redis channel `nexus.agents.decision` for the frontend
  to render in a future Decision HUD.

### Sprint 5f — Hard-limit safety guardrails

The two-step phrasing is intentional: **safety guardrails ship
BEFORE actual order placement, not after.** The guardrails enforce
limits the LLM coordinator cannot bypass:

| Guardrail | Hard limit | Source of truth |
|---|---|---|
| Max position per symbol | configurable (default ₩50M) | `core/config.py` |
| Max concurrent positions | configurable (default 5) | persisted in `position_state` table |
| Max daily new-position count | configurable (default 10) | rolling count in DB |
| Trading hours window | KRX session only (09:00–15:30 KST) | hardcoded enum + holiday calendar |
| Single-symbol concentration | < 30% of portfolio NAV | computed from `position_state` |
| Anomaly-flagged symbol veto | symbol with `anomaly > 0.85` blocked | live read from canvas state |

The coordinator's `Decision` is **filtered** through `Guardrail`
before any execution path can pick it up. A blocked decision
emits a structured log (`event: guardrail_blocked`) and is
recorded in `decision_log` with the blocking rule — every veto is
auditable.

### Sprint 5g — Order placement bridge

- Only after 5e + 5f land. New module `src/infrastructure/kis_order.py`
  wraps KIS's order-placement REST endpoint. Strict rate limiting,
  idempotency keys, full request/response logging.
- Paper-only for the first 30 days regardless of `KIS_ENV`. A
  separate explicit env flag (`ALLOW_LIVE_ORDERS=true`) is required
  to flip to live; the flag is **not** in `.env.example` and must be
  set by hand on the production cluster.
- Frontend gets a "Live trading enabled" badge that's only visible
  when the flag is set. The badge color is the lime that's been
  reserved for market anomaly until now — transferred here because
  the visual semantics align (lime = "this is real money flowing").

### Verification gates — Goal 3

- 100% of guardrail rules have unit tests with explicit pass / fail
  cases.
- Integration test: a synthetic LLM that always returns
  `BUY_SUGGEST` with extreme size — every order MUST be blocked by
  one or more guardrails.
- Paper-trading 30-day soak with > 1000 decisions logged before
  `ALLOW_LIVE_ORDERS` is even considered.

---

## 4. Cross-cutting Phase 5 deliverables

These are background investments running in parallel with the three
goals above:

### 4.1. Observability

- `/metrics` endpoint with Prometheus exposition (request counts,
  latency histogram, agent token spend, KIS frames/sec, active
  WebSocket sessions, guardrail block rate).
- Grafana dashboard JSON committed to
  `deploy/grafana/nexus-os.json` so the deploy is reproducible.

### 4.2. Compliance / audit

- The existing `request_id` is the correlation primary key.
- New `audit_log` table aggregates: every request_id, the principal
  (oid), the endpoint, every guardrail decision, every order
  placement. Retention 365 days minimum (Korean financial regulation).

### 4.3. Frontend

- Decision HUD panel in the right column showing the latest
  `Decision` with contributing signals, blocked-by-guardrail badges
  if applicable, and a one-click "explain" affordance that calls a
  `/v1/agents/explain/{decision_id}` endpoint for the reasoning.
- New source option `BACKEND·LIVE·KIS` in the harness panel —
  separate from `BACKEND·LIVE` (mock) so an operator can flip
  between them in dev to validate the cutover.

### 4.4. KIS sandbox infrastructure (already prepared)

The existing scaffold is consumed in Phase 5 with **zero changes**:

- `KisConnectionState` enum — used by `KisClient`.
- `kis_client.py` stub — implementation lands in 5c.
- `KIS_APP_KEY` / `KIS_APP_SECRET` / `KIS_ACCOUNT_NUMBER` /
  `KIS_ENV` env slots — already in `.env.example` and the k8s
  Secret template.
- 12 seeded KRX tickers — used by the live KIS client too (paper
  trading first, but tickers are real).

---

## 5. Phase 5 go/no-go gate matrix

| Gate | Pass condition | Reviewer |
|---|---|---|
| **G5.1** Phase 5 kickoff | KIS paper credentials issued, `KIS_APP_KEY` populated in `.env` | architect |
| **G5.2** Quant agent online (after 5a) | 24 h of `agent_signal` rows, 0 schema-violation rejects, token spend < budget | architect |
| **G5.3** Live KIS cutover (after 5c-5d) | 1 h paper session with 0 reconnects + frame parser fixtures pass | architect |
| **G5.4** Macro agent online (after 5b) | Same as G5.2 | architect |
| **G5.5** Coordinator online (after 5e) | 100 decisions logged, all manually reviewed | architect + ops |
| **G5.6** Guardrails verified (after 5f) | 100 % unit-test coverage of guardrails + adversarial agent test passes | architect + ops |
| **G5.7** Live orders enabled (after 5g) | 30-day paper soak + explicit `ALLOW_LIVE_ORDERS=true` toggle + sign-off | architect + ops + business |

Until G5.7 fires, **no real-money order leaves the cluster**. Paper
trading is the default and the only mode reachable by configuration
alone.

---

## 6. Estimated cadence

These are nominal cadence targets; a single integration bug can
absorb a full sprint. No business decision rides on these dates:

| Sprint | Topic | Nominal target |
|---|---|---|
| 5a | Quant Agent vertical slice | T+1 week |
| 5b | Macro Agent | T+2 weeks |
| 5c | KIS REST OAuth + WS | T+3 weeks |
| 5d | Lifespan cutover + dual-write | T+4 weeks |
| 5e | Coordinator | T+5 weeks |
| 5f | Guardrails | T+6 weeks |
| 5g | Order bridge (paper-only) | T+7 weeks |
| 5g* | Live trading toggle | **T+11 weeks at earliest** (after 30-day paper soak) |

Phase 5 close: a `phase5_retrospective.md` companion to this
document, written the moment the live-trading toggle is ready.

---

## 7. Open questions to resolve before Sprint 5a

1. LLM provider lock-in — start with OpenAI `gpt-4o-mini`, or go
   straight to a local model via Ollama (mentioned in the analysis
   module's TODO)? Cost vs. latency vs. data residency trade-off.
2. KIS paper-vs-live cutover criterion — purely 30-day soak, or
   also a manual canary of N small live orders first?
3. Audit retention — 365 days is a regulatory floor; do we store
   in the same TimescaleDB hypertable or a separate cold-storage
   tier?
4. n8n integration mentioned in `analysis/anomaly.py` — which Phase
   5 sprint owns it? My suggestion: 5b (Macro agent) since macro
   is where workflow orchestration is most useful.

These decisions can defer to the start of each relevant sprint, but
should be answered before the sprint begins, not during it.
