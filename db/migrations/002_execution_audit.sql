-- ─────────────────────────────────────────────────────────────────────────
--  NEXUS OS — Migration 002: Trading execution audit ledger (Sprint 5m)
--  Apply with:  python -m db.migrate            (from project root)
--
--  Captures every ExecutionResult emitted by the OrderExecutor — live,
--  shadow, and noop alike — alongside the TradeSignal that produced it
--  (action, confidence, score, contributors). One row per pipeline
--  decision, append-only, time-partitioned via TimescaleDB hypertable
--  so range scans for "what did we trade between 09:00 and 09:30?" are
--  cheap.
--
--  Idempotent: every CREATE / INSERT uses IF NOT EXISTS / ON CONFLICT.
-- ─────────────────────────────────────────────────────────────────────────

-- ── Execution audit ─────────────────────────────────────────────────────
-- Schema rationale:
--   `mode`            — live / shadow / noop. Lets ops query "what would we
--                       have traded had ALLOW_LIVE_ORDERS been on" by
--                       filtering for mode='shadow' AND executed=FALSE.
--   `executed`        — TRUE only for `mode=live` AND broker accepted.
--                       Distinct from a successful shadow log.
--   `intended_*`      — what the executor TRIED to do (may differ from
--                       what hit the wire if rejected).
--   `signal_score`    — coordinator's signed score in [-1, +1]; needed
--                       for backtest replay even when action is HOLD.
--   `signal_rationale` — JSONB so we can store the full contributors[]
--                       array from the coordinator without flattening
--                       to one row per agent.
--   `blocked_by`      — guard_id that converted intent to HOLD (5f).
--   `reason`          — free-form string from executor / sizer / guard.
CREATE TABLE IF NOT EXISTS execution_audit (
    ts                  TIMESTAMPTZ      NOT NULL,
    symbol              TEXT             NOT NULL,
    mode                TEXT             NOT NULL CHECK (mode IN ('live', 'shadow', 'noop')),
    executed            BOOLEAN          NOT NULL,
    intended_action     TEXT             NOT NULL CHECK (intended_action IN ('buy', 'hold', 'sell')),
    intended_quantity   INTEGER          NOT NULL,
    order_id            TEXT,
    blocked_by          TEXT,
    reason              TEXT,
    signal_action       TEXT             NOT NULL CHECK (signal_action IN ('buy', 'hold', 'sell')),
    signal_confidence   DOUBLE PRECISION NOT NULL CHECK (signal_confidence BETWEEN 0.0 AND 1.0),
    signal_score        DOUBLE PRECISION NOT NULL CHECK (signal_score BETWEEN -1.0 AND 1.0),
    signal_rationale    JSONB
);

-- Time-partitioned for fast ranged queries over the audit log. 1-day
-- chunks match `market_tick` so backtest joins stay on one chunk.
SELECT create_hypertable(
    'execution_audit', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_execution_audit_symbol_ts
    ON execution_audit (symbol, ts DESC);

-- Frequent dashboard query: "show me all live fills today, newest first."
CREATE INDEX IF NOT EXISTS idx_execution_audit_mode_ts
    ON execution_audit (mode, ts DESC);


-- ── Schema bookkeeping ──────────────────────────────────────────────────
INSERT INTO schema_version (version, note)
VALUES (2, 'execution_audit hypertable + indexes (Sprint 5m)')
ON CONFLICT (version) DO NOTHING;
