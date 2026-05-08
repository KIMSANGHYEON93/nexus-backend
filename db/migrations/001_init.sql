-- ─────────────────────────────────────────────────────────────────────────
--  NEXUS OS — Migration 001: Initial schema
--  Apply with:  python -m db.migrate            (from project root)
--           or: docker exec -i nexus_timescaledb psql -U nexus_admin
--                            -d nexus_os < db/migrations/001_init.sql
--
--  Idempotent: every CREATE uses IF NOT EXISTS so partial runs recover.
-- ─────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── Ticks (체결) ─────────────────────────────────────────────────────────
-- High-cardinality append-only stream. Hypertable partitioned by time so
-- range scans for the last N seconds touch one chunk, not the whole table.
CREATE TABLE IF NOT EXISTS market_tick (
    ts          TIMESTAMPTZ      NOT NULL,
    symbol      TEXT             NOT NULL,
    price       NUMERIC(18, 4)   NOT NULL,
    volume      BIGINT           NOT NULL,
    side        TEXT             NOT NULL CHECK (side IN ('buy', 'sell')),
    PRIMARY KEY (symbol, ts)
);

SELECT create_hypertable(
    'market_tick', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_market_tick_symbol_ts
    ON market_tick (symbol, ts DESC);

-- ── Quotes (호가) ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_quote (
    ts          TIMESTAMPTZ      NOT NULL,
    symbol      TEXT             NOT NULL,
    bid_price   NUMERIC(18, 4)   NOT NULL,
    bid_size    BIGINT           NOT NULL,
    ask_price   NUMERIC(18, 4)   NOT NULL,
    ask_size    BIGINT           NOT NULL,
    PRIMARY KEY (symbol, ts)
);

SELECT create_hypertable(
    'market_quote', 'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_market_quote_symbol_ts
    ON market_quote (symbol, ts DESC);

-- ── Entities (NEXUS canvas state) ───────────────────────────────────────
-- Low-cardinality, frequently overwritten. Plain table — not a hypertable.
CREATE TABLE IF NOT EXISTS entity (
    id          TEXT             PRIMARY KEY,
    cluster     TEXT             NOT NULL,
    anomaly     DOUBLE PRECISION NOT NULL CHECK (anomaly BETWEEN 0.0 AND 1.0),
    tx_vol      DOUBLE PRECISION NOT NULL,
    updated_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entity_cluster ON entity (cluster);
CREATE INDEX IF NOT EXISTS idx_entity_anomaly ON entity (anomaly DESC);

-- ── Edges (directional graph topology) ──────────────────────────────────
CREATE TABLE IF NOT EXISTS edge (
    from_id     TEXT             NOT NULL REFERENCES entity (id) ON DELETE CASCADE,
    to_id       TEXT             NOT NULL REFERENCES entity (id) ON DELETE CASCADE,
    weight      DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    updated_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (from_id, to_id)
);

CREATE INDEX IF NOT EXISTS idx_edge_from ON edge (from_id);
CREATE INDEX IF NOT EXISTS idx_edge_to   ON edge (to_id);

-- ── Continuous aggregate — 1m OHLC roll-up over ticks ───────────────────
-- Materialized view backed by Timescale's continuous aggregate engine; the
-- /v1/snapshot endpoint can hit this for fast historical chart payloads
-- without running window functions over the raw tick hypertable.
CREATE MATERIALIZED VIEW IF NOT EXISTS market_tick_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', ts) AS bucket,
    symbol,
    first(price, ts) AS open,
    max(price)       AS high,
    min(price)       AS low,
    last(price, ts)  AS close,
    sum(volume)      AS volume
FROM market_tick
GROUP BY bucket, symbol
WITH NO DATA;

-- Keep the aggregate fresh; refresh policy can be tuned for staging vs prod.
SELECT add_continuous_aggregate_policy(
    'market_tick_1m',
    start_offset      => INTERVAL '7 days',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '30 seconds',
    if_not_exists     => TRUE
);

-- ── Schema version table (manual bookkeeping for migrate.py) ────────────
CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER          PRIMARY KEY,
    applied_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    note        TEXT
);

INSERT INTO schema_version (version, note)
VALUES (1, 'initial: tick/quote hypertables, entity/edge tables, 1m cagg')
ON CONFLICT (version) DO NOTHING;
