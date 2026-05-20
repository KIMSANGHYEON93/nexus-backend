-- ─────────────────────────────────────────────────────────────────────────
--  NEXUS OS — Migration 003: Securities master + relations
--  Apply with:  python -m db.migrate            (from project root)
--           or: docker exec -i nexus_timescaledb psql -U nexus_admin
--                            -d nexus_os < db/migrations/003_securities.sql
--
--  Why a separate master table (vs extending `entity`):
--    The existing `entity` table is high-churn (anomaly / tx_vol mutated by
--    the analysis loop on every recompute), while ticker / name / sector /
--    market_cap are static-ish reference data that change at most daily.
--    Keeping them in `security_master` lets the v1 read path JOIN on need
--    instead of carrying ref-data columns in the hot-update row.
--
--  Idempotent: every CREATE uses IF NOT EXISTS; INSERT into schema_version
--  is ON CONFLICT DO NOTHING.
-- ─────────────────────────────────────────────────────────────────────────


-- ── Securities master ───────────────────────────────────────────────────
-- One row per investable security. Joined on `ticker` to:
--   • `entity.id`          (snapshot enrichment — display_name / sector)
--   • `alarm.entity_id`    (alarm enrichment — entity_display)
--   • `market_tick.symbol` (live-tick subscription gating via is_subscribed)
--
-- `aliases TEXT[]` + GIN index powers the §2 fuzzy search rule
-- ("삼성"/"Samsung"/"SEC" all match 005930 in one indexed lookup).
CREATE TABLE IF NOT EXISTS security_master (
    ticker              TEXT             PRIMARY KEY,
    name_ko             TEXT,
    name_en             TEXT,
    aliases             TEXT[]           NOT NULL DEFAULT '{}',
    market              TEXT             NOT NULL
        CHECK (market IN ('KRX','KOSDAQ','NASDAQ','NYSE','OTHER')),
    sector              TEXT             NOT NULL,
    sector_label        TEXT             NOT NULL,
    currency            TEXT             NOT NULL,
    shares_outstanding  BIGINT,
    market_cap          DOUBLE PRECISION,
    is_subscribed       BOOLEAN          NOT NULL DEFAULT FALSE,
    data_source         TEXT             NOT NULL DEFAULT 'static_master',
    updated_at          TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sec_market   ON security_master (market);
CREATE INDEX IF NOT EXISTS idx_sec_sector   ON security_master (sector);
CREATE INDEX IF NOT EXISTS idx_sec_aliases  ON security_master USING GIN (aliases);
CREATE INDEX IF NOT EXISTS idx_sec_is_sub   ON security_master (is_subscribed);


-- ── Securities relations ────────────────────────────────────────────────
-- Multi-kind directional graph. `kind` enumerates the relationship type so
-- a single table can host sector-membership, correlation, chaebol-group,
-- supply-chain, and cross-listing edges without one table per kind. PK
-- spans (from, to, kind) so two tickers can share multiple distinct edges.
--
-- `directed` flags whether the relationship is symmetric (correlation =
-- false → render once, undirected) or asymmetric (supply_chain = true →
-- A supplies B). `weight` is 0..1 so the front-end force sim can use it
-- as a spring constant without rescaling.
CREATE TABLE IF NOT EXISTS security_relation (
    from_ticker  TEXT             NOT NULL,
    to_ticker    TEXT             NOT NULL,
    kind         TEXT             NOT NULL
        CHECK (kind IN ('sector','correlation','same_chaebol','supply_chain','cross_listing')),
    weight       DOUBLE PRECISION NOT NULL
        CHECK (weight BETWEEN 0.0 AND 1.0),
    directed     BOOLEAN          NOT NULL DEFAULT FALSE,
    evidence     TEXT,
    updated_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (from_ticker, to_ticker, kind)
);

CREATE INDEX IF NOT EXISTS idx_rel_from    ON security_relation (from_ticker);
CREATE INDEX IF NOT EXISTS idx_rel_to      ON security_relation (to_ticker);
CREATE INDEX IF NOT EXISTS idx_rel_kind    ON security_relation (kind);
CREATE INDEX IF NOT EXISTS idx_rel_weight  ON security_relation (weight DESC);


-- ── Schema bookkeeping ──────────────────────────────────────────────────
INSERT INTO schema_version (version, note)
VALUES (3, 'security_master + security_relation (Sprint 5s)')
ON CONFLICT (version) DO NOTHING;
