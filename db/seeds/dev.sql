-- ─────────────────────────────────────────────────────────────────────────
--  NEXUS OS — Dev Seed Data (NOT for production)
--
--  Apply with:  python -m db.seed              (after migrations are up)
--           or: docker exec -i nexus_timescaledb psql -U nexus_admin
--                            -d nexus_os < db/seeds/dev.sql
--
--  Why these specific tickers: KRX 6-digit IDs match the KIS OpenAPI
--  payload format exactly, so when Sprint 4c lands the live KIS adapter
--  these same rows get UPDATEd in place — no schema change, no relabeling.
--
--  Idempotent: every row uses ON CONFLICT DO NOTHING so re-runs are safe.
-- ─────────────────────────────────────────────────────────────────────────

-- ── Entities — 12 KRX-listed companies grouped into 4 sector clusters ──
-- Anomaly values are seeded across a realistic distribution: most sit in
-- the [0.05, 0.30] noise band, two are deliberately elevated (~0.7) so
-- the canvas's amber halo shows up immediately on the dev /v1/snapshot.

INSERT INTO entity (id, cluster, anomaly, tx_vol) VALUES
    -- Tech / semis / platforms
    ('005930', 'TECH',          0.12,   8_412_000_000),  -- Samsung Electronics
    ('000660', 'TECH',          0.18,   3_220_000_000),  -- SK Hynix
    ('035420', 'TECH',          0.71,   1_180_000_000),  -- NAVER  (elevated)
    ('035720', 'TECH',          0.22,     842_000_000),  -- Kakao

    -- Financial holdings
    ('105560', 'FINANCE',       0.08,   1_540_000_000),  -- KB Financial
    ('055550', 'FINANCE',       0.11,   1_180_000_000),  -- Shinhan
    ('086790', 'FINANCE',       0.09,     720_000_000),  -- Hana Financial

    -- Heavy manufacturing / materials
    ('005380', 'MANUFACTURING', 0.27,   1_910_000_000),  -- Hyundai Motor
    ('005490', 'MANUFACTURING', 0.31,   1_240_000_000),  -- POSCO Holdings
    ('051910', 'MANUFACTURING', 0.68,   2_080_000_000),  -- LG Chem  (elevated)

    -- Biopharma
    ('207940', 'BIO',           0.14,     980_000_000),  -- Samsung Biologics
    ('068270', 'BIO',           0.19,     620_000_000)   -- Celltrion
ON CONFLICT (id) DO NOTHING;


-- ── Edges — directional correlations the canvas force-directed layout
--    will use to cluster the four sector groups visually.
--
--    Patterns modeled here:
--      • Strong intra-sector co-movement (same cluster).
--      • Selected cross-sector links (Samsung Electronics ↔ Hyundai
--        Motor: same chaebol structure; bio peers; etc.).
--    Weights are 0.4–0.95 reflecting the loose-to-tight correlation band.

INSERT INTO edge (from_id, to_id, weight) VALUES
    -- TECH cluster (memory pair, platform pair, plus cross-link)
    ('005930', '000660', 0.92),
    ('000660', '005930', 0.92),
    ('035420', '035720', 0.78),
    ('035720', '035420', 0.78),
    ('005930', '035420', 0.45),
    ('000660', '035720', 0.42),

    -- FINANCE cluster (3-bank triangle, fully connected — KOSPI banks
    -- move on the same rate-cycle factor)
    ('105560', '055550', 0.86),
    ('055550', '086790', 0.84),
    ('086790', '105560', 0.81),

    -- MANUFACTURING cluster (commodity-exposure pair + auto link)
    ('005490', '051910', 0.76),
    ('051910', '005490', 0.76),
    ('005380', '005490', 0.58),

    -- BIO cluster
    ('207940', '068270', 0.71),
    ('068270', '207940', 0.71),

    -- Cross-cluster: Samsung group structural link
    ('005930', '005380', 0.40),

    -- Cross-cluster: chemicals ↔ semis (substrate / wafer chemical supply)
    ('051910', '000660', 0.52),

    -- Cross-cluster: financial exposure to industrials
    ('105560', '005380', 0.36),
    ('086790', '005490', 0.34)
ON CONFLICT (from_id, to_id) DO NOTHING;


-- ── Verification — print row counts so the operator sees the seed took.
DO $$
DECLARE
    ent_count   INT;
    edge_count  INT;
BEGIN
    SELECT COUNT(*) INTO ent_count  FROM entity;
    SELECT COUNT(*) INTO edge_count FROM edge;
    RAISE NOTICE 'NEXUS OS dev seed applied: % entities, % edges', ent_count, edge_count;
END
$$;
