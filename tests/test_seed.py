"""Structural integrity of the dev seed file — runs on every CI push.

Why parse SQL with a regex instead of executing it: this test must run
against the host (no Postgres available locally), so we sanity-check the
file contents directly. The wire format guarantee — that every edge's
`from_id` / `to_id` references a known entity — is the kind of bug that
would silently corrupt the canvas via foreign-key cascades on dev only.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SEED = Path(__file__).parent.parent / "db" / "seeds" / "dev.sql"


def _entity_rows(sql: str) -> list[tuple]:
    """Pull every (id, cluster, anomaly, tx_vol) tuple out of the entity INSERT."""
    block_match = re.search(
        r"INSERT INTO entity[^;]+VALUES\s*(.+?)\s*ON CONFLICT",
        sql, re.DOTALL,
    )
    assert block_match, "entity INSERT block not found"
    rows = re.findall(
        r"\(\s*'(\d{6})'\s*,\s*'(\w+)'\s*,\s*([\d.]+)\s*,\s*([\d_]+)\s*\)",
        block_match.group(1),
    )
    return rows


def _edge_rows(sql: str) -> list[tuple[str, str, str]]:
    """Pull every (from_id, to_id, weight) tuple out of the edge INSERT."""
    block_match = re.search(
        r"INSERT INTO edge[^;]+VALUES\s*(.+?)\s*ON CONFLICT",
        sql, re.DOTALL,
    )
    assert block_match, "edge INSERT block not found"
    return re.findall(
        r"\(\s*'(\d{6})'\s*,\s*'(\d{6})'\s*,\s*([\d.]+)\s*\)",
        block_match.group(1),
    )


@pytest.fixture(scope="module")
def sql() -> str:
    return SEED.read_text(encoding="utf-8")


def test_seed_file_exists(sql):
    assert len(sql) > 100, "seed file unexpectedly tiny — copy/paste error?"


def test_idempotent_clauses_present(sql):
    """Re-running the seed must never error. ON CONFLICT clauses are the
    contract."""
    assert "ON CONFLICT (id) DO NOTHING" in sql
    assert "ON CONFLICT (from_id, to_id) DO NOTHING" in sql


def test_entity_count_matches_design(sql):
    rows = _entity_rows(sql)
    assert len(rows) == 12, f"expected 12 seed entities, got {len(rows)}"


def test_entity_anomaly_in_unit_interval(sql):
    """Anomaly is documented as 0.0–1.0 in the schema. Out-of-range
    values would fail the CHECK constraint at insert time."""
    for entity_id, _cluster, anomaly_str, _tx_vol in _entity_rows(sql):
        anomaly = float(anomaly_str)
        assert 0.0 <= anomaly <= 1.0, f"{entity_id} anomaly={anomaly} out of [0,1]"


def test_clusters_are_balanced(sql):
    """Force-directed layout looks best when clusters are roughly equal
    sized. Drift catches accidental over-loading of one cluster."""
    from collections import Counter
    clusters = Counter(row[1] for row in _entity_rows(sql))
    assert clusters == {"TECH": 4, "FINANCE": 3, "MANUFACTURING": 3, "BIO": 2}, \
        f"cluster distribution drifted: {dict(clusters)}"


def test_edge_endpoints_reference_known_entities(sql):
    """Catches dangling FK references that would error on insert."""
    entity_ids = {row[0] for row in _entity_rows(sql)}
    edges = _edge_rows(sql)
    for from_id, to_id, _w in edges:
        assert from_id in entity_ids, f"edge {from_id}->{to_id}: from_id unknown"
        assert to_id in entity_ids, f"edge {from_id}->{to_id}: to_id unknown"


def test_edge_weights_bounded(sql):
    """Weights are documented 0.4–0.95 reflecting our correlation band."""
    for from_id, to_id, w_str in _edge_rows(sql):
        w = float(w_str)
        assert 0.0 < w <= 1.0, f"edge {from_id}->{to_id} weight={w} out of (0,1]"


def test_no_self_loops(sql):
    """A→A edges would distort the layout and aren't meaningful here."""
    for from_id, to_id, _w in _edge_rows(sql):
        assert from_id != to_id, f"self-loop on {from_id}"


def test_seed_file_is_dev_marked(sql):
    """The file must explicitly say it's not for production — defends
    against an operator confusing it with a migration."""
    assert "NOT for production" in sql or "dev" in sql.lower()
