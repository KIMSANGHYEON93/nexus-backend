"""Integration tests for `/v1/securities*` — Sprint 5s.

Pattern follows `test_alarms_api.py` and `test_api_integration.py`:
  • Build a minimal FastAPI app with the v1 router + exception handlers.
  • Skip-on-import when full backend deps aren't present (Windows dev box
    without docker — the suite passes once the container is built).
  • Drive via `TestClient`; the asyncpg `Pool` is mocked at the
    `src.infrastructure.database._pool` module attribute so the router's
    `get_pool()` returns a `MagicMock` whose `fetch` / `fetchrow` answer
    based on the SQL prefix.

Spec §5 backend AC mapped 1:1 to test names where possible:
  • `/v1/securities` 200 + envelope shape.
  • `market` / `sector` / `search` filters.
  • `/v1/securities/{ticker}` 200 vs 404 ProblemDetail (`urn:nexus:errors:security-not-found`).
  • `/v1/securities/relations` filtered by `kind` / `min_weight`.
  • `/v1/snapshot` carries `display_name` / `ticker` / `sector` on enriched rows.
  • `/v1/alarms` carries `entity_display` when entity_id matches a master row.
  • Invalid enum → 400 ProblemDetail (`urn:nexus:errors:invalid-input`).
  • snake_case on the wire (no aliases).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("fastapi", reason="full backend deps required")
pytest.importorskip("pydantic_settings", reason="full backend deps required")
pytest.importorskip("asyncpg", reason="full backend deps required")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.v1.router import (
    reset_alarm_repo_for_tests,
    router as v1_router,
)
from src.core.exception_handlers import install as install_exception_handlers
from src.core.middleware import RequestIdMiddleware
from src.domain.alarms.models import Alarm, Severity, Status
from src.infrastructure.alarms.in_memory_repo import InMemoryAlarmRepository


# ── Test fixtures ─────────────────────────────────────────────────────────

_NOW = datetime(2026, 5, 13, 12, 0, 0, tzinfo=timezone.utc)


def _security_row(
    *,
    ticker: str,
    name_ko: str | None = None,
    name_en: str | None = None,
    aliases: list[str] | None = None,
    market: str = "KRX",
    sector: str = "SEMI",
    sector_label: str = "반도체",
    currency: str = "KRW",
    shares_outstanding: int | None = None,
    market_cap: float | None = None,
    is_subscribed: bool = True,
    data_source: str = "static_master",
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    """One `security_master` row as asyncpg would hand it back."""
    return {
        "ticker":             ticker,
        "name_ko":            name_ko,
        "name_en":            name_en,
        "aliases":            aliases or [],
        "market":             market,
        "sector":             sector,
        "sector_label":       sector_label,
        "currency":           currency,
        "shares_outstanding": shares_outstanding,
        "market_cap":         market_cap,
        "is_subscribed":      is_subscribed,
        "data_source":        data_source,
        "updated_at":         updated_at or _NOW,
    }


def _relation_row(
    *,
    from_ticker: str,
    to_ticker: str,
    kind: str = "sector",
    weight: float = 0.9,
    directed: bool = False,
    evidence: str | None = None,
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    """One `security_relation` row as asyncpg would hand it back."""
    return {
        "from_ticker": from_ticker,
        "to_ticker":   to_ticker,
        "kind":        kind,
        "weight":      weight,
        "directed":    directed,
        "evidence":    evidence,
        "updated_at":  updated_at or _NOW,
    }


_SEEDED_SECURITIES = [
    _security_row(
        ticker="005930",
        name_ko="삼성전자",
        name_en="Samsung Electronics",
        aliases=["삼성", "Samsung", "SEC"],
        market="KRX",
        sector="SEMI",
        sector_label="반도체",
        currency="KRW",
        shares_outstanding=5969782550,
        market_cap=425000000000000.0,
        is_subscribed=True,
    ),
    _security_row(
        ticker="000660",
        name_ko="SK하이닉스",
        name_en="SK Hynix",
        aliases=["하이닉스", "Hynix"],
        market="KRX",
        sector="SEMI",
        sector_label="반도체",
        currency="KRW",
        shares_outstanding=728002365,
        market_cap=165000000000000.0,
        is_subscribed=True,
    ),
    _security_row(
        ticker="AAPL",
        name_ko="애플",
        name_en="Apple Inc.",
        aliases=["Apple", "애플"],
        market="NASDAQ",
        sector="TECH_US",
        sector_label="Big Tech",
        currency="USD",
        shares_outstanding=15204140000,
        market_cap=3400000000000.0,
        is_subscribed=False,
    ),
]

_SEEDED_RELATIONS = [
    _relation_row(from_ticker="005930", to_ticker="SECTOR:SEMI",
                  kind="sector", weight=0.95, evidence="KRX sector"),
    _relation_row(from_ticker="000660", to_ticker="SECTOR:SEMI",
                  kind="sector", weight=0.92, evidence="KRX sector"),
    _relation_row(from_ticker="AAPL", to_ticker="SECTOR:TECH_US",
                  kind="sector", weight=0.90, evidence="GICS"),
    _relation_row(from_ticker="NVDA", to_ticker="TSM",
                  kind="supply_chain", weight=0.92, directed=True,
                  evidence="TSMC fab"),
]


def _build_securities_pool(
    securities: list[dict[str, Any]] | None = None,
    relations:  list[dict[str, Any]] | None = None,
    entities:   list[dict[str, Any]] | None = None,
    edges:      list[dict[str, Any]] | None = None,
) -> MagicMock:
    """asyncpg.Pool mock that answers securities + market queries.

    Routes by SQL substring — the repository builds parameterised SELECTs
    against `security_master` / `security_relation` so we can dispatch on
    table name in the FROM clause. `entity` / `edge` reads fall through
    to the lists for snapshot tests.
    """
    secs = securities if securities is not None else _SEEDED_SECURITIES
    rels = relations  if relations  is not None else _SEEDED_RELATIONS
    ents = entities   if entities   is not None else []
    eds  = edges      if edges      is not None else []

    async def _fetch(sql: str, *args: Any) -> list[dict[str, Any]]:
        if "FROM security_master" in sql:
            return _filter_securities(secs, sql, args)
        if "FROM security_relation" in sql:
            return _filter_relations(rels, sql, args)
        if "FROM entity" in sql:
            return ents
        if "FROM edge" in sql:
            return eds
        return []

    async def _fetchrow(sql: str, *args: Any) -> dict[str, Any] | None:
        if "FROM security_master" in sql and "COUNT" not in sql:
            # Single-ticker fetch: WHERE ticker = $1
            target = args[0]
            for s in secs:
                if s["ticker"] == target:
                    return s
            return None
        if "FROM security_master" in sql and "COUNT" in sql:
            return {"n": len(_filter_securities(secs, sql.replace("COUNT(*)::BIGINT AS n", "*"), args))}
        if "FROM security_relation" in sql and "COUNT" in sql:
            return {"n": len(_filter_relations(rels, sql, args))}
        return None

    conn = MagicMock()
    conn.execute  = AsyncMock(return_value="SELECT 1")
    conn.fetchval = AsyncMock(return_value=3)
    conn.fetch    = AsyncMock(side_effect=_fetch)
    conn.fetchrow = AsyncMock(side_effect=_fetchrow)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__  = AsyncMock(return_value=None)

    pool = MagicMock()
    pool.acquire  = MagicMock(return_value=cm)
    pool.fetch    = AsyncMock(side_effect=_fetch)
    pool.fetchrow = AsyncMock(side_effect=_fetchrow)
    pool.execute  = AsyncMock(return_value=None)
    return pool


def _filter_securities(
    rows: list[dict[str, Any]],
    sql: str,
    args: tuple[Any, ...],
) -> list[dict[str, Any]]:
    """Apply WHERE clauses + LIMIT in Python.

    The mock can't ask Postgres to evaluate the parameterised conditions,
    so we infer from substring presence which args belong to which clause.
    Order matches `SecuritiesRepository.list_securities`:
      [markets?, sectors?, search_prefix?, search_substring?, search_aliases?, limit]
    """
    out = list(rows)
    arg_iter = iter(args)
    if "market = ANY(" in sql:
        markets = next(arg_iter)
        out = [r for r in out if r["market"] in markets]
    if "sector = ANY(" in sql:
        sectors = next(arg_iter)
        out = [r for r in out if r["sector"] in sectors]
    if "ticker ILIKE" in sql:
        prefix = next(arg_iter).replace("%", "")
        sub    = next(arg_iter).replace("%", "")
        aliases_list = next(arg_iter)
        out = [
            r for r in out
            if r["ticker"].lower().startswith(prefix.lower())
            or (r["name_ko"] and sub.lower() in r["name_ko"].lower())
            or (r["name_en"] and sub.lower() in r["name_en"].lower())
            or any(a in r["aliases"] for a in aliases_list)
        ]
    # Last arg = LIMIT (for list SQL) — count SQL has no LIMIT, so guard.
    if "LIMIT $" in sql:
        try:
            limit = next(arg_iter)
            out = out[:limit]
        except StopIteration:
            pass
    out.sort(key=lambda r: r["ticker"])
    return out


def _filter_relations(
    rows: list[dict[str, Any]],
    sql: str,
    args: tuple[Any, ...],
) -> list[dict[str, Any]]:
    """Apply WHERE clauses + ORDER BY weight DESC in Python."""
    out = list(rows)
    arg_iter = iter(args)
    # Always first: weight >= $1
    if "weight >=" in sql:
        min_weight = next(arg_iter)
        out = [r for r in out if r["weight"] >= min_weight]
    if "kind = ANY(" in sql:
        kinds = next(arg_iter)
        out = [r for r in out if r["kind"] in kinds]
    if "from_ticker = ANY(" in sql:
        tickers = next(arg_iter)
        out = [
            r for r in out
            if r["from_ticker"] in tickers or r["to_ticker"] in tickers
        ]
    out.sort(key=lambda r: (-r["weight"], r["from_ticker"], r["to_ticker"]))
    return out


def _build_app() -> FastAPI:
    app = FastAPI(title="nexus-backend test (securities)")
    app.add_middleware(RequestIdMiddleware)  # type: ignore[arg-type]
    install_exception_handlers(app)
    app.include_router(v1_router)
    return app


@pytest.fixture
def securities_app(env_minimal, monkeypatch):
    """App with the securities pool mock injected."""
    pool = _build_securities_pool()
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    redis_client = MagicMock()
    redis_client.ping = AsyncMock(return_value=True)
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)
    return _build_app(), pool


# ──────────────────────────────────────────────────────────────────────────
#  /v1/securities — list + filters
# ──────────────────────────────────────────────────────────────────────────


def test_securities_returns_200_with_envelope_shape(securities_app):
    """Valid empty-query request returns SecurityListDTO with items/total/server_time."""
    app, _ = securities_app
    response = TestClient(app).get("/v1/securities")
    assert response.status_code == 200
    body = response.json()
    assert set(body.keys()) >= {"items", "total", "server_time"}
    assert body["total"] == 3
    assert len(body["items"]) == 3
    # Stable ticker-ASC ordering.
    assert [item["ticker"] for item in body["items"]] == ["000660", "005930", "AAPL"]


def test_securities_item_carries_display_name_and_snake_case(securities_app):
    """Each item carries `display_name` (ko-first) and only snake_case keys."""
    app, _ = securities_app
    body = TestClient(app).get("/v1/securities").json()
    samsung = next(i for i in body["items"] if i["ticker"] == "005930")
    assert samsung["display_name"] == "삼성전자"
    assert samsung["name_ko"]      == "삼성전자"
    assert samsung["name_en"]      == "Samsung Electronics"
    assert samsung["sector_label"] == "반도체"
    assert samsung["is_subscribed"] is True
    # snake_case only — no camelCase aliases leaked.
    for key in samsung:
        assert "-" not in key
        assert key == key.lower() or key.isupper()  # all lowercase keys


def test_securities_market_filter_returns_only_matching(securities_app):
    """`?market=KRX` returns KRX rows only."""
    app, _ = securities_app
    body = TestClient(app).get("/v1/securities?market=KRX").json()
    assert {i["market"] for i in body["items"]} == {"KRX"}
    assert {i["ticker"] for i in body["items"]} == {"005930", "000660"}


def test_securities_invalid_market_returns_400_problem_detail(securities_app):
    """Unknown enum value → 400 + invalid-input ProblemDetail."""
    app, _ = securities_app
    response = TestClient(app).get("/v1/securities?market=INVALID")
    assert response.status_code == 400
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert "invalid-input" in body["type"]
    assert "INVALID" in body["detail"]


def test_securities_search_matches_korean_name(securities_app):
    """`?search=삼성` matches via name_ko or alias."""
    app, _ = securities_app
    body = TestClient(app).get("/v1/securities?search=삼성").json()
    tickers = {i["ticker"] for i in body["items"]}
    assert "005930" in tickers


def test_securities_search_matches_english_name(securities_app):
    """`?search=samsung` matches via name_en (case-insensitive)."""
    app, _ = securities_app
    body = TestClient(app).get("/v1/securities?search=samsung").json()
    tickers = {i["ticker"] for i in body["items"]}
    assert "005930" in tickers


def test_securities_search_matches_alias_token(securities_app):
    """`?search=Hynix` matches via aliases."""
    app, _ = securities_app
    body = TestClient(app).get("/v1/securities?search=Hynix").json()
    tickers = {i["ticker"] for i in body["items"]}
    assert "000660" in tickers


# ──────────────────────────────────────────────────────────────────────────
#  /v1/securities/{ticker} — single getter + 404
# ──────────────────────────────────────────────────────────────────────────


def test_securities_detail_returns_200_for_known_ticker(securities_app):
    """Known ticker returns SecurityDTO directly (no envelope)."""
    app, _ = securities_app
    response = TestClient(app).get("/v1/securities/005930")
    assert response.status_code == 200
    body = response.json()
    assert body["ticker"]       == "005930"
    assert body["display_name"] == "삼성전자"
    assert body["market"]       == "KRX"


def test_securities_detail_returns_404_problem_detail_for_unknown(securities_app):
    """Unknown ticker → 404 with `security-not-found` problem-type URI."""
    app, _ = securities_app
    response = TestClient(app).get("/v1/securities/INVALID")
    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["type"]   == "urn:nexus:errors:security-not-found"
    assert body["title"]  == "Security not found"
    assert "INVALID" in body["detail"]


# ──────────────────────────────────────────────────────────────────────────
#  /v1/securities/relations — filtered edge fetch
# ──────────────────────────────────────────────────────────────────────────


def test_relations_default_returns_all_edges(securities_app):
    """No filters → all 4 seeded edges, weight-DESC ordered."""
    app, _ = securities_app
    response = TestClient(app).get("/v1/securities/relations")
    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, list)
    assert len(body) == 4
    assert body[0]["weight"] >= body[-1]["weight"]  # weight DESC


def test_relations_kind_filter(securities_app):
    """`?kind=sector` returns only sector edges."""
    app, _ = securities_app
    body = TestClient(app).get("/v1/securities/relations?kind=sector").json()
    assert {e["kind"] for e in body} == {"sector"}


def test_relations_min_weight_filter(securities_app):
    """`?min_weight=0.93` drops the 0.90 / 0.92 edges, keeps 0.95."""
    app, _ = securities_app
    body = TestClient(app).get("/v1/securities/relations?min_weight=0.93").json()
    assert all(e["weight"] >= 0.93 for e in body)
    assert len(body) == 1
    assert body[0]["from_ticker"] == "005930"


def test_relations_route_does_not_collide_with_ticker_route(securities_app):
    """Spec §2: `/relations` MUST be registered before `/{ticker}` so the
    word "relations" is never interpreted as a ticker. A request to
    `/v1/securities/relations` returns a list, not a 404."""
    app, _ = securities_app
    response = TestClient(app).get("/v1/securities/relations")
    assert response.status_code == 200
    # If the {ticker} route had won, we'd get 404 with the security-not-found URI.
    assert isinstance(response.json(), list)


def test_relations_invalid_kind_returns_400_problem_detail(securities_app):
    """Unknown kind → 400 + invalid-input ProblemDetail."""
    app, _ = securities_app
    response = TestClient(app).get("/v1/securities/relations?kind=bogus")
    assert response.status_code == 400
    body = response.json()
    assert "invalid-input" in body["type"]


# ──────────────────────────────────────────────────────────────────────────
#  /v1/snapshot enrichment — display_name / ticker / sector
# ──────────────────────────────────────────────────────────────────────────


def test_snapshot_enriches_entity_with_display_name(env_minimal, monkeypatch):
    """When entity.id matches a security_master.ticker, EntityDTO carries
    display_name / ticker / sector populated from the master row."""
    pool = _build_securities_pool(
        entities=[
            {"id": "005930", "cluster": "TECH",   "anomaly": 0.12, "tx_vol": 8_412_000_000.0},
            {"id": "HUB",    "cluster": "TECH",   "anomaly": 0.05, "tx_vol": 0.0},
        ],
        edges=[],
    )
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    redis_client = MagicMock(); redis_client.ping = AsyncMock(return_value=True)
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)

    body = TestClient(_build_app()).get("/v1/snapshot").json()
    by_id = {e["id"]: e for e in body["entities"]}
    assert by_id["005930"]["display_name"] == "삼성전자"
    assert by_id["005930"]["ticker"]       == "005930"
    assert by_id["005930"]["sector"]       == "SEMI"
    # Non-security ontology node → enrichment fields null.
    assert by_id["HUB"]["display_name"] is None
    assert by_id["HUB"]["ticker"]       is None
    assert by_id["HUB"]["sector"]       is None


# ──────────────────────────────────────────────────────────────────────────
#  /v1/alarms enrichment — entity_display
# ──────────────────────────────────────────────────────────────────────────


def test_alarms_enriches_entity_display_when_entity_is_ticker(env_minimal, monkeypatch):
    """Alarm with entity_id matching a ticker carries entity_display; alarm
    with non-ticker entity_id (or no entity_id) carries entity_display=null."""
    pool = _build_securities_pool()
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    redis_client = MagicMock(); redis_client.ping = AsyncMock(return_value=True)
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)

    # occurred_at must fall inside the default 24h lookup window; using
    # `datetime.now(UTC)` keeps the test independent of the wall clock.
    occurred_at = datetime.now(timezone.utc)
    alarm_with_ticker = Alarm(
        id="alarm-1",
        severity=Severity.ANOMALY,
        status=Status.ACTIVE,
        source="trading-coordinator",
        code="ANOMALY",
        title="ANOMALY ALERT",
        message="Anomaly score elevated",
        occurred_at=occurred_at,
        entity_id="005930",
    )
    alarm_without_match = Alarm(
        id="alarm-2",
        severity=Severity.INFO,
        status=Status.ACTIVE,
        source="trading-coordinator",
        code="INFO",
        title="INFO",
        message="ontology hub event",
        occurred_at=occurred_at,
        entity_id="HUB",
    )
    repo = InMemoryAlarmRepository([alarm_with_ticker, alarm_without_match])
    reset_alarm_repo_for_tests(repo)
    try:
        body = TestClient(_build_app()).get("/v1/alarms").json()
    finally:
        reset_alarm_repo_for_tests(None)

    by_id = {a["id"]: a for a in body["items"]}
    assert by_id["alarm-1"]["entity_display"] == "삼성전자"
    assert by_id["alarm-1"]["entity_id"]      == "005930"
    assert by_id["alarm-2"]["entity_display"] is None
    assert by_id["alarm-2"]["entity_id"]      == "HUB"


# ──────────────────────────────────────────────────────────────────────────
#  Snake-case + alias absence regression
# ──────────────────────────────────────────────────────────────────────────


def test_securities_dto_has_no_camel_case_aliases(securities_app):
    """Spec mandates snake_case ONLY — no Pydantic alias serialization."""
    app, _ = securities_app
    body = TestClient(app).get("/v1/securities").json()
    item = body["items"][0]
    forbidden = {"displayName", "nameKo", "nameEn", "sectorLabel",
                 "sharesOutstanding", "marketCap", "lastPrice",
                 "changePct", "txVol", "isSubscribed", "dataSource", "updatedAt"}
    assert not (set(item.keys()) & forbidden), (
        f"camelCase keys leaked into response: {set(item.keys()) & forbidden}"
    )
