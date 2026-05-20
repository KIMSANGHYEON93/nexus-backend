"""End-to-end integration tests against the FastAPI app.

What's covered here that the per-module unit tests cannot reach:
  • The full middleware stack actually runs against a real ASGI cycle —
    request-id propagation, CORS preflight, exception handler dispatch.
  • Dependency injection wiring from get_pool / get_client through to
    the router functions (the real pool/redis are mocked at module level
    rather than calling init_pool()/init_client(), so we never touch
    Postgres or Redis from a unit test).
  • RFC 7807 response shape on every error path the router can produce.
  • The router's own conditional logic (db ping fail vs schema stale vs
    full green) renders the documented ReadinessDTO shape per cell.

Skips on hosts without the runtime deps installed (Windows dev box);
runs in CI / Docker where requirements.txt has been applied.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

# Skip the whole module if the FastAPI runtime isn't installed.
pytest.importorskip("fastapi", reason="full backend deps required")
pytest.importorskip("pydantic_settings", reason="full backend deps required")
pytest.importorskip("asyncpg", reason="full backend deps required")
pytest.importorskip("redis", reason="full backend deps required")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.v1.router import router as v1_router
from src.core.exception_handlers import install as install_exception_handlers
from src.core.middleware import RequestIdMiddleware
from src.infrastructure.database import EXPECTED_SCHEMA_VERSION


# ──────────────────────────────────────────────────────────────────────────
#  Mock builders — produce objects shaped exactly like the real Pool / Redis
# ──────────────────────────────────────────────────────────────────────────

def _build_mock_pool(
    schema_version: int | None = EXPECTED_SCHEMA_VERSION,
    entities: list[dict[str, Any]] | None = None,
    edges: list[dict[str, Any]] | None = None,
    audit_rows: list[dict[str, Any]] | None = None,
    tick_rows: list[dict[str, Any]] | None = None,
    db_ping_raises: Exception | None = None,
) -> MagicMock:
    """Pool whose acquire().__aenter__() returns a connection. The connection
    answers `SELECT 1`, schema_version queries, entity / edge SELECTs."""
    conn = MagicMock()

    if db_ping_raises is not None:
        conn.execute = AsyncMock(side_effect=db_ping_raises)
    else:
        conn.execute = AsyncMock(return_value="SELECT 1")

    # `fetchval` is used by verify_schema() exclusively in our codebase.
    conn.fetchval = AsyncMock(return_value=schema_version)

    # `fetch` is used by the repository for entities / edges / audit. The
    # repository itself is built per-request from the pool, so we route
    # fetches by SQL prefix to keep tests readable.
    async def _fetch(sql: str, *args: Any) -> list[dict[str, Any]]:
        if "FROM entity" in sql:
            return entities or []
        if "FROM edge" in sql:
            return edges or []
        if "FROM execution_audit" in sql:
            return audit_rows or []
        if "FROM market_tick" in sql:
            return tick_rows or []
        return []
    conn.fetch = AsyncMock(side_effect=_fetch)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=None)

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=cm)
    # Pool itself also exposes fetch / execute as shortcuts in some asyncpg
    # versions; the repository uses pool.fetch / pool.execute directly.
    pool.fetch = AsyncMock(side_effect=_fetch)
    pool.execute = AsyncMock(return_value=None)
    return pool


def _build_mock_redis(ping_raises: Exception | None = None) -> MagicMock:
    client = MagicMock()
    if ping_raises is not None:
        client.ping = AsyncMock(side_effect=ping_raises)
    else:
        client.ping = AsyncMock(return_value=True)
    return client


# ──────────────────────────────────────────────────────────────────────────
#  Fixtures — build a FastAPI app per test with the mocks injected
# ──────────────────────────────────────────────────────────────────────────

def _build_app() -> FastAPI:
    """Build the same app shape as src/main.py's `app`, but without the
    lifespan side effects (init_pool / init_client / mock_publisher).
    Tests inject the pool / redis via monkeypatch directly."""
    app = FastAPI(title="nexus-backend test")
    app.add_middleware(RequestIdMiddleware)  # type: ignore[arg-type]
    install_exception_handlers(app)
    app.include_router(v1_router)
    return app


@pytest.fixture
def app_with_mocks(env_minimal, monkeypatch):
    """Default-healthy app: pool answers SELECT 1, schema_version=1,
    redis ping returns True, entity/edge tables empty."""
    pool = _build_mock_pool()
    redis_client = _build_mock_redis()
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)
    return _build_app(), pool, redis_client


# ──────────────────────────────────────────────────────────────────────────
#  /v1/health
# ──────────────────────────────────────────────────────────────────────────

def test_health_returns_ok_with_publisher_field(app_with_mocks):
    """publisher 필드가 응답에 포함되어야 한다."""
    app, _, _ = app_with_mocks
    mock_supervisor = MagicMock()
    mock_supervisor.active_kind = "mock"
    app.state.supervisor = mock_supervisor

    client = TestClient(app)
    response = client.get("/v1/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "nexus-backend"
    assert body["publisher"] == "mock"


def test_health_publisher_reflects_active_kind(app_with_mocks):
    """active_kind가 'kis'이면 publisher='kis'를 반환해야 한다."""
    app, _, _ = app_with_mocks
    mock_supervisor = MagicMock()
    mock_supervisor.active_kind = "kis"
    app.state.supervisor = mock_supervisor

    client = TestClient(app)
    response = client.get("/v1/health")
    assert response.status_code == 200
    assert response.json()["publisher"] == "kis"


def test_health_does_not_touch_db_or_redis(app_with_mocks):
    """Liveness probe is intentionally cheap. Confirms it stays so."""
    app, pool, redis_client = app_with_mocks
    TestClient(app).get("/v1/health")
    assert pool.acquire.call_count == 0
    assert redis_client.ping.await_count == 0


# ──────────────────────────────────────────────────────────────────────────
#  /v1/readyz — every fail-cell of the documented matrix
# ──────────────────────────────────────────────────────────────────────────

def test_readyz_all_green(app_with_mocks):
    app, _, _ = app_with_mocks
    response = TestClient(app).get("/v1/readyz")
    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["database"] is True
    assert body["redis"] is True
    assert body["migration"]["ok"] is True
    assert body["migration"]["applied"] == EXPECTED_SCHEMA_VERSION


def test_readyz_db_down(env_minimal, monkeypatch):
    pool = _build_mock_pool(db_ping_raises=OSError("connection refused"))
    redis_client = _build_mock_redis()
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)

    response = TestClient(_build_app()).get("/v1/readyz")
    assert response.status_code == 200      # 200 with detailed body, NOT 503
    body = response.json()
    assert body["ok"] is False
    assert body["database"] is False
    assert body["redis"] is True
    # When DB is down we don't probe schema; reason explains.
    assert body["migration"]["ok"] is False
    assert "unreachable" in body["migration"]["reason"]


def test_readyz_redis_down(env_minimal, monkeypatch):
    pool = _build_mock_pool()
    redis_client = _build_mock_redis(ping_raises=OSError("redis down"))
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)

    response = TestClient(_build_app()).get("/v1/readyz")
    body = response.json()
    assert body["ok"] is False
    assert body["database"] is True
    assert body["redis"] is False


def test_readyz_schema_stale(env_minimal, monkeypatch):
    """Container code has bumped EXPECTED_SCHEMA_VERSION but the DB
    is still on an older migration → ok=false, reason guides operator."""
    pool = _build_mock_pool(schema_version=0)   # < EXPECTED_SCHEMA_VERSION
    redis_client = _build_mock_redis()
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)

    body = TestClient(_build_app()).get("/v1/readyz").json()
    assert body["ok"] is False
    assert body["database"] is True
    assert body["redis"] is True
    assert body["migration"]["ok"] is False
    assert body["migration"]["applied"] == 0
    assert "db/migrate.py" in body["migration"]["reason"]


# ──────────────────────────────────────────────────────────────────────────
#  /v1/snapshot
# ──────────────────────────────────────────────────────────────────────────

def test_snapshot_dev_bypass_returns_payload(app_with_mocks):
    """Dev + no Entra config + no token → anonymous principal lets the
    snapshot through; empty payload because the mocked entity/edge
    queries return empty lists."""
    app, _, _ = app_with_mocks
    response = TestClient(app).get("/v1/snapshot")
    assert response.status_code == 200
    body = response.json()
    assert body["entities"] == []
    assert body["edges"] == []
    assert "ts" in body  # ISO timestamp string


def test_snapshot_with_seeded_data(env_minimal, monkeypatch):
    entities = [
        {"id": "005930", "cluster": "TECH",   "anomaly": 0.12, "tx_vol": 8_412_000_000},
        {"id": "035420", "cluster": "TECH",   "anomaly": 0.71, "tx_vol": 1_180_000_000},
    ]
    edges = [
        {"from": "005930", "to": "035420", "weight": 0.45},
    ]
    pool = _build_mock_pool(entities=entities, edges=edges)
    redis_client = _build_mock_redis()
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)

    body = TestClient(_build_app()).get("/v1/snapshot").json()
    assert len(body["entities"]) == 2
    assert body["entities"][0]["id"] == "005930"
    assert body["entities"][1]["anomaly"] == pytest.approx(0.71)
    assert body["edges"] == [{"from": "005930", "to": "035420", "weight": 0.45}]


def test_snapshot_in_prod_without_token_returns_problem_json(monkeypatch, env_minimal):
    """No Entra config but APP_ENV=production → bearer required, fail-closed."""
    monkeypatch.setenv("APP_ENV", "production")
    from src.core.config import get_settings
    get_settings.cache_clear()

    pool = _build_mock_pool()
    redis_client = _build_mock_redis()
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)

    response = TestClient(_build_app()).get("/v1/snapshot")
    assert response.status_code == 401
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.headers.get("www-authenticate", "").lower().startswith("bearer")
    body = response.json()
    assert body["status"] == 401
    assert body["title"] == "Unauthorized"


# ──────────────────────────────────────────────────────────────────────────
#  /v1/me
# ──────────────────────────────────────────────────────────────────────────

def test_me_in_dev_returns_anonymous(app_with_mocks):
    response = TestClient(app_with_mocks[0]).get("/v1/me")
    assert response.status_code == 200
    body = response.json()
    assert body["subject"] == "anonymous"
    assert body["tenant"] == "dev"
    assert body["roles"] == []
    assert body["scopes"] == []


# ──────────────────────────────────────────────────────────────────────────
#  /v1/audit/recent — Sprint 5o-C-3
# ──────────────────────────────────────────────────────────────────────────

import json as _json
from datetime import datetime as _dt, timezone as _tz


def _audit_pg_row(**overrides: Any) -> dict[str, Any]:
    base = {
        "ts":                _dt(2026, 5, 11, 4, 30, 0, tzinfo=_tz.utc),
        "symbol":            "005930",
        "mode":              "shadow",
        "executed":          False,
        "intended_action":   "buy",
        "intended_quantity": 7,
        "order_id":          None,
        "blocked_by":        None,
        "reason":            "ALLOW_LIVE_ORDERS=false",
        "signal_action":     "buy",
        "signal_confidence": 0.65,
        "signal_score":      0.65,
        "signal_rationale":  _json.dumps([
            {"agent_id": "quant.rsi", "action": "buy", "confidence": 0.7},
        ]),
    }
    base.update(overrides)
    return base


def test_audit_recent_empty_returns_envelope(app_with_mocks):
    """No rows in DB → 200 with empty list (frontend renders empty state)."""
    app, _, _ = app_with_mocks
    response = TestClient(app).get("/v1/audit/recent?symbol=005930")
    assert response.status_code == 200
    body = response.json()
    assert body == {"symbol": "005930", "rows": []}


def test_audit_recent_returns_rows_in_order(env_minimal, monkeypatch):
    """Repository hands back two rows; envelope passes them through."""
    rows = [
        _audit_pg_row(),  # newer
        _audit_pg_row(
            ts=_dt(2026, 5, 11, 4, 25, 0, tzinfo=_tz.utc),
            mode="noop",
            blocked_by="cooldown",
            reason="60s remaining",
        ),
    ]
    pool = _build_mock_pool(audit_rows=rows)
    redis_client = _build_mock_redis()
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)

    body = TestClient(_build_app()).get(
        "/v1/audit/recent?symbol=005930&limit=5"
    ).json()
    assert body["symbol"] == "005930"
    assert len(body["rows"]) == 2
    assert body["rows"][0]["mode"] == "shadow"
    assert body["rows"][1]["mode"] == "noop"
    assert body["rows"][1]["blocked_by"] == "cooldown"
    # Rationale JSON parsed into list[dict] before crossing the wire
    assert body["rows"][0]["signal_rationale"][0]["agent_id"] == "quant.rsi"


def test_audit_recent_missing_symbol_returns_problem_json(app_with_mocks):
    """Query param is required — FastAPI 422 must flow through the
    RFC 7807 envelope our middleware installs."""
    app, _, _ = app_with_mocks
    response = TestClient(app).get("/v1/audit/recent")
    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")


def test_audit_recent_limit_out_of_range_rejected(app_with_mocks):
    """Defense-in-depth — 0 and 201 are both rejected at the FastAPI
    layer so the repository never sees a query with no LIMIT."""
    app, _, _ = app_with_mocks
    assert TestClient(app).get(
        "/v1/audit/recent?symbol=005930&limit=0"
    ).status_code == 422
    assert TestClient(app).get(
        "/v1/audit/recent?symbol=005930&limit=201"
    ).status_code == 422


# ──────────────────────────────────────────────────────────────────────────
#  /v1/ticks/recent — Sprint 5p-C
# ──────────────────────────────────────────────────────────────────────────
from decimal import Decimal as _Decimal


def _tick_pg_row(**overrides: Any) -> dict[str, Any]:
    base = {
        "ts":     _dt(2026, 5, 11, 0, 30, 0, tzinfo=_tz.utc),
        "price":  _Decimal("78900"),
        "volume": 120,
        "side":   "buy",
    }
    base.update(overrides)
    return base


def test_ticks_recent_empty_returns_envelope(app_with_mocks):
    """No ticks in DB → 200 with empty list (HUD shows empty sparkline)."""
    app, _, _ = app_with_mocks
    response = TestClient(app).get("/v1/ticks/recent?symbol=005930")
    assert response.status_code == 200
    body = response.json()
    assert body == {"symbol": "005930", "ticks": []}


def test_ticks_recent_returns_rows(env_minimal, monkeypatch):
    """Two seeded ticks come back through the envelope with prices cast
    to float (NUMERIC → Decimal → float at the repo edge)."""
    rows = [
        _tick_pg_row(),
        _tick_pg_row(
            ts=_dt(2026, 5, 11, 0, 29, 58, tzinfo=_tz.utc),
            price=_Decimal("78850"),
            side="sell",
        ),
    ]
    pool = _build_mock_pool(tick_rows=rows)
    redis_client = _build_mock_redis()
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)

    body = TestClient(_build_app()).get(
        "/v1/ticks/recent?symbol=005930&limit=60"
    ).json()
    assert body["symbol"] == "005930"
    assert len(body["ticks"]) == 2
    assert body["ticks"][0]["price"] == 78900.0
    assert isinstance(body["ticks"][0]["price"], float)
    assert body["ticks"][1]["side"] == "sell"


def test_ticks_recent_missing_symbol_returns_problem_json(app_with_mocks):
    app, _, _ = app_with_mocks
    response = TestClient(app).get("/v1/ticks/recent")
    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")


def test_ticks_recent_limit_out_of_range_rejected(app_with_mocks):
    """Limit clamped to 1..500 at the FastAPI layer."""
    app, _, _ = app_with_mocks
    assert TestClient(app).get(
        "/v1/ticks/recent?symbol=005930&limit=0"
    ).status_code == 422
    assert TestClient(app).get(
        "/v1/ticks/recent?symbol=005930&limit=501"
    ).status_code == 422


# ──────────────────────────────────────────────────────────────────────────
#  /v1/ticks/snapshot — Sprint 5p-D
# ──────────────────────────────────────────────────────────────────────────


def test_ticks_snapshot_empty_when_no_rows(app_with_mocks):
    """Symbols requested but DB has nothing → `snapshots` is [] and the
    `requested` array is preserved verbatim for HUD row ordering."""
    app, _, _ = app_with_mocks
    body = TestClient(app).get(
        "/v1/ticks/snapshot?symbols=005930,000660"
    ).json()
    assert body == {"requested": ["005930", "000660"], "snapshots": []}


def test_ticks_snapshot_returns_one_row_per_symbol(env_minimal, monkeypatch):
    rows = [
        {
            "symbol": "005930",
            "ts":     _dt(2026, 5, 11, 0, 30, 0, tzinfo=_tz.utc),
            "price":  _Decimal("79100"),
            "volume": 250,
            "side":   "buy",
        },
        {
            "symbol": "000660",
            "ts":     _dt(2026, 5, 11, 0, 29, 58, tzinfo=_tz.utc),
            "price":  _Decimal("197500"),
            "volume": 110,
            "side":   "sell",
        },
    ]
    pool = _build_mock_pool(tick_rows=rows)
    redis_client = _build_mock_redis()
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)

    body = TestClient(_build_app()).get(
        "/v1/ticks/snapshot?symbols=005930,000660,035420"
    ).json()
    assert body["requested"] == ["005930", "000660", "035420"]
    assert len(body["snapshots"]) == 2
    assert body["snapshots"][0]["price"] == 79100.0
    assert isinstance(body["snapshots"][0]["price"], float)


def test_ticks_snapshot_missing_symbols_returns_problem_json(app_with_mocks):
    app, _, _ = app_with_mocks
    response = TestClient(app).get("/v1/ticks/snapshot")
    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")


def test_ticks_snapshot_whitespace_only_returns_empty_envelope(app_with_mocks):
    """A comma-only or whitespace-only `symbols` is non-empty per the
    Query min_length check, but parses to zero usable symbols. Endpoint
    should respond cleanly rather than 500ing the SQL."""
    app, _, _ = app_with_mocks
    body = TestClient(app).get("/v1/ticks/snapshot?symbols=,,, ").json()
    assert body == {"requested": [], "snapshots": []}


# ──────────────────────────────────────────────────────────────────────────
#  /v1/ticks/tape — Sprint 5p-E
# ──────────────────────────────────────────────────────────────────────────


def test_ticks_tape_empty_returns_envelope(app_with_mocks):
    """No rows → 200 with empty entries (TapePanel renders empty hint)."""
    app, _, _ = app_with_mocks
    body = TestClient(app).get(
        "/v1/ticks/tape?symbols=005930,000660"
    ).json()
    assert body == {"entries": []}


def test_ticks_tape_returns_newest_first(env_minimal, monkeypatch):
    rows = [
        {
            "ts":     _dt(2026, 5, 11, 0, 30, 10, tzinfo=_tz.utc),
            "symbol": "005930",
            "price":  _Decimal("79100"),
            "volume": 250,
            "side":   "buy",
        },
        {
            "ts":     _dt(2026, 5, 11, 0, 30, 9, tzinfo=_tz.utc),
            "symbol": "000660",
            "price":  _Decimal("197500"),
            "volume": 110,
            "side":   "sell",
        },
    ]
    pool = _build_mock_pool(tick_rows=rows)
    redis_client = _build_mock_redis()
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)

    body = TestClient(_build_app()).get(
        "/v1/ticks/tape?symbols=005930,000660&limit=50"
    ).json()
    assert len(body["entries"]) == 2
    assert body["entries"][0]["symbol"] == "005930"
    assert body["entries"][0]["price"] == 79100.0
    assert body["entries"][1]["side"] == "sell"


def test_ticks_tape_missing_symbols_returns_problem_json(app_with_mocks):
    app, _, _ = app_with_mocks
    response = TestClient(app).get("/v1/ticks/tape")
    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")


def test_ticks_tape_limit_out_of_range_rejected(app_with_mocks):
    app, _, _ = app_with_mocks
    assert TestClient(app).get(
        "/v1/ticks/tape?symbols=005930&limit=0"
    ).status_code == 422
    assert TestClient(app).get(
        "/v1/ticks/tape?symbols=005930&limit=501"
    ).status_code == 422


# ──────────────────────────────────────────────────────────────────────────
#  /v1/ticks/volume — Sprint 5p-H
# ──────────────────────────────────────────────────────────────────────────


def test_ticks_volume_empty_returns_envelope(app_with_mocks):
    """No tick rows → repo returns zero entries per requested symbol so
    the HUD renders empty bars instead of dropping rows."""
    app, _, _ = app_with_mocks
    body = TestClient(app).get(
        "/v1/ticks/volume?symbols=005930,000660&window_minutes=30"
    ).json()
    assert body["window_minutes"] == 30
    # Zero-filled buckets for the two requested symbols
    assert [b["symbol"] for b in body["buckets"]] == ["005930", "000660"]
    assert all(b["total_volume"] == 0 for b in body["buckets"])


def test_ticks_volume_returns_aggregates(env_minimal, monkeypatch):
    rows = [
        {"symbol": "005930", "total_volume": 12_500, "tick_count": 87},
        {"symbol": "000660", "total_volume":  4_300, "tick_count": 32},
    ]
    pool = _build_mock_pool(tick_rows=rows)
    redis_client = _build_mock_redis()
    import src.infrastructure.database as db_mod
    import src.infrastructure.redis_pubsub as redis_mod
    monkeypatch.setattr(db_mod, "_pool", pool)
    monkeypatch.setattr(redis_mod, "_client", redis_client)

    body = TestClient(_build_app()).get(
        "/v1/ticks/volume?symbols=005930,000660,035420&window_minutes=60"
    ).json()
    assert body["window_minutes"] == 60
    assert len(body["buckets"]) == 3
    by_sym = {b["symbol"]: b for b in body["buckets"]}
    assert by_sym["005930"]["total_volume"] == 12_500
    assert by_sym["000660"]["total_volume"] == 4_300
    assert by_sym["035420"]["total_volume"] == 0


def test_ticks_volume_window_out_of_range_rejected(app_with_mocks):
    app, _, _ = app_with_mocks
    assert TestClient(app).get(
        "/v1/ticks/volume?symbols=005930&window_minutes=0"
    ).status_code == 422
    assert TestClient(app).get(
        "/v1/ticks/volume?symbols=005930&window_minutes=1441"
    ).status_code == 422


def test_ticks_volume_missing_symbols_returns_problem_json(app_with_mocks):
    app, _, _ = app_with_mocks
    response = TestClient(app).get("/v1/ticks/volume")
    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")


# ──────────────────────────────────────────────────────────────────────────
#  Error envelope shape (RFC 7807) + request_id propagation
# ──────────────────────────────────────────────────────────────────────────

def test_404_renders_problem_json(app_with_mocks):
    """Even framework-emitted 404s flow through our exception handler."""
    response = TestClient(app_with_mocks[0]).get("/v1/does-not-exist")
    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["status"] == 404
    assert body["title"] == "Not Found"
    assert body["request_id"] != "-"   # middleware generated one


def test_request_id_header_round_trips(app_with_mocks):
    """Inbound X-Request-ID flows through the middleware, lands in the
    response header AND inside the problem body, and stays verbatim."""
    response = TestClient(app_with_mocks[0]).get(
        "/v1/does-not-exist",
        headers={"X-Request-ID": "trace-abc-12345"},
    )
    assert response.headers["x-request-id"] == "trace-abc-12345"
    assert response.json()["request_id"] == "trace-abc-12345"


def test_request_id_generated_when_absent(app_with_mocks):
    """Missing header → middleware generates a UUID4 visible to the client."""
    import re
    response = TestClient(app_with_mocks[0]).get("/v1/health")
    rid = response.headers["x-request-id"]
    assert re.match(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
        rid,
    ), f"not a UUID4: {rid!r}"


def test_problem_envelope_minimum_fields(app_with_mocks):
    body = TestClient(app_with_mocks[0]).get("/v1/does-not-exist").json()
    for key in ("type", "title", "status", "instance", "request_id"):
        assert key in body
    assert body["instance"] == "/v1/does-not-exist"


# ── Balance endpoint ────────────────────────────────────────────────────────

def test_get_balance_mock_mode_returns_200(app_with_mocks):
    """Mock mode (no KIS client) returns synthetic balance as 200."""
    app, _, _ = app_with_mocks
    # app.state.balance_client is not set → _get_balance_client returns None → mock mode
    response = TestClient(app).get("/v1/balance")
    assert response.status_code == 200
    body = response.json()
    assert body["summary"]["cash"] == 10_000_000
    assert body["holdings"] == []
    assert "ts" in body


def test_get_balance_kis_error_returns_503(app_with_mocks, monkeypatch):
    """KIS upstream error maps to 503 UPSTREAM_ERROR."""
    from unittest.mock import AsyncMock
    from src.infrastructure.kis_balance_client import KisBalanceClient
    from src.infrastructure.kis_client import KisUpstreamError
    from src.api.v1 import router as router_module

    app, _, _ = app_with_mocks
    mock_client = AsyncMock(spec=KisBalanceClient)
    mock_client.fetch_balance.side_effect = KisUpstreamError("KIS down")
    monkeypatch.setattr(router_module, "_get_balance_client", lambda req: mock_client)

    response = TestClient(app).get("/v1/balance")
    assert response.status_code == 503
    body = response.json()
    assert body["type"] == "https://nexus-os.local/problems/upstream-error"
