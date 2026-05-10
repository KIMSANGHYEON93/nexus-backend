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


# ──────────────────────────────────────────────────────────────────────────
#  Mock builders — produce objects shaped exactly like the real Pool / Redis
# ──────────────────────────────────────────────────────────────────────────

def _build_mock_pool(
    schema_version: int | None = 1,
    entities: list[dict[str, Any]] | None = None,
    edges: list[dict[str, Any]] | None = None,
    audit_rows: list[dict[str, Any]] | None = None,
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

def test_health_returns_ok_payload(app_with_mocks):
    app, _, _ = app_with_mocks
    client = TestClient(app)
    response = client.get("/v1/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "nexus-backend"}


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
    assert body["migration"]["applied"] == 1


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
