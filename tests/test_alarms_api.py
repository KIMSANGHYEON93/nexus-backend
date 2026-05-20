"""Integration tests for `GET /v1/alarms` — Sprint 5r.

Pattern follows `test_api_integration.py`:
  • Build a minimal FastAPI app with the v1 router mounted and the
    exception handlers / middleware installed.
  • Skip-on-import when full backend deps aren't present (Windows dev box).
  • Drive the router via `TestClient` and assert on the JSON envelope
    + response headers (RFC 7807 content-type on the error paths).

Spec acceptance cases mapped 1:1 to test names where possible.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

# Skip the whole module if the FastAPI runtime isn't installed.
pytest.importorskip("fastapi", reason="full backend deps required")
pytest.importorskip("pydantic_settings", reason="full backend deps required")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.v1.router import reset_alarm_repo_for_tests, router as v1_router
from src.core.exception_handlers import install as install_exception_handlers
from src.core.middleware import RequestIdMiddleware
from src.domain.alarms.models import Alarm, Severity, Status
from src.infrastructure.alarms.in_memory_repo import InMemoryAlarmRepository


_ANCHOR = datetime(2026, 5, 13, 12, 0, 0, tzinfo=timezone.utc)


def _alarm(
    *,
    id: str,
    severity: Severity = Severity.INFO,
    status: Status = Status.ACTIVE,
    source: str = "trading-coordinator",
    occurred_at: datetime | None = None,
    acknowledged_at: datetime | None = None,
    resolved_at: datetime | None = None,
    code: str = "TEST_CODE",
    metadata: dict[str, Any] | None = None,
) -> Alarm:
    return Alarm(
        id=id,
        severity=severity,
        status=status,
        source=source,
        code=code,
        title="TEST TITLE",
        message="test message",
        occurred_at=occurred_at or _ANCHOR,
        acknowledged_at=acknowledged_at,
        resolved_at=resolved_at,
        metadata=metadata,
    )


def _build_app() -> FastAPI:
    app = FastAPI(title="nexus-backend test (alarms)")
    app.add_middleware(RequestIdMiddleware)  # type: ignore[arg-type]
    install_exception_handlers(app)
    app.include_router(v1_router)
    return app


@pytest.fixture
def empty_repo_app(env_minimal):
    """Mount the v1 router with an empty in-memory alarm repo. The DI
    singleton inside router.py is swapped via `reset_alarm_repo_for_tests`
    and restored after the test."""
    repo = InMemoryAlarmRepository()
    reset_alarm_repo_for_tests(repo)
    try:
        yield _build_app(), repo
    finally:
        reset_alarm_repo_for_tests(None)


@pytest.fixture
def seeded_repo_app(env_minimal):
    """Repo pre-loaded with a known fixture set so we can assert on
    field shapes, sort order, and counters deterministically."""
    repo = InMemoryAlarmRepository([
        _alarm(
            id="alarm-active-anomaly",
            severity=Severity.ANOMALY,
            status=Status.ACTIVE,
            source="trading-coordinator",
            occurred_at=_ANCHOR,
            metadata={"symbol": "SMH", "sigma": 6.1},
        ),
        _alarm(
            id="alarm-active-warn",
            severity=Severity.WARN,
            status=Status.ACTIVE,
            source="system-monitor",
            occurred_at=_ANCHOR - timedelta(minutes=2),
        ),
        _alarm(
            id="alarm-ack",
            severity=Severity.WARN,
            status=Status.ACKNOWLEDGED,
            source="trading-coordinator",
            occurred_at=_ANCHOR - timedelta(minutes=5),
            acknowledged_at=_ANCHOR - timedelta(minutes=4),
        ),
        _alarm(
            id="alarm-resolved",
            severity=Severity.INFO,
            status=Status.RESOLVED,
            source="news-provider",
            occurred_at=_ANCHOR - timedelta(hours=2),
            resolved_at=_ANCHOR - timedelta(hours=1),
        ),
    ])
    reset_alarm_repo_for_tests(repo)
    try:
        yield _build_app(), repo
    finally:
        reset_alarm_repo_for_tests(None)


# ──────────────────────────────────────────────────────────────────────────
#  Happy path — empty envelope
# ──────────────────────────────────────────────────────────────────────────

def test_alarms_returns_200_with_envelope_when_empty(empty_repo_app):
    app, _ = empty_repo_app
    response = TestClient(app).get("/v1/alarms")
    assert response.status_code == 200
    body = response.json()
    # Envelope shape (spec §2 — every key is snake_case)
    assert set(body.keys()) == {
        "items", "total", "unacknowledged_count", "window_since", "server_time",
    }
    assert body["items"] == []
    assert body["total"] == 0
    assert body["unacknowledged_count"] == 0
    # `since` not provided → server defaulted to now - 24h
    assert body["window_since"] is not None
    assert body["server_time"] is not None


# ──────────────────────────────────────────────────────────────────────────
#  Snake-case wire fidelity + default `status` filter
# ──────────────────────────────────────────────────────────────────────────

def test_alarms_default_filter_returns_only_active(seeded_repo_app):
    """Spec: `status` defaults to `active` when omitted."""
    app, _ = seeded_repo_app
    body = TestClient(app).get("/v1/alarms").json()
    ids = [a["id"] for a in body["items"]]
    # Only the two ACTIVE alarms surface; ack'd + resolved are filtered out.
    assert ids == ["alarm-active-anomaly", "alarm-active-warn"]
    assert body["total"] == 2


def test_alarms_dto_fields_are_snake_case(seeded_repo_app):
    """Spec AC: every AlarmDTO field name on the wire is snake_case."""
    app, _ = seeded_repo_app
    item = TestClient(app).get("/v1/alarms").json()["items"][0]
    # The full snake_case field set must be present.
    required = {
        "id", "severity", "status", "source", "code",
        "title", "message", "entity_id", "occurred_at",
        "acknowledged_at", "resolved_at", "metadata",
    }
    missing = required - set(item.keys())
    assert not missing, f"missing snake_case fields: {missing}"


def test_alarms_items_sorted_newest_first(seeded_repo_app):
    app, _ = seeded_repo_app
    body = TestClient(app).get(
        "/v1/alarms?status=active,acknowledged,resolved"
    ).json()
    ids = [a["id"] for a in body["items"]]
    assert ids == [
        "alarm-active-anomaly",   # _ANCHOR
        "alarm-active-warn",      # -2m
        "alarm-ack",              # -5m
        "alarm-resolved",         # -2h
    ]


# ──────────────────────────────────────────────────────────────────────────
#  unacknowledged_count is GLOBAL — independent of filters
# ──────────────────────────────────────────────────────────────────────────

def test_unacknowledged_count_is_global_active_count(seeded_repo_app):
    """Spec AC: `unacknowledged_count` ignores filters."""
    app, _ = seeded_repo_app
    # Filter narrows visible items to one severity, but the badge counter
    # must still report the global ACTIVE count (2).
    body = TestClient(app).get("/v1/alarms?severity=anomaly").json()
    assert len(body["items"]) == 1
    assert body["unacknowledged_count"] == 2


def test_unacknowledged_count_zero_when_no_active(env_minimal):
    repo = InMemoryAlarmRepository([
        _alarm(
            id="only-resolved",
            status=Status.RESOLVED,
            resolved_at=_ANCHOR,
            occurred_at=_ANCHOR - timedelta(minutes=1),
        ),
    ])
    reset_alarm_repo_for_tests(repo)
    try:
        body = TestClient(_build_app()).get("/v1/alarms?status=resolved").json()
        assert body["unacknowledged_count"] == 0
        assert len(body["items"]) == 1
    finally:
        reset_alarm_repo_for_tests(None)


# ──────────────────────────────────────────────────────────────────────────
#  window_since default — server_time - 24h
# ──────────────────────────────────────────────────────────────────────────

def test_window_since_defaults_to_24h_lookback(empty_repo_app):
    """Spec AC: when `since` omitted, window_since == server_time - 24h
    (±1s allowed for serialization latency)."""
    app, _ = empty_repo_app
    body = TestClient(app).get("/v1/alarms").json()
    server_time = datetime.fromisoformat(body["server_time"].replace("Z", "+00:00"))
    window_since = datetime.fromisoformat(body["window_since"].replace("Z", "+00:00"))
    delta = server_time - window_since
    assert timedelta(hours=24, seconds=-1) <= delta <= timedelta(hours=24, seconds=1)


def test_explicit_since_is_echoed_back(empty_repo_app):
    """A caller-supplied `since` flows straight into `window_since`."""
    app, _ = empty_repo_app
    iso = "2026-05-12T00:00:00Z"
    body = TestClient(app).get(f"/v1/alarms?since={iso}").json()
    # Pydantic emits with explicit offset; just verify the instant.
    window_since = datetime.fromisoformat(body["window_since"].replace("Z", "+00:00"))
    assert window_since == datetime(2026, 5, 12, 0, 0, 0, tzinfo=timezone.utc)


# ──────────────────────────────────────────────────────────────────────────
#  Severity filter — happy path + invalid token (400)
# ──────────────────────────────────────────────────────────────────────────

def test_severity_filter_returns_matching_subset(seeded_repo_app):
    app, _ = seeded_repo_app
    body = TestClient(app).get("/v1/alarms?severity=warn").json()
    ids = {a["id"] for a in body["items"]}
    assert ids == {"alarm-active-warn"}


# Spec §2: 400 responses MUST use the `invalid-input` problem-type URI
# (not Starlette's default `about:blank`). The frontend `PROBLEM_TYPE`
# switch keys off `type` so this is a load-bearing contract.
_INVALID_INPUT_TYPE = "https://nexus-os.local/problems/invalid-input"


def test_invalid_severity_token_returns_400_problem_json(empty_repo_app):
    app, _ = empty_repo_app
    response = TestClient(app).get("/v1/alarms?severity=foo")
    assert response.status_code == 400
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["type"] == _INVALID_INPUT_TYPE
    assert body["status"] == 400
    assert body["title"] == "Invalid query parameter"
    assert "foo" in (body.get("detail") or "")
    # `instance` echoes the request path so operator tooling can correlate.
    assert body.get("instance") == "/v1/alarms"


def test_invalid_status_token_returns_400_problem_json(empty_repo_app):
    app, _ = empty_repo_app
    response = TestClient(app).get("/v1/alarms?status=banana")
    assert response.status_code == 400
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["type"] == _INVALID_INPUT_TYPE
    assert body["status"] == 400
    assert "banana" in (body.get("detail") or "")


def test_invalid_since_returns_400_problem_json(empty_repo_app):
    app, _ = empty_repo_app
    response = TestClient(app).get("/v1/alarms?since=not-a-date")
    assert response.status_code == 400
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["type"] == _INVALID_INPUT_TYPE
    assert body["status"] == 400
    assert "not-a-date" in (body.get("detail") or "")


def test_invalid_since_iso_format_returns_400_problem_json(empty_repo_app):
    """Spec §2: malformed RFC-3339 → 400 with `invalid-input` type URI."""
    app, _ = empty_repo_app
    response = TestClient(app).get("/v1/alarms?since=not-an-iso")
    assert response.status_code == 400
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["type"] == _INVALID_INPUT_TYPE
    assert "not-an-iso" in (body.get("detail") or "")


def test_invalid_severity_bar_returns_400_with_type_uri(empty_repo_app):
    """Spec §2: enum-outside-set → 400 + `invalid-input` URI (regression
    guard for the LOW 2 finding — previously `type=about:blank`)."""
    app, _ = empty_repo_app
    response = TestClient(app).get("/v1/alarms?severity=bar")
    assert response.status_code == 400
    body = response.json()
    assert body["type"] == _INVALID_INPUT_TYPE
    assert "bar" in (body.get("detail") or "")


def test_invalid_status_bar_returns_400_with_type_uri(empty_repo_app):
    """Spec §2: status outside enum → 400 + `invalid-input` URI."""
    app, _ = empty_repo_app
    response = TestClient(app).get("/v1/alarms?status=bar")
    assert response.status_code == 400
    body = response.json()
    assert body["type"] == _INVALID_INPUT_TYPE
    assert "bar" in (body.get("detail") or "")


# ──────────────────────────────────────────────────────────────────────────
#  Limit clamp — 422 on out-of-range (FastAPI's own validation)
# ──────────────────────────────────────────────────────────────────────────

def test_limit_below_min_returns_422(empty_repo_app):
    app, _ = empty_repo_app
    response = TestClient(app).get("/v1/alarms?limit=0")
    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")


def test_limit_above_max_returns_422(empty_repo_app):
    app, _ = empty_repo_app
    response = TestClient(app).get("/v1/alarms?limit=201")
    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")


# ──────────────────────────────────────────────────────────────────────────
#  Authentication — production env without bearer should 401
# ──────────────────────────────────────────────────────────────────────────

def test_unauthorized_in_production_returns_401_problem_json(monkeypatch, env_minimal):
    """Spec AC: same dev-bypass / fail-closed policy as `/v1/snapshot`."""
    monkeypatch.setenv("APP_ENV", "production")
    from src.core.config import get_settings
    get_settings.cache_clear()

    repo = InMemoryAlarmRepository()
    reset_alarm_repo_for_tests(repo)
    try:
        response = TestClient(_build_app()).get("/v1/alarms")
        assert response.status_code == 401
        assert response.headers["content-type"].startswith("application/problem+json")
        body = response.json()
        assert body["status"] == 401
        assert body["title"] == "Unauthorized"
    finally:
        reset_alarm_repo_for_tests(None)


# ──────────────────────────────────────────────────────────────────────────
#  Source filter — exact match + multi-token
# ──────────────────────────────────────────────────────────────────────────

def test_source_filter_returns_only_listed_sources(seeded_repo_app):
    app, _ = seeded_repo_app
    body = TestClient(app).get(
        "/v1/alarms?source=trading-coordinator&status=active,acknowledged"
    ).json()
    ids = {a["id"] for a in body["items"]}
    assert ids == {"alarm-active-anomaly", "alarm-ack"}


# ──────────────────────────────────────────────────────────────────────────
#  Fault tolerance — repo blows up → empty envelope, not 503
# ──────────────────────────────────────────────────────────────────────────

class _FailingRepo:
    """Stand-in repo whose `list` raises a transient backend-style error.

    Spec mandates the router converts this to an empty 200 envelope (same
    rule as `/v1/audit/recent`), keeping the HUD alive."""

    async def list(self, filters):
        raise ConnectionError("backend unreachable")

    async def count_active(self) -> int:  # pragma: no cover — never reached
        raise ConnectionError("backend unreachable")


def test_repo_failure_yields_empty_envelope_not_503(env_minimal):
    reset_alarm_repo_for_tests(_FailingRepo())
    try:
        response = TestClient(_build_app()).get("/v1/alarms")
        assert response.status_code == 200
        body = response.json()
        assert body["items"] == []
        assert body["total"] == 0
        assert body["unacknowledged_count"] == 0
    finally:
        reset_alarm_repo_for_tests(None)


class _DomainCorruptRepo:
    """Repo whose `list` violates a domain invariant — should bubble up
    to the 500 catch-all per spec ("repository raises ValueError → 500
    + internal-error ProblemDetail").
    """

    async def list(self, filters):
        raise ValueError("alarm row corrupt: status=resolved but resolved_at is None")

    async def count_active(self) -> int:  # pragma: no cover
        return 0


def test_repo_domain_violation_returns_500_problem_json(env_minimal):
    reset_alarm_repo_for_tests(_DomainCorruptRepo())
    try:
        response = TestClient(_build_app(), raise_server_exceptions=False).get(
            "/v1/alarms"
        )
        assert response.status_code == 500
        assert response.headers["content-type"].startswith("application/problem+json")
        body = response.json()
        assert body["status"] == 500
        assert body["title"] == "Internal Server Error"
    finally:
        reset_alarm_repo_for_tests(None)
