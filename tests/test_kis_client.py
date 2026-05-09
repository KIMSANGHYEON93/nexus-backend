"""Unit tests for `KisClient.authenticate()`.

Network is replaced with `httpx.MockTransport`; tests assert state
transitions, token caching, expiry parsing, and the four error branches
that map to the structured `kis_auth_failed` log lines.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import httpx
import pytest

from src.core.config import Settings
from src.infrastructure.kis_client import (
    KisAuthError,
    KisClient,
    KisConnectionState,
)


def _settings(env: str = "paper", *, key: str = "ak", secret: str = "as") -> Settings:
    return Settings(
        database_url="postgresql://t:t@localhost/t",
        redis_url="redis://localhost:6379/0",
        kis_app_key=key,
        kis_app_secret=secret,
        kis_env=env,  # type: ignore[arg-type]
    )


def _client(handler: Callable[[httpx.Request], httpx.Response], *, env: str = "paper") -> KisClient:
    transport = httpx.MockTransport(handler)
    http = httpx.AsyncClient(transport=transport, base_url="https://unused")
    return KisClient(_settings(env=env), http_client=http)


def _ok_body(token: str = "eyJabc.def.ghi", expires_at: str | None = None) -> dict[str, Any]:
    if expires_at is None:
        future = datetime.now() + timedelta(hours=24)
        expires_at = future.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "access_token": token,
        "access_token_token_expired": expires_at,
        "token_type": "Bearer",
        "expires_in": 86400,
    }


# ─────────────────────────────────────────────────────────────────────
#  Happy path
# ─────────────────────────────────────────────────────────────────────

async def test_authenticate_paper_endpoint_and_state_transition() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = request.content.decode()
        return httpx.Response(200, json=_ok_body())

    client = _client(handler, env="paper")
    initial_state: KisConnectionState = client.state
    assert initial_state is KisConnectionState.DISCONNECTED

    await client.authenticate()

    final_state: KisConnectionState = client.state
    assert final_state is KisConnectionState.AUTHENTICATING
    assert client.access_token == "eyJabc.def.ghi"
    assert client.access_token_expires_at is not None
    assert client.access_token_expires_at.tzinfo == timezone.utc

    assert "openapivts.koreainvestment.com:29443" in seen["url"]
    assert "/oauth2/tokenP" in seen["url"]
    assert '"grant_type": "client_credentials"' in seen["body"]
    assert '"appkey": "ak"' in seen["body"]
    assert '"appsecret": "as"' in seen["body"]


async def test_authenticate_live_endpoint_routing() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json=_ok_body())

    client = _client(handler, env="live")
    await client.authenticate()
    assert "openapi.koreainvestment.com:9443" in seen["url"]


async def test_authenticate_caches_fresh_token_and_skips_network() -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=_ok_body())

    client = _client(handler)
    await client.authenticate()
    await client.authenticate()
    assert calls == 1


async def test_authenticate_refreshes_when_token_within_margin() -> None:
    # Expiry 1 minute from now → within the 5-minute margin → refresh.
    near_expiry = (datetime.now() + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
    far_expiry = (datetime.now() + timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    bodies = iter([
        _ok_body(token="t1", expires_at=near_expiry),
        _ok_body(token="t2", expires_at=far_expiry),
    ])

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=next(bodies))

    client = _client(handler)
    await client.authenticate()
    assert client.access_token == "t1"
    await client.authenticate()
    assert client.access_token == "t2"


async def test_token_head_only_in_logs(caplog: pytest.LogCaptureFixture) -> None:
    """Full token must never appear in any log record."""
    full_token = "eyJSECRETPAYLOAD.never.log.this.fully"

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_ok_body(token=full_token))

    caplog.set_level("INFO")
    client = _client(handler)
    await client.authenticate()

    rendered = "\n".join(rec.getMessage() + " " + repr(rec.__dict__) for rec in caplog.records)
    assert full_token not in rendered
    assert "eyJSEC" in rendered  # masked head present (correlation aid)


# ─────────────────────────────────────────────────────────────────────
#  Error paths — every branch maps to a typed exception + FAILED state
# ─────────────────────────────────────────────────────────────────────

async def test_authenticate_4xx_raises_and_marks_failed() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"rt_cd": "1", "msg1": "approval key expired"})

    client = _client(handler)
    with pytest.raises(KisAuthError, match="HTTP 401"):
        await client.authenticate()
    assert client.state is KisConnectionState.FAILED
    assert client.access_token is None


async def test_authenticate_network_error_raises_and_marks_failed() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("dns")

    client = _client(handler)
    with pytest.raises(KisAuthError, match="network failure"):
        await client.authenticate()
    assert client.state is KisConnectionState.FAILED


async def test_authenticate_malformed_body_raises_and_marks_failed() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": "shape"})

    client = _client(handler)
    with pytest.raises(KisAuthError, match="missing required fields"):
        await client.authenticate()
    assert client.state is KisConnectionState.FAILED


async def test_authenticate_unparseable_expiry_raises_and_marks_failed() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "access_token": "tok",
            "access_token_token_expired": "not-a-date",
        })

    client = _client(handler)
    with pytest.raises(KisAuthError, match="expiry timestamp unparseable"):
        await client.authenticate()
    assert client.state is KisConnectionState.FAILED
