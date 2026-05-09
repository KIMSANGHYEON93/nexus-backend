"""Unit tests for PublisherSupervisor — Sprint 5d failover behavior.

Three scenarios drive the design:
  1. Happy path: KIS bring-up succeeds → KisPublisher is active.
  2. Bring-up failure: KIS auth/connect raises → MockPublisher in dev,
     no publisher in prod.
  3. Runtime failover: KisPublisher dies after start → watchdog spawns
     MockPublisher. The watchdog's whole job is preventing a silent
     canvas while operators investigate.

Patches `KisClient` at the supervisor's import path so the test never
touches httpx, websockets, or the network.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.infrastructure.publisher_supervisor import PublisherSupervisor
from src.infrastructure.kis_client import KisAuthError


def _make_settings(
    *,
    app_key: str = "AKEY",
    app_secret: str = "ASECRET",
    app_env: str = "development",
) -> Any:
    s = MagicMock()
    s.kis_app_key      = app_key
    s.kis_app_secret   = app_secret
    s.kis_env          = "paper"
    s.app_env          = app_env
    s.kis_subscribe_symbol_list = ["005930", "000660"]
    return s


def _make_redis() -> Any:
    r = MagicMock()
    r.publish = AsyncMock(return_value=None)
    return r


# ── Bring-up: KIS happy path ────────────────────────────────────────────


async def test_bringup_uses_kis_when_creds_present_and_auth_succeeds():
    settings = _make_settings()
    fake_kis = MagicMock()
    fake_kis.authenticate     = AsyncMock(return_value=None)
    fake_kis.connect          = AsyncMock(return_value=None)
    fake_kis.close            = AsyncMock(return_value=None)
    fake_kis.token_expires_at = None  # refresh loop will treat as needs-refresh
    fake_kis.subscribe        = AsyncMock(return_value=None)

    async def _empty_stream():
        # Hold the stream open so the publisher task stays alive.
        await asyncio.sleep(60)
        if False:  # pragma: no cover
            yield None

    fake_kis.stream_ticks = _empty_stream

    with patch(
        "src.infrastructure.publisher_supervisor.KisClient",
        return_value=fake_kis,
    ):
        sup = PublisherSupervisor(_make_redis(), settings)
        try:
            await sup.start()
            assert sup.active_kind == "kis"
            assert fake_kis.authenticate.await_count == 1
            assert fake_kis.connect.await_count == 1
        finally:
            await sup.stop()


# ── Bring-up: KIS fails → mock fallback in dev ──────────────────────────


async def test_bringup_falls_back_to_mock_when_kis_auth_fails_in_dev():
    settings = _make_settings(app_env="development")
    fake_kis = MagicMock()
    fake_kis.authenticate = AsyncMock(side_effect=KisAuthError("bad creds"))
    fake_kis.close        = AsyncMock(return_value=None)

    with patch(
        "src.infrastructure.publisher_supervisor.KisClient",
        return_value=fake_kis,
    ):
        sup = PublisherSupervisor(_make_redis(), settings)
        try:
            await sup.start()
            assert sup.active_kind == "mock"
            assert fake_kis.close.await_count == 1, "failed KIS client must be closed"
        finally:
            await sup.stop()


async def test_bringup_no_publisher_when_kis_fails_in_production():
    """Production must NOT silently swap to MockPublisher at startup —
    operators need to see the failure on the canvas, not synthetic data."""
    settings = _make_settings(app_env="production")
    fake_kis = MagicMock()
    fake_kis.authenticate = AsyncMock(side_effect=KisAuthError("bad creds"))
    fake_kis.close        = AsyncMock(return_value=None)

    with patch(
        "src.infrastructure.publisher_supervisor.KisClient",
        return_value=fake_kis,
    ):
        sup = PublisherSupervisor(_make_redis(), settings)
        await sup.start()
        assert sup.active_kind == "none"
        await sup.stop()


# ── Bring-up: no creds at all ───────────────────────────────────────────


async def test_bringup_uses_mock_when_no_kis_creds_in_dev():
    settings = _make_settings(app_key="", app_secret="", app_env="development")
    sup = PublisherSupervisor(_make_redis(), settings)
    try:
        await sup.start()
        assert sup.active_kind == "mock"
    finally:
        await sup.stop()


# ── Runtime failover: KisPublisher dies → watchdog → MockPublisher ──────


async def test_watchdog_swaps_to_mock_when_kis_publisher_dies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tight watchdog interval so the test runs in ~1s instead of ~5s."""
    monkeypatch.setattr(
        "src.infrastructure.publisher_supervisor._WATCHDOG_INTERVAL_S", 0.1
    )

    settings = _make_settings(app_env="development")
    fake_kis = MagicMock()
    fake_kis.authenticate     = AsyncMock(return_value=None)
    fake_kis.connect          = AsyncMock(return_value=None)
    fake_kis.close            = AsyncMock(return_value=None)
    fake_kis.token_expires_at = None
    fake_kis.subscribe        = AsyncMock(return_value=None)

    # Stream that immediately exits — simulates "KIS publisher loop ended
    # because the WS dropped and reconnect was exhausted".
    async def _dead_stream():
        return
        yield  # pragma: no cover

    fake_kis.stream_ticks = _dead_stream

    with patch(
        "src.infrastructure.publisher_supervisor.KisClient",
        return_value=fake_kis,
    ):
        sup = PublisherSupervisor(_make_redis(), settings)
        try:
            await sup.start()
            assert sup.active_kind == "kis"

            # Wait for the publisher task to finish (immediately) and the
            # watchdog to observe is_running=False on its next tick.
            for _ in range(40):
                if sup.active_kind == "mock":
                    break
                await asyncio.sleep(0.05)

            assert sup.active_kind == "mock", "watchdog should have failed over"
            assert sup.failover_count == 1
            assert fake_kis.close.await_count >= 1, "failed KIS client must be closed on failover"
        finally:
            await sup.stop()


async def test_watchdog_failover_works_in_production_too(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bring-up policy and runtime failover policy differ on purpose:
    bring-up in prod = stay silent (operator signal), runtime in prod =
    failover to mock (the canvas was already alive; better synthetic
    than dead). This test pins that intentional asymmetry."""
    monkeypatch.setattr(
        "src.infrastructure.publisher_supervisor._WATCHDOG_INTERVAL_S", 0.1
    )

    settings = _make_settings(app_env="production")
    fake_kis = MagicMock()
    fake_kis.authenticate     = AsyncMock(return_value=None)
    fake_kis.connect          = AsyncMock(return_value=None)
    fake_kis.close            = AsyncMock(return_value=None)
    fake_kis.token_expires_at = None
    fake_kis.subscribe        = AsyncMock(return_value=None)

    async def _dead_stream():
        return
        yield  # pragma: no cover

    fake_kis.stream_ticks = _dead_stream

    with patch(
        "src.infrastructure.publisher_supervisor.KisClient",
        return_value=fake_kis,
    ):
        sup = PublisherSupervisor(_make_redis(), settings)
        try:
            await sup.start()
            for _ in range(40):
                if sup.active_kind == "mock":
                    break
                await asyncio.sleep(0.05)
            assert sup.active_kind == "mock"
            assert sup.failover_count == 1
        finally:
            await sup.stop()


# ── stop() cleanup ──────────────────────────────────────────────────────


async def test_stop_cancels_watchdog_and_active_publisher():
    settings = _make_settings(app_env="development", app_key="", app_secret="")
    sup = PublisherSupervisor(_make_redis(), settings)
    await sup.start()
    assert sup.active_kind == "mock"
    await sup.stop()
    # After stop, calling stop again must not raise.
    await sup.stop()
