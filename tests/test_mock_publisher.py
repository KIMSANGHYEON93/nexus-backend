"""Tests for src/infrastructure/mock_publisher.py.

Covers the contract the WebSocket layer relies on:
  • Each tick has every required field with the right type.
  • Symbols never escape the seeded set (frontend would NoOp on an
    unknown symbol; the mock must not introduce drift).
  • The geometric random walk floor stops prices from going negative
    even after a long run of bad-luck drift draws.
  • start / stop are idempotent and don't leak background tasks.

Skips on hosts without redis-py installed (Windows dev box); runs
end-to-end on CI / Docker.
"""

from __future__ import annotations

import asyncio
import json
import random
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("redis", reason="full backend deps required")

from src.infrastructure.mock_publisher import (
    _BASE_PRICES,
    _SEEDED_TICKERS,
    DRIFT_STD,
    MockPublisher,
    PUBLISH_INTERVAL_S,
)


def _fake_client() -> MagicMock:
    """Minimal `redis.asyncio.Redis` stand-in: exposes `publish` as
    AsyncMock so we can assert call args without booting Redis."""
    client = MagicMock()
    client.publish = AsyncMock(return_value=1)  # 1 = subscribers reached
    return client


# ──────────────────────────────────────────────────────────────────────────
#  Tick shape
# ──────────────────────────────────────────────────────────────────────────

def test_next_tick_has_required_fields():
    pub = MockPublisher(_fake_client())
    tick = pub._next_tick()
    assert set(tick.keys()) == {"symbol", "ts", "price", "volume", "side"}


def test_next_tick_field_types():
    pub = MockPublisher(_fake_client())
    tick = pub._next_tick()
    assert isinstance(tick["symbol"], str)
    assert isinstance(tick["ts"],     str)  # ISO-8601 string, not datetime
    assert isinstance(tick["price"],  float)
    assert isinstance(tick["volume"], int)
    assert tick["side"] in ("buy", "sell")


def test_next_tick_symbol_in_seeded_set():
    pub = MockPublisher(_fake_client())
    for _ in range(50):
        assert pub._next_tick()["symbol"] in _SEEDED_TICKERS


def test_next_tick_volume_in_documented_band():
    pub = MockPublisher(_fake_client())
    for _ in range(50):
        v = pub._next_tick()["volume"]
        assert 100 <= v <= 10_000


def test_next_tick_timestamp_iso_with_ms_precision():
    pub = MockPublisher(_fake_client())
    ts = pub._next_tick()["ts"]
    # YYYY-MM-DDTHH:MM:SS.fff+00:00 form — millisecond truncation
    assert "T" in ts and ts.endswith("+00:00")
    # Trim TZ then parse back — should round-trip
    from datetime import datetime
    datetime.fromisoformat(ts)


# ──────────────────────────────────────────────────────────────────────────
#  Random-walk safety
# ──────────────────────────────────────────────────────────────────────────

def test_price_floor_holds_under_pathological_drift(monkeypatch):
    """Force gauss() to always return a large negative drift; the price
    must clamp at 1.0 KRW rather than going negative."""
    pub = MockPublisher(_fake_client())
    monkeypatch.setattr("src.infrastructure.mock_publisher.random.gauss",
                        lambda mu, sigma: -1000.0)
    # Force every choice to the same symbol so we accumulate drift on it
    monkeypatch.setattr("src.infrastructure.mock_publisher.random.choice",
                        lambda seq: "005930")
    for _ in range(100):
        tick = pub._next_tick()
    assert tick["price"] >= 1.0


def test_price_walk_starts_from_documented_base():
    pub = MockPublisher(_fake_client())
    # Force gauss=0 so first draw doesn't drift
    import unittest.mock as mock
    with mock.patch("src.infrastructure.mock_publisher.random.gauss", return_value=0.0):
        with mock.patch("src.infrastructure.mock_publisher.random.choice", return_value="005930"):
            tick = pub._next_tick()
    assert tick["price"] == round(_BASE_PRICES["005930"], 2)


def test_drift_std_is_documented_value():
    """Pin the calibration so refactors don't silently shift the band."""
    assert DRIFT_STD == 0.002


# ──────────────────────────────────────────────────────────────────────────
#  Lifecycle
# ──────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_start_publishes_to_correct_channel():
    """One full publish cycle should hit the configured tick channel
    with a valid JSON payload."""
    from src.infrastructure.redis_pubsub import CHANNEL_TICK

    client = _fake_client()
    pub = MockPublisher(client)
    await pub.start()
    # Let the loop run one iteration — sleep is well under the cadence.
    await asyncio.sleep(0.05)
    await pub.stop()

    assert client.publish.await_count >= 1
    channel, payload = client.publish.await_args_list[0].args
    assert channel == CHANNEL_TICK
    parsed = json.loads(payload)
    assert parsed["symbol"] in _SEEDED_TICKERS
    assert isinstance(parsed["price"], (int, float))


@pytest.mark.asyncio
async def test_start_is_idempotent():
    pub = MockPublisher(_fake_client())
    await pub.start()
    task_first = pub._task
    await pub.start()
    assert pub._task is task_first  # same task, no orphan
    await pub.stop()


@pytest.mark.asyncio
async def test_stop_without_start_is_safe():
    pub = MockPublisher(_fake_client())
    await pub.stop()  # no-op, no exception


@pytest.mark.asyncio
async def test_stop_cancels_inner_task_cleanly():
    pub = MockPublisher(_fake_client())
    await pub.start()
    inner = pub._task
    assert inner is not None
    await pub.stop()
    assert inner.done()
    assert pub._task is None


@pytest.mark.asyncio
async def test_published_count_increments():
    pub = MockPublisher(_fake_client())
    assert pub.published_count == 0
    await pub.start()
    await asyncio.sleep(0.05)
    await pub.stop()
    assert pub.published_count >= 1
