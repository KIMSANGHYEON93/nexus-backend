"""Unit tests for KisPublisher — proves the wire payload exactly matches
MockPublisher's so the cutover is invisible to the frontend BackendStreamer.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

from src.infrastructure.kis_client import KisClient as _KisClient  # for casts only

import pytest

from src.domain.market.models import Tick, TickSide
from src.infrastructure.kis_publisher import KisPublisher, _tick_to_wire
from src.infrastructure.redis_pubsub import CHANNEL_TICK


# ── Wire-format contract ────────────────────────────────────────────────


def test_tick_to_wire_matches_mock_publisher_shape():
    """The 5 fields and their types must match MockPublisher exactly:
    {symbol: str, ts: ISO str, price: float, volume: int, side: str}."""
    tick = Tick(
        symbol="005930",
        ts=datetime(2026, 5, 9, 4, 0, 0, tzinfo=timezone.utc),
        price=Decimal("79100.50"),
        volume=150,
        side=TickSide.BUY,
    )
    wire = _tick_to_wire(tick)
    assert set(wire.keys()) == {"symbol", "ts", "price", "volume", "side"}
    assert wire["symbol"] == "005930"
    assert wire["ts"]     == "2026-05-09T04:00:00.000+00:00"
    assert wire["price"]  == 79100.5
    assert isinstance(wire["price"], float), "frontend expects JSON number, not Decimal/string"
    assert wire["volume"] == 150
    assert isinstance(wire["volume"], int)
    assert wire["side"]   == "buy"


def test_tick_to_wire_serializes_to_json_cleanly():
    tick = Tick(
        symbol="005930",
        ts=datetime(2026, 5, 9, 4, 0, 0, tzinfo=timezone.utc),
        price=Decimal("79100"),
        volume=100,
        side=TickSide.SELL,
    )
    raw = json.dumps(_tick_to_wire(tick))
    parsed = json.loads(raw)
    assert parsed["price"] == 79100.0
    assert parsed["side"] == "sell"


# ── Lifecycle: start / stop / publish ───────────────────────────────────


def _make_tick(symbol: str = "005930", price: str = "79100", side: TickSide = TickSide.BUY) -> Tick:
    return Tick(
        symbol=symbol,
        ts=datetime(2026, 5, 9, 4, 0, 0, tzinfo=timezone.utc),
        price=Decimal(price),
        volume=100,
        side=side,
    )


class _FakeKisClient:
    """Minimal stand-in for KisClient — controls subscribe + stream_ticks.

    Also exposes the refresh-side surface (`token_expires_at`,
    `authenticate`) so `_TokenRefreshLoop` running under publisher.start()
    can introspect without AttributeError. Default expiry is 24h so the
    refresh loop never triggers in publisher-focused tests."""

    def __init__(self, ticks: list[Tick]) -> None:
        self.subscribed_with: list[str] | None = None
        self._ticks = list(ticks)
        self.token_expires_at = datetime.now(timezone.utc) + timedelta(hours=24)

    async def subscribe(self, symbols: list[str], tr_id: str = "H0STCNT0") -> None:
        self.subscribed_with = list(symbols)

    async def stream_ticks(self):
        for t in self._ticks:
            yield t

    async def authenticate(self, *, force: bool = False) -> None:
        self.token_expires_at = datetime.now(timezone.utc) + timedelta(hours=24)


def _make_redis(captured: list[tuple[str, str]]) -> Any:
    """Fake Redis whose .publish() captures (channel, payload) tuples."""
    client = MagicMock()
    async def _publish(channel, payload):  # noqa: ANN001
        captured.append((channel, payload))
    client.publish = _publish
    return client


async def test_publisher_subscribes_to_configured_symbols():
    captured: list[tuple[str, str]] = []
    fake_kis = _FakeKisClient(ticks=[])
    pub = KisPublisher(_make_redis(captured), cast(_KisClient, fake_kis), ["005930", "000660", "035420"])

    await pub.start()
    await asyncio.sleep(0)  # let the task run
    await pub.stop()

    assert fake_kis.subscribed_with == ["005930", "000660", "035420"]


async def test_publisher_publishes_each_tick_to_correct_channel():
    captured: list[tuple[str, str]] = []
    fake_kis = _FakeKisClient(ticks=[
        _make_tick("005930", "79100", TickSide.BUY),
        _make_tick("000660", "197000", TickSide.SELL),
    ])
    pub = KisPublisher(_make_redis(captured), cast(_KisClient, fake_kis), ["005930", "000660"])

    await pub.start()
    # Wait for the run loop to finish processing all scripted ticks.
    for _ in range(20):
        if pub.published_count >= 2:
            break
        await asyncio.sleep(0.01)
    await pub.stop()

    assert pub.published_count == 2
    assert len(captured) == 2
    for channel, _ in captured:
        assert channel == CHANNEL_TICK

    # Wire payload assertions — proves the frontend sees the right shape.
    payload_a = json.loads(captured[0][1])
    assert payload_a == {
        "symbol": "005930",
        "ts":     "2026-05-09T04:00:00.000+00:00",
        "price":  79100.0,
        "volume": 100,
        "side":   "buy",
    }
    payload_b = json.loads(captured[1][1])
    assert payload_b["symbol"] == "000660"
    assert payload_b["side"]   == "sell"


async def test_publisher_start_is_idempotent():
    captured: list[tuple[str, str]] = []
    fake_kis = _FakeKisClient(ticks=[])
    pub = KisPublisher(_make_redis(captured), cast(_KisClient, fake_kis), ["005930"])

    await pub.start()
    await pub.start()  # second call must be a no-op
    await pub.start()
    assert pub.is_running is True
    await pub.stop()


async def test_publisher_stop_is_safe_when_never_started():
    captured: list[tuple[str, str]] = []
    fake_kis = _FakeKisClient(ticks=[])
    pub = KisPublisher(_make_redis(captured), cast(_KisClient, fake_kis), ["005930"])
    await pub.stop()  # must not raise
    assert pub.is_running is False


# ════════════════════════════════════════════════════════════════════════
#                  Sprint 5d — _TokenRefreshLoop
# ════════════════════════════════════════════════════════════════════════
#
# Tests use very short headroom + interval (seconds, not minutes) so the
# refresh trigger fires within ~1s instead of ~30min. Token expiry is set
# to a real datetime relative to `now` — no time mocking, just compressed
# timeline.

from datetime import timedelta  # noqa: E402

from src.infrastructure.kis_client import KisError, KisAuthError  # noqa: E402
from src.infrastructure.kis_publisher import _TokenRefreshLoop  # noqa: E402


class _FakeKisClientForRefresh:
    """KisClient stand-in with controllable expiry + failable authenticate()."""

    def __init__(
        self,
        *,
        expires_in_seconds: float | None = 86400.0,
        fail_n_times: int = 0,
    ) -> None:
        self._expires_in     = expires_in_seconds
        self._fail_remaining = fail_n_times
        self.auth_calls: list[bool] = []  # records the `force` flag of each call
        self._set_expiry()

    def _set_expiry(self) -> None:
        if self._expires_in is None:
            self.token_expires_at = None
        else:
            self.token_expires_at = (
                datetime.now(timezone.utc) + timedelta(seconds=self._expires_in)
            )

    async def authenticate(self, *, force: bool = False) -> None:
        self.auth_calls.append(force)
        if self._fail_remaining > 0:
            self._fail_remaining -= 1
            raise KisAuthError("simulated refresh failure")
        # Successful refresh: re-anchor expiry to now + 24h.
        self._expires_in = 86400.0
        self._set_expiry()


async def test_refresh_triggers_when_token_within_headroom():
    """Token expires in 5s, headroom is 30s → refresh on first tick."""
    kis = _FakeKisClientForRefresh(expires_in_seconds=5.0)
    loop = _TokenRefreshLoop(cast(_KisClient, kis), headroom_seconds=30.0, interval_seconds=0.1)
    await loop.start()
    # Wait for at least one refresh tick.
    for _ in range(50):
        if loop.refresh_count >= 1:
            break
        await asyncio.sleep(0.05)
    await loop.stop()
    assert loop.refresh_count >= 1
    assert kis.auth_calls and all(force is True for force in kis.auth_calls), (
        "refresh must call authenticate(force=True) to bypass the idempotency check"
    )


async def test_refresh_skipped_when_token_is_fresh():
    """Token good for 24h, headroom is 30s → no refresh needed for many ticks."""
    kis = _FakeKisClientForRefresh(expires_in_seconds=86400.0)
    loop = _TokenRefreshLoop(cast(_KisClient, kis), headroom_seconds=30.0, interval_seconds=0.05)
    await loop.start()
    await asyncio.sleep(0.5)  # ~10 ticks
    await loop.stop()
    assert loop.refresh_count == 0
    assert kis.auth_calls == []


async def test_refresh_loop_survives_kis_auth_failures():
    """Two transient failures, then success — loop must not die after first failure."""
    kis = _FakeKisClientForRefresh(expires_in_seconds=5.0, fail_n_times=2)
    loop = _TokenRefreshLoop(cast(_KisClient, kis), headroom_seconds=30.0, interval_seconds=0.05)
    await loop.start()
    # Wait for the loop to push past the failures and land a success.
    for _ in range(100):
        if loop.refresh_count >= 1:
            break
        await asyncio.sleep(0.05)
    await loop.stop()
    assert loop.failure_count == 2
    assert loop.refresh_count >= 1, "loop must recover after transient failures"


async def test_refresh_treats_no_expiry_as_needs_refresh():
    """If we somehow lost the expiry timestamp, a refresh must be triggered."""
    kis = _FakeKisClientForRefresh(expires_in_seconds=None)
    loop = _TokenRefreshLoop(cast(_KisClient, kis), headroom_seconds=30.0, interval_seconds=0.05)
    await loop.start()
    for _ in range(50):
        if loop.refresh_count >= 1:
            break
        await asyncio.sleep(0.05)
    await loop.stop()
    assert loop.refresh_count >= 1


async def test_refresh_loop_stops_cleanly_mid_sleep():
    """stop() during the inter-tick sleep must cancel + return promptly."""
    kis = _FakeKisClientForRefresh(expires_in_seconds=86400.0)
    loop = _TokenRefreshLoop(cast(_KisClient, kis), headroom_seconds=30.0, interval_seconds=10.0)
    await loop.start()
    await asyncio.sleep(0.1)  # ensure we're INSIDE the long sleep
    # Should not hang for 10s.
    await asyncio.wait_for(loop.stop(), timeout=1.0)
    assert loop.is_running is False


async def test_publisher_exposes_refresh_metrics():
    """KisPublisher's lifetime metrics must include refresh + failure counts."""
    captured: list[tuple[str, str]] = []
    fake_kis = _FakeKisClient(ticks=[])
    pub = KisPublisher(
        _make_redis(captured), cast(_KisClient, fake_kis), ["005930"],
        refresh_headroom_seconds=30.0,
        refresh_interval_seconds=0.05,
    )
    await pub.start()
    await asyncio.sleep(0)
    assert pub.refresh_count == 0
    assert pub.refresh_failure_count == 0
    await pub.stop()


# ════════════════════════════════════════════════════════════════════════
#                  Quote DTOs and wire conversion
# ════════════════════════════════════════════════════════════════════════


def test_quote_dto_has_type_field():
    from src.api.v1.dto import QuoteDTO, QuoteLevelDTO
    dto = QuoteDTO(
        symbol="005930",
        ts="2026-05-18T09:45:23.000",
        bids=[QuoteLevelDTO(price=71900, volume=15600)],
        asks=[QuoteLevelDTO(price=72000, volume=3241)],
    )
    assert dto.type == "quote"
    wire = dto.model_dump()
    assert wire["type"] == "quote"
    assert wire["bids"][0]["price"] == 71900
    assert wire["asks"][0]["price"] == 72000


def test_quote_to_wire_converts_domain_to_dict():
    """_quote_to_wire must convert domain Quote to wire-format dict with type discriminator."""
    from src.domain.market.models import Quote, QuoteLevel
    from src.infrastructure.kis_publisher import _quote_to_wire

    quote = Quote(
        symbol="005930",
        ts=datetime(2026, 5, 18, 9, 45, 23, tzinfo=timezone.utc),
        bids=[QuoteLevel(price=71900, volume=15600), QuoteLevel(price=71850, volume=8000)],
        asks=[QuoteLevel(price=72000, volume=3241), QuoteLevel(price=72050, volume=5500)],
    )
    wire = _quote_to_wire(quote)

    assert wire["type"] == "quote"
    assert wire["symbol"] == "005930"
    assert wire["ts"] == "2026-05-18T09:45:23.000+00:00"
    assert len(wire["bids"]) == 2
    assert wire["bids"][0] == {"price": 71900, "volume": 15600}
    assert wire["bids"][1] == {"price": 71850, "volume": 8000}
    assert len(wire["asks"]) == 2
    assert wire["asks"][0] == {"price": 72000, "volume": 3241}
    assert wire["asks"][1] == {"price": 72050, "volume": 5500}

    # Verify it serializes cleanly to JSON
    json_str = json.dumps(wire)
    parsed = json.loads(json_str)
    assert parsed["type"] == "quote"
    assert parsed["bids"][0]["price"] == 71900
