"""Tests for src/infrastructure/us_publisher.py.

Covers the contract the WebSocket layer + persistence worker rely on:
  • Wire payload byte-shape matches Kis/MockPublisher (interop invariant).
  • Volume delta is correctly computed across polls (day-rollover safe).
  • Side derivation follows derived change_percent sign.
  • Empty/error envelopes from Yahoo (rate-limit signal, unknown symbol)
    are skipped silently rather than poisoning the batch.
  • Transport errors on one symbol don't kill the batch.
  • Round-robin cursor rotates through the full universe.
  • start / stop are idempotent and don't leak background tasks.

Uses httpx.MockTransport to fake Yahoo Finance — no network, no rate
limit concerns. Mirrors the fake-client posture in test_mock_publisher.py
for Redis.

Skips on hosts without redis-py installed (Windows dev box); runs
end-to-end on CI / Docker.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

pytest.importorskip("redis", reason="full backend deps required")

from src.infrastructure.us_publisher import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_POLL_INTERVAL_S,
    DEFAULT_US_SYMBOLS,
    UsPublisher,
)


def _fake_client() -> MagicMock:
    client = MagicMock()
    client.publish = AsyncMock(return_value=1)
    return client


def _quote_payload(price: float, change_percent: float, volume: int) -> dict[str, Any]:
    """Build a Yahoo `/v8/finance/chart` response envelope.

    We derive `previousClose` from `price` + `change_percent` so the
    publisher's parser computes back to the requested change_percent
    (within float rounding). The shape mirrors what Yahoo actually
    returns — only the fields the publisher reads are populated.
    """
    # price = prev × (1 + pct/100) → prev = price / (1 + pct/100)
    prev = price / (1.0 + change_percent / 100.0) if change_percent != 0 else price
    return {
        "chart": {
            "error": None,
            "result": [{
                "meta": {
                    "symbol":               "TEST",
                    "regularMarketPrice":   price,
                    "previousClose":        prev,
                    "regularMarketVolume":  volume,
                },
            }],
        }
    }


def _mock_transport(symbol_to_payload: dict[str, dict[str, Any]]) -> httpx.MockTransport:
    """Build a MockTransport that routes /chart/X to the matching payload.
    Unknown symbols return a Yahoo-style `{chart: {error: ...}}`
    envelope (mimicking the rate-limit / unknown-symbol response).
    """

    def handler(request: httpx.Request) -> httpx.Response:
        # Yahoo URL shape: /v8/finance/chart/AAPL?interval=1m&range=1d
        symbol = request.url.path.rsplit("/", 1)[-1]
        body = symbol_to_payload.get(
            symbol,
            {"chart": {"error": {"code": "Not Found"}, "result": None}},
        )
        return httpx.Response(200, json=body)

    return httpx.MockTransport(handler)


def _make_publisher(
    client: MagicMock,
    *,
    symbols:     list[str] | None = None,
    transport:   httpx.MockTransport | None = None,
    poll_interval_s: float = 0.05,
    batch_size:  int = 4,
) -> UsPublisher:
    """Construct UsPublisher with an injected httpx.AsyncClient using the
    given mock transport. Default symbols = small 4-ticker universe so
    tests stay fast.
    """
    syms = symbols if symbols is not None else ["AAPL", "MSFT", "NVDA", "TSLA"]
    transport = transport or _mock_transport({})
    http_client = httpx.AsyncClient(transport=transport)
    return UsPublisher(
        client,
        syms,
        poll_interval_s = poll_interval_s,
        batch_size      = batch_size,
        http_client     = http_client,
    )


# ──────────────────────────────────────────────────────────────────────────
#  Wire payload
# ──────────────────────────────────────────────────────────────────────────

def _meta(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract the Yahoo `meta` dict from a `_quote_payload(...)` envelope.
    Convenience for the per-method unit tests that pump the inner shape
    straight into `_quote_to_tick`.
    """
    return payload["chart"]["result"][0]["meta"]


def test_quote_to_tick_has_required_fields():
    pub = _make_publisher(_fake_client())
    tick = pub._quote_to_tick(
        "AAPL", _meta(_quote_payload(price=170.0, change_percent=1.2, volume=1_000_000)),
    )
    assert tick is not None
    assert set(tick.keys()) == {"symbol", "ts", "price", "volume", "side"}


def test_quote_to_tick_field_types():
    pub = _make_publisher(_fake_client())
    tick = pub._quote_to_tick(
        "AAPL", _meta(_quote_payload(price=170.0, change_percent=1.2, volume=1_000_000)),
    )
    assert tick is not None
    assert isinstance(tick["symbol"], str)
    assert isinstance(tick["ts"],     str)
    assert isinstance(tick["price"],  float)
    assert isinstance(tick["volume"], int)
    assert tick["side"] in ("buy", "sell")


def test_side_follows_change_percent_sign():
    pub = _make_publisher(_fake_client())
    up = pub._quote_to_tick("X", _meta(_quote_payload(100, 0.5, 1)))
    down = pub._quote_to_tick("Y", _meta(_quote_payload(100, -0.5, 1)))
    assert up is not None and up["side"] == "buy"
    assert down is not None and down["side"] == "sell"


def test_zero_change_is_buy_side():
    """Flat change_percent counts as buy (>=0) — documented convention."""
    pub = _make_publisher(_fake_client())
    tick = pub._quote_to_tick("X", _meta(_quote_payload(100, 0.0, 1)))
    assert tick is not None and tick["side"] == "buy"


def test_quote_to_tick_rejects_invalid_price():
    pub = _make_publisher(_fake_client())
    # Negative price → reject
    bad = pub._quote_to_tick("X", {"regularMarketPrice": -10, "regularMarketVolume": 1})
    assert bad is None
    # Missing price AND previousClose → reject
    bad2 = pub._quote_to_tick("X", {"regularMarketVolume": 1})
    assert bad2 is None
    # Non-numeric price → reject
    bad3 = pub._quote_to_tick("X", {"regularMarketPrice": "abc", "regularMarketVolume": 1})
    assert bad3 is None


def test_quote_to_tick_falls_back_to_previous_close():
    """Pre-market sometimes only has `previousClose`. Publisher should
    use that as the price rather than dropping the row."""
    pub = _make_publisher(_fake_client())
    tick = pub._quote_to_tick(
        "X", {"previousClose": 287.0, "regularMarketVolume": 100},
    )
    assert tick is not None
    assert tick["price"] == 287.0


def test_quote_to_tick_handles_missing_previous_close():
    """No previousClose → change_percent collapses to 0, side=buy.
    Shouldn't crash."""
    pub = _make_publisher(_fake_client())
    tick = pub._quote_to_tick(
        "X", {"regularMarketPrice": 100, "regularMarketVolume": 1},
    )
    assert tick is not None
    assert tick["side"] == "buy"


# ──────────────────────────────────────────────────────────────────────────
#  Volume delta tracking
# ──────────────────────────────────────────────────────────────────────────

def test_first_observation_yields_zero_volume():
    """First time we see a symbol, we have no baseline → emit 0."""
    pub = _make_publisher(_fake_client())
    tick = pub._quote_to_tick("AAPL", _meta(_quote_payload(170, 1.0, 1_000_000)))
    assert tick is not None
    assert tick["volume"] == 0


def test_volume_delta_across_polls():
    pub = _make_publisher(_fake_client())
    pub._quote_to_tick("AAPL", _meta(_quote_payload(170, 1.0, 1_000_000)))
    second = pub._quote_to_tick("AAPL", _meta(_quote_payload(171, 1.5, 1_050_000)))
    assert second is not None
    assert second["volume"] == 50_000


def test_day_rollover_re_baselines_silently():
    """When day-cumulative volume drops (NYSE midnight reset), we shouldn't
    publish a negative delta. Re-baseline to the new value, emit 0."""
    pub = _make_publisher(_fake_client())
    pub._quote_to_tick("AAPL", _meta(_quote_payload(170, 1.0, 50_000_000)))
    after_rollover = pub._quote_to_tick(
        "AAPL", _meta(_quote_payload(170, 0.0, 100_000)),
    )
    assert after_rollover is not None
    assert after_rollover["volume"] == 0
    # Subsequent poll measures delta against the new baseline, not the stale one
    next_tick = pub._quote_to_tick(
        "AAPL", _meta(_quote_payload(170, 0.0, 200_000)),
    )
    assert next_tick is not None
    assert next_tick["volume"] == 100_000


def test_volume_tracked_per_symbol_independently():
    pub = _make_publisher(_fake_client())
    pub._quote_to_tick("AAPL", _meta(_quote_payload(170, 1.0, 1_000_000)))
    pub._quote_to_tick("MSFT", _meta(_quote_payload(340, 1.0, 500_000)))
    aapl2 = pub._quote_to_tick("AAPL", _meta(_quote_payload(170, 1.0, 1_100_000)))
    msft2 = pub._quote_to_tick("MSFT", _meta(_quote_payload(340, 1.0, 600_000)))
    assert aapl2 is not None and aapl2["volume"] == 100_000
    assert msft2 is not None and msft2["volume"] == 100_000


# ──────────────────────────────────────────────────────────────────────────
#  Constructor validation
# ──────────────────────────────────────────────────────────────────────────

def test_constructor_rejects_empty_symbol_list():
    with pytest.raises(ValueError, match="at least one symbol"):
        UsPublisher(_fake_client(), [])


def test_constructor_rejects_zero_batch_size():
    with pytest.raises(ValueError, match="batch_size"):
        UsPublisher(_fake_client(), ["AAPL"], batch_size=0)


def test_constructor_rejects_zero_poll_interval():
    with pytest.raises(ValueError, match="poll_interval"):
        UsPublisher(_fake_client(), ["AAPL"], poll_interval_s=0)


# ──────────────────────────────────────────────────────────────────────────
#  Default universe + constants
# ──────────────────────────────────────────────────────────────────────────

def test_default_universe_matches_frontend_momentum_count():
    """28 tickers — must stay in sync with MomentumStreamer.MOMENTUM_UNIVERSE
    on the frontend or backend ticks will hit entities the frontend hasn't
    registered (silent drop, the 5n bug repeating)."""
    assert len(DEFAULT_US_SYMBOLS) == 28


def test_default_universe_contains_no_duplicates():
    assert len(DEFAULT_US_SYMBOLS) == len(set(DEFAULT_US_SYMBOLS))


def test_default_constants_pinned():
    """Pin the calibration so changes are explicit."""
    assert DEFAULT_BATCH_SIZE == 4
    assert DEFAULT_POLL_INTERVAL_S == 30.0


# ──────────────────────────────────────────────────────────────────────────
#  Lifecycle / round-robin polling
# ──────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_start_publishes_to_correct_channel():
    from src.infrastructure.redis_pubsub import CHANNEL_TICK

    client = _fake_client()
    transport = _mock_transport({
        "AAPL": _quote_payload(170, 1.0, 1_000_000),
        "MSFT": _quote_payload(340, 0.5, 500_000),
        "NVDA": _quote_payload(850, 2.0, 800_000),
        "TSLA": _quote_payload(250, -1.0, 400_000),
    })
    pub = _make_publisher(client, transport=transport)
    await pub.start()
    # One poll cycle is enough — interval is 50ms, batch is 4.
    await asyncio.sleep(0.15)
    await pub.stop()

    assert client.publish.await_count >= 1
    channel, payload = client.publish.await_args_list[0].args
    assert channel == CHANNEL_TICK
    parsed = json.loads(payload)
    assert parsed["symbol"] in {"AAPL", "MSFT", "NVDA", "TSLA"}
    assert isinstance(parsed["price"], (int, float))


@pytest.mark.asyncio
async def test_start_is_idempotent():
    pub = _make_publisher(_fake_client())
    await pub.start()
    task_first = pub._task
    await pub.start()
    assert pub._task is task_first
    await pub.stop()


@pytest.mark.asyncio
async def test_stop_without_start_is_safe():
    pub = _make_publisher(_fake_client())
    await pub.stop()  # no-op, no exception


@pytest.mark.asyncio
async def test_stop_cancels_inner_task_cleanly():
    pub = _make_publisher(_fake_client())
    await pub.start()
    inner = pub._task
    assert inner is not None
    await pub.stop()
    assert inner.done()
    assert pub._task is None


@pytest.mark.asyncio
async def test_round_robin_cursor_rotates_through_universe():
    """Across enough polls, every symbol should be hit at least once."""
    client = _fake_client()
    payloads = {
        s: _quote_payload(100, 1.0, 1_000_000)
        for s in ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META"]
    }
    transport = _mock_transport(payloads)
    pub = _make_publisher(
        client,
        symbols    = list(payloads.keys()),
        transport  = transport,
        batch_size = 2,
    )
    await pub.start()
    # 6 symbols / 2 per batch = 3 batches; 0.05 × 4 = plenty of cycles.
    await asyncio.sleep(0.25)
    await pub.stop()

    seen_symbols: set[str] = set()
    for call in client.publish.await_args_list:
        _, payload = call.args
        seen_symbols.add(json.loads(payload)["symbol"])
    assert seen_symbols == set(payloads.keys())


# ──────────────────────────────────────────────────────────────────────────
#  Resilience
# ──────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_empty_global_quote_envelopes_are_skipped_silently():
    """Proxy returns `{"Global Quote": {}}` when rate-limited upstream.
    Those symbols should be skipped without poisoning the batch — the
    other symbols in the same batch should still publish."""
    client = _fake_client()
    transport = _mock_transport({
        "AAPL": _quote_payload(170, 1.0, 1_000_000),
        # MSFT, NVDA, TSLA → default {} envelope (rate-limited)
    })
    pub = _make_publisher(client, transport=transport)
    await pub.start()
    await asyncio.sleep(0.15)
    await pub.stop()

    # Only AAPL published — MSFT/NVDA/TSLA got empty envelopes.
    for call in client.publish.await_args_list:
        _, payload = call.args
        assert json.loads(payload)["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_transport_error_on_one_symbol_doesnt_kill_batch():
    """If httpx raises mid-batch (timeout, network blip), the remaining
    symbols in the batch should still get a chance to publish."""
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        symbol = request.url.params.get("symbol", "")
        if symbol == "MSFT":
            raise httpx.ConnectError("simulated network blip")
        return httpx.Response(200, json=_quote_payload(170, 1.0, 1_000_000))

    client = _fake_client()
    transport = httpx.MockTransport(handler)
    pub = _make_publisher(client, transport=transport)
    await pub.start()
    await asyncio.sleep(0.15)
    await pub.stop()

    # AAPL, NVDA, TSLA all published; MSFT errored but didn't abort the batch.
    published_symbols = [
        json.loads(call.args[1])["symbol"] for call in client.publish.await_args_list
    ]
    assert "AAPL" in published_symbols
    assert "NVDA" in published_symbols
    assert "TSLA" in published_symbols
    assert "MSFT" not in published_symbols
    assert pub.failure_count >= 1


@pytest.mark.asyncio
async def test_on_tick_observer_errors_dont_kill_publisher():
    """If the trading-pipeline observer raises, the publisher should
    keep producing ticks. Exact contract of mock_publisher."""
    client = _fake_client()
    transport = _mock_transport({
        "AAPL": _quote_payload(170, 1.0, 1_000_000),
    })

    async def angry_observer(tick: Any) -> None:
        raise RuntimeError("observer crashed")

    http_client = httpx.AsyncClient(transport=transport)
    pub = UsPublisher(
        client,
        ["AAPL"],
        poll_interval_s = 0.05,
        batch_size      = 1,
        http_client     = http_client,
        on_tick         = angry_observer,
    )
    await pub.start()
    await asyncio.sleep(0.15)
    await pub.stop()

    # Publishes kept happening despite the observer raising every time.
    assert client.publish.await_count >= 2
