"""Tests for TickContext — per-symbol rolling window."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.domain.market.models import Tick, TickSide
from src.domain.trading.context import TickContext


def _tick(symbol: str = "005930", price: str = "79000") -> Tick:
    return Tick(
        symbol=symbol,
        ts=datetime(2026, 5, 10, 4, 0, 0, tzinfo=timezone.utc),
        price=Decimal(price),
        volume=100,
        side=TickSide.BUY,
    )


def test_default_window_is_reasonable():
    ctx = TickContext()
    assert ctx.max_window >= 64


def test_record_and_recent_prices_roundtrip():
    ctx = TickContext()
    ctx.record(_tick("005930", "79000"))
    ctx.record(_tick("005930", "79100"))
    ctx.record(_tick("005930", "79200"))
    assert ctx.recent_prices("005930") == [Decimal("79000"), Decimal("79100"), Decimal("79200")]


def test_recent_prices_with_n_returns_only_tail():
    ctx = TickContext()
    for p in ("100", "101", "102", "103", "104"):
        ctx.record(_tick("005930", p))
    assert ctx.recent_prices("005930", n=3) == [Decimal("102"), Decimal("103"), Decimal("104")]


def test_recent_prices_unknown_symbol_is_empty():
    ctx = TickContext()
    assert ctx.recent_prices("UNKNOWN") == []
    assert ctx.recent_ticks("UNKNOWN") == []
    assert ctx.latest("UNKNOWN") is None


def test_window_evicts_oldest_when_full():
    ctx = TickContext(max_window=3)
    for p in ("1", "2", "3", "4", "5"):
        ctx.record(_tick("005930", p))
    assert ctx.recent_prices("005930") == [Decimal("3"), Decimal("4"), Decimal("5")]
    assert ctx.tick_count("005930") == 3


def test_per_symbol_isolation():
    """A noisy symbol must not evict the history of a quiet one."""
    ctx = TickContext(max_window=3)
    ctx.record(_tick("005930", "100"))
    for p in ("200", "201", "202", "203", "204"):
        ctx.record(_tick("000660", p))
    assert ctx.recent_prices("005930") == [Decimal("100")]
    assert ctx.recent_prices("000660") == [Decimal("202"), Decimal("203"), Decimal("204")]


def test_latest_returns_most_recent_tick():
    ctx = TickContext()
    ctx.record(_tick("005930", "100"))
    ctx.record(_tick("005930", "101"))
    latest = ctx.latest("005930")
    assert latest is not None
    assert latest.price == Decimal("101")


def test_known_symbols_lists_only_seen_symbols():
    ctx = TickContext()
    ctx.record(_tick("005930"))
    ctx.record(_tick("000660"))
    assert set(ctx.known_symbols()) == {"005930", "000660"}


def test_record_many_appends_in_order():
    ctx = TickContext()
    ctx.record_many([_tick("005930", "1"), _tick("005930", "2"), _tick("005930", "3")])
    assert ctx.recent_prices("005930") == [Decimal("1"), Decimal("2"), Decimal("3")]


def test_max_window_validation():
    with pytest.raises(ValueError):
        TickContext(max_window=1)
    with pytest.raises(ValueError):
        TickContext(max_window=0)
