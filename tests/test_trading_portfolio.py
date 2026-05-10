"""Tests for Portfolio — in-memory positions + last_trade_at tracker."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.domain.trading.models import Action
from src.domain.trading.portfolio import Portfolio


def _ts(hour: int = 12, minute: int = 0) -> datetime:
    return datetime(2026, 5, 10, hour, minute, 0, tzinfo=timezone.utc)


def test_fresh_portfolio_is_empty():
    p = Portfolio()
    assert p.positions == {}
    assert p.last_trade_at == {}
    assert p.position_of("005930") == 0


def test_buy_increments_position_and_records_timestamp():
    p = Portfolio()
    p.record_fill(symbol="005930", action=Action.BUY, quantity=10, ts=_ts(12, 0))
    assert p.position_of("005930") == 10
    assert p.last_trade_at["005930"] == _ts(12, 0)


def test_multiple_buys_accumulate():
    p = Portfolio()
    p.record_fill(symbol="005930", action=Action.BUY, quantity=10, ts=_ts(12, 0))
    p.record_fill(symbol="005930", action=Action.BUY, quantity=15, ts=_ts(12, 1))
    assert p.position_of("005930") == 25


def test_sell_decrements_position():
    p = Portfolio()
    p.record_fill(symbol="005930", action=Action.BUY,  quantity=20, ts=_ts(12, 0))
    p.record_fill(symbol="005930", action=Action.SELL, quantity=8,  ts=_ts(12, 1))
    assert p.position_of("005930") == 12


def test_sell_clamps_at_zero_long_only():
    """Long-only — selling more than held doesn't go negative."""
    p = Portfolio()
    p.record_fill(symbol="005930", action=Action.BUY,  quantity=5,  ts=_ts(12, 0))
    p.record_fill(symbol="005930", action=Action.SELL, quantity=99, ts=_ts(12, 1))
    assert p.position_of("005930") == 0


def test_sell_with_no_position_clamps_at_zero():
    p = Portfolio()
    p.record_fill(symbol="005930", action=Action.SELL, quantity=10, ts=_ts(12, 0))
    assert p.position_of("005930") == 0
    # last_trade_at IS recorded — the sell attempt happened.
    assert p.last_trade_at["005930"] == _ts(12, 0)


def test_per_symbol_isolation():
    p = Portfolio()
    p.record_fill(symbol="005930", action=Action.BUY, quantity=10, ts=_ts(12, 0))
    p.record_fill(symbol="000660", action=Action.BUY, quantity=20, ts=_ts(12, 1))
    assert p.position_of("005930") == 10
    assert p.position_of("000660") == 20


def test_positions_property_returns_snapshot_not_live_dict():
    """Mutating the returned dict must not affect the portfolio."""
    p = Portfolio()
    p.record_fill(symbol="005930", action=Action.BUY, quantity=10, ts=_ts(12, 0))
    snapshot = p.positions
    snapshot["005930"] = 99999  # type: ignore[index]
    assert p.position_of("005930") == 10


def test_last_trade_at_property_returns_snapshot_not_live_dict():
    p = Portfolio()
    p.record_fill(symbol="005930", action=Action.BUY, quantity=10, ts=_ts(12, 0))
    snapshot = p.last_trade_at
    snapshot["005930"] = _ts(23, 59)  # type: ignore[index]
    assert p.last_trade_at["005930"] == _ts(12, 0)


def test_record_fill_rejects_hold():
    p = Portfolio()
    with pytest.raises(ValueError, match="HOLD"):
        p.record_fill(symbol="005930", action=Action.HOLD, quantity=1, ts=_ts())


def test_record_fill_rejects_zero_or_negative_quantity():
    p = Portfolio()
    with pytest.raises(ValueError):
        p.record_fill(symbol="005930", action=Action.BUY, quantity=0, ts=_ts())
    with pytest.raises(ValueError):
        p.record_fill(symbol="005930", action=Action.SELL, quantity=-1, ts=_ts())
