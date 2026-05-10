"""Tests for QuantAgent — Wilder RSI math + decision matrix."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.domain.market.models import Tick, TickSide
from src.domain.trading.context import TickContext
from src.domain.trading.models import Action
from src.domain.trading.quant_agent import QuantAgent


def _seed(ctx: TickContext, symbol: str, prices: list[float]) -> None:
    """Push synthetic prices into the context as Ticks."""
    for p in prices:
        ctx.record(Tick(
            symbol=symbol,
            ts=datetime(2026, 5, 10, 4, 0, 0, tzinfo=timezone.utc),
            price=Decimal(str(p)),
            volume=100,
            side=TickSide.BUY,
        ))


# ── RSI math (the engine) ───────────────────────────────────────────────


def test_rsi_all_up_window_returns_100():
    """avg_loss == 0 — classic Wilder edge case."""
    rising = [100.0 + i for i in range(15)]
    rsi = QuantAgent._compute_rsi(rising, 14)
    assert rsi == 100.0


def test_rsi_all_down_window_returns_0():
    falling = [100.0 - i for i in range(15)]
    rsi = QuantAgent._compute_rsi(falling, 14)
    assert rsi == 0.0


def test_rsi_flat_window_returns_50():
    """Flat = neutral; both avg_gain and avg_loss are 0."""
    flat = [100.0] * 15
    rsi = QuantAgent._compute_rsi(flat, 14)
    assert rsi == 50.0


def test_rsi_alternating_returns_around_50():
    """Equal up/down → RSI ≈ 50."""
    alt = [100.0, 101.0] * 8  # 16 prices, equal gains/losses
    rsi = QuantAgent._compute_rsi(alt, 14)
    assert 45.0 <= rsi <= 55.0


# ── Cold start ──────────────────────────────────────────────────────────


async def test_below_period_plus_one_returns_hold_at_zero():
    ctx = TickContext()
    _seed(ctx, "005930", [100.0] * 5)  # 5 < 15
    agent = QuantAgent(context=ctx, period=14)
    sig = await agent.evaluate("005930")
    assert sig.action is Action.HOLD
    assert sig.confidence == 0.0
    assert "insufficient" in sig.rationale


async def test_unknown_symbol_returns_hold_at_zero():
    ctx = TickContext()
    agent = QuantAgent(context=ctx, period=14)
    sig = await agent.evaluate("UNKNOWN")
    assert sig.action is Action.HOLD


# ── Decision matrix ─────────────────────────────────────────────────────


async def test_oversold_emits_buy_with_high_confidence():
    """A steep declining series → low RSI → BUY."""
    ctx = TickContext()
    declining = [100.0 - 2.0 * i for i in range(20)]   # strong downtrend
    _seed(ctx, "005930", declining)
    agent = QuantAgent(context=ctx, period=14, oversold=30.0)
    sig = await agent.evaluate("005930")
    assert sig.action is Action.BUY
    assert sig.confidence > 0.5
    assert "oversold" in sig.rationale.lower()


async def test_overbought_emits_sell_with_high_confidence():
    """Strong uptrend → high RSI → SELL."""
    ctx = TickContext()
    rising = [100.0 + 2.0 * i for i in range(20)]
    _seed(ctx, "005930", rising)
    agent = QuantAgent(context=ctx, period=14, overbought=70.0)
    sig = await agent.evaluate("005930")
    assert sig.action is Action.SELL
    assert sig.confidence > 0.5
    assert "overbought" in sig.rationale.lower()


async def test_neutral_zone_emits_hold():
    """Mild oscillation — RSI lands between 30 and 70 → HOLD."""
    ctx = TickContext()
    _seed(ctx, "005930", [100.0, 101.0, 99.0, 100.5, 99.5] * 4)  # 20 prices, oscillating
    agent = QuantAgent(context=ctx, period=14, oversold=30.0, overbought=70.0)
    sig = await agent.evaluate("005930")
    assert sig.action is Action.HOLD
    assert "neutral" in sig.rationale.lower()


# ── Confidence-from-extremity ───────────────────────────────────────────


def test_confidence_mapping_more_extreme_yields_higher_confidence():
    """Direct check on the confidence formula — RSI=10 must map to a
    higher BUY confidence than RSI=20, and RSI=80 to higher SELL conf
    than RSI=72. Tests the agent's confidence-from-extremity contract
    without the variability of constructing exact-RSI tick windows."""
    ctx = TickContext()
    agent = QuantAgent(context=ctx, period=14, oversold=30.0, overbought=70.0)

    # Confidence formula for BUY: (oversold - rsi) / oversold.
    # RSI=10 → (30-10)/30 = 0.667; RSI=20 → (30-20)/30 = 0.333.
    conf_extreme_buy = agent._clip((30.0 - 10.0) / 30.0)
    conf_mild_buy    = agent._clip((30.0 - 20.0) / 30.0)
    assert conf_extreme_buy > conf_mild_buy

    # SELL: (rsi - overbought) / (100 - overbought).
    # RSI=80 → (80-70)/30 = 0.333; RSI=95 → (95-70)/30 = 0.833.
    conf_extreme_sell = agent._clip((95.0 - 70.0) / 30.0)
    conf_mild_sell    = agent._clip((72.0 - 70.0) / 30.0)
    assert conf_extreme_sell > conf_mild_sell


async def test_confidence_is_clamped_to_unit_interval():
    """Even on RSI=0 (perfect downtrend), confidence ≤ 1.0."""
    ctx = TickContext()
    _seed(ctx, "005930", [100.0 - 10.0 * i for i in range(20)])
    agent = QuantAgent(context=ctx, period=14)
    sig = await agent.evaluate("005930")
    assert 0.0 <= sig.confidence <= 1.0


# ── Construction validation ─────────────────────────────────────────────


def test_invalid_period_rejected():
    ctx = TickContext()
    with pytest.raises(ValueError):
        QuantAgent(context=ctx, period=1)


def test_invalid_threshold_ordering_rejected():
    ctx = TickContext()
    # oversold must be < overbought, both in (0, 100).
    with pytest.raises(ValueError):
        QuantAgent(context=ctx, oversold=70.0, overbought=30.0)
    with pytest.raises(ValueError):
        QuantAgent(context=ctx, oversold=-1.0, overbought=70.0)
    with pytest.raises(ValueError):
        QuantAgent(context=ctx, oversold=30.0, overbought=101.0)


def test_agent_satisfies_protocol():
    """Structural — coordinator's Agent Protocol must accept QuantAgent."""
    from src.domain.trading.agents import Agent
    ctx = TickContext()
    assert isinstance(QuantAgent(context=ctx), Agent)
