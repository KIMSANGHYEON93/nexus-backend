"""Tests for TradingPipeline — end-to-end on_tick orchestration.

The pipeline is where the trading layer's surface area finally meets
the live tick stream. These tests assemble all five collaborators
(context, coordinator, guardrails, executor, portfolio) with mocks
where appropriate and prove the data flow:

    on_tick → context.record → coordinator.evaluate → guardrails.evaluate
            → executor.execute → portfolio.record_fill (on confirmed fill)
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest

from src.domain.market.models import Tick, TickSide
from src.domain.trading.agents import MockMacroAgent, MockQuantAgent
from src.domain.trading.context import TickContext
from src.domain.trading.coordinator import TradingCoordinator
from src.domain.trading.executor import OrderExecutor, OrderResult
from src.domain.trading.guardrails import (
    CoolDownGuard,
    GuardrailPipeline,
    MaxPositionSizeGuard,
)
from src.domain.trading.models import Action
from src.domain.trading.pipeline import TradingPipeline
from src.domain.trading.portfolio import Portfolio


# ── Fixtures ────────────────────────────────────────────────────────────


def _tick(symbol: str = "005930", price: str = "79000") -> Tick:
    return Tick(
        symbol=symbol,
        ts=datetime(2026, 5, 10, 4, 0, 0, tzinfo=timezone.utc),
        price=Decimal(price),
        volume=100,
        side=TickSide.BUY,
    )


class _RecordingClient:
    def __init__(self, *, success: bool = True, order_id: str | None = "ODR-1") -> None:
        self._success = success
        self._order_id = order_id
        self.calls: list[dict[str, Any]] = []

    async def place_order(self, *, symbol: str, action: Action, quantity: int) -> OrderResult:
        self.calls.append({"symbol": symbol, "action": action, "quantity": quantity})
        return OrderResult(success=self._success, order_id=self._order_id)


class _ForbidsCallsClient:
    """Same sentinel pattern as the executor tests — RAISES on any call."""

    def __init__(self) -> None:
        self.call_count = 0

    async def place_order(self, *, symbol: str, action: Action, quantity: int) -> OrderResult:
        self.call_count += 1
        raise RuntimeError("SAFETY VIOLATION: place_order called from pipeline")


def _build_pipeline(
    *,
    quant_signal:  tuple[Action, float] = (Action.HOLD, 0.0),
    macro_signal:  tuple[Action, float] = (Action.HOLD, 0.0),
    allow_live:    bool                  = False,
    max_shares:    int                   = 1000,
    cooldown_s:    float                 = 0.0,
    quantity:      int                   = 5,
    order_client:  Any                   = None,
) -> tuple[TradingPipeline, Portfolio, TickContext, Any]:
    context = TickContext()
    portfolio = Portfolio()

    coord = TradingCoordinator()
    coord.register(MockQuantAgent(script=[quant_signal]))
    coord.register(MockMacroAgent(script=[macro_signal]))

    pipeline_guards = GuardrailPipeline([
        MaxPositionSizeGuard(max_shares=max_shares),
        CoolDownGuard(cooldown_seconds=cooldown_s),
    ])

    if order_client is None:
        order_client = _RecordingClient()
    executor = OrderExecutor(
        order_client=order_client, allow_live_orders=allow_live, default_quantity=quantity,
    )

    pipeline = TradingPipeline(
        context=context, coordinator=coord, guardrails=pipeline_guards,
        executor=executor, portfolio=portfolio,
    )
    return pipeline, portfolio, context, order_client


# ── Happy path ─────────────────────────────────────────────────────────


async def test_on_tick_records_to_context():
    pipeline, _, ctx, _ = _build_pipeline()
    await pipeline.on_tick(_tick("005930", "79000"))
    assert ctx.recent_prices("005930") == [Decimal("79000")]
    assert pipeline.tick_count == 1


async def test_on_tick_runs_full_chain_to_executor():
    """Strong BUY signal + permissive guards + live mode → REST call attempted + portfolio updated."""
    client = _RecordingClient(success=True, order_id="ODR-XYZ")
    pipeline, portfolio, _, _ = _build_pipeline(
        quant_signal=(Action.BUY, 1.0), macro_signal=(Action.BUY, 1.0),
        allow_live=True, quantity=7, order_client=client,
    )
    await pipeline.on_tick(_tick("005930"))
    assert len(client.calls) == 1
    assert client.calls[0] == {"symbol": "005930", "action": Action.BUY, "quantity": 7}
    assert portfolio.position_of("005930") == 7  # fill recorded
    assert pipeline.executed_count == 1


async def test_on_tick_shadow_mode_never_reaches_client():
    """The pipeline must honor the executor's safety switch."""
    forbidder = _ForbidsCallsClient()
    pipeline, portfolio, _, _ = _build_pipeline(
        quant_signal=(Action.BUY, 1.0), macro_signal=(Action.BUY, 1.0),
        allow_live=False, order_client=forbidder,    # SHADOW MODE
    )
    await pipeline.on_tick(_tick("005930"))
    assert forbidder.call_count == 0
    assert portfolio.position_of("005930") == 0  # no fill in shadow
    assert pipeline.executed_count == 0


async def test_on_tick_blocked_signal_does_not_update_portfolio():
    """Guard blocks → executor sees HOLD → no fill → no portfolio change."""
    portfolio_seed = Portfolio()
    portfolio_seed.record_fill(
        symbol="005930", action=Action.BUY, quantity=5_000,
        ts=datetime(2026, 5, 10, 11, 0, 0, tzinfo=timezone.utc),
    )

    context = TickContext()
    coord = TradingCoordinator()
    coord.register(MockQuantAgent(script=[(Action.BUY, 1.0)]))
    coord.register(MockMacroAgent(script=[(Action.BUY, 1.0)]))
    guards = GuardrailPipeline([MaxPositionSizeGuard(max_shares=1000)])  # would block
    client = _ForbidsCallsClient()    # if executor ran, this would explode
    executor = OrderExecutor(order_client=client, allow_live_orders=True, default_quantity=1)

    pipeline = TradingPipeline(
        context=context, coordinator=coord, guardrails=guards,
        executor=executor, portfolio=portfolio_seed,
    )
    await pipeline.on_tick(_tick("005930"))
    # Position cap 1000 < held 5000 → guard blocks → HOLD → no fill → forbidder safe.
    assert client.call_count == 0
    assert portfolio_seed.position_of("005930") == 5_000  # unchanged


# ── Multi-tick flows ──────────────────────────────────────────────────


async def test_multiple_ticks_build_up_context_window():
    pipeline, _, ctx, _ = _build_pipeline()
    for p in ("100", "101", "102", "103"):
        await pipeline.on_tick(_tick("005930", p))
    assert ctx.recent_prices("005930") == [Decimal(p) for p in ("100", "101", "102", "103")]
    assert pipeline.tick_count == 4


async def test_multi_symbol_ticks_keep_per_symbol_isolation():
    pipeline, _, ctx, _ = _build_pipeline()
    await pipeline.on_tick(_tick("005930", "100"))
    await pipeline.on_tick(_tick("000660", "200"))
    await pipeline.on_tick(_tick("005930", "101"))
    assert ctx.recent_prices("005930") == [Decimal("100"), Decimal("101")]
    assert ctx.recent_prices("000660") == [Decimal("200")]


# ── Failure isolation ────────────────────────────────────────────────


async def test_executor_exception_is_swallowed_pipeline_keeps_running():
    """A bug in the executor must NOT break the publisher's tick loop."""
    class _Boom:
        async def place_order(self, **kwargs):  # noqa: ANN003, ANN201
            raise ConnectionError("network down")

    pipeline, _, _, _ = _build_pipeline(
        quant_signal=(Action.BUY, 1.0), macro_signal=(Action.BUY, 1.0),
        allow_live=True, order_client=_Boom(),
    )
    # Executor catches its own ConnectionError and returns mode=live executed=False.
    # Pipeline must complete the tick without raising.
    await pipeline.on_tick(_tick("005930"))
    assert pipeline.tick_count == 1
    # error_count stays 0 because the executor handled it gracefully.


async def test_coordinator_failure_is_caught_pipeline_continues():
    """Even if the coordinator itself raises, the publisher loop survives."""
    context = TickContext()
    portfolio = Portfolio()
    coord = AsyncMock()
    coord.evaluate = AsyncMock(side_effect=RuntimeError("coordinator boom"))
    guards = GuardrailPipeline()
    executor = OrderExecutor(
        order_client=_RecordingClient(), allow_live_orders=False, default_quantity=1,
    )
    pipeline = TradingPipeline(
        context=context, coordinator=coord, guardrails=guards,
        executor=executor, portfolio=portfolio,
    )
    await pipeline.on_tick(_tick("005930"))   # must not raise
    assert pipeline.tick_count == 1
    assert pipeline.error_count == 1


# ── Counters ──────────────────────────────────────────────────────────


async def test_counters_track_lifecycle_correctly():
    client = _RecordingClient(success=True)
    pipeline, _, _, _ = _build_pipeline(
        quant_signal=(Action.BUY, 1.0), macro_signal=(Action.BUY, 1.0),
        allow_live=True, order_client=client,
    )
    await pipeline.on_tick(_tick("005930", "100"))
    await pipeline.on_tick(_tick("005930", "101"))
    await pipeline.on_tick(_tick("005930", "102"))
    assert pipeline.tick_count == 3
    assert pipeline.executed_count == 3
    assert pipeline.error_count == 0
