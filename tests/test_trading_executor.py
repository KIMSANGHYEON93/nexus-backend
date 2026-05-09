"""Tests for OrderExecutor — Sprint 5g safety-switch enforcement.

The HEADLINE test in this file is `test_shadow_mode_NEVER_calls_order_client_*`
— it pins the user's explicit Sprint 5g requirement: when
ALLOW_LIVE_ORDERS=false, the order-client method MUST NOT be called,
regardless of how confident the upstream signal is. The mock client
fails the test loudly (RuntimeError) the moment it's invoked, so a future
refactor that accidentally bypasses the safety check will fail this test
on the first signal it processes. There is no way to "almost" pass these
tests.

Two layers of coverage:
  • Behavior matrix — HOLD/blocked/BUY/SELL × shadow/live × success/error
  • Audit log shape — `executor.shadow_trade` and `executor.live_order_*`
    structured fields, since these will feed the live-trading dashboard.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pytest

from src.domain.trading.executor import (
    ExecutionResult,
    OrderClient,
    OrderExecutor,
    OrderResult,
)
from src.domain.trading.guardrails import GuardedSignal
from src.domain.trading.models import Action, AgentSignal, TradeSignal


# ── Fixtures / helpers ─────────────────────────────────────────────────


def _signal(symbol: str = "005930", action: Action = Action.BUY, score: float = 0.9) -> TradeSignal:
    return TradeSignal(
        symbol=symbol, action=action, confidence=abs(score), score=score,
        contributors=[
            AgentSignal(
                agent_id="test", symbol=symbol, action=action,
                confidence=abs(score), ts=datetime.now(timezone.utc),
            ),
        ],
        ts=datetime.now(timezone.utc),
    )


def _allowed(signal: TradeSignal) -> GuardedSignal:
    return GuardedSignal(signal=signal, allowed=True)


def _blocked(signal: TradeSignal, reason: str = "test_block") -> GuardedSignal:
    return GuardedSignal(signal=signal, allowed=False, blocked_by=reason, reason=reason)


class _ForbidsCallsClient:
    """OrderClient that EXPLODES if anyone calls it.

    Used in shadow-mode tests to make any accidental client invocation
    impossible to ignore — the test fails with a RuntimeError naming the
    safety bug, not a quiet assertion miss buried five lines down.
    """

    def __init__(self) -> None:
        self.call_count = 0
        self.calls: list[dict[str, Any]] = []

    async def place_order(self, *, symbol: str, action: Action, quantity: int) -> OrderResult:
        self.call_count += 1
        self.calls.append({"symbol": symbol, "action": action, "quantity": quantity})
        raise RuntimeError(
            "SAFETY VIOLATION: place_order was called while ALLOW_LIVE_ORDERS=false. "
            f"symbol={symbol} action={action.value} quantity={quantity}"
        )


class _RecordingClient:
    """OrderClient stand-in used in live-mode tests. Records every call
    + returns a configurable success/failure response."""

    def __init__(self, *, success: bool = True, order_id: str | None = "ODR-1") -> None:
        self._success  = success
        self._order_id = order_id
        self.calls: list[dict[str, Any]] = []

    async def place_order(self, *, symbol: str, action: Action, quantity: int) -> OrderResult:
        self.calls.append({"symbol": symbol, "action": action, "quantity": quantity})
        return OrderResult(
            success=self._success,
            order_id=self._order_id if self._success else None,
            message="ok" if self._success else "rt_cd=1 잔고부족",
        )


# ════════════════════════════════════════════════════════════════════════
#       THE HEADLINE TESTS — shadow mode must NEVER call the client
# ════════════════════════════════════════════════════════════════════════


async def test_shadow_mode_NEVER_calls_order_client_on_max_confidence_buy():
    """ALLOW_LIVE_ORDERS=false. Even on the strongest possible BUY signal
    (score=+1.0), `place_order()` MUST NOT be invoked. If this test ever
    fails, a real-money safety violation is in flight."""
    forbidder = _ForbidsCallsClient()
    executor = OrderExecutor(
        order_client=forbidder,
        allow_live_orders=False,           # SHADOW MODE
        default_quantity=10,
    )
    result = await executor.execute(_allowed(_signal(action=Action.BUY, score=1.0)))

    assert forbidder.call_count == 0, (
        "place_order was called in shadow mode — SAFETY VIOLATION"
    )
    assert result.mode == "shadow"
    assert result.executed is False
    assert result.intended_action is Action.BUY
    assert result.intended_quantity == 10
    assert "ALLOW_LIVE_ORDERS=false" in (result.reason or "")


async def test_shadow_mode_NEVER_calls_order_client_on_max_confidence_sell():
    """Same as above but for SELL — neither direction may bypass the switch."""
    forbidder = _ForbidsCallsClient()
    executor = OrderExecutor(
        order_client=forbidder, allow_live_orders=False, default_quantity=5,
    )
    result = await executor.execute(_allowed(_signal(action=Action.SELL, score=-1.0)))
    assert forbidder.call_count == 0
    assert result.mode == "shadow"
    assert result.intended_action is Action.SELL


async def test_shadow_mode_NEVER_calls_order_client_across_a_storm_of_signals():
    """Throw 50 mixed BUY/SELL signals through. Not a single one may
    reach the client. Catches subtler failure modes than the single-shot
    test (e.g. counter mishandling, race in some future async refactor)."""
    forbidder = _ForbidsCallsClient()
    executor = OrderExecutor(
        order_client=forbidder, allow_live_orders=False, default_quantity=1,
    )
    for i in range(25):
        await executor.execute(_allowed(_signal(action=Action.BUY,  score=0.9)))
        await executor.execute(_allowed(_signal(action=Action.SELL, score=-0.9)))
    assert forbidder.call_count == 0


# ── HOLD / blocked → noop, regardless of switch ────────────────────────


@pytest.mark.parametrize("allow_live_orders", [True, False])
async def test_hold_signal_is_noop_regardless_of_switch(allow_live_orders: bool) -> None:
    forbidder = _ForbidsCallsClient()
    executor = OrderExecutor(
        order_client=forbidder, allow_live_orders=allow_live_orders, default_quantity=1,
    )
    result = await executor.execute(_allowed(_signal(action=Action.HOLD, score=0.0)))
    assert result.mode == "noop"
    assert result.executed is False
    assert forbidder.call_count == 0


@pytest.mark.parametrize("allow_live_orders", [True, False])
async def test_guard_blocked_signal_is_noop_regardless_of_switch(allow_live_orders: bool) -> None:
    """Pipeline-blocked signals come through with effective_signal.action=HOLD —
    must not execute even when the switch is on."""
    forbidder = _ForbidsCallsClient()
    executor = OrderExecutor(
        order_client=forbidder, allow_live_orders=allow_live_orders, default_quantity=1,
    )
    blocked = _blocked(_signal(action=Action.BUY, score=0.95), reason="cooldown")
    result = await executor.execute(blocked)
    assert result.mode == "noop"
    assert result.executed is False
    assert result.reason == "cooldown"
    assert forbidder.call_count == 0


# ── Live mode — order client IS called, with the right params ──────────


async def test_live_mode_invokes_order_client_with_correct_params():
    client = _RecordingClient(success=True, order_id="ODR-42")
    executor = OrderExecutor(
        order_client=client, allow_live_orders=True, default_quantity=7,
    )
    result = await executor.execute(_allowed(_signal(action=Action.BUY, score=0.6)))

    assert client.calls == [{"symbol": "005930", "action": Action.BUY, "quantity": 7}]
    assert result.mode == "live"
    assert result.executed is True
    assert result.order_id == "ODR-42"


async def test_live_mode_business_rejection_marks_executed_false_no_crash():
    """Broker returned success=False (e.g. insufficient balance). Executor
    surfaces this as `executed=False` + reason — no exception bubbles."""
    client = _RecordingClient(success=False)
    executor = OrderExecutor(
        order_client=client, allow_live_orders=True, default_quantity=1,
    )
    result = await executor.execute(_allowed(_signal(action=Action.BUY)))
    assert result.mode == "live"
    assert result.executed is False
    assert result.order_id is None
    assert "잔고부족" in (result.reason or "")


async def test_live_mode_client_raises_is_caught_and_returned_as_error_result():
    class _Boom:
        async def place_order(self, *, symbol, action, quantity):  # noqa: ANN001, ANN201
            raise ConnectionError("network down")

    executor = OrderExecutor(
        order_client=_Boom(), allow_live_orders=True, default_quantity=1,
    )
    result = await executor.execute(_allowed(_signal(action=Action.BUY)))
    assert result.mode == "live"
    assert result.executed is False
    assert "ConnectionError" in (result.reason or "")
    assert "network down" in (result.reason or "")


# ── Construction validation ────────────────────────────────────────────


def test_executor_rejects_zero_or_negative_default_quantity():
    with pytest.raises(ValueError):
        OrderExecutor(order_client=_RecordingClient(), allow_live_orders=False, default_quantity=0)
    with pytest.raises(ValueError):
        OrderExecutor(order_client=_RecordingClient(), allow_live_orders=False, default_quantity=-5)


def test_allow_live_orders_is_captured_at_construction():
    """Sprint 5g design choice: the flag is fixed for the executor's
    lifetime. Restart the process to change stance."""
    executor = OrderExecutor(
        order_client=_RecordingClient(), allow_live_orders=True, default_quantity=1,
    )
    assert executor.allow_live_orders is True


# ── Audit log shape ────────────────────────────────────────────────────


async def test_shadow_trade_log_carries_full_audit_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Live-trading dashboard will subscribe to `executor.shadow_trade` —
    these structured fields are the contract."""
    caplog.set_level(logging.WARNING, logger="src.domain.trading.executor")
    executor = OrderExecutor(
        order_client=_ForbidsCallsClient(),
        allow_live_orders=False, default_quantity=3,
    )
    await executor.execute(_allowed(_signal(action=Action.BUY, score=0.85)))

    shadow = [r for r in caplog.records if r.message == "executor.shadow_trade"]
    assert len(shadow) == 1
    rec = shadow[0]
    assert getattr(rec, "symbol")            == "005930"
    assert getattr(rec, "intended_action")   == "buy"
    assert getattr(rec, "intended_quantity") == 3
    assert getattr(rec, "intended_score")    == pytest.approx(0.85)
    assert getattr(rec, "live_orders")       is False


async def test_live_filled_log_carries_order_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="src.domain.trading.executor")
    client = _RecordingClient(success=True, order_id="ODR-99")
    executor = OrderExecutor(order_client=client, allow_live_orders=True, default_quantity=2)
    await executor.execute(_allowed(_signal(action=Action.BUY)))

    fills = [r for r in caplog.records if r.message == "executor.live_order_filled"]
    assert len(fills) == 1
    assert getattr(fills[0], "order_id") == "ODR-99"
    assert getattr(fills[0], "quantity") == 2


# ── Protocol satisfaction ──────────────────────────────────────────────


def test_recording_client_satisfies_order_client_protocol():
    assert isinstance(_RecordingClient(), OrderClient)


def test_forbids_calls_client_satisfies_order_client_protocol():
    """Even the test sentinel honors the contract — proves there's no
    secret way to slip a non-conforming client past the executor."""
    assert isinstance(_ForbidsCallsClient(), OrderClient)
