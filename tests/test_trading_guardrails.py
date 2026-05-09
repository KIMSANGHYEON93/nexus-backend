"""Tests for the Sprint 5f guardrail pipeline.

Three layers of coverage:
  1. Each guard in isolation — happy paths and rejection paths, with
     exact reason-string assertions so future log scrapers don't quietly
     drift away from what dashboards expect.
  2. Pipeline behavior — fail-fast ordering, HOLD short-circuit,
     pass-through when nothing trips.
  3. Audit log shape — Sprint 5g executor will subscribe to
     `guardrail.blocked` for shadow trading; the structured fields
     it depends on are pinned here.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from src.domain.trading.guardrails import (
    CoolDownGuard,
    GuardContext,
    GuardDecision,
    GuardedSignal,
    GuardrailPipeline,
    MaxPositionSizeGuard,
    OrderGuardrail,
    VolatilityCircuitBreaker,
)
from src.domain.trading.models import Action, AgentSignal, TradeSignal


# ── Helpers ─────────────────────────────────────────────────────────────


def _signal(symbol: str = "005930", action: Action = Action.BUY, score: float = 0.5) -> TradeSignal:
    return TradeSignal(
        symbol=symbol,
        action=action,
        confidence=abs(score),
        score=score,
        contributors=[
            AgentSignal(
                agent_id="test", symbol=symbol, action=action,
                confidence=abs(score), ts=datetime.now(timezone.utc),
            ),
        ],
        ts=datetime.now(timezone.utc),
    )


def _ctx(
    *,
    positions: dict[str, int]                            | None = None,
    last_trade_at: dict[str, datetime]                    | None = None,
    recent_prices: dict[str, list[Decimal]]               | None = None,
    now: datetime                                          | None = None,
) -> GuardContext:
    return GuardContext(
        positions     = positions     or {},
        last_trade_at = last_trade_at or {},
        recent_prices = recent_prices or {},
        now           = now or datetime.now(timezone.utc),
    )


# ── GuardDecision validation ────────────────────────────────────────────


def test_blocked_decision_requires_reason():
    with pytest.raises(ValueError):
        GuardDecision.block("")


def test_allow_decision_has_no_reason():
    d = GuardDecision.allow()
    assert d.allowed is True
    assert d.reason == ""


# ── MaxPositionSizeGuard ────────────────────────────────────────────────


async def test_max_position_blocks_buy_when_at_limit():
    guard = MaxPositionSizeGuard(max_shares=100)
    decision = await guard.check(
        _signal("005930", Action.BUY),
        _ctx(positions={"005930": 100}),
    )
    assert decision.allowed is False
    assert "100" in decision.reason
    assert "005930" in decision.reason


async def test_max_position_blocks_buy_when_over_limit():
    guard = MaxPositionSizeGuard(max_shares=100)
    decision = await guard.check(
        _signal("005930", Action.BUY),
        _ctx(positions={"005930": 250}),
    )
    assert decision.allowed is False


async def test_max_position_allows_buy_under_limit():
    guard = MaxPositionSizeGuard(max_shares=100)
    decision = await guard.check(
        _signal("005930", Action.BUY),
        _ctx(positions={"005930": 50}),
    )
    assert decision.allowed is True


async def test_max_position_always_allows_sell_regardless_of_holding():
    """SELL reduces exposure — the position-size cap doesn't apply."""
    guard = MaxPositionSizeGuard(max_shares=100)
    decision = await guard.check(
        _signal("005930", Action.SELL),
        _ctx(positions={"005930": 1_000_000}),
    )
    assert decision.allowed is True


async def test_max_position_allows_buy_for_unheld_symbol():
    guard = MaxPositionSizeGuard(max_shares=100)
    decision = await guard.check(
        _signal("000660", Action.BUY),
        _ctx(positions={"005930": 999}),  # different symbol — irrelevant
    )
    assert decision.allowed is True


def test_max_position_rejects_negative_construction():
    with pytest.raises(ValueError):
        MaxPositionSizeGuard(max_shares=-1)


# ── CoolDownGuard ───────────────────────────────────────────────────────


async def test_cooldown_blocks_when_last_trade_within_window():
    guard = CoolDownGuard(cooldown_seconds=60.0)
    now = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
    decision = await guard.check(
        _signal("005930", Action.BUY),
        _ctx(last_trade_at={"005930": now - timedelta(seconds=15)}, now=now),
    )
    assert decision.allowed is False
    assert "15.0s" in decision.reason
    assert "60.0s" in decision.reason
    assert "remaining" in decision.reason  # operator can read time-to-clear


async def test_cooldown_allows_when_window_elapsed():
    guard = CoolDownGuard(cooldown_seconds=60.0)
    now = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
    decision = await guard.check(
        _signal("005930", Action.BUY),
        _ctx(last_trade_at={"005930": now - timedelta(seconds=120)}, now=now),
    )
    assert decision.allowed is True


async def test_cooldown_allows_when_no_prior_trade():
    """Fresh symbol — no last_trade_at entry — should pass through."""
    guard = CoolDownGuard(cooldown_seconds=60.0)
    decision = await guard.check(
        _signal("005930", Action.BUY),
        _ctx(),
    )
    assert decision.allowed is True


async def test_cooldown_applies_to_sell_too():
    """Cooldown throttles frequency, not direction — SELL must be blocked."""
    guard = CoolDownGuard(cooldown_seconds=60.0)
    now = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
    decision = await guard.check(
        _signal("005930", Action.SELL),
        _ctx(last_trade_at={"005930": now - timedelta(seconds=10)}, now=now),
    )
    assert decision.allowed is False


def test_cooldown_rejects_negative_construction():
    with pytest.raises(ValueError):
        CoolDownGuard(cooldown_seconds=-5.0)


# ── VolatilityCircuitBreaker ────────────────────────────────────────────


async def test_volatility_blocks_on_sharp_single_tick_drop():
    guard = VolatilityCircuitBreaker(max_drop_pct=0.05, max_cv_pct=0.50, window_size=10)
    # 10000 → 9400 = 6% drop on the last tick, above 5% threshold.
    prices = [Decimal(p) for p in ["10000", "10000", "10000", "10000", "9400"]]
    decision = await guard.check(
        _signal("005930", Action.BUY),
        _ctx(recent_prices={"005930": prices}),
    )
    assert decision.allowed is False
    assert "dropped" in decision.reason
    assert "6.00%" in decision.reason


async def test_volatility_blocks_on_high_window_cv():
    """Even without a single sharp drop, sustained chop trips the breaker."""
    guard = VolatilityCircuitBreaker(max_drop_pct=0.50, max_cv_pct=0.05, window_size=8)
    # Chop pattern: stddev ~ large fraction of mean.
    prices = [Decimal(p) for p in ["100", "120", "90", "115", "85", "110", "95", "105"]]
    decision = await guard.check(
        _signal("005930", Action.BUY),
        _ctx(recent_prices={"005930": prices}),
    )
    assert decision.allowed is False
    assert "CV" in decision.reason


async def test_volatility_allows_calm_market():
    guard = VolatilityCircuitBreaker(max_drop_pct=0.05, max_cv_pct=0.03, window_size=10)
    prices = [Decimal(p) for p in
              ["10000", "10005", "10003", "10008", "10001", "10007", "10004", "10002"]]
    decision = await guard.check(
        _signal("005930", Action.BUY),
        _ctx(recent_prices={"005930": prices}),
    )
    assert decision.allowed is True


async def test_volatility_allows_when_history_too_short():
    """Cold-start defers to other guards rather than tripping on no data."""
    guard = VolatilityCircuitBreaker()
    decision = await guard.check(
        _signal("005930", Action.BUY),
        _ctx(recent_prices={"005930": [Decimal("10000"), Decimal("10005")]}),
    )
    assert decision.allowed is True


async def test_volatility_blocks_sell_too():
    """High vol = freeze trading entirely, regardless of direction."""
    guard = VolatilityCircuitBreaker(max_drop_pct=0.05, max_cv_pct=0.50)
    prices = [Decimal(p) for p in ["10000", "10000", "10000", "10000", "9000"]]  # 10% drop
    decision = await guard.check(
        _signal("005930", Action.SELL),
        _ctx(recent_prices={"005930": prices}),
    )
    assert decision.allowed is False


def test_volatility_rejects_invalid_construction():
    with pytest.raises(ValueError):
        VolatilityCircuitBreaker(max_drop_pct=0.0)
    with pytest.raises(ValueError):
        VolatilityCircuitBreaker(window_size=1)


# ── GuardrailPipeline ───────────────────────────────────────────────────


async def test_pipeline_passes_signal_through_when_no_guards_trip():
    pipeline = GuardrailPipeline([
        MaxPositionSizeGuard(max_shares=1000),
        CoolDownGuard(cooldown_seconds=10.0),
    ])
    sig = _signal("005930", Action.BUY)
    guarded = await pipeline.evaluate(sig, _ctx())
    assert guarded.allowed is True
    assert guarded.blocked_by is None
    assert guarded.effective_signal.action is Action.BUY
    assert guarded.effective_signal is sig  # untouched


async def test_pipeline_short_circuits_on_hold_without_running_guards():
    """HOLD signals skip every guard — there's nothing to throttle."""
    class _CountingGuard:
        guard_id = "counting"
        def __init__(self) -> None:
            self.calls = 0
        async def check(self, signal, context):  # noqa: ANN001, ANN201
            self.calls += 1
            return GuardDecision.allow()

    counter = _CountingGuard()
    pipeline = GuardrailPipeline([counter])
    guarded = await pipeline.evaluate(_signal(action=Action.HOLD), _ctx())
    assert guarded.allowed is True
    assert counter.calls == 0


async def test_pipeline_fails_fast_first_blocking_guard_wins():
    """Cheap guard first; if it trips, expensive guards must NOT run."""
    class _ShouldNotRunGuard:
        guard_id = "should_not_run"
        def __init__(self) -> None:
            self.calls = 0
        async def check(self, signal, context):  # noqa: ANN001, ANN201
            self.calls += 1
            return GuardDecision.allow()

    later = _ShouldNotRunGuard()
    pipeline = GuardrailPipeline([
        MaxPositionSizeGuard(max_shares=100),  # this trips first
        later,
    ])
    guarded = await pipeline.evaluate(
        _signal("005930", Action.BUY),
        _ctx(positions={"005930": 200}),
    )
    assert guarded.allowed is False
    assert guarded.blocked_by == "max_position_size"
    assert later.calls == 0, "fail-fast: later guards must not run after a block"


async def test_pipeline_block_converts_signal_to_hold_for_executor():
    """`effective_signal.action` MUST be HOLD when blocked, but the
    original `signal.action` (intent) MUST be preserved for audit."""
    pipeline = GuardrailPipeline([MaxPositionSizeGuard(max_shares=100)])
    sig = _signal("005930", Action.BUY)
    guarded = await pipeline.evaluate(sig, _ctx(positions={"005930": 500}))
    assert guarded.effective_signal.action is Action.HOLD
    assert guarded.signal.action is Action.BUY  # original intent preserved
    assert guarded.effective_signal.score == sig.score  # score retained
    assert guarded.effective_signal.contributors == sig.contributors


async def test_pipeline_with_zero_guards_always_allows():
    """Empty pipeline is a no-op pass-through."""
    pipeline = GuardrailPipeline()
    guarded = await pipeline.evaluate(_signal(action=Action.BUY), _ctx())
    assert guarded.allowed is True


async def test_pipeline_register_appends_in_order():
    pipeline = GuardrailPipeline()
    pipeline.register(MaxPositionSizeGuard(max_shares=10))
    pipeline.register(CoolDownGuard(cooldown_seconds=5.0))
    assert pipeline.guard_count == 2
    assert pipeline.guard_ids == ["max_position_size", "cooldown"]


async def test_three_rule_pipeline_only_one_needs_to_trip():
    """The full Sprint 5f pipeline — the one each rule blocks fails."""
    pipeline = GuardrailPipeline([
        MaxPositionSizeGuard(max_shares=1000),
        CoolDownGuard(cooldown_seconds=60.0),
        VolatilityCircuitBreaker(max_drop_pct=0.05, max_cv_pct=0.50),
    ])
    now = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)

    # Position OK, cooldown trips, volatility never runs.
    g = await pipeline.evaluate(
        _signal("005930", Action.BUY),
        _ctx(
            positions     = {"005930": 100},
            last_trade_at = {"005930": now - timedelta(seconds=20)},
            now           = now,
        ),
    )
    assert g.blocked_by == "cooldown"

    # Position OK, cooldown OK, volatility trips.
    g = await pipeline.evaluate(
        _signal("005930", Action.BUY),
        _ctx(
            recent_prices = {"005930": [Decimal(p) for p in
                              ["10000", "10000", "10000", "10000", "9000"]]},  # 10% drop
            now           = now,
        ),
    )
    assert g.blocked_by == "volatility_circuit_breaker"

    # All three OK → BUY passes.
    g = await pipeline.evaluate(
        _signal("005930", Action.BUY),
        _ctx(
            positions     = {"005930": 50},
            recent_prices = {"005930": [Decimal("10000")] * 8},  # zero vol
            now           = now,
        ),
    )
    assert g.allowed is True


# ── Audit log shape ────────────────────────────────────────────────────


async def test_blocked_log_carries_full_audit_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Sprint 5g's executor will subscribe to `guardrail.blocked` for its
    shadow-trade log. The structured fields below are the contract; if
    this test breaks, alert dashboards downstream will too."""
    caplog.set_level(logging.WARNING, logger="src.domain.trading.guardrails")
    pipeline = GuardrailPipeline([MaxPositionSizeGuard(max_shares=100)])
    await pipeline.evaluate(
        _signal("005930", Action.BUY, score=0.7),
        _ctx(positions={"005930": 500}),
    )

    blocks = [r for r in caplog.records if r.message == "guardrail.blocked"]
    assert len(blocks) == 1
    rec = blocks[0]
    assert getattr(rec, "symbol")          == "005930"
    assert getattr(rec, "intended_action") == "buy"
    assert getattr(rec, "blocked_by")      == "max_position_size"
    assert "max is 100" in getattr(rec, "reason")
    assert getattr(rec, "intended_score")  == pytest.approx(0.7)


async def test_passed_signal_logs_evaluated_with_allowed_true(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="src.domain.trading.guardrails")
    pipeline = GuardrailPipeline([MaxPositionSizeGuard(max_shares=1000)])
    await pipeline.evaluate(_signal("005930", Action.BUY), _ctx())

    evals = [r for r in caplog.records if r.message == "guardrail.evaluated"]
    assert len(evals) == 1
    rec = evals[0]
    assert getattr(rec, "allowed") is True
    assert getattr(rec, "guards_run") == 1


# ── Protocol satisfaction ──────────────────────────────────────────────


def test_each_concrete_guard_satisfies_protocol():
    assert isinstance(MaxPositionSizeGuard(max_shares=1), OrderGuardrail)
    assert isinstance(CoolDownGuard(cooldown_seconds=1.0), OrderGuardrail)
    assert isinstance(VolatilityCircuitBreaker(), OrderGuardrail)
