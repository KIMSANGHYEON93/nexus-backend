"""Tests for TradingCoordinator — aggregation matrix + isolation guarantees.

The aggregation tests pin the worked-example math so any future tweak
to the weighting algorithm is loud, not silent. The isolation tests pin
the failure semantics: one agent's exception MUST NOT poison a decision
that other agents could have produced.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import pytest

from src.domain.trading.agents import (
    Agent,
    MockMacroAgent,
    MockQuantAgent,
    _ScriptedMockAgent,
)
from src.domain.trading.coordinator import (
    DEFAULT_DECISION_THRESHOLD,
    TradingCoordinator,
)
from src.domain.trading.models import Action, AgentSignal, TradeSignal


# ── Models ──────────────────────────────────────────────────────────────


def test_agent_signal_validates_confidence_range():
    with pytest.raises(Exception):  # pydantic ValidationError
        AgentSignal(
            agent_id="x", symbol="005930", action=Action.BUY,
            confidence=1.5, ts=datetime.now(timezone.utc),
        )


def test_trade_signal_hold_default_is_well_formed():
    sig = TradeSignal.hold_default("005930")
    assert sig.action is Action.HOLD
    assert sig.confidence == 0.0
    assert sig.score == 0.0
    assert sig.contributors == []
    assert sig.symbol == "005930"


# ── Mock agents ─────────────────────────────────────────────────────────


async def test_mock_agent_satisfies_agent_protocol():
    """Structural typing check — MockQuantAgent must `isinstance(Agent)`."""
    agent = MockQuantAgent(script=[(Action.BUY, 0.5)])
    assert isinstance(agent, Agent)


async def test_mock_agent_scripted_cycles_through_signals():
    agent = MockQuantAgent(script=[
        (Action.BUY,  0.8),
        (Action.SELL, 0.6),
        (Action.HOLD, 0.4),
    ])
    a = await agent.evaluate("005930")
    b = await agent.evaluate("005930")
    c = await agent.evaluate("005930")
    d = await agent.evaluate("005930")  # wraps
    assert (a.action, a.confidence) == (Action.BUY,  0.8)
    assert (b.action, b.confidence) == (Action.SELL, 0.6)
    assert (c.action, c.confidence) == (Action.HOLD, 0.4)
    assert (d.action, d.confidence) == (Action.BUY,  0.8)


async def test_mock_agent_random_mode_produces_valid_signals():
    """Without a script, the agent draws random — but must stay valid."""
    agent = MockMacroAgent(seed=42)  # seed for repeatability
    for _ in range(20):
        sig = await agent.evaluate("005930")
        assert sig.action in (Action.BUY, Action.HOLD, Action.SELL)
        assert 0.0 <= sig.confidence <= 1.0
        assert sig.symbol == "005930"
        assert sig.agent_id == "mock.macro"


# ── Aggregation: the worked example from the docstring ──────────────────


async def test_user_worked_example_buy_strong_plus_hold_weak_resolves_to_buy():
    """User's spec: QuantAgent BUY 0.8 + MacroAgent HOLD 0.3 → BUY.
    With equal weights and the default 0.3 threshold:
        score = (1*1*0.8 + 0*1*0.3) / 2 = 0.4 → BUY @ 0.4
    """
    coord = TradingCoordinator()
    coord.register(MockQuantAgent(script=[(Action.BUY,  0.8)]))
    coord.register(MockMacroAgent(script=[(Action.HOLD, 0.3)]))
    sig = await coord.evaluate("005930")
    assert sig.action is Action.BUY
    assert sig.score == pytest.approx(0.4, abs=1e-9)
    assert sig.confidence == pytest.approx(0.4, abs=1e-9)
    assert len(sig.contributors) == 2


# ── Aggregation matrix ─────────────────────────────────────────────────


async def test_unanimous_buy_at_full_confidence_lands_at_score_1():
    coord = TradingCoordinator()
    coord.register(MockQuantAgent(script=[(Action.BUY, 1.0)]))
    coord.register(MockMacroAgent(script=[(Action.BUY, 1.0)]))
    sig = await coord.evaluate("005930")
    assert sig.action is Action.BUY
    assert sig.score == pytest.approx(1.0)


async def test_unanimous_sell_at_full_confidence_lands_at_score_negative_1():
    coord = TradingCoordinator()
    coord.register(MockQuantAgent(script=[(Action.SELL, 1.0)]))
    coord.register(MockMacroAgent(script=[(Action.SELL, 1.0)]))
    sig = await coord.evaluate("005930")
    assert sig.action is Action.SELL
    assert sig.score == pytest.approx(-1.0)


async def test_offsetting_buy_and_sell_resolves_to_hold():
    """BUY 0.7 + SELL 0.7 (equal weight) → score 0 → HOLD."""
    coord = TradingCoordinator()
    coord.register(MockQuantAgent(script=[(Action.BUY,  0.7)]))
    coord.register(MockMacroAgent(script=[(Action.SELL, 0.7)]))
    sig = await coord.evaluate("005930")
    assert sig.action is Action.HOLD
    assert sig.score == pytest.approx(0.0)
    assert sig.confidence == pytest.approx(0.0)


async def test_below_threshold_resolves_to_hold_even_when_unanimous():
    """Magnitude matters, not just sign. Two BUYs at 0.2 each = score 0.2,
    below the 0.3 threshold → HOLD. Honest about thin consensus."""
    coord = TradingCoordinator(decision_threshold=0.3)
    coord.register(MockQuantAgent(script=[(Action.BUY, 0.2)]))
    coord.register(MockMacroAgent(script=[(Action.BUY, 0.2)]))
    sig = await coord.evaluate("005930")
    assert sig.action is Action.HOLD
    assert sig.score == pytest.approx(0.2)


async def test_weighting_lets_one_agent_dominate():
    """QuantAgent weight=3, MacroAgent weight=1.
    Quant says BUY 0.8, Macro says SELL 0.8.
        score = (3*1*0.8 + 1*-1*0.8) / 4 = (2.4 - 0.8)/4 = 0.4 → BUY"""
    coord = TradingCoordinator()
    coord.register(MockQuantAgent(script=[(Action.BUY,  0.8)]), weight=3.0)
    coord.register(MockMacroAgent(script=[(Action.SELL, 0.8)]), weight=1.0)
    sig = await coord.evaluate("005930")
    assert sig.action is Action.BUY
    assert sig.score == pytest.approx(0.4)


async def test_zero_weight_agent_is_silenced_but_still_counted_as_contributor():
    """Weight=0 zeros out the vote; still recorded in contributors for audit."""
    coord = TradingCoordinator()
    coord.register(MockQuantAgent(script=[(Action.BUY, 1.0)]), weight=1.0)
    coord.register(MockMacroAgent(script=[(Action.SELL, 1.0)]), weight=0.0)
    sig = await coord.evaluate("005930")
    assert sig.action is Action.BUY
    assert sig.score == pytest.approx(1.0)
    assert len(sig.contributors) == 2  # both recorded for audit


# ── Threshold edge cases ────────────────────────────────────────────────


async def test_threshold_at_exactly_zero_makes_any_lean_a_decision():
    """Threshold=0 means even score=+0.01 is a BUY. Useful for backtesting."""
    coord = TradingCoordinator(decision_threshold=0.0)
    coord.register(MockQuantAgent(script=[(Action.BUY, 0.01)]))
    sig = await coord.evaluate("005930")
    assert sig.action is Action.BUY


async def test_threshold_validation_rejects_out_of_range():
    with pytest.raises(ValueError):
        TradingCoordinator(decision_threshold=-0.1)
    with pytest.raises(ValueError):
        TradingCoordinator(decision_threshold=1.5)


async def test_negative_weight_rejected():
    coord = TradingCoordinator()
    with pytest.raises(ValueError):
        coord.register(MockQuantAgent(script=[(Action.BUY, 0.5)]), weight=-1.0)


# ── Empty / failure paths ───────────────────────────────────────────────


async def test_no_agents_registered_emits_hold_zero():
    coord = TradingCoordinator()
    sig = await coord.evaluate("005930")
    assert sig.action is Action.HOLD
    assert sig.confidence == 0.0
    assert sig.contributors == []


class _ExplodingAgent:
    """Agent that always raises — drives the failure-isolation tests."""

    def __init__(self, agent_id: str = "boom") -> None:
        self.agent_id = agent_id

    async def evaluate(self, symbol: str, context: Any = None) -> AgentSignal:
        raise RuntimeError(f"{self.agent_id} simulated failure")


async def test_one_agent_failure_does_not_block_decision():
    """The good agent's signal must still drive the final decision."""
    coord = TradingCoordinator()
    coord.register(_ExplodingAgent("flaky.quant"))
    coord.register(MockMacroAgent(script=[(Action.BUY, 0.9)]))
    sig = await coord.evaluate("005930")
    assert sig.action is Action.BUY
    assert sig.score == pytest.approx(0.9)
    assert len(sig.contributors) == 1
    assert sig.contributors[0].agent_id == "mock.macro"


async def test_all_agents_failing_yields_hold_default():
    coord = TradingCoordinator()
    coord.register(_ExplodingAgent("a"))
    coord.register(_ExplodingAgent("b"))
    sig = await coord.evaluate("005930")
    assert sig.action is Action.HOLD
    assert sig.confidence == 0.0
    assert sig.contributors == []


class _WrongSymbolAgent:
    """Agent that returns a signal for the wrong symbol — defensive check."""

    agent_id = "buggy"

    async def evaluate(self, symbol: str, context: Any = None) -> AgentSignal:
        return AgentSignal(
            agent_id=self.agent_id, symbol="WRONG_SYMBOL",
            action=Action.BUY, confidence=1.0,
            ts=datetime.now(timezone.utc),
        )


async def test_agent_returning_wrong_symbol_is_dropped():
    coord = TradingCoordinator()
    coord.register(_WrongSymbolAgent())
    coord.register(MockMacroAgent(script=[(Action.BUY, 0.5)]))
    sig = await coord.evaluate("005930")
    assert len(sig.contributors) == 1
    assert sig.contributors[0].agent_id == "mock.macro"


# ── Concurrency: agents run in parallel, not sequentially ───────────────


class _SlowAgent:
    """Agent that sleeps before returning — proves parallel scheduling."""

    def __init__(self, agent_id: str, delay_s: float, action: Action, conf: float) -> None:
        self.agent_id = agent_id
        self._delay = delay_s
        self._action = action
        self._conf = conf

    async def evaluate(self, symbol: str, context: Any = None) -> AgentSignal:
        await asyncio.sleep(self._delay)
        return AgentSignal(
            agent_id=self.agent_id, symbol=symbol,
            action=self._action, confidence=self._conf,
            ts=datetime.now(timezone.utc),
        )


async def test_agents_evaluated_in_parallel_not_serialized():
    """Three agents, each sleeping 200ms. Serial would take ~600ms;
    parallel must finish in well under 400ms."""
    coord = TradingCoordinator()
    for i in range(3):
        coord.register(_SlowAgent(f"slow.{i}", 0.2, Action.BUY, 0.5))

    loop = asyncio.get_event_loop()
    started = loop.time()
    sig = await coord.evaluate("005930")
    elapsed = loop.time() - started

    assert elapsed < 0.4, f"agents must run in parallel, took {elapsed:.3f}s"
    assert sig.action is Action.BUY
    assert len(sig.contributors) == 3


# ── Logging — the only persistence in Sprint 5e ─────────────────────────


async def test_signal_emitted_log_carries_full_decision_audit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Sprint 5f/5g consumers will subscribe to this log surface — the
    structured fields must include action, score, and contributor list
    so a downstream guardrail or executor can act without re-running."""
    caplog.set_level(logging.INFO, logger="src.domain.trading.coordinator")
    coord = TradingCoordinator()
    coord.register(MockQuantAgent(script=[(Action.BUY,  0.8)]))
    coord.register(MockMacroAgent(script=[(Action.HOLD, 0.3)]))
    await coord.evaluate("005930")

    emit_records = [
        r for r in caplog.records if r.message == "trading.coordinator.signal_emitted"
    ]
    assert len(emit_records) == 1
    rec = emit_records[0]
    assert getattr(rec, "symbol") == "005930"
    assert getattr(rec, "action") == "buy"
    assert getattr(rec, "score") == pytest.approx(0.4)
    assert getattr(rec, "contributor_count") == 2
    contribs = getattr(rec, "contributors")
    agent_ids = {c["agent_id"] for c in contribs}
    assert agent_ids == {"mock.quant", "mock.macro"}
