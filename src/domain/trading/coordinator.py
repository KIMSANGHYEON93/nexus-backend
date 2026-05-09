"""TradingCoordinator — weighted-confidence consensus across agents.

Algorithm (kept deliberately simple for Sprint 5e):

    Each agent's signal is mapped to a numeric vote:
        BUY  → +1
        HOLD →  0
        SELL → -1
    Then weighted by `agent_weight × signal_confidence` and summed.
    The result is normalized by total agent weight so it stays in
    `[-1, +1]` regardless of how many agents are registered.

        score = Σ (action_score(s) × weight × confidence)  /  Σ weight

    Decision threshold (default ±0.3):
        score ≥ +threshold  → BUY
        score ≤ -threshold  → SELL
        otherwise           → HOLD
        confidence = |score|

User's worked example: QuantAgent BUY @ 0.8, MacroAgent HOLD @ 0.3,
both weight 1.0:
    score = (1×1×0.8 + 0×1×0.3) / 2 = 0.4
    0.4 ≥ 0.3 → BUY @ confidence 0.4

Why no execution here: per Sprint 5e brief, the coordinator's only job
is emitting a `TradeSignal`. Sprint 5f adds risk/position guardrails
that filter signals; Sprint 5g adds the order-execution adapter that
acts on signals that survive guardrails. Stacking those concerns into
the coordinator would couple decision-making to side-effects and make
A/B testing or shadow evaluation impossible.

Agent failures are isolated: an exception from one agent does NOT
abort the whole evaluation. The contributor list shows only agents
that returned successfully; if EVERY agent fails, the coordinator
emits a HOLD@0 with empty contributors so downstream consumers always
get a well-formed signal.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from .agents import Agent
from .models import Action, AgentSignal, TradeSignal, _action_score

logger = logging.getLogger(__name__)


# Default consensus threshold. Below this, the coordinator emits HOLD
# even if every agent voted the same direction — the magnitude matters,
# not just the sign. Tuned so that the worked example in the docstring
# (BUY 0.8 + HOLD 0.3) correctly resolves to BUY.
DEFAULT_DECISION_THRESHOLD = 0.3


class TradingCoordinator:
    """Holds a set of weighted agents and produces a final TradeSignal.

    Threading model: agents are evaluated in parallel via `asyncio.gather`
    so a slow agent (e.g. the LLM-backed MacroAgent) doesn't serialize
    behind a fast one (the quant agent over rolling ticks). Per-agent
    timeouts are NOT enforced here — that's the agent's responsibility.
    The coordinator assumes any agent willing to be registered honors a
    reasonable per-call latency budget.
    """

    def __init__(self, *, decision_threshold: float = DEFAULT_DECISION_THRESHOLD) -> None:
        if not 0.0 <= decision_threshold <= 1.0:
            raise ValueError(
                f"decision_threshold must be in [0, 1], got {decision_threshold!r}"
            )
        self._threshold = decision_threshold
        self._agents: list[tuple[Agent, float]] = []

    @property
    def decision_threshold(self) -> float:
        return self._threshold

    @property
    def agent_count(self) -> int:
        return len(self._agents)

    def register(self, agent: Agent, *, weight: float = 1.0) -> None:
        """Add an agent. Weight scales its vote relative to peers; default 1.0
        means equal voice. Negative weights are rejected — to vote against
        an agent, lower its weight, don't invert it."""
        if weight < 0.0:
            raise ValueError(f"agent weight must be >= 0, got {weight!r}")
        self._agents.append((agent, weight))
        logger.info(
            "trading.coordinator.agent_registered",
            extra={
                "event":    "trading_agent_registered",
                "agent_id": agent.agent_id,
                "weight":   weight,
            },
        )

    async def evaluate(self, symbol: str, context: Any = None) -> TradeSignal:
        """Run every agent against `symbol`, aggregate, return TradeSignal.

        Failed agents (raised exception or returned a signal for the wrong
        symbol) are logged + dropped from the contributor list. The
        aggregation runs over only the surviving signals.
        """
        if not self._agents:
            logger.warning(
                "trading.coordinator.no_agents_registered",
                extra={"event": "trading_no_agents", "symbol": symbol},
            )
            signal = TradeSignal.hold_default(symbol)
            self._emit(signal)
            return signal

        # Schedule every agent in parallel; collect results + exceptions.
        tasks = [a.evaluate(symbol, context) for a, _ in self._agents]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        contributors: list[AgentSignal] = []
        active_weights: list[float] = []
        for (agent, weight), result in zip(self._agents, results, strict=True):
            if isinstance(result, BaseException):
                logger.warning(
                    "trading.coordinator.agent_failed",
                    extra={
                        "event":      "trading_agent_failed",
                        "agent_id":   agent.agent_id,
                        "error_type": type(result).__name__,
                    },
                )
                continue
            if result.symbol != symbol:
                logger.warning(
                    "trading.coordinator.agent_symbol_mismatch",
                    extra={
                        "event":     "trading_agent_symbol_mismatch",
                        "agent_id":  agent.agent_id,
                        "expected":  symbol,
                        "got":       result.symbol,
                    },
                )
                continue
            contributors.append(result)
            active_weights.append(weight)

        signal = self._aggregate(symbol, contributors, active_weights)
        self._emit(signal)
        return signal

    def _aggregate(
        self,
        symbol: str,
        contributors: list[AgentSignal],
        weights: list[float],
    ) -> TradeSignal:
        total_weight = sum(weights)
        if total_weight <= 0.0:
            return TradeSignal.hold_default(symbol, contributors)

        raw = sum(
            _action_score(sig.action) * w * sig.confidence
            for sig, w in zip(contributors, weights, strict=True)
        )
        score = max(-1.0, min(1.0, raw / total_weight))

        if   score >=  self._threshold:  action = Action.BUY
        elif score <= -self._threshold:  action = Action.SELL
        else:                            action = Action.HOLD

        return TradeSignal(
            symbol=symbol,
            action=action,
            confidence=abs(score),
            score=score,
            contributors=contributors,
            ts=datetime.now(timezone.utc),
        )

    def _emit(self, signal: TradeSignal) -> None:
        """Single structured-log surface for every emitted decision.

        Sprint 5f's guardrails will subscribe to this point (or wrap
        evaluate()) to filter; Sprint 5g's executor will act on the
        survivors. Until then, the log is the only persistence — which
        is intentional. No DB writes from a layer that doesn't yet have
        an audit story.
        """
        logger.info(
            "trading.coordinator.signal_emitted",
            extra={
                "event":            "trade_signal_emitted",
                "symbol":           signal.symbol,
                "action":           signal.action.value,
                "confidence":       round(signal.confidence, 4),
                "score":            round(signal.score, 4),
                "contributor_count": len(signal.contributors),
                "contributors": [
                    {
                        "agent_id":   s.agent_id,
                        "action":     s.action.value,
                        "confidence": s.confidence,
                    }
                    for s in signal.contributors
                ],
            },
        )
