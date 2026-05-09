"""Agent contracts + lightweight mocks for the TradingCoordinator.

Real agents in later sprints:
  • `QuantAgent` (Sprint 5h-ish) — RSI / MACD / volume z-score over
    rolling tick windows; deterministic, fast, runs every tick batch.
  • `MacroAgent` (Sprint 5h-ish, gated on Open Q1 LLM provider) — feeds
    company filings + news headlines into a Claude/Ollama prompt and
    parses a structured Action+confidence response.

Both production agents will conform to the `Agent` protocol below, so
the coordinator code never needs to import them — it only needs the
shape. The mocks ship now so coordinator tests + integration scaffolding
can run before either real agent exists.

Why a `Protocol` and not an ABC: structural typing means a future agent
written in a different module (or even a synchronous wrapper around an
HTTP service) doesn't need to inherit anything — it just needs to expose
the right method signature.
"""

from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

from .models import Action, AgentSignal


@runtime_checkable
class Agent(Protocol):
    """One async method: turn a symbol (+ optional context) into a signal.

    `context` is intentionally `Any` so different agents can demand
    different inputs (recent ticks for the quant agent, news+filings for
    the macro agent). The coordinator builds the context once per
    evaluation and passes the same object to every agent — agents pluck
    out what they need.
    """

    agent_id: str

    async def evaluate(self, symbol: str, context: Any = None) -> AgentSignal: ...


class _ScriptedMockAgent:
    """Shared implementation for the two named mocks below.

    Two production modes:
      • Scripted — caller supplies a list of `(action, confidence)`
        tuples; the agent cycles through them. Deterministic — perfect
        for unit tests that need to pin specific aggregation scenarios.
      • Random — caller leaves the script empty; the agent draws a
        uniform action and confidence per call. Useful for smoke-testing
        the wiring without baking in test-friendly outputs.
    """

    def __init__(
        self,
        agent_id: str,
        *,
        script: list[tuple[Action, float]] | None = None,
        seed: int | None = None,
    ) -> None:
        self.agent_id = agent_id
        self._script = list(script) if script else []
        self._cursor = 0
        self._rng = random.Random(seed)

    async def evaluate(self, symbol: str, context: Any = None) -> AgentSignal:
        if self._script:
            action, confidence = self._script[self._cursor % len(self._script)]
            rationale = f"scripted[{self._cursor % len(self._script)}]"
            self._cursor += 1
        else:
            action = self._rng.choice([Action.BUY, Action.HOLD, Action.SELL])
            confidence = round(self._rng.uniform(0.0, 1.0), 3)
            rationale = "random"

        return AgentSignal(
            agent_id=self.agent_id,
            symbol=symbol,
            action=action,
            confidence=confidence,
            rationale=rationale,
            ts=datetime.now(timezone.utc),
        )


class MockQuantAgent(_ScriptedMockAgent):
    """Stand-in for the technical-analysis agent (RSI / MACD / volume).

    Defaults to `agent_id="mock.quant"`. Pass `script=[...]` to drive
    deterministic test scenarios; otherwise emits uniform-random signals.
    """

    def __init__(
        self,
        *,
        script: list[tuple[Action, float]] | None = None,
        seed: int | None = None,
        agent_id: str = "mock.quant",
    ) -> None:
        super().__init__(agent_id, script=script, seed=seed)


class MockMacroAgent(_ScriptedMockAgent):
    """Stand-in for the LLM-driven sentiment / fundamental agent.

    Defaults to `agent_id="mock.macro"`. Same scripted/random modes as
    `MockQuantAgent` — kept as a separate class so the coordinator's
    structured logs distinguish the two contributors at a glance.
    """

    def __init__(
        self,
        *,
        script: list[tuple[Action, float]] | None = None,
        seed: int | None = None,
        agent_id: str = "mock.macro",
    ) -> None:
        super().__init__(agent_id, script=script, seed=seed)
