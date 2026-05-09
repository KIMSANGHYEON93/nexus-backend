"""Trading domain models — agent signals + final coordinator output.

Two distinct types on purpose:
  • `AgentSignal`  — produced by ONE agent for ONE symbol at ONE moment.
                     Carries the agent's identity + rationale so a final
                     decision can be audited back to its inputs.
  • `TradeSignal`  — emitted by the `TradingCoordinator` after aggregation.
                     Carries the contributing AgentSignals + a normalized
                     score so downstream consumers (Sprint 5f guardrails,
                     Sprint 5g executor) have everything they need without
                     re-running agents.

Action vocabulary stays narrow on purpose: BUY / HOLD / SELL only.
Position sizing, stop-loss, and order type are all guardrail/executor
concerns (Sprints 5f/5g), not coordinator concerns. The coordinator's
single output question is "should we move toward, away from, or stay flat
on this symbol right now."
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field


class Action(str, Enum):
    BUY  = "buy"
    HOLD = "hold"
    SELL = "sell"


def _action_score(action: Action) -> float:
    """Numeric mapping for weighted-aggregation: BUY=+1, HOLD=0, SELL=-1.

    Pulled out of the enum because Pydantic prefers plain string enums
    (cleaner JSON serialization) and an external mapping keeps the score
    semantics in the layer that uses them — the coordinator.
    """
    return {Action.BUY: 1.0, Action.HOLD: 0.0, Action.SELL: -1.0}[action]


class AgentSignal(BaseModel):
    """One agent's vote on one symbol at one timestamp.

    `confidence` is the agent's *self-reported* certainty in [0, 1] — the
    coordinator multiplies it against the agent's registered weight when
    aggregating. An agent that's unsure should report low confidence, not
    HOLD with high confidence; HOLD is a position, not a confidence level.
    """

    agent_id:   str
    symbol:     str
    action:     Action
    confidence: float = Field(ge=0.0, le=1.0)
    rationale:  str   = ""
    ts:         datetime


class TradeSignal(BaseModel):
    """Final coordinator output for one symbol.

    `score` is the weighted sum in `[-1, +1]`: > +threshold → BUY,
    < -threshold → SELL, otherwise HOLD. `confidence` is `abs(score)` so
    a barely-tipping decision (score=0.31 with threshold=0.3) reports
    confidence ≈ 0.31 — honest about how thin the consensus is.

    `contributors` is the full list of agent signals that fed in,
    INCLUDING agents whose action lost. Auditing a contrarian execution
    requires seeing the dissent, not just the winning votes.
    """

    symbol:       str
    action:       Action
    confidence:   float = Field(ge=0.0, le=1.0)
    score:        float = Field(ge=-1.0, le=1.0)
    contributors: list[AgentSignal]
    ts:           datetime

    @classmethod
    def hold_default(cls, symbol: str, contributors: list[AgentSignal] | None = None) -> "TradeSignal":
        """Helper for the no-agents / all-failed paths — explicit-HOLD with zero confidence."""
        return cls(
            symbol=symbol,
            action=Action.HOLD,
            confidence=0.0,
            score=0.0,
            contributors=contributors or [],
            ts=datetime.now(timezone.utc),
        )
