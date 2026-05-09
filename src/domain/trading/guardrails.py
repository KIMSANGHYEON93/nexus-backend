"""Hard-limit guardrails — Sprint 5f safety net for the executor.

The TradingCoordinator (Sprint 5e) emits a TradeSignal based purely on
agent consensus. That signal is a *recommendation*, not an *order*. The
guardrail pipeline is the last line of defense: it converts any signal
into HOLD if a hard rule would be violated, regardless of how confident
the agents are. The pipeline is intentionally NOT smart — it doesn't
weigh, it doesn't tune, it doesn't ask "are you sure". Each rule is a
single boolean question with a clear "NO" path.

Design split:

    Coordinator   →  TradeSignal  (what we WANT to do)
                          │
                          ▼
    GuardrailPipeline  →  GuardedSignal  (what we are ALLOWED to do)
                          │
                          ▼
        Sprint 5g executor reads `effective_signal` and acts

Why a wrapper (`GuardedSignal`) instead of mutating `TradeSignal`:
  • Audit — the original (would-have-been) action is preserved alongside
    the block reason. Sprint 5g's "shadow trade" log needs both.
  • Layer hygiene — the coordinator's contract stays clean, no
    "blocked_by" leaks into the agent layer.
  • Consumer ergonomics — `guarded.effective_signal` returns a plain
    TradeSignal (HOLD-mutated if blocked) so executor code reads as
    `if guarded.effective_signal.action != HOLD: execute(...)`.

Guards run sequentially, fail-fast, in registration order. Cheap guards
(in-memory position lookup) should be registered before expensive ones
(DB-backed history) so the common rejection path is fast.
"""

from __future__ import annotations

import logging
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Protocol, runtime_checkable

from pydantic import BaseModel

from .models import Action, TradeSignal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GuardContext:
    """Per-evaluation snapshot of state that guards reason about.

    Constructed by the caller (Sprint 5g executor scaffold or test) and
    passed unchanged through the pipeline. Frozen because guards must
    NOT mutate context — that would couple decisions across guards in
    invisible ways.

    Defaults are empty mappings/sequences so a partial context (e.g.
    fresh-system with no trade history) doesn't NPE inside a guard.
    """

    positions:     Mapping[str, int]                       = field(default_factory=dict)
    last_trade_at: Mapping[str, datetime]                  = field(default_factory=dict)
    recent_prices: Mapping[str, Sequence[Decimal]]         = field(default_factory=dict)
    now:           datetime                                = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class GuardDecision:
    """One guard's verdict. `reason` is required when `allowed=False`."""

    allowed: bool
    reason:  str = ""

    @classmethod
    def allow(cls) -> "GuardDecision":
        return cls(allowed=True)

    @classmethod
    def block(cls, reason: str) -> "GuardDecision":
        if not reason:
            raise ValueError("a blocked GuardDecision must carry a reason")
        return cls(allowed=False, reason=reason)


@runtime_checkable
class OrderGuardrail(Protocol):
    """Async predicate over (signal, context). Stateless from the
    pipeline's perspective — any state lives in GuardContext or the
    guard's __init__ config (thresholds, etc.)."""

    guard_id: str

    async def check(self, signal: TradeSignal, context: GuardContext) -> GuardDecision: ...


# ── Output wrapper ──────────────────────────────────────────────────────


class GuardedSignal(BaseModel):
    """Pipeline output — preserves the original signal AND the verdict.

    `effective_signal` is what Sprint 5g's executor should act on:
    a HOLD-mutated copy when blocked, the original signal otherwise.
    The original (`signal`) is kept verbatim for audit so the
    "what we wanted vs what guardrails permitted" diff is recoverable
    at any time.
    """

    signal:     TradeSignal
    allowed:    bool
    blocked_by: str | None = None
    reason:     str | None = None

    @property
    def effective_signal(self) -> TradeSignal:
        if self.allowed:
            return self.signal
        # HOLD-mutated copy with score/contributors preserved for audit.
        return self.signal.model_copy(update={"action": Action.HOLD})


# ════════════════════════════════════════════════════════════════════════
#                          Concrete guards
# ════════════════════════════════════════════════════════════════════════


class MaxPositionSizeGuard:
    """Blocks BUY when current holding ≥ max_shares.

    SELL is always allowed (selling reduces exposure). HOLD is also
    always allowed but is short-circuited by the pipeline before any
    guard runs. Long-only by design for Sprint 5f — short positions
    will need a separate guard (or a sign-aware extension) in 5g+.
    """

    guard_id = "max_position_size"

    def __init__(self, *, max_shares: int) -> None:
        if max_shares < 0:
            raise ValueError(f"max_shares must be >= 0, got {max_shares}")
        self._max_shares = max_shares

    @property
    def max_shares(self) -> int:
        return self._max_shares

    async def check(self, signal: TradeSignal, context: GuardContext) -> GuardDecision:
        if signal.action is not Action.BUY:
            return GuardDecision.allow()
        held = context.positions.get(signal.symbol, 0)
        if held >= self._max_shares:
            return GuardDecision.block(
                f"already holding {held} shares of {signal.symbol}, "
                f"max is {self._max_shares}"
            )
        return GuardDecision.allow()


class CoolDownGuard:
    """Blocks any execution within `cooldown_seconds` of the last trade.

    Applies to BUY and SELL equally — the goal is to throttle the
    *frequency* of orders per symbol, not to bias direction. Common
    use case: prevent the same agent reacting to its own market impact
    by re-firing on a stale ladder.

    Symbols with no prior trade in the context are always allowed
    through (no last_trade_at entry → infinite cooldown elapsed).
    """

    guard_id = "cooldown"

    def __init__(self, *, cooldown_seconds: float) -> None:
        if cooldown_seconds < 0.0:
            raise ValueError(f"cooldown_seconds must be >= 0, got {cooldown_seconds}")
        self._cooldown_seconds = cooldown_seconds

    @property
    def cooldown_seconds(self) -> float:
        return self._cooldown_seconds

    async def check(self, signal: TradeSignal, context: GuardContext) -> GuardDecision:
        last = context.last_trade_at.get(signal.symbol)
        if last is None:
            return GuardDecision.allow()
        elapsed = (context.now - last).total_seconds()
        if elapsed < self._cooldown_seconds:
            remaining = self._cooldown_seconds - elapsed
            return GuardDecision.block(
                f"{signal.symbol} traded {elapsed:.1f}s ago, "
                f"cooldown requires {self._cooldown_seconds:.1f}s "
                f"({remaining:.1f}s remaining)"
            )
        return GuardDecision.allow()


class VolatilityCircuitBreaker:
    """Blocks any execution when short-term volatility breaches threshold.

    Two complementary checks against `recent_prices[symbol]`:
      • Tick-to-tick drop — `(last - prev) / prev`. If a single tick
        moves more than `max_drop_pct` *down*, freeze trading.
      • Window stddev/mean — coefficient of variation over the last
        `window_size` ticks. If it exceeds `max_cv_pct`, freeze.

    Why both: a single sharp drop catches flash-crash style moves; the
    rolling CV catches sustained chop where every individual tick is
    small but the regime is unstable. Either tripping is enough to
    block — defense in depth.

    `max_drop_pct` and `max_cv_pct` are fractions (0.05 = 5%). Less
    than 4 prices in history → not enough data to judge → ALLOW
    (defer to other guards rather than trip on cold-start).
    """

    guard_id = "volatility_circuit_breaker"

    def __init__(
        self,
        *,
        max_drop_pct: float    = 0.05,   # 5% single-tick drop
        max_cv_pct:   float    = 0.03,   # 3% rolling coefficient of variation
        window_size:  int      = 10,
    ) -> None:
        if max_drop_pct <= 0.0 or max_cv_pct <= 0.0:
            raise ValueError("thresholds must be > 0")
        if window_size < 2:
            raise ValueError(f"window_size must be >= 2, got {window_size}")
        self._max_drop_pct = max_drop_pct
        self._max_cv_pct   = max_cv_pct
        self._window_size  = window_size

    async def check(self, signal: TradeSignal, context: GuardContext) -> GuardDecision:
        prices = context.recent_prices.get(signal.symbol, ())
        if len(prices) < 4:
            return GuardDecision.allow()  # cold start — defer

        window = list(prices[-self._window_size:])

        # Tick-to-tick drop check (last vs previous).
        prev = float(window[-2])
        last = float(window[-1])
        if prev > 0:
            drop = (prev - last) / prev
            if drop > self._max_drop_pct:
                return GuardDecision.block(
                    f"{signal.symbol} dropped {drop * 100:.2f}% on last tick "
                    f"({prev:.2f} → {last:.2f}), threshold {self._max_drop_pct * 100:.2f}%"
                )

        # Rolling coefficient of variation.
        floats = [float(p) for p in window]
        mean = statistics.fmean(floats)
        if mean > 0:
            sigma = statistics.pstdev(floats)
            cv = sigma / mean
            if cv > self._max_cv_pct:
                return GuardDecision.block(
                    f"{signal.symbol} window CV {cv * 100:.2f}% over last "
                    f"{len(window)} ticks, threshold {self._max_cv_pct * 100:.2f}%"
                )

        return GuardDecision.allow()


# ════════════════════════════════════════════════════════════════════════
#                          The pipeline
# ════════════════════════════════════════════════════════════════════════


class GuardrailPipeline:
    """Runs every registered guard sequentially; first block wins.

    The pipeline emits TWO structured log lines per evaluation:
        guardrail.evaluated  — every signal that went through (audit)
        guardrail.blocked    — only when a guard rejected (alert)

    Sprint 5g's executor will subscribe to `guardrail.blocked` for its
    "would-have-traded" shadow log so post-mortems can reconstruct the
    counterfactual portfolio.
    """

    def __init__(self, guards: Sequence[OrderGuardrail] | None = None) -> None:
        self._guards: list[OrderGuardrail] = list(guards or [])

    @property
    def guard_count(self) -> int:
        return len(self._guards)

    @property
    def guard_ids(self) -> list[str]:
        return [g.guard_id for g in self._guards]

    def register(self, guard: OrderGuardrail) -> None:
        self._guards.append(guard)
        logger.info(
            "guardrail.registered",
            extra={"event": "guardrail_registered", "guard_id": guard.guard_id},
        )

    async def evaluate(self, signal: TradeSignal, context: GuardContext) -> GuardedSignal:
        # HOLD short-circuit — nothing to guard against; saves N async calls
        # per tick during quiet markets where the coordinator emits HOLD.
        if signal.action is Action.HOLD:
            return self._allow(signal, evaluated_count=0)

        for guard in self._guards:
            decision = await guard.check(signal, context)
            if not decision.allowed:
                return self._block(signal, guard.guard_id, decision.reason)

        return self._allow(signal, evaluated_count=len(self._guards))

    # ── Internals ──────────────────────────────────────────────────────

    def _allow(self, signal: TradeSignal, *, evaluated_count: int) -> GuardedSignal:
        guarded = GuardedSignal(signal=signal, allowed=True)
        logger.info(
            "guardrail.evaluated",
            extra={
                "event":            "guardrail_evaluated",
                "symbol":           signal.symbol,
                "action":           signal.action.value,
                "allowed":          True,
                "guards_run":       evaluated_count,
            },
        )
        return guarded

    def _block(self, signal: TradeSignal, guard_id: str, reason: str) -> GuardedSignal:
        guarded = GuardedSignal(
            signal=signal, allowed=False, blocked_by=guard_id, reason=reason,
        )
        # Two logs: the rejection-specific event for alerting, plus the
        # general evaluated event so a single dashboard query covers
        # both passes and blocks.
        logger.warning(
            "guardrail.blocked",
            extra={
                "event":             "guardrail_blocked",
                "symbol":            signal.symbol,
                "intended_action":   signal.action.value,
                "blocked_by":        guard_id,
                "reason":            reason,
                "intended_score":    round(signal.score, 4),
            },
        )
        logger.info(
            "guardrail.evaluated",
            extra={
                "event":      "guardrail_evaluated",
                "symbol":     signal.symbol,
                "action":     signal.action.value,
                "allowed":    False,
                "blocked_by": guard_id,
            },
        )
        return guarded
