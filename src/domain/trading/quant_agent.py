"""QuantAgent — Wilder RSI(period) over the symbol's tick window.

Replaces `MockQuantAgent` in production wiring. Mocks remain in tests
because they're testing the coordinator's aggregation logic, not RSI.

Decision rule (matches the user's spec):
    RSI ≤ oversold   → BUY,  confidence = (oversold - rsi) / oversold
    RSI ≥ overbought → SELL, confidence = (rsi - overbought) / (100 - overbought)
    otherwise        → HOLD, confidence = 0

Confidence-from-extremity means RSI=10 is a stronger BUY (conf 0.67)
than RSI=29 (conf 0.03). The coordinator multiplies that against the
agent's registered weight, so a "barely oversold" tick won't dominate a
"strongly bearish" macro vote.

Cold-start safe: `period + 1` prices are needed for the first RSI value
(N changes from N+1 prices). Below that, returns HOLD@0 — never raises.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from .context import TickContext
from .models import Action, AgentSignal

logger = logging.getLogger(__name__)


# Wilder's classic period. 14 ticks at our cadence (~2/sec) is ~7s of
# microstructure — short enough to react, long enough to dampen single-
# tick spikes.
_DEFAULT_PERIOD     = 14
_DEFAULT_OVERSOLD   = 30.0
_DEFAULT_OVERBOUGHT = 70.0


class QuantAgent:
    """Reads recent prices from a shared TickContext, emits RSI-based signals."""

    def __init__(
        self,
        *,
        context:     TickContext,
        period:      int   = _DEFAULT_PERIOD,
        oversold:    float = _DEFAULT_OVERSOLD,
        overbought:  float = _DEFAULT_OVERBOUGHT,
        agent_id:    str   = "quant.rsi",
    ) -> None:
        if period < 2:
            raise ValueError(f"period must be >= 2, got {period}")
        if not (0.0 < oversold < overbought < 100.0):
            raise ValueError(
                f"need 0 < oversold < overbought < 100, got "
                f"oversold={oversold}, overbought={overbought}"
            )
        self.agent_id    = agent_id
        self._context    = context
        self._period     = period
        self._oversold   = oversold
        self._overbought = overbought

    @property
    def period(self) -> int:
        return self._period

    async def evaluate(self, symbol: str, context: Any = None) -> AgentSignal:
        prices = self._context.recent_prices(symbol)
        # Need period+1 prices because RSI is computed from period changes,
        # which require period+1 sequential observations.
        if len(prices) < self._period + 1:
            return self._hold(
                symbol,
                f"insufficient history ({len(prices)} < {self._period + 1})",
            )

        rsi = self._compute_rsi([float(p) for p in prices], self._period)

        if rsi <= self._oversold:
            confidence = self._clip((self._oversold - rsi) / self._oversold)
            return self._signal(
                symbol, Action.BUY, confidence,
                f"RSI({self._period})={rsi:.2f} ≤ {self._oversold:.0f} (oversold)",
            )
        if rsi >= self._overbought:
            confidence = self._clip(
                (rsi - self._overbought) / (100.0 - self._overbought)
            )
            return self._signal(
                symbol, Action.SELL, confidence,
                f"RSI({self._period})={rsi:.2f} ≥ {self._overbought:.0f} (overbought)",
            )

        return self._hold(symbol, f"RSI({self._period})={rsi:.2f} in neutral zone")

    # ── helpers ────────────────────────────────────────────────────────

    def _signal(
        self,
        symbol:     str,
        action:     Action,
        confidence: float,
        rationale:  str,
    ) -> AgentSignal:
        return AgentSignal(
            agent_id=self.agent_id, symbol=symbol, action=action,
            confidence=confidence, rationale=rationale,
            ts=datetime.now(timezone.utc),
        )

    def _hold(self, symbol: str, reason: str) -> AgentSignal:
        return self._signal(symbol, Action.HOLD, 0.0, reason)

    @staticmethod
    def _clip(x: float) -> float:
        return max(0.0, min(1.0, x))

    @staticmethod
    def _compute_rsi(prices: list[float], period: int) -> float:
        """Wilder's RSI over the LAST `period+1` prices.

        Edge cases:
          • avg_loss == 0 (only up moves)   → return 100.0
          • avg_gain == 0 (only down moves) → return 0.0
          • Both 0 (flat window)            → return 50.0 (neutral)
        """
        window = prices[-(period + 1):]
        gains  = 0.0
        losses = 0.0
        # Pairwise consecutive: window[1:] is intentionally one shorter,
        # so strict=False (pair count = len(window) - 1 = period).
        for prev, curr in zip(window, window[1:], strict=False):
            change = curr - prev
            if change > 0:
                gains += change
            else:
                losses += -change

        avg_gain = gains  / period
        avg_loss = losses / period

        if avg_gain == 0.0 and avg_loss == 0.0:
            return 50.0
        if avg_loss == 0.0:
            return 100.0
        if avg_gain == 0.0:
            return 0.0

        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))
