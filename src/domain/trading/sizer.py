"""PositionSizer — Sprint 5k confidence-aware order sizing.

The coordinator (5e) attaches `confidence ∈ [0,1]` to every TradeSignal,
but the executor (5g) has been ignoring it — every order is the same
hardcoded `default_quantity`. Sprint 5k wires confidence into the size
decision so a barely-tipping signal trades small and a strong consensus
trades larger, all still gated by the 5f hard guards and the 5g global
safety switch.

Sizer is a Protocol so multiple strategies can coexist:

  • FixedSizer — always returns the same quantity. Behaviorally
    identical to pre-5k executor; the default when an operator hasn't
    chosen a strategy.
  • ConfidenceLinearSizer — linear interpolation between
    `min_quantity` (at confidence=0) and `max_quantity` (at confidence=1).
  • Future: KellySizer, VolatilityScaledSizer, etc. — same Protocol,
    no executor changes required.

Why sync (not async): sizing is pure arithmetic over the signal — no IO,
no DB lookup. Keeping the contract sync makes calling it in the executor
hot path zero-allocation and lets future test fakes be one-line lambdas.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .models import TradeSignal


@runtime_checkable
class PositionSizer(Protocol):
    """Sync predicate: TradeSignal → integer share count.

    Returning 0 means "skip this trade" — the executor treats it as a
    noop (logged, no order submitted). Negative values are invalid;
    any sizer that wants to express "block this trade" should return 0,
    not a negative number.
    """

    def size_for(self, signal: TradeSignal) -> int: ...


class FixedSizer:
    """Always returns the same quantity, regardless of signal confidence.

    Equivalent to pre-Sprint-5k executor behavior. Used as the implicit
    default when `OrderExecutor` is constructed without an explicit sizer
    so that existing call sites and tests keep working unchanged.
    """

    def __init__(self, quantity: int) -> None:
        if quantity <= 0:
            raise ValueError(f"FixedSizer quantity must be > 0, got {quantity}")
        self._quantity = quantity

    @property
    def quantity(self) -> int:
        return self._quantity

    def size_for(self, signal: TradeSignal) -> int:
        return self._quantity


class ConfidenceLinearSizer:
    """Linear interpolation between min and max based on signal.confidence.

        size = round(min_quantity + (max_quantity - min_quantity) × confidence)

    Examples (min=1, max=100):
        confidence=0.0  → 1
        confidence=0.3  → 31
        confidence=0.5  → 51
        confidence=0.85 → 85
        confidence=1.0  → 100

    Optional `min_confidence` floor — if the signal's confidence is below
    this, returns 0 (sizer asks executor to skip the trade). Keeps the
    pipeline from emitting micro-orders on barely-confident signals;
    operators set this to e.g. 0.2 to require "at least somewhat sure"
    before trading at all.
    """

    def __init__(
        self,
        *,
        min_quantity:    int,
        max_quantity:    int,
        min_confidence:  float = 0.0,
    ) -> None:
        if min_quantity <= 0:
            raise ValueError(f"min_quantity must be > 0, got {min_quantity}")
        if max_quantity < min_quantity:
            raise ValueError(
                f"max_quantity ({max_quantity}) must be >= min_quantity ({min_quantity})"
            )
        if not 0.0 <= min_confidence <= 1.0:
            raise ValueError(
                f"min_confidence must be in [0, 1], got {min_confidence}"
            )
        self._min_quantity   = min_quantity
        self._max_quantity   = max_quantity
        self._min_confidence = min_confidence

    @property
    def min_quantity(self) -> int:
        return self._min_quantity

    @property
    def max_quantity(self) -> int:
        return self._max_quantity

    @property
    def min_confidence(self) -> float:
        return self._min_confidence

    def size_for(self, signal: TradeSignal) -> int:
        c = signal.confidence
        # Defensive clamp — coordinator is supposed to keep this in [0,1]
        # but a future agent bug shouldn't size 10× max from a stray 9.5.
        c = max(0.0, min(1.0, c))
        if c < self._min_confidence:
            return 0
        span = self._max_quantity - self._min_quantity
        size = round(self._min_quantity + span * c)
        return max(self._min_quantity, min(self._max_quantity, size))
