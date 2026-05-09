"""Portfolio — in-memory position + last-trade tracker.

Sprint 5h scaffold. The guards (MaxPositionSizeGuard, CoolDownGuard) need
to read current holdings + last-trade timestamps; the pipeline updates
those mappings whenever the executor reports a fill. A real broker
reconciliation loop (Sprint 5i+) will eventually replace this in-memory
state with a durable ledger that survives process restarts and
reconciles against KIS account statements.

Long-only by design — short positions clamp to 0 on SELL rather than
going negative. Matches the MaxPositionSizeGuard's long-only assumption.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from threading import Lock

from .models import Action


class Portfolio:
    """Tracks shares-held + last-trade-at per symbol.

    Lock-protected because the trading pipeline can be invoked from
    multiple sibling tasks (one per active publisher fed via the on_tick
    hook) and `record_fill` mutates two dicts together — those updates
    must appear atomic to readers building a `GuardContext`.
    """

    def __init__(self) -> None:
        self._positions:     dict[str, int]      = {}
        self._last_trade_at: dict[str, datetime] = {}
        self._lock = Lock()

    @property
    def positions(self) -> Mapping[str, int]:
        """Snapshot copy — never the live dict, so a guard reading this
        can't be torn by a concurrent record_fill."""
        with self._lock:
            return dict(self._positions)

    @property
    def last_trade_at(self) -> Mapping[str, datetime]:
        with self._lock:
            return dict(self._last_trade_at)

    def position_of(self, symbol: str) -> int:
        with self._lock:
            return self._positions.get(symbol, 0)

    def record_fill(
        self,
        *,
        symbol: str,
        action: Action,
        quantity: int,
        ts:     datetime,
    ) -> None:
        """Apply one executed fill. HOLD is rejected — fills are BUY or SELL."""
        if action is Action.HOLD:
            raise ValueError("HOLD is not a fill — only BUY/SELL update positions")
        if quantity <= 0:
            raise ValueError(f"fill quantity must be > 0, got {quantity}")
        with self._lock:
            current = self._positions.get(symbol, 0)
            if action is Action.BUY:
                self._positions[symbol] = current + quantity
            else:  # SELL — long-only clamp
                self._positions[symbol] = max(0, current - quantity)
            self._last_trade_at[symbol] = ts
