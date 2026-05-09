"""TickContext — per-symbol rolling window of recent ticks.

Both agents (QuantAgent's RSI window, MacroAgent's prompt context) and
guards (VolatilityCircuitBreaker's recent_prices) read from this. Single
in-memory source of truth so the trading pipeline doesn't have to plumb
five different "give me the last N prices" call paths.

Implementation: `dict[symbol, deque(maxlen=N)]` — O(1) append, O(1)
trim, O(N) read. Per-symbol windows so a chatty symbol doesn't evict
the history of a quiet one.

Not asyncio-locked: in the single-event-loop architecture this entire
backend runs on, the only writers are the publisher's tick consumer
(one task) and the only readers are the trading pipeline (sibling task).
Both alternate at await points, so there's no concurrent mutation
window. If we ever shard tick consumption across loops, this comment
becomes a bug — add a lock then.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from decimal import Decimal

from ..market.models import Tick


# Default per-symbol window. 256 ticks at ~2/sec ≈ 2 minutes of context —
# enough for RSI(14), short SMAs, and a 30-tick prompt summary without
# letting memory grow unbounded over a full trading day.
_DEFAULT_MAX_WINDOW = 256


class TickContext:
    """In-memory rolling cache of recent ticks per symbol."""

    def __init__(self, *, max_window: int = _DEFAULT_MAX_WINDOW) -> None:
        if max_window < 2:
            raise ValueError(f"max_window must be >= 2, got {max_window}")
        self._max_window = max_window
        self._by_symbol: dict[str, deque[Tick]] = {}

    @property
    def max_window(self) -> int:
        return self._max_window

    def known_symbols(self) -> list[str]:
        return list(self._by_symbol.keys())

    def tick_count(self, symbol: str) -> int:
        return len(self._by_symbol.get(symbol, ()))

    def record(self, tick: Tick) -> None:
        """Append a tick to its symbol's window (creates window on first tick)."""
        bucket = self._by_symbol.get(tick.symbol)
        if bucket is None:
            bucket = deque(maxlen=self._max_window)
            self._by_symbol[tick.symbol] = bucket
        bucket.append(tick)

    def record_many(self, ticks: Iterable[Tick]) -> None:
        for t in ticks:
            self.record(t)

    def recent_ticks(self, symbol: str, n: int | None = None) -> list[Tick]:
        bucket = self._by_symbol.get(symbol)
        if not bucket:
            return []
        if n is None or n >= len(bucket):
            return list(bucket)
        # deques don't support negative-index slicing directly.
        return list(bucket)[-n:]

    def recent_prices(self, symbol: str, n: int | None = None) -> list[Decimal]:
        return [t.price for t in self.recent_ticks(symbol, n)]

    def latest(self, symbol: str) -> Tick | None:
        bucket = self._by_symbol.get(symbol)
        if not bucket:
            return None
        return bucket[-1]
