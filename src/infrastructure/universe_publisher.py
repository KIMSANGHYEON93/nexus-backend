"""DB-backed Yahoo Finance publisher — extended universe (Sprint 5s+).

Reads the entire `security_master` table at startup, filters out tickers
already covered by KIS (the 12 `is_subscribed=true` Korean equities live
on a separate WS path), and polls Yahoo Finance's chart endpoint for
everything else. Publishes ticks to the same `nexus.market.tick` channel
the other publishers feed so persistence + audit + trading pipelines
treat universe ticks identically to KIS / Us / Extra-Yahoo ticks.

Design choice — reuse `UsPublisher` body, not a fresh implementation:
    UsPublisher already speaks the exact Yahoo chart endpoint we want
    here, handles per-symbol suffix/map remapping, round-robin batching,
    cumulative-volume delta correction, day-rollover detection, the
    `on_tick` observer hook, and idempotent start/stop. Re-inventing all
    of that would just duplicate bugs. Instead, this module composes a
    single `UsPublisher` instance after the DB-side ticker resolution +
    Yahoo-symbol mapping is done.

Why this isn't merged INTO `UsPublisher.__init__(pool=...)`:
    `UsPublisher` is in the symbol-class-agnostic infrastructure layer
    and shouldn't import `asyncpg`. Keeping the DB read here preserves
    the dependency direction (infrastructure → no DB-aware modules).

The exclude_tickers set mirrors `settings.kis_subscribe_symbol_list` —
those 12 symbols are streamed by the dedicated KisPublisher (sub-second
WS) plus optionally the KRX Yahoo publisher (60s supplement), so adding
them here would just triple-publish.

Padding tickers (data_source='static_master_padding') are ALWAYS
excluded from polling — they have no real Yahoo counterpart. They live
in the DB so frontend universe-size counters reflect the canonical
index sizes (KOSPI 200, S&P 500, etc.) but the publisher cannot fetch
them.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg
import httpx
import redis.asyncio as redis

from ..domain.market.models import Tick
from .us_publisher import (
    DEFAULT_DATA_SOURCE_URL,
    DEFAULT_HTTP_TIMEOUT_S,
    UsPublisher,
)


TickObserver = Callable[[Tick], Awaitable[None]]

logger = logging.getLogger(__name__)


# Yahoo Finance ticker suffix per NEXUS market enum. Padding markets
# ("OTHER") aren't mappable to a real Yahoo symbol so the publisher will
# never see them (they're filtered earlier by data_source).
#   KRX     → "{ticker}.KS"        (000660 → 000660.KS)
#   KOSDAQ  → "{ticker}.KQ"        (247540 → 247540.KQ)
#   NASDAQ  → "{ticker}"           (AAPL   → AAPL)
#   NYSE    → "{ticker}"           (JPM    → JPM)
_MARKET_SUFFIX: dict[str, str] = {
    "KRX":    ".KS",
    "KOSDAQ": ".KQ",
    "NASDAQ": "",
    "NYSE":   "",
}


# Sensible defaults — slower than the dedicated US publisher because we
# have ~6× more symbols to rotate through. With 900 tickers at
# batch=10 / interval=60s, full universe rotation = ceil(900/10)·60 ≈
# 90 min, which is fine for backend persist + the "show all KOSPI 200
# in the canvas" use case (these aren't trading-hot symbols).
DEFAULT_POLL_INTERVAL_S: float = 60.0
DEFAULT_BATCH_SIZE:      int   = 10


class UniversePublisher:
    """DB-backed Yahoo Finance publisher for the full extended universe.

    Reads tickers from `security_master` once at startup, builds a
    per-symbol Yahoo ticker map, and delegates the polling loop to
    `UsPublisher`. Lifecycle parity with the other publishers:

      • `start()`  — load tickers → build publisher → start its loop.
      • `stop()`   — defer to the inner publisher's stop.
      • Idempotent on both ends.

    On a DB error during `_load_tickers()`, the publisher logs and
    starts with an empty universe (no symbols → never wakes up). This
    matches the fault-tolerance policy used elsewhere (logs the symptom,
    keeps the service alive).
    """

    def __init__(
        self,
        client:           redis.Redis,
        pool:             asyncpg.Pool,
        *,
        exclude_tickers:  set[str] | None = None,
        poll_interval_s:  float = DEFAULT_POLL_INTERVAL_S,
        batch_size:       int   = DEFAULT_BATCH_SIZE,
        http_timeout_s:   float = DEFAULT_HTTP_TIMEOUT_S,
        data_source_url:  str   = DEFAULT_DATA_SOURCE_URL,
        on_tick:          TickObserver | None = None,
        http_client:      httpx.AsyncClient | None = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if poll_interval_s <= 0:
            raise ValueError("poll_interval_s must be > 0")

        self._client          = client
        self._pool            = pool
        self._exclude         = set(exclude_tickers or ())
        self._poll_interval   = poll_interval_s
        self._batch_size      = batch_size
        self._http_timeout    = http_timeout_s
        self._data_source_url = data_source_url
        self._on_tick         = on_tick
        self._http_client     = http_client

        self._inner: UsPublisher | None = None
        # Diagnostic counters surfaced for /readyz / metrics endpoints.
        self._loaded_tickers: int = 0
        self._excluded:       int = 0

    @property
    def is_running(self) -> bool:
        return self._inner is not None and self._inner.is_running

    @property
    def published_count(self) -> int:
        return self._inner.published_count if self._inner else 0

    @property
    def failure_count(self) -> int:
        return self._inner.failure_count if self._inner else 0

    @property
    def universe_size(self) -> int:
        return self._loaded_tickers

    async def _load_tickers(self) -> tuple[list[str], dict[str, str]]:
        """Read `security_master` once and return (symbols, yahoo_map).

        Filters:
          • `is_subscribed = false`  — the 12 KIS tickers are streamed by
            KisPublisher (sub-second WS); skip them here so we don't
            double-publish. NB: `exclude_tickers` (passed by main.py) is
            ALSO applied as a defense in depth.
          • `data_source != 'static_master_padding'` — PAD_* placeholders
            have no real Yahoo counterpart.
          • `market IN ('KRX','KOSDAQ','NASDAQ','NYSE')` — `OTHER` market
            is excluded (the padding market_label).

        Returns:
          • symbols   — list of internal ticker ids (the wire `symbol`).
          • yahoo_map — per-symbol override map for the Yahoo lookup,
                        keyed by internal ticker, value is the Yahoo
                        ticker (with .KS / .KQ suffix where applicable).
        """
        try:
            rows = await self._pool.fetch(
                """
                SELECT ticker, market
                  FROM security_master
                 WHERE is_subscribed = FALSE
                   AND data_source <> 'static_master_padding'
                   AND market IN ('KRX','KOSDAQ','NASDAQ','NYSE')
                 ORDER BY ticker ASC
                """
            )
        except (
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            OSError, TimeoutError,
        ) as exc:
            logger.warning(
                "universe_publisher.load_failed",
                extra={
                    "event":      "universe_publisher_load_failed",
                    "error_type": type(exc).__name__,
                    "error":      str(exc)[:200],
                },
            )
            return [], {}

        symbols:    list[str]         = []
        yahoo_map:  dict[str, str]    = {}
        skipped_excl = 0
        for row in rows:
            ticker = row["ticker"]
            market = row["market"]
            if ticker in self._exclude:
                skipped_excl += 1
                continue
            suffix = _MARKET_SUFFIX.get(market, "")
            symbols.append(ticker)
            yahoo_map[ticker] = ticker + suffix
        self._excluded = skipped_excl
        return symbols, yahoo_map

    async def start(self) -> None:
        if self.is_running:
            return

        symbols, yahoo_map = await self._load_tickers()
        self._loaded_tickers = len(symbols)
        if not symbols:
            # Empty universe — log and bail. We don't raise because the
            # service must still boot (other publishers may be healthy);
            # operator sees the log and either fixes the DB or disables
            # the flag.
            logger.warning(
                "universe publisher has no tickers — start() is a no-op",
                extra={
                    "event":    "universe_publisher_empty",
                    "excluded": self._excluded,
                },
            )
            return

        self._inner = UsPublisher(
            self._client,
            symbols,
            data_source_url   = self._data_source_url,
            poll_interval_s   = self._poll_interval,
            batch_size        = self._batch_size,
            http_timeout_s    = self._http_timeout,
            on_tick           = self._on_tick,
            http_client       = self._http_client,
            yahoo_symbol_map  = yahoo_map,
        )
        await self._inner.start()
        logger.info(
            "universe publisher armed",
            extra={
                "event":           "universe_publisher_start",
                "symbols":         len(symbols),
                "excluded":        self._excluded,
                "batch_size":      self._batch_size,
                "poll_interval_s": self._poll_interval,
            },
        )

    async def stop(self) -> None:
        if self._inner is not None:
            await self._inner.stop()
        # We intentionally do NOT null out `self._inner` so post-stop
        # property reads (`published_count`, `failure_count`) still return
        # the final totals. start() will overwrite it on the next bring-up.
        logger.info(
            "universe publisher stopped",
            extra={
                "event":           "universe_publisher_stop",
                "published_total": self.published_count,
                "failure_total":   self.failure_count,
            },
        )


def _resolve_yahoo_symbol(ticker: str, market: str) -> str | None:
    """Public helper — translate (internal ticker, market) → Yahoo ticker.

    Returns None when the market isn't Yahoo-mappable (e.g. padding rows
    or future market enums we don't recognize). Useful for ops scripts.
    """
    suffix = _MARKET_SUFFIX.get(market)
    if suffix is None:
        return None
    return ticker + suffix


__all__ = ["UniversePublisher", "_resolve_yahoo_symbol"]
