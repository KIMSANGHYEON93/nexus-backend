"""News providers — Sprint 5j real-data swap-in for MockNewsProvider.

Two classes here, deliberately split:

  • GoogleNewsRSSProvider — fetches real headlines via the public Google
    News RSS endpoint (no API key, no quota). Pure fetch + parse, no
    caching of its own. On any HTTP/XML failure returns [] so the
    MacroAgent's prompt template degrades gracefully to "(no headlines)"
    rather than crashing the publisher loop.

  • CachingNewsProvider — wrapper/decorator that caches whatever inner
    provider it's given for `ttl_seconds`. Per-symbol asyncio.Lock so a
    burst of 600 cache-miss ticks (theoretical worst case at the 5-min
    TTL boundary with 2-tick/sec) doesn't fire 600 concurrent fetches.
    On a fetch failure it falls back to the last good cached value if
    one exists — no point throwing away yesterday's news just because
    the upstream is hiccuping right now.

The Protocol they implement is `domain.trading.macro_agent.NewsProvider`
(defined there because it's the agent's outbound port). Wiring lives one
layer up in `main.py` via `build_news_provider()`.

KRX symbol → company name:
  Google News RSS searches by keyword. A bare 6-digit KRX code returns
  near-zero usable signal because the model hasn't seen it associated
  with the company. We hard-code the 12 seeded tickers (matches the
  MockPublisher universe) and fall back to the bare code for unknowns.
"""

from __future__ import annotations

import asyncio
import logging
import time
import urllib.parse
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import httpx

logger = logging.getLogger(__name__)


# ── Symbol → search keyword table ───────────────────────────────────────
# Mirrors the MockPublisher / kis_subscribe_symbols seed list. Keep in
# sync when that universe changes. Korean equity names work better in
# Google News than the 6-digit codes — the codes don't index well in
# English-language news the LLM analyst is reasoning about.
_SYMBOL_TO_QUERY: dict[str, str] = {
    "005930": "Samsung Electronics",
    "000660": "SK Hynix",
    "035420": "Naver Corp",
    "035720": "Kakao Corp",
    "105560": "KB Financial",
    "055550": "Shinhan Financial",
    "086790": "Hana Financial",
    "005380": "Hyundai Motor",
    "005490": "POSCO",
    "051910": "LG Chem",
    "207940": "Samsung Biologics",
    "068270": "Celltrion",
}


def query_for_symbol(symbol: str) -> str:
    """Map a KRX 6-digit code to a Google News search keyword.

    Falls back to the bare code for unknown symbols — gives noisy results
    but better than crashing or returning [] silently. Sprint 5j extension
    point: load this map from a config file or DB if the symbol universe
    grows past hand-maintainable size.
    """
    return _SYMBOL_TO_QUERY.get(symbol, symbol)


# ── Protocol re-export ──────────────────────────────────────────────────
# The Protocol itself lives in `domain.trading.macro_agent` (it's the
# agent's outbound port). Re-exported here for callers that want to
# import everything news-related from one place.

@runtime_checkable
class NewsProvider(Protocol):
    async def recent_headlines(self, symbol: str) -> list[str]: ...


# ── Real RSS fetcher ────────────────────────────────────────────────────


# Generous timeout — RSS endpoints are sometimes slow but the call is
# infrequent (cached behind the decorator). Keep below LLM timeout so a
# news hiccup never dominates per-tick latency budget.
_DEFAULT_TIMEOUT_SECONDS = 5.0
_MAX_HEADLINES           = 8


class GoogleNewsRSSProvider:
    """Fetches headlines from `news.google.com/rss/search?q=...`.

    Public, no-auth, free. Single GET per call. Returns at most
    `_MAX_HEADLINES` (8) titles — more would inflate the LLM prompt
    without adding signal (Google News surfaces the top-N by recency
    already). Returns [] on any failure so the MacroAgent degrades
    gracefully.
    """

    def __init__(
        self,
        *,
        hl:              str   = "en-US",
        gl:              str   = "US",
        ceid:            str   = "US:en",
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        base_url:        str   = "https://news.google.com/rss/search",
    ) -> None:
        self._hl              = hl
        self._gl              = gl
        self._ceid            = ceid
        self._timeout_seconds = timeout_seconds
        self._base_url        = base_url

    def build_url(self, symbol: str) -> str:
        """Public for testing — returns the exact URL we'll GET."""
        params = {
            "q":    query_for_symbol(symbol),
            "hl":   self._hl,
            "gl":   self._gl,
            "ceid": self._ceid,
        }
        return f"{self._base_url}?{urllib.parse.urlencode(params)}"

    async def recent_headlines(self, symbol: str) -> list[str]:
        url = self.build_url(symbol)
        try:
            async with httpx.AsyncClient(
                timeout=self._timeout_seconds,
                # Some Google endpoints 403 empty UAs. A generic UA is
                # fine — we're not pretending to be a browser, just a
                # well-behaved feed reader.
                headers={"user-agent": "nexus-os-news-fetcher/1.0"},
            ) as client:
                resp = await client.get(url)
        except httpx.TimeoutException:
            logger.warning(
                "news.rss.timeout",
                extra={"event": "news_rss_timeout", "symbol": symbol},
            )
            return []
        except httpx.HTTPError as exc:
            logger.warning(
                "news.rss.network_error",
                extra={
                    "event":      "news_rss_network_error",
                    "symbol":     symbol,
                    "error_type": type(exc).__name__,
                },
            )
            return []

        if resp.status_code >= 400:
            logger.warning(
                "news.rss.http_error",
                extra={
                    "event":       "news_rss_http_error",
                    "symbol":      symbol,
                    "status_code": resp.status_code,
                },
            )
            return []

        return _parse_rss_titles(resp.text, max_headlines=_MAX_HEADLINES, symbol=symbol)


def _parse_rss_titles(xml_text: str, *, max_headlines: int, symbol: str) -> list[str]:
    """Pull `<title>` text from each `<item>` block, capped at max.

    Google News RSS structure:
        <rss><channel>
          <title>...</title>          ← channel-level (skip)
          <item><title>HEADLINE</title>...</item>
          ...
        </channel></rss>

    Returns [] on parse failure rather than raising — the agent's prompt
    template handles empty headlines gracefully. Defensive against
    HTML-encoded entities, missing items, BOM-prefixed feeds, etc.
    """
    if not xml_text or not xml_text.strip():
        return []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.warning(
            "news.rss.parse_error",
            extra={
                "event":      "news_rss_parse_error",
                "symbol":     symbol,
                "error":      str(exc)[:120],
                "preview":    xml_text[:120],
            },
        )
        return []

    # Channel may be at root or one level down depending on namespace
    # weirdness. Both `channel/item/title` and `item/title` are accepted.
    titles: list[str] = []
    for path in ("channel/item/title", "item/title", ".//item/title"):
        for elem in root.iterfind(path):
            text = (elem.text or "").strip()
            if text:
                titles.append(text)
        if titles:
            break

    return titles[:max_headlines]


# ── Cache decorator ────────────────────────────────────────────────────


@dataclass
class _CacheEntry:
    headlines:  list[str]
    fetched_at: float          # monotonic seconds


class CachingNewsProvider:
    """TTL cache wrapper around any NewsProvider.

    Per-symbol `asyncio.Lock` so a burst of N concurrent cache-miss
    requests for the same symbol fires exactly ONE upstream fetch (the
    others wait for the lock, then read the freshly-cached result).
    Without this, the first 600 ticks at startup could each fire a
    Google News request before any of them populated the cache.

    Stale-cache fallback: if the inner provider raises (or returns []
    after raising — RSS provider's contract) AND we have an old cache
    entry from a prior successful fetch, return THAT instead of empty.
    Trader instinct: yesterday's news is more useful than no news when
    the upstream is broken.
    """

    def __init__(
        self,
        inner: NewsProvider,
        *,
        ttl_seconds: float,
    ) -> None:
        if ttl_seconds <= 0.0:
            raise ValueError(f"ttl_seconds must be > 0, got {ttl_seconds}")
        self._inner       = inner
        self._ttl         = ttl_seconds
        self._cache: dict[str, _CacheEntry]      = {}
        self._locks: dict[str, asyncio.Lock]     = {}
        self._hit_count:  int = 0
        self._miss_count: int = 0

    @property
    def hit_count(self) -> int:
        return self._hit_count

    @property
    def miss_count(self) -> int:
        return self._miss_count

    @property
    def ttl_seconds(self) -> float:
        return self._ttl

    def cached_count(self) -> int:
        """Number of distinct symbols currently in cache (any age)."""
        return len(self._cache)

    async def recent_headlines(self, symbol: str) -> list[str]:
        # Fast path — fresh cache hit, no lock needed.
        entry = self._cache.get(symbol)
        if entry is not None and self._is_fresh(entry):
            self._hit_count += 1
            return list(entry.headlines)

        # Slow path — acquire per-symbol lock, double-check inside it
        # (another coroutine may have populated while we waited).
        lock = self._locks.setdefault(symbol, asyncio.Lock())
        async with lock:
            entry = self._cache.get(symbol)
            if entry is not None and self._is_fresh(entry):
                self._hit_count += 1
                return list(entry.headlines)

            # True cache miss — go fetch.
            self._miss_count += 1
            try:
                headlines = await self._inner.recent_headlines(symbol)
            except Exception:  # noqa: BLE001 — defensive; RSS provider returns [] but other impls may raise
                headlines = []
                logger.warning(
                    "news.cache.inner_raised",
                    extra={"event": "news_cache_inner_raised", "symbol": symbol},
                )

            # Stale-cache fallback: if the fetch came back empty AND we
            # have something old, prefer the old data over silence.
            if not headlines and entry is not None and entry.headlines:
                logger.info(
                    "news.cache.stale_fallback",
                    extra={
                        "event":            "news_cache_stale_fallback",
                        "symbol":           symbol,
                        "stale_age_seconds": time.monotonic() - entry.fetched_at,
                    },
                )
                # Update fetched_at even though the data is stale, so we
                # don't hammer the upstream every tick during an outage.
                self._cache[symbol] = _CacheEntry(
                    headlines=list(entry.headlines), fetched_at=time.monotonic(),
                )
                return list(entry.headlines)

            self._cache[symbol] = _CacheEntry(
                headlines=list(headlines), fetched_at=time.monotonic(),
            )
            return list(headlines)

    def _is_fresh(self, entry: _CacheEntry) -> bool:
        return (time.monotonic() - entry.fetched_at) < self._ttl


# ── Factory ─────────────────────────────────────────────────────────────


# Type alias for the factory return — matches the Protocol but is
# concrete so build_news_provider() can advertise the wrapper layering.
NewsProviderInstance = NewsProvider | None


def build_news_provider(
    *,
    provider:           str,
    cache_ttl_seconds:  float,
) -> NewsProviderInstance:
    """Build the configured news provider with cache, or return None
    so the caller falls back to MockNewsProvider.

    Returning None on `provider="none"` lets `main.py` keep the simple
    "swap MockNewsProvider in" branch without needing to know what's
    inside this module.
    """
    p = (provider or "none").strip().lower()
    if p == "none":
        return None
    if p == "google_rss":
        return CachingNewsProvider(
            GoogleNewsRSSProvider(),
            ttl_seconds=cache_ttl_seconds,
        )
    logger.error(
        "news.factory.unknown_provider",
        extra={"event": "news_factory_unknown_provider", "provider": p},
    )
    return None
