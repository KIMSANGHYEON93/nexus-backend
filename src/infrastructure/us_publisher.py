"""US equities tick publisher — server-side data source for the same
`nexus.market.tick` Redis channel the KIS publisher feeds.

Sprint 5s (API expansion to replace remaining mock data paths with real
data). The frontend has been running MomentumStreamer ('live') against
Alpha Vantage's Cloud Functions proxy since Sprint 3, but those ticks
never reached the backend — no DB persist, no audit pipeline coverage,
no SystemHealthPanel/RecentDecisions/SignalSparkline rows for US
symbols. This module moves real US data server-side so:

  • TickRepository persists US ticks alongside KRX (TimescaleDB
    `market_tick` schema is symbol-agnostic — confirmed in
    `db/migrations/001_init.sql`).
  • TradingPipeline.on_tick sees US ticks and runs the same
    coordinator/guard/executor chain.
  • The frontend HybridStreamer (Sprint 5r) becomes redundant — the
    BackendStreamer alone receives both KRX + US ticks via one WS.

Data source — Yahoo Finance public chart endpoint:
  https://query1.finance.yahoo.com/v8/finance/chart/AAPL?interval=1m&range=1d
No API key, no auth, day-cap that's effectively unlimited at our
scale. We initially tried the same Alpha Vantage Cloud Functions
proxy the frontend uses, but its shared free-tier key hit AV's
25-requests-per-DAY ceiling within hours of the frontend's traffic
(2026-05-11 11:24 UTC: probe returned `{"Information": "...25
requests per day..."}` instead of `Global Quote`). Yahoo's chart
endpoint is the right backend choice — it's the canonical free
quote source and Yahoo Finance's own web UI hits it from every
session, so blocking it would break Yahoo.

The frontend KEEPS using the AV proxy because its 25/day budget is
spread across 28 symbols × 4-symbol batches → 7 polls per full
rotation, easily within 25/day if the frontend isn't constantly
open. The backend wants persistent flow → different source.

Response shape (Yahoo `chart.result[0].meta`):
  symbol, regularMarketPrice, regularMarketVolume, previousClose,
  regularMarketTime (epoch seconds)
We derive change_percent = (price - previousClose) / previousClose,
side = "buy" if change_percent >= 0 else "sell", and volume as the
delta against the last observed `regularMarketVolume` for that
symbol (the field is day-cumulative, so publishing raw values every
poll would corrupt the VolumeHistogram window sum). On day-rollover
the cumulative resets backward — we detect that and re-baseline,
emitting 0 instead of a negative delta.

Per-symbol fetches at BATCH per poll, round-robin cursor across the
universe. Default 30s interval × 4 symbols = 28-symbol full rotation
every 3.5 min — fast enough for SystemHealthPanel's STALLED
threshold (90s) when at least one US symbol shows up per minute.

Lifecycle mirrors MockPublisher — no auth refresh, single
`asyncio.create_task` for the poll loop, idempotent start/stop.
on_tick observer is wired through so the trading pipeline sees US
ticks; failures inside the observer are logged but don't crash the
publisher (same defensive contract as MockPublisher).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

import httpx
import redis.asyncio as redis

from ..domain.market.models import Tick, TickSide
from .redis_pubsub import CHANNEL_TICK


TickObserver = Callable[[Tick], Awaitable[None]]

logger = logging.getLogger(__name__)


# Polymorphic universe — sector ETFs + FX + commodities + crypto +
# indices + treasuries. Each internal entity id (left) maps to its
# Yahoo Finance ticker (right). All probed live 2026-05-11 and confirmed
# to return real `regularMarketPrice` + `previousClose`. Sovereign
# yields outside US (BUND/JGB/GILT/BTP/OAT) and central-bank policy
# rates aren't on Yahoo's chart endpoint → omitted from this map; if
# needed they'd come from FRED / a calendar source on a different
# cadence (separate scope).
EXTRA_YAHOO_SYMBOLS: dict[str, str] = {
    # ── Equity sector ETFs (US, bare ticker) ────────────────────────
    "XLK":  "XLK",   "XLF":  "XLF",   "XLE":  "XLE",   "XLV":  "XLV",
    "XLI":  "XLI",   "XLU":  "XLU",   "XLY":  "XLY",   "XLRE": "XLRE",
    "SMH":  "SMH",
    # ── FX pairs ────────────────────────────────────────────────────
    # Yahoo's convention: USD-base pairs DROP the leading "USD" and use
    # `=X` suffix (USDJPY → JPY=X, USDCHF → CHF=X, USDTRY → TRY=X).
    # Non-USD-base pairs keep both legs (EURUSD=X, GBPUSD=X).
    "EURUSD": "EURUSD=X",
    "USDJPY": "JPY=X",
    "GBPUSD": "GBPUSD=X",
    "USDCNH": "USDCNH=X",
    "USDCHF": "CHF=X",
    "AUDUSD": "AUDUSD=X",
    "USDTRY": "TRY=X",
    # ── Commodities (Yahoo futures `=F`) ────────────────────────────
    "WTI":   "CL=F",  # Crude Oil (WTI)
    "BRT":   "BZ=F",  # Crude Oil (Brent)
    "XAU":   "GC=F",  # Gold
    "XAG":   "SI=F",  # Silver
    "CU":    "HG=F",  # Copper
    "NG":    "NG=F",  # Natural Gas
    "WHEAT": "ZW=F",
    # ── Crypto (Yahoo `-USD`) ───────────────────────────────────────
    "BTC":  "BTC-USD",
    "ETH":  "ETH-USD",
    "USDT": "USDT-USD",
    "USDC": "USDC-USD",
    # ── Market indices ──────────────────────────────────────────────
    "VIX":  "^VIX",
    "DXY":  "DX-Y.NYB",
    # ── US Treasury yields ──────────────────────────────────────────
    # ^TNX = 10-year, ^TYX = 30-year, ^FVX = 5-year, ^IRX = 13-week.
    # NEXUS dataset has UST10 + UST2; map 10Y to ^TNX. UST2 isn't on
    # Yahoo directly so we leave it out (the publisher will only emit
    # ticks for symbols listed here).
    "UST10": "^TNX",
    # ── Korea benchmarks ────────────────────────────────────────────
    # KOSPI index + USDKRW. KOSDAQ entity isn't in the NEXUS dataset
    # so we omit ^KQ11.
    "KOSPI":  "^KS11",
    "USDKRW": "KRW=X",
    # ── Sprint 5s+ AML / crypto-bridge aliases (operator: "가상화폐 연결") ──
    # Synthetic AML watchlist entities and crypto bridges had no
    # real-world tradable counterpart, so the operator's choice was
    # "delete OR connect to crypto". Operator picked connect-to-crypto —
    # each entity now mirrors the price of a crypto asset whose ledger
    # is plausibly involved in the entity's notional activity:
    #   • TORNADO  → ETH-USD     (Tornado Cash is an Ethereum mixer)
    #   • WALLET_X → BTC-USD     (BTC is the AML-poster-child base layer)
    #   • CYGNUS   → USDT-USD    (Tether dominates exchange-bridge flows)
    #   • OBSIDIAN → BTC-USD     (notional flagship investigation target)
    #   • BAKU_TR  → USDT-USD    (cross-border stablecoin laundering route)
    #   • NORDSEE  → ETH-USD     (DeFi-routed obfuscation)
    #   • HELIX    → USDC-USD    (regulated-stablecoin layering)
    #   • SAFFRON  → BTC-USD     (legacy mixer footprint)
    #   • TIDE_FX  → USDT-USD    (off-ramp via stablecoin)
    # The publisher will fetch each crypto ticker once per cycle (already
    # in the universe) AND once per alias — small redundancy, harmless.
    # The wire `symbol` stays the bare AML id, so the canvas still shows
    # OBSIDIAN/HELIX/etc. with their cluster identity intact; only the
    # underlying price signal comes from the aliased crypto.
    "TORNADO":  "ETH-USD",
    "WALLET_X": "BTC-USD",
    "CYGNUS":   "USDT-USD",
    "OBSIDIAN": "BTC-USD",
    "BAKU_TR":  "USDT-USD",
    "NORDSEE":  "ETH-USD",
    "HELIX":    "USDC-USD",
    "SAFFRON":  "BTC-USD",
    "TIDE_FX":  "USDT-USD",
}


# Shape matches MomentumStreamer's MOMENTUM_UNIVERSE on the frontend so
# entity ids line up 1:1 — the canvas already renders these as entities
# in the MOMENTUM cluster (Sprint 3d).
DEFAULT_US_SYMBOLS: tuple[str, ...] = (
    # Technology
    "AAPL", "MSFT", "NVDA", "AVGO", "CRM", "AMD", "INTC", "ORCL",
    # Communication
    "GOOGL", "META", "NFLX", "DIS",
    # Consumer Discretionary
    "AMZN", "TSLA", "HD", "MCD", "NKE",
    # Healthcare
    "LLY", "JNJ", "UNH", "MRK", "PFE",
    # Finance
    "JPM", "V", "MA", "BAC",
    # Energy
    "XOM", "CVX",
)


# Yahoo Finance public chart endpoint — base URL only, the symbol is
# appended in `_fetch_quote()`. `query1.finance.yahoo.com` and
# `query2` are interchangeable; query1 has been more reliable on this
# corp network (query2 occasionally 401s without crumb cookies even
# for the chart endpoint).
DEFAULT_DATA_SOURCE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

# Yahoo's chart endpoint occasionally responds 401/403 when the
# User-Agent looks like a non-browser bot. A plain Mozilla string is
# enough; we don't need to spoof a specific browser.
_USER_AGENT = "Mozilla/5.0 (compatible; NexusOS/1.0; +https://example.org)"

# Per-poll batch. A 28-symbol universe rotates fully in ceil(28/4)=7
# polls; at poll_interval=30s that's ~3.5 minutes per symbol per cycle.
# Enough freshness for backend persist + a SystemHealthPanel that won't
# show STALLED on US ticks during NYSE hours.
DEFAULT_BATCH_SIZE: int = 4
DEFAULT_POLL_INTERVAL_S: float = 30.0
DEFAULT_HTTP_TIMEOUT_S: float = 8.0


class UsPublisher:
    """Async background task that pulls US quotes via the AV proxy and
    publishes per-symbol ticks to `nexus.market.tick`.

    Symbol-agnostic from the consumer's view — payload shape matches
    KisPublisher and MockPublisher byte-for-byte, so the WebSocket
    consumer, persistence worker, and trading pipeline need zero
    changes to absorb US data.
    """

    def __init__(
        self,
        client:           redis.Redis,
        symbols:          list[str],
        *,
        data_source_url:  str  = DEFAULT_DATA_SOURCE_URL,
        poll_interval_s:  float = DEFAULT_POLL_INTERVAL_S,
        batch_size:       int   = DEFAULT_BATCH_SIZE,
        http_timeout_s:   float = DEFAULT_HTTP_TIMEOUT_S,
        on_tick:          TickObserver | None = None,
        http_client:      httpx.AsyncClient | None = None,
        yahoo_symbol_suffix: str = "",
        yahoo_symbol_map:  dict[str, str] | None = None,
    ) -> None:
        if not symbols:
            raise ValueError("UsPublisher requires at least one symbol")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if poll_interval_s <= 0:
            raise ValueError("poll_interval_s must be > 0")

        self._client          = client
        self._symbols         = list(symbols)
        self._data_source_url = data_source_url
        self._poll_interval   = poll_interval_s
        self._batch_size      = batch_size
        self._http_timeout    = http_timeout_s
        self._on_tick         = on_tick
        # Yahoo Finance encodes non-US instruments with assorted tag
        # conventions that don't fit a single suffix:
        #   • KRX equity   → "{symbol}.KS"           (suffix-style works)
        #   • TSE equity   → "{symbol}.T"            (suffix)
        #   • FX pairs     → "EURUSD=X" / "JPY=X"    (suffix `=X`, but USD-base pairs DROP the leading 'USD')
        #   • Futures      → "CL=F" / "GC=F"         (full alias, no relation to internal id)
        #   • Crypto       → "{symbol}-USD"          (suffix)
        #   • Indices      → "^VIX" / "DX-Y.NYB"    (prefix / arbitrary)
        #   • Yields       → "^TNX" (10Y) / "^TYX"  (arbitrary)
        # Two knobs cover both worlds:
        #   1. `yahoo_symbol_map` — per-symbol override dict. If a symbol
        #      appears here, its mapped value is the Yahoo ticker
        #      verbatim. Used for the polymorphic Extra-Universe
        #      publisher (FX + commodities + crypto + indices + yields).
        #   2. `yahoo_symbol_suffix` — fallback for homogeneous universes
        #      where every symbol shares a suffix (KRX uses ".KS").
        # The wire `symbol` field always uses the INTERNAL id (the input
        # symbol), so downstream entity matching is unchanged.
        self._yahoo_suffix    = yahoo_symbol_suffix
        self._yahoo_map       = dict(yahoo_symbol_map) if yahoo_symbol_map else {}

        # Injected http_client makes unit tests trivial (use httpx.MockTransport);
        # production path owns the client + closes it in stop().
        self._http_client    = http_client
        self._owns_client    = http_client is None

        self._task: asyncio.Task[None] | None = None
        self._cursor:    int = 0
        self._published: int = 0
        self._failures:  int = 0

        # Per-symbol last-seen day-total volume; used to compute the per-poll
        # delta so VolumeHistogram aggregates a meaningful figure rather than
        # the cumulative day-total inflated by every poll.
        self._last_volume: dict[str, int] = {}

    @property
    def published_count(self) -> int:
        return self._published

    @property
    def failure_count(self) -> int:
        return self._failures

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.is_running:
            return
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=self._http_timeout,
                headers={"User-Agent": _USER_AGENT},
            )
        self._task = asyncio.create_task(self._run(), name="us_publisher")
        logger.info(
            "us publisher task spawned",
            extra={
                "event":           "us_publisher_start",
                "symbols":         len(self._symbols),
                "batch_size":      self._batch_size,
                "poll_interval_s": self._poll_interval,
                "data_source":     self._data_source_url,
            },
        )

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._owns_client and self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None
        logger.info(
            "us publisher stopped",
            extra={
                "event":           "us_publisher_stop",
                "published_total": self._published,
                "failure_total":   self._failures,
            },
        )

    async def _run(self) -> None:
        try:
            while True:
                await self._poll_one_batch()
                await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Same posture as MockPublisher: log with full traceback, re-raise
            # so the supervisor-style watchdog (if/when added) can observe
            # is_running=False. Today there's no watchdog for US — operator
            # sees the log and restarts container.
            logger.exception(
                "us publisher loop crashed",
                extra={"event": "us_publisher_error"},
            )
            raise

    async def _poll_one_batch(self) -> None:
        """Fetch BATCH symbols from the round-robin cursor, publish each
        that parsed cleanly. Per-symbol failures don't abort the batch —
        Yahoo occasionally returns an empty/error envelope for valid
        symbols (rate-limit pulse, regional anti-bot), and that one
        symbol shouldn't poison the others.
        """
        if self._http_client is None:  # pragma: no cover — start() guarantees this
            return

        start = self._cursor % len(self._symbols)
        batch = [
            self._symbols[(start + i) % len(self._symbols)]
            for i in range(min(self._batch_size, len(self._symbols)))
        ]
        self._cursor = (start + len(batch)) % len(self._symbols)

        for symbol in batch:
            try:
                meta = await self._fetch_quote(symbol)
            except (httpx.HTTPError, httpx.TimeoutException, asyncio.TimeoutError):
                self._failures += 1
                logger.warning(
                    "us publisher quote fetch failed",
                    extra={
                        "event":  "us_publisher_fetch_failed",
                        "symbol": symbol,
                    },
                )
                continue
            except Exception:
                # Unknown shape error — log and skip; don't poison batch.
                self._failures += 1
                logger.exception(
                    "us publisher quote parse failed",
                    extra={
                        "event":  "us_publisher_parse_failed",
                        "symbol": symbol,
                    },
                )
                continue

            if meta is None:
                continue

            tick_dict = self._quote_to_tick(symbol, meta)
            if tick_dict is None:
                continue

            await self._client.publish(CHANNEL_TICK, json.dumps(tick_dict))
            self._published += 1

            if self._on_tick is not None:
                try:
                    await self._on_tick(_dict_to_tick(tick_dict))
                except Exception:
                    logger.exception(
                        "us publisher on_tick observer raised",
                        extra={"event": "us_publisher_on_tick_error"},
                    )

    async def _fetch_quote(self, symbol: str) -> dict[str, Any] | None:
        """Hit Yahoo Finance's chart endpoint for one symbol. Returns
        the `chart.result[0].meta` dict (price + previousClose +
        regularMarketVolume), or None on empty/malformed response.
        Raises on transport errors so the caller can log them
        distinctly from parse misses.

        The `yahoo_symbol_suffix` is appended ONLY for the upstream
        Yahoo lookup (e.g. `000660` → `000660.KS`). The bare `symbol`
        the caller passes in is what gets published on the wire, so
        downstream entity matching (frontend `liveDataset.ENTITIES`,
        KIS audit pipeline) is unchanged.

        We don't care about the candles array — `meta` already has the
        latest price + day volume which is everything we need for a
        per-symbol per-poll tick.
        """
        assert self._http_client is not None
        params = urlencode({"interval": "1m", "range": "1d"})
        # Per-symbol override wins over suffix. Symbols absent from the
        # map fall back to suffix-append (which is "" for US equity).
        yahoo_symbol = self._yahoo_map.get(symbol, symbol + self._yahoo_suffix)
        url = f"{self._data_source_url}/{yahoo_symbol}?{params}"
        resp = await self._http_client.get(url)
        resp.raise_for_status()
        body = resp.json()
        if not isinstance(body, dict):
            return None
        chart = body.get("chart")
        if not isinstance(chart, dict):
            return None
        # Yahoo returns `{"chart": {"error": {...}, "result": null}}` on
        # rate-limit / unknown symbol. Treat that as a parse miss.
        if chart.get("error") is not None:
            return None
        result = chart.get("result")
        if not isinstance(result, list) or not result:
            return None
        head = result[0]
        if not isinstance(head, dict):
            return None
        meta = head.get("meta")
        if not isinstance(meta, dict) or not meta:
            return None
        return meta

    def _quote_to_tick(
        self, symbol: str, meta: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Translate Yahoo's `chart.result[0].meta` to the wire-tick dict
        used by Kis/Mock publishers. Returns None when the quote can't be
        parsed (missing price, etc.) so the caller can skip cleanly.
        Volume is delta-vs-last-poll to keep VolumeHistogram window-sum
        meaningful (see module docstring).
        """
        price_raw = meta.get("regularMarketPrice")
        if price_raw is None:
            # Pre-market sometimes only sets `chartPreviousClose`. Fall
            # back to the previous close so a quiet hour still emits a
            # row instead of dropping out of the panel entirely.
            price_raw = meta.get("previousClose") or meta.get("chartPreviousClose")
        if price_raw is None:
            return None
        try:
            price = float(price_raw)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        try:
            prev_close = float(
                meta.get("previousClose") or meta.get("chartPreviousClose") or price
            )
        except (TypeError, ValueError):
            prev_close = price
        change_pct = ((price - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0

        # Volume: Yahoo's `regularMarketVolume` is day-cumulative. Compute
        # delta against last poll; first observation emits 0 so the
        # histogram doesn't show a giant bar at startup.
        try:
            day_volume = int(float(meta.get("regularMarketVolume") or 0))
        except (TypeError, ValueError):
            day_volume = 0

        last_seen = self._last_volume.get(symbol)
        if last_seen is None:
            tick_volume = 0
        elif day_volume >= last_seen:
            tick_volume = day_volume - last_seen
        else:
            # Day rollover (Yahoo reset). Re-baseline; emit zero this round.
            tick_volume = 0
        self._last_volume[symbol] = day_volume

        side = "buy" if change_pct >= 0 else "sell"

        return {
            "symbol": symbol,
            "ts":     datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "price":  round(price, 4),
            "volume": tick_volume,
            "side":   side,
        }


def _dict_to_tick(d: dict[str, Any]) -> Tick:
    """Wire-dict → domain Tick, identical shape to mock_publisher._dict_to_tick.
    Kept local so this module doesn't import from a sibling publisher.
    """
    side_str = str(d.get("side", "buy")).lower()
    side = TickSide.SELL if side_str == "sell" else TickSide.BUY
    return Tick(
        symbol = str(d["symbol"]),
        ts     = datetime.fromisoformat(str(d["ts"])),
        price  = Decimal(str(d["price"])),
        volume = int(d["volume"]),
        side   = side,
    )
