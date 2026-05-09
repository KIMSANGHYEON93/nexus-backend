"""Live KIS publisher — drop-in replacement for MockPublisher.

Takes a connected `KisClient`, subscribes to the configured symbols, then
loops `stream_ticks()` and republishes each Tick to the same Redis channel
that `MockPublisher` writes (`nexus.market.tick`). The wire payload shape
is byte-identical to the mock — `{symbol, ts, price, volume, side}` — so
the frontend `BackendStreamer` and `useMarketData` consumers don't need
to know which producer is running.

Sprint 5d additions:
  • `_TokenRefreshLoop` — background task that watches `access_token`
    expiry and proactively re-authenticates well before the deadline
    (default headroom 30 min). The WS connection persists across REST
    refreshes — KIS issues access_token (REST) and approval_key (WS)
    independently, so swapping the access_token doesn't perturb the open
    realtime stream.
  • Refresh failures are logged + retried (no death-spiral of the loop)
    so a transient 5xx from /oauth2/tokenP doesn't take down the publisher.

Lifespan switchover (in `main.py`):
    PublisherSupervisor handles selection + runtime failover — see
    `publisher_supervisor.py`.

`start()` / `stop()` mirror the MockPublisher interface (idempotent,
cancellation-aware) so the supervisor code is symmetric for either backend.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import redis.asyncio as redis

from ..domain.market.models import Tick
from .kis_client import KisClient, KisError
from .redis_pubsub import CHANNEL_TICK

logger = logging.getLogger(__name__)


# ── Token refresh defaults ──────────────────────────────────────────────
# 30-min headroom: KIS tokens live 24h; refreshing 30 min before expiry
# leaves room for ~30 retries (1/min KIS rate limit) before we run dry.
_DEFAULT_REFRESH_HEADROOM_S  = 30 * 60
# 60s check cadence: token expiry is wall-clock, not RTT-sensitive — a
# tighter loop just burns CPU. Coarser than 60s risks missing the headroom
# window during a clock skew event.
_DEFAULT_REFRESH_INTERVAL_S  = 60.0


class _TokenRefreshLoop:
    """Background task: monitors access_token expiry, re-auths near deadline.

    Lifecycle is owned by `KisPublisher` — start() spawns the task,
    stop() cancels and awaits it. Refresh failures are logged + counted
    but never raise out of the loop; the next tick of the loop retries.

    Exhaustion scenario: if KIS rejects every refresh attempt for the
    full 30-min headroom window, the access_token expires, the WS server
    eventually drops the connection (because subscribe-time auth is
    invalid), `stream_ticks()` raises ConnectionClosed, the publisher
    task exits, and the supervisor's watchdog fails us over to the mock.
    """

    def __init__(
        self,
        kis_client: KisClient,
        *,
        headroom_seconds: float = _DEFAULT_REFRESH_HEADROOM_S,
        interval_seconds: float = _DEFAULT_REFRESH_INTERVAL_S,
    ) -> None:
        self._client            = kis_client
        self._headroom_seconds  = headroom_seconds
        self._interval_seconds  = interval_seconds
        self._task: asyncio.Task[None] | None = None
        self._refresh_count: int  = 0
        self._failure_count: int  = 0

    @property
    def refresh_count(self) -> int:
        return self._refresh_count

    @property
    def failure_count(self) -> int:
        return self._failure_count

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.is_running:
            return
        self._task = asyncio.create_task(self._loop(), name="kis_token_refresh")
        logger.info(
            "kis.refresh.loop_started",
            extra={
                "event":            "kis_refresh_loop_started",
                "headroom_seconds": self._headroom_seconds,
                "interval_seconds": self._interval_seconds,
            },
        )

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        logger.info(
            "kis.refresh.loop_stopped",
            extra={
                "event":     "kis_refresh_loop_stopped",
                "refreshes": self._refresh_count,
                "failures":  self._failure_count,
            },
        )

    def _needs_refresh(self) -> bool:
        """True iff `now + headroom >= token_expires_at` (or no token at all)."""
        expires_at = self._client.token_expires_at
        if expires_at is None:
            return True
        deadline = datetime.now(timezone.utc) + timedelta(seconds=self._headroom_seconds)
        return deadline >= expires_at

    async def _loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._interval_seconds)
                if not self._needs_refresh():
                    continue
                try:
                    await self._client.authenticate(force=True)
                    self._refresh_count += 1
                    logger.info(
                        "kis.refresh.success",
                        extra={
                            "event":           "kis_refresh_success",
                            "refresh_count":   self._refresh_count,
                            "new_expires_at":  (
                                self._client.token_expires_at.isoformat()
                                if self._client.token_expires_at else None
                            ),
                        },
                    )
                except KisError:
                    self._failure_count += 1
                    logger.exception(
                        "kis.refresh.failed",
                        extra={
                            "event":         "kis_refresh_failed",
                            "failure_count": self._failure_count,
                        },
                    )
        except asyncio.CancelledError:
            raise


def _tick_to_wire(tick: Tick) -> dict[str, Any]:
    """Convert domain Tick → MockPublisher-compatible JSON dict.

    Matches `MockPublisher._next_tick()` exactly:
        symbol str, ts ISO-8601 ms, price float, volume int, side str.

    Decimal → float at the boundary because JSON has no Decimal type and
    the existing wire format is `"price": 79000.0` (number, not string).
    KRW prices fit in float64 with full precision.
    """
    return {
        "symbol": tick.symbol,
        "ts":     tick.ts.isoformat(timespec="milliseconds"),
        "price":  float(tick.price),
        "volume": tick.volume,
        "side":   tick.side.value,
    }


class KisPublisher:
    """Background task: KIS WebSocket ticks → Redis pub/sub channel.

    Identical lifecycle to MockPublisher (start/stop/is_running) so the
    lifespan code in main.py treats them interchangeably. The KisClient
    must already be `connect()`-ed before `start()` is called — the
    publisher does NOT own auth or connection lifecycle, only the
    subscribe → stream → publish loop.
    """

    def __init__(
        self,
        client: redis.Redis,
        kis_client: KisClient,
        symbols: list[str],
        *,
        refresh_headroom_seconds: float = _DEFAULT_REFRESH_HEADROOM_S,
        refresh_interval_seconds: float = _DEFAULT_REFRESH_INTERVAL_S,
    ) -> None:
        self._client     = client
        self._kis        = kis_client
        self._symbols    = list(symbols)
        self._task: asyncio.Task[None] | None = None
        self._published: int = 0
        # Token refresh runs as a sibling task — independent of the
        # subscribe/stream loop so a transient OAuth blip doesn't kill
        # the WS, and a WS disconnect doesn't kill the refresh loop.
        self._refresh = _TokenRefreshLoop(
            kis_client,
            headroom_seconds=refresh_headroom_seconds,
            interval_seconds=refresh_interval_seconds,
        )

    @property
    def published_count(self) -> int:
        return self._published

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def refresh_count(self) -> int:
        """Lifetime count of successful access_token refreshes."""
        return self._refresh.refresh_count

    @property
    def refresh_failure_count(self) -> int:
        """Lifetime count of failed refresh attempts (logged + retried)."""
        return self._refresh.failure_count

    async def start(self) -> None:
        if self.is_running:
            return
        await self._refresh.start()
        self._task = asyncio.create_task(self._run(), name="kis_publisher")
        logger.info(
            "kis publisher task spawned",
            extra={
                "event":   "kis_publisher_start",
                "symbols": len(self._symbols),
                "channel": CHANNEL_TICK,
            },
        )

    async def stop(self) -> None:
        # Order matters: stop the refresh loop FIRST so it can't issue a
        # mid-shutdown REST call against a connection we're tearing down.
        await self._refresh.stop()
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        logger.info(
            "kis publisher stopped",
            extra={
                "event":           "kis_publisher_stop",
                "published_total": self._published,
                "refreshes":       self._refresh.refresh_count,
                "refresh_failures": self._refresh.failure_count,
            },
        )

    async def _run(self) -> None:
        try:
            await self._kis.subscribe(self._symbols)
            async for tick in self._kis.stream_ticks():
                payload = json.dumps(_tick_to_wire(tick))
                await self._client.publish(CHANNEL_TICK, payload)
                self._published += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            # Surface the failure with full traceback while keeping the
            # structured event for log routing — matches MockPublisher's
            # crash semantics so ops dashboards don't need a special case.
            logger.exception(
                "kis publisher loop crashed",
                extra={"event": "kis_publisher_error"},
            )
            raise
