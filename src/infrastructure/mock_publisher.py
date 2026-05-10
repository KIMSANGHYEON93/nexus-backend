"""Mock tick publisher — synthetic data for end-to-end pipeline verification.

Activated in lifespan when APP_ENV=development AND no KIS credentials are
configured. Generates ~2 ticks/sec across the 12 seeded KRX symbols and
publishes each as JSON to the Redis tick channel. The KIS adapter (Sprint
4c) will replace this once real credentials land — the channel name and
payload shape are stable so the WebSocket consumer + frontend BackendStreamer
do not need any changes when the cutover happens.

Why hardcode the symbol list (instead of reading from `entity` table):
the mock should run even when migrations are stale, and a DB read on
every iteration would couple the publisher to schema state we already
verify elsewhere via verify_schema().
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import redis.asyncio as redis

from ..domain.market.models import Tick, TickSide
from .redis_pubsub import CHANNEL_TICK


# Sprint 5h: shared TickObserver alias — same shape as KisPublisher's,
# so the supervisor can hand the same observer to either publisher
# without conditional code.
TickObserver = Callable[[Tick], Awaitable[None]]

logger = logging.getLogger(__name__)


# Mirrors db/seeds/dev.sql exactly. When 4c lands, the live adapter will
# UPDATE these same rows; until then this gives the canvas a steady stream.
_SEEDED_TICKERS: tuple[str, ...] = (
    # TECH
    "005930", "000660", "035420", "035720",
    # FINANCE
    "105560", "055550", "086790",
    # MANUFACTURING
    "005380", "005490", "051910",
    # BIO
    "207940", "068270",
)

# Realistic 2026-05 KRW levels — used as the random-walk anchor.
_BASE_PRICES: dict[str, float] = {
    "005930":      79_000.0,
    "000660":     197_000.0,
    "035420":     215_000.0,
    "035720":      50_500.0,
    "105560":      84_300.0,
    "055550":      47_900.0,
    "086790":      65_700.0,
    "005380":     245_000.0,
    "005490":     412_000.0,
    "051910":     387_000.0,
    "207940":   1_055_000.0,
    "068270":     198_500.0,
}

PUBLISH_INTERVAL_S: float = 0.5
"""Mean cadence — 2 ticks/sec aggregate across all 12 symbols."""


def _dict_to_tick(d: dict[str, Any]) -> Tick:
    """Wire-dict → domain Tick for handing to the trading-pipeline observer.

    Mirror inverse of `_tick_to_wire()` in kis_publisher: parses the same
    five fields back into a Pydantic Tick. Unknown side strings default to
    BUY rather than raising — the observer chain shouldn't crash on a
    malformed mock tick (defensive; mocks shouldn't produce them).
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

DRIFT_STD: float = 0.002
"""Per-step lognormal drift std (~0.2%). Tuned so a 60-tick window ~3 σ
band aligns with the frontend anomaly detector's calibration."""


class MockPublisher:
    """Async background task that drives synthetic ticks into Redis.

    Cancellation-aware: `stop()` cancels the inner task and awaits its
    exit so shutdown is deterministic. `start()` / `stop()` are both
    idempotent — useful when lifespan re-entry happens during HMR.
    """

    def __init__(
        self,
        client: redis.Redis,
        *,
        on_tick: TickObserver | None = None,
    ) -> None:
        self._client  = client
        self._on_tick = on_tick
        self._task: asyncio.Task[None] | None = None
        self._prices: dict[str, float] = dict(_BASE_PRICES)
        self._published: int = 0

    @property
    def published_count(self) -> int:
        """Lifetime tick count — useful for debug endpoints."""
        return self._published

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.is_running:
            return
        self._task = asyncio.create_task(self._run(), name="mock_publisher")
        logger.info(
            "mock publisher task spawned",
            extra={
                "event": "mock_publisher_start",
                "symbols": len(_SEEDED_TICKERS),
                "interval_s": PUBLISH_INTERVAL_S,
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
            "mock publisher stopped",
            extra={"event": "mock_publisher_stop", "published_total": self._published},
        )

    async def _run(self) -> None:
        try:
            while True:
                tick_dict = self._next_tick()
                payload = json.dumps(tick_dict)
                await self._client.publish(CHANNEL_TICK, payload)
                self._published += 1
                # Sprint 5h: feed the trading pipeline (if observer wired).
                # Convert the wire dict back to a Tick so the observer
                # signature is identical to the KisPublisher path.
                if self._on_tick is not None:
                    try:
                        await self._on_tick(_dict_to_tick(tick_dict))
                    except Exception:
                        logger.exception(
                            "mock publisher on_tick observer raised",
                            extra={"event": "mock_publisher_on_tick_error"},
                        )
                await asyncio.sleep(PUBLISH_INTERVAL_S)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Don't silently die — surface the failure with full traceback
            # while keeping the structured `event` for log routing.
            logger.exception(
                "mock publisher loop crashed",
                extra={"event": "mock_publisher_error"},
            )
            raise

    def _next_tick(self) -> dict[str, Any]:
        """Generate one synthetic tick. Geometric random walk so prices
        drift naturally rather than jittering around a fixed mean.

        Floor at 1.0 KRW guards against pathological drift sequences that
        could otherwise drive a price negative in a long-running session.
        """  # noqa: D401
        symbol = random.choice(_SEEDED_TICKERS)
        drift = random.gauss(0.0, 1.0) * DRIFT_STD
        self._prices[symbol] = max(1.0, self._prices[symbol] * (1.0 + drift))
        return {
            "symbol": symbol,
            "ts":     datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "price":  round(self._prices[symbol], 2),
            "volume": random.randint(100, 10_000),
            "side":   random.choice(("buy", "sell")),
        }
