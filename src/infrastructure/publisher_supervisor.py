"""PublisherSupervisor — Sprint 5d dual-write failsafe.

Owns whichever publisher is currently writing to the Redis tick channel
and swaps to a fallback at runtime when the primary fails irrecoverably.
The supervisor's promise to the rest of the system: as long as it is
running, *something* is publishing to `nexus.market.tick` (in dev mode)
so the canvas never goes silent.

State diagram:

    ┌─────────────────┐  has_kis_creds + bring-up OK
    │   start()       │ ───────────────────────────► [PRIMARY: KisPublisher]
    └────────┬────────┘
             │  bring-up FAIL                            │
             ▼                                           │ KisPublisher
       app_env=development?                              │ task dies
             │                                           │ (refresh
        yes  │  no                                       │  exhausted,
             ▼                                           │  WS broken,
   [FALLBACK: MockPublisher]   [NO PUBLISHER]            │  ...)
             ▲                                           │
             │                                           ▼
             └─────── watchdog observes failure ─── [FAILOVER]

The watchdog runs every `_WATCHDOG_INTERVAL_S`. Once it has failed over,
it exits — MockPublisher is generated locally and won't crash, so
no further supervision is needed for this lifespan.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Protocol

import redis.asyncio as redis

from ..core.config import Settings
from .kis_client import KisClient, KisError
from .kis_publisher import KisPublisher, TickObserver
from .mock_publisher import MockPublisher

logger = logging.getLogger(__name__)


# How often the watchdog checks the active publisher's health. 5s is a
# pragmatic compromise: fast enough that a dead canvas reconnects in well
# under "the user notices", slow enough that the loop is invisible in
# steady-state CPU profiles.
_WATCHDOG_INTERVAL_S = 5.0


class _PublisherLike(Protocol):
    """Minimal interface both KisPublisher and MockPublisher honor."""

    @property
    def is_running(self) -> bool: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...


class PublisherSupervisor:
    """Selects the right publisher at startup and fails over at runtime.

    Bring-up matrix:
        has_kis_creds=True  → try KisPublisher; on KisError fall through
        app_env=development → MockPublisher (always available in dev)
        production + no KIS → no publisher (canvas is intentionally blank)

    Runtime failover (watchdog):
        Active publisher's `is_running` flips to False outside `stop()`.
        Supervisor cancels the dead one (releases tokens / WS) and starts
        a MockPublisher on the same channel. Logged as `publisher_failover`
        with the reason captured.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        settings:     Settings,
        *,
        on_tick:      TickObserver | None = None,
    ) -> None:
        # `on_tick` is the Sprint 5h trading-pipeline hook. Wired by main.py
        # to whichever publisher comes up; survives KIS→mock failover so the
        # pipeline keeps reasoning even when synthetic data is in flight.
        self._redis    = redis_client
        self._settings = settings
        self._on_tick  = on_tick
        self._active: _PublisherLike | None     = None
        self._kis_client: KisClient | None      = None
        self._watchdog: asyncio.Task[None] | None = None
        self._failover_count: int = 0

    @property
    def active_kind(self) -> str:
        """Tag for observability — `kis`, `mock`, or `none`."""
        if isinstance(self._active, KisPublisher):
            return "kis"
        if isinstance(self._active, MockPublisher):
            return "mock"
        return "none"

    @property
    def failover_count(self) -> int:
        return self._failover_count

    @property
    def kis_client(self) -> "KisClient | None":
        """Expose the live KisClient for downstream consumers (e.g.
        KisBalanceClient). Returns None when KIS is not active or has
        failed over to MockPublisher."""
        return self._kis_client

    async def start(self) -> None:
        has_kis_creds = bool(self._settings.kis_app_key and self._settings.kis_app_secret)

        if has_kis_creds:
            kis_ok = await self._try_start_kis()
            if kis_ok:
                self._watchdog = asyncio.create_task(self._watch(), name="publisher_watchdog")
                return

        # Either no creds, or KIS bring-up failed → mock (dev only).
        if self._settings.app_env == "development":
            await self._start_mock(reason="no_kis_creds" if not has_kis_creds else "kis_bringup_failed")
        else:
            logger.info(
                "no publisher armed (prod mode without working KIS)",
                extra={
                    "event":         "supervisor_no_publisher",
                    "app_env":       self._settings.app_env,
                    "has_kis_creds": has_kis_creds,
                },
            )

    async def stop(self) -> None:
        if self._watchdog is not None:
            self._watchdog.cancel()
            try:
                await self._watchdog
            except asyncio.CancelledError:
                pass
            self._watchdog = None
        if self._active is not None:
            await self._active.stop()
            self._active = None
        if self._kis_client is not None:
            await self._kis_client.close()
            self._kis_client = None
        logger.info(
            "supervisor stopped",
            extra={"event": "supervisor_stop", "failovers": self._failover_count},
        )

    # ── Internals ──────────────────────────────────────────────────────

    async def _try_start_kis(self) -> bool:
        """Auth + connect + start KIS publisher. Returns False on KisError
        (caller falls back to MockPublisher). Cleans up partial state on
        failure so we don't leak a half-built KisClient."""
        kis_client = KisClient(self._settings)
        try:
            await kis_client.authenticate()
            await kis_client.connect()
        except KisError:
            logger.exception(
                "supervisor: KIS bring-up failed",
                extra={"event": "supervisor_kis_bringup_failed"},
            )
            try:
                await kis_client.close()
            except Exception:  # noqa: BLE001
                pass
            return False

        publisher = KisPublisher(
            self._redis,
            kis_client,
            self._settings.kis_subscribe_symbol_list,
            on_tick=self._on_tick,
        )
        await publisher.start()
        self._kis_client = kis_client
        self._active     = publisher
        logger.info(
            "supervisor: KIS publisher armed",
            extra={
                "event":   "supervisor_kis_armed",
                "kis_env": self._settings.kis_env,
                "symbols": len(self._settings.kis_subscribe_symbol_list),
            },
        )
        return True

    async def _start_mock(self, *, reason: str) -> None:
        publisher = MockPublisher(self._redis, on_tick=self._on_tick)
        await publisher.start()
        self._active = publisher
        logger.info(
            "supervisor: mock publisher armed",
            extra={"event": "supervisor_mock_armed", "reason": reason},
        )

    async def _watch(self) -> None:
        """Watchdog: poll active.is_running, fail over on flip-to-False."""
        try:
            while True:
                await asyncio.sleep(_WATCHDOG_INTERVAL_S)
                if self._active is None or self._active.is_running:
                    continue
                await self._failover_to_mock()
                # Mock doesn't die on its own — watchdog's job is done.
                return
        except asyncio.CancelledError:
            raise

    async def _failover_to_mock(self) -> None:
        """Tear down the dead KIS publisher and bring up MockPublisher."""
        self._failover_count += 1
        logger.error(
            "supervisor: KIS publisher died — failing over to MockPublisher",
            extra={
                "event":          "publisher_failover",
                "failover_count": self._failover_count,
            },
        )
        # Best-effort cleanup; don't raise out of the watchdog.
        if self._active is not None:
            try:
                await self._active.stop()
            except Exception:  # noqa: BLE001
                logger.exception(
                    "supervisor: failed to stop dead KIS publisher",
                    extra={"event": "supervisor_stop_dead_kis_failed"},
                )
        if self._kis_client is not None:
            try:
                await self._kis_client.close()
            except Exception:  # noqa: BLE001
                logger.exception(
                    "supervisor: failed to close KIS client during failover",
                    extra={"event": "supervisor_close_kis_failed"},
                )
            self._kis_client = None
        # In production we also failover (rather than going silent) — a dead
        # canvas is a worse signal than synthetic data on a known channel.
        await self._start_mock(reason="kis_runtime_failure")
