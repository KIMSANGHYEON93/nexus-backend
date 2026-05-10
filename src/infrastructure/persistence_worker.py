"""PersistenceWorker — Sprint 5m off-hot-path DB writer.

THE CRITICAL ARCHITECTURE PROMISE: the trading hot path
(KisPublisher.on_tick → TradingPipeline.on_tick → coordinator → guards →
executor) NEVER awaits a DB write. Instead, the publisher publishes ticks
to Redis (already does, since 5c) and the TradingPipeline publishes audit
JSON envelopes to a new Redis channel after every executor invocation.

This module is the consumer of those two channels:

    nexus.market.tick   ── consumed ──▶  buffer ──▶ TickRepository.insert_batch
    nexus.trading.audit ── consumed ──▶            ExecutionRepository.insert

Two sibling tasks (one per channel) so a slow audit insert can't stall
tick batching and vice-versa. Both tasks survive every recoverable
failure mode:

  • DB connection drop      → repo.insert returns 0/False, log + drop batch
  • Redis subscription drop → reconnect via pubsub.listen() loop
  • Malformed JSON          → log + skip
  • Anything else (catch-all) → log + continue; never crash the worker

Backpressure: tick buffer is bounded — when the DB is down for an extended
window, we cap memory at `_TICK_BUFFER_MAX` and drop the OLDEST ticks
(prefer fresh data; old failed batches are already lost from a write
perspective). Counters expose visibility:
  ticks_inserted / ticks_dropped / executions_inserted / executions_failed.

Lifecycle owned by main.py — start() spawns two tasks, stop() cancels
+ flushes the pending tick batch one last time so we don't lose ≤ 500
ticks on graceful shutdown.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import datetime
from decimal import Decimal
from typing import Any

import redis.asyncio as redis

from ..domain.market.models import Tick, TickSide
from .execution_repository import ExecutionRepository
from .redis_pubsub import CHANNEL_AUDIT, CHANNEL_TICK
from .tick_repository import TickRepository

logger = logging.getLogger(__name__)


# ── Tunables ────────────────────────────────────────────────────────────
# Max ticks held in the worker's buffer before flushing. 500 chosen so
# at the seeded universe rate (~24 ticks/sec) the buffer flushes every
# ~20s — frequent enough that crash window loses < half a minute, sparse
# enough that DB doesn't see > 1 INSERT every several seconds.
_TICK_FLUSH_THRESHOLD = 500
# Backstop flush interval — even if traffic is below threshold, force a
# flush every N seconds so a quiet symbol's last tick isn't held for
# minutes. Balances tail latency against round-trip cost.
_TICK_FLUSH_INTERVAL_S = 5.0
# Buffer cap — when DB is down and ticks pile up, cap memory growth.
# At this point we drop the OLDEST tick to make room for the newest;
# fresh data has higher decision-value than old un-persisted data.
_TICK_BUFFER_MAX = 5000


def _dict_to_tick(d: dict[str, Any]) -> Tick:
    """Mirror inverse of `kis_publisher._tick_to_wire()`. Defensive on
    unknown side strings (defaults to BUY rather than raising) so a
    malformed publisher message can't crash the persistence loop."""
    side_str = str(d.get("side", "buy")).lower()
    side = TickSide.SELL if side_str == "sell" else TickSide.BUY
    return Tick(
        symbol = str(d["symbol"]),
        ts     = datetime.fromisoformat(str(d["ts"])),
        price  = Decimal(str(d["price"])),
        volume = int(d["volume"]),
        side   = side,
    )


class PersistenceWorker:
    """Owns the two consumer tasks + tick buffer + repos.

    `start()` spawns:
      • _tick_loop  — subscribe nexus.market.tick → buffer → batch insert
      • _audit_loop — subscribe nexus.trading.audit → insert each row
    `stop()` cancels both, then flushes any remaining buffered ticks
    one last time so a clean shutdown doesn't lose pending writes.
    """

    def __init__(
        self,
        redis_client:        redis.Redis,
        tick_repo:           TickRepository,
        execution_repo:      ExecutionRepository,
        *,
        flush_threshold:     int   = _TICK_FLUSH_THRESHOLD,
        flush_interval_s:    float = _TICK_FLUSH_INTERVAL_S,
        buffer_max:          int   = _TICK_BUFFER_MAX,
    ) -> None:
        if flush_threshold < 1:
            raise ValueError(f"flush_threshold must be >= 1, got {flush_threshold}")
        if flush_interval_s <= 0:
            raise ValueError(f"flush_interval_s must be > 0, got {flush_interval_s}")
        if buffer_max < flush_threshold:
            raise ValueError(
                f"buffer_max ({buffer_max}) must be >= flush_threshold ({flush_threshold})"
            )

        self._redis             = redis_client
        self._tick_repo         = tick_repo
        self._execution_repo    = execution_repo
        self._flush_threshold   = flush_threshold
        self._flush_interval_s  = flush_interval_s
        self._buffer:           deque[Tick] = deque(maxlen=buffer_max)
        self._tick_task:        asyncio.Task[None] | None = None
        self._audit_task:       asyncio.Task[None] | None = None
        # Counters for observability — exposed via /v1/readyz extension
        # in Sprint 5n+ if desired; for now used by tests + structured logs.
        self._ticks_inserted:    int = 0
        self._ticks_dropped:     int = 0
        self._executions_inserted: int = 0
        self._executions_failed:  int = 0

    @property
    def is_running(self) -> bool:
        return any(
            t is not None and not t.done()
            for t in (self._tick_task, self._audit_task)
        )

    @property
    def buffer_size(self) -> int:
        return len(self._buffer)

    @property
    def ticks_inserted(self) -> int:
        return self._ticks_inserted

    @property
    def ticks_dropped(self) -> int:
        return self._ticks_dropped

    @property
    def executions_inserted(self) -> int:
        return self._executions_inserted

    @property
    def executions_failed(self) -> int:
        return self._executions_failed

    async def start(self) -> None:
        if self._tick_task is None or self._tick_task.done():
            self._tick_task  = asyncio.create_task(self._tick_loop(), name="persist_ticks")
        if self._audit_task is None or self._audit_task.done():
            self._audit_task = asyncio.create_task(self._audit_loop(), name="persist_audit")
        logger.info(
            "persistence.worker_started",
            extra={
                "event":            "persistence_worker_started",
                "flush_threshold":  self._flush_threshold,
                "flush_interval_s": self._flush_interval_s,
            },
        )

    async def stop(self) -> None:
        for t in (self._tick_task, self._audit_task):
            if t is None:
                continue
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        self._tick_task  = None
        self._audit_task = None
        # Final drain — never lose what's in the buffer at shutdown.
        if self._buffer:
            drained = list(self._buffer)
            self._buffer.clear()
            inserted = await self._tick_repo.insert_batch(drained)
            self._ticks_inserted += inserted
            if inserted < len(drained):
                self._ticks_dropped += len(drained) - inserted
        logger.info(
            "persistence.worker_stopped",
            extra={
                "event":              "persistence_worker_stopped",
                "ticks_inserted":     self._ticks_inserted,
                "ticks_dropped":      self._ticks_dropped,
                "executions_inserted": self._executions_inserted,
                "executions_failed":  self._executions_failed,
            },
        )

    # ── Tick consumer ──────────────────────────────────────────────────

    async def _tick_loop(self) -> None:
        """Subscribe to CHANNEL_TICK, buffer messages, flush on threshold
        or interval. Reconnects on Redis subscription drop with backoff;
        DB failures don't kill the loop (repo returns 0 on error)."""
        try:
            while True:
                try:
                    await self._tick_consume_session()
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 — defensive, never die
                    logger.exception(
                        "persistence.tick_session_crashed",
                        extra={"event": "persistence_tick_session_crashed"},
                    )
                    await asyncio.sleep(2.0)
                    # Loop back and re-subscribe.
        except asyncio.CancelledError:
            raise

    async def _tick_consume_session(self) -> None:
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(CHANNEL_TICK)
        try:
            last_flush = asyncio.get_event_loop().time()
            while True:
                # Bounded read so the time-based flush gets a chance even
                # in a chatty stream — get_message returns None on timeout.
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=self._flush_interval_s,
                )
                now = asyncio.get_event_loop().time()
                if msg is not None and msg.get("type") == "message":
                    self._enqueue_tick(msg["data"])
                    if len(self._buffer) >= self._flush_threshold:
                        await self._flush_ticks()
                        last_flush = now
                # Time-based flush even if no message hit threshold.
                if (now - last_flush) >= self._flush_interval_s and self._buffer:
                    await self._flush_ticks()
                    last_flush = now
        finally:
            try:
                await pubsub.unsubscribe()
                await pubsub.close()
            except Exception:  # noqa: BLE001
                pass

    def _enqueue_tick(self, raw: Any) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw)
            tick = _dict_to_tick(payload)
        except (ValueError, KeyError, TypeError) as exc:
            logger.warning(
                "persistence.tick_parse_failed",
                extra={
                    "event":      "persistence_tick_parse_failed",
                    "error_type": type(exc).__name__,
                    "preview":    str(raw)[:120],
                },
            )
            return
        # deque(maxlen=N) silently evicts oldest on append once full.
        # Detect the eviction by length-stays-constant after a successful
        # append so we can count drops accurately.
        was_full = len(self._buffer) == self._buffer.maxlen
        self._buffer.append(tick)
        if was_full:
            self._ticks_dropped += 1

    async def _flush_ticks(self) -> None:
        if not self._buffer:
            return
        batch = list(self._buffer)
        self._buffer.clear()
        inserted = await self._tick_repo.insert_batch(batch)
        self._ticks_inserted += inserted
        # Repo returning < batch_size means DB rejected — count as drops.
        # Don't re-buffer; preserves the "fresh wins" invariant.
        if inserted < len(batch):
            self._ticks_dropped += len(batch) - inserted

    # ── Audit consumer ─────────────────────────────────────────────────

    async def _audit_loop(self) -> None:
        try:
            while True:
                try:
                    await self._audit_consume_session()
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "persistence.audit_session_crashed",
                        extra={"event": "persistence_audit_session_crashed"},
                    )
                    await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            raise

    async def _audit_consume_session(self) -> None:
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(CHANNEL_AUDIT)
        try:
            async for msg in pubsub.listen():
                if msg.get("type") != "message":
                    continue
                await self._handle_audit(msg["data"])
        finally:
            try:
                await pubsub.unsubscribe()
                await pubsub.close()
            except Exception:  # noqa: BLE001
                pass

    async def _handle_audit(self, raw: Any) -> None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            envelope = json.loads(raw)
        except ValueError as exc:
            logger.warning(
                "persistence.audit_parse_failed",
                extra={
                    "event":      "persistence_audit_parse_failed",
                    "error_type": type(exc).__name__,
                    "preview":    str(raw)[:120],
                },
            )
            self._executions_failed += 1
            return
        ok = await self._execution_repo.insert(envelope)
        if ok:
            self._executions_inserted += 1
        else:
            self._executions_failed += 1
