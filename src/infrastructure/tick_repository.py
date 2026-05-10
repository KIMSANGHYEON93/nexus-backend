"""Tick repository — Sprint 5m batched writer for the market_tick hypertable.

Owned exclusively by the PersistenceWorker (off-hot-path). The trading
pipeline never calls this directly — it publishes to Redis and the worker
consumes + batches. Batching matters because tick rates can spike to
hundreds/sec during volatile periods and a per-tick INSERT round-trip
would dominate Postgres CPU.

Why asyncpg `executemany` instead of `copy_records_to_table`:
  • COPY is faster but bypasses our PRIMARY KEY (symbol, ts) UPSERT
    semantics — duplicates would either raise or get silently inserted
    twice depending on conflict mode.
  • `executemany` with `ON CONFLICT DO NOTHING` lets us replay safely
    if the worker is restarted mid-batch (e.g. crash recovery).

The worker is the only writer to `market_tick`, so concurrency is
single-tasked — no need for transaction isolation gymnastics.
"""

from __future__ import annotations

import logging

import asyncpg

from ..domain.market.models import Tick

logger = logging.getLogger(__name__)


# Insert query — `ON CONFLICT (symbol, ts) DO NOTHING` makes the call
# idempotent against worker restarts that re-process the same tick.
# `executemany` over this is much faster than per-tick `execute()`.
_INSERT_TICK_SQL = """
    INSERT INTO market_tick (ts, symbol, price, volume, side)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (symbol, ts) DO NOTHING
"""


class TickRepository:
    """Owner of the asyncpg pool's connection-acquire lifecycle for ticks."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def insert_batch(self, ticks: list[Tick]) -> int:
        """Insert N ticks in one round-trip. Returns count attempted.

        On asyncpg.PostgresError (connection drop, deadlock, schema-version
        mismatch on a stale chunk, etc.) the call logs + returns 0 — never
        raises. The caller (PersistenceWorker) treats 0 as "this batch lost"
        and continues — our promise is uptime over forensic completeness
        when the DB is hiccuping.
        """
        if not ticks:
            return 0
        rows = [
            (t.ts, t.symbol, t.price, t.volume, t.side.value)
            for t in ticks
        ]
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(_INSERT_TICK_SQL, rows)
        except (
            asyncpg.PostgresError,    # server-side errors (deadlock, etc.)
            asyncpg.InterfaceError,   # connection drops, pool exhaustion
            OSError, TimeoutError,
        ) as exc:
            logger.warning(
                "tick_repo.insert_failed",
                extra={
                    "event":      "tick_repo_insert_failed",
                    "batch_size": len(rows),
                    "error_type": type(exc).__name__,
                    "error":      str(exc)[:200],
                },
            )
            return 0
        logger.debug(
            "tick_repo.insert_ok",
            extra={"event": "tick_repo_insert_ok", "batch_size": len(rows)},
        )
        return len(rows)
