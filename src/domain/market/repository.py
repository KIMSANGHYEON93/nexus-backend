"""Market domain repository — pure SQL behind a typed Python surface.

The router never touches asyncpg directly; it asks the repository for a
domain-shaped result. This keeps SQL changes localized and makes the
domain testable with a single fake (no in-memory Postgres needed).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import asyncpg

from .models import Entity


class MarketRepository:
    """All read/write ops against the market_* and entity/edge tables."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ── Entities ────────────────────────────────────────────────────────
    async def list_entities(self, limit: int = 500) -> list[Entity]:
        rows = await self._pool.fetch(
            """
            SELECT id, cluster, anomaly, tx_vol
              FROM entity
             ORDER BY anomaly DESC
             LIMIT $1
            """,
            limit,
        )
        return [
            Entity(
                id=r["id"],
                cluster=r["cluster"],
                anomaly=r["anomaly"],
                tx_vol=r["tx_vol"],
            )
            for r in rows
        ]

    async def upsert_entity(self, entity: Entity) -> None:
        await self._pool.execute(
            """
            INSERT INTO entity (id, cluster, anomaly, tx_vol)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE
               SET cluster    = EXCLUDED.cluster,
                   anomaly    = EXCLUDED.anomaly,
                   tx_vol     = EXCLUDED.tx_vol,
                   updated_at = NOW()
            """,
            entity.id, entity.cluster, entity.anomaly, entity.tx_vol,
        )

    # ── Edges ───────────────────────────────────────────────────────────
    async def list_edges(self, limit: int = 2000) -> list[dict[str, Any]]:
        rows = await self._pool.fetch(
            """
            SELECT from_id AS "from", to_id AS "to", weight
              FROM edge
             ORDER BY updated_at DESC
             LIMIT $1
            """,
            limit,
        )
        return [dict(r) for r in rows]

    # ── Ticks (write path used by the KIS bridge) ───────────────────────
    async def insert_tick(
        self,
        symbol: str,
        ts: datetime,
        price: Decimal,
        volume: int,
        side: str,
    ) -> None:
        await self._pool.execute(
            """
            INSERT INTO market_tick (ts, symbol, price, volume, side)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (symbol, ts) DO NOTHING
            """,
            ts, symbol, price, volume, side,
        )

    # ── Latest 1m OHLC (continuous aggregate read) ──────────────────────
    async def latest_ohlc_1m(self, symbol: str, limit: int = 60) -> list[dict[str, Any]]:
        rows = await self._pool.fetch(
            """
            SELECT bucket, open, high, low, close, volume
              FROM market_tick_1m
             WHERE symbol = $1
             ORDER BY bucket DESC
             LIMIT $2
            """,
            symbol, limit,
        )
        return [dict(r) for r in rows]

    # ── Multi-symbol last-tick snapshot (Sprint 5p-D) ────────────────────
    # Surface for the KisLiveSnapshot HUD panel — operator needs to see
    # all 12 KIS subscriptions at once without clicking through each one.
    # `DISTINCT ON (symbol)` collapses to the newest tick per symbol in
    # a single index scan, much cheaper than 12 separate fetch_recent
    # calls. Falls back silently on DB error (same rule as fetch_recent
    # paths) so the HUD renders empty placeholders rather than 500-ing.
    #
    # Symbols filter is `= ANY($1)` — asyncpg maps Python list → Postgres
    # array, so we don't have to dynamically build a paramaterized IN list.
    async def snapshot_per_symbol(
        self,
        symbols: list[str],
    ) -> list[dict[str, Any]]:
        if not symbols:
            return []
        try:
            rows = await self._pool.fetch(
                """
                SELECT DISTINCT ON (symbol)
                       symbol, ts, price, volume, side
                  FROM market_tick
                 WHERE symbol = ANY($1)
                 ORDER BY symbol, ts DESC
                """,
                symbols,
            )
        except (
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            OSError, TimeoutError,
        ):
            return []
        return [
            {
                "symbol": r["symbol"],
                "ts":     r["ts"],
                "price":  float(r["price"]),
                "volume": int(r["volume"]),
                "side":   r["side"],
            }
            for r in rows
        ]

    # ── Cross-symbol tape (Sprint 5p-E) ──────────────────────────────────
    # Forensic surface — answers "what hit the wire between 09:34:50 and
    # 09:35:10 across all subscribed symbols?". `list_recent_ticks` is
    # per-symbol; this is its cross-symbol sibling, ORDER BY ts DESC over
    # the whole `symbol = ANY($1)` slice. Same `(symbol, ts DESC)` index
    # still services it — PG does an index scan per symbol then merges,
    # which is fine for 12 symbols × LIMIT 200 (~2400 fetched rows max).
    # Same fault-tolerance + Decimal→float edge cast as the rest.
    async def list_recent_tape(
        self,
        symbols: list[str],
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if not symbols or limit < 1:
            return []
        try:
            rows = await self._pool.fetch(
                """
                SELECT ts, symbol, price, volume, side
                  FROM market_tick
                 WHERE symbol = ANY($1)
                 ORDER BY ts DESC
                 LIMIT $2
                """,
                symbols, limit,
            )
        except (
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            OSError, TimeoutError,
        ):
            return []
        return [
            {
                "ts":     r["ts"],
                "symbol": r["symbol"],
                "price":  float(r["price"]),
                "volume": int(r["volume"]),
                "side":   r["side"],
            }
            for r in rows
        ]

    # ── Recent raw ticks (Sprint 5p-C) ───────────────────────────────────
    # Sprint 5p-C: tick-level read path for the PropertyHUD price sparkline.
    # The continuous-aggregate path above is rounded to 1-minute buckets and
    # only useful for longer windows; the live HUD wants per-tick resolution
    # so the operator can see the wiggle inside the current minute. Backed
    # by the `(symbol, ts DESC)` index from migration 001; LIMIT keeps the
    # range scan bounded even when the table grows.
    #
    # Read path is fault-tolerant by the same rule as ExecutionRepository.
    # fetch_recent: PG/interface/OS/timeout errors all collapse to [] so a
    # transient outage shows an empty sparkline instead of 500-ing the HUD.
    async def list_recent_ticks(
        self,
        symbol: str,
        limit: int = 60,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            return []
        try:
            rows = await self._pool.fetch(
                """
                SELECT ts, price, volume, side
                  FROM market_tick
                 WHERE symbol = $1
                 ORDER BY ts DESC
                 LIMIT $2
                """,
                symbol, limit,
            )
        except (
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            OSError, TimeoutError,
        ):
            # Repo silently degrades — the router converts to a clean empty
            # response. Operators see "no ticks" in the HUD instead of a
            # confusing error chip; ops sees the underlying cause in DB logs.
            return []
        # `price` is NUMERIC in the schema — asyncpg returns Decimal. The
        # router serializes via pydantic Decimal handling, but the HUD wants
        # a float for sparkline math, so we cast at the edge.
        return [
            {
                "ts":     r["ts"],
                "price":  float(r["price"]),
                "volume": int(r["volume"]),
                "side":   r["side"],
            }
            for r in rows
        ]
