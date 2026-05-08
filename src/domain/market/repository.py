"""Market domain repository — pure SQL behind a typed Python surface.

The router never touches asyncpg directly; it asks the repository for a
domain-shaped result. This keeps SQL changes localized and makes the
domain testable with a single fake (no in-memory Postgres needed).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

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
    async def list_edges(self, limit: int = 2000) -> list[dict]:
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
    async def latest_ohlc_1m(self, symbol: str, limit: int = 60) -> list[dict]:
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
