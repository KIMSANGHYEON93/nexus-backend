"""Redis Pub/Sub broker — fan-out from KIS adapter to N WebSocket clients.

Topology:
    kis_client  ──publish──▶  Redis channel "nexus.market.tick"
                                          │
                                          ├──▶  ws session #1 (browser A)
                                          ├──▶  ws session #2 (browser B)
                                          └──▶  ws session #N

Why Redis instead of in-process broadcasting: this lets the API tier scale
horizontally — KIS ticks arrive on whichever node holds the upstream
WebSocket and are delivered to clients connected to *any* node.
"""

from __future__ import annotations

import redis.asyncio as redis

from ..core.config import Settings


CHANNEL_TICK = "nexus.market.tick"
CHANNEL_QUOTE = "nexus.market.quote"
CHANNEL_ANOMALY = "nexus.analysis.anomaly"
# Sprint 5m: every TradingPipeline decision lands here as a JSON envelope
# (action + confidence + score + executor outcome). Consumed by the
# PersistenceWorker for the execution_audit hypertable.
CHANNEL_AUDIT = "nexus.trading.audit"


_client: redis.Redis | None = None


async def init_client(settings: Settings) -> redis.Redis:
    """Create the global Redis client. Called once during FastAPI startup."""
    global _client
    if _client is not None:
        return _client
    # redis-py 5's `from_url` factory ships untyped in current stubs (it's a
    # classmethod that returns Self via runtime introspection); annotate the
    # left-hand side and silence the narrow no-untyped-call warning.
    client: redis.Redis = redis.from_url(  # type: ignore[no-untyped-call]
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,
    )
    _client = client
    await _client.ping()
    return _client


async def close_client() -> None:
    """Close the client. Called during FastAPI shutdown."""
    global _client
    if _client is None:
        return
    await _client.aclose()
    _client = None


def get_client() -> redis.Redis:
    """Return the live Redis client. Raises if accessed before startup."""
    if _client is None:
        raise RuntimeError(
            "Redis client not initialized — init_client() must run during app startup"
        )
    return _client
