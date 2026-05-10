"""AuditPublisher — Sprint 5m one-line Redis publish for the trading pipeline.

`make_audit_publisher(redis_client)` returns the AuditPublisher callable
the TradingPipeline expects (matches the `AuditPublisher` type alias in
`domain.trading.pipeline`). Kept in infrastructure so the domain layer
has zero knowledge of Redis.

Why a tiny module rather than inlining: lets us swap the transport
(Kafka, NATS, etc.) without touching domain code, and gives tests a
clean seam to mock — they pass a recording callable into the pipeline
constructor instead of patching `redis.asyncio.Redis.publish`.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable

import redis.asyncio as redis

from .redis_pubsub import CHANNEL_AUDIT


def make_audit_publisher(
    redis_client: redis.Redis,
) -> Callable[[dict[str, object]], Awaitable[None]]:
    """Return an async callable that JSON-encodes + publishes the
    envelope on `nexus.trading.audit`."""

    async def _publish(envelope: dict[str, object]) -> None:
        await redis_client.publish(CHANNEL_AUDIT, json.dumps(envelope, default=str))

    return _publish
