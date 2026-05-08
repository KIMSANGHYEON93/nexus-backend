"""WebSocket fan-out — Redis Pub/Sub → connected NEXUS OS browsers.

Each connection subscribes to the project's Redis channels and forwards
JSON-encoded frames as they arrive. We deliberately do not buffer per-
client; if a client falls behind we drop frames rather than risk OOM.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ...infrastructure.redis_pubsub import (
    CHANNEL_ANOMALY,
    CHANNEL_QUOTE,
    CHANNEL_TICK,
    get_client,
)

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/ws", tags=["websockets"])


@router.websocket("/market")
async def market_stream(ws: WebSocket) -> None:
    """Forward every tick/quote/anomaly frame to one browser session.

    The frontend opens one connection per dashboard tab. If you need
    per-symbol filtering, do it server-side here — pushing 100 % of the
    firehose to every browser does not scale past ~50 clients.
    """
    await ws.accept()
    redis_client = get_client()
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(CHANNEL_TICK, CHANNEL_QUOTE, CHANNEL_ANOMALY)
    logger.info("ws.market connected — peer=%s", ws.client)

    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message is None:
                # Heartbeat — keeps idle proxies (nginx, ELB) from cutting us.
                await ws.send_json({"type": "heartbeat"})
                continue
            await ws.send_text(message["data"])
    except WebSocketDisconnect:
        logger.info("ws.market disconnected — peer=%s", ws.client)
    except asyncio.CancelledError:
        raise
    finally:
        await pubsub.unsubscribe()
        await pubsub.close()
