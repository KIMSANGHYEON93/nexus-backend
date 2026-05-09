"""WebSocket fan-out — Redis Pub/Sub → connected NEXUS OS browsers.

Endpoint:  /v1/stream
    Versioned under v1 alongside the REST surface so contract bumps are
    coordinated. Browsers can't set Authorization headers on the WS
    upgrade, so production auth reads `?token=...`; in development with
    no Entra config the connection is accepted unauthenticated.

Topology:
    mock_publisher (or KIS adapter)  →  Redis channel
                                              │
                                              ├──▶  ws session #1
                                              ├──▶  ws session #2
                                              └──▶  ws session #N

Each connection runs two cooperating tasks:
    • forward_pubsub_to_ws  — async-for-iterates pubsub.listen() and
      writes every payload to the socket.
    • detect_client_close   — reads from the socket so a client TCP RST
      / orderly close raises WebSocketDisconnect we can act on.

asyncio.wait(FIRST_COMPLETED) joins them so the moment either side exits,
the other is cancelled and Redis subscriptions are released — no orphan
listeners after a flaky reload.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ...core.config import Settings, get_settings
from ...core.security import Principal
from ...infrastructure.redis_pubsub import (
    CHANNEL_ANOMALY,
    CHANNEL_QUOTE,
    CHANNEL_TICK,
    get_client,
)

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/v1", tags=["v1"])


WS_CLOSE_UNAUTHENTICATED = 4401   # custom 4xxx codes are app-defined per RFC 6455


async def _ws_principal(ws: WebSocket, settings: Settings) -> Principal | None:
    """Authenticate the WS upgrade. Returns None to reject.

    Dev / no-Entra: anonymous Principal (matches HTTP gate's dev bypass).
    Configured tenant + Token in `?token=...`: full validation TODO once
    we expose `_validate_token` from core.security as a reusable helper.
    """
    is_dev = settings.app_env == "development"
    has_entra = bool(settings.entra_tenant_id and settings.entra_client_id)

    if is_dev and not has_entra:
        return Principal(subject="anonymous", tenant="dev")

    token = ws.query_params.get("token")
    if not token:
        logger.warning(
            "ws: missing token on protected stream",
            extra={"event": "ws_auth_failed", "reason": "missing_token"},
        )
        return None

    # TODO(4c): wire to core.security._validate_token once it's promoted
    # to a public helper. Until then, a present-but-unverified token in a
    # configured tenant must FAIL CLOSED.
    logger.warning(
        "ws: token validation not yet implemented in production mode",
        extra={"event": "ws_auth_failed", "reason": "validation_pending"},
    )
    return None


@router.websocket("/stream")
async def market_stream(ws: WebSocket) -> None:
    settings = get_settings()
    principal = await _ws_principal(ws, settings)
    if principal is None:
        await ws.close(code=WS_CLOSE_UNAUTHENTICATED, reason="unauthenticated")
        return

    await ws.accept()
    redis_client = get_client()
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(CHANNEL_TICK, CHANNEL_QUOTE, CHANNEL_ANOMALY)
    logger.info(
        "ws.stream connected",
        extra={
            "event": "ws_connect",
            "subject": principal.subject,
            "client": f"{ws.client.host}:{ws.client.port}" if ws.client else "?",
        },
    )

    async def forward_pubsub_to_ws() -> None:
        """Pump every Redis pub/sub message to the socket."""
        try:
            async for message in pubsub.listen():
                # Skip subscribe/unsubscribe acks — only real messages forward.
                if message.get("type") != "message":
                    continue
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8", errors="replace")
                await ws.send_text(data)
        except WebSocketDisconnect:
            return

    async def detect_client_close() -> None:
        """Read from the socket — yields control until disconnect raises."""
        try:
            while True:
                # We don't expect inbound payloads; the read just exists to
                # surface a WebSocketDisconnect when the client goes away.
                await ws.receive_text()
        except WebSocketDisconnect:
            return

    forward_task = asyncio.create_task(forward_pubsub_to_ws(), name="ws_forward")
    detect_task  = asyncio.create_task(detect_client_close(), name="ws_detect")

    try:
        _, pending = await asyncio.wait(
            (forward_task, detect_task),
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, WebSocketDisconnect):
                pass
    finally:
        try:
            await pubsub.unsubscribe()
        except Exception:  # noqa: BLE001
            logger.exception("ws.stream: unsubscribe failed")
        try:
            await pubsub.close()
        except Exception:  # noqa: BLE001
            logger.exception("ws.stream: pubsub close failed")
        logger.info(
            "ws.stream disconnected",
            extra={"event": "ws_disconnect", "subject": principal.subject},
        )
