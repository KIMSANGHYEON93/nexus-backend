"""Korea Investment & Securities (한국투자증권) OpenAPI adapter.

This is the basecamp stub. It captures the contract — connect, subscribe,
parse pipe-delimited frames, publish to Redis — without yet binding to
the live endpoints. The frontend `HanTooStreamer` already implements the
mirror of this state machine so the two will line up cleanly.

Transport notes:
  • Real-time WebSocket: wss://openapi.koreainvestment.com:9443/ws
    (paper: wss://openapivts.koreainvestment.com:31000/ws)
  • REST OAuth: POST /oauth2/tokenP for app-level access tokens (24h)
  • Frame format: `0|H0STCNT0|<len>|<csv-data>`  (체결 — execution ticks)
                   `0|H0STASP0|<len>|<csv-data>`  (호가 — best bid/ask)
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from enum import Enum

from ..core.config import Settings

logger = logging.getLogger(__name__)


class KisConnectionState(str, Enum):
    DISCONNECTED = "disconnected"
    AUTHENTICATING = "authenticating"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"


class KisClient:
    """Lifecycle owner for the upstream KIS WebSocket session.

    Mirrors the frontend `HanTooStreamer` state machine so debugging across
    the stack uses the same vocabulary. `subscribe()` returns an async
    iterator over normalized frames; the caller (typically a background
    task in `main.py`) republishes them to Redis.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._state = KisConnectionState.DISCONNECTED
        self._access_token: str | None = None

    @property
    def state(self) -> KisConnectionState:
        return self._state

    async def authenticate(self) -> None:
        """Exchange app key/secret for a 24h access token (REST)."""
        self._state = KisConnectionState.AUTHENTICATING
        # TODO(phase-4c): POST /oauth2/tokenP, store self._access_token + expiry.
        logger.info("KIS auth stub — env=%s", self._settings.kis_env)

    async def connect(self) -> None:
        """Open the WebSocket session and send the approval handshake."""
        self._state = KisConnectionState.CONNECTING
        # TODO(phase-4c): websockets.connect(URL, extra_headers={...})
        self._state = KisConnectionState.CONNECTED
        logger.info("KIS websocket stub — connected")

    async def subscribe(self, symbols: list[str]) -> AsyncIterator[dict]:
        """Yield normalized tick/quote frames from the live stream.

        Stub yields nothing and exits cleanly so the rest of the app can
        boot end-to-end without live credentials.
        """
        if self._state is not KisConnectionState.CONNECTED:
            raise RuntimeError(f"KIS client not connected (state={self._state.value})")
        logger.info("KIS subscribe stub — symbols=%s", symbols)
        await asyncio.sleep(0)
        return
        yield  # pragma: no cover  — keeps the function an async generator

    async def close(self) -> None:
        self._state = KisConnectionState.DISCONNECTED
        logger.info("KIS websocket stub — closed")
