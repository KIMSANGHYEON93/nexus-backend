"""Korea Investment & Securities (한국투자증권) OpenAPI adapter.

Owns the upstream KIS session lifecycle: REST OAuth handshake (this module
implements that today), WebSocket subscription, frame parsing, and Redis
re-publication. State transitions mirror the frontend `HanTooStreamer`
state machine so logs across the stack share vocabulary.

Transport:
  • REST OAuth      — POST /oauth2/tokenP (this file)
  • Real-time WS    — wss://...koreainvestment.com:.../ws
  • Frame format    — `0|H0STCNT0|<len>|<csv-data>` (체결)
                       `0|H0STASP0|<len>|<csv-data>` (호가)
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

import httpx

from ..core.config import Settings

logger = logging.getLogger(__name__)


# Paper trading and live get distinct host:port pairs but identical paths.
_REST_HOSTS: dict[str, str] = {
    "paper": "https://openapivts.koreainvestment.com:29443",
    "live":  "https://openapi.koreainvestment.com:9443",
}

# KIS issues 24h tokens. Refresh 5 min early so an in-flight WS subscribe
# never collides with a token rotation cliff.
_REFRESH_MARGIN = timedelta(minutes=5)

# KIS sends `access_token_token_expired` as KST wall-clock without a TZ
# suffix; treat it as UTC+9 and store UTC internally.
_KST = timezone(timedelta(hours=9))


class KisAuthError(Exception):
    """OAuth handshake failed (HTTP error, malformed body, network).

    Constructed with a short, operator-facing reason. Never wraps the
    appkey/appsecret values; the structured `auth_failed` log line emitted
    at the call site carries sanitized context (status, kis rt_cd, msg1).
    The generic 500 problem+json handler catches it if it ever bubbles to
    a request boundary; for now it surfaces in lifespan startup logs only.
    """


class KisConnectionState(str, Enum):
    DISCONNECTED   = "disconnected"
    AUTHENTICATING = "authenticating"
    CONNECTING     = "connecting"
    CONNECTED      = "connected"
    RECONNECTING   = "reconnecting"
    FAILED         = "failed"


class KisClient:
    """Lifecycle owner for the upstream KIS session.

    `authenticate()` populates `_access_token` + `_access_token_expires_at`.
    `connect()` (Sprint 5c step 2) opens the WebSocket using that token.
    `subscribe()` (step 3) yields normalized frames for Redis re-publish.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings
        self._state: KisConnectionState = KisConnectionState.DISCONNECTED
        self._access_token: str | None = None
        self._access_token_expires_at: datetime | None = None
        # Tests inject an AsyncClient bound to httpx.MockTransport. Production
        # gets a fresh client owned by this instance and closed by aclose().
        self._http = http_client or httpx.AsyncClient(timeout=10.0)
        self._owns_http = http_client is None

    @property
    def state(self) -> KisConnectionState:
        return self._state

    @property
    def access_token(self) -> str | None:
        return self._access_token

    @property
    def access_token_expires_at(self) -> datetime | None:
        return self._access_token_expires_at

    def _rest_base(self) -> str:
        return _REST_HOSTS[self._settings.kis_env]

    def _token_is_fresh(self) -> bool:
        if self._access_token is None or self._access_token_expires_at is None:
            return False
        return datetime.now(timezone.utc) < self._access_token_expires_at - _REFRESH_MARGIN

    async def authenticate(self) -> None:
        """Exchange app key/secret for a 24h access token (REST OAuth2).

        Idempotent: a fresh cached token short-circuits the network call.
        On HTTP 4xx/5xx, malformed body, or network error: state →
        ``FAILED`` and `KisAuthError` is raised. Caller decides whether to
        retry (Sprint 5d adds backoff at the lifespan level).
        """
        if self._token_is_fresh():
            logger.debug(
                "kis auth — cached token still fresh",
                extra={"event": "kis_auth_cached", "kis_env": self._settings.kis_env},
            )
            return

        self._state = KisConnectionState.AUTHENTICATING
        url = f"{self._rest_base()}/oauth2/tokenP"
        payload = {
            "grant_type": "client_credentials",
            "appkey":     self._settings.kis_app_key,
            "appsecret":  self._settings.kis_app_secret,
        }
        logger.info(
            "kis auth — attempt",
            extra={"event": "kis_auth_attempt", "kis_env": self._settings.kis_env},
        )

        try:
            response = await self._http.post(url, json=payload)
        except httpx.HTTPError as exc:
            self._state = KisConnectionState.FAILED
            logger.error(
                "kis auth — network failure",
                extra={
                    "event":   "kis_auth_failed",
                    "reason":  "network",
                    "kis_env": self._settings.kis_env,
                    "exc":     type(exc).__name__,
                },
            )
            raise KisAuthError(f"KIS OAuth network failure ({type(exc).__name__})") from exc

        if response.status_code != 200:
            self._state = KisConnectionState.FAILED
            # KIS returns its own structured error body — surface rt_cd /
            # msg1 in the log without echoing the request payload.
            try:
                body: dict[str, Any] = response.json()
            except ValueError:
                body = {}
            logger.error(
                "kis auth — rejected",
                extra={
                    "event":     "kis_auth_failed",
                    "reason":    "rejected",
                    "status":    response.status_code,
                    "kis_rt_cd": body.get("rt_cd"),
                    "kis_msg1":  body.get("msg1"),
                    "kis_env":   self._settings.kis_env,
                },
            )
            raise KisAuthError(
                f"KIS OAuth rejected (HTTP {response.status_code}: "
                f"{body.get('msg1') or 'no msg'})"
            )

        try:
            body = response.json()
            token = body["access_token"]
            expired_at_str = body["access_token_token_expired"]
        except (ValueError, KeyError) as exc:
            self._state = KisConnectionState.FAILED
            logger.error(
                "kis auth — malformed response",
                extra={"event": "kis_auth_failed", "reason": "malformed", "kis_env": self._settings.kis_env},
            )
            raise KisAuthError("KIS OAuth response missing required fields") from exc

        try:
            naive = datetime.strptime(expired_at_str, "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            self._state = KisConnectionState.FAILED
            raise KisAuthError(f"KIS OAuth expiry timestamp unparseable: {expired_at_str!r}") from exc
        expires_at = naive.replace(tzinfo=_KST).astimezone(timezone.utc)

        self._access_token = token
        self._access_token_expires_at = expires_at
        # Stay in AUTHENTICATING — the token is cached but the WebSocket is
        # not yet open. `connect()` advances to CONNECTING.
        logger.info(
            "kis auth — success",
            extra={
                "event":      "kis_auth_success",
                "kis_env":    self._settings.kis_env,
                "expires_at": expires_at.isoformat(),
                # Mask the token: full value never appears in logs.
                "token_head": token[:6] + "…",
            },
        )

    async def connect(self) -> None:
        """Open the WebSocket session and send the approval handshake."""
        self._state = KisConnectionState.CONNECTING
        # TODO(sprint-5c-step-2): websockets.connect(URL, extra_headers={...})
        self._state = KisConnectionState.CONNECTED
        logger.info("KIS websocket stub — connected")

    async def subscribe(self, symbols: list[str]) -> AsyncIterator[dict[str, Any]]:
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

    async def aclose(self) -> None:
        """Release the owned httpx client, if any."""
        if self._owns_http:
            await self._http.aclose()
