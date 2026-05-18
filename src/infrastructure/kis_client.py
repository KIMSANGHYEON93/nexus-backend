"""Korea Investment & Securities (한국투자증권) OpenAPI adapter.

Sprint 5c covers the full live-KIS cutover:
  • Step 1 (done) — `authenticate()` POST /oauth2/tokenP → 24h access_token
  • Step 2 (this) — `connect()` POST /oauth2/Approval → approval_key,
                    open WebSocket to KIS gateway, ready for subscribe()
  • Step 3 (next) — `subscribe()` send tr_id frames, parse pipe-delimited
                    ticks, republish to Redis channel

Two distinct credentials (don't conflate them):
  • access_token  — REST Bearer (24h, rate-limited 1/min), via /oauth2/tokenP
  • approval_key  — WebSocket subscribe header, via /oauth2/Approval
                    (request body uses field name `secretkey`, NOT `appsecret`)

Transport endpoints:
  REST  paper → https://openapivts.koreainvestment.com:29443
        live  → https://openapi.koreainvestment.com:9443
  WS    paper → ws://ops.koreainvestment.com:31000
        live  → ws://ops.koreainvestment.com:21000
  (KIS uses unencrypted ws:// on the realtime gateway by design — the
   approval_key is the per-session credential, single-use in-flight.)

Frame format on the WebSocket (after subscribe):
  `0|H0STCNT0|<len>|<csv-data>`  체결 — execution ticks
  `0|H0STASP0|<len>|<csv-data>`  호가 — best bid/ask
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

import httpx
from websockets.asyncio.client import ClientConnection, connect as ws_connect
from websockets.exceptions import ConnectionClosed

from ..core.config import Settings
from ..domain.market.models import Quote, QuoteLevel, Tick, TickSide

logger = logging.getLogger(__name__)


# ── REST endpoints ──────────────────────────────────────────────────────
_KIS_REST_BASE = {
    "paper": "https://openapivts.koreainvestment.com:29443",
    "live":  "https://openapi.koreainvestment.com:9443",
}
_OAUTH_PATH    = "/oauth2/tokenP"
_APPROVAL_PATH = "/oauth2/Approval"
# 24h tokens — KIS rate-limits OAuth aggressively. 10s is generous for the
# round-trip; httpx default of 5s would flap on first-call warm-up.
_OAUTH_TIMEOUT_SECONDS = 10.0


# ── WebSocket endpoints ─────────────────────────────────────────────────
# KIS uses a different host and port for the realtime gateway than the REST
# API. ws:// not wss:// — the approval_key is the per-session credential.
_KIS_WS_URL = {
    "paper": "ws://ops.koreainvestment.com:31000",
    "live":  "ws://ops.koreainvestment.com:21000",
}
# WS open includes a TLS-style handshake; 10s tolerates a cold-cache DNS
# lookup + the gateway's leisurely first-message timing.
_WS_OPEN_TIMEOUT_SECONDS = 10.0


# ── KIS realtime tr_ids (subscribe topics) ──────────────────────────────
TR_ID_TICK  = "H0STCNT0"   # 체결 (executions / trades)
TR_ID_QUOTE = "H0STASP0"   # 호가 (bid/ask snapshots) — Sprint 5d uses this

# H0STCNT0 record field layout per KIS spec (46 fields per record).
# We only consume the five fields below — the others are intentionally
# ignored to keep the wire contract narrow. If KIS reorders or extends
# the spec, only this constant block changes.
_H0STCNT0_FIELDS = 46
_FLD_SYMBOL    = 0    # MKSC_SHRN_ISCD  — 6-digit KRX ticker
_FLD_TIME_HMS  = 1    # STCK_CNTG_HOUR  — HHMMSS in KST
_FLD_PRICE     = 2    # STCK_PRPR       — current execution price
_FLD_TICK_VOL  = 12   # CNTG_VOL        — volume of this tick (NOT cumulative)
_FLD_CCLD_DVSN = 21   # CCLD_DVSN       — "1"=buyer-initiated, "5"=seller-initiated

# H0STASP0 caret-delimited field indices (5-level order book).
# ⚠ VERIFY: 실제 H0STASP0 응답 프레임에서 확인할 것.
# 일부 KIS 버전은 ASKP1-10 (idx 2-11) 다음 BIDP1-10 (idx 12-21) 순이다.
# 그 경우 _FLD_ASP_BIDP_START=12, _FLD_ASP_ASKRSQN_START=22, _FLD_ASP_BIDRSQN_START=32
_FLD_ASP_SYMBOL     = 0
_FLD_ASP_TIME       = 1
_FLD_ASP_ASKP_START    = 2    # ASKP1  — best ask (lowest price), 5 consecutive
_FLD_ASP_BIDP_START    = 7    # BIDP1  — best bid (highest price), 5 consecutive
_FLD_ASP_ASKRSQN_START = 12   # ASKP_RSQN1, 5 consecutive
_FLD_ASP_BIDRSQN_START = 17   # BIDP_RSQN1, 5 consecutive
_H0STASP0_MIN_FIELDS   = 22   # fields[0..21] must be present
_ASP_DEPTH             = 5    # levels to parse

_KST = timezone(timedelta(hours=9))


class KisConnectionState(str, Enum):
    DISCONNECTED   = "disconnected"
    AUTHENTICATING = "authenticating"
    AUTHENTICATED  = "authenticated"   # token in hand, WS not yet open
    CONNECTING     = "connecting"
    CONNECTED      = "connected"
    RECONNECTING   = "reconnecting"
    FAILED         = "failed"


class KisError(Exception):
    """Base for all KIS-adapter failures.

    Caught by the FastAPI exception handler and rendered as RFC 7807
    `application/problem+json` with `type=PROBLEM_TYPE_UPSTREAM`.
    """


class KisAuthError(KisError):
    """Credential rejection or malformed OAuth response.

    Distinct from `KisUpstreamError` because the remediation differs:
    operator must rotate / re-issue the APP Secret at the KIS portal.
    """


class KisUpstreamError(KisError):
    """Network failure or non-2xx HTTP status that isn't a credential issue.

    Includes connect timeouts, DNS failures, 5xx from KIS, or unexpected
    response shapes. Retry-able in principle.
    """


def _mask_token(token: str) -> str:
    """`eyJhbGciOiJIUzI1NiIs...c2lnbg` — head/tail only, never the body.

    Length is preserved-ish in the prefix but the middle is elided so
    structured-log scrapers can't reassemble the secret from rotated logs.
    """
    if len(token) <= 12:
        return "***"
    return f"{token[:6]}...{token[-4:]}"


class KisClient:
    """Lifecycle owner for the upstream KIS REST + WebSocket session.

    State transitions:
        DISCONNECTED → AUTHENTICATING → AUTHENTICATED → CONNECTING → CONNECTED
        any state    → FAILED          (terminal until close()/reset)
        CONNECTED    → RECONNECTING    (transient — driven by ws layer)

    `authenticate()` is idempotent on success: re-calling with a still-valid
    token is a no-op. Re-authentication is the caller's job (Sprint 5d will
    schedule a refresh ~5 min before expiry).
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._state = KisConnectionState.DISCONNECTED
        # ── REST OAuth (Step 1) ────────────────────────────────────────
        self._access_token: str | None = None
        self._token_type: str = "Bearer"
        self._token_expires_at: datetime | None = None
        # ── WebSocket session (Step 2) ─────────────────────────────────
        # approval_key is a SEPARATE credential from access_token — issued
        # by /oauth2/Approval, lives in the WS subscribe header, not the
        # REST Authorization header.
        self._approval_key: str | None = None
        self._ws: ClientConnection | None = None

    # ── State / token introspection ────────────────────────────────────

    @property
    def state(self) -> KisConnectionState:
        return self._state

    @property
    def is_token_valid(self) -> bool:
        """True iff we hold a token whose expiry is still in the future
        (with a 60s safety margin so we don't race against KIS's clock)."""
        if self._access_token is None or self._token_expires_at is None:
            return False
        return datetime.now(timezone.utc) + timedelta(seconds=60) < self._token_expires_at

    @property
    def access_token(self) -> str | None:
        return self._access_token

    @property
    def token_expires_at(self) -> datetime | None:
        return self._token_expires_at

    def authorization_header(self) -> str:
        """`Bearer <token>` for downstream REST calls."""
        if self._access_token is None:
            raise KisAuthError("no access token — call authenticate() first")
        return f"{self._token_type} {self._access_token}"

    @property
    def approval_key(self) -> str | None:
        """WebSocket subscribe-header credential (different from access_token)."""
        return self._approval_key

    @property
    def is_ws_open(self) -> bool:
        return self._ws is not None and self._state is KisConnectionState.CONNECTED

    # ── OAuth ──────────────────────────────────────────────────────────

    async def authenticate(self, *, force: bool = False) -> None:
        """Exchange app key/secret for a 24h access token (REST `/oauth2/tokenP`).

        Idempotent: returns immediately if a still-valid token is already held.
        Pass `force=True` to bypass the idempotency check — used by the
        token refresh scheduler when nearing expiry.

        Raises:
            KisAuthError    — credentials missing/wrong, or OAuth body malformed.
            KisUpstreamError — network failure, timeout, or non-2xx that isn't
                              a credential rejection.
        """
        if not force and self.is_token_valid:
            logger.debug(
                "kis.auth.skip — token still valid",
                extra={"event": "kis_auth_skipped", "expires_at": self._token_expires_at},
            )
            return

        if not self._settings.kis_app_key or not self._settings.kis_app_secret:
            self._state = KisConnectionState.FAILED
            raise KisAuthError("KIS_APP_KEY / KIS_APP_SECRET not configured")

        prev_state = self._state
        self._state = KisConnectionState.AUTHENTICATING
        url = _KIS_REST_BASE[self._settings.kis_env] + _OAUTH_PATH
        body = {
            "grant_type": "client_credentials",
            "appkey":     self._settings.kis_app_key,
            "appsecret":  self._settings.kis_app_secret,
        }

        logger.info(
            "kis.auth.start",
            extra={"event": "kis_auth_start", "kis_env": self._settings.kis_env, "url": url},
        )

        try:
            async with httpx.AsyncClient(timeout=_OAUTH_TIMEOUT_SECONDS) as client:
                resp = await client.post(url, json=body)
        except httpx.TimeoutException as exc:
            self._state = KisConnectionState.FAILED
            logger.error(
                "kis.auth.timeout",
                extra={"event": "kis_auth_timeout", "kis_env": self._settings.kis_env},
            )
            raise KisUpstreamError(f"KIS OAuth timed out after {_OAUTH_TIMEOUT_SECONDS}s") from exc
        except httpx.HTTPError as exc:
            self._state = KisConnectionState.FAILED
            logger.error(
                "kis.auth.network_error",
                extra={"event": "kis_auth_network_error", "error_type": type(exc).__name__},
            )
            raise KisUpstreamError(f"KIS OAuth network failure: {exc!s}") from exc

        # 403 / 401 from KIS == credential rejection. Treat anything else
        # non-2xx as upstream — could be 5xx, rate limit (429), maintenance.
        if resp.status_code in (401, 403):
            self._state = KisConnectionState.FAILED
            error_payload = self._safe_json_summary(resp)
            logger.error(
                "kis.auth.rejected",
                extra={
                    "event": "kis_auth_rejected",
                    "status_code": resp.status_code,
                    "error_payload": error_payload,
                },
            )
            raise KisAuthError(
                f"KIS rejected credentials (HTTP {resp.status_code}): {error_payload}"
            )
        if resp.status_code >= 400:
            self._state = KisConnectionState.FAILED
            error_payload = self._safe_json_summary(resp)
            logger.error(
                "kis.auth.upstream_error",
                extra={
                    "event": "kis_auth_upstream_error",
                    "status_code": resp.status_code,
                    "error_payload": error_payload,
                },
            )
            raise KisUpstreamError(
                f"KIS OAuth returned HTTP {resp.status_code}: {error_payload}"
            )

        try:
            payload: dict[str, Any] = resp.json()
            access_token = payload["access_token"]
            token_type   = payload.get("token_type", "Bearer")
        except (ValueError, KeyError, TypeError) as exc:
            self._state = KisConnectionState.FAILED
            logger.error(
                "kis.auth.malformed_response",
                extra={"event": "kis_auth_malformed", "status_code": resp.status_code},
            )
            raise KisAuthError(f"KIS OAuth response malformed: {exc!s}") from exc

        # Prefer `expires_in` (delta seconds) — wall-clock comparable across
        # tz-naive vs tz-aware boundaries. Fall back to parsing the
        # "access_token_token_expired" KST string if missing.
        expires_at = self._compute_expiry(payload)

        self._access_token     = access_token
        self._token_type       = token_type
        self._token_expires_at = expires_at
        self._state            = KisConnectionState.AUTHENTICATED

        logger.info(
            "kis.auth.success",
            extra={
                "event":           "kis_auth_success",
                "kis_env":         self._settings.kis_env,
                "token_masked":    _mask_token(access_token),
                "token_type":      token_type,
                "expires_at":      expires_at.isoformat() if expires_at else None,
                "prev_state":      prev_state.value,
            },
        )

    # ── WebSocket (Sprint 5c Step 2) ───────────────────────────────────

    async def connect(self) -> None:
        """Issue an approval_key and open the realtime WebSocket session.

        Two-stage handshake:
            1. POST /oauth2/Approval (REST)  → approval_key (per-WS credential)
            2. websockets.connect(ws_url)    → opens TCP + WS handshake

        After connect() returns, the channel is alive and ready for
        subscribe() — but no tr_id has been registered yet, so KIS won't
        push any frames. State transitions: AUTHENTICATED → CONNECTING → CONNECTED.

        Raises:
            KisAuthError    — no valid access token, or KIS rejects appkey/secret.
            KisUpstreamError — network/timeout/non-2xx during approval, or WS
                              handshake failure (DNS, TCP refused, timeout).
        """
        if not self.is_token_valid:
            raise KisAuthError("connect() requires a valid token — call authenticate() first")
        if self._state is KisConnectionState.CONNECTED and self._ws is not None:
            logger.debug("kis.ws.skip — already connected", extra={"event": "kis_ws_skipped"})
            return

        prev_state = self._state
        self._state = KisConnectionState.CONNECTING
        ws_url = _KIS_WS_URL[self._settings.kis_env]

        # ── Stage 1: approval_key (REST) ────────────────────────────────
        try:
            approval_key = await self._issue_approval_key()
        except KisError:
            self._state = KisConnectionState.FAILED
            raise

        # ── Stage 2: WebSocket open ─────────────────────────────────────
        logger.info(
            "kis.ws.connecting",
            extra={"event": "kis_ws_connecting", "kis_env": self._settings.kis_env, "url": ws_url},
        )
        try:
            ws = await asyncio.wait_for(ws_connect(ws_url), timeout=_WS_OPEN_TIMEOUT_SECONDS)
        except asyncio.TimeoutError as exc:
            self._state = KisConnectionState.FAILED
            logger.error(
                "kis.ws.timeout",
                extra={"event": "kis_ws_timeout", "url": ws_url},
            )
            raise KisUpstreamError(
                f"KIS WebSocket open timed out after {_WS_OPEN_TIMEOUT_SECONDS}s"
            ) from exc
        except OSError as exc:
            # TCP refused, DNS failure, etc. — websockets re-raises these.
            self._state = KisConnectionState.FAILED
            logger.error(
                "kis.ws.network_error",
                extra={"event": "kis_ws_network_error", "error_type": type(exc).__name__},
            )
            raise KisUpstreamError(f"KIS WebSocket open failed: {exc!s}") from exc
        except Exception as exc:  # noqa: BLE001 — websockets raises a wide hierarchy
            self._state = KisConnectionState.FAILED
            logger.error(
                "kis.ws.handshake_error",
                extra={"event": "kis_ws_handshake_error", "error_type": type(exc).__name__},
            )
            raise KisUpstreamError(f"KIS WebSocket handshake failed: {exc!s}") from exc

        self._approval_key = approval_key
        self._ws           = ws
        self._state        = KisConnectionState.CONNECTED

        logger.info(
            "kis.ws.connected",
            extra={
                "event":              "kis_ws_connected",
                "kis_env":            self._settings.kis_env,
                "url":                ws_url,
                "approval_key_masked": _mask_token(approval_key),
                "prev_state":         prev_state.value,
            },
        )

    async def _issue_approval_key(self) -> str:
        """POST /oauth2/Approval — distinct credential from access_token.

        KIS quirk: request body uses `secretkey` (not `appsecret` like the
        OAuth call). Response: `{"approval_key": "..."}`. Lives ~24h, used
        only inside the WS subscribe message header.
        """
        url = _KIS_REST_BASE[self._settings.kis_env] + _APPROVAL_PATH
        body = {
            "grant_type": "client_credentials",
            "appkey":     self._settings.kis_app_key,
            "secretkey":  self._settings.kis_app_secret,
        }
        try:
            async with httpx.AsyncClient(timeout=_OAUTH_TIMEOUT_SECONDS) as client:
                resp = await client.post(url, json=body)
        except httpx.TimeoutException as exc:
            raise KisUpstreamError(f"KIS Approval timed out after {_OAUTH_TIMEOUT_SECONDS}s") from exc
        except httpx.HTTPError as exc:
            raise KisUpstreamError(f"KIS Approval network failure: {exc!s}") from exc

        if resp.status_code in (401, 403):
            raise KisAuthError(
                f"KIS rejected approval_key request (HTTP {resp.status_code}): "
                f"{self._safe_json_summary(resp)}"
            )
        if resp.status_code >= 400:
            raise KisUpstreamError(
                f"KIS Approval returned HTTP {resp.status_code}: {self._safe_json_summary(resp)}"
            )

        try:
            payload: dict[str, Any] = resp.json()
            approval_key = payload["approval_key"]
        except (ValueError, KeyError, TypeError) as exc:
            raise KisAuthError(f"KIS Approval response malformed: {exc!s}") from exc
        if not isinstance(approval_key, str) or not approval_key:
            raise KisAuthError("KIS Approval returned empty approval_key")

        logger.info(
            "kis.approval.success",
            extra={
                "event":               "kis_approval_success",
                "kis_env":             self._settings.kis_env,
                "approval_key_masked": _mask_token(approval_key),
            },
        )
        return approval_key

    async def subscribe(self, symbols: list[str], tr_id: str = TR_ID_TICK) -> None:
        """Send one subscribe frame per symbol over the open WebSocket.

        Each frame: `{"header": {approval_key, custtype="P", tr_type="1",
        content-type="utf-8"}, "body": {"input": {tr_id, tr_key=symbol}}}`.
        KIS replies with one ACK frame per symbol (msg_cd=OPSP0000) which
        is consumed silently inside `stream_ticks()`.

        Raises RuntimeError if called before connect(). Symbols list may
        be empty (no-op) — convenient for callers that gate on config.
        """
        if self._state is not KisConnectionState.CONNECTED or self._ws is None:
            raise RuntimeError(
                f"subscribe() requires CONNECTED state (have {self._state.value})"
            )
        if self._approval_key is None:
            raise KisAuthError("subscribe() requires approval_key — call connect() first")

        for symbol in symbols:
            frame = self._build_subscribe_frame(self._approval_key, tr_id, symbol)
            await self._ws.send(frame)
            logger.info(
                "kis.subscribe.sent",
                extra={
                    "event":  "kis_subscribe_sent",
                    "tr_id":  tr_id,
                    "tr_key": symbol,
                },
            )

    @staticmethod
    def _build_subscribe_frame(approval_key: str, tr_id: str, tr_key: str) -> str:
        return json.dumps({
            "header": {
                "approval_key": approval_key,
                "custtype":     "P",
                "tr_type":      "1",
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id":  tr_id,
                    "tr_key": tr_key,
                },
            },
        })

    async def stream_ticks(self) -> AsyncIterator[Tick | Quote]:
        """Async iterator over live ticks and order-book snapshots.

        Routes incoming WS frames by shape:
            • JSON (`{...}`) with tr_id=PINGPONG    → echo back, continue
            • JSON subscribe ACK / error envelopes  → log, continue
            • `0|H0STCNT0|<count>|<csv>`             → parse, yield each Tick
            • `0|H0STASP0|1|<csv>`                   → parse, yield Quote
            • `1|...` (encrypted)                    → log warn, skip
            • Anything else                          → log warn, skip

        Cleanly exits on `ConnectionClosed`; the caller's loop terminates
        and a higher-level supervisor (KisPublisher) decides whether to
        reconnect. Per-frame parse failures are logged + skipped — one bad
        record never kills the stream.
        """
        if self._state is not KisConnectionState.CONNECTED or self._ws is None:
            raise RuntimeError(
                f"stream_ticks() requires CONNECTED state (have {self._state.value})"
            )
        ws = self._ws

        try:
            async for raw in ws:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")

                if raw.startswith("{"):
                    # JSON control frame — heartbeat, ack, or error envelope.
                    await self._handle_control_frame(raw)
                    continue

                if raw.startswith("0|"):
                    # Peek at tr_id to route H0STASP0 frames separately.
                    parts = raw.split("|", 3)
                    if len(parts) >= 2 and parts[1] == TR_ID_QUOTE:
                        for quote in self._parse_h0stasp0_frame(raw):
                            yield quote
                    else:
                        for tick in self._parse_h0stcnt0_frame(raw):
                            yield tick
                    continue

                if raw.startswith("1|"):
                    logger.warning(
                        "kis.stream.encrypted_frame_skipped",
                        extra={"event": "kis_stream_encrypted_skipped"},
                    )
                    continue

                logger.warning(
                    "kis.stream.unknown_frame",
                    extra={"event": "kis_stream_unknown_frame", "preview": raw[:80]},
                )
        except ConnectionClosed as exc:
            # websockets 13.1+ deprecates `.code` / `.reason` on the exception;
            # the canonical source is `.rcvd` (a frames.Close instance, possibly None).
            close = exc.rcvd
            logger.info(
                "kis.stream.closed",
                extra={
                    "event":  "kis_stream_closed",
                    "code":   close.code if close is not None else None,
                    "reason": close.reason if close is not None else None,
                },
            )
            return

    async def _handle_control_frame(self, raw: str) -> None:
        """Heartbeats + subscribe ACKs + error envelopes (all JSON)."""
        try:
            payload = json.loads(raw)
        except ValueError:
            logger.warning(
                "kis.stream.bad_json",
                extra={"event": "kis_stream_bad_json", "preview": raw[:80]},
            )
            return

        header = payload.get("header") or {}
        body   = payload.get("body") or {}
        tr_id  = header.get("tr_id", "")

        # KIS keeps the connection alive by sending PINGPONG ~30s; the
        # client must echo the SAME frame verbatim. Missing this kills
        # the connection after ~60s of silence.
        if tr_id == "PINGPONG":
            if self._ws is not None:
                await self._ws.send(raw)
            logger.debug("kis.stream.pong", extra={"event": "kis_stream_pong"})
            return

        # Subscribe / unsubscribe ACK envelopes.
        if isinstance(body, dict) and "rt_cd" in body:
            rt_cd  = body.get("rt_cd")
            msg_cd = body.get("msg_cd", "")
            msg    = body.get("msg1", "")
            if rt_cd == "0":
                logger.info(
                    "kis.stream.subscribe_ack",
                    extra={
                        "event":  "kis_stream_subscribe_ack",
                        "tr_id":  tr_id,
                        "msg_cd": msg_cd,
                        "msg1":   msg,
                    },
                )
            else:
                logger.warning(
                    "kis.stream.subscribe_error",
                    extra={
                        "event":  "kis_stream_subscribe_error",
                        "tr_id":  tr_id,
                        "rt_cd":  rt_cd,
                        "msg_cd": msg_cd,
                        "msg1":   msg,
                    },
                )
            return

        # Anything else — log and move on. Don't crash the stream.
        logger.debug(
            "kis.stream.json_other",
            extra={"event": "kis_stream_json_other", "tr_id": tr_id},
        )

    def _parse_h0stcnt0_frame(self, frame: str) -> list[Tick]:
        """Split `0|H0STCNT0|<count>|<csv>` into a list of Tick instances.

        Each record has 46 caret-delimited fields; the recordset for N
        ticks is N*46 fields concatenated. Records that fail validation
        (bad price, bad time, missing field) are logged and skipped — one
        malformed record must NOT break the rest of the batch.
        """
        try:
            _flag, tr_id, count_str, payload = frame.split("|", 3)
            count = int(count_str)
        except (ValueError, IndexError):
            logger.warning(
                "kis.parse.bad_envelope",
                extra={"event": "kis_parse_bad_envelope", "preview": frame[:80]},
            )
            return []
        if tr_id != TR_ID_TICK:
            logger.debug(
                "kis.parse.unhandled_tr_id",
                extra={"event": "kis_parse_unhandled_tr_id", "tr_id": tr_id},
            )
            return []

        fields = payload.split("^")
        expected = count * _H0STCNT0_FIELDS
        if len(fields) < expected:
            logger.warning(
                "kis.parse.short_payload",
                extra={
                    "event":    "kis_parse_short_payload",
                    "got":      len(fields),
                    "expected": expected,
                },
            )
            # Try to parse what we can — better than dropping the whole batch.
            count = len(fields) // _H0STCNT0_FIELDS

        ticks: list[Tick] = []
        for i in range(count):
            record = fields[i * _H0STCNT0_FIELDS : (i + 1) * _H0STCNT0_FIELDS]
            tick = self._record_to_tick(record)
            if tick is not None:
                ticks.append(tick)
        return ticks

    def _parse_h0stasp0_frame(self, frame: str) -> list[Quote]:
        """Split `0|H0STASP0|1|<csv>` into a list[Quote] (len ≤ 1).

        Parsing failures log a warning and return [] — never raises, never
        breaks the stream. Call once per raw frame; each frame is one snapshot.
        """
        try:
            _flag, tr_id, _count_str, payload = frame.split("|", 3)
        except ValueError:
            logger.warning(
                "kis.parse.h0stasp0.bad_envelope",
                extra={"event": "kis_parse_asp_bad_envelope", "preview": frame[:80]},
            )
            return []

        fields = payload.split("^")
        if len(fields) < _H0STASP0_MIN_FIELDS:
            logger.warning(
                "kis.parse.h0stasp0.short_payload",
                extra={
                    "event": "kis_parse_asp_short_payload",
                    "got":   len(fields),
                    "need":  _H0STASP0_MIN_FIELDS,
                },
            )
            return []

        try:
            symbol = fields[_FLD_ASP_SYMBOL].strip()
            hms    = fields[_FLD_ASP_TIME].strip()

            asks = []
            bids = []
            for i in range(_ASP_DEPTH):
                ask_price  = int(fields[_FLD_ASP_ASKP_START    + i].strip() or "0")
                bid_price  = int(fields[_FLD_ASP_BIDP_START    + i].strip() or "0")
                ask_volume = int(fields[_FLD_ASP_ASKRSQN_START + i].strip() or "0")
                bid_volume = int(fields[_FLD_ASP_BIDRSQN_START + i].strip() or "0")
                if ask_price > 0:
                    asks.append(QuoteLevel(price=ask_price, volume=ask_volume))
                if bid_price > 0:
                    bids.append(QuoteLevel(price=bid_price, volume=bid_volume))

        except (IndexError, ValueError) as exc:
            logger.warning(
                "kis.parse.h0stasp0.bad_fields",
                extra={"event": "kis_parse_asp_bad_fields", "error": str(exc)},
            )
            return []

        if not symbol or not asks or not bids:
            logger.warning(
                "kis.parse.h0stasp0.empty_levels",
                extra={"event": "kis_parse_asp_empty_levels", "symbol": symbol},
            )
            return []

        # Anchor to today in KST (same as H0STCNT0). KRX market hours 09-15 KST.
        try:
            now_kst = datetime.now(_KST)
            hour    = int(hms[0:2])
            minute  = int(hms[2:4])
            second  = int(hms[4:6])
            ts = now_kst.replace(hour=hour, minute=minute, second=second, microsecond=0)
        except (ValueError, IndexError):
            ts = datetime.now(_KST)

        return [Quote(symbol=symbol, ts=ts, bids=bids, asks=asks)]

    @staticmethod
    def _record_to_tick(record: list[str]) -> Tick | None:
        """Map one 46-field H0STCNT0 record → Tick. Returns None on bad data."""
        try:
            symbol = record[_FLD_SYMBOL].strip()
            hms    = record[_FLD_TIME_HMS].strip()
            price  = Decimal(record[_FLD_PRICE].strip() or "0")
            volume = int(record[_FLD_TICK_VOL].strip() or "0")
            ccld   = record[_FLD_CCLD_DVSN].strip()
        except (IndexError, ValueError, InvalidOperation) as exc:
            logger.warning(
                "kis.parse.bad_record",
                extra={
                    "event":      "kis_parse_bad_record",
                    "error_type": type(exc).__name__,
                },
            )
            return None

        if not symbol or len(hms) != 6 or price <= 0:
            logger.warning(
                "kis.parse.invalid_values",
                extra={
                    "event":  "kis_parse_invalid_values",
                    "symbol": symbol,
                    "hms":    hms,
                    "price":  str(price),
                },
            )
            return None

        # KIS sends HHMMSS in KST (no date). Anchor to "today in KST" — KRX
        # never sends a tick that crosses midnight (market hours are 09-15 KST).
        try:
            now_kst = datetime.now(_KST)
            ts = now_kst.replace(
                hour=int(hms[0:2]),
                minute=int(hms[2:4]),
                second=int(hms[4:6]),
                microsecond=0,
            ).astimezone(timezone.utc)
        except ValueError:
            logger.warning(
                "kis.parse.bad_time",
                extra={"event": "kis_parse_bad_time", "hms": hms},
            )
            return None

        # CCLD_DVSN: "1"=매수체결 (buyer hit ask), "5"=매도체결 (seller hit bid).
        # Default to BUY when missing — the wire contract requires a value.
        side = TickSide.SELL if ccld == "5" else TickSide.BUY

        return Tick(symbol=symbol, ts=ts, price=price, volume=volume, side=side)

    async def close(self) -> None:
        """Close the WebSocket cleanly and reset to DISCONNECTED.

        Safe to call from any state — idempotent. The access_token /
        approval_key are intentionally retained so a subsequent connect()
        can short-circuit if they're still valid.
        """
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "kis.ws.close_error",
                    extra={"event": "kis_ws_close_error", "error_type": type(exc).__name__},
                )
            self._ws = None
        self._state = KisConnectionState.DISCONNECTED
        logger.info("kis.ws.closed", extra={"event": "kis_ws_closed"})

    # ── Helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _compute_expiry(payload: dict[str, Any]) -> datetime | None:
        """KIS returns both `expires_in` (seconds) and
        `access_token_token_expired` (KST wall-clock). Prefer the delta —
        it survives clock skew between us and KIS."""
        expires_in = payload.get("expires_in")
        if isinstance(expires_in, (int, float)) and expires_in > 0:
            return datetime.now(timezone.utc) + timedelta(seconds=float(expires_in))
        wall = payload.get("access_token_token_expired")
        if isinstance(wall, str):
            try:
                # KIS publishes KST (UTC+9). Parse as naive KST → UTC.
                naive = datetime.strptime(wall, "%Y-%m-%d %H:%M:%S")
                kst = naive.replace(tzinfo=timezone(timedelta(hours=9)))
                return kst.astimezone(timezone.utc)
            except ValueError:
                return None
        return None

    @staticmethod
    def _safe_json_summary(resp: httpx.Response) -> str:
        """Best-effort one-line summary of an error response, never the secret."""
        try:
            data = resp.json()
            if isinstance(data, dict):
                # KIS error envelope: error_code + error_description.
                code = data.get("error_code") or data.get("code") or "?"
                desc = data.get("error_description") or data.get("message") or "?"
                return f"code={code} desc={desc}"
        except ValueError:
            pass
        text = resp.text or ""
        return text[:200] if text else "<empty body>"
