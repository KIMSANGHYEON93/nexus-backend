"""Unit tests for `KisClient.authenticate()` — mocked httpx, no network.

These tests freeze the wire contract (URL by env, request body shape,
state transitions, token masking, error mapping) so the live integration
test only has to confirm that the real KIS server agrees with that
contract. CI runs these on every push; the live test is opt-in.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.infrastructure.kis_client import (
    KisAuthError,
    KisClient,
    KisConnectionState,
    KisUpstreamError,
    _mask_token,
)


def _make_settings(
    app_key: str = "TEST_APP_KEY_AAAAAAA",
    app_secret: str = "TEST_APP_SECRET_BBBBBBBBBBBBBBBBB",
    kis_env: str = "paper",
) -> Any:
    """Build a minimal settings stand-in. Only the four fields KisClient
    touches matter, so a MagicMock is cheaper than constructing the full
    pydantic Settings (which requires DATABASE_URL etc.)."""
    s = MagicMock()
    s.kis_app_key = app_key
    s.kis_app_secret = app_secret
    s.kis_env = kis_env
    s.kis_account_number = "12345678-01"
    return s


def _ok_response(token: str = "eyJhbGciFAKETOKENBODYsigvalue", expires_in: int = 86400) -> httpx.Response:
    payload = {
        "access_token":               token,
        "access_token_token_expired": "2099-01-01 23:59:59",
        "token_type":                 "Bearer",
        "expires_in":                 expires_in,
    }
    return httpx.Response(status_code=200, json=payload)


def _err_response(status: int, code: str = "EGW00121", desc: str = "invalid appkey") -> httpx.Response:
    return httpx.Response(
        status_code=status,
        json={"error_code": code, "error_description": desc},
    )


# ── Token masking ───────────────────────────────────────────────────────


def test_mask_token_short_returns_stars():
    assert _mask_token("abc") == "***"
    assert _mask_token("a" * 12) == "***"


def test_mask_token_long_keeps_head_and_tail_only():
    masked = _mask_token("eyJhbGciOiJIUzI1NiJ9.body.signaturetail")
    assert masked.startswith("eyJhbG")
    assert masked.endswith("tail")
    assert "..." in masked
    # Body must NOT appear.
    assert "body" not in masked
    assert "signaturetail" not in masked


# ── Initial state ───────────────────────────────────────────────────────


def test_initial_state_is_disconnected_and_no_token():
    client = KisClient(_make_settings())
    assert client.state is KisConnectionState.DISCONNECTED
    assert client.access_token is None
    assert client.is_token_valid is False
    with pytest.raises(KisAuthError):
        client.authorization_header()


# ── Happy path ──────────────────────────────────────────────────────────


async def test_authenticate_success_paper_env_uses_paper_url():
    client = KisClient(_make_settings(kis_env="paper"))
    fake = _ok_response()
    captured: dict[str, Any] = {}

    async def _fake_post(self, url, json=None):  # noqa: ANN001
        captured["url"] = url
        captured["json"] = json
        return fake

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        await client.authenticate()

    assert captured["url"] == "https://openapivts.koreainvestment.com:29443/oauth2/tokenP"
    assert captured["json"]["grant_type"] == "client_credentials"
    assert captured["json"]["appkey"] == "TEST_APP_KEY_AAAAAAA"
    assert captured["json"]["appsecret"] == "TEST_APP_SECRET_BBBBBBBBBBBBBBBBB"
    assert client.state is KisConnectionState.AUTHENTICATED
    assert client.access_token == "eyJhbGciFAKETOKENBODYsigvalue"
    assert client.is_token_valid is True
    assert client.authorization_header() == "Bearer eyJhbGciFAKETOKENBODYsigvalue"


async def test_authenticate_success_live_env_uses_live_url():
    client = KisClient(_make_settings(kis_env="live"))
    captured: dict[str, Any] = {}

    async def _fake_post(self, url, json=None):  # noqa: ANN001
        captured["url"] = url
        return _ok_response()

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        await client.authenticate()

    assert captured["url"] == "https://openapi.koreainvestment.com:9443/oauth2/tokenP"


async def test_authenticate_idempotent_when_token_still_valid():
    client = KisClient(_make_settings())
    call_count = 0

    async def _fake_post(self, url, json=None):  # noqa: ANN001
        nonlocal call_count
        call_count += 1
        return _ok_response(expires_in=3600)

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        await client.authenticate()
        await client.authenticate()
        await client.authenticate()

    assert call_count == 1, "valid-token check must short-circuit re-auth"


async def test_authenticate_expiry_uses_expires_in_seconds():
    client = KisClient(_make_settings())

    async def _fake_post(self, url, json=None):  # noqa: ANN001
        return _ok_response(expires_in=600)

    before = datetime.now(timezone.utc)
    with patch.object(httpx.AsyncClient, "post", _fake_post):
        await client.authenticate()
    after = datetime.now(timezone.utc)

    assert client.token_expires_at is not None
    delta = client.token_expires_at - before
    assert timedelta(seconds=590) < delta < timedelta(seconds=610)
    # Sanity: must be in the future relative to "after" too.
    assert client.token_expires_at > after


# ── Error paths ─────────────────────────────────────────────────────────


async def test_authenticate_missing_credentials_raises_auth_error():
    client = KisClient(_make_settings(app_key="", app_secret=""))
    with pytest.raises(KisAuthError, match="not configured"):
        await client.authenticate()
    assert client.state is KisConnectionState.FAILED


async def test_authenticate_403_raises_auth_error_with_kis_error_code():
    client = KisClient(_make_settings())

    async def _fake_post(self, url, json=None):  # noqa: ANN001
        return _err_response(403, code="EGW00121", desc="invalid appkey")

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        with pytest.raises(KisAuthError, match="EGW00121"):
            await client.authenticate()
    assert client.state is KisConnectionState.FAILED


async def test_authenticate_500_raises_upstream_error_not_auth_error():
    client = KisClient(_make_settings())

    async def _fake_post(self, url, json=None):  # noqa: ANN001
        return _err_response(500, code="UPSTREAM", desc="kis maintenance")

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        with pytest.raises(KisUpstreamError, match="HTTP 500"):
            await client.authenticate()
    assert client.state is KisConnectionState.FAILED


async def test_authenticate_timeout_raises_upstream_error():
    client = KisClient(_make_settings())

    async def _raise_timeout(self, url, json=None):  # noqa: ANN001
        raise httpx.ConnectTimeout("simulated timeout")

    with patch.object(httpx.AsyncClient, "post", _raise_timeout):
        with pytest.raises(KisUpstreamError, match="timed out"):
            await client.authenticate()
    assert client.state is KisConnectionState.FAILED


async def test_authenticate_malformed_response_raises_auth_error():
    client = KisClient(_make_settings())

    async def _fake_post(self, url, json=None):  # noqa: ANN001
        return httpx.Response(status_code=200, json={"unexpected": "shape"})

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        with pytest.raises(KisAuthError, match="malformed"):
            await client.authenticate()
    assert client.state is KisConnectionState.FAILED


# ── connect() guards on token presence ──────────────────────────────────


async def test_connect_without_token_raises():
    client = KisClient(_make_settings())
    with pytest.raises(KisAuthError, match="requires a valid token"):
        await client.connect()


# ════════════════════════════════════════════════════════════════════════
#                       Sprint 5c Step 2 — connect()
# ════════════════════════════════════════════════════════════════════════
#
# WebSocket connect() is two stages: REST /oauth2/Approval to get the
# approval_key, then websockets.connect() to open the realtime gateway.
# Tests mock both — `httpx.AsyncClient.post` is routed by URL so a single
# patch handles both /oauth2/tokenP (auth) and /oauth2/Approval (connect).


def _approval_ok_response(approval_key: str = "approval-ABCDEF1234567890") -> httpx.Response:
    return httpx.Response(status_code=200, json={"approval_key": approval_key})


def _route_post(token_resp: httpx.Response, approval_resp: httpx.Response):
    """Build a fake httpx post that routes by URL — keeps tests thin."""

    async def _fake(self, url, json=None):  # noqa: ANN001
        if url.endswith("/oauth2/tokenP"):
            return token_resp
        if url.endswith("/oauth2/Approval"):
            return approval_resp
        raise AssertionError(f"unexpected URL in test: {url}")

    return _fake


class _FakeWebSocket:
    """Minimal stand-in for `websockets.asyncio.client.ClientConnection`."""

    def __init__(self) -> None:
        self.closed = False
        self.sent: list[str] = []

    async def close(self) -> None:
        self.closed = True

    async def send(self, msg: str) -> None:
        self.sent.append(msg)


async def _authed_client(kis_env: str = "paper") -> KisClient:
    """Helper: return a KisClient that has already passed authenticate()."""
    client = KisClient(_make_settings(kis_env=kis_env))
    fake = _route_post(_ok_response(), _approval_ok_response())
    with patch.object(httpx.AsyncClient, "post", fake):
        await client.authenticate()
    assert client.state is KisConnectionState.AUTHENTICATED
    return client


# ── Happy path ──────────────────────────────────────────────────────────


async def test_connect_success_paper_env_uses_paper_ws_url():
    client = await _authed_client(kis_env="paper")
    fake_ws = _FakeWebSocket()
    captured: dict[str, Any] = {}

    async def _fake_ws_connect(url):  # noqa: ANN001
        captured["url"] = url
        return fake_ws

    fake = _route_post(_ok_response(), _approval_ok_response("approval-PAPER-9999"))
    with patch.object(httpx.AsyncClient, "post", fake), \
         patch("src.infrastructure.kis_client.ws_connect", _fake_ws_connect):
        await client.connect()

    assert captured["url"] == "ws://ops.koreainvestment.com:31000"
    assert client.state is KisConnectionState.CONNECTED
    assert client.approval_key == "approval-PAPER-9999"
    assert client.is_ws_open is True


async def test_connect_success_live_env_uses_live_ws_url():
    client = await _authed_client(kis_env="live")
    captured: dict[str, Any] = {}

    async def _fake_ws_connect(url):  # noqa: ANN001
        captured["url"] = url
        return _FakeWebSocket()

    fake = _route_post(_ok_response(), _approval_ok_response())
    with patch.object(httpx.AsyncClient, "post", fake), \
         patch("src.infrastructure.kis_client.ws_connect", _fake_ws_connect):
        await client.connect()

    assert captured["url"] == "ws://ops.koreainvestment.com:21000"


async def test_approval_request_uses_secretkey_field_not_appsecret():
    """KIS quirk: /oauth2/Approval body uses `secretkey`, NOT `appsecret`."""
    client = await _authed_client()
    captured: dict[str, Any] = {}

    async def _fake_post(self, url, json=None):  # noqa: ANN001
        if url.endswith("/oauth2/Approval"):
            captured["url"] = url
            captured["json"] = json
            return _approval_ok_response()
        return _ok_response()

    async def _fake_ws_connect(url):  # noqa: ANN001
        return _FakeWebSocket()

    with patch.object(httpx.AsyncClient, "post", _fake_post), \
         patch("src.infrastructure.kis_client.ws_connect", _fake_ws_connect):
        await client.connect()

    assert captured["url"].endswith("/oauth2/Approval")
    assert captured["json"]["grant_type"] == "client_credentials"
    assert captured["json"]["appkey"] == "TEST_APP_KEY_AAAAAAA"
    # The contract test that future-me must not break:
    assert "secretkey" in captured["json"], "KIS Approval needs `secretkey`, not `appsecret`"
    assert "appsecret" not in captured["json"]
    assert captured["json"]["secretkey"] == "TEST_APP_SECRET_BBBBBBBBBBBBBBBBB"


async def test_connect_idempotent_when_already_connected():
    client = await _authed_client()
    ws_open_count = 0
    fake_ws = _FakeWebSocket()

    async def _fake_ws_connect(url):  # noqa: ANN001
        nonlocal ws_open_count
        ws_open_count += 1
        return fake_ws

    fake = _route_post(_ok_response(), _approval_ok_response())
    with patch.object(httpx.AsyncClient, "post", fake), \
         patch("src.infrastructure.kis_client.ws_connect", _fake_ws_connect):
        await client.connect()
        await client.connect()
        await client.connect()

    assert ws_open_count == 1, "connected → re-connect must short-circuit"


# ── Error paths ─────────────────────────────────────────────────────────


async def test_connect_approval_403_raises_auth_error_and_marks_failed():
    client = await _authed_client()

    async def _fake_post(self, url, json=None):  # noqa: ANN001
        if url.endswith("/oauth2/Approval"):
            return _err_response(403, code="EGW00001", desc="invalid appkey for approval")
        return _ok_response()

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        with pytest.raises(KisAuthError, match="EGW00001"):
            await client.connect()
    assert client.state is KisConnectionState.FAILED
    assert client.approval_key is None


async def test_connect_approval_500_raises_upstream_error():
    client = await _authed_client()

    async def _fake_post(self, url, json=None):  # noqa: ANN001
        if url.endswith("/oauth2/Approval"):
            return _err_response(500, code="UPSTREAM", desc="approval gateway down")
        return _ok_response()

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        with pytest.raises(KisUpstreamError, match="HTTP 500"):
            await client.connect()
    assert client.state is KisConnectionState.FAILED


async def test_connect_ws_timeout_raises_upstream_error():
    client = await _authed_client()

    async def _fake_ws_connect(url):  # noqa: ANN001
        # Sleep longer than the configured timeout so asyncio.wait_for fires.
        await asyncio.sleep(30)

    fake = _route_post(_ok_response(), _approval_ok_response())
    # Patch the timeout constant down so the test runs in <1s.
    with patch.object(httpx.AsyncClient, "post", fake), \
         patch("src.infrastructure.kis_client.ws_connect", _fake_ws_connect), \
         patch("src.infrastructure.kis_client._WS_OPEN_TIMEOUT_SECONDS", 0.05):
        with pytest.raises(KisUpstreamError, match="timed out"):
            await client.connect()
    assert client.state is KisConnectionState.FAILED


async def test_connect_ws_oserror_raises_upstream_error():
    """TCP refused / DNS failure → OSError from websockets layer."""
    client = await _authed_client()

    async def _fake_ws_connect(url):  # noqa: ANN001
        raise OSError("Connection refused")

    fake = _route_post(_ok_response(), _approval_ok_response())
    with patch.object(httpx.AsyncClient, "post", fake), \
         patch("src.infrastructure.kis_client.ws_connect", _fake_ws_connect):
        with pytest.raises(KisUpstreamError, match="Connection refused"):
            await client.connect()
    assert client.state is KisConnectionState.FAILED


async def test_close_resets_state_and_calls_ws_close():
    client = await _authed_client()
    fake_ws = _FakeWebSocket()

    async def _fake_ws_connect(url):  # noqa: ANN001
        return fake_ws

    fake = _route_post(_ok_response(), _approval_ok_response())
    with patch.object(httpx.AsyncClient, "post", fake), \
         patch("src.infrastructure.kis_client.ws_connect", _fake_ws_connect):
        await client.connect()
        assert client.state is KisConnectionState.CONNECTED
        await client.close()

    assert fake_ws.closed is True
    # .value comparison sidesteps mypy's narrowed-Literal carry-through from
    # the earlier `is CONNECTED` assertion across the close() boundary.
    assert client.state.value == "disconnected"
    assert client.is_ws_open is False
    # Token + approval_key intentionally retained for fast re-connect.
    assert client.access_token is not None
    assert client.approval_key is not None


async def test_close_is_safe_when_never_connected():
    """close() must be idempotent — call from DISCONNECTED is a no-op."""
    client = KisClient(_make_settings())
    await client.close()  # must not raise
    assert client.state is KisConnectionState.DISCONNECTED


# ════════════════════════════════════════════════════════════════════════
#                Sprint 5c Step 3 — subscribe() + parser
# ════════════════════════════════════════════════════════════════════════

import json as _json  # noqa: E402  (kept local to test section)

from src.infrastructure.kis_client import (  # noqa: E402
    TR_ID_TICK,
    _H0STCNT0_FIELDS,
)
from src.domain.market.models import Tick, TickSide  # noqa: E402


def test_quote_level_model():
    from src.domain.market.models import QuoteLevel
    lvl = QuoteLevel(price=72000, volume=3241)
    assert lvl.price == 72000
    assert lvl.volume == 3241


def test_quote_model_has_bid_ask_lists():
    from datetime import datetime, timezone
    from src.domain.market.models import Quote, QuoteLevel
    ts = datetime.now(timezone.utc)
    q = Quote(
        symbol="005930",
        ts=ts,
        bids=[QuoteLevel(price=71900, volume=15600)],
        asks=[QuoteLevel(price=72000, volume=3241)],
    )
    assert q.symbol == "005930"
    assert q.bids[0].price == 71900
    assert q.asks[0].price == 72000


def _make_h0stcnt0_record(
    symbol: str = "005930",
    hms: str = "130000",
    price: str = "79000",
    volume: str = "100",
    ccld: str = "1",
) -> list[str]:
    """Build a 46-field H0STCNT0 record with our 5 fields populated."""
    record = [""] * _H0STCNT0_FIELDS
    record[0]  = symbol
    record[1]  = hms
    record[2]  = price
    record[12] = volume
    record[21] = ccld
    return record


def _make_h0stcnt0_frame(records: list[list[str]]) -> str:
    """Build a `0|H0STCNT0|<count>|<csv>` frame from one or more records."""
    flat = []
    for r in records:
        flat.extend(r)
    return f"0|{TR_ID_TICK}|{len(records)}|" + "^".join(flat)


# ── Subscribe frame contract ────────────────────────────────────────────


async def test_subscribe_sends_one_frame_per_symbol_with_correct_shape():
    client = await _authed_client()
    fake_ws = _FakeWebSocket()

    async def _fake_ws_connect(url):  # noqa: ANN001
        return fake_ws

    fake = _route_post(_ok_response(), _approval_ok_response("appkey-XYZ-9999"))
    with patch.object(httpx.AsyncClient, "post", fake), \
         patch("src.infrastructure.kis_client.ws_connect", _fake_ws_connect):
        await client.connect()
        await client.subscribe(["005930", "000660"])

    assert len(fake_ws.sent) == 2
    frame_a = _json.loads(fake_ws.sent[0])
    assert frame_a["header"]["approval_key"] == "appkey-XYZ-9999"
    assert frame_a["header"]["custtype"]     == "P"
    assert frame_a["header"]["tr_type"]      == "1"
    assert frame_a["body"]["input"]["tr_id"]  == "H0STCNT0"
    assert frame_a["body"]["input"]["tr_key"] == "005930"
    frame_b = _json.loads(fake_ws.sent[1])
    assert frame_b["body"]["input"]["tr_key"] == "000660"


async def test_subscribe_empty_list_is_noop():
    client = await _authed_client()
    fake_ws = _FakeWebSocket()
    async def _fake_ws_connect(url):  # noqa: ANN001
        return fake_ws
    fake = _route_post(_ok_response(), _approval_ok_response())
    with patch.object(httpx.AsyncClient, "post", fake), \
         patch("src.infrastructure.kis_client.ws_connect", _fake_ws_connect):
        await client.connect()
        await client.subscribe([])
    assert fake_ws.sent == []


async def test_subscribe_before_connect_raises():
    client = await _authed_client()
    with pytest.raises(RuntimeError, match="CONNECTED"):
        await client.subscribe(["005930"])


# ── Frame parser ────────────────────────────────────────────────────────


def test_parse_single_tick_record():
    client = KisClient(_make_settings())
    frame = _make_h0stcnt0_frame([
        _make_h0stcnt0_record("005930", "130000", "79100", "150", "1"),
    ])
    ticks = client._parse_h0stcnt0_frame(frame)  # noqa: SLF001
    assert len(ticks) == 1
    t = ticks[0]
    assert t.symbol == "005930"
    assert t.price == Decimal("79100")
    assert t.volume == 150
    assert t.side is TickSide.BUY


def test_parse_batch_of_three_ticks():
    """KIS sometimes batches multiple records in one frame (count > 1)."""
    client = KisClient(_make_settings())
    frame = _make_h0stcnt0_frame([
        _make_h0stcnt0_record("005930", "130000", "79100", "100", "1"),
        _make_h0stcnt0_record("000660", "130001", "197500", "50",  "5"),
        _make_h0stcnt0_record("035420", "130002", "215000", "20",  "1"),
    ])
    ticks = client._parse_h0stcnt0_frame(frame)  # noqa: SLF001
    assert [t.symbol for t in ticks] == ["005930", "000660", "035420"]
    assert ticks[1].side is TickSide.SELL  # ccld=5


def test_parse_unknown_tr_id_skipped():
    client = KisClient(_make_settings())
    # H0STASP0 (호가) — handled in Sprint 5d, ignored here.
    frame = "0|H0STASP0|1|" + "^".join([""] * 46)
    assert client._parse_h0stcnt0_frame(frame) == []  # noqa: SLF001


def test_parse_bad_envelope_returns_empty_not_raise():
    client = KisClient(_make_settings())
    assert client._parse_h0stcnt0_frame("not-a-frame") == []  # noqa: SLF001
    assert client._parse_h0stcnt0_frame("0|H0STCNT0|notanint|x") == []  # noqa: SLF001


def test_parse_skips_invalid_record_keeps_rest():
    """One bad record must NOT poison the whole batch."""
    client = KisClient(_make_settings())
    good = _make_h0stcnt0_record("005930", "130000", "79100", "100", "1")
    bad  = _make_h0stcnt0_record("000660", "BADTIME", "0",   "x",   "1")  # invalid hms + price
    frame = _make_h0stcnt0_frame([good, bad])
    ticks = client._parse_h0stcnt0_frame(frame)  # noqa: SLF001
    assert len(ticks) == 1
    assert ticks[0].symbol == "005930"


def test_ccld_dvsn_default_buy_when_unknown():
    """ccld other than '5' (sell) defaults to buy — wire contract requires a value."""
    client = KisClient(_make_settings())
    frame = _make_h0stcnt0_frame([
        _make_h0stcnt0_record(ccld="3"),    # 장중
        _make_h0stcnt0_record(ccld=""),     # missing
    ])
    ticks = client._parse_h0stcnt0_frame(frame)  # noqa: SLF001
    assert all(t.side is TickSide.BUY for t in ticks)


def test_parse_kst_time_converted_to_utc():
    """13:00:00 KST should map to 04:00:00 UTC (KST is UTC+9)."""
    client = KisClient(_make_settings())
    frame = _make_h0stcnt0_frame([
        _make_h0stcnt0_record(hms="130000"),
    ])
    ticks = client._parse_h0stcnt0_frame(frame)  # noqa: SLF001
    assert ticks[0].ts.utcoffset() == timedelta(0)
    assert ticks[0].ts.hour == 4  # 13 KST - 9h = 04 UTC
    assert ticks[0].ts.minute == 0


# ── stream_ticks() — control frames ─────────────────────────────────────


class _ScriptedWebSocket:
    """WebSocket whose `__aiter__` yields a scripted list of frames, then exits."""

    def __init__(self, frames: list[str]) -> None:
        self._frames = list(frames)
        self.sent: list[str] = []
        self.closed = False

    def __aiter__(self) -> Any:
        return self._iter()

    async def _iter(self) -> Any:
        for f in self._frames:
            yield f

    async def send(self, msg: str) -> None:
        self.sent.append(msg)

    async def close(self) -> None:
        self.closed = True


async def _connected_client_with_ws(ws: Any) -> KisClient:
    client = await _authed_client()
    fake = _route_post(_ok_response(), _approval_ok_response("apk"))
    with patch.object(httpx.AsyncClient, "post", fake), \
         patch("src.infrastructure.kis_client.ws_connect", AsyncMock(return_value=ws)):
        await client.connect()
    return client


async def test_stream_ticks_yields_parsed_ticks():
    frame = _make_h0stcnt0_frame([
        _make_h0stcnt0_record("005930", "130000", "79100", "150", "1"),
        _make_h0stcnt0_record("005930", "130001", "79050", "75",  "5"),
    ])
    ws = _ScriptedWebSocket([frame])
    client = await _connected_client_with_ws(ws)
    ticks = [t async for t in client.stream_ticks()]
    assert len(ticks) == 2
    assert ticks[0].side is TickSide.BUY
    assert ticks[1].side is TickSide.SELL


async def test_stream_ticks_echoes_pingpong_back_verbatim():
    pingpong = _json.dumps({"header": {"tr_id": "PINGPONG", "datetime": "20260509134211"}})
    ws = _ScriptedWebSocket([pingpong])
    client = await _connected_client_with_ws(ws)
    ticks = [t async for t in client.stream_ticks()]
    assert ticks == []
    assert ws.sent == [pingpong], "PINGPONG must be echoed exactly to keep WS alive"


async def test_stream_ticks_silently_consumes_subscribe_ack():
    ack = _json.dumps({
        "header": {"tr_id": "H0STCNT0"},
        "body":   {"rt_cd": "0", "msg_cd": "OPSP0000", "msg1": "SUBSCRIBE SUCCESS"},
    })
    tick = _make_h0stcnt0_frame([_make_h0stcnt0_record()])
    ws = _ScriptedWebSocket([ack, tick])
    client = await _connected_client_with_ws(ws)
    ticks = [t async for t in client.stream_ticks()]
    # ACK consumed (not yielded), tick yielded.
    assert len(ticks) == 1
    assert ticks[0].symbol == "005930"


async def test_stream_ticks_skips_encrypted_and_unknown_frames():
    ws = _ScriptedWebSocket([
        "1|H0STCNT0|1|encrypted-payload",
        "X|garbage",
        _make_h0stcnt0_frame([_make_h0stcnt0_record("005930")]),
    ])
    client = await _connected_client_with_ws(ws)
    ticks = [t async for t in client.stream_ticks()]
    assert len(ticks) == 1


async def test_stream_ticks_handles_connection_closed_gracefully():
    """Iterator must exit cleanly on WS close — no exception bubbling."""
    from websockets.exceptions import ConnectionClosed
    from websockets.frames import Close

    class _ClosingWs:
        sent: list[str] = []
        def __aiter__(self) -> Any:
            return self._gen()
        async def _gen(self) -> Any:
            yield _make_h0stcnt0_frame([_make_h0stcnt0_record()])
            raise ConnectionClosed(Close(1000, "ok"), None)
        async def send(self, msg):  # noqa: ANN001, ANN201
            self.sent.append(msg)
        async def close(self):  # noqa: ANN201
            return None

    ws = _ClosingWs()
    client = await _connected_client_with_ws(ws)
    ticks = [t async for t in client.stream_ticks()]
    assert len(ticks) == 1  # the one before the close


# ════════════════════════════════════════════════════════════════════════
#              Sprint 5d — H0STASP0 parser + stream_ticks routing
# ════════════════════════════════════════════════════════════════════════


def _make_settings_for_client() -> Any:
    s = MagicMock()
    s.kis_app_key = "TEST_KEY_AAAAAAA"
    s.kis_app_secret = "TEST_SECRET_BBBBBBB"
    s.kis_env = "paper"
    s.kis_account_number = "12345678-01"
    return s


def test_parse_h0stasp0_frame_basic():
    """Sample H0STASP0 frame → Quote with correct 5 bid/ask levels."""
    from src.infrastructure.kis_client import KisClient
    from src.domain.market.models import Quote
    client = KisClient(_make_settings_for_client())
    # Field layout per KIS H0STASP0 spec (22 fields minimum):
    #   [0]=symbol, [1]=time,
    #   [2-6]=ASKP1-5 (best ask first), [7-11]=BIDP1-5 (best bid first),
    #   [12-16]=ASKP_RSQN1-5, [17-21]=BIDP_RSQN1-5
    fields = ["005930", "094523",
              "72000", "72100", "72200", "72300", "72400",   # asks [2-6] — best ask first
              "71900", "71800", "71700", "71600", "71500",   # bids [7-11] — best bid first
              "3241",  "8102",  "12440", "18900", "21300",   # ask vols [12-16]
              "15600", "9800",  "5200",  "2100",  "1500",    # bid vols [17-21]
              ]
    frame = "0|H0STASP0|1|" + "^".join(fields)
    result = client._parse_h0stasp0_frame(frame)
    assert len(result) == 1
    q = result[0]
    assert isinstance(q, Quote)
    assert q.symbol == "005930"
    assert len(q.asks) == 5
    assert len(q.bids) == 5
    assert q.asks[0].price == 72000   # best ask
    assert q.asks[0].volume == 3241
    assert q.bids[0].price == 71900   # best bid
    assert q.bids[0].volume == 15600


def test_parse_h0stasp0_frame_bad_envelope_returns_empty():
    from src.infrastructure.kis_client import KisClient
    client = KisClient(_make_settings_for_client())
    assert client._parse_h0stasp0_frame("not|a|valid") == []
    assert client._parse_h0stasp0_frame("0|H0STASP0|bad|data") == []


def test_parse_h0stasp0_frame_short_payload_returns_empty():
    from src.infrastructure.kis_client import KisClient
    client = KisClient(_make_settings_for_client())
    # Only 5 fields — not enough for 22 minimum
    frame = "0|H0STASP0|1|005930^094523^72000^72100^72200"
    assert client._parse_h0stasp0_frame(frame) == []


async def test_stream_ticks_yields_quote_for_h0stasp0_frame():
    """stream_ticks() must yield Quote objects for H0STASP0 frames."""
    from src.infrastructure.kis_client import KisClient
    from src.domain.market.models import Quote

    # Field layout matches _FLD_ASP_* constants (22 fields, no blank filler):
    #   [0]=symbol, [1]=time, [2-6]=ASKP1-5, [7-11]=BIDP1-5,
    #   [12-16]=ASKRSQN1-5, [17-21]=BIDRSQN1-5
    fields = ["005930", "094523",
              "72000", "72100", "72200", "72300", "72400",
              "71900", "71800", "71700", "71600", "71500",
              "3241",  "8102",  "12440", "18900", "21300",
              "15600", "9800",  "5200",  "2100",  "1500"]
    frame = "0|H0STASP0|1|" + "^".join(fields)

    ws = _ScriptedWebSocket([frame])
    client = await _connected_client_with_ws(ws)
    items = [item async for item in client.stream_ticks()]
    assert len(items) == 1
    assert isinstance(items[0], Quote)
