"""Tests for KisOrderClient — wire contract to KIS order REST endpoint.

These tests use mocked httpx so no live order is ever submitted. The
critical "shadow mode never POSTs" guarantee is tested separately in
`test_trading_executor.py` against the executor — this file pins the
REST contract assuming the executor has decided to call us.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.domain.trading.executor import OrderResult
from src.domain.trading.models import Action
from src.infrastructure.kis_client import KisAuthError, KisUpstreamError
from src.infrastructure.kis_order_client import KisOrderClient


def _make_settings(env: str = "paper", account: str = "12345678-01") -> Any:
    s = MagicMock()
    s.kis_env            = env
    s.kis_app_key        = "TEST_APP_KEY"
    s.kis_app_secret     = "TEST_APP_SECRET"
    s.kis_account_number = account
    return s


def _make_kis_client(token: str | None = "eyJTESTACCESSTOKEN") -> Any:
    k = MagicMock()
    k.access_token = token
    return k


def _ok_response() -> httpx.Response:
    return httpx.Response(
        status_code=200,
        json={
            "rt_cd":  "0",
            "msg_cd": "APBK0013",
            "msg1":   "주문 전송 완료되었습니다.",
            "output": {
                "KRX_FWDG_ORD_ORGNO": "06010",
                "ODNO":                "0000123456",
                "ORD_TMD":             "100412",
            },
        },
    )


# ── Account-number parsing ──────────────────────────────────────────────


def test_parse_hyphenated_account_splits_cleanly():
    cano, prdt = KisOrderClient._parse_account_number("12345678-01")
    assert cano == "12345678"
    assert prdt == "01"


def test_parse_unhyphenated_account_uses_first_8():
    cano, prdt = KisOrderClient._parse_account_number("1234567801")
    assert cano == "12345678"
    assert prdt == "1"  # leading zeros stripped, defaults to "01" only when empty


def test_parse_empty_account_returns_safe_default():
    cano, prdt = KisOrderClient._parse_account_number("")
    assert cano == ""
    assert prdt == "01"


# ── tr_id matrix ────────────────────────────────────────────────────────


def test_tr_id_paper_buy():
    assert KisOrderClient._tr_id_for("paper", Action.BUY) == "VTTC0802U"


def test_tr_id_paper_sell():
    assert KisOrderClient._tr_id_for("paper", Action.SELL) == "VTTC0801U"


def test_tr_id_live_buy():
    assert KisOrderClient._tr_id_for("live", Action.BUY) == "TTTC0802U"


def test_tr_id_live_sell():
    assert KisOrderClient._tr_id_for("live", Action.SELL) == "TTTC0801U"


def test_tr_id_for_hold_raises():
    """HOLD must never reach the order client; defensive check anyway."""
    with pytest.raises(KisUpstreamError):
        KisOrderClient._tr_id_for("paper", Action.HOLD)


# ── place_order — happy path ────────────────────────────────────────────


async def test_place_order_buy_paper_posts_correct_url_headers_body():
    client = KisOrderClient(_make_settings(env="paper"), _make_kis_client())
    captured: dict[str, Any] = {}

    async def _fake_post(self, url, json=None, headers=None):  # noqa: ANN001
        captured["url"]     = url
        captured["json"]    = json
        captured["headers"] = headers
        return _ok_response()

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        result = await client.place_order(symbol="005930", action=Action.BUY, quantity=10)

    assert captured["url"] == (
        "https://openapivts.koreainvestment.com:29443"
        "/uapi/domestic-stock/v1/trading/order-cash"
    )
    # Body shape — every field KIS expects, no extras.
    assert captured["json"] == {
        "CANO":         "12345678",
        "ACNT_PRDT_CD": "01",
        "PDNO":         "005930",
        "ORD_DVSN":     "01",       # 시장가
        "ORD_QTY":      "10",
        "ORD_UNPR":     "0",
    }
    # Headers — auth + tr_id matrix selection.
    assert captured["headers"]["authorization"] == "Bearer eyJTESTACCESSTOKEN"
    assert captured["headers"]["tr_id"]         == "VTTC0802U"   # paper BUY
    assert captured["headers"]["appkey"]        == "TEST_APP_KEY"
    assert captured["headers"]["appsecret"]     == "TEST_APP_SECRET"
    assert captured["headers"]["custtype"]      == "P"

    assert isinstance(result, OrderResult)
    assert result.success is True
    assert result.order_id == "0000123456"


async def test_place_order_sell_live_uses_live_url_and_tr_id():
    client = KisOrderClient(_make_settings(env="live"), _make_kis_client())
    captured: dict[str, Any] = {}

    async def _fake_post(self, url, json=None, headers=None):  # noqa: ANN001
        captured["url"]     = url
        captured["headers"] = headers
        return _ok_response()

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        await client.place_order(symbol="005930", action=Action.SELL, quantity=5)

    assert captured["url"].startswith("https://openapi.koreainvestment.com:9443")
    assert captured["headers"]["tr_id"] == "TTTC0801U"   # live SELL


# ── place_order — input validation ──────────────────────────────────────


async def test_place_order_rejects_hold():
    client = KisOrderClient(_make_settings(), _make_kis_client())
    with pytest.raises(ValueError, match="HOLD"):
        await client.place_order(symbol="005930", action=Action.HOLD, quantity=1)


async def test_place_order_rejects_zero_quantity():
    client = KisOrderClient(_make_settings(), _make_kis_client())
    with pytest.raises(ValueError, match="quantity"):
        await client.place_order(symbol="005930", action=Action.BUY, quantity=0)


async def test_place_order_without_token_raises_auth_error():
    client = KisOrderClient(_make_settings(), _make_kis_client(token=None))
    with pytest.raises(KisAuthError):
        await client.place_order(symbol="005930", action=Action.BUY, quantity=1)


# ── place_order — KIS response handling ─────────────────────────────────


async def test_business_rejection_returns_success_false_not_raise():
    """rt_cd != "0" is a business rejection (insufficient balance, halted
    symbol, etc.). HTTP-200 with non-zero rt_cd MUST come back as a
    well-formed OrderResult with success=False — not an exception. The
    executor logs these as `live_order_rejected`, not as crashes."""
    client = KisOrderClient(_make_settings(), _make_kis_client())

    async def _fake_post(self, url, json=None, headers=None):  # noqa: ANN001
        return httpx.Response(status_code=200, json={
            "rt_cd": "1", "msg_cd": "APBK1234", "msg1": "주문가능 잔고 부족",
            "output": {},
        })

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        result = await client.place_order(symbol="005930", action=Action.BUY, quantity=1)

    assert result.success is False
    assert "rt_cd=1" in result.message
    assert "잔고" in result.message
    assert result.order_id is None


async def test_http_403_raises_kis_auth_error():
    client = KisOrderClient(_make_settings(), _make_kis_client())
    async def _fake_post(self, url, json=None, headers=None):  # noqa: ANN001
        return httpx.Response(status_code=403, json={"error": "forbidden"})
    with patch.object(httpx.AsyncClient, "post", _fake_post):
        with pytest.raises(KisAuthError):
            await client.place_order(symbol="005930", action=Action.BUY, quantity=1)


async def test_timeout_raises_kis_upstream_error():
    client = KisOrderClient(_make_settings(), _make_kis_client())
    async def _raise(self, url, json=None, headers=None):  # noqa: ANN001
        raise httpx.ConnectTimeout("simulated")
    with patch.object(httpx.AsyncClient, "post", _raise):
        with pytest.raises(KisUpstreamError, match="timed out"):
            await client.place_order(symbol="005930", action=Action.BUY, quantity=1)


async def test_non_json_response_raises_upstream_error():
    client = KisOrderClient(_make_settings(), _make_kis_client())
    async def _fake_post(self, url, json=None, headers=None):  # noqa: ANN001
        return httpx.Response(status_code=200, content=b"<html>oops</html>")
    with patch.object(httpx.AsyncClient, "post", _fake_post):
        with pytest.raises(KisUpstreamError, match="non-JSON"):
            await client.place_order(symbol="005930", action=Action.BUY, quantity=1)
