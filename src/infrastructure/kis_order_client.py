"""KisOrderClient — KIS REST adapter for cash-equity orders.

Implements the `OrderClient` Protocol from `domain/trading/executor.py`.
Translates `(symbol, action, quantity)` into a KIS `/uapi/domestic-stock/v1/
trading/order-cash` POST and shapes the response back into `OrderResult`.

CRITICAL: this class can submit real orders. The hard safety switch
lives one layer up in `OrderExecutor` — this client trusts that anyone
calling `place_order()` has already cleared the `ALLOW_LIVE_ORDERS`
gate. The split is on purpose: the safety check is in the domain layer
where it can be tested in total isolation from any HTTP code.

KIS tr_id matrix (paper vs live × buy vs sell):

    paper BUY  → VTTC0802U   (모의투자 현금매수)
    paper SELL → VTTC0801U   (모의투자 현금매도)
    live  BUY  → TTTC0802U   (실전투자 현금매수)
    live  SELL → TTTC0801U   (실전투자 현금매도)

Order is always 시장가 (market, ORD_DVSN="01") with ORD_UNPR="0" for
Sprint 5g. Limit-order support is a Sprint 5h+ concern when we have a
reason to care about price discipline beyond what the market gives us.

Account number convention: KIS expects CANO (account 8 digits) and
ACNT_PRDT_CD (product code, usually "01") as separate fields. The
`KIS_ACCOUNT_NUMBER` env var stores them joined with `-` (e.g.
`12345678-01`); this adapter splits at construction time so the order
hot-path doesn't pay for string parsing on every call.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

import httpx

from ..core.config import Settings
from ..domain.trading.executor import OrderResult
from ..domain.trading.models import Action
from .kis_client import KisAuthError, KisClient, KisError, KisUpstreamError

logger = logging.getLogger(__name__)


_ORDER_PATH = "/uapi/domestic-stock/v1/trading/order-cash"
_ORDER_TIMEOUT_SECONDS = 10.0

# tr_id table — see module docstring.
_TR_ID = {
    ("paper", Action.BUY):  "VTTC0802U",
    ("paper", Action.SELL): "VTTC0801U",
    ("live",  Action.BUY):  "TTTC0802U",
    ("live",  Action.SELL): "TTTC0801U",
}


class KisOrderClient:
    """Concrete OrderClient backed by the KIS cash-order REST endpoint.

    Reuses the access_token from a connected `KisClient` rather than
    re-authenticating — Sprint 5d's refresh loop keeps that token alive
    and the rate-limited /oauth2/tokenP endpoint can't tolerate per-order
    re-auth anyway (1/min ceiling).
    """

    def __init__(self, settings: Settings, kis_client: KisClient) -> None:
        self._settings = settings
        self._kis      = kis_client
        self._cano, self._acnt_prdt_cd = self._parse_account_number(
            settings.kis_account_number
        )

    @staticmethod
    def _parse_account_number(raw: str) -> tuple[str, str]:
        """`12345678-01` → ('12345678', '01'). Empty → ('', '01')."""
        if not raw:
            return ("", "01")
        if "-" in raw:
            cano, prdt = raw.split("-", 1)
            return (cano.strip(), prdt.strip() or "01")
        # Tolerate the unhyphenated form — first 8 = CANO, rest = ACNT_PRDT_CD.
        return (raw[:8], raw[8:].lstrip("0") or "01")

    @staticmethod
    def _tr_id_for(env: str, action: Action) -> str:
        try:
            return _TR_ID[(env, action)]
        except KeyError as exc:
            raise KisUpstreamError(
                f"no KIS tr_id for env={env!r} action={action.value!r}"
            ) from exc

    async def place_order(
        self,
        *,
        symbol:     str,
        action:     Action,
        quantity:   int,
        order_type: Literal["market", "limit"] = "market",
        price:      int = 0,
    ) -> OrderResult:
        if action is Action.HOLD:
            # Defensive — executor short-circuits HOLD before reaching us,
            # but the client's contract should still refuse meaningless input.
            raise ValueError("place_order requires BUY or SELL, got HOLD")
        if quantity <= 0:
            raise ValueError(f"quantity must be > 0, got {quantity}")
        if order_type == "limit" and price <= 0:
            raise ValueError("limit order requires price > 0")

        token = self._kis.access_token
        if not token:
            raise KisAuthError("place_order requires a valid access_token")

        url   = (
            ("https://openapivts.koreainvestment.com:29443"
             if self._settings.kis_env == "paper" else
             "https://openapi.koreainvestment.com:9443")
            + _ORDER_PATH
        )
        tr_id = self._tr_id_for(self._settings.kis_env, action)

        body = {
            "CANO":         self._cano,
            "ACNT_PRDT_CD": self._acnt_prdt_cd,
            "PDNO":         symbol,
            "ORD_DVSN":     "01" if order_type == "market" else "00",
            "ORD_QTY":      str(quantity),
            "ORD_UNPR":     "0" if order_type == "market" else str(price),
        }
        headers = {
            "content-type":  "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey":        self._settings.kis_app_key,
            "appsecret":     self._settings.kis_app_secret,
            "tr_id":         tr_id,
            "custtype":      "P",          # 개인 (corporate would be "B")
        }

        logger.warning(
            "kis.order.submitting",
            extra={
                "event":    "kis_order_submitting",
                "kis_env":  self._settings.kis_env,
                "tr_id":    tr_id,
                "symbol":   symbol,
                "action":   action.value,
                "quantity": quantity,
            },
        )

        try:
            async with httpx.AsyncClient(timeout=_ORDER_TIMEOUT_SECONDS) as client:
                resp = await client.post(url, json=body, headers=headers)
        except httpx.TimeoutException as exc:
            raise KisUpstreamError(
                f"KIS order POST timed out after {_ORDER_TIMEOUT_SECONDS}s"
            ) from exc
        except httpx.HTTPError as exc:
            raise KisUpstreamError(f"KIS order network failure: {exc!s}") from exc

        return self._parse_response(resp, symbol=symbol, action=action)

    def _parse_response(
        self,
        resp: httpx.Response,
        *,
        symbol: str,
        action: Action,
    ) -> OrderResult:
        if resp.status_code in (401, 403):
            raise KisAuthError(
                f"KIS order rejected at auth layer (HTTP {resp.status_code})"
            )

        try:
            payload: dict[str, Any] = resp.json()
        except ValueError as exc:
            raise KisUpstreamError(
                f"KIS order returned non-JSON response (HTTP {resp.status_code})"
            ) from exc

        rt_cd  = payload.get("rt_cd", "?")
        msg_cd = payload.get("msg_cd", "?")
        msg1   = payload.get("msg1", "")
        output = payload.get("output") or {}
        order_id = output.get("ODNO") if isinstance(output, dict) else None

        # `rt_cd == "0"` is KIS's "all good"; anything else is a business
        # rejection (insufficient balance, halted symbol, etc.). HTTP-200
        # with non-zero rt_cd is normal and SHOULD return success=False
        # rather than raise — the executor maps non-success to a
        # `live_order_rejected` log line, not a crash.
        success = (resp.status_code < 400) and (rt_cd == "0")

        logger.warning(
            "kis.order.response",
            extra={
                "event":       "kis_order_response",
                "symbol":      symbol,
                "action":      action.value,
                "http_status": resp.status_code,
                "rt_cd":       rt_cd,
                "msg_cd":      msg_cd,
                "msg1":        msg1,
                "order_id":    order_id,
                "success":     success,
            },
        )

        return OrderResult(
            success=success,
            order_id=order_id if isinstance(order_id, str) else None,
            message=f"rt_cd={rt_cd} msg_cd={msg_cd} msg={msg1!r}",
            raw_response=payload,
        )


__all__ = ["KisOrderClient"]


# Re-exported for type-checker convenience; consumers that catch order
# failures will want to handle KisError subclasses uniformly.
_ = KisError
