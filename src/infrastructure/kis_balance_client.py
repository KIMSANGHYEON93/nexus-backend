"""KisBalanceClient — KIS REST adapter for account balance inquiry.

Calls GET /uapi/domestic-stock/v1/inquire-balance and maps the
KIS response into domain dataclasses (HoldingItem, BalanceResult).

KIS tr_id matrix:
    paper → VTTC8434R   (모의투자 잔고조회)
    live  → TTTC8434R   (실전투자 잔고조회)

Account number convention: same as KisOrderClient — `12345678-01`
is split into CANO="12345678" and ACNT_PRDT_CD="01".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import httpx

from ..core.config import Settings
from .kis_client import KisAuthError, KisClient, KisUpstreamError

logger = logging.getLogger(__name__)


_BALANCE_PATH = "/uapi/domestic-stock/v1/inquire-balance"
_BALANCE_TIMEOUT_SECONDS = 10.0

_TR_ID = {
    "paper": "VTTC8434R",
    "live":  "TTTC8434R",
}


@dataclass
class HoldingItem:
    symbol:          str
    name:            str
    quantity:        int
    avg_price:       float
    current_price:   int
    eval_amount:     int
    profit_loss:     int
    profit_loss_pct: float


@dataclass
class BalanceResult:
    cash:            int
    eval_total:      int
    profit_loss:     int
    profit_loss_pct: float
    holdings:        list[HoldingItem] = field(default_factory=list)


class KisBalanceClient:
    """Concrete balance client backed by the KIS inquire-balance REST endpoint.

    Reuses the access_token from a connected KisClient rather than
    re-authenticating — same pattern as KisOrderClient.
    """

    def __init__(self, settings: Settings, kis_client: KisClient) -> None:
        self._settings = settings
        self._kis_client = kis_client
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
        # Tolerate unhyphenated form — first 8 = CANO, rest = ACNT_PRDT_CD.
        return (raw[:8], raw[8:].lstrip("0") or "01")

    def _is_paper(self) -> bool:
        """Return True when running in paper-trading mode."""
        return self._settings.kis_env == "paper"

    def _build_url(self) -> str:
        host = (
            "https://openapivts.koreainvestment.com:29443"
            if self._is_paper() else
            "https://openapi.koreainvestment.com:9443"
        )
        return host + _BALANCE_PATH

    def _build_tr_id(self) -> str:
        return _TR_ID["paper" if self._is_paper() else "live"]

    async def fetch_balance(self) -> BalanceResult:
        """Call KIS inquire-balance and return parsed BalanceResult.

        Raises:
            KisAuthError     — access_token is None / missing.
            KisUpstreamError — network failure, 4xx/5xx HTTP, or unexpected
                               response shape (missing output2).
        """
        token = self._kis_client.access_token
        if not token:
            raise KisAuthError("No KIS access token")

        url   = self._build_url()
        tr_id = self._build_tr_id()

        params: dict[str, str] = {
            "CANO":                   self._cano,
            "ACNT_PRDT_CD":           self._acnt_prdt_cd,
            "AFHR_FLPR_YN":           "N",
            "OFL_YN":                 "N",
            "INQR_DVSN":              "02",
            "UNPR_DVSN":              "01",
            "FUND_STTL_ICLD_YN":      "N",
            "FNCG_AMT_AUTO_RDMT_YN":  "N",
            "PRCS_DVSN":              "00",
            "CTX_AREA_FK100":         "",
            "CTX_AREA_NK100":         "",
        }
        headers = {
            "content-type":  "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey":        getattr(self._settings, "kis_app_key", ""),
            "appsecret":     getattr(self._settings, "kis_app_secret", ""),
            "tr_id":         tr_id,
            "custtype":      "P",
        }

        logger.info(
            "kis.balance.requesting",
            extra={
                "event":   "kis_balance_requesting",
                "kis_env": "paper" if self._is_paper() else "live",
                "tr_id":   tr_id,
                "cano":    self._cano,
            },
        )

        try:
            async with httpx.AsyncClient(timeout=_BALANCE_TIMEOUT_SECONDS) as client:
                resp = await client.get(url, params=params, headers=headers)
        except httpx.TimeoutException as exc:
            raise KisUpstreamError(
                f"KIS balance GET timed out after {_BALANCE_TIMEOUT_SECONDS}s"
            ) from exc
        except httpx.HTTPError as exc:
            raise KisUpstreamError(f"KIS balance network failure: {exc!s}") from exc

        return self._parse_response(resp)

    def _parse_response(self, resp: httpx.Response) -> BalanceResult:
        if resp.status_code in (401, 403):
            raise KisAuthError(
                f"KIS balance rejected at auth layer (HTTP {resp.status_code})"
            )
        if resp.status_code >= 400:
            raise KisUpstreamError(
                f"KIS balance returned HTTP {resp.status_code}"
            )

        try:
            payload: dict[str, Any] = resp.json()
        except ValueError as exc:
            raise KisUpstreamError(
                f"KIS balance returned non-JSON response (HTTP {resp.status_code})"
            ) from exc

        output1: list[dict[str, Any]] = payload.get("output1") or []
        output2: list[dict[str, Any]] = payload.get("output2") or []

        if not output2:
            raise KisUpstreamError("Missing output2 in KIS balance response")

        holdings = self._parse_holdings(output1)
        summary  = self._parse_summary(output2[0])

        logger.info(
            "kis.balance.parsed",
            extra={
                "event":         "kis_balance_parsed",
                "holdings_count": len(holdings),
                "cash":          summary["cash"],
            },
        )

        return BalanceResult(
            cash=summary["cash"],
            eval_total=summary["eval_total"],
            profit_loss=summary["profit_loss"],
            profit_loss_pct=summary["profit_loss_pct"],
            holdings=holdings,
        )

    @staticmethod
    def _parse_holdings(output1: list[dict[str, Any]]) -> list[HoldingItem]:
        holdings: list[HoldingItem] = []
        for raw in output1:
            try:
                item = HoldingItem(
                    symbol=raw["pdno"],
                    name=raw["prdt_name"],
                    quantity=int(raw["hldg_qty"]),
                    avg_price=float(raw["pchs_avg_pric"]),
                    current_price=int(raw["prpr"]),
                    eval_amount=int(raw["evlu_amt"]),
                    profit_loss=int(raw["evlu_pfls_amt"]),
                    profit_loss_pct=float(raw["evlu_pfls_rt"]),
                )
                holdings.append(item)
            except (KeyError, ValueError, TypeError) as exc:
                logger.warning(
                    "kis.balance.holding_parse_error",
                    extra={
                        "event":      "kis_balance_holding_parse_error",
                        "error_type": type(exc).__name__,
                        "error":      str(exc),
                        "pdno":       raw.get("pdno", "?"),
                    },
                )
                # Skip malformed holding; continue with the rest.
                continue
        return holdings

    @staticmethod
    def _parse_summary(raw: dict[str, Any]) -> dict[str, Any]:
        return {
            "cash":            int(raw["dnca_tot_amt"]),
            "eval_total":      int(raw["tot_evlu_amt"]),
            "profit_loss":     int(raw["evlu_pfls_amt_smtl"]),
            "profit_loss_pct": float(raw["tot_evlu_pfls_rt"]),
        }


__all__ = ["KisBalanceClient", "BalanceResult", "HoldingItem"]
