"""Tests for KIS balance client and related DTOs."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.api.v1.dto import BalanceDTO, BalanceSummaryDTO, HoldingDTO


def test_holding_dto_serialization():
    h = HoldingDTO(
        symbol="005930",
        name="삼성전자",
        quantity=100,
        avg_price=67000.0,
        current_price=72000,
        eval_amount=7_200_000,
        profit_loss=500_000,
        profit_loss_pct=7.46,
    )
    d = h.model_dump()
    assert d["symbol"] == "005930"
    assert d["quantity"] == 100
    assert d["profit_loss_pct"] == 7.46


def test_balance_summary_dto_serialization():
    s = BalanceSummaryDTO(
        cash=10_000_000,
        eval_total=52_100_000,
        profit_loss=1_234_000,
        profit_loss_pct=2.43,
    )
    d = s.model_dump()
    assert d["cash"] == 10_000_000
    assert d["profit_loss_pct"] == 2.43


def test_balance_dto_round_trip():
    dto = BalanceDTO(
        summary=BalanceSummaryDTO(
            cash=10_000_000,
            eval_total=52_100_000,
            profit_loss=1_234_000,
            profit_loss_pct=2.43,
        ),
        holdings=[
            HoldingDTO(
                symbol="005930",
                name="삼성전자",
                quantity=100,
                avg_price=67000.0,
                current_price=72000,
                eval_amount=7_200_000,
                profit_loss=500_000,
                profit_loss_pct=7.46,
            )
        ],
        ts="2026-05-18T09:00:00.000+00:00",
    )
    assert dto.summary.cash == 10_000_000
    assert len(dto.holdings) == 1
    assert dto.holdings[0].symbol == "005930"
    assert dto.ts == "2026-05-18T09:00:00.000+00:00"


# ── KisBalanceClient tests ──────────────────────────────────────────────────

SAMPLE_KIS_RESPONSE = {
    "rt_cd": "0",
    "msg_cd": "MAPY0299",
    "msg1": "정상처리 되었습니다.",
    "output1": [
        {
            "pdno": "005930",
            "prdt_name": "삼성전자",
            "hldg_qty": "100",
            "pchs_avg_pric": "67000.00",
            "prpr": "72000",
            "evlu_amt": "7200000",
            "evlu_pfls_amt": "500000",
            "evlu_pfls_rt": "7.46",
        },
        {
            "pdno": "000660",
            "prdt_name": "SK하이닉스",
            "hldg_qty": "50",
            "pchs_avg_pric": "110000.00",
            "prpr": "115150",
            "evlu_amt": "5757500",
            "evlu_pfls_amt": "257500",
            "evlu_pfls_rt": "4.68",
        },
    ],
    "output2": [
        {
            "dnca_tot_amt": "10000000",
            "tot_evlu_amt": "52100000",
            "evlu_pfls_amt_smtl": "1234000",
            "tot_evlu_pfls_rt": "2.43",
        }
    ],
}


@pytest.mark.asyncio
async def test_fetch_balance_parses_holdings():
    from src.infrastructure.kis_balance_client import KisBalanceClient
    from src.infrastructure.settings import Settings

    settings = MagicMock(spec=Settings)
    settings.KIS_ACCOUNT_NUMBER = "12345678-01"
    settings.KIS_IS_PAPER = False

    kis_client = MagicMock()
    kis_client.access_token = "tok-abc"

    client = KisBalanceClient(settings, kis_client)

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_KIS_RESPONSE

    with patch.object(client, "_http", new_callable=lambda: type("C", (), {"get": AsyncMock(return_value=mock_response)})()) as mock_http:
        # patch the _http.get call
        pass

    # Patch at the httpx level instead
    with patch("httpx.AsyncClient") as mock_async_client_cls:
        mock_async_client = AsyncMock()
        mock_async_client_cls.return_value.__aenter__.return_value = mock_async_client
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = SAMPLE_KIS_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_async_client.get.return_value = mock_resp

        result = await client.fetch_balance()

    assert result.cash == 10_000_000
    assert result.eval_total == 52_100_000
    assert result.profit_loss == 1_234_000
    assert abs(result.profit_loss_pct - 2.43) < 0.001
    assert len(result.holdings) == 2

    h0 = result.holdings[0]
    assert h0.symbol == "005930"
    assert h0.name == "삼성전자"
    assert h0.quantity == 100
    assert abs(h0.avg_price - 67000.0) < 0.001
    assert h0.current_price == 72000
    assert h0.eval_amount == 7_200_000
    assert h0.profit_loss == 500_000
    assert abs(h0.profit_loss_pct - 7.46) < 0.001


@pytest.mark.asyncio
async def test_fetch_balance_no_token_raises():
    from src.infrastructure.kis_balance_client import KisBalanceClient
    from src.infrastructure.kis_client import KisAuthError
    from src.infrastructure.settings import Settings

    settings = MagicMock(spec=Settings)
    settings.KIS_ACCOUNT_NUMBER = "12345678-01"
    settings.KIS_IS_PAPER = False

    kis_client = MagicMock()
    kis_client.access_token = None  # no token

    client = KisBalanceClient(settings, kis_client)

    with pytest.raises(KisAuthError):
        await client.fetch_balance()


@pytest.mark.asyncio
async def test_fetch_balance_malformed_holding_skipped():
    """A holding with unparseable fields is skipped with a warning; others are kept."""
    from src.infrastructure.kis_balance_client import KisBalanceClient
    from src.infrastructure.settings import Settings

    settings = MagicMock(spec=Settings)
    settings.KIS_ACCOUNT_NUMBER = "12345678-01"
    settings.KIS_IS_PAPER = False

    kis_client = MagicMock()
    kis_client.access_token = "tok-abc"

    client = KisBalanceClient(settings, kis_client)

    bad_response = {
        "rt_cd": "0",
        "output1": [
            {"pdno": "005930", "prdt_name": "삼성전자", "hldg_qty": "NOT_A_NUMBER",
             "pchs_avg_pric": "67000", "prpr": "72000", "evlu_amt": "7200000",
             "evlu_pfls_amt": "500000", "evlu_pfls_rt": "7.46"},
            {"pdno": "000660", "prdt_name": "SK하이닉스", "hldg_qty": "50",
             "pchs_avg_pric": "110000", "prpr": "115150", "evlu_amt": "5757500",
             "evlu_pfls_amt": "257500", "evlu_pfls_rt": "4.68"},
        ],
        "output2": [{"dnca_tot_amt": "10000000", "tot_evlu_amt": "52100000",
                     "evlu_pfls_amt_smtl": "1234000", "tot_evlu_pfls_rt": "2.43"}],
    }

    with patch("httpx.AsyncClient") as mock_async_client_cls:
        mock_async_client = AsyncMock()
        mock_async_client_cls.return_value.__aenter__.return_value = mock_async_client
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = bad_response
        mock_resp.raise_for_status = MagicMock()
        mock_async_client.get.return_value = mock_resp

        result = await client.fetch_balance()

    assert len(result.holdings) == 1  # only the good one
    assert result.holdings[0].symbol == "000660"
