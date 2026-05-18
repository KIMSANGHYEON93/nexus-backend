"""Tests for KIS balance client and related DTOs."""
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
