"""Tests for Order Entry DTOs and POST /v1/order endpoint."""
from src.api.v1.dto import OrderRequestDTO, OrderResponseDTO


def test_order_request_dto_market_defaults():
    dto = OrderRequestDTO(symbol="005930", action="buy", quantity=100)
    assert dto.order_type == "market"
    assert dto.price == 0


def test_order_request_dto_limit_fields():
    dto = OrderRequestDTO(
        symbol="005930", action="sell", quantity=50,
        order_type="limit", price=72000,
    )
    assert dto.order_type == "limit"
    assert dto.price == 72000


def test_order_request_dto_zero_quantity_rejected():
    import pytest
    with pytest.raises(Exception):
        OrderRequestDTO(symbol="005930", action="buy", quantity=0)


def test_order_response_dto_serialization():
    dto = OrderResponseDTO(
        order_id="0000123456",
        symbol="005930",
        action="buy",
        quantity=100,
        status="accepted",
        message="주문 전송 완료",
        ts="2026-05-18T09:00:00.000+00:00",
    )
    d = dto.model_dump()
    assert d["order_id"] == "0000123456"
    assert d["status"] == "accepted"
