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


import pytest
from fastapi.testclient import TestClient


# ── Integration: POST /v1/order ────────────────────────────────────────────

@pytest.fixture
def client():
    """Reuse the app_with_mocks fixture pattern from test_api_integration.py."""
    from src.main import create_app
    from src.core.config import Settings
    from unittest.mock import MagicMock
    settings = MagicMock(spec=Settings)
    settings.allow_live_orders = False
    settings.kis_env = "paper"
    settings.kis_account_number = "12345678-01"
    settings.redis_url = "redis://localhost:6379/0"
    settings.database_url = "postgresql://localhost/nexus_test"
    app = create_app(settings)
    return TestClient(app, raise_server_exceptions=True)


def test_post_order_mock_mode_returns_201(client):
    """Mock mode (no KIS client) returns synthetic accepted response."""
    resp = client.post("/v1/order", json={
        "symbol": "005930", "action": "buy", "quantity": 100,
        "order_type": "market", "price": 0,
    })
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["symbol"] == "005930"
    assert body["order_id"].startswith("MOCK-")
    assert "ts" in body


def test_post_order_kis_error_returns_503(client, monkeypatch):
    """KIS upstream error maps to 503 UPSTREAM_ERROR."""
    from unittest.mock import AsyncMock
    from src.infrastructure.kis_order_client import KisOrderClient
    from src.infrastructure.kis_client import KisUpstreamError
    from src.api.v1 import router as router_module

    mock_client = AsyncMock(spec=KisOrderClient)
    mock_client.place_order.side_effect = KisUpstreamError("KIS down")
    monkeypatch.setattr(router_module, "_get_order_client", lambda req: mock_client)

    resp = client.post("/v1/order", json={
        "symbol": "005930", "action": "buy", "quantity": 100,
        "order_type": "market", "price": 0,
    })
    assert resp.status_code == 503
    body = resp.json()
    assert "upstream" in body["type"].lower() or "error" in body["type"].lower()


def test_post_order_kis_rejection_returns_201_rejected(client, monkeypatch):
    """KIS business rejection (insufficient balance etc.) returns 201 with status=rejected."""
    from unittest.mock import AsyncMock
    from src.infrastructure.kis_order_client import KisOrderClient
    from src.domain.trading.executor import OrderResult
    from src.api.v1 import router as router_module

    mock_client = AsyncMock(spec=KisOrderClient)
    mock_client.place_order.return_value = OrderResult(
        success=False,
        order_id=None,
        message="rt_cd=1 msg_cd=APBK0014 msg='잔고 부족'",
    )
    monkeypatch.setattr(router_module, "_get_order_client", lambda req: mock_client)

    resp = client.post("/v1/order", json={
        "symbol": "005930", "action": "buy", "quantity": 100,
        "order_type": "market", "price": 0,
    })
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "rejected"
    assert "잔고" in body["message"] or "rt_cd" in body["message"]
