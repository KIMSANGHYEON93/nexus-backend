"""Market domain models — TimescaleDB hypertable shape, vendor-agnostic.

These are the *domain* shapes that flow through the pipeline:
    KIS adapter → infrastructure → domain → API → WebSocket → NEXUS OS UI

The KIS payload format (pipe-delimited `0|H0STCNT0|len|data`) is parsed and
normalized in `infrastructure.kis_client`, never here. If we ever swap KIS
for another vendor, only the adapter changes; this module stays stable.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field


class TickSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class Tick(BaseModel):
    """Single trade execution — maps 1:1 to KIS H0STCNT0 체결 frames."""

    symbol: str = Field(..., description="KRX 6-digit ticker, e.g. '005930'")
    ts: datetime
    price: Decimal
    volume: int
    side: TickSide


class Quote(BaseModel):
    """Top-of-book snapshot — maps to KIS H0STASP0 호가 frames."""

    symbol: str
    ts: datetime
    bid_price: Decimal
    bid_size: int
    ask_price: Decimal
    ask_size: int


class Entity(BaseModel):
    """Aggregated entity state pushed to the NEXUS OS canvas.

    Mirrors the frontend `NexusEntity` contract so the WebSocket payload
    can be consumed without a translation layer.
    """

    id: str
    cluster: str
    anomaly: float = Field(ge=0.0, le=1.0)
    tx_vol: float
