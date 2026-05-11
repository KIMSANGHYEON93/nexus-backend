"""HTTP response DTOs for the v1 surface.

Field names are chosen to mirror the NEXUS OS frontend `NexusEntity` /
`NexusEdge` types in `repo/src/types/nexus.ts` so the browser can use the
payload without an adapter layer. If we ever need to break that contract,
this is the one place to bump a v2 router with new DTOs and keep v1
responding the old shape.
"""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class EntityDTO(BaseModel):
    id: str
    cluster: str
    anomaly: float = Field(ge=0.0, le=1.0)
    tx_vol: float


class EdgeDTO(BaseModel):
    from_: str = Field(alias="from")
    to: str
    weight: float = 1.0

    model_config = {"populate_by_name": True}


class SnapshotDTO(BaseModel):
    entities: list[EntityDTO]
    edges: list[EdgeDTO]
    ts: Optional[datetime] = None


class MigrationStatusDTO(BaseModel):
    """Migration application state, reported on every /readyz call.

    `applied` is null when the schema_version table itself is missing —
    distinguishes 'fresh DB, no migrations ever ran' from 'partial run'.
    `reason` carries operator guidance when ok=False (e.g. 'run db/migrate.py').
    """

    applied:  Optional[int]
    expected: int
    ok:       bool
    reason:   Optional[str] = None


class ReadinessDTO(BaseModel):
    """Detailed readiness — booleans per dependency so liveness probes can
    distinguish 'app is up but DB is down' from 'app is dead', and a stale
    container (code newer than the migrated schema) from a healthy one."""

    ok:        bool
    database:  bool
    redis:     bool
    migration: MigrationStatusDTO


class AuditRationaleDTO(BaseModel):
    """One agent's contribution to a coordinator decision. Mirrors the
    `AgentSignal`-shaped dict that the TradingCoordinator stores in the
    audit envelope's `signal.rationale`. We keep it permissive (extra
    fields allowed) so additions to AgentSignal don't break the modal —
    the frontend renders agent_id + action + confidence and ignores
    the rest until it knows about new fields."""

    agent_id:    str
    action:      str
    confidence:  float

    model_config = {"extra": "allow"}


class AuditRowDTO(BaseModel):
    """One row from `execution_audit` — what the coordinator decided,
    what the executor intended, and what the broker actually did. The
    ⌘L Audit modal renders these newest-first per symbol so an operator
    can answer "why didn't we trade NAVER at 09:32?" or "why did we
    flip from BUY to HOLD when the signal was 0.7 confident?".

    `signal_rationale` is the per-agent contributor list — empty when
    the row is malformed in storage (we'd rather render the row with an
    empty rationale than drop it from the modal entirely)."""

    ts:                datetime
    symbol:            str
    mode:              str  # 'live' | 'shadow' | 'noop'
    executed:          bool
    intended_action:   str  # 'buy' | 'hold' | 'sell'
    intended_quantity: int
    order_id:          Optional[str] = None
    blocked_by:        Optional[str] = None
    reason:            Optional[str] = None
    signal_action:     str
    signal_confidence: float
    signal_score:      float
    signal_rationale:  list[dict[str, Any]] = Field(default_factory=list)


class AuditRecentDTO(BaseModel):
    """Response envelope for `/v1/audit/recent`. The list is wrapped in
    an envelope so we can later add cursor pagination / total-count
    metadata without breaking the contract — versioning a top-level
    list shape is awkward."""

    symbol: str
    rows:   list[AuditRowDTO]


class MarketTickDTO(BaseModel):
    """One raw tick row from `market_tick`. Powers the PropertyHUD price
    sparkline so the operator sees per-tick wiggle inside the current
    minute (not just the 1m OHLC aggregate). `price` arrives as a Python
    float at the repo edge — NUMERIC in PG, Decimal in asyncpg, cast to
    float in `MarketRepository.list_recent_ticks` because sparkline
    rendering wants ordinary numbers."""

    ts:     datetime
    price:  float
    volume: int
    side:   str  # 'buy' | 'sell'


class MarketTickRecentDTO(BaseModel):
    """Response envelope for `/v1/ticks/recent`. Wrapped for the same
    forward-compat reason as `AuditRecentDTO` — adding total/cursor
    fields later won't break the contract."""

    symbol: str
    ticks:  list[MarketTickDTO]


class MarketTickSnapshotDTO(BaseModel):
    """Single-symbol entry inside a snapshot response — the LATEST tick
    seen on the wire for that symbol. Used by the KisLiveSnapshot HUD
    grid to show all subscribed tickers at a glance."""

    symbol: str
    ts:     datetime
    price:  float
    volume: int
    side:   str  # 'buy' | 'sell'


class MarketTickSnapshotsDTO(BaseModel):
    """Response envelope for `/v1/ticks/snapshot`. The `requested`
    array preserves the operator's symbol order so the HUD can render
    rows in a deterministic sequence even when DISTINCT ON drops
    symbols without recorded ticks. `snapshots` covers only the
    symbols that have at least one tick on file — missing entries
    surface as empty rows in the HUD."""

    requested: list[str]
    snapshots: list[MarketTickSnapshotDTO]


class MarketTickTapeEntryDTO(BaseModel):
    """One row of the cross-symbol tape (Sprint 5p-E). Same shape as
    MarketTickSnapshotDTO — the type is intentionally separate so the
    OpenAPI schema reflects the distinct surfaces (snapshot = per-symbol
    latest, tape = cross-symbol stream). Frontend keeps them as one
    TypeScript type because the field shape is identical today; if
    they ever diverge (e.g. tape gets a trade-id), splitting will be a
    one-line change."""

    ts:     datetime
    symbol: str
    price:  float
    volume: int
    side:   str


class MarketTickTapeDTO(BaseModel):
    """Response envelope for `/v1/ticks/tape`. Newest-first across all
    requested symbols — forensic surface for "what hit the wire between
    09:34:50 and 09:35:10?". Wrapped in an envelope so a future since/
    cursor parameter doesn't break the contract."""

    entries: list[MarketTickTapeEntryDTO]
