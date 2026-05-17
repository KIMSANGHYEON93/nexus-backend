"""HTTP response DTOs for the v1 surface.

Field names are chosen to mirror the NEXUS OS frontend `NexusEntity` /
`NexusEdge` types in `repo/src/types/nexus.ts` so the browser can use the
payload without an adapter layer. If we ever need to break that contract,
this is the one place to bump a v2 router with new DTOs and keep v1
responding the old shape.
"""

from datetime import datetime
from enum import Enum
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


class MarketVolumeBucketDTO(BaseModel):
    """Volume aggregate for one symbol over the requested window.
    `total_volume = 0, tick_count = 0` for symbols with no recorded
    ticks — caller gets one entry per requested symbol so the HUD
    renders an empty bar rather than dropping the row entirely."""

    symbol:       str
    total_volume: int
    tick_count:   int


class MarketVolumeWindowDTO(BaseModel):
    """Response envelope for `/v1/ticks/volume` — relative volume
    histogram source. `window_minutes` echoed back so the HUD can
    label the panel ("VOLUME · 60M") without re-parsing the request."""

    window_minutes: int
    buckets:        list[MarketVolumeBucketDTO]


class DecisionBucketDTO(BaseModel):
    """One minute-bucket of coordinator decision activity. Powers the
    SystemHealthPanel decisions/min sparkline + ALIVE indicator.

    Split counters let the HUD show the mix (live fills vs shadow vs
    noop) on top of total throughput — operator catches "all decisions
    are noop right now" without scanning audit rows by hand."""

    bucket:    datetime
    n_total:   int
    n_live:    int
    n_shadow:  int
    n_noop:    int
    n_blocked: int


class DecisionRateDTO(BaseModel):
    """Response envelope for `/v1/metrics/decisions`. `window_minutes`
    echoed back so the HUD can label the chart ("DECISIONS · 30M")
    without re-parsing the request. Buckets are newest-first; the HUD
    reverses for left-to-right time axis."""

    window_minutes: int
    buckets:        list[DecisionBucketDTO]


class BlockedReasonDTO(BaseModel):
    """One guardrail's blocked-decision count within the window, plus
    the most recent timestamp it fired. Sorted desc by `n_blocked` at
    the SQL layer so the operator sees the dominant rate-limiter at
    the top of the breakdown chart."""

    guard_id:      str
    n_blocked:     int
    last_fired_at: datetime


class BlockedReasonsDTO(BaseModel):
    """Response envelope for `/v1/metrics/blocked`. `total_blocked` is
    pre-summed across reasons so the HUD doesn't have to add the
    array client-side just to label the panel."""

    window_minutes: int
    total_blocked:  int
    reasons:        list[BlockedReasonDTO]


# ──────────────────────────────────────────────────────────────────────────
#  Operator Alarms — Sprint 5r (GET /v1/alarms)
# ──────────────────────────────────────────────────────────────────────────
#
# Wire enums mirror the domain enums in `src/domain/alarms/models.py` 1:1.
# We don't `from ...domain.alarms import Severity` here on purpose — the
# DTO layer is the boundary, and we want the OpenAPI schema to enumerate
# the literal strings without making the consumer guess at the domain
# package's import path. The string values are identical, so the mapping
# is a `.value` lookup in the router.


class AlarmSeverity(str, Enum):
    """Operator-visible severity enum on the JSON wire — string values
    must match `src.domain.alarms.models.Severity` exactly."""

    INFO     = "info"
    WARN     = "warn"
    ANOMALY  = "anomaly"
    CRITICAL = "critical"


class AlarmStatus(str, Enum):
    """Alarm lifecycle on the JSON wire — string values must match
    `src.domain.alarms.models.Status` exactly."""

    ACTIVE       = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED     = "resolved"


class AlarmDTO(BaseModel):
    """One row in the alarm panel — snake_case fields direct from the
    domain model. The HUD reads every field by name; the only thing the
    router does is cast enums to their `.value` and pass timestamps
    through Pydantic's default ISO-8601 serializer."""

    id:               str
    severity:         AlarmSeverity
    status:           AlarmStatus
    source:           str
    code:             str
    title:            str
    message:          str
    occurred_at:      datetime
    entity_id:        Optional[str]                = None
    acknowledged_at:  Optional[datetime]           = None
    resolved_at:      Optional[datetime]           = None
    metadata:         Optional[dict[str, Any]]     = None


class AlarmListDTO(BaseModel):
    """Response envelope for `GET /v1/alarms`.

    `unacknowledged_count` is the GLOBAL count of `status==active`
    alarms, independent of the page filters — the panel header shows
    "{n} UNACK" regardless of which severities the operator has filtered
    to view. `window_since` echoes the effective lookback start (whether
    explicitly requested or filled with `server_time - 24h` by the
    router) so the HUD can label the time scale. `server_time` is the
    response serialization moment, used by the frontend for clock-skew
    correction on "age" labels."""

    items:                list[AlarmDTO]
    total:                int
    unacknowledged_count: int
    window_since:         Optional[datetime]
    server_time:          datetime


class HealthDTO(BaseModel):
    status:    str   # 항상 "ok" — liveness probe는 5xx를 반환하지 않음
    service:   str   # "nexus-backend"
    publisher: str   # "kis" | "mock" | "none"
