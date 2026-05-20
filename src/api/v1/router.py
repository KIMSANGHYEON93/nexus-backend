"""REST v1 router — basecamp surface, now wired to TimescaleDB.

The frontend NEXUS OS reads its bootstrap state through these endpoints
before subscribing to the WebSocket stream. Keep them pure-read where
possible; mutating operations land in dedicated sub-routers.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request, status as http_status
from fastapi.responses import JSONResponse

from ...core.errors import PROBLEM_MEDIA_TYPE, PROBLEM_TYPE_UPSTREAM, PROBLEM_TYPE_VALIDATION, ProblemDetail
from ...core.logging import request_id_var
from ...core.security import Principal, get_current_user, require_principal
from ...domain.alarms.models import Alarm, Severity, Status
from ...domain.alarms.repository import (
    LIMIT_DEFAULT,
    LIMIT_MAX,
    LIMIT_MIN,
    AlarmListFilters,
    AlarmRepository,
)
from ...domain.market.repository import MarketRepository
from ...infrastructure.alarms import build_seeded_repository
from ...infrastructure.database import (
    EXPECTED_SCHEMA_VERSION,
    get_pool,
    verify_schema,
)
from ...infrastructure.execution_repository import ExecutionRepository
from ...infrastructure.redis_pubsub import get_client
from ...infrastructure.securities_repo import SecuritiesRepository
from .dto import (
    AlarmDTO,
    AlarmListDTO,
    AlarmSeverity,
    AlarmStatus,
    AuditRecentDTO,
    AuditRowDTO,
    BalanceDTO,
    BalanceSummaryDTO,
    BlockedReasonDTO,
    BlockedReasonsDTO,
    DecisionBucketDTO,
    DecisionRateDTO,
    EdgeDTO,
    EntityDTO,
    HealthDTO,
    HoldingDTO,
    MarketTickDTO,
    MarketTickRecentDTO,
    MarketTickSnapshotDTO,
    MarketTickSnapshotsDTO,
    MarketTickTapeDTO,
    MarketTickTapeEntryDTO,
    MarketVolumeBucketDTO,
    MarketVolumeWindowDTO,
    MigrationStatusDTO,
    OrderRequestDTO,
    OrderResponseDTO,
    ReadinessDTO,
    SecurityDTO,
    SecurityListDTO,
    SecurityMarket,
    SecurityRelationDTO,
    SecurityRelationKind,
    SnapshotDTO,
)

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/v1", tags=["v1"])


def _repo() -> MarketRepository:
    """Per-request repository factory. The pool itself is process-wide."""
    return MarketRepository(get_pool())


def _audit_repo() -> ExecutionRepository:
    """Per-request audit repository factory. Same pool, distinct table."""
    return ExecutionRepository(get_pool())


class _NullSecuritiesRepo:
    """Stand-in returned when the asyncpg pool has not been initialized
    (e.g. unit tests that mount the router without touching `init_pool`).

    Every read returns the empty result — the calling route degrades into
    a non-enriched response rather than 500-ing on a fresh test app. The
    integration tests that exercise enrichment explicitly install a pool
    mock so they exercise the real `SecuritiesRepository`."""

    async def list_securities(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []

    async def count_securities(self, *args: Any, **kwargs: Any) -> int:
        return 0

    async def get_security(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def get_by_ticker_batch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return {}

    async def list_relations(self, *args: Any, **kwargs: Any) -> list[Any]:
        return []

    async def count_relations(self, *args: Any, **kwargs: Any) -> int:
        return 0


def _securities_repo() -> SecuritiesRepository | _NullSecuritiesRepo:
    """Per-request securities repository factory. Same process-wide pool.

    Falls back to `_NullSecuritiesRepo` when the asyncpg pool hasn't been
    initialized — keeps the existing alarm-API tests (which don't mount
    a DB pool) working while the snapshot/alarms enrichment quietly
    degrades to "no enrichment" instead of raising 500."""
    try:
        return SecuritiesRepository(get_pool())
    except RuntimeError:
        return _NullSecuritiesRepo()


# ── Alarm repository — process-wide in-memory backend (Sprint 5r) ───────
#
# Persistence is deferred per spec §3: the read surface ships now with an
# in-memory store seeded at process start, and the router only holds a
# reference to the `AlarmRepository` Protocol. Swapping to a Timescale-
# backed adapter later means changing only this factory.
#
# Singleton-on-first-use so the seed is built exactly once. Tests reach
# in via `monkeypatch.setattr` to install their own empty repo before
# making any HTTP call.
_alarm_repo_singleton: AlarmRepository | None = None


def _alarm_repo() -> AlarmRepository:
    """Per-request DI factory — returns the process-wide alarm store.

    First call constructs and seeds; subsequent calls re-use. Same shape
    as `_repo()` / `_audit_repo()` so the router's import surface is
    uniform across endpoints.
    """
    global _alarm_repo_singleton
    if _alarm_repo_singleton is None:
        _alarm_repo_singleton = build_seeded_repository()
    return _alarm_repo_singleton


def reset_alarm_repo_for_tests(repo: AlarmRepository | None = None) -> None:
    """Replace (or clear) the process-wide alarm repo. Used by tests that
    want to drive the router against an empty / hand-built repo. Calling
    with `None` makes the next `_alarm_repo()` rebuild from the seed.
    """
    global _alarm_repo_singleton
    _alarm_repo_singleton = repo


@router.get("/health", response_model=HealthDTO)
async def health(request: Request) -> HealthDTO:
    """Liveness probe — does NOT touch DB or Redis (use /readyz for that).
    publisher 필드로 현재 활성 tick 소스를 노출: kis | mock | none."""
    from ...infrastructure.publisher_supervisor import PublisherSupervisor
    supervisor: PublisherSupervisor | None = getattr(request.app.state, "supervisor", None)
    return HealthDTO(
        status="ok",
        service="nexus-backend",
        publisher=supervisor.active_kind if supervisor is not None else "none",
    )


@router.get("/readyz", response_model=ReadinessDTO)
async def readyz() -> ReadinessDTO:
    """Readiness probe — pings every external dependency AND verifies the
    DB schema version. Returns 200 with granular booleans so a load
    balancer / Kubernetes probe / operator dashboard can distinguish
    'app is up but DB is down' from 'app is up, DB is up, but the
    container's code is newer than the applied migrations.'

    The service self-reports `ok=true` only when DB ping AND Redis ping
    AND schema version are all green.
    """
    db_ok = False
    redis_ok = False
    pool = get_pool()

    try:
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
        db_ok = True
    except Exception:  # noqa: BLE001
        logger.exception("readyz: database ping failed")

    try:
        await get_client().ping()
        redis_ok = True
    except Exception:  # noqa: BLE001
        logger.exception("readyz: redis ping failed")

    if db_ok:
        check = await verify_schema(pool)
        migration = MigrationStatusDTO(
            applied=check.applied, expected=check.expected,
            ok=check.ok, reason=check.reason,
        )
    else:
        # Skip schema probe if DB ping failed — duplicate noise; surface
        # the underlying connection failure instead.
        migration = MigrationStatusDTO(
            applied=None, expected=EXPECTED_SCHEMA_VERSION, ok=False,
            reason="database unreachable — schema unknown",
        )

    return ReadinessDTO(
        ok=db_ok and redis_ok and migration.ok,
        database=db_ok, redis=redis_ok, migration=migration,
    )


@router.get("/me")
async def me(
    principal: Annotated[Principal, Depends(require_principal)],
) -> dict[str, Any]:
    """Echo the authenticated identity. Useful for verifying the Entra
    bearer flow end-to-end from the browser."""
    return {
        "subject": principal.subject,
        "tenant": principal.tenant,
        "roles": principal.roles,
        "scopes": principal.scopes,
    }


@router.get("/snapshot", response_model=SnapshotDTO)
async def latest_snapshot(
    repo: Annotated[MarketRepository, Depends(_repo)],
    sec_repo: Annotated[SecuritiesRepository, Depends(_securities_repo)],
    principal: Annotated[Principal, Depends(get_current_user)],
) -> SnapshotDTO:
    """Bootstrap dataset for the canvas — the most recent live frame.

    Reads the entity table (low-cardinality, frequently overwritten) and
    the edge table. Field shapes mirror the frontend `NexusEntity` /
    `NexusEdge` contracts so no client-side adapter is needed.

    Sprint 5s enrichment: when an entity id matches a `security_master.ticker`
    the response carries `display_name` / `ticker` / `sector` so the canvas
    can render the operator's local label without a second round-trip.
    Non-security ontology nodes (sector aggregators, hubs) keep the new
    fields at null — existing clients pre-5s simply ignore them.

    Protected: requires a verified Entra ID bearer token (or a dev-bypass
    when Entra is not configured and APP_ENV=development).
    """
    logger.debug("snapshot served to %s (tenant=%s)", principal.subject, principal.tenant)
    entities = await repo.list_entities()
    edges_raw = await repo.list_edges()

    # ── Securities enrichment (Sprint 5s) ──────────────────────────────
    # One batched lookup against security_master so we do NOT issue one
    # SELECT per entity. Missing tickers simply absent from the dict and
    # the EntityDTO ends up with display_name/ticker/sector = null.
    ticker_ids = [e.id for e in entities]
    sec_map = await sec_repo.get_by_ticker_batch(ticker_ids)

    return SnapshotDTO(
        entities=[
            EntityDTO(
                id=e.id,
                cluster=e.cluster,
                anomaly=e.anomaly,
                tx_vol=e.tx_vol,
                display_name=(sec_map[e.id].display_name if e.id in sec_map else None),
                ticker=(sec_map[e.id].ticker if e.id in sec_map else None),
                sector=(sec_map[e.id].sector if e.id in sec_map else None),
            )
            for e in entities
        ],
        edges=[
            EdgeDTO.model_validate({"from": r["from"], "to": r["to"], "weight": r["weight"]})
            for r in edges_raw
        ],
        ts=datetime.now(timezone.utc),
    )


# ──────────────────────────────────────────────────────────────────────────
#  Securities ontology (Sprint 5s) — GET /v1/securities, ./relations, ./{ticker}
# ──────────────────────────────────────────────────────────────────────────
#
# Route registration order is load-bearing: `/relations` MUST be declared
# before `/{ticker}` or FastAPI's dynamic-segment matcher swallows
# "relations" as a ticker value. The pure-list `/v1/securities` route is
# first because it's the canvas bootstrap; `/relations` second; the
# single-ticker getter last.

_SECURITY_NOT_FOUND_TYPE  = "urn:nexus:errors:security-not-found"
_SECURITY_NOT_FOUND_TITLE = "Security not found"

_VALID_MARKETS = frozenset({"KRX", "KOSDAQ", "NASDAQ", "NYSE", "OTHER"})
_VALID_KINDS   = frozenset({
    "sector", "correlation", "same_chaebol", "supply_chain", "cross_listing",
})


def _parse_markets(raw: str | None) -> list[str] | None:
    """CSV → list[str] of validated `Market` enum values.

    Unknown tokens raise `_InvalidInput` rendered as 400 with the spec's
    `invalid-input` problem-type. We reuse the existing parser scaffolding
    (defined for the alarms router) rather than rebuilding it.
    """
    tokens = _parse_csv(raw, max_items=10)
    if tokens is None:
        return None
    for tok in tokens:
        if tok not in _VALID_MARKETS:
            raise _InvalidInput(
                f"market {tok!r} is not one of "
                f"{'|'.join(sorted(_VALID_MARKETS))}"
            )
    return tokens


def _parse_sectors(raw: str | None) -> list[str] | None:
    """CSV → list[str]. Sector ids are free-form internal strings (per
    spec §2 — up to 32 chars per token, max 20 tokens) so we only enforce
    the bounds; an unknown sector simply returns an empty result set.
    """
    tokens = _parse_csv(raw, max_items=20)
    if tokens is None:
        return None
    for tok in tokens:
        if len(tok) > 32:
            raise _InvalidInput(
                f"sector token length {len(tok)} > 32 ({tok!r})"
            )
    return tokens


def _parse_relation_kinds(raw: str | None) -> list[str] | None:
    """CSV → list[str] of validated `RelationKind` values."""
    tokens = _parse_csv(raw, max_items=10)
    if tokens is None:
        return None
    for tok in tokens:
        if tok not in _VALID_KINDS:
            raise _InvalidInput(
                f"kind {tok!r} is not one of "
                f"{'|'.join(sorted(_VALID_KINDS))}"
            )
    return tokens


def _security_to_dto(sec: Any) -> SecurityDTO:
    """Domain `Security` → wire `SecurityDTO`.

    `display_name` is materialised here (property → field) so the wire
    response carries the operator-visible label without the client
    re-computing the ko/en/ticker fallback. Enums via `.value`.
    """
    return SecurityDTO(
        ticker=sec.ticker,
        display_name=sec.display_name,
        name_ko=sec.name_ko,
        name_en=sec.name_en,
        aliases=list(sec.aliases),
        market=SecurityMarket(sec.market.value),
        sector=sec.sector,
        sector_label=sec.sector_label,
        currency=sec.currency,
        shares_outstanding=sec.shares_outstanding,
        market_cap=sec.market_cap,
        last_price=sec.last_price,
        change_pct=sec.change_pct,
        anomaly=sec.anomaly,
        tx_vol=sec.tx_vol,
        is_subscribed=sec.is_subscribed,
        data_source=sec.data_source,
        updated_at=sec.updated_at,
    )


def _relation_to_dto(rel: Any) -> SecurityRelationDTO:
    """Domain `SecurityRelation` → wire `SecurityRelationDTO`."""
    return SecurityRelationDTO(
        from_ticker=rel.from_ticker,
        to_ticker=rel.to_ticker,
        kind=SecurityRelationKind(rel.kind.value),
        weight=rel.weight,
        directed=rel.directed,
        evidence=rel.evidence,
    )


@router.get("/securities", response_model=SecurityListDTO)
async def securities_list(
    request: Request,
    sec_repo: Annotated[SecuritiesRepository, Depends(_securities_repo)],
    principal: Annotated[Principal, Depends(get_current_user)],
    market: Annotated[str | None, Query(
        description="CSV of KRX|KOSDAQ|NASDAQ|NYSE|OTHER",
    )] = None,
    sector: Annotated[str | None, Query(
        description="CSV of internal sector ids (≤32 chars each, max 20)",
    )] = None,
    search: Annotated[str | None, Query(
        min_length=1, max_length=64,
        description="Ticker / name_ko / name_en / alias prefix match",
    )] = None,
    limit: Annotated[int, Query(
        ge=1, le=1000,
        description="Row cap (1..1000); default 500",
    )] = 500,
) -> SecurityListDTO | JSONResponse:
    """Universe-wide securities master list — bootstrap for the canvas.

    Filters (CSV-encoded, all optional):
      • `market`  — KRX, KOSDAQ, NASDAQ, NYSE, OTHER
      • `sector`  — internal sector ids (e.g. SEMI, FIN, AUTO)
      • `search`  — fuzzy match across ticker / name_ko / name_en / aliases
      • `limit`   — 1..1000

    Returns the matching items, the total count under the same filters
    (for the "SHOWING N OF total" footer), and the server's response
    serialization time. Fault-tolerant: a repo-side failure returns an
    empty envelope rather than 503 so the canvas paints an empty state.
    """
    try:
        markets = _parse_markets(market)
        sectors = _parse_sectors(sector)
    except _InvalidInput as exc:
        return _invalid_input_response(exc, instance=str(request.url.path))

    logger.debug(
        "securities served market=%s sector=%s search=%r limit=%d to %s (tenant=%s)",
        markets or "*", sectors or "*", search, limit,
        principal.subject, principal.tenant,
    )

    items = await sec_repo.list_securities(
        markets=markets, sectors=sectors, search=search, limit=limit,
    )
    total = await sec_repo.count_securities(
        markets=markets, sectors=sectors, search=search,
    )
    return SecurityListDTO(
        items=[_security_to_dto(s) for s in items],
        total=total,
        server_time=datetime.now(timezone.utc),
    )


@router.get("/securities/relations", response_model=list[SecurityRelationDTO])
async def securities_relations(
    request: Request,
    sec_repo: Annotated[SecuritiesRepository, Depends(_securities_repo)],
    principal: Annotated[Principal, Depends(get_current_user)],
    kind: Annotated[str | None, Query(
        description="CSV of sector|correlation|same_chaebol|supply_chain|cross_listing",
    )] = None,
    min_weight: Annotated[float, Query(
        ge=0.0, le=1.0,
        description="Drop edges with weight < this (default: 0.0)",
    )] = 0.0,
    tickers: Annotated[str | None, Query(
        description="CSV of tickers; returns edges touching any of them (≤50)",
    )] = None,
) -> list[SecurityRelationDTO] | JSONResponse:
    """Securities relation edges — input to the canvas force layout.

    Spec §2 declares this route lives at `/v1/securities/relations`; it is
    registered BEFORE `/v1/securities/{ticker}` so FastAPI does not match
    "relations" as a ticker value. Returns a bare array (not envelope) per
    spec §2 response table.
    """
    try:
        kinds          = _parse_relation_kinds(kind)
        ticker_tokens  = _parse_csv(tickers, max_items=50)
    except _InvalidInput as exc:
        return _invalid_input_response(exc, instance=str(request.url.path))

    logger.debug(
        "securities-relations served kind=%s min_weight=%.2f tickers=%s to %s (tenant=%s)",
        kinds or "*", min_weight, ticker_tokens or "*",
        principal.subject, principal.tenant,
    )

    edges = await sec_repo.list_relations(
        kinds=kinds, min_weight=min_weight, tickers=ticker_tokens,
    )
    return [_relation_to_dto(e) for e in edges]


@router.get("/securities/{ticker}", response_model=SecurityDTO)
async def securities_detail(
    request: Request,
    ticker: str,
    sec_repo: Annotated[SecuritiesRepository, Depends(_securities_repo)],
    principal: Annotated[Principal, Depends(get_current_user)],
) -> SecurityDTO | JSONResponse:
    """Single-ticker detail. 404 + ProblemDetail on unknown ticker.

    The 404 carries the spec's `urn:nexus:errors:security-not-found` type
    URI so a frontend can switch on it without parsing the detail string.
    """
    sec = await sec_repo.get_security(ticker)
    if sec is None:
        problem = ProblemDetail(
            type=_SECURITY_NOT_FOUND_TYPE,
            title=_SECURITY_NOT_FOUND_TITLE,
            status=http_status.HTTP_404_NOT_FOUND,
            detail=f"ticker {ticker!r} not in universe",
            instance=str(request.url.path),
            request_id=request_id_var.get(),
        )
        return JSONResponse(
            status_code=http_status.HTTP_404_NOT_FOUND,
            content=problem.model_dump(exclude_none=True),
            media_type=PROBLEM_MEDIA_TYPE,
        )
    logger.debug(
        "securities-detail served ticker=%s to %s (tenant=%s)",
        ticker, principal.subject, principal.tenant,
    )
    return _security_to_dto(sec)


@router.get("/ticks/volume", response_model=MarketVolumeWindowDTO)
async def ticks_volume(
    repo: Annotated[MarketRepository, Depends(_repo)],
    principal: Annotated[Principal, Depends(get_current_user)],
    symbols: Annotated[str, Query(min_length=1, max_length=2048,
                                   description="Comma-separated symbols")],
    window_minutes: Annotated[int, Query(ge=1, le=1440,
                                          description="Lookback window")] = 60,
) -> MarketVolumeWindowDTO:
    """Per-symbol volume aggregate over the trailing `window_minutes`.
    Powers the VolumeHistogram HUD panel — operator can compare which
    KIS subscriptions are getting the most action at a glance.

    Symbols are aligned to the operator's request order so the HUD
    renders rows deterministically; symbols with zero ticks in the
    window come back as `total_volume = 0` entries rather than being
    silently dropped, so the bar chart shows their (empty) presence.

    50-symbol hard cap + 1..1440 (24h) window clamp; empty / whitespace-
    only `symbols` short-circuits to an empty buckets list.
    """
    parts = [s.strip() for s in symbols.split(",") if s.strip()]
    if not parts:
        return MarketVolumeWindowDTO(window_minutes=window_minutes, buckets=[])
    if len(parts) > 50:
        parts = parts[:50]
    since = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    logger.debug(
        "ticks-volume served n=%d window=%dm to %s (tenant=%s)",
        len(parts), window_minutes, principal.subject, principal.tenant,
    )
    rows = await repo.aggregate_volume(symbols=parts, since=since)
    return MarketVolumeWindowDTO(
        window_minutes=window_minutes,
        buckets=[MarketVolumeBucketDTO.model_validate(r) for r in rows],
    )


@router.get("/ticks/tape", response_model=MarketTickTapeDTO)
async def ticks_tape(
    repo: Annotated[MarketRepository, Depends(_repo)],
    principal: Annotated[Principal, Depends(get_current_user)],
    symbols: Annotated[str, Query(min_length=1, max_length=2048,
                                   description="Comma-separated symbols")],
    limit: Annotated[int, Query(ge=1, le=500,
                                 description="Newest-first row cap")] = 100,
) -> MarketTickTapeDTO:
    """Cross-symbol tape — every recorded tick across the requested
    symbols, newest-first. Drives the TapePanel HUD for forensic
    "what hit between 09:34:50 and 09:35:10" review.

    Distinct from `/v1/ticks/snapshot` (which collapses to one row
    per symbol) and `/v1/ticks/recent` (single-symbol per-tick).
    Limit clamped 1..500 at the FastAPI layer; symbol list capped
    at 50 to keep the SQL bounded. Empty/whitespace-only input
    short-circuits to an empty envelope rather than erroring.
    """
    parts = [s.strip() for s in symbols.split(",") if s.strip()]
    if not parts:
        return MarketTickTapeDTO(entries=[])
    if len(parts) > 50:
        parts = parts[:50]
    logger.debug(
        "ticks-tape served n_syms=%d limit=%d to %s (tenant=%s)",
        len(parts), limit, principal.subject, principal.tenant,
    )
    rows = await repo.list_recent_tape(symbols=parts, limit=limit)
    return MarketTickTapeDTO(
        entries=[MarketTickTapeEntryDTO.model_validate(r) for r in rows],
    )


@router.get("/ticks/snapshot", response_model=MarketTickSnapshotsDTO)
async def ticks_snapshot(
    repo: Annotated[MarketRepository, Depends(_repo)],
    principal: Annotated[Principal, Depends(get_current_user)],
    symbols: Annotated[str, Query(min_length=1, max_length=2048,
                                   description="Comma-separated symbols, e.g. '005930,000660'")],
) -> MarketTickSnapshotsDTO:
    """Last-tick snapshot for many symbols in one round-trip. Drives
    the right-column KisLiveSnapshot grid so the operator sees all 12
    KIS subscriptions at a glance without 12 separate fetches.

    Empty symbols (after splitting + trimming) → 422 by Query length
    check above. Symbols with no recorded ticks are silently dropped
    from `snapshots`; the `requested` field preserves the operator's
    input order so the HUD can render placeholder rows for the gaps.

    Hard cap of 50 symbols to keep the SQL bounded — the KIS universe
    is 12 today and unlikely to balloon past that, but a defensive cap
    prevents a malformed query from sweeping the whole hypertable.
    """
    parts = [s.strip() for s in symbols.split(",") if s.strip()]
    if not parts:
        # Empty after trim → equivalent to "no symbols", return empty.
        return MarketTickSnapshotsDTO(requested=[], snapshots=[])
    if len(parts) > 50:
        parts = parts[:50]

    logger.debug(
        "ticks-snapshot served n=%d to %s (tenant=%s)",
        len(parts), principal.subject, principal.tenant,
    )
    rows = await repo.snapshot_per_symbol(symbols=parts)
    return MarketTickSnapshotsDTO(
        requested=parts,
        snapshots=[MarketTickSnapshotDTO.model_validate(r) for r in rows],
    )


@router.get("/ticks/recent", response_model=MarketTickRecentDTO)
async def ticks_recent(
    repo: Annotated[MarketRepository, Depends(_repo)],
    principal: Annotated[Principal, Depends(get_current_user)],
    symbol: Annotated[str, Query(min_length=1, max_length=64,
                                  description="Entity / KIS ticker — e.g. '005930'")],
    limit: Annotated[int, Query(ge=1, le=500,
                                 description="Newest-first tick cap")] = 60,
) -> MarketTickRecentDTO:
    """Recent raw ticks for one symbol, newest-first. Powers the
    PropertyHUD price sparkline (Sprint 5p-C) so the operator sees the
    actual price wiggle inside the current minute instead of a
    1m-rounded OHLC bar. Authenticated under the same dev-bypass rule
    as `/v1/snapshot` and `/v1/audit/recent`.

    Empty list on DB issue (repo-side fault tolerance) — the HUD
    renders an empty sparkline + status hint rather than 500-ing.
    """
    logger.debug(
        "ticks served symbol=%s limit=%d to %s (tenant=%s)",
        symbol, limit, principal.subject, principal.tenant,
    )
    rows = await repo.list_recent_ticks(symbol=symbol, limit=limit)
    return MarketTickRecentDTO(
        symbol=symbol,
        ticks=[MarketTickDTO.model_validate(r) for r in rows],
    )


@router.get("/metrics/decisions", response_model=DecisionRateDTO)
async def metrics_decisions(
    audit: Annotated[ExecutionRepository, Depends(_audit_repo)],
    principal: Annotated[Principal, Depends(get_current_user)],
    window_minutes: Annotated[int, Query(ge=1, le=1440,
                                          description="Lookback window")] = 30,
) -> DecisionRateDTO:
    """Per-minute coordinator decision counts over the trailing window.
    Powers the SystemHealthPanel decisions/min sparkline. Splits the
    total into live-fill / shadow / noop / blocked so the operator can
    catch "all noop right now" vs "fills firing" at a glance.

    Window clamped 1..1440 (24h) at FastAPI layer. Empty array on DB
    error so the HUD renders a flat trace instead of 500-ing.
    """
    since = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    logger.debug(
        "metrics-decisions served window=%dm to %s (tenant=%s)",
        window_minutes, principal.subject, principal.tenant,
    )
    rows = await audit.aggregate_decisions_per_minute(since=since)
    return DecisionRateDTO(
        window_minutes=window_minutes,
        buckets=[DecisionBucketDTO.model_validate(r) for r in rows],
    )


@router.get("/metrics/blocked", response_model=BlockedReasonsDTO)
async def metrics_blocked(
    audit: Annotated[ExecutionRepository, Depends(_audit_repo)],
    principal: Annotated[Principal, Depends(get_current_user)],
    window_minutes: Annotated[int, Query(ge=1, le=1440,
                                          description="Lookback window")] = 60,
) -> BlockedReasonsDTO:
    """Distribution of guardrail blocks over the trailing window.
    Powers the SystemHealthPanel blocked-reason breakdown chart —
    operator sees which guard (cooldown / max_position / volatility_
    breaker) is filtering the most signals.

    `total_blocked` is pre-summed so the HUD doesn't have to reduce
    the array client-side just to label the panel.
    """
    since = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    rows = await audit.aggregate_blocked_reasons(since=since)
    total = sum(int(r["n_blocked"]) for r in rows)
    logger.debug(
        "metrics-blocked served window=%dm total=%d to %s (tenant=%s)",
        window_minutes, total, principal.subject, principal.tenant,
    )
    return BlockedReasonsDTO(
        window_minutes=window_minutes,
        total_blocked=total,
        reasons=[BlockedReasonDTO.model_validate(r) for r in rows],
    )


@router.get("/audit/recent", response_model=AuditRecentDTO)
async def audit_recent(
    audit: Annotated[ExecutionRepository, Depends(_audit_repo)],
    principal: Annotated[Principal, Depends(get_current_user)],
    symbol: Annotated[str, Query(min_length=1, max_length=64,
                                  description="Entity / KIS ticker — e.g. '005930'")],
    limit: Annotated[int, Query(ge=1, le=200,
                                 description="Newest-first row cap")] = 20,
) -> AuditRecentDTO:
    """Recent audit rows for one symbol, newest-first. Powers the ⌘L Audit
    modal: every coordinator decision (live fill / shadow / noop / blocked)
    surfaces with intended action, guard verdict, and the per-agent
    rationale that produced it.

    Repository read is fault-tolerant — an upstream DB hiccup yields an
    empty list rather than a 500 so the modal renders an empty state and
    the operator stays unblocked. A subsequent call retries cleanly. We
    do NOT pretend "no rows" is the same as "DB down" beyond this layer:
    the warning log on the repo side is where ops finds the actual cause.

    Auth: same anonymous-OK-in-dev rule as `/v1/snapshot`.
    """
    logger.debug(
        "audit served symbol=%s limit=%d to %s (tenant=%s)",
        symbol, limit, principal.subject, principal.tenant,
    )
    rows = await audit.fetch_recent(symbol=symbol, limit=limit)
    return AuditRecentDTO(
        symbol=symbol,
        rows=[AuditRowDTO.model_validate(r) for r in rows],
    )


# ──────────────────────────────────────────────────────────────────────────
#  Operator alarms (Sprint 5r) — GET /v1/alarms
# ──────────────────────────────────────────────────────────────────────────
#
# Read-side surface for the right-column AlarmPanel HUD. Polled at 4s by
# the frontend hook; spec §2 defines the CSV-encoded query params and
# the AlarmListDTO envelope. Fault-tolerance policy mirrors
# `/v1/audit/recent` — a repo-side failure yields an empty `items` list
# and the operator's HUD renders an empty state instead of 500-ing.

_DEFAULT_WINDOW = timedelta(hours=24)
_INVALID_INPUT_TYPE = "https://nexus-os.local/problems/invalid-input"
_INVALID_INPUT_TITLE = "Invalid query parameter"


class _InvalidInput(Exception):
    """Raised by the alarms query-param parsers when an enum/format is
    malformed. Carries the spec's `invalid-input` problem-type URI plus a
    human detail string that echoes the offending value. The route handler
    catches this and renders it as `application/problem+json` so the
    response body matches RFC 7807 with the correct `type` URI (instead
    of Starlette's default `about:blank`).
    """

    __slots__ = ("detail",)

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


def _invalid_input_response(exc: _InvalidInput, instance: str) -> JSONResponse:
    """Render `_InvalidInput` → 400 `application/problem+json` with the
    spec's `invalid-input` type URI. Matches the shape produced by
    `core.exception_handlers.http_exception_handler` so the frontend sees
    a uniform ProblemDetail across every error path.
    """
    problem = ProblemDetail(
        type=_INVALID_INPUT_TYPE,
        title=_INVALID_INPUT_TITLE,
        status=http_status.HTTP_400_BAD_REQUEST,
        detail=exc.detail,
        instance=instance,
        request_id=request_id_var.get(),
    )
    return JSONResponse(
        status_code=http_status.HTTP_400_BAD_REQUEST,
        content=problem.model_dump(exclude_none=True),
        media_type=PROBLEM_MEDIA_TYPE,
    )


def _parse_csv(raw: str | None, max_items: int = 50) -> list[str] | None:
    """Split a comma-separated query value into trimmed non-empty tokens.

    Returns None when the param is absent or trims to zero tokens — the
    repository layer reads None as "no filter on this dimension". Hard
    cap on token count keeps the in-memory linear scan bounded even if
    a buggy client sends a 10k-token list.
    """
    if raw is None:
        return None
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if not parts:
        return None
    if len(parts) > max_items:
        parts = parts[:max_items]
    return parts


def _parse_severities(raw: str | None) -> frozenset[Severity] | None:
    """CSV → `frozenset[Severity]`. Unknown tokens raise `_InvalidInput`
    which the route handler renders as a 400 ProblemDetail with the spec's
    `invalid-input` type URI (per spec — "severity 'foo' is not one of
    info|warn|anomaly|critical")."""
    tokens = _parse_csv(raw)
    if tokens is None:
        return None
    out: set[Severity] = set()
    for tok in tokens:
        try:
            out.add(Severity(tok))
        except ValueError as e:
            raise _InvalidInput(
                f"severity {tok!r} is not one of "
                f"{'|'.join(s.value for s in Severity)}"
            ) from e
    return frozenset(out) if out else None


def _parse_statuses(raw: str | None) -> frozenset[Status]:
    """CSV → `frozenset[Status]`. Default (raw=None) is `{ACTIVE}`,
    matching the spec's "status filter default is active". Unknown tokens
    raise `_InvalidInput` → 400 ProblemDetail with `invalid-input` URI."""
    tokens = _parse_csv(raw)
    if tokens is None:
        return frozenset({Status.ACTIVE})
    out: set[Status] = set()
    for tok in tokens:
        try:
            out.add(Status(tok))
        except ValueError as e:
            raise _InvalidInput(
                f"status {tok!r} is not one of "
                f"{'|'.join(s.value for s in Status)}"
            ) from e
    return frozenset(out) if out else frozenset({Status.ACTIVE})


def _parse_since(raw: str | None) -> datetime | None:
    """RFC 3339 / ISO-8601 → tz-aware datetime. `Z` suffix supported.
    A malformed timestamp raises `_InvalidInput` → 400 ProblemDetail with
    `invalid-input` URI (matches spec §2 error table)."""
    if raw is None:
        return None
    cleaned = raw.strip()
    if not cleaned:
        return None
    # `fromisoformat` since 3.11 accepts trailing 'Z' on the same call,
    # but we normalize for cross-runtime safety.
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError as e:
        raise _InvalidInput(
            f"since {raw!r} is not a valid RFC 3339 / ISO-8601 timestamp"
        ) from e
    if parsed.tzinfo is None:
        # Treat naive timestamps as UTC — the spec mandates UTC, and we
        # don't want a missing offset to silently shift the window.
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _alarm_to_dto(alarm: Alarm, entity_display: str | None = None) -> AlarmDTO:
    """Domain `Alarm` → wire `AlarmDTO`. Enums via `.value`, datetimes
    pass through to Pydantic's default ISO-8601 serializer. Snake-case
    is identity on the field names (spec §3 mapping table).

    `entity_display` is the Sprint 5s securities-enrichment hook —
    populated by the alarms handler when `alarm.entity_id` matches a
    `security_master.ticker`; null otherwise."""
    return AlarmDTO(
        id=alarm.id,
        severity=AlarmSeverity(alarm.severity.value),
        status=AlarmStatus(alarm.status.value),
        source=alarm.source,
        code=alarm.code,
        title=alarm.title,
        message=alarm.message,
        occurred_at=alarm.occurred_at,
        entity_id=alarm.entity_id,
        entity_display=entity_display,
        acknowledged_at=alarm.acknowledged_at,
        resolved_at=alarm.resolved_at,
        metadata=alarm.metadata,
    )


@router.get("/alarms", response_model=AlarmListDTO)
async def alarms_list(
    request: Request,
    repo: Annotated[AlarmRepository, Depends(_alarm_repo)],
    sec_repo: Annotated[SecuritiesRepository, Depends(_securities_repo)],
    principal: Annotated[Principal, Depends(get_current_user)],
    limit: Annotated[int, Query(
        ge=LIMIT_MIN, le=LIMIT_MAX,
        description="Newest-first row cap (1..200)",
    )] = LIMIT_DEFAULT,
    since: Annotated[str | None, Query(
        description="ISO-8601 UTC lookback start; default is server_time - 24h",
    )] = None,
    severity: Annotated[str | None, Query(
        description="CSV of info|warn|anomaly|critical",
    )] = None,
    status: Annotated[str | None, Query(
        description="CSV of active|acknowledged|resolved (default: active)",
    )] = None,
    source: Annotated[str | None, Query(
        description="CSV of source identifiers (kebab-case)",
    )] = None,
) -> AlarmListDTO | JSONResponse:
    """Operator alarm list — newest-first, filterable, fault-tolerant.

    Powers the right-column AlarmPanel HUD. Frontend polls every 4s; we
    keep the read path forgiving (empty list on repo trouble) so the
    panel never goes red just because the store hiccuped. Authentication
    matches `/v1/snapshot` (Entra bearer in prod; anonymous dev-bypass
    when Entra isn't configured and APP_ENV=development).

    Filters are CSV-encoded query params parsed in-router (FastAPI's
    Pydantic-driven validation can't enumerate per-token enum values for
    a comma-separated string). Unknown enum tokens produce a 400 with the
    spec's `invalid-input` problem-type URI and the offending value
    echoed in `detail`. `since` defaults to `now - 24h` so the HUD has a
    bounded window even when the operator hasn't passed a timestamp.

    `unacknowledged_count` is the GLOBAL active-count — spec mandates it
    is independent of the page filters so the panel header always shows
    the true unack total even while the operator is filtering the list.
    """
    # ── Parse + validate query inputs ─────────────────────────────────
    # `_InvalidInput` carries the spec's invalid-input type URI. Catch
    # here (rather than letting FastAPI render via HTTPException, which
    # produces `type=about:blank`) so the 400 body matches RFC 7807 with
    # the correct `type` field per spec §2 error table.
    try:
        severities = _parse_severities(severity)
        statuses   = _parse_statuses(status)
        sources    = _parse_csv(source, max_items=20)
        since_dt   = _parse_since(since)
    except _InvalidInput as exc:
        return _invalid_input_response(exc, instance=str(request.url.path))

    server_time = datetime.now(timezone.utc)
    window_since = since_dt if since_dt is not None else server_time - _DEFAULT_WINDOW

    filters = AlarmListFilters(
        statuses=statuses,
        severities=severities,
        sources=tuple(sources) if sources is not None else None,
        since=window_since,
        limit=limit,
    )

    logger.debug(
        "alarms served limit=%d sev=%s status=%s src=%s to %s (tenant=%s)",
        limit,
        sorted(s.value for s in severities) if severities else "*",
        sorted(s.value for s in statuses),
        sources if sources else "*",
        principal.subject, principal.tenant,
    )

    # ── Fetch from repo — fault-tolerant on backend trouble ────────────
    # Two reads (`list` + `count_active`) so the global UNACK badge is
    # independent of the page filter. Per spec: a transient repo failure
    # yields a 200 with empty items and unacknowledged_count=0 rather
    # than a 503 — the HUD renders an empty state and recovers on the
    # next poll. Domain-invariant violations are different — those are
    # programmer errors and we want them surfaced as 500.
    try:
        items, total = await repo.list(filters)
        unacked = await repo.count_active()
    except ValueError:
        # Domain invariant violated upstream of the router — re-raise so
        # the 500 catch-all renders an internal-error ProblemDetail.
        # ValueError signals "the data we stored was malformed", which is
        # a different class of problem from a transient backend hiccup.
        raise
    except Exception:  # noqa: BLE001
        # Anything else (network, OS, DB driver) collapses to an empty
        # envelope — the spec's fault-tolerant fallback.
        logger.exception("alarms repo read failed; returning empty envelope")
        items, total, unacked = [], 0, 0

    # ── Securities enrichment (Sprint 5s) ──────────────────────────────
    # Batch-fetch master rows for every distinct entity_id that looks like
    # a ticker, then map per-row. Empty dict on master-side failure so
    # the panel still renders with bare entity_id strings.
    ticker_ids = sorted({a.entity_id for a in items if a.entity_id})
    sec_map = await sec_repo.get_by_ticker_batch(ticker_ids) if ticker_ids else {}

    return AlarmListDTO(
        items=[
            _alarm_to_dto(
                a,
                entity_display=(
                    sec_map[a.entity_id].display_name
                    if a.entity_id and a.entity_id in sec_map
                    else None
                ),
            )
            for a in items
        ],
        total=total,
        unacknowledged_count=unacked,
        window_since=window_since,
        server_time=server_time,
    )


# ──────────────────────────────────────────────────────────────────────────
#  KIS Account Balance (GET /v1/balance)
# ──────────────────────────────────────────────────────────────────────────
#
# Mock-mode: when no KIS client was armed at startup (no creds, dev mode),
# `balance_client` is None and we return a synthetic 10M KRW balance so
# the BalancePanel HUD has data to render in dev.
#
# Live-mode: delegates to KisBalanceClient.fetch_balance() and maps the
# result into BalanceDTO. KisAuthError / KisUpstreamError → 503 with the
# spec's upstream-error problem-type URI (the panel can display a degraded
# indicator without crashing the whole HUD).


def _get_balance_client(request: Request):
    """Per-request balance client accessor. Extracted as a named function
    so integration tests can monkeypatch it without reaching into app.state."""
    return getattr(request.app.state, "balance_client", None)


@router.get("/balance", response_model=BalanceDTO)
async def get_balance(request: Request) -> BalanceDTO | JSONResponse:
    """Current KIS account balance.

    Returns a synthetic 10M KRW balance when no KIS client is configured
    (dev mode / no credentials). In live mode calls KIS inquire-balance
    and maps the result; KIS failures yield 503 with the upstream-error
    problem-type so the HUD can show a degraded indicator.
    """
    balance_client = _get_balance_client(request)
    if balance_client is None:
        return BalanceDTO(
            summary=BalanceSummaryDTO(
                cash=10_000_000,
                eval_total=10_000_000,
                profit_loss=0,
                profit_loss_pct=0.0,
            ),
            holdings=[],
            ts=datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        )
    try:
        result = await balance_client.fetch_balance()
    except Exception as exc:
        from ...infrastructure.kis_client import KisAuthError, KisUpstreamError
        if isinstance(exc, (KisAuthError, KisUpstreamError)):
            return JSONResponse(
                status_code=503,
                media_type=PROBLEM_MEDIA_TYPE,
                content=ProblemDetail(
                    type=PROBLEM_TYPE_UPSTREAM,
                    title="KIS balance unavailable",
                    detail=str(exc),
                    status=503,
                ).model_dump(exclude_none=True),
            )
        raise
    return BalanceDTO(
        summary=BalanceSummaryDTO(
            cash=result.cash,
            eval_total=result.eval_total,
            profit_loss=result.profit_loss,
            profit_loss_pct=result.profit_loss_pct,
        ),
        holdings=[
            HoldingDTO(
                symbol=h.symbol,
                name=h.name,
                quantity=h.quantity,
                avg_price=h.avg_price,
                current_price=h.current_price,
                eval_amount=h.eval_amount,
                profit_loss=h.profit_loss,
                profit_loss_pct=h.profit_loss_pct,
            )
            for h in result.holdings
        ],
        ts=datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    )


# ── POST /v1/order — Manual Order Entry ─────────────────────────────────────
#
# Operator-initiated manual order. Bypasses the coordinator pipeline and
# calls KisOrderClient directly. Mock mode returns a synthetic accepted
# response so the UI can be tested without KIS credentials.


def _get_order_client(request: Request):
    """Named function so integration tests can monkeypatch without app.state."""
    return getattr(request.app.state, "order_client", None)


@router.post("/order", response_model=OrderResponseDTO, status_code=201)
async def post_order(body: OrderRequestDTO, request: Request) -> OrderResponseDTO | JSONResponse:
    import uuid
    from ...domain.trading.models import Action
    from ...infrastructure.kis_client import KisAuthError, KisUpstreamError

    order_client = _get_order_client(request)

    if order_client is None:
        return OrderResponseDTO(
            order_id=f"MOCK-{uuid.uuid4().hex[:8].upper()}",
            symbol=body.symbol,
            action=body.action,
            quantity=body.quantity,
            status="accepted",
            message="Mock mode: order accepted (no KIS credentials)",
            ts=datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        )

    action = Action(body.action)

    try:
        result = await order_client.place_order(
            symbol=body.symbol,
            action=action,
            quantity=body.quantity,
            order_type=body.order_type,
            price=body.price,
        )
    except ValueError as exc:
        return JSONResponse(
            status_code=422,
            media_type=PROBLEM_MEDIA_TYPE,
            content=ProblemDetail(
                type=PROBLEM_TYPE_VALIDATION,
                title="Invalid order parameters",
                detail=str(exc),
                status=422,
                request_id=request_id_var.get(),
            ).model_dump(exclude_none=True),
        )
    except (KisAuthError, KisUpstreamError) as exc:
        return JSONResponse(
            status_code=503,
            media_type=PROBLEM_MEDIA_TYPE,
            content=ProblemDetail(
                type=PROBLEM_TYPE_UPSTREAM,
                title="KIS order unavailable",
                detail=str(exc),
                status=503,
                request_id=request_id_var.get(),
            ).model_dump(exclude_none=True),
        )

    return OrderResponseDTO(
        order_id=result.order_id or f"MOCK-{uuid.uuid4().hex[:8].upper()}",
        symbol=body.symbol,
        action=body.action,
        quantity=body.quantity,
        status="accepted" if result.success else "rejected",
        message=result.message,
        ts=datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    )
