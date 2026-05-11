"""REST v1 router — basecamp surface, now wired to TimescaleDB.

The frontend NEXUS OS reads its bootstrap state through these endpoints
before subscribing to the WebSocket stream. Keep them pure-read where
possible; mutating operations land in dedicated sub-routers.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from ...core.security import Principal, get_current_user, require_principal
from ...domain.market.repository import MarketRepository
from ...infrastructure.database import (
    EXPECTED_SCHEMA_VERSION,
    get_pool,
    verify_schema,
)
from ...infrastructure.execution_repository import ExecutionRepository
from ...infrastructure.redis_pubsub import get_client
from .dto import (
    AuditRecentDTO,
    AuditRowDTO,
    BlockedReasonDTO,
    BlockedReasonsDTO,
    DecisionBucketDTO,
    DecisionRateDTO,
    EdgeDTO,
    EntityDTO,
    MarketTickDTO,
    MarketTickRecentDTO,
    MarketTickSnapshotDTO,
    MarketTickSnapshotsDTO,
    MarketTickTapeDTO,
    MarketTickTapeEntryDTO,
    MarketVolumeBucketDTO,
    MarketVolumeWindowDTO,
    MigrationStatusDTO,
    ReadinessDTO,
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


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe — does NOT touch DB or Redis (use /readyz for that)."""
    return {"status": "ok", "service": "nexus-backend"}


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
    principal: Annotated[Principal, Depends(get_current_user)],
) -> SnapshotDTO:
    """Bootstrap dataset for the canvas — the most recent live frame.

    Reads the entity table (low-cardinality, frequently overwritten) and
    the edge table. Field shapes mirror the frontend `NexusEntity` /
    `NexusEdge` contracts so no client-side adapter is needed.

    Protected: requires a verified Entra ID bearer token (or a dev-bypass
    when Entra is not configured and APP_ENV=development).
    """
    logger.debug("snapshot served to %s (tenant=%s)", principal.subject, principal.tenant)
    entities = await repo.list_entities()
    edges_raw = await repo.list_edges()
    return SnapshotDTO(
        entities=[
            EntityDTO(id=e.id, cluster=e.cluster, anomaly=e.anomaly, tx_vol=e.tx_vol)
            for e in entities
        ],
        edges=[
            EdgeDTO.model_validate({"from": r["from"], "to": r["to"], "weight": r["weight"]})
            for r in edges_raw
        ],
        ts=datetime.now(timezone.utc),
    )


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
