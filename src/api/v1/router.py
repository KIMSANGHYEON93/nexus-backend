"""REST v1 router — basecamp surface, now wired to TimescaleDB.

The frontend NEXUS OS reads its bootstrap state through these endpoints
before subscribing to the WebSocket stream. Keep them pure-read where
possible; mutating operations land in dedicated sub-routers.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Depends

from ...core.security import Principal, get_current_user, require_principal
from ...domain.market.repository import MarketRepository
from ...infrastructure.database import (
    EXPECTED_SCHEMA_VERSION,
    get_pool,
    verify_schema,
)
from ...infrastructure.redis_pubsub import get_client
from .dto import EdgeDTO, EntityDTO, MigrationStatusDTO, ReadinessDTO, SnapshotDTO

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/v1", tags=["v1"])


def _repo() -> MarketRepository:
    """Per-request repository factory. The pool itself is process-wide."""
    return MarketRepository(get_pool())


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
