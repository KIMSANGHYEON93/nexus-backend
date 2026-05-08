"""HTTP response DTOs for the v1 surface.

Field names are chosen to mirror the NEXUS OS frontend `NexusEntity` /
`NexusEdge` types in `repo/src/types/nexus.ts` so the browser can use the
payload without an adapter layer. If we ever need to break that contract,
this is the one place to bump a v2 router with new DTOs and keep v1
responding the old shape.
"""

from datetime import datetime
from typing import Optional

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
