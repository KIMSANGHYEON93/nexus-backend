"""Securities domain — investable instruments + cross-instrument relations.

The domain shapes here flow through the pipeline:
    security_master (DB) → SecuritiesRepository → API → NEXUS canvas

Pure Pydantic models with a single computed property (`display_name`) so
the v1 DTO layer stays mechanical. The infrastructure layer translates
asyncpg rows into these objects; the router never sees a raw DB record.
"""

from .models import Market, RelationKind, Security, SecurityRelation

__all__ = ["Market", "RelationKind", "Security", "SecurityRelation"]
