"""TimescaleDB connection pool — owned at app lifespan scope.

asyncpg is preferred over SQLAlchemy here: the hot path for NEXUS OS is
high-throughput tick ingestion + range scans against hypertables, where
ORM overhead would dominate. For complex aggregations we still drop to
raw SQL anchored to TimescaleDB's `time_bucket` and continuous aggregates.

The pool is created in `main.py`'s lifespan handler and accessed via
`get_pool()` — never instantiate a pool per-request.

Schema-version contract:
    The `schema_version` table is populated by db/migrations/*.sql.
    `EXPECTED_SCHEMA_VERSION` is bumped here whenever a migration is added,
    and the runtime verifies `MAX(schema_version.version) >= expected` on
    startup AND on every /readyz call. A stale container (deployed code
    that needs an unapplied migration) self-reports via /readyz instead of
    silently corrupting writes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import asyncpg

from ..core.config import Settings

logger = logging.getLogger(__name__)


# Bumped in lockstep with db/migrations/NNN_*.sql additions.
EXPECTED_SCHEMA_VERSION: int = 3


_pool: asyncpg.Pool | None = None


# ──────────────────────────────────────────────────────────────────────────
#  Pool lifecycle
# ──────────────────────────────────────────────────────────────────────────

async def init_pool(settings: Settings) -> asyncpg.Pool:
    """Create the global pool. Called once during FastAPI startup."""
    global _pool
    if _pool is not None:
        return _pool
    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=2,
        max_size=20,
        command_timeout=10,
    )
    return _pool


async def close_pool() -> None:
    """Drain and close the pool. Called during FastAPI shutdown."""
    global _pool
    if _pool is None:
        return
    await _pool.close()
    _pool = None


def get_pool() -> asyncpg.Pool:
    """Return the live pool. Raises if accessed before lifespan startup."""
    if _pool is None:
        raise RuntimeError(
            "Database pool not initialized — init_pool() must run during app startup"
        )
    return _pool


# ──────────────────────────────────────────────────────────────────────────
#  Schema version verification
# ──────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SchemaCheck:
    """Outcome of a schema_version probe.

    `applied` is `None` when the `schema_version` table itself is missing
    (a fresh DB with no migrations ever applied). `0` means the table
    exists but is empty — a more partial state worth distinguishing.
    """

    applied:  int | None
    expected: int
    ok:       bool
    reason:   str | None = None


async def verify_schema(
    pool: asyncpg.Pool,
    expected: int = EXPECTED_SCHEMA_VERSION,
) -> SchemaCheck:
    """Probe the `schema_version` table and compare against `expected`.

    Returns a SchemaCheck rather than raising — the caller decides whether
    a stale schema should fail startup or just light up /readyz red.
    The function is read-only and safe to call on every readiness probe.
    """
    try:
        async with pool.acquire() as conn:
            applied = await conn.fetchval(
                "SELECT MAX(version) FROM schema_version",
            )
    except asyncpg.UndefinedTableError:
        logger.error(
            "schema verification failed: schema_version table missing",
            extra={"event": "schema_check", "reason": "table_missing", "expected": expected},
        )
        return SchemaCheck(
            applied=None, expected=expected, ok=False,
            reason="schema_version table missing — no migrations applied",
        )
    except (asyncpg.PostgresError, OSError) as e:
        logger.exception(
            "schema verification failed: database error",
            extra={"event": "schema_check", "reason": "db_error", "error_type": type(e).__name__},
        )
        return SchemaCheck(
            applied=None, expected=expected, ok=False,
            reason=f"database error: {type(e).__name__}",
        )

    if applied is None:
        # Table exists but no rows — partial migration run.
        logger.error(
            "schema verification failed: schema_version table empty",
            extra={"event": "schema_check", "reason": "table_empty", "expected": expected},
        )
        return SchemaCheck(
            applied=0, expected=expected, ok=False,
            reason="schema_version table is empty — no migrations recorded",
        )

    ok = applied >= expected
    if ok:
        logger.info(
            "schema verification passed",
            extra={"event": "schema_check", "applied": applied, "expected": expected},
        )
        return SchemaCheck(applied=applied, expected=expected, ok=True)

    logger.error(
        "schema verification failed: applied < expected (stale container)",
        extra={
            "event": "schema_check", "reason": "stale_schema",
            "applied": applied, "expected": expected,
        },
    )
    return SchemaCheck(
        applied=applied, expected=expected, ok=False,
        reason=f"applied={applied} < expected={expected}: run db/migrate.py",
    )
