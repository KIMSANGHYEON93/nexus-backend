"""TimescaleDB connection pool — owned at app lifespan scope.

asyncpg is preferred over SQLAlchemy here: the hot path for NEXUS OS is
high-throughput tick ingestion + range scans against hypertables, where
ORM overhead would dominate. For complex aggregations we still drop to
raw SQL anchored to TimescaleDB's `time_bucket` and continuous aggregates.

The pool is created in `main.py`'s lifespan handler and accessed via
`get_pool()` — never instantiate a pool per-request.
"""

from __future__ import annotations

import asyncpg

from ..core.config import Settings


_pool: asyncpg.Pool | None = None


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
