"""Tests for the schema-version verification path.

Exercises the four logical states `verify_schema` distinguishes:
  • applied >= expected           → ok=True
  • applied < expected            → ok=False, "stale_schema"
  • schema_version table empty    → ok=False, applied=0
  • schema_version table missing  → ok=False, applied=None
  • database unreachable / errors → ok=False, applied=None

The whole module skips on hosts without asyncpg installed (Windows dev
box); CI / Docker runs it end-to-end against the imported asyncpg
exception classes.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

asyncpg = pytest.importorskip("asyncpg", reason="full backend deps required")

from src.infrastructure.database import (
    EXPECTED_SCHEMA_VERSION,
    SchemaCheck,
    verify_schema,
)


def _fake_pool(fetchval_result=None, fetchval_raises: Exception | None = None) -> MagicMock:
    """Build a MagicMock whose `acquire()` is an async-context-manager
    yielding a connection whose `fetchval` returns or raises as configured.
    Mirrors asyncpg's `pool.acquire()` interface without needing a real DB.
    """
    conn = MagicMock()
    if fetchval_raises is not None:
        conn.fetchval = AsyncMock(side_effect=fetchval_raises)
    else:
        conn.fetchval = AsyncMock(return_value=fetchval_result)

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=None)

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=cm)
    return pool


@pytest.mark.asyncio
async def test_applied_equal_to_expected_is_ok():
    pool = _fake_pool(fetchval_result=EXPECTED_SCHEMA_VERSION)
    check = await verify_schema(pool)
    assert check == SchemaCheck(
        applied=EXPECTED_SCHEMA_VERSION,
        expected=EXPECTED_SCHEMA_VERSION,
        ok=True,
        reason=None,
    )


@pytest.mark.asyncio
async def test_applied_above_expected_is_ok():
    """Future-version DB (rolling deploy mid-window) is fine — newer
    schema is backwards compatible by our migration policy."""
    pool = _fake_pool(fetchval_result=EXPECTED_SCHEMA_VERSION + 5)
    check = await verify_schema(pool)
    assert check.ok is True
    assert check.applied == EXPECTED_SCHEMA_VERSION + 5


@pytest.mark.asyncio
async def test_applied_below_expected_is_stale():
    pool = _fake_pool(fetchval_result=0)  # imagine expected=1, applied=0
    check = await verify_schema(pool, expected=1)
    assert check.ok is False
    assert check.applied == 0
    assert check.expected == 1
    assert "applied=0" in (check.reason or "")
    assert "db/migrate.py" in (check.reason or "")


@pytest.mark.asyncio
async def test_schema_version_table_empty_is_not_ok():
    """Table exists but no rows → MAX(version) returns NULL → applied=0, ok=False."""
    pool = _fake_pool(fetchval_result=None)
    check = await verify_schema(pool)
    assert check.ok is False
    assert check.applied == 0
    assert "empty" in (check.reason or "").lower()


@pytest.mark.asyncio
async def test_schema_version_table_missing():
    pool = _fake_pool(fetchval_raises=asyncpg.UndefinedTableError("relation does not exist"))
    check = await verify_schema(pool)
    assert check.ok is False
    assert check.applied is None
    assert "missing" in (check.reason or "").lower()


@pytest.mark.asyncio
async def test_database_connection_error_reported_safely():
    """Pool transport failures must NOT bubble up — readiness probes
    that crash defeat the whole point of having a probe."""
    pool = _fake_pool(fetchval_raises=OSError("connection refused"))
    check = await verify_schema(pool)
    assert check.ok is False
    assert check.applied is None
    assert "OSError" in (check.reason or "")


@pytest.mark.asyncio
async def test_postgres_error_reported_safely():
    pool = _fake_pool(fetchval_raises=asyncpg.PostgresError("server gone away"))
    check = await verify_schema(pool)
    assert check.ok is False
    assert check.applied is None


@pytest.mark.asyncio
async def test_explicit_expected_overrides_module_default():
    """Caller can override expected version (useful for staging/canary
    deploys where the binary is ahead of the migration window)."""
    pool = _fake_pool(fetchval_result=10)
    check = await verify_schema(pool, expected=15)
    assert check.ok is False
    assert check.applied == 10
    assert check.expected == 15
