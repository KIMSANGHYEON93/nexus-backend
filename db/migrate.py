"""Minimal migration runner — applies db/migrations/*.sql in order.

Run from project root:
    python -m db.migrate
    python -m db.migrate --dry-run

Each file is wrapped in a transaction so a syntax error rolls back; already-
applied versions (tracked in `schema_version`) are skipped. The runner is
intentionally simple — no Alembic — because the schema surface here is tiny
and TimescaleDB-specific DDL doesn't always survive an ORM round-trip.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
from pathlib import Path

import asyncpg

from src.core.config import get_settings  # noqa: E402  (project-root import)


MIGRATIONS_DIR = Path(__file__).parent / "migrations"
VERSION_PATTERN = re.compile(r"^(\d+)_.+\.sql$")

logger = logging.getLogger("nexus.migrate")


def discover() -> list[tuple[int, Path]]:
    """Return migrations sorted by version. Filenames must be `NNN_*.sql`."""
    out: list[tuple[int, Path]] = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        match = VERSION_PATTERN.match(path.name)
        if not match:
            logger.warning("skipping unversioned migration: %s", path.name)
            continue
        out.append((int(match.group(1)), path))
    return out


async def applied_versions(conn: asyncpg.Connection) -> set[int]:
    exists = await conn.fetchval(
        "SELECT to_regclass('public.schema_version') IS NOT NULL",
    )
    if not exists:
        return set()
    rows = await conn.fetch("SELECT version FROM schema_version")
    return {r["version"] for r in rows}


async def apply(conn: asyncpg.Connection, version: int, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    logger.info("applying migration %03d (%s)", version, path.name)
    async with conn.transaction():
        await conn.execute(sql)


async def run(dry_run: bool = False) -> int:
    settings = get_settings()
    pending = discover()
    if not pending:
        logger.info("no migrations found in %s", MIGRATIONS_DIR)
        return 0

    conn = await asyncpg.connect(dsn=settings.database_url)
    try:
        done = await applied_versions(conn)
        plan = [(v, p) for v, p in pending if v not in done]
        if not plan:
            logger.info("up to date — %d migration(s) already applied", len(done))
            return 0
        logger.info("plan: %s", [v for v, _ in plan])
        if dry_run:
            return 0
        for version, path in plan:
            await apply(conn, version, path)
        logger.info("applied %d migration(s)", len(plan))
        return 0
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply NEXUS OS DB migrations.")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the migration plan without applying it.",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    return asyncio.run(run(dry_run=args.dry_run))


if __name__ == "__main__":
    sys.exit(main())
