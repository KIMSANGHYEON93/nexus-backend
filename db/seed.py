"""Seed-data runner — opt-in, idempotent, dev-environment only.

Usage:
    python -m db.seed                # apply db/seeds/dev.sql
    python -m db.seed --file other.sql
    python -m db.seed --refuse-prod  # default: refuse to run when APP_ENV=production

Why a separate script (not a migration): seed data is *content* not
*schema*. Mixing them means rolling back a content change becomes a
schema migration. Keeping them apart lets us re-run seeds freely without
touching schema_version.

Production safety: refuses to apply when `APP_ENV` is anything other
than `development` unless the operator passes `--allow-non-dev`.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

import asyncpg

from src.core.config import get_settings  # noqa: E402

SEEDS_DIR = Path(__file__).parent / "seeds"
DEFAULT_SEED_FILE = SEEDS_DIR / "dev.sql"

logger = logging.getLogger("nexus.seed")


async def apply_seed(path: Path) -> None:
    settings = get_settings()
    if settings.app_env != "development":
        raise SystemExit(
            f"refusing to apply seed in app_env={settings.app_env!r} "
            "(pass --allow-non-dev to override)",
        )

    sql = path.read_text(encoding="utf-8")
    if not sql.strip():
        raise SystemExit(f"seed file is empty: {path}")

    conn = await asyncpg.connect(dsn=settings.database_url)
    try:
        async with conn.transaction():
            await conn.execute(sql)
        logger.info(
            "seed applied",
            extra={"event": "seed_applied", "file": str(path), "bytes": len(sql)},
        )
    finally:
        await conn.close()


async def apply_seed_unsafe(path: Path) -> None:
    """Like apply_seed but skips the env check. Reserved for explicit
    --allow-non-dev use; never call from app code."""
    sql = path.read_text(encoding="utf-8")
    settings = get_settings()
    conn = await asyncpg.connect(dsn=settings.database_url)
    try:
        async with conn.transaction():
            await conn.execute(sql)
        logger.warning(
            "seed applied with safety override",
            extra={"event": "seed_applied_unsafe", "file": str(path), "app_env": settings.app_env},
        )
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply NEXUS OS dev seed data.")
    parser.add_argument(
        "--file", type=Path, default=DEFAULT_SEED_FILE,
        help=f"seed SQL file to apply (default: {DEFAULT_SEED_FILE})",
    )
    parser.add_argument(
        "--allow-non-dev", action="store_true",
        help="apply seed even when APP_ENV is not 'development' (DANGEROUS)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    if not args.file.exists():
        print(f"seed file not found: {args.file}", file=sys.stderr)
        return 1

    runner = apply_seed_unsafe if args.allow_non_dev else apply_seed
    asyncio.run(runner(args.file))
    return 0


if __name__ == "__main__":
    sys.exit(main())
