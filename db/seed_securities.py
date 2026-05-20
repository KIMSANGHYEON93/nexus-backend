"""Seed security_master + security_relation from db/seeds/securities_master.json.

Unlike db/seed.py (which guards on APP_ENV=development), this script is
production-safe: upsert_seed() is idempotent (ON CONFLICT DO UPDATE) so
running it on every Railway deploy is harmless.

Usage (from nexus-backend/):
    python -m db.seed_securities           # dry-run — shows counts, no writes
    python -m db.seed_securities --apply   # commit to DB
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

import asyncpg

from src.core.config import get_settings
from src.infrastructure.securities_repo import upsert_seed

SEED_FILE = Path(__file__).parent / "seeds" / "securities_master.json"

logger = logging.getLogger("nexus.seed_securities")


async def run(apply: bool) -> int:
    if not SEED_FILE.exists():
        print(f"[ERROR] seed file not found: {SEED_FILE}", file=sys.stderr)
        return 1

    payload = json.loads(SEED_FILE.read_text(encoding="utf-8"))
    securities = payload.get("securities", [])
    relations  = payload.get("relations",  [])

    if not apply:
        print(
            f"[dry-run] would upsert {len(securities)} securities and "
            f"{len(relations)} relations — pass --apply to commit.",
        )
        return 0

    settings = get_settings()
    pool = await asyncpg.create_pool(
        dsn=settings.database_url, min_size=1, max_size=3, command_timeout=30,
    )
    try:
        n_sec, n_rel = await upsert_seed(pool, securities, relations)
        print(f"[OK] upserted {n_sec} securities and {n_rel} relations.")
        logger.info(
            "securities seed applied",
            extra={
                "event":        "securities_seed_applied",
                "n_securities": n_sec,
                "n_relations":  n_rel,
            },
        )
    finally:
        await pool.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Idempotent upsert of securities_master.json into DB.",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Commit the upsert (default: dry-run, no DB writes).",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    return asyncio.run(run(apply=args.apply))


if __name__ == "__main__":
    sys.exit(main())
