"""ExecutionRepository — Sprint 5m audit-ledger writer for execution_audit.

Per-decision insert (no batching needed — execution rate is bounded by
trade rate, orders of magnitude lower than tick rate). Owned by the
PersistenceWorker; the trading pipeline never touches this directly.

Audit row payload mirrors the JSON envelope the TradingPipeline publishes
on `nexus.trading.audit`:

    {
      "ts":              ISO 8601 UTC,
      "symbol":          "005930",
      "mode":            "live" | "shadow" | "noop",
      "executed":        bool,
      "intended_action": "buy" | "hold" | "sell",
      "intended_quantity": int,
      "order_id":        str | null,
      "blocked_by":      str | null,    # guard_id when blocked
      "reason":          str | null,
      "signal": {
        "action":     "buy" | "hold" | "sell",
        "confidence": float [0,1],
        "score":      float [-1,+1],
        "rationale":  [{agent_id, action, confidence}, ...]
      }
    }

The repository is intentionally opinionated about what audit it stores —
NEVER drop fields silently. Schema evolution goes through new migrations,
not adapter-side coercion.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)


_INSERT_AUDIT_SQL = """
    INSERT INTO execution_audit (
        ts, symbol, mode, executed,
        intended_action, intended_quantity,
        order_id, blocked_by, reason,
        signal_action, signal_confidence, signal_score,
        signal_rationale
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
"""


class ExecutionRepository:
    """Owner of one row insert per pipeline decision."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def insert(self, envelope: dict[str, Any]) -> bool:
        """Insert one audit row from the JSON envelope. Returns True on
        success. Returns False on DB error (logged, not raised) so the
        worker keeps consuming the next message instead of dying.
        """
        try:
            row = self._envelope_to_row(envelope)
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning(
                "execution_repo.bad_envelope",
                extra={
                    "event":      "execution_repo_bad_envelope",
                    "error_type": type(exc).__name__,
                    "error":      str(exc)[:200],
                },
            )
            return False

        try:
            async with self._pool.acquire() as conn:
                await conn.execute(_INSERT_AUDIT_SQL, *row)
        except (
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            OSError, TimeoutError,
        ) as exc:
            logger.warning(
                "execution_repo.insert_failed",
                extra={
                    "event":      "execution_repo_insert_failed",
                    "symbol":     envelope.get("symbol"),
                    "error_type": type(exc).__name__,
                    "error":      str(exc)[:200],
                },
            )
            return False
        return True

    @staticmethod
    def _envelope_to_row(env: dict[str, Any]) -> tuple[Any, ...]:
        """Validate + flatten the envelope into the 13 SQL parameters.

        Raises KeyError / ValueError on shape violations — caller logs
        + drops the message rather than inserting partial garbage.
        """
        signal = env["signal"]
        return (
            datetime.fromisoformat(str(env["ts"])),
            str(env["symbol"]),
            str(env["mode"]),
            bool(env["executed"]),
            str(env["intended_action"]),
            int(env.get("intended_quantity", 0)),
            (str(env["order_id"]) if env.get("order_id") is not None else None),
            (str(env["blocked_by"]) if env.get("blocked_by") is not None else None),
            (str(env["reason"]) if env.get("reason") is not None else None),
            str(signal["action"]),
            float(signal["confidence"]),
            float(signal["score"]),
            json.dumps(signal.get("rationale", [])),
        )
