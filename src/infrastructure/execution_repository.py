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

# Newest-first audit lookup for the ⌘L Audit modal (Sprint 5o-C-3). Backed
# by `idx_execution_audit_symbol_ts (symbol, ts DESC)` — index-only on
# the WHERE/ORDER pair, so the LIMIT slice is a chunk-local range scan
# even with months of audit history.
_FETCH_RECENT_SQL = """
    SELECT ts, symbol, mode, executed,
           intended_action, intended_quantity,
           order_id, blocked_by, reason,
           signal_action, signal_confidence, signal_score,
           signal_rationale
      FROM execution_audit
     WHERE symbol = $1
     ORDER BY ts DESC
     LIMIT $2
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

    async def fetch_recent(
        self,
        symbol: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Newest-first audit rows for one symbol — backs the ⌘L modal.

        Returns plain dicts (not domain objects) because the audit row
        lives only at the repository edge — there's no domain logic that
        needs a typed wrapper. `signal_rationale` is decoded from JSONB
        into a Python list[dict] here so the router doesn't have to know
        about asyncpg's text-vs-decoded ambiguity (older asyncpg builds
        hand back a JSON string, newer ones a parsed object — we
        normalize to the parsed shape).

        Read-only; never raises into the request — DB / connection errors
        return an empty list and log so the modal renders an empty state
        rather than 500-ing the operator. The route layer remains the
        place to surface "service unavailable" if we need to harden this
        later.
        """
        if limit < 1:
            return []
        try:
            rows = await self._pool.fetch(_FETCH_RECENT_SQL, symbol, limit)
        except (
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            OSError, TimeoutError,
        ) as exc:
            logger.warning(
                "execution_repo.fetch_recent_failed",
                extra={
                    "event":      "execution_repo_fetch_recent_failed",
                    "symbol":     symbol,
                    "error_type": type(exc).__name__,
                    "error":      str(exc)[:200],
                },
            )
            return []
        return [self._row_to_dict(r) for r in rows]

    @staticmethod
    def _row_to_dict(row: Any) -> dict[str, Any]:
        """Normalize one asyncpg Record into the API DTO shape.

        `signal_rationale` may arrive as a JSON string (older asyncpg) or
        an already-parsed list[dict] (newer asyncpg with codec config) —
        both paths land at the same Python shape. A malformed string
        falls back to an empty list rather than raising; an audit row
        with corrupt rationale should still be visible in the modal,
        with the contributors panel empty rather than crashing the
        whole list.
        """
        rationale_raw = row["signal_rationale"]
        if isinstance(rationale_raw, str):
            try:
                rationale = json.loads(rationale_raw)
            except (ValueError, TypeError):
                rationale = []
        elif isinstance(rationale_raw, list):
            rationale = rationale_raw
        else:
            rationale = []
        return {
            "ts":                row["ts"],
            "symbol":            row["symbol"],
            "mode":              row["mode"],
            "executed":          row["executed"],
            "intended_action":   row["intended_action"],
            "intended_quantity": row["intended_quantity"],
            "order_id":          row["order_id"],
            "blocked_by":        row["blocked_by"],
            "reason":            row["reason"],
            "signal_action":     row["signal_action"],
            "signal_confidence": row["signal_confidence"],
            "signal_score":      row["signal_score"],
            "signal_rationale":  rationale,
        }

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
