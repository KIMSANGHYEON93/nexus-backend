"""TradingPipeline — connects tick stream to the full trading chain.

Per Sprint 5h spec, the data flow is:
    tick arrives → context updated → coordinator evaluates →
    guardrails check → executor acts → portfolio updated on fill

Sprint 5m extension: after every `executor.execute()` invocation, an
audit envelope (signal + result) is published to a Redis channel for
the off-hot-path PersistenceWorker to consume and persist. The publish
is a single async send — microseconds — so the hot path stays fast
while the database work happens entirely in a sibling task.

Hot-path discipline (SPRINT 5m CRITICAL RULE):
    DB writes MUST NOT block the trading loop. Pipeline only publishes
    to Redis (cheap). Any DB pain stays in PersistenceWorker — its
    repos return False/0 on error rather than raising, so even a long
    Postgres outage cannot back-pressure the pipeline.

This class imports only from `domain/trading/` for trading logic; the
audit publish takes an optional `audit_publisher: AuditPublisher` whose
implementation lives in infrastructure. Pre-5m callers can omit it
(constructor default is None) to keep the older test signatures working.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from ..market.models import Tick
from .context import TickContext
from .coordinator import TradingCoordinator
from .executor import ExecutionResult, OrderExecutor
from .guardrails import GuardContext, GuardedSignal, GuardrailPipeline
from .models import Action, TradeSignal
from .portfolio import Portfolio

logger = logging.getLogger(__name__)


# Sprint 5m: outbound port for shipping the audit envelope. The pipeline
# fires this after every executor decision; the implementation (in
# `infrastructure.audit_publisher`) is a one-line redis.publish. Async
# because Redis publish is async, but it returns in microseconds — the
# hot path is not meaningfully extended.
AuditPublisher = Callable[[dict[str, object]], Awaitable[None]]


class TradingPipeline:
    """Single tick → single trading-decision invocation.

    Holds references to all five collaborators (context, coordinator,
    guardrails, executor, portfolio). Doesn't own any background tasks
    of its own — the publisher's tick loop is the driver.
    """

    def __init__(
        self,
        *,
        context:           TickContext,
        coordinator:       TradingCoordinator,
        guardrails:        GuardrailPipeline,
        executor:          OrderExecutor,
        portfolio:         Portfolio,
        volatility_window: int                    = 10,
        audit_publisher:   AuditPublisher | None  = None,
    ) -> None:
        self._context           = context
        self._coordinator       = coordinator
        self._guardrails        = guardrails
        self._executor          = executor
        self._portfolio         = portfolio
        self._volatility_window = volatility_window
        self._audit_publisher   = audit_publisher
        self._tick_count:        int = 0
        self._executed_count:    int = 0
        self._error_count:       int = 0
        self._audit_failure_count: int = 0

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def executed_count(self) -> int:
        return self._executed_count

    @property
    def error_count(self) -> int:
        return self._error_count

    @property
    def audit_failure_count(self) -> int:
        return self._audit_failure_count

    async def on_tick(self, tick: Tick) -> None:
        """Suitable as a publisher.on_tick callback. Catches all
        exceptions internally so the publisher loop is never broken
        by a bug in agents/guards/executor."""
        self._tick_count += 1
        try:
            await self._process(tick)
        except Exception:
            self._error_count += 1
            logger.exception(
                "trading.pipeline.on_tick_failed",
                extra={
                    "event":  "trading_pipeline_on_tick_failed",
                    "symbol": tick.symbol,
                },
            )

    async def _process(self, tick: Tick) -> None:
        # 1. Context update — every tick, every symbol.
        self._context.record(tick)

        # 2. Build a guard context snapshot from the portfolio +
        #    short-window prices. Volatility window is bounded so a
        #    long quiet streak doesn't dilute the breaker's reaction.
        guard_ctx = GuardContext(
            positions     = self._portfolio.positions,
            last_trade_at = self._portfolio.last_trade_at,
            recent_prices = {
                tick.symbol: self._context.recent_prices(
                    tick.symbol, n=self._volatility_window,
                ),
            },
            now           = datetime.now(timezone.utc),
        )

        # 3. Coordinator → 4. Guardrails → 5. Executor.
        signal  = await self._coordinator.evaluate(tick.symbol)
        guarded = await self._guardrails.evaluate(signal, guard_ctx)
        result  = await self._executor.execute(guarded)

        # 6. Portfolio update — only on confirmed fill (mode=live + executed).
        #    Shadow-trade fills do NOT update the portfolio because no
        #    real position changed hands; updating would corrupt the
        #    guard context for subsequent signals.
        if result.executed and result.intended_action is not Action.HOLD:
            self._portfolio.record_fill(
                symbol   = result.intended_symbol,
                action   = result.intended_action,
                quantity = result.intended_quantity,
                ts       = result.ts,
            )
            self._executed_count += 1

        # 7. Audit publish (Sprint 5m) — async one-shot to Redis. Wrapped
        # so a Redis hiccup never propagates back into the hot path. The
        # PersistenceWorker on the other end will write to the
        # execution_audit hypertable; failures THERE don't reach us either.
        if self._audit_publisher is not None:
            envelope = _build_audit_envelope(signal, guarded, result)
            try:
                await self._audit_publisher(envelope)
            except Exception:  # noqa: BLE001 — defensive: audit must NEVER break the loop
                self._audit_failure_count += 1
                logger.warning(
                    "trading.pipeline.audit_publish_failed",
                    extra={
                        "event":  "trading_pipeline_audit_publish_failed",
                        "symbol": tick.symbol,
                    },
                )


# ── Audit envelope construction ────────────────────────────────────────


def _build_audit_envelope(
    signal:  TradeSignal,
    guarded: GuardedSignal,
    result:  ExecutionResult,
) -> dict[str, object]:
    """Pack the per-decision audit row into the wire envelope consumed
    by the PersistenceWorker → ExecutionRepository chain.

    Schema is the source of truth for `db/migrations/002_execution_audit.sql`
    columns. Adding a field here without bumping the migration means the
    persistence layer silently drops it; bump both together.
    """
    return {
        "ts":               result.ts.isoformat(),
        "symbol":           result.intended_symbol,
        "mode":             result.mode,
        "executed":         result.executed,
        "intended_action":  result.intended_action.value,
        "intended_quantity": result.intended_quantity,
        "order_id":         result.order_id,
        "blocked_by":       guarded.blocked_by,
        "reason":           result.reason,
        "signal": {
            "action":     signal.action.value,
            "confidence": signal.confidence,
            "score":      signal.score,
            "rationale":  [
                {
                    "agent_id":   c.agent_id,
                    "action":     c.action.value,
                    "confidence": c.confidence,
                }
                for c in signal.contributors
            ],
        },
    }
