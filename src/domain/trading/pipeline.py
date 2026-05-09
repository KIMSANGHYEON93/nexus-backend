"""TradingPipeline — connects tick stream to the full trading chain.

Per Sprint 5h spec, the data flow is:
    tick arrives → context updated → coordinator evaluates →
    guardrails check → executor acts → portfolio updated on fill

This class owns that orchestration and exposes ONE public method,
`on_tick(tick)`, suitable for direct use as a publisher's `on_tick`
observer hook. Errors inside the pipeline are caught and logged so they
never propagate up into the publisher loop — a malformed tick or a
flaky agent must not silence the live data stream.

Symmetric with the rest of the trading layer: this class imports only
from `domain/trading/`, never from infrastructure. Wiring the right
KisOrderClient (or paper-broker / simulator) into the executor happens
one level up in `main.py` / `PublisherSupervisor`.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from ..market.models import Tick
from .context import TickContext
from .coordinator import TradingCoordinator
from .executor import OrderExecutor
from .guardrails import GuardContext, GuardrailPipeline
from .models import Action
from .portfolio import Portfolio

logger = logging.getLogger(__name__)


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
        volatility_window: int = 10,
    ) -> None:
        self._context           = context
        self._coordinator       = coordinator
        self._guardrails        = guardrails
        self._executor          = executor
        self._portfolio         = portfolio
        self._volatility_window = volatility_window
        self._tick_count:        int = 0
        self._executed_count:    int = 0
        self._error_count:       int = 0

    @property
    def tick_count(self) -> int:
        return self._tick_count

    @property
    def executed_count(self) -> int:
        return self._executed_count

    @property
    def error_count(self) -> int:
        return self._error_count

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
