"""OrderExecutor — Sprint 5g bridge from GuardedSignal to a real fill.

Three responsibilities and nothing else:
  1. Read `settings.allow_live_orders` — the hard safety switch.
  2. Refuse to act on HOLD or guard-blocked signals.
  3. For BUY/SELL signals:
        • If switch is OFF → emit a SHADOW log, NEVER call the order client.
        • If switch is ON  → call the order client, log the result.

Why a separate class (rather than wiring straight into the publisher
loop): the executor is the LAST thing that will ever touch real money,
so it must be:
  • Trivially auditable — one place to read the safety logic from.
  • Trivially testable — `OrderClient` is a Protocol, so the critical
    "shadow mode never http-posts" test can pass a mock that fails the
    test if it's called at all.
  • Trivially swappable — a dry-run / simulator / paper-broker
    implementation conforms to the same Protocol with no domain changes.

The Protocol lives here (not in infrastructure) because the domain
layer dictates the shape of the outbound dependency. The infrastructure
KIS adapter implements it; future broker adapters will too. This is the
same dependency-direction discipline the rest of `domain/` follows
(`Agent` Protocol in `agents.py`, `OrderGuardrail` Protocol in
`guardrails.py`).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Literal, Protocol, runtime_checkable

from pydantic import BaseModel

from .guardrails import GuardedSignal
from .models import Action

logger = logging.getLogger(__name__)


# ── Outbound port ──────────────────────────────────────────────────────


class OrderResult(BaseModel):
    """What the order client returns after a place_order call.

    Keep narrow: success bool + machine-readable order_id when present +
    human-readable message + raw vendor envelope for forensic logging.
    Everything else (fills, partial fills, status updates) is a Sprint 5h+
    concern when we add an order-status reconciliation loop.
    """

    success:      bool
    order_id:     str | None             = None
    message:      str                    = ""
    raw_response: dict[str, Any] | None  = None


@runtime_checkable
class OrderClient(Protocol):
    """Outbound port — vendor-agnostic order placement.

    Implementations (KisOrderClient today; paper-broker / simulator
    tomorrow) translate (symbol, action, quantity) into vendor-specific
    REST/FIX/etc. calls and shape the response back into OrderResult.
    """

    async def place_order(
        self,
        *,
        symbol:   str,
        action:   Action,
        quantity: int,
    ) -> OrderResult: ...


# ── Executor output ────────────────────────────────────────────────────


class ExecutionResult(BaseModel):
    """Single record describing one executor invocation.

    `mode` answers "what code path ran":
        live   — http call was made to the broker (regardless of outcome)
        shadow — safety switch off; no http call; just logged the intent
        noop   — HOLD signal or guard-blocked signal; nothing to do

    `executed` answers "did money actually move (or attempt to)":
        True only for `mode=live` AND the broker accepted the order.
    """

    mode:             Literal["live", "shadow", "noop"]
    executed:         bool
    intended_action:  Action
    intended_symbol:  str
    intended_quantity: int                        = 0
    order_id:         str | None                  = None
    reason:           str | None                  = None    # block reason or error text
    ts:               datetime


# ── The executor ───────────────────────────────────────────────────────


class OrderExecutor:
    """Reads safety switch, dispatches to OrderClient (or doesn't).

    Construction takes the OrderClient + the live-orders flag + a default
    quantity. The flag is captured at construction time, NOT re-read on
    every call — that's intentional. A live deploy doesn't get to be
    "ALLOW_LIVE_ORDERS=true at lifespan boot, then someone toggles it
    off in a sidecar config file mid-flight". Restart the process if you
    want a different stance; that's safer than runtime mutability.
    """

    def __init__(
        self,
        *,
        order_client:     OrderClient,
        allow_live_orders: bool,
        default_quantity: int = 1,
    ) -> None:
        if default_quantity <= 0:
            raise ValueError(
                f"default_quantity must be > 0, got {default_quantity}"
            )
        self._order_client      = order_client
        self._allow_live_orders = bool(allow_live_orders)
        self._default_quantity  = default_quantity

    @property
    def allow_live_orders(self) -> bool:
        return self._allow_live_orders

    async def execute(self, guarded: GuardedSignal) -> ExecutionResult:
        effective = guarded.effective_signal
        symbol    = effective.symbol
        action    = effective.action
        now       = datetime.now(timezone.utc)

        # ── HOLD or guard-blocked → no-op (one log surface, audit-friendly).
        if action is Action.HOLD:
            logger.info(
                "executor.noop",
                extra={
                    "event":       "executor_noop",
                    "symbol":      symbol,
                    "reason":      "hold_or_blocked",
                    "blocked_by":  guarded.blocked_by,
                },
            )
            return ExecutionResult(
                mode="noop", executed=False,
                intended_action=action, intended_symbol=symbol,
                reason=guarded.blocked_by,
                ts=now,
            )

        quantity = self._default_quantity

        # ── SAFETY SWITCH OFF — log + return WITHOUT touching the client.
        # WARNING level (not INFO) so dashboards highlight that real
        # decisions were intentionally not executed.
        if not self._allow_live_orders:
            logger.warning(
                "executor.shadow_trade",
                extra={
                    "event":             "executor_shadow_trade",
                    "symbol":            symbol,
                    "intended_action":   action.value,
                    "intended_quantity": quantity,
                    "intended_score":    round(effective.score, 4),
                    "live_orders":       False,
                },
            )
            return ExecutionResult(
                mode="shadow", executed=False,
                intended_action=action, intended_symbol=symbol,
                intended_quantity=quantity,
                reason="ALLOW_LIVE_ORDERS=false",
                ts=now,
            )

        # ── LIVE TRADING — actually call the broker.
        logger.warning(
            "executor.live_order_attempt",
            extra={
                "event":     "executor_live_order_attempt",
                "symbol":    symbol,
                "action":    action.value,
                "quantity":  quantity,
            },
        )
        try:
            result = await self._order_client.place_order(
                symbol=symbol, action=action, quantity=quantity,
            )
        except Exception as exc:
            logger.exception(
                "executor.live_order_failed",
                extra={
                    "event":      "executor_live_order_failed",
                    "symbol":     symbol,
                    "action":     action.value,
                    "error_type": type(exc).__name__,
                },
            )
            return ExecutionResult(
                mode="live", executed=False,
                intended_action=action, intended_symbol=symbol,
                intended_quantity=quantity,
                reason=f"{type(exc).__name__}: {exc!s}",
                ts=now,
            )

        if result.success:
            logger.warning(
                "executor.live_order_filled",
                extra={
                    "event":    "executor_live_order_filled",
                    "symbol":   symbol,
                    "action":   action.value,
                    "quantity": quantity,
                    "order_id": result.order_id,
                },
            )
        else:
            logger.error(
                "executor.live_order_rejected",
                extra={
                    "event":         "executor_live_order_rejected",
                    "symbol":        symbol,
                    "action":        action.value,
                    "quantity":      quantity,
                    "broker_message": result.message,
                },
            )

        return ExecutionResult(
            mode="live", executed=result.success,
            intended_action=action, intended_symbol=symbol,
            intended_quantity=quantity,
            order_id=result.order_id,
            reason=None if result.success else result.message,
            ts=now,
        )
