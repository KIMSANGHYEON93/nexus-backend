"""Sprint 5m persistence layer tests — repos + worker + pipeline audit.

Three layers:
  1. TickRepository — mocked asyncpg pool, prove batch insert calls the
     right SQL with the right rows + survives DB errors gracefully.
  2. ExecutionRepository — same shape, plus envelope validation.
  3. PersistenceWorker — buffer/flush/threshold/interval semantics +
     malformed-message tolerance + DB-outage drop counting.
  4. TradingPipeline audit envelope — proves the publisher is called
     with the right shape, and that audit-publisher failure NEVER
     propagates back into the hot path.

Pure unit tests (no real Postgres). Sprint 5m ships an integration
test in tests/integration/ that exercises the same path against the
docker-compose Timescale instance.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from src.domain.market.models import Tick, TickSide
from src.domain.trading.executor import OrderClient, OrderExecutor, OrderResult
from src.domain.trading.guardrails import GuardrailPipeline
from src.domain.trading.models import Action, AgentSignal, TradeSignal
from src.domain.trading.pipeline import TradingPipeline, _build_audit_envelope
from src.domain.trading.portfolio import Portfolio
from src.domain.trading.context import TickContext
from src.domain.trading.coordinator import TradingCoordinator
from src.domain.trading.agents import MockMacroAgent, MockQuantAgent
from src.infrastructure.execution_repository import ExecutionRepository
from src.infrastructure.persistence_worker import PersistenceWorker, _dict_to_tick
from src.infrastructure.tick_repository import TickRepository


# ── Shared fixtures ───────────────────────────────────────────────────


def _tick(symbol: str = "005930", price: str = "79000", offset_s: int = 0) -> Tick:
    return Tick(
        symbol=symbol,
        ts=datetime(2026, 5, 10, 4, 0, offset_s, tzinfo=timezone.utc),
        price=Decimal(price),
        volume=100,
        side=TickSide.BUY,
    )


class _MockPool:
    """asyncpg.Pool stand-in. `acquire()` returns an async context manager
    yielding `MockConnection`. Records every executemany / execute call
    + can be configured to raise on demand."""

    def __init__(self, *, raise_on: type[BaseException] | None = None) -> None:
        self.executemany_calls: list[tuple[str, list[Any]]] = []
        self.execute_calls:     list[tuple[str, tuple[Any, ...]]] = []
        self._raise_on = raise_on

    def acquire(self):  # noqa: ANN201
        outer = self

        class _Ctx:
            async def __aenter__(self_):  # noqa: ANN001
                return outer
            async def __aexit__(self_, *exc):  # noqa: ANN001, ANN002
                return False

        return _Ctx()

    async def executemany(self, sql: str, rows: list[Any]) -> None:
        if self._raise_on is not None:
            raise self._raise_on("simulated DB error")
        self.executemany_calls.append((sql, list(rows)))

    async def execute(self, sql: str, *args: Any) -> None:
        if self._raise_on is not None:
            raise self._raise_on("simulated DB error")
        self.execute_calls.append((sql, args))


# ════════════════════════════════════════════════════════════════════════
#                              TickRepository
# ════════════════════════════════════════════════════════════════════════


async def test_tick_repo_empty_batch_is_noop():
    pool = _MockPool()
    repo = TickRepository(pool)
    assert await repo.insert_batch([]) == 0
    assert pool.executemany_calls == []


async def test_tick_repo_batch_insert_passes_correct_rows():
    pool = _MockPool()
    repo = TickRepository(pool)
    ticks = [_tick("005930", "79000"), _tick("000660", "197000", offset_s=1)]
    inserted = await repo.insert_batch(ticks)
    assert inserted == 2
    assert len(pool.executemany_calls) == 1
    sql, rows = pool.executemany_calls[0]
    assert "INSERT INTO market_tick" in sql
    assert "ON CONFLICT (symbol, ts) DO NOTHING" in sql
    # Each row must be (ts, symbol, price, volume, side) tuple
    assert len(rows) == 2
    assert rows[0] == (
        ticks[0].ts, "005930", Decimal("79000"), 100, "buy",
    )


async def test_tick_repo_db_error_returns_zero_not_raise():
    pool = _MockPool(raise_on=asyncpg.InterfaceError)
    repo = TickRepository(pool)
    out = await repo.insert_batch([_tick(), _tick()])
    assert out == 0
    # Pipeline must keep running — no exception propagates.


async def test_tick_repo_oserror_returns_zero():
    """Connection drop surfaces as OSError on some asyncpg versions."""
    pool = _MockPool(raise_on=OSError)
    repo = TickRepository(pool)
    assert await repo.insert_batch([_tick()]) == 0


async def test_tick_repo_timeout_returns_zero():
    pool = _MockPool(raise_on=TimeoutError)
    repo = TickRepository(pool)
    assert await repo.insert_batch([_tick()]) == 0


# ════════════════════════════════════════════════════════════════════════
#                          ExecutionRepository
# ════════════════════════════════════════════════════════════════════════


def _envelope(**overrides: Any) -> dict[str, Any]:
    base = {
        "ts":               "2026-05-10T04:00:00+00:00",
        "symbol":           "005930",
        "mode":             "shadow",
        "executed":         False,
        "intended_action":  "buy",
        "intended_quantity": 7,
        "order_id":         None,
        "blocked_by":       None,
        "reason":           "ALLOW_LIVE_ORDERS=false",
        "signal": {
            "action":     "buy",
            "confidence": 0.65,
            "score":      0.65,
            "rationale":  [{"agent_id": "quant.rsi", "action": "buy", "confidence": 0.7}],
        },
    }
    base.update(overrides)
    return base


async def test_execution_repo_inserts_complete_envelope():
    pool = _MockPool()
    repo = ExecutionRepository(pool)
    assert await repo.insert(_envelope()) is True
    assert len(pool.execute_calls) == 1
    sql, args = pool.execute_calls[0]
    assert "INSERT INTO execution_audit" in sql
    # Args order matches the SQL — 13 positional params
    assert len(args) == 13
    assert args[1] == "005930"          # symbol
    assert args[2] == "shadow"          # mode
    assert args[3] is False             # executed
    assert args[4] == "buy"             # intended_action
    assert args[5] == 7                 # intended_quantity
    # signal_rationale is JSON-encoded
    parsed_rationale = json.loads(args[12])
    assert parsed_rationale[0]["agent_id"] == "quant.rsi"


async def test_execution_repo_blocked_envelope_carries_blocked_by():
    pool = _MockPool()
    repo = ExecutionRepository(pool)
    env = _envelope(mode="noop", blocked_by="cooldown", reason="60s remaining")
    await repo.insert(env)
    args = pool.execute_calls[0][1]
    assert args[7] == "cooldown"
    assert args[8] == "60s remaining"


async def test_execution_repo_db_error_returns_false_not_raise():
    pool = _MockPool(raise_on=asyncpg.InterfaceError)
    repo = ExecutionRepository(pool)
    assert await repo.insert(_envelope()) is False


async def test_execution_repo_malformed_envelope_returns_false():
    """Missing required field → log + drop, no insert."""
    pool = _MockPool()
    repo = ExecutionRepository(pool)
    assert await repo.insert({"missing": "everything"}) is False
    assert pool.execute_calls == []


async def test_execution_repo_envelope_with_fill_carries_order_id():
    pool = _MockPool()
    repo = ExecutionRepository(pool)
    env = _envelope(mode="live", executed=True, order_id="ODR-99")
    await repo.insert(env)
    args = pool.execute_calls[0][1]
    assert args[6] == "ODR-99"
    assert args[3] is True


# ════════════════════════════════════════════════════════════════════════
#                             PersistenceWorker
# ════════════════════════════════════════════════════════════════════════


def test_dict_to_tick_roundtrip():
    """Mirror inverse of `kis_publisher._tick_to_wire`."""
    wire = {
        "symbol": "005930", "ts": "2026-05-10T04:00:00+00:00",
        "price": 79100.5, "volume": 150, "side": "sell",
    }
    t = _dict_to_tick(wire)
    assert t.symbol == "005930"
    assert t.price == Decimal("79100.5")
    assert t.volume == 150
    assert t.side is TickSide.SELL


def test_dict_to_tick_unknown_side_defaults_to_buy():
    """Defensive — malformed mock tick shouldn't crash the worker loop."""
    wire = {
        "symbol": "x", "ts": "2026-05-10T04:00:00+00:00",
        "price": 1, "volume": 1, "side": "garbage",
    }
    assert _dict_to_tick(wire).side is TickSide.BUY


# ── Worker construction ────────────────────────────────────────────────


def test_worker_rejects_invalid_construction_args():
    redis_client = MagicMock()
    tr = TickRepository(MagicMock())
    er = ExecutionRepository(MagicMock())
    with pytest.raises(ValueError):
        PersistenceWorker(redis_client, tr, er, flush_threshold=0)
    with pytest.raises(ValueError):
        PersistenceWorker(redis_client, tr, er, flush_interval_s=0.0)
    with pytest.raises(ValueError):
        PersistenceWorker(
            redis_client, tr, er, flush_threshold=100, buffer_max=50,
        )


# ── Tick enqueue + flush ─────────────────────────────────────────────


async def test_worker_enqueue_buffers_ticks_until_flush():
    """Direct test of the buffer: feed via _enqueue_tick, flush manually."""
    pool = _MockPool()
    worker = PersistenceWorker(
        MagicMock(), TickRepository(pool), ExecutionRepository(pool),
    )
    # Buffer 3 ticks
    for i in range(3):
        worker._enqueue_tick(json.dumps({   # noqa: SLF001
            "symbol": "005930",
            "ts":     f"2026-05-10T04:00:0{i}+00:00",
            "price":  79000 + i, "volume": 100, "side": "buy",
        }))
    assert worker.buffer_size == 3
    assert pool.executemany_calls == []  # nothing flushed yet
    await worker._flush_ticks()  # noqa: SLF001
    assert worker.buffer_size == 0
    assert len(pool.executemany_calls) == 1
    assert worker.ticks_inserted == 3


async def test_worker_malformed_tick_message_logged_and_skipped():
    pool = _MockPool()
    worker = PersistenceWorker(
        MagicMock(), TickRepository(pool), ExecutionRepository(pool),
    )
    worker._enqueue_tick("not even json")  # noqa: SLF001
    worker._enqueue_tick(json.dumps({"missing": "fields"}))  # noqa: SLF001
    assert worker.buffer_size == 0


async def test_worker_buffer_cap_drops_oldest():
    """When buffer is full, oldest tick is evicted; counter increments."""
    pool = _MockPool()
    worker = PersistenceWorker(
        MagicMock(), TickRepository(pool), ExecutionRepository(pool),
        flush_threshold=10, buffer_max=10,
    )
    for i in range(15):
        worker._enqueue_tick(json.dumps({   # noqa: SLF001
            "symbol": "x",
            "ts":     f"2026-05-10T04:00:{i:02d}+00:00",
            "price":  100, "volume": 1, "side": "buy",
        }))
    assert worker.buffer_size == 10           # capped
    assert worker.ticks_dropped == 5          # 5 evictions counted


async def test_worker_db_outage_during_flush_increments_drops():
    pool = _MockPool(raise_on=asyncpg.InterfaceError)
    worker = PersistenceWorker(
        MagicMock(), TickRepository(pool), ExecutionRepository(pool),
    )
    for i in range(5):
        worker._enqueue_tick(json.dumps({   # noqa: SLF001
            "symbol": "x",
            "ts":     f"2026-05-10T04:00:0{i}+00:00",
            "price":  100, "volume": 1, "side": "buy",
        }))
    await worker._flush_ticks()  # noqa: SLF001
    assert worker.ticks_inserted == 0
    assert worker.ticks_dropped == 5    # batch lost to DB error


async def test_worker_audit_handler_inserts_on_valid_envelope():
    pool = _MockPool()
    worker = PersistenceWorker(
        MagicMock(), TickRepository(pool), ExecutionRepository(pool),
    )
    await worker._handle_audit(json.dumps(_envelope()))  # noqa: SLF001
    assert worker.executions_inserted == 1
    assert worker.executions_failed == 0
    assert len(pool.execute_calls) == 1


async def test_worker_audit_handler_db_outage_counts_failure_no_crash():
    pool = _MockPool(raise_on=asyncpg.InterfaceError)
    worker = PersistenceWorker(
        MagicMock(), TickRepository(pool), ExecutionRepository(pool),
    )
    await worker._handle_audit(json.dumps(_envelope()))  # noqa: SLF001
    assert worker.executions_failed == 1
    assert worker.executions_inserted == 0


async def test_worker_audit_handler_malformed_json_counts_failure():
    pool = _MockPool()
    worker = PersistenceWorker(
        MagicMock(), TickRepository(pool), ExecutionRepository(pool),
    )
    await worker._handle_audit("totally not json")  # noqa: SLF001
    assert worker.executions_failed == 1


# ════════════════════════════════════════════════════════════════════════
#                  TradingPipeline audit publish (Sprint 5m)
# ════════════════════════════════════════════════════════════════════════


def _make_signal(action: Action = Action.BUY, score: float = 0.7) -> TradeSignal:
    return TradeSignal(
        symbol="005930", action=action, confidence=abs(score), score=score,
        contributors=[
            AgentSignal(
                agent_id="quant.rsi", symbol="005930", action=action,
                confidence=abs(score), ts=datetime.now(timezone.utc),
            ),
        ],
        ts=datetime.now(timezone.utc),
    )


def test_audit_envelope_shape_carries_signal_and_result():
    """Pin the envelope schema so PersistenceWorker / SQL columns stay in sync."""
    from src.domain.trading.executor import ExecutionResult
    from src.domain.trading.guardrails import GuardedSignal

    sig = _make_signal()
    guarded = GuardedSignal(signal=sig, allowed=True)
    result = ExecutionResult(
        mode="shadow", executed=False,
        intended_action=Action.BUY, intended_symbol="005930",
        intended_quantity=7, order_id=None, reason="ALLOW_LIVE_ORDERS=false",
        ts=datetime(2026, 5, 10, 4, 0, 0, tzinfo=timezone.utc),
    )
    env = _build_audit_envelope(sig, guarded, result)

    # All 13 audit columns represented in the envelope.
    assert env["ts"] == "2026-05-10T04:00:00+00:00"
    assert env["symbol"] == "005930"
    assert env["mode"] == "shadow"
    assert env["executed"] is False
    assert env["intended_action"] == "buy"
    assert env["intended_quantity"] == 7
    assert env["blocked_by"] is None
    assert env["reason"] == "ALLOW_LIVE_ORDERS=false"
    signal = env["signal"]
    assert isinstance(signal, dict)
    assert signal["action"] == "buy"
    assert signal["confidence"] == pytest.approx(0.7)
    assert signal["score"] == pytest.approx(0.7)
    rationale = signal["rationale"]
    assert isinstance(rationale, list) and rationale[0]["agent_id"] == "quant.rsi"


async def test_pipeline_publishes_audit_after_executor_runs():
    captured: list[dict[str, Any]] = []

    async def _capture(env: dict[str, Any]) -> None:
        captured.append(env)

    class _OkClient:
        async def place_order(self, *, symbol, action, quantity):  # noqa: ANN001, ANN201
            return OrderResult(success=True, order_id="ODR-1")

    ctx = TickContext()
    coord = TradingCoordinator()
    coord.register(MockQuantAgent(script=[(Action.BUY, 0.9)]))
    coord.register(MockMacroAgent(script=[(Action.BUY, 0.7)]))
    pipeline = TradingPipeline(
        context=ctx, coordinator=coord,
        guardrails=GuardrailPipeline(),
        executor=OrderExecutor(
            order_client=_OkClient(), allow_live_orders=True, default_quantity=2,
        ),
        portfolio=Portfolio(),
        audit_publisher=_capture,
    )

    await pipeline.on_tick(_tick("005930"))
    assert len(captured) == 1
    env = captured[0]
    assert env["symbol"] == "005930"
    assert env["mode"] == "live"
    assert env["executed"] is True


async def test_pipeline_audit_publisher_failure_does_not_break_hot_path():
    """Audit publish raising MUST NOT prevent the executor from completing
    or the next tick from being processed. This is the Sprint 5m
    bulletproofing of the hot-path-no-DB-blocks guarantee."""
    async def _explode(env):  # noqa: ANN001, ANN202
        raise RuntimeError("redis pub/sub down")

    ctx = TickContext()
    coord = TradingCoordinator()
    coord.register(MockQuantAgent(script=[(Action.HOLD, 0.0)]))
    pipeline = TradingPipeline(
        context=ctx, coordinator=coord, guardrails=GuardrailPipeline(),
        executor=OrderExecutor(
            order_client=_OkRecorder(), allow_live_orders=False, default_quantity=1,
        ),
        portfolio=Portfolio(),
        audit_publisher=_explode,
    )
    # Three ticks despite the publisher exploding each time.
    await pipeline.on_tick(_tick("005930", offset_s=0))
    await pipeline.on_tick(_tick("005930", offset_s=1))
    await pipeline.on_tick(_tick("005930", offset_s=2))
    assert pipeline.tick_count == 3
    assert pipeline.error_count == 0   # outer try/except is for pipeline body, not audit
    assert pipeline.audit_failure_count == 3


class _OkRecorder:
    async def place_order(self, *, symbol, action, quantity):  # noqa: ANN001, ANN201
        return OrderResult(success=True, order_id="x")


async def test_pipeline_no_audit_publisher_keeps_pre_5m_behavior():
    """Backward compat: omitting audit_publisher behaves like Sprint 5h."""
    ctx = TickContext()
    coord = TradingCoordinator()
    coord.register(MockQuantAgent(script=[(Action.HOLD, 0.0)]))
    pipeline = TradingPipeline(
        context=ctx, coordinator=coord, guardrails=GuardrailPipeline(),
        executor=OrderExecutor(
            order_client=_OkRecorder(), allow_live_orders=False, default_quantity=1,
        ),
        portfolio=Portfolio(),
        # audit_publisher omitted
    )
    await pipeline.on_tick(_tick("005930"))
    assert pipeline.audit_failure_count == 0
