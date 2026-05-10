"""Tests for MacroAgent — prompt shape + safe HOLD default.

CRITICAL — until Open Q1 (LLM provider) lands, this agent must NEVER
make an HTTP call. The test `test_evaluate_makes_no_http_call` patches
`httpx.AsyncClient.post` to fail loudly if invoked.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import httpx
import pytest

from src.domain.market.models import Tick, TickSide
from src.domain.trading.agents import Agent
from src.domain.trading.context import TickContext
from src.domain.trading.macro_agent import (
    MacroAgent,
    MockNewsProvider,
    NewsProvider,
    _LLM_PENDING_RATIONALE,
)
from src.domain.trading.models import Action


def _seed(ctx: TickContext, symbol: str, prices: list[float]) -> None:
    for p in prices:
        ctx.record(Tick(
            symbol=symbol,
            ts=datetime(2026, 5, 10, 4, 0, 0, tzinfo=timezone.utc),
            price=Decimal(str(p)),
            volume=100,
            side=TickSide.BUY,
        ))


# ── Mock news provider ─────────────────────────────────────────────────


async def test_mock_news_provider_returns_configured_headlines():
    p = MockNewsProvider({"005930": ["Samsung beats earnings", "Memory glut concern"]})
    assert await p.recent_headlines("005930") == [
        "Samsung beats earnings", "Memory glut concern",
    ]


async def test_mock_news_provider_returns_placeholder_for_unknown_symbol():
    p = MockNewsProvider({})
    headlines = await p.recent_headlines("000660")
    assert len(headlines) >= 1
    assert "000660" in headlines[0]


def test_mock_news_provider_satisfies_protocol():
    assert isinstance(MockNewsProvider(), NewsProvider)


# ── Safe-default behavior (the most important contract) ───────────────


async def test_evaluate_returns_hold_with_zero_confidence():
    """No matter what news/prices say, the agent MUST return HOLD@0
    until Open Q1 resolves. This is the safety contract."""
    ctx = TickContext()
    _seed(ctx, "005930", [100.0, 105.0, 110.0])  # strong uptrend — irrelevant
    agent = MacroAgent(
        context=ctx,
        news_provider=MockNewsProvider({"005930": ["Wildly bullish news"]}),
    )
    sig = await agent.evaluate("005930")
    assert sig.action is Action.HOLD
    assert sig.confidence == 0.0
    assert _LLM_PENDING_RATIONALE in sig.rationale


async def test_evaluate_makes_no_http_call():
    """The headline test for Sprint 5h: prove the agent does NOT touch
    the network. If a future refactor accidentally wires the LLM call
    in too early, this test fails loudly."""
    ctx = TickContext()
    _seed(ctx, "005930", [100.0])
    agent = MacroAgent(context=ctx, news_provider=MockNewsProvider())

    async def _explode(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise RuntimeError(
            "SAFETY VIOLATION: MacroAgent made an HTTP call before Open Q1 resolved."
        )

    with patch.object(httpx.AsyncClient, "post",   _explode), \
         patch.object(httpx.AsyncClient, "get",    _explode), \
         patch.object(httpx.AsyncClient, "send",   _explode), \
         patch.object(httpx.AsyncClient, "request", _explode):
        sig = await agent.evaluate("005930")
        assert sig.action is Action.HOLD


# ── Prompt shape ──────────────────────────────────────────────────────


async def test_prompt_carries_symbol_section():
    ctx = TickContext()
    _seed(ctx, "005930", [100.0])
    agent = MacroAgent(context=ctx, news_provider=MockNewsProvider())
    prompt = await agent.build_prompt("005930")
    assert "SYMBOL: 005930" in prompt


async def test_prompt_carries_price_context_when_ticks_present():
    ctx = TickContext()
    _seed(ctx, "005930", [100.0, 105.0, 110.0])
    agent = MacroAgent(context=ctx, news_provider=MockNewsProvider())
    prompt = await agent.build_prompt("005930")
    assert "PRICE CONTEXT:" in prompt
    assert "Current price:" in prompt
    assert "KRW" in prompt
    assert "+10.00%" in prompt   # 100 → 110 = +10%


async def test_prompt_handles_empty_history_gracefully():
    """Cold-start safety — no prices yet, but the prompt must still be valid."""
    ctx = TickContext()
    agent = MacroAgent(context=ctx, news_provider=MockNewsProvider())
    prompt = await agent.build_prompt("005930")
    assert "no recent ticks" in prompt


async def test_prompt_carries_news_section():
    ctx = TickContext()
    _seed(ctx, "005930", [100.0])
    agent = MacroAgent(
        context=ctx,
        news_provider=MockNewsProvider({"005930": [
            "Beat earnings", "New AI chip announced",
        ]}),
    )
    prompt = await agent.build_prompt("005930")
    assert "RECENT NEWS:" in prompt
    assert "Beat earnings" in prompt
    assert "New AI chip announced" in prompt


async def test_prompt_carries_response_format_instruction():
    """The LLM caller (Sprint 5i) parses JSON; this contract MUST be in the prompt."""
    ctx = TickContext()
    _seed(ctx, "005930", [100.0])
    agent = MacroAgent(context=ctx, news_provider=MockNewsProvider())
    prompt = await agent.build_prompt("005930")
    assert "RESPONSE FORMAT" in prompt
    assert '"action"' in prompt
    assert '"confidence"' in prompt
    assert '"rationale"' in prompt
    assert '"buy" | "hold" | "sell"' in prompt


async def test_prompt_carries_schema_version():
    """Locking the prompt schema version — Sprint 5i bumps when the LLM
    caller's parser is shipped, so we can detect mismatched fixtures."""
    ctx = TickContext()
    agent = MacroAgent(context=ctx, news_provider=MockNewsProvider())
    prompt = await agent.build_prompt("005930")
    assert "[NEXUS-MACRO-PROMPT v1]" in prompt


# ── Audit log ──────────────────────────────────────────────────────────


async def test_evaluate_logs_prompt_generated_event(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="src.domain.trading.macro_agent")
    ctx = TickContext()
    _seed(ctx, "005930", [100.0])
    agent = MacroAgent(context=ctx, news_provider=MockNewsProvider())
    await agent.evaluate("005930")

    records = [r for r in caplog.records if r.message == "macro.agent.prompt_generated"]
    assert len(records) == 1
    rec = records[0]
    assert getattr(rec, "symbol") == "005930"
    assert getattr(rec, "agent_id") == "macro.llm"
    assert getattr(rec, "prompt_chars") > 0


# ── Construction validation ────────────────────────────────────────────


def test_invalid_history_window_rejected():
    ctx = TickContext()
    with pytest.raises(ValueError):
        MacroAgent(context=ctx, news_provider=MockNewsProvider(), history_window=1)


def test_agent_satisfies_protocol():
    ctx = TickContext()
    assert isinstance(MacroAgent(context=ctx, news_provider=MockNewsProvider()), Agent)
