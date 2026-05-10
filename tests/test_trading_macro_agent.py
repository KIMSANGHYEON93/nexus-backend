"""Tests for MacroAgent — stub-mode contract + Sprint 5i LLM integration.

Two layers:
  • Stub mode (no LLM client injected) — preserved verbatim from Sprint 5h.
    Always emits HOLD@0; never touches the network. Pinned by the original
    `test_evaluate_makes_no_http_call` sentinel test.
  • LLM mode (Sprint 5i) — injectable `LLMClient` is called per evaluate().
    Successful structured-JSON responses become real BUY/SELL signals.
    EVERY failure mode (timeout, 5xx, malformed JSON, hallucinated values,
    catch-all) MUST gracefully fall back to HOLD@0 — a third-party outage
    cannot crash the trading pipeline. The Sprint 5i test block at the
    bottom of this file is the contract for that fallback.
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
from src.domain.trading.llm_client import LLMClient, LLMClientError
from src.domain.trading.macro_agent import (
    MacroAgent,
    MockNewsProvider,
    NewsProvider,
    _LLM_STUB_RATIONALE,
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


async def test_evaluate_returns_hold_with_zero_confidence_in_stub_mode():
    """No matter what news/prices say, the agent MUST return HOLD@0 when
    no LLM client is configured. This is the post-Sprint-5i stub-mode
    contract — operators with `llm_provider=none` get exactly the
    Sprint 5h behavior, no surprises."""
    ctx = TickContext()
    _seed(ctx, "005930", [100.0, 105.0, 110.0])  # strong uptrend — irrelevant
    agent = MacroAgent(
        context=ctx,
        news_provider=MockNewsProvider({"005930": ["Wildly bullish news"]}),
        # llm_client=None implicitly (stub mode)
    )
    assert agent.has_llm is False
    sig = await agent.evaluate("005930")
    assert sig.action is Action.HOLD
    assert sig.confidence == 0.0
    assert _LLM_STUB_RATIONALE in sig.rationale


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


# ════════════════════════════════════════════════════════════════════════
#               Sprint 5i — LLM mode + failure fallbacks
# ════════════════════════════════════════════════════════════════════════
#
# THE HEADLINE PROMISE: every conceivable failure of the external LLM
# (timeout, 5xx, rate-limit, hallucinated JSON, missing fields, invalid
# action label, non-numeric confidence, totally unexpected exception)
# MUST resolve to HOLD@0. The pipeline never sees an exception.


class _FakeLLMClient:
    """Configurable LLMClient stand-in. Either returns a string OR raises."""

    provider = "fake"
    model    = "fake-model"

    def __init__(
        self,
        *,
        text:  str | None       = None,
        error: BaseException | None = None,
    ) -> None:
        self._text  = text
        self._error = error
        self.calls: list[str] = []

    async def complete(self, prompt: str) -> str:
        self.calls.append(prompt)
        if self._error is not None:
            raise self._error
        if self._text is None:
            raise RuntimeError("test misconfigured: neither text nor error set")
        return self._text


def _build_agent_with_llm(llm: _FakeLLMClient) -> MacroAgent:
    ctx = TickContext()
    _seed(ctx, "005930", [100.0, 101.0, 102.0])
    return MacroAgent(
        context=ctx,
        news_provider=MockNewsProvider({"005930": ["Mixed signals overnight"]}),
        llm_client=llm,
    )


def test_agent_reports_has_llm_when_client_injected():
    ctx = TickContext()
    agent = MacroAgent(
        context=ctx, news_provider=MockNewsProvider(),
        llm_client=_FakeLLMClient(text='{"action":"hold","confidence":0.0}'),
    )
    assert agent.has_llm is True


# ── Happy paths ────────────────────────────────────────────────────────


async def test_llm_response_buy_parses_to_buy_signal():
    llm = _FakeLLMClient(text='{"action":"buy","confidence":0.82,"rationale":"earnings beat + memory pricing firming"}')
    agent = _build_agent_with_llm(llm)
    sig = await agent.evaluate("005930")
    assert sig.action is Action.BUY
    assert sig.confidence == pytest.approx(0.82)
    assert "earnings beat" in sig.rationale
    assert len(llm.calls) == 1


async def test_llm_response_sell_parses_to_sell_signal():
    llm = _FakeLLMClient(text='{"action":"sell","confidence":0.71,"rationale":"china export ban headline"}')
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.SELL
    assert sig.confidence == pytest.approx(0.71)


async def test_llm_response_hold_parses_to_hold_with_reported_confidence():
    """A real HOLD@0.5 from the LLM is distinct from a fallback HOLD@0."""
    llm = _FakeLLMClient(text='{"action":"hold","confidence":0.5,"rationale":"signals balanced"}')
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.HOLD
    assert sig.confidence == 0.5
    assert "balanced" in sig.rationale


async def test_llm_response_tolerates_markdown_fenced_json():
    """Some models wrap JSON in ```json fences. The parser must cope."""
    llm = _FakeLLMClient(text='```json\n{"action":"buy","confidence":0.6,"rationale":"fence test"}\n```')
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.BUY
    assert sig.confidence == 0.6


async def test_llm_response_tolerates_leading_prose():
    """Some models prepend a sentence before the JSON. The parser scans for {}."""
    llm = _FakeLLMClient(text='Here is my analysis. {"action":"sell","confidence":0.4,"rationale":"prose-prefixed"}')
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.SELL


# ── Failure modes — every one MUST resolve to HOLD@0 ──────────────────


async def test_llm_timeout_falls_back_to_hold_zero(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="src.domain.trading.macro_agent")
    llm = _FakeLLMClient(error=LLMClientError("openai timeout after 8.0s"))
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.HOLD
    assert sig.confidence == 0.0
    assert "LLM call failed" in sig.rationale
    fail_logs = [r for r in caplog.records if r.message == "macro.agent.llm_call_failed"]
    assert fail_logs, "fallback must emit a structured log for alerting"


async def test_llm_http_5xx_falls_back_to_hold_zero():
    llm = _FakeLLMClient(error=LLMClientError("openai HTTP 502: bad gateway"))
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.HOLD
    assert sig.confidence == 0.0


async def test_llm_rate_limit_falls_back_to_hold_zero():
    llm = _FakeLLMClient(error=LLMClientError("anthropic HTTP 429: rate_limit_error"))
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.HOLD


async def test_llm_unexpected_exception_falls_back_to_hold_zero():
    """Even a surprise exception class (e.g. SDK upgrade) must not crash."""
    llm = _FakeLLMClient(error=RuntimeError("some new SDK error class"))
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.HOLD


async def test_llm_returns_invalid_json_falls_back_to_hold_zero(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="src.domain.trading.macro_agent")
    llm = _FakeLLMClient(text="not even close to JSON {{{ broken ")
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.HOLD
    assert sig.confidence == 0.0
    assert "unparseable" in sig.rationale
    parse_logs = [r for r in caplog.records if r.message == "macro.agent.parse_failed"]
    assert parse_logs


async def test_llm_returns_empty_string_falls_back_to_hold_zero():
    llm = _FakeLLMClient(text="")
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.HOLD


async def test_llm_returns_json_array_not_object_falls_back_to_hold_zero():
    llm = _FakeLLMClient(text='[{"action":"buy","confidence":0.8}]')
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.HOLD


async def test_llm_response_missing_action_field_falls_back_to_hold_zero():
    llm = _FakeLLMClient(text='{"confidence":0.7,"rationale":"no action"}')
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.HOLD


async def test_llm_response_missing_confidence_field_falls_back_to_hold_zero():
    llm = _FakeLLMClient(text='{"action":"buy","rationale":"no confidence"}')
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.HOLD


async def test_llm_response_invalid_action_label_falls_back_to_hold_zero():
    """Hallucinated action like 'HODL' or 'STRONG_BUY' → HOLD@0."""
    llm = _FakeLLMClient(text='{"action":"HODL","confidence":0.9,"rationale":"hallucinated label"}')
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.HOLD
    assert sig.confidence == 0.0


async def test_llm_response_non_numeric_confidence_falls_back_to_hold_zero():
    llm = _FakeLLMClient(text='{"action":"buy","confidence":"high","rationale":"strings not numbers"}')
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.HOLD


async def test_llm_response_confidence_above_one_clamped_not_failed():
    """confidence=1.5 is clearly intent-to-be-strong — clamp to 1.0 rather
    than reject, since the action+rationale are still valid signal."""
    llm = _FakeLLMClient(text='{"action":"buy","confidence":1.5,"rationale":"overshot conf"}')
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.BUY
    assert sig.confidence == 1.0


async def test_llm_response_negative_confidence_clamped_to_zero():
    llm = _FakeLLMClient(text='{"action":"sell","confidence":-0.3,"rationale":"negative conf"}')
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.SELL
    assert sig.confidence == 0.0


async def test_llm_response_oversized_rationale_truncated():
    """A model emitting an essay must not blow up our log lines."""
    long_rationale = "A" * 2000
    llm = _FakeLLMClient(text='{"action":"buy","confidence":0.5,"rationale":"' + long_rationale + '"}')
    sig = await _build_agent_with_llm(llm).evaluate("005930")
    assert sig.action is Action.BUY
    assert len(sig.rationale) <= 510  # 500 cap + ellipsis allowance
    assert sig.rationale.endswith("…")


# ── Protocol satisfaction (sentinel) ──────────────────────────────────


def test_fake_llm_client_satisfies_protocol():
    """If this fails, our test fakes have drifted from the LLMClient contract."""
    assert isinstance(_FakeLLMClient(text="x"), LLMClient)
