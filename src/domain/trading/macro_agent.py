"""MacroAgent — LLM prompt generator (no actual HTTP call yet).

The user's Sprint 5h brief is explicit: implement the EXACT prompt
shape now, but DO NOT make the HTTP call until Open Q1 (LLM provider
choice — Anthropic Claude vs Ollama vs OpenAI) is resolved. So this
module is a deliberate half-implementation:

    1. Build the structured prompt from symbol + price context + news.
    2. Log the prompt at INFO so dashboards (and Sprint 5i regression
       fixtures) can verify the shape stays stable.
    3. Return a HOLD signal with confidence 0 — guaranteed safe default
       that won't accidentally drive trading decisions through the
       coordinator while the brain is still offline.

When Open Q1 lands, only the body of `evaluate()` changes — the prompt
schema, NewsProvider Protocol, and tests stay the same. That's the whole
point of writing the prompt first: locking the contract so the eventual
LLM swap is a 20-line patch instead of a sprint.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

from .context import TickContext
from .models import Action, AgentSignal

logger = logging.getLogger(__name__)


# Default summary window for the price-context section. Wider than the
# RSI window because macro reasoning benefits from more ticks of trend
# context, even at the cost of slower inference latency once the LLM is
# wired in.
_DEFAULT_HISTORY_WINDOW = 30

# Stable rationale string returned in the HOLD signal. The presence of
# this exact substring in coordinator audit logs is the canonical
# marker that "we're still in pre-LLM stub mode" — Sprint 5i tests will
# assert on it disappearing after the LLM is wired in.
_LLM_PENDING_RATIONALE = (
    "LLM provider not wired (Open Q1) — defaulting to HOLD"
)


@runtime_checkable
class NewsProvider(Protocol):
    """Outbound port for headline retrieval. Async because the real
    impl will hit a news API (HTTP) and we don't want the agent body
    to assume in-process synchronous data."""

    async def recent_headlines(self, symbol: str) -> list[str]: ...


class MockNewsProvider:
    """Deterministic stub. Initialize with a per-symbol headline map; any
    symbol not in the map gets a single placeholder line so the prompt
    template never collapses to an empty section."""

    def __init__(self, headlines: dict[str, list[str]] | None = None) -> None:
        self._headlines = dict(headlines or {})

    async def recent_headlines(self, symbol: str) -> list[str]:
        if symbol in self._headlines:
            return list(self._headlines[symbol])
        return [f"[mock] No real headlines configured for {symbol}"]


class MacroAgent:
    """Builds + logs the LLM prompt; returns HOLD until Open Q1 resolves."""

    def __init__(
        self,
        *,
        context:        TickContext,
        news_provider:  NewsProvider,
        history_window: int = _DEFAULT_HISTORY_WINDOW,
        agent_id:       str = "macro.llm",
    ) -> None:
        if history_window < 2:
            raise ValueError(f"history_window must be >= 2, got {history_window}")
        self.agent_id        = agent_id
        self._context        = context
        self._news           = news_provider
        self._history_window = history_window

    async def evaluate(self, symbol: str, context: Any = None) -> AgentSignal:
        prompt = await self.build_prompt(symbol)

        # Log the prompt so dashboards can confirm the shape — this is
        # also the diff anchor when Sprint 5i flips the agent to live.
        logger.info(
            "macro.agent.prompt_generated",
            extra={
                "event":         "macro_prompt_generated",
                "symbol":        symbol,
                "agent_id":      self.agent_id,
                "prompt_chars":  len(prompt),
                "prompt_preview": prompt[:300],
            },
        )

        # CRITICAL: NO HTTP call. Return safe HOLD until Open Q1 lands.
        # Confidence 0 means the coordinator's weighted score from this
        # vote is exactly 0 — agent is wired but contributes nothing.
        return AgentSignal(
            agent_id=self.agent_id,
            symbol=symbol,
            action=Action.HOLD,
            confidence=0.0,
            rationale=_LLM_PENDING_RATIONALE,
            ts=datetime.now(timezone.utc),
        )

    async def build_prompt(self, symbol: str) -> str:
        """Public so tests can assert on the shape and so Sprint 5i can
        unit-test the LLM caller against canned prompts."""
        prices    = self._context.recent_prices(symbol, n=self._history_window)
        headlines = await self._news.recent_headlines(symbol)
        return _render_prompt(symbol, prices, headlines)


# ── Prompt template ────────────────────────────────────────────────────


def _render_prompt(symbol: str, prices: list[Decimal], headlines: list[str]) -> str:
    """Pure render function — no IO, easy to unit-test against fixtures.

    The schema below is the contract the LLM caller will be wired
    against. Changing any of these section headers or the JSON shape is
    a breaking change for Sprint 5i fixtures and the eventual prompt-eval
    harness — bump the schema version (currently v1) when that day comes.
    """
    if prices:
        floats = [float(p) for p in prices]
        current = floats[-1]
        first   = floats[0]
        pct     = ((current - first) / first * 100.0) if first > 0 else 0.0
        lo      = min(floats)
        hi      = max(floats)
        price_section = (
            f"Current price: {current:,.2f} KRW\n"
            f"Window: {len(floats)} ticks, range {lo:,.2f} → {hi:,.2f}, "
            f"net change {pct:+.2f}%"
        )
    else:
        price_section = "Current price: (no recent ticks in window)"

    headlines_section = (
        "\n".join(f"  - {h}" for h in headlines)
        if headlines else "  (no headlines)"
    )

    return f"""[NEXUS-MACRO-PROMPT v1]
You are a financial sentiment analyst for a KRX-listed equity. Your output
will be parsed by automated trading code; respond ONLY with the exact JSON
shape specified at the bottom of this prompt.

SYMBOL: {symbol}

PRICE CONTEXT:
{price_section}

RECENT NEWS:
{headlines_section}

INSTRUCTIONS:
Weigh the price action and the news together. Confidence MUST reflect your
own certainty in the call (not the strength of the signal); weak evidence
requires low confidence even if the direction feels clear. Return HOLD with
low confidence whenever evidence is mixed or thin.

RESPONSE FORMAT (strict JSON, no surrounding text):
{{"action": "buy" | "hold" | "sell", "confidence": <0.0-1.0>, "rationale": "<one sentence>"}}
"""
