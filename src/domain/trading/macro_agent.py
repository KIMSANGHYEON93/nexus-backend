"""MacroAgent — LLM-backed sentiment + fundamental signal.

Sprint 5h shipped this as a prompt-generator stub (no HTTP, always HOLD@0).
Sprint 5i wires the actual external LLM call in via an injectable
`LLMClient` Protocol while preserving the safe-stub mode for environments
without a configured provider (`llm_client=None` → behave like Sprint 5h).

Defensive contract — third-party APIs WILL fail. The promise of this
module is that ANY failure path yields a HOLD@0 signal, never an
exception that propagates into the trading pipeline:

    - LLMClient timeout / network error → HOLD@0 (logged at WARNING)
    - LLMClient HTTP non-2xx           → HOLD@0
    - Response is not parseable JSON   → HOLD@0
    - JSON missing required fields     → HOLD@0
    - JSON has invalid action/conf     → HOLD@0
    - Anything else (catch-all)        → HOLD@0

The catch-all is intentional — even an SDK upgrade that introduces a new
exception class must not crash the publisher loop. Belt-and-suspenders
because trader confidence in the system being available always trumps
"propagate the bug for diagnosis" for THIS one layer.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

from .context import TickContext
from .llm_client import LLMClient
from .models import Action, AgentSignal

logger = logging.getLogger(__name__)


# ── Defaults ────────────────────────────────────────────────────────────
_DEFAULT_HISTORY_WINDOW = 30
# Rationale length cap — defensive against LLMs emitting essays. Keeps
# audit logs grep-friendly and avoids unbounded log lines if a future
# model misbehaves.
_RATIONALE_MAX_CHARS    = 500

# Rationale used when no LLM client is configured (stub mode, identical
# to Sprint 5h behavior). Tests assert on this exact substring as the
# canonical "still in pre-LLM mode" marker.
_LLM_STUB_RATIONALE = (
    "LLM client not configured — defaulting to HOLD"
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
    """Builds the structured prompt, calls the configured LLM, parses
    the structured JSON response into an AgentSignal. Falls back to
    HOLD@0 on any failure — a third-party outage MUST NOT take down
    the trading loop."""

    def __init__(
        self,
        *,
        context:        TickContext,
        news_provider:  NewsProvider,
        llm_client:     LLMClient | None = None,
        history_window: int              = _DEFAULT_HISTORY_WINDOW,
        agent_id:       str              = "macro.llm",
    ) -> None:
        if history_window < 2:
            raise ValueError(f"history_window must be >= 2, got {history_window}")
        self.agent_id        = agent_id
        self._context        = context
        self._news           = news_provider
        self._llm            = llm_client
        self._history_window = history_window

    @property
    def has_llm(self) -> bool:
        """True iff an LLM client was injected (i.e. not in stub mode)."""
        return self._llm is not None

    async def evaluate(self, symbol: str, context: Any = None) -> AgentSignal:
        prompt = await self.build_prompt(symbol)
        logger.info(
            "macro.agent.prompt_generated",
            extra={
                "event":          "macro_prompt_generated",
                "symbol":         symbol,
                "agent_id":       self.agent_id,
                "prompt_chars":   len(prompt),
                "prompt_preview": prompt[:300],
                "llm_provider":   self._llm.provider if self._llm else "none",
            },
        )

        # Stub mode — identical to Sprint 5h, preserves the safe default
        # whenever the operator has not configured a provider.
        if self._llm is None:
            return self._hold(symbol, _LLM_STUB_RATIONALE)

        # ── Real LLM path ──────────────────────────────────────────────
        # Outermost catch-all: even if the LLM client (or downstream
        # parser) raises something we don't expect, a HOLD@0 is the
        # safe answer. Trader trust in the agent staying responsive
        # dominates "propagate the bug for diagnosis" at this seam.
        try:
            raw = await self._llm.complete(prompt)
        except Exception as exc:  # noqa: BLE001 — defensive on purpose
            logger.warning(
                "macro.agent.llm_call_failed",
                extra={
                    "event":      "macro_llm_call_failed",
                    "symbol":     symbol,
                    "provider":   self._llm.provider,
                    "model":      self._llm.model,
                    "error_type": type(exc).__name__,
                    "error":      str(exc)[:200],
                },
            )
            return self._hold(
                symbol,
                f"LLM call failed ({type(exc).__name__}): falling back to HOLD",
            )

        try:
            return self._parse_signal(symbol, raw)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "macro.agent.parse_failed",
                extra={
                    "event":      "macro_parse_failed",
                    "symbol":     symbol,
                    "provider":   self._llm.provider,
                    "raw_preview": raw[:200] if isinstance(raw, str) else "<non-str>",
                    "error_type": type(exc).__name__,
                    "error":      str(exc)[:200],
                },
            )
            return self._hold(
                symbol,
                f"LLM response unparseable ({type(exc).__name__}): falling back to HOLD",
            )

    # ── Prompt assembly ────────────────────────────────────────────────

    async def build_prompt(self, symbol: str) -> str:
        """Public so tests can assert on the shape and so Sprint 5i can
        unit-test the LLM caller against canned prompts."""
        prices    = self._context.recent_prices(symbol, n=self._history_window)
        headlines = await self._news.recent_headlines(symbol)
        return _render_prompt(symbol, prices, headlines)

    # ── Response parsing ───────────────────────────────────────────────

    def _parse_signal(self, symbol: str, raw: str) -> AgentSignal:
        """Strict parse: extract one JSON object, validate the three
        required fields, clamp confidence to [0,1], reject unknown actions.

        Tolerates light noise (model wrapping the JSON in ```json fences,
        leading/trailing whitespace) but does NOT try to repair malformed
        JSON — a model that can't honor the contract should be exposed,
        not papered over. Failures here surface to evaluate()'s outer
        try/except which converts to HOLD@0.
        """
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError("empty LLM response")

        body = self._extract_json_block(raw)
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise ValueError(f"not valid JSON: {exc.msg}") from exc

        if not isinstance(payload, dict):
            raise ValueError(f"top-level JSON must be object, got {type(payload).__name__}")

        for key in ("action", "confidence"):
            if key not in payload:
                raise ValueError(f"missing required field: {key!r}")

        action_raw = str(payload["action"]).strip().lower()
        try:
            action = Action(action_raw)
        except ValueError as exc:
            raise ValueError(
                f"invalid action {action_raw!r} (must be buy/hold/sell)"
            ) from exc

        # Confidence — accept int or float, clamp into [0, 1] hard.
        # Reject NaN / -inf / +inf explicitly (Pydantic would too but
        # the message would be less actionable in audit logs).
        conf_raw = payload["confidence"]
        if not isinstance(conf_raw, (int, float)):
            raise ValueError(
                f"confidence must be number, got {type(conf_raw).__name__}"
            )
        try:
            conf_dec = Decimal(str(conf_raw))
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"confidence not numeric: {conf_raw!r}") from exc
        if conf_dec.is_nan() or conf_dec.is_infinite():
            raise ValueError(f"confidence non-finite: {conf_raw!r}")
        confidence = max(0.0, min(1.0, float(conf_raw)))

        rationale = str(payload.get("rationale", "")).strip()
        if len(rationale) > _RATIONALE_MAX_CHARS:
            rationale = rationale[:_RATIONALE_MAX_CHARS] + "…"

        sig = AgentSignal(
            agent_id=self.agent_id, symbol=symbol, action=action,
            confidence=confidence, rationale=rationale or "(no rationale)",
            ts=datetime.now(timezone.utc),
        )
        logger.info(
            "macro.agent.llm_signal",
            extra={
                "event":      "macro_llm_signal",
                "symbol":     symbol,
                "action":     action.value,
                "confidence": confidence,
            },
        )
        return sig

    @staticmethod
    def _extract_json_block(raw: str) -> str:
        """Pull the JSON object out of `raw`, tolerating ```json fences and
        leading/trailing prose. Returns the substring `{...}` containing
        the first balanced JSON object, or raises ValueError.

        Rejects top-level arrays: `[{...}]` would otherwise have its inner
        object plucked out and accepted, but the contract is "respond with
        ONE object" — an array means the model misunderstood the schema
        and we should not silently honor the first element.
        """
        text = raw.strip()
        # Strip common ```json ... ``` and ``` ... ``` fence patterns.
        if text.startswith("```"):
            # Drop the opening fence line.
            first_newline = text.find("\n")
            if first_newline != -1:
                text = text[first_newline + 1:]
            # Drop trailing fence.
            if text.rstrip().endswith("```"):
                text = text.rstrip()[:-3]
            text = text.strip()

        # Scan for the first non-whitespace structural char.
        # If it's `[`, reject — top-level array violates the schema.
        for c in text:
            if c.isspace():
                continue
            if c == "[":
                raise ValueError("top-level JSON must be object, got array")
            break

        start = text.find("{")
        if start == -1:
            raise ValueError("no '{' in LLM response")
        # Naive balanced-brace scan (good enough — our JSON is shallow,
        # one level of object, no nested braces inside strings beyond
        # what json.loads will reject anyway).
        depth = 0
        for i in range(start, len(text)):
            c = text[i]
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return text[start:i + 1]
        raise ValueError("unbalanced braces in LLM response")

    # ── Helpers ────────────────────────────────────────────────────────

    def _hold(self, symbol: str, rationale: str) -> AgentSignal:
        return AgentSignal(
            agent_id=self.agent_id, symbol=symbol, action=Action.HOLD,
            confidence=0.0, rationale=rationale,
            ts=datetime.now(timezone.utc),
        )


# ── Prompt template ────────────────────────────────────────────────────


def _render_prompt(symbol: str, prices: list[Decimal], headlines: list[str]) -> str:
    """Pure render function — no IO, easy to unit-test against fixtures.

    The schema below is the contract the LLM caller is wired against.
    Changing any of these section headers or the JSON shape is a breaking
    change for Sprint 5i tests and the eventual prompt-eval harness —
    bump the schema version (currently v1) when that day comes.
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
