"""LLM client layer — Sprint 5i.

`LLMClient` is the outbound port the MacroAgent calls when an external
provider is configured. Two implementations ship here, both backed by our
existing `httpx` (no new official-SDK dependencies — easier to mock, no
per-provider quirks leaking into the agent):

  • OpenAIClient   — POST https://api.openai.com/v1/chat/completions
                     Uses `response_format: {"type":"json_object"}` so the
                     server-side parser refuses non-JSON output.
  • AnthropicClient — POST https://api.anthropic.com/v1/messages
                      JSON enforced via system-prompt instruction; the
                      MacroAgent's downstream parser is the safety net
                      either way (Anthropic doesn't have native JSON mode
                      that's worth the extra request shape).

The Protocol returns the LLM's raw text. Parsing/validation lives in the
agent because that's where the AgentSignal contract is enforced — the
client only owns "talk to the API and hand back a string".

Failure surface: any HTTP / parse / shape error inside this module raises
`LLMClientError`. The MacroAgent catches that (and any other exception)
and falls back to HOLD@0. We don't try to recover here — recovery is the
agent's job because only the agent knows what HOLD-with-rationale should
say.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol, runtime_checkable

import httpx

logger = logging.getLogger(__name__)


# ── Defaults ────────────────────────────────────────────────────────────
# Default-model rationale (Sprint 5n bumped Anthropic default Haiku → Sonnet):
#   The MacroAgent's job is reasoning over price action + multi-locale news to
#   produce a structured trade-direction call. Reasoning quality matters more
#   than per-call latency at this seat: Sonnet 4.7's better instruction-
#   following gives more reliable JSON-schema adherence (fewer parse failures
#   → fewer HOLD@0 fallbacks), and its richer chain-of-reasoning yields
#   stronger confidence calibration. Cost is ~5x Haiku, but tick-driven
#   evaluation is cache-buffered behind the 10-min news TTL + per-symbol
#   coordinator throttle that lands in 5o, so total spend stays bounded.
#   Operators tuning for cost can override via `LLM_MODEL=claude-haiku-...`.
# OpenAI default stays gpt-4o-mini — it's the equivalent cost/quality knee.
_DEFAULT_OPENAI_MODEL    = "gpt-4o-mini"
_DEFAULT_ANTHROPIC_MODEL = "claude-sonnet-4-7-20250828"
# 8s caps the per-tick latency the trading pipeline can absorb. Most
# completions land in 1-3s; a hung provider must surrender well before
# the next tick arrives so the publisher loop isn't dominated by one
# slow call.
_DEFAULT_TIMEOUT_SECONDS = 8.0
# Tokens needed to express our 3-key JSON envelope (action+confidence+
# rationale-one-sentence). 200 leaves headroom for verbose rationales
# while keeping the bill predictable.
_MAX_OUTPUT_TOKENS       = 200


class LLMClientError(Exception):
    """Anything that goes wrong inside the LLM client. Caught by the
    MacroAgent's outer try/except and converted to HOLD@0."""


@runtime_checkable
class LLMClient(Protocol):
    """Outbound port — async string-in, string-out.

    `complete(prompt)` MUST return the LLM's raw text response or raise
    `LLMClientError`. JSON parsing is the caller's problem.
    """

    provider: str
    model:    str

    async def complete(self, prompt: str) -> str: ...


# ── OpenAI ─────────────────────────────────────────────────────────────


class OpenAIClient:
    """OpenAI Chat Completions adapter.

    Uses `response_format: {"type": "json_object"}` so the server enforces
    JSON-only output — when this is set, the user message MUST contain the
    word "JSON" or the API rejects the request. Our prompt template
    (Sprint 5h `_render_prompt`) already ends with "RESPONSE FORMAT (strict
    JSON, no surrounding text)" so this is satisfied by construction.
    """

    provider = "openai"

    def __init__(
        self,
        api_key: str,
        *,
        model:           str   = _DEFAULT_OPENAI_MODEL,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        base_url:        str   = "https://api.openai.com/v1",
    ) -> None:
        if not api_key:
            raise ValueError("OpenAIClient requires a non-empty api_key")
        self._api_key         = api_key
        self.model            = model
        self._timeout_seconds = timeout_seconds
        self._base_url        = base_url.rstrip("/")

    async def complete(self, prompt: str) -> str:
        url = f"{self._base_url}/chat/completions"
        body = {
            "model":           self.model,
            "messages":        [{"role": "user", "content": prompt}],
            "response_format": {"type": "json_object"},
            "max_tokens":      _MAX_OUTPUT_TOKENS,
            "temperature":     0.2,   # low — we want stable structured output, not creativity
        }
        headers = {
            "authorization": f"Bearer {self._api_key}",
            "content-type":  "application/json",
        }

        try:
            async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                resp = await client.post(url, json=body, headers=headers)
        except httpx.TimeoutException as exc:
            raise LLMClientError(f"openai timeout after {self._timeout_seconds}s") from exc
        except httpx.HTTPError as exc:
            raise LLMClientError(f"openai network error: {exc!s}") from exc

        if resp.status_code >= 400:
            raise LLMClientError(
                f"openai HTTP {resp.status_code}: {resp.text[:200]}"
            )

        try:
            payload: dict[str, Any] = resp.json()
            content = payload["choices"][0]["message"]["content"]
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise LLMClientError(f"openai response shape unexpected: {exc!s}") from exc

        if not isinstance(content, str):
            raise LLMClientError(f"openai content non-string: {type(content).__name__}")
        return content


# ── Anthropic ──────────────────────────────────────────────────────────


class AnthropicClient:
    """Anthropic Messages API adapter.

    No native JSON mode — we lean on a strict system prompt and the
    MacroAgent's downstream parser. Anthropic's stop-token discipline
    is good enough that this works reliably in practice.
    """

    provider = "anthropic"

    _SYSTEM_PROMPT = (
        "You are a financial sentiment analyst. Your output is consumed by "
        "automated trading code. Respond with exactly one JSON object on a "
        "single line, of shape "
        '{"action": "buy" | "hold" | "sell", "confidence": <0.0-1.0>, '
        '"rationale": "<one sentence>"}. '
        "Do not wrap the JSON in markdown fences. Do not emit anything outside "
        "the JSON object."
    )

    def __init__(
        self,
        api_key: str,
        *,
        model:           str   = _DEFAULT_ANTHROPIC_MODEL,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        base_url:        str   = "https://api.anthropic.com/v1",
        api_version:     str   = "2023-06-01",
    ) -> None:
        if not api_key:
            raise ValueError("AnthropicClient requires a non-empty api_key")
        self._api_key         = api_key
        self.model            = model
        self._timeout_seconds = timeout_seconds
        self._base_url        = base_url.rstrip("/")
        self._api_version     = api_version

    async def complete(self, prompt: str) -> str:
        url = f"{self._base_url}/messages"
        body = {
            "model":      self.model,
            "max_tokens": _MAX_OUTPUT_TOKENS,
            "system":     self._SYSTEM_PROMPT,
            "messages":   [{"role": "user", "content": prompt}],
            "temperature": 0.2,
        }
        headers = {
            "x-api-key":         self._api_key,
            "anthropic-version": self._api_version,
            "content-type":      "application/json",
        }

        try:
            async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                resp = await client.post(url, json=body, headers=headers)
        except httpx.TimeoutException as exc:
            raise LLMClientError(f"anthropic timeout after {self._timeout_seconds}s") from exc
        except httpx.HTTPError as exc:
            raise LLMClientError(f"anthropic network error: {exc!s}") from exc

        if resp.status_code >= 400:
            raise LLMClientError(
                f"anthropic HTTP {resp.status_code}: {resp.text[:200]}"
            )

        try:
            payload: dict[str, Any] = resp.json()
            blocks = payload["content"]
            text_parts = [
                b["text"] for b in blocks
                if isinstance(b, dict) and b.get("type") == "text" and "text" in b
            ]
        except (ValueError, KeyError, TypeError) as exc:
            raise LLMClientError(f"anthropic response shape unexpected: {exc!s}") from exc

        if not text_parts:
            raise LLMClientError("anthropic response had no text blocks")
        return "".join(text_parts)


# ── Factory ─────────────────────────────────────────────────────────────


def build_llm_client(
    *,
    provider: str,
    api_key:  str,
    model:    str = "",
) -> LLMClient | None:
    """Build the configured LLM client, or return None if disabled.

    Returning None (rather than raising) lets the lifespan code keep the
    pre-LLM safe-stub behavior whenever provider is `none` or the API
    key is missing — the MacroAgent treats `llm_client=None` as "act as
    the Sprint 5h stub". Operators get the same code path on a forgotten
    key as on a deliberately disabled provider.
    """
    p = (provider or "none").strip().lower()
    if p == "none":
        return None
    if not api_key:
        logger.warning(
            "llm.client.disabled_no_api_key",
            extra={
                "event":    "llm_client_disabled_no_api_key",
                "provider": p,
            },
        )
        return None

    if p == "openai":
        return OpenAIClient(api_key, model=model or _DEFAULT_OPENAI_MODEL)
    if p == "anthropic":
        return AnthropicClient(api_key, model=model or _DEFAULT_ANTHROPIC_MODEL)
    logger.error(
        "llm.client.unknown_provider",
        extra={"event": "llm_client_unknown_provider", "provider": p},
    )
    return None
