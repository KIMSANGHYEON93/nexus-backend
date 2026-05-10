"""Tests for LLMClient implementations + factory.

These cover the wire contract for both providers (URL, headers, body
shape, response parsing) and every documented failure surface
(LLMClientError on timeout / 4xx / 5xx / malformed shape). Failures
inside the LLMClient are the agent's responsibility to catch — covered
in `test_trading_macro_agent.py`.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import httpx
import pytest

from src.domain.trading.llm_client import (
    AnthropicClient,
    LLMClient,
    LLMClientError,
    OpenAIClient,
    build_llm_client,
)


# ════════════════════════════════════════════════════════════════════════
#                              OpenAIClient
# ════════════════════════════════════════════════════════════════════════


def _openai_ok(text: str = '{"action":"buy","confidence":0.7}') -> httpx.Response:
    return httpx.Response(
        status_code=200,
        json={
            "choices": [{"message": {"content": text}}],
            "usage":   {"total_tokens": 50},
        },
    )


def test_openai_client_rejects_empty_api_key():
    with pytest.raises(ValueError):
        OpenAIClient(api_key="")


async def test_openai_client_posts_to_correct_url_with_correct_headers_and_body():
    client = OpenAIClient(api_key="sk-test-XYZ")
    captured: dict[str, Any] = {}

    async def _fake_post(self, url, json=None, headers=None):  # noqa: ANN001
        captured["url"]     = url
        captured["json"]    = json
        captured["headers"] = headers
        return _openai_ok()

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        text = await client.complete("test prompt — must mention JSON")

    assert captured["url"] == "https://api.openai.com/v1/chat/completions"
    assert captured["headers"]["authorization"] == "Bearer sk-test-XYZ"
    assert captured["json"]["model"]            == "gpt-4o-mini"
    assert captured["json"]["messages"][0]["role"]    == "user"
    assert captured["json"]["messages"][0]["content"] == "test prompt — must mention JSON"
    # The structured-output enforcer — without this, the API would happily
    # return prose. This is the contract Sprint 5i depends on.
    assert captured["json"]["response_format"] == {"type": "json_object"}
    assert text == '{"action":"buy","confidence":0.7}'


async def test_openai_client_propagates_custom_model():
    client = OpenAIClient(api_key="sk-x", model="gpt-4o")

    async def _fake_post(self, url, json=None, headers=None):  # noqa: ANN001
        assert json["model"] == "gpt-4o"
        return _openai_ok()

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        await client.complete("p")


async def test_openai_client_timeout_raises_llm_client_error():
    client = OpenAIClient(api_key="sk-x")
    async def _raise(self, *a, **k):  # noqa: ANN001, ANN002, ANN003
        raise httpx.ConnectTimeout("simulated")
    with patch.object(httpx.AsyncClient, "post", _raise):
        with pytest.raises(LLMClientError, match="timeout"):
            await client.complete("p")


async def test_openai_client_network_error_raises_llm_client_error():
    client = OpenAIClient(api_key="sk-x")
    async def _raise(self, *a, **k):  # noqa: ANN001, ANN002, ANN003
        raise httpx.ConnectError("dns lookup failed")
    with patch.object(httpx.AsyncClient, "post", _raise):
        with pytest.raises(LLMClientError, match="network"):
            await client.complete("p")


async def test_openai_client_http_5xx_raises_llm_client_error():
    client = OpenAIClient(api_key="sk-x")
    async def _fake_post(self, *a, **k):  # noqa: ANN001, ANN002, ANN003
        return httpx.Response(status_code=502, json={"error": "bad gateway"})
    with patch.object(httpx.AsyncClient, "post", _fake_post):
        with pytest.raises(LLMClientError, match="HTTP 502"):
            await client.complete("p")


async def test_openai_client_rate_limit_429_raises_llm_client_error():
    client = OpenAIClient(api_key="sk-x")
    async def _fake_post(self, *a, **k):  # noqa: ANN001, ANN002, ANN003
        return httpx.Response(status_code=429, json={"error": {"type": "rate_limit_exceeded"}})
    with patch.object(httpx.AsyncClient, "post", _fake_post):
        with pytest.raises(LLMClientError, match="HTTP 429"):
            await client.complete("p")


async def test_openai_client_unexpected_response_shape_raises_llm_client_error():
    client = OpenAIClient(api_key="sk-x")
    async def _fake_post(self, *a, **k):  # noqa: ANN001, ANN002, ANN003
        return httpx.Response(status_code=200, json={"unexpected": "shape"})
    with patch.object(httpx.AsyncClient, "post", _fake_post):
        with pytest.raises(LLMClientError, match="shape unexpected"):
            await client.complete("p")


# ════════════════════════════════════════════════════════════════════════
#                            AnthropicClient
# ════════════════════════════════════════════════════════════════════════


def _anthropic_ok(text: str = '{"action":"sell","confidence":0.6}') -> httpx.Response:
    return httpx.Response(
        status_code=200,
        json={
            "id":      "msg_test",
            "type":    "message",
            "role":    "assistant",
            "content": [{"type": "text", "text": text}],
        },
    )


def test_anthropic_client_rejects_empty_api_key():
    with pytest.raises(ValueError):
        AnthropicClient(api_key="")


async def test_anthropic_client_posts_to_correct_url_with_correct_headers_and_body():
    client = AnthropicClient(api_key="sk-ant-xxx")
    captured: dict[str, Any] = {}

    async def _fake_post(self, url, json=None, headers=None):  # noqa: ANN001
        captured["url"]     = url
        captured["json"]    = json
        captured["headers"] = headers
        return _anthropic_ok()

    with patch.object(httpx.AsyncClient, "post", _fake_post):
        text = await client.complete("user prompt body")

    assert captured["url"] == "https://api.anthropic.com/v1/messages"
    assert captured["headers"]["x-api-key"]         == "sk-ant-xxx"
    assert captured["headers"]["anthropic-version"] == "2023-06-01"
    assert captured["json"]["model"]                == "claude-sonnet-4-7-20250828"
    # Anthropic enforces JSON via the SYSTEM prompt — verify it's present
    # so we don't drift from the structured-output contract.
    assert "JSON" in captured["json"]["system"]
    assert captured["json"]["messages"][0]["role"]    == "user"
    assert captured["json"]["messages"][0]["content"] == "user prompt body"
    assert text == '{"action":"sell","confidence":0.6}'


async def test_anthropic_client_concatenates_multiple_text_blocks():
    """Anthropic can return multiple content blocks; our client joins them."""
    client = AnthropicClient(api_key="sk-x")

    async def _fake_post(self, *a, **k):  # noqa: ANN001, ANN002, ANN003
        return httpx.Response(status_code=200, json={
            "content": [
                {"type": "text", "text": '{"action":"buy",'},
                {"type": "text", "text": '"confidence":0.5}'},
            ],
        })
    with patch.object(httpx.AsyncClient, "post", _fake_post):
        text = await client.complete("p")
    assert text == '{"action":"buy","confidence":0.5}'


async def test_anthropic_client_no_text_blocks_raises_llm_client_error():
    client = AnthropicClient(api_key="sk-x")
    async def _fake_post(self, *a, **k):  # noqa: ANN001, ANN002, ANN003
        return httpx.Response(status_code=200, json={
            "content": [{"type": "tool_use", "name": "x", "input": {}}],
        })
    with patch.object(httpx.AsyncClient, "post", _fake_post):
        with pytest.raises(LLMClientError, match="no text blocks"):
            await client.complete("p")


async def test_anthropic_client_timeout_raises_llm_client_error():
    client = AnthropicClient(api_key="sk-x")
    async def _raise(self, *a, **k):  # noqa: ANN001, ANN002, ANN003
        raise httpx.ConnectTimeout("simulated")
    with patch.object(httpx.AsyncClient, "post", _raise):
        with pytest.raises(LLMClientError, match="timeout"):
            await client.complete("p")


async def test_anthropic_client_http_4xx_raises_llm_client_error():
    client = AnthropicClient(api_key="sk-x")
    async def _fake_post(self, *a, **k):  # noqa: ANN001, ANN002, ANN003
        return httpx.Response(status_code=401, json={"error": "unauthorized"})
    with patch.object(httpx.AsyncClient, "post", _fake_post):
        with pytest.raises(LLMClientError, match="HTTP 401"):
            await client.complete("p")


# ════════════════════════════════════════════════════════════════════════
#                              Factory
# ════════════════════════════════════════════════════════════════════════


def test_factory_none_provider_returns_none():
    assert build_llm_client(provider="none", api_key="anything") is None


def test_factory_empty_api_key_returns_none_even_with_provider_set():
    """An operator that set llm_provider but forgot the key should land
    in stub mode, not crash on first signal."""
    assert build_llm_client(provider="openai", api_key="") is None
    assert build_llm_client(provider="anthropic", api_key="") is None


def test_factory_unknown_provider_returns_none():
    assert build_llm_client(provider="bogus", api_key="sk-x") is None


def test_factory_openai_with_key_returns_openai_client():
    client = build_llm_client(provider="openai", api_key="sk-x")
    assert isinstance(client, OpenAIClient)
    assert client.provider == "openai"


def test_factory_anthropic_with_key_returns_anthropic_client():
    client = build_llm_client(provider="anthropic", api_key="sk-ant-x")
    assert isinstance(client, AnthropicClient)
    assert client.provider == "anthropic"


def test_factory_propagates_custom_model_override():
    o = build_llm_client(provider="openai", api_key="sk-x", model="gpt-4o")
    assert isinstance(o, OpenAIClient) and o.model == "gpt-4o"
    a = build_llm_client(provider="anthropic", api_key="sk-x", model="claude-sonnet-4-6")
    assert isinstance(a, AnthropicClient) and a.model == "claude-sonnet-4-6"


def test_factory_provider_string_is_normalized():
    """`OpenAI`, `OPENAI`, `  anthropic  ` should all work."""
    assert isinstance(build_llm_client(provider="OpenAI", api_key="sk-x"), OpenAIClient)
    assert isinstance(build_llm_client(provider="ANTHROPIC", api_key="sk-x"), AnthropicClient)
    assert isinstance(build_llm_client(provider="  openai  ", api_key="sk-x"), OpenAIClient)


# ── Protocol satisfaction ──────────────────────────────────────────────


def test_openai_client_satisfies_protocol():
    assert isinstance(OpenAIClient(api_key="sk-x"), LLMClient)


def test_anthropic_client_satisfies_protocol():
    assert isinstance(AnthropicClient(api_key="sk-x"), LLMClient)
