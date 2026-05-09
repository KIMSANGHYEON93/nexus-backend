"""ASGI tests for `RequestIdMiddleware`.

Four scenarios that together pin down the contract:
  1. Inbound `X-Request-ID` is reused verbatim and echoed on the response.
  2. Missing header → middleware generates a UUID4.
  3. ContextVar resets to default after the request completes.
  4. If the inner app erroneously emits its own `X-Request-ID`, the
     middleware strips it and substitutes the authoritative value.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from src.core.logging import request_id_var
from src.core.middleware import RequestIdMiddleware


def _scope(headers: list[tuple[bytes, bytes]] | None = None) -> dict[str, Any]:
    return {
        "type": "http",
        "method": "GET",
        "path": "/v1/snapshot",
        "headers": headers or [(b"host", b"localhost")],
    }


async def _drive(
    mw: RequestIdMiddleware,
    scope: dict[str, Any],
) -> tuple[list[dict[str, Any]], None]:
    """Run one request through the middleware. Returns (sent_messages, None)."""
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b""}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await mw(scope, receive, send)
    return sent, None


def _start_msg(sent: list[dict[str, Any]]) -> dict[str, Any]:
    return next(m for m in sent if m["type"] == "http.response.start")


def _header(start: dict[str, Any], name: bytes) -> bytes | None:
    for n, v in start["headers"]:
        if n == name:
            return v
    return None


@pytest.mark.asyncio
async def test_reuse_incoming_request_id():
    captured = {}

    async def app(scope, receive, send):
        captured["rid"] = request_id_var.get()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    mw = RequestIdMiddleware(app)
    sent, _ = await _drive(mw, _scope([(b"x-request-id", b"client-trace-42")]))

    assert captured["rid"] == "client-trace-42"
    echoed = _header(_start_msg(sent), b"x-request-id")
    assert echoed == b"client-trace-42"


@pytest.mark.asyncio
async def test_generate_uuid_when_header_absent():
    captured = {}

    async def app(scope, receive, send):
        captured["rid"] = request_id_var.get()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    mw = RequestIdMiddleware(app)
    sent, _ = await _drive(mw, _scope())

    # parses cleanly as UUID4
    parsed = uuid.UUID(captured["rid"])
    assert parsed.version == 4
    echoed = _header(_start_msg(sent), b"x-request-id")
    assert echoed == captured["rid"].encode()


@pytest.mark.asyncio
async def test_context_var_resets_after_request():
    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    mw = RequestIdMiddleware(app)
    await _drive(mw, _scope([(b"x-request-id", b"during-request")]))

    # The middleware must reset on the way out.
    assert request_id_var.get() == "-"


@pytest.mark.asyncio
async def test_dedupes_when_app_emits_its_own_request_id():
    async def app_emits_stale(scope, receive, send):
        await send({
            "type": "http.response.start", "status": 200,
            "headers": [(b"x-request-id", b"WRONG-stale")],
        })
        await send({"type": "http.response.body", "body": b""})

    mw = RequestIdMiddleware(app_emits_stale)
    sent, _ = await _drive(mw, _scope([(b"x-request-id", b"authoritative")]))

    rids = [v for n, v in _start_msg(sent)["headers"] if n == b"x-request-id"]
    assert rids == [b"authoritative"], "middleware must dedupe + use its own value"


@pytest.mark.asyncio
async def test_lifespan_scope_passes_through_untouched() -> None:
    """Middleware must NOT try to wrap lifespan or unknown scopes."""
    seen: dict[str, Any] = {"called": False}

    async def app(
        scope: dict[str, Any],
        receive: Any,
        send: Any,
    ) -> None:
        seen["called"] = True
        seen["type"] = scope["type"]

    async def noop_receive() -> dict[str, Any]:
        return {}

    async def noop_send(_: dict[str, Any]) -> None:
        return None

    mw = RequestIdMiddleware(app)
    await mw({"type": "lifespan"}, noop_receive, noop_send)
    assert seen["called"] is True
    assert seen["type"] == "lifespan"
