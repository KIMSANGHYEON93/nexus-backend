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

import pytest

from src.core.logging import request_id_var
from src.core.middleware import RequestIdMiddleware


def _scope(headers: list[tuple[bytes, bytes]] | None = None) -> dict:
    return {
        "type": "http",
        "method": "GET",
        "path": "/v1/snapshot",
        "headers": headers or [(b"host", b"localhost")],
    }


async def _drive(mw, scope) -> tuple[list[dict], dict | None]:
    """Run one request through the middleware. Returns (sent_messages, captured_state)."""
    sent: list[dict] = []

    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(message):
        sent.append(message)

    await mw(scope, receive, send)
    return sent, None


def _start_msg(sent: list[dict]) -> dict:
    return next(m for m in sent if m["type"] == "http.response.start")


def _header(start: dict, name: bytes) -> bytes | None:
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
async def test_lifespan_scope_passes_through_untouched():
    """Middleware must NOT try to wrap lifespan or unknown scopes."""
    seen = {"called": False}

    async def app(scope, receive, send):
        seen["called"] = True
        seen["type"] = scope["type"]

    mw = RequestIdMiddleware(app)
    await mw({"type": "lifespan"}, lambda: None, lambda m: None)
    assert seen["called"] is True
    assert seen["type"] == "lifespan"
