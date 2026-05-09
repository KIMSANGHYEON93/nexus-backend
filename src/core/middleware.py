"""ASGI middleware — request_id propagation.

Pure-ASGI form (not Starlette `BaseHTTPMiddleware`) so we can wrap the
`send` callable directly and inject the response header without paying
the overhead of an inner Request/Response abstraction.

Order in the middleware stack matters. Starlette wraps `user_middleware`
in reverse, so the FIRST `add_middleware(...)` call becomes the OUTERMOST
layer. Add this middleware before CORS in `main.py` so request_id is set
prior to any other middleware (including CORS preflight responses) and
every log line in the request's lifetime carries it.
"""

from __future__ import annotations

import uuid
from typing import Any, Awaitable, Callable

from .logging import REQUEST_ID_HEADER, request_id_var

ASGIScope = dict[str, Any]
ASGIMessage = dict[str, Any]
ASGIReceive = Callable[[], Awaitable[ASGIMessage]]
ASGISend = Callable[[ASGIMessage], Awaitable[None]]
# An ASGI app is a callable taking (scope, receive, send) and returning an
# awaitable. Spelled out here because importing Starlette's `ASGIApp` type
# would couple this module to the framework — the middleware itself is
# vanilla ASGI by design.
ASGIApp = Callable[[ASGIScope, ASGIReceive, ASGISend], Awaitable[None]]


class RequestIdMiddleware:
    """Read or generate `X-Request-ID`, propagate via ContextVar, echo on response.

    Behavior:
      • Inbound `X-Request-ID` header is reused verbatim if present (so a
        reverse proxy / load balancer / frontend correlation ID survives).
      • Otherwise a fresh UUID4 is generated.
      • Outbound responses always carry the same `X-Request-ID` header so
        the client can correlate without parsing the body.
      • The ContextVar is reset in a `finally` block so background tasks
        that outlive the response don't inherit a stale id.
    """

    def __init__(self, app: ASGIApp, header_name: str = REQUEST_ID_HEADER) -> None:
        self.app = app
        self._header_lower_bytes = header_name.lower().encode("latin-1")

    async def __call__(
        self,
        scope: ASGIScope,
        receive: ASGIReceive,
        send: ASGISend,
    ) -> None:
        if scope["type"] not in ("http", "websocket"):
            # lifespan and unknown — pass through untouched.
            await self.app(scope, receive, send)
            return

        incoming: bytes | None = None
        for name, value in scope.get("headers", ()):
            if name == self._header_lower_bytes:
                incoming = value
                break

        if incoming:
            request_id = incoming.decode("latin-1", errors="replace").strip() or str(uuid.uuid4())
        else:
            request_id = str(uuid.uuid4())

        rid_bytes = request_id.encode("latin-1")
        token = request_id_var.set(request_id)

        async def send_with_header(message: ASGIMessage) -> None:
            if message["type"] == "http.response.start":
                # Strip any header the app already added so we don't double-emit,
                # then append our canonical value last.
                headers = [
                    (n, v) for n, v in message.get("headers", ())
                    if n != self._header_lower_bytes
                ]
                headers.append((self._header_lower_bytes, rid_bytes))
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, send_with_header)
        finally:
            request_id_var.reset(token)
