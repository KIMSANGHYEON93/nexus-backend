"""Exception handlers — every error path becomes `application/problem+json`.

Three handlers cover the full surface:

    StarletteHTTPException  → handler picks status from exc.status_code
                              and uses the standard reason phrase as title.
                              Forwards exc.headers (so 401 keeps WWW-Authenticate).
    RequestValidationError  → 422 with field-level `errors` array; `type`
                              points at our stable validation-error URI.
    Exception (catch-all)   → 500 with sanitized body. The full traceback
                              goes to logs (with the request_id stamped)
                              so the response never leaks internals.

All three set `media_type=application/problem+json` on the response and
copy the current request_id ContextVar into the body so the frontend can
display it without parsing the response header.
"""

from __future__ import annotations

import logging
from http import HTTPStatus

from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request

from .errors import (
    PROBLEM_MEDIA_TYPE,
    PROBLEM_TYPE_INTERNAL,
    PROBLEM_TYPE_VALIDATION,
    ProblemDetail,
)
from .logging import request_id_var

logger = logging.getLogger(__name__)


def _phrase(status_code: int) -> str:
    """Standard HTTP reason phrase, falling back to a generic title."""
    try:
        return HTTPStatus(status_code).phrase
    except ValueError:
        return "Error"


def _problem_response(
    problem: ProblemDetail,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=problem.status,
        content=problem.model_dump(exclude_none=True),
        media_type=PROBLEM_MEDIA_TYPE,
        headers=headers or None,
    )


async def http_exception_handler(
    request: Request,
    exc: StarletteHTTPException,
) -> JSONResponse:
    """Render any `HTTPException` (raised by us or by Starlette internally)."""
    detail_str: str | None
    if isinstance(exc.detail, str):
        detail_str = exc.detail
    elif exc.detail is None:
        detail_str = None
    else:
        # FastAPI allows non-string detail (dict/list). Stringify so the
        # response stays a valid problem+json (detail is `str | None` per RFC).
        detail_str = str(exc.detail)

    problem = ProblemDetail(
        type="about:blank",
        title=_phrase(exc.status_code),
        status=exc.status_code,
        detail=detail_str,
        instance=str(request.url.path),
        request_id=request_id_var.get(),
    )
    # Forward auth headers (e.g. WWW-Authenticate on 401) so clients know
    # how to authenticate. Without this our 401s lose the bearer challenge.
    return _problem_response(problem, headers=getattr(exc, "headers", None))


async def validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    """422 with the per-field errors surfaced under `errors`."""
    problem = ProblemDetail(
        type=PROBLEM_TYPE_VALIDATION,
        title="Validation Error",
        status=422,
        detail="One or more request fields failed validation",
        instance=str(request.url.path),
        request_id=request_id_var.get(),
        errors=jsonable_encoder(exc.errors()),
    )
    return _problem_response(problem)


async def unhandled_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """500 catch-all. Logs the full traceback (with request_id) and
    returns a sanitized body — never leak stack traces to clients."""
    logger.exception(
        "unhandled exception",
        extra={
            "event": "server_error",
            "path": str(request.url.path),
            "method": request.method,
            "error_type": type(exc).__name__,
        },
    )
    problem = ProblemDetail(
        type=PROBLEM_TYPE_INTERNAL,
        title="Internal Server Error",
        status=500,
        detail="An unexpected error occurred. The incident has been logged.",
        instance=str(request.url.path),
        request_id=request_id_var.get(),
    )
    return _problem_response(problem)


def install(app: FastAPI) -> None:
    """Register all three handlers on a FastAPI app. Call once at import time.

    Starlette's `add_exception_handler` types the handler as
    `Callable[[Request, Exception], ...]` (broadest), but FastAPI / Starlette
    dispatch by exception type at runtime — passing the narrower
    `Callable[[Request, HTTPException], ...]` is the documented pattern and
    works correctly. The `arg-type` ignores are scoped to that single
    contravariance mismatch.
    """
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(RequestValidationError, validation_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(Exception, unhandled_exception_handler)
