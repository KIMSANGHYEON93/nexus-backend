"""NEXUS OS backend — FastAPI entrypoint.

Lifespan owns the long-lived resources (DB pool, Redis client, KIS
session). Routers consume them via dedicated accessors so request handlers
stay free of bootstrap concerns.

Middleware order (outermost → innermost):
    RequestIdMiddleware  →  CORSMiddleware  →  router
Starlette wraps `user_middleware` in reverse, so the FIRST add_middleware
call becomes the OUTERMOST layer. RequestId must run first so every other
middleware (including CORS) and every log record carries the id.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.v1.router import router as v1_router
from .api.websockets.stream import router as ws_router
from .core.config import get_settings
from .core.exception_handlers import install as install_exception_handlers
from .core.logging import configure_logging
from .core.middleware import RequestIdMiddleware
from .infrastructure.database import close_pool, init_pool, verify_schema
from .infrastructure.mock_publisher import MockPublisher
from .infrastructure.redis_pubsub import close_client, get_client, init_client


# Configure logging at import time so module-load messages also flow
# through the JSON pipeline. configure_logging() is idempotent — lifespan
# re-applies it once settings are fully resolved.
_bootstrap_settings = get_settings()
configure_logging(level=_bootstrap_settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(level=settings.log_level)
    logger = logging.getLogger("nexus.main")
    logger.info(
        "backend starting",
        extra={"env": settings.app_env, "log_level": settings.log_level},
    )

    pool = await init_pool(settings)
    await init_client(settings)
    logger.info("infrastructure ready")

    # Schema verification is non-blocking by design: a stale container
    # boots, /readyz reports the problem, an operator runs db/migrate.py
    # and the next probe goes green — no restart needed. Crashing here
    # would force restart loops in Kubernetes during a partial deploy.
    try:
        check = await verify_schema(pool)
        if not check.ok:
            logger.error(
                "startup: schema verification failed — service will report "
                "NOT READY on /v1/readyz until migrations are applied",
                extra={
                    "event": "startup_schema_stale",
                    "applied": check.applied,
                    "expected": check.expected,
                    "reason": check.reason,
                },
            )
    except Exception:  # noqa: BLE001
        logger.exception(
            "startup: schema verification raised unexpectedly",
            extra={"event": "startup_schema_error"},
        )

    # Mock publisher: only when this looks like a credential-less dev box.
    # In any other configuration we let the real KIS adapter (Sprint 4c)
    # own the channel — running both would collide on the same Redis key.
    mock_publisher: MockPublisher | None = None
    has_kis_creds = bool(settings.kis_app_key and settings.kis_app_secret)
    if settings.app_env == "development" and not has_kis_creds:
        mock_publisher = MockPublisher(get_client())
        await mock_publisher.start()
        logger.info(
            "mock publisher armed (dev mode, no KIS creds)",
            extra={"event": "mock_publisher_armed", "kis_env": settings.kis_env},
        )
    else:
        logger.info(
            "mock publisher NOT armed",
            extra={
                "event": "mock_publisher_skipped",
                "app_env": settings.app_env,
                "kis_creds": has_kis_creds,
            },
        )

    try:
        yield
    finally:
        logger.info("backend shutting down")
        if mock_publisher is not None:
            await mock_publisher.stop()
        await close_client()
        await close_pool()


app = FastAPI(
    title="NEXUS OS Backend",
    version="0.1.0",
    description="Real-time market intelligence backend for the NEXUS OS dashboard.",
    lifespan=lifespan,
)


# RequestId FIRST so it becomes the outermost layer.
app.add_middleware(RequestIdMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_bootstrap_settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)


install_exception_handlers(app)

app.include_router(v1_router)
app.include_router(ws_router)


@app.get("/", include_in_schema=False)
async def root() -> dict:
    return {"service": "nexus-backend", "docs": "/docs", "health": "/v1/health"}
