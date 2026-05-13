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
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.v1.router import router as v1_router
from .api.websockets.stream import router as ws_router
from .core.config import Settings, get_settings
from .core.exception_handlers import install as install_exception_handlers
from .core.logging import configure_logging
from .core.middleware import RequestIdMiddleware
from .domain.trading.context import TickContext
from .domain.trading.coordinator import TradingCoordinator
from .domain.trading.executor import OrderClient, OrderExecutor, OrderResult
from .domain.trading.guardrails import (
    CoolDownGuard,
    GuardrailPipeline,
    MaxPositionSizeGuard,
    VolatilityCircuitBreaker,
)
from .domain.trading.llm_client import build_llm_client
from .domain.trading.macro_agent import MacroAgent, MockNewsProvider
from .domain.trading.models import Action
from .domain.trading.pipeline import TradingPipeline
from .domain.trading.portfolio import Portfolio
from .domain.trading.quant_agent import QuantAgent
from .domain.trading.sizer import ConfidenceLinearSizer, FixedSizer, PositionSizer
from .infrastructure.audit_publisher import make_audit_publisher
from .infrastructure.database import close_pool, get_pool, init_pool, verify_schema
from .infrastructure.execution_repository import ExecutionRepository
from .infrastructure.news_provider import build_news_provider
from .infrastructure.persistence_worker import PersistenceWorker
from .infrastructure.publisher_supervisor import PublisherSupervisor
from .infrastructure.redis_pubsub import close_client, get_client, init_client
from .infrastructure.tick_repository import TickRepository
from .infrastructure.us_publisher import EXTRA_YAHOO_SYMBOLS, UsPublisher


# Configure logging at import time so module-load messages also flow
# through the JSON pipeline. configure_logging() is idempotent — lifespan
# re-applies it once settings are fully resolved.
_bootstrap_settings = get_settings()
configure_logging(level=_bootstrap_settings.log_level)


def _build_sizer(settings: Settings) -> PositionSizer:
    """Settings → PositionSizer. `fixed` (default) yields the pre-Sprint-5k
    behavior; `linear` enables confidence-driven sizing between
    `min_order_quantity` and `max_order_quantity`, with an optional
    `min_order_confidence` floor below which the executor skips the trade.
    """
    if settings.position_sizer == "linear":
        return ConfidenceLinearSizer(
            min_quantity   = settings.min_order_quantity,
            max_quantity   = settings.max_order_quantity,
            min_confidence = settings.min_order_confidence,
        )
    return FixedSizer(settings.default_order_quantity)


class _NoopOrderClient:
    """Fail-closed OrderClient placeholder for Sprint 5h.

    The executor's `ALLOW_LIVE_ORDERS=False` default means this is never
    invoked in practice. If someone manually flips the flag before the
    real KisOrderClient is wired (Sprint 5i), this returns a clean
    `success=False` rather than crashing — the executor will log it as
    `live_order_rejected` and move on.
    """

    async def place_order(
        self, *, symbol: str, action: Action, quantity: int,
    ) -> OrderResult:
        return OrderResult(
            success=False, order_id=None,
            message="no broker wired (Sprint 5h placeholder; flip ALLOW_LIVE_ORDERS only after Sprint 5i)",
        )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
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

    # ── Sprint 5h trading pipeline ─────────────────────────────────────
    # Build the agent + guard + executor stack ONCE at startup. The
    # supervisor pipes every tick from whichever publisher is active
    # (KIS or Mock) into pipeline.on_tick.
    #
    # Order client is intentionally a no-op here for Sprint 5h. The
    # `ALLOW_LIVE_ORDERS=False` default means the executor never invokes
    # it; if an operator manually flips the flag without first wiring a
    # real broker (Sprint 5i), they get a clean rejection instead of an
    # exception. Real KisOrderClient wiring needs the supervisor's
    # KisClient access_token which is why it lives one sprint later.
    tick_context = TickContext()
    portfolio = Portfolio()
    coordinator = TradingCoordinator()
    coordinator.register(QuantAgent(context=tick_context))
    # MacroAgent collaborators:
    #   • llm_client (Sprint 5i)  — None when provider="none" or key empty,
    #     keeping the safe HOLD@0 stub. Otherwise OpenAI / Anthropic.
    #   • news_provider (Sprint 5j) — None when provider="none", in which
    #     case we fall back to MockNewsProvider with placeholder headlines.
    #     `google_rss` returns a CachingNewsProvider wrapping the real RSS
    #     fetcher (TTL via news_cache_ttl_seconds).
    llm_client = build_llm_client(
        provider=settings.llm_provider,
        api_key=settings.llm_api_key,
        model=settings.llm_model,
    )
    news_provider = build_news_provider(
        provider=settings.news_provider,
        cache_ttl_seconds=settings.news_cache_ttl_seconds,
        locales=settings.news_locales,
    ) or MockNewsProvider()
    coordinator.register(MacroAgent(
        context=tick_context,
        news_provider=news_provider,
        llm_client=llm_client,
    ))
    guards = GuardrailPipeline([
        MaxPositionSizeGuard(max_shares=1000),
        CoolDownGuard(cooldown_seconds=60.0),
        VolatilityCircuitBreaker(),
    ])
    sizer = _build_sizer(settings)
    executor = OrderExecutor(
        order_client=_NoopOrderClient(),
        allow_live_orders=settings.allow_live_orders,
        default_quantity=settings.default_order_quantity,
        sizer=sizer,
    )
    # Sprint 5m: hot-path-safe audit publisher. Pipeline calls this after
    # every executor invocation; the PersistenceWorker (started below)
    # consumes from the same Redis channel and persists to TimescaleDB.
    audit_publisher = make_audit_publisher(get_client())
    trading_pipeline = TradingPipeline(
        context=tick_context, coordinator=coordinator, guardrails=guards,
        executor=executor, portfolio=portfolio,
        audit_publisher=audit_publisher,
    )

    # PersistenceWorker — off-hot-path consumer of nexus.market.tick +
    # nexus.trading.audit. Batches ticks (default flush at 500 or 5s),
    # one-shot inserts for executions. DB outages drop batches but
    # never crash the worker; trading hot path is fully insulated.
    persistence_worker = PersistenceWorker(
        get_client(),
        TickRepository(get_pool()),
        ExecutionRepository(get_pool()),
    )
    await persistence_worker.start()
    logger.info(
        "persistence worker armed",
        extra={"event": "persistence_worker_armed"},
    )

    # ── Publisher selection + runtime failover (Sprint 5d) ─────────────
    #   • Bring-up: KIS if creds present and reachable; MockPublisher in dev
    #     if KIS bring-up fails or no creds.
    #   • Runtime: a watchdog inside the supervisor swaps to MockPublisher
    #     if the KIS publisher dies (token refresh exhausted, WS irrecoverable,
    #     etc.) so the canvas never goes silent during trading hours.
    #   • on_tick: every parsed tick from the active publisher feeds the
    #     trading pipeline above — survives KIS→mock failover.
    supervisor = PublisherSupervisor(
        get_client(), settings, on_tick=trading_pipeline.on_tick,
    )
    await supervisor.start()
    logger.info(
        "publisher supervisor armed",
        extra={
            "event":             "supervisor_armed",
            "active":            supervisor.active_kind,
            "allow_live_orders": settings.allow_live_orders,
        },
    )

    # ── US equities publisher (Sprint 5s) ──────────────────────────────
    # Sibling to PublisherSupervisor — runs independently so a US-side
    # outage (Yahoo blip) can't take down the KRX path. Same on_tick
    # observer so audit/persistence pipelines treat US ticks identically
    # to KRX. Gated off by default; flip US_PUBLISHER_ENABLED=true.
    us_publisher: UsPublisher | None = None
    if settings.us_publisher_enabled:
        us_publisher = UsPublisher(
            get_client(),
            settings.us_subscribe_symbol_list,
            data_source_url = settings.us_data_source_url,
            poll_interval_s = settings.us_poll_interval_seconds,
            batch_size      = settings.us_batch_size,
            on_tick         = trading_pipeline.on_tick,
        )
        await us_publisher.start()
        logger.info(
            "us publisher armed",
            extra={
                "event":   "us_publisher_armed",
                "symbols": len(settings.us_subscribe_symbol_list),
            },
        )

    # ── KRX Yahoo publisher (Sprint 5s+) ───────────────────────────────
    # Real KRX prices via Yahoo as a SUPPLEMENT to KisPublisher. The two
    # publish to the same `nexus.market.tick` channel:
    #   • During KRX hours (09–15:30 KST) — KIS streams ~2 ticks/sec via
    #     WS; Yahoo adds 1 tick per symbol per 60s. UI/DB latest-write
    #     reflects whichever fired most recently, which during hours is
    #     overwhelmingly KIS.
    #   • Off-hours / weekends / KIS WS outages — only Yahoo publishes,
    #     so SK Hynix shows its real ~1.88M close instead of Mock's
    #     stale 197K.
    # Same symbol list as KIS so the universes line up. `yahoo_symbol_
    # suffix=".KS"` is appended for the Yahoo lookup; the published
    # symbol stays bare (000660), matching the existing entity table
    # and KIS publisher's wire format.
    # ── Extra-universe Yahoo publisher (Sprint 5s 전면 개선) ────────────
    # Real prices for everything OUTSIDE the KRX/US equity pair: sector
    # ETFs (XLK/XLF/...), FX (EURUSD/USDJPY/...), commodities (WTI/Gold/
    # Silver/...), crypto (BTC/ETH/USDT/USDC), market indices (VIX/DXY),
    # US Treasury yields (UST10). Replaces the synthetic MockStreamer
    # random walk that used to back these canvas nodes.
    #
    # Each symbol has its own Yahoo ticker via EXTRA_YAHOO_SYMBOLS map
    # because Yahoo's ticker conventions vary by instrument class
    # (`CL=F` futures vs `^VIX` index vs `EURUSD=X` FX).
    extra_yahoo_publisher: UsPublisher | None = None
    if settings.extra_yahoo_publisher_enabled:
        extra_yahoo_publisher = UsPublisher(
            get_client(),
            list(EXTRA_YAHOO_SYMBOLS.keys()),
            data_source_url  = settings.us_data_source_url,
            poll_interval_s  = settings.extra_yahoo_poll_interval_seconds,
            batch_size       = settings.extra_yahoo_batch_size,
            yahoo_symbol_map = EXTRA_YAHOO_SYMBOLS,
            on_tick          = trading_pipeline.on_tick,
        )
        await extra_yahoo_publisher.start()
        logger.info(
            "extra yahoo publisher armed",
            extra={
                "event":   "extra_yahoo_publisher_armed",
                "symbols": len(EXTRA_YAHOO_SYMBOLS),
            },
        )

    krx_yahoo_publisher: UsPublisher | None = None
    if settings.krx_yahoo_publisher_enabled:
        krx_yahoo_publisher = UsPublisher(
            get_client(),
            settings.kis_subscribe_symbol_list,
            data_source_url     = settings.us_data_source_url,
            poll_interval_s     = settings.krx_yahoo_poll_interval_seconds,
            batch_size          = settings.krx_yahoo_batch_size,
            yahoo_symbol_suffix = settings.krx_yahoo_symbol_suffix,
            on_tick             = trading_pipeline.on_tick,
        )
        await krx_yahoo_publisher.start()
        logger.info(
            "krx yahoo publisher armed",
            extra={
                "event":   "krx_yahoo_publisher_armed",
                "symbols": len(settings.kis_subscribe_symbol_list),
                "suffix":  settings.krx_yahoo_symbol_suffix,
            },
        )

    try:
        yield
    finally:
        logger.info("backend shutting down")
        # Order matters:
        #   1. Supervisor + UsPublisher first → stops both publishers → no
        #      new ticks flow into the pipeline → no new audit envelopes
        #      get published.
        #   2. PersistenceWorker → drains pending tick buffer + cancels
        #      its Redis subscriptions before we close the Redis client.
        #   3. Redis client.
        #   4. DB pool LAST — repos still need it for the worker's final
        #      drain insert.
        if us_publisher is not None:
            await us_publisher.stop()
        if extra_yahoo_publisher is not None:
            await extra_yahoo_publisher.stop()
        if krx_yahoo_publisher is not None:
            await krx_yahoo_publisher.stop()
        await supervisor.stop()
        await persistence_worker.stop()
        await close_client()
        await close_pool()


app = FastAPI(
    title="NEXUS OS Backend",
    version="0.1.0",
    description="Real-time market intelligence backend for the NEXUS OS dashboard.",
    lifespan=lifespan,
)


# RequestId FIRST so it becomes the outermost layer.
# Starlette's add_middleware() is generic over `_MiddlewareClass[*P]`; pure
# ASGI middlewares like ours don't satisfy that protocol because they take
# scope/receive/send rather than the Starlette-flavored Request/Response
# wrapper. The runtime accepts this fine — Starlette only inspects __init__
# arity at registration time. The `arg-type` ignore is narrowly scoped here.
app.add_middleware(RequestIdMiddleware)  # type: ignore[arg-type]
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
async def root() -> dict[str, str]:
    return {"service": "nexus-backend", "docs": "/docs", "health": "/v1/health"}
