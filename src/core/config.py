"""Centralized settings — `.env` → typed Pydantic Settings → `get_settings()`.

Everything that varies by environment (dev / staging / prod / paper / live)
lands here. `lru_cache` makes `get_settings()` a process-wide singleton, so
secrets are read from disk exactly once and re-used by every request.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── App ─────────────────────────────────────────────────────────────
    app_env: Literal["development", "staging", "production"] = "development"
    log_level: str = "INFO"
    cors_origins: str = "http://localhost:5173,http://localhost:3000"

    # ── Infrastructure ─────────────────────────────────────────────────
    database_url: str
    redis_url: str

    # ── Korea Investment & Securities ──────────────────────────────────
    kis_app_key: str = ""
    kis_app_secret: str = ""
    kis_account_number: str = ""
    kis_env: Literal["paper", "live"] = "paper"
    # Subscribe list for the live publisher. Default mirrors the seeded
    # KRX universe (db/seeds/dev.sql + MockPublisher) so the cutover is
    # symbol-for-symbol comparable to the mock stream.
    kis_subscribe_symbols: str = (
        "005930,000660,035420,035720,105560,055550,"
        "086790,005380,005490,051910,207940,068270"
    )

    # ── US equities publisher (Sprint 5s) ──────────────────────────────
    # Server-side counterpart to the frontend's MomentumStreamer('live').
    # When enabled, UsPublisher polls Yahoo Finance's public chart
    # endpoint and publishes US ticks to `nexus.market.tick` alongside
    # KRX — the persistence worker, audit pipeline, and trading pipeline
    # are all symbol-agnostic, so flipping this on automatically backfills
    # DB persist + audit coverage + signal-sparkline rows for US tickers.
    #
    # Off by default to keep the bring-up path narrow until operator opts in.
    #
    # Why Yahoo, not the Alpha Vantage proxy the frontend uses: the AV
    # proxy's shared free-tier key is capped at 25 requests per DAY,
    # easily exhausted by frontend traffic alone. Yahoo's chart endpoint
    # has no per-day cap that matters at our scale. Symbol-agnostic
    # wire-format means the swap is invisible to every downstream
    # consumer; frontend stays on AV for its own reasons.
    us_publisher_enabled: bool = False
    us_subscribe_symbols: str = (
        # Mirrors the frontend MomentumStreamer MOMENTUM_UNIVERSE (28 tickers).
        # Keep them in sync — adding a symbol here without registering it as
        # a frontend entity means useMarketData silently drops the tick.
        "AAPL,MSFT,NVDA,AVGO,CRM,AMD,INTC,ORCL,"
        "GOOGL,META,NFLX,DIS,"
        "AMZN,TSLA,HD,MCD,NKE,"
        "LLY,JNJ,UNH,MRK,PFE,"
        "JPM,V,MA,BAC,"
        "XOM,CVX"
    )
    # 30s × 4-symbol batches → 28-symbol rotation every ~3.5 min. Yahoo
    # tolerates much higher rates but we don't need it — backend persist
    # doesn't need sub-second freshness.
    us_poll_interval_seconds: float = 30.0
    us_batch_size: int = 4
    us_data_source_url: str = "https://query1.finance.yahoo.com/v8/finance/chart"

    # ── Extra-universe Yahoo publisher (Sprint 5s 전면 개선) ────────────
    # Polymorphic publisher covering everything the canvas renders OUTSIDE
    # the 12 KRX + 28 US equity universes — sector ETFs, FX pairs,
    # commodities, crypto, market indices (VIX/DXY), US Treasury yields.
    # The full symbol→Yahoo-ticker mapping lives in
    # `src/infrastructure/us_publisher.py::EXTRA_YAHOO_SYMBOLS` (30 entries
    # at 2026-05-11). Yahoo ticker conventions diverge across instrument
    # classes (`CL=F` for WTI futures, `^VIX` for the index, `EURUSD=X`
    # for FX) so the publisher takes a per-symbol map instead of a
    # uniform suffix.
    #
    # Off by default for the same bring-up gate as the other publishers.
    extra_yahoo_publisher_enabled: bool = False
    # Slower than equity publishers — these instruments move more slowly
    # (commodities, yields) and the canvas doesn't need sub-minute freshness.
    extra_yahoo_poll_interval_seconds: float = 60.0
    extra_yahoo_batch_size: int = 4

    # ── KRX Yahoo publisher (Sprint 5s+) ───────────────────────────────
    # Same Yahoo Finance chart endpoint, KRX ticker suffix ".KS"
    # (000660 → 000660.KS). Runs as a SUPPLEMENT to KisPublisher: when
    # KIS is alive and streaming during 09:00–15:30 KST, KIS's sub-
    # second WS ticks dominate the wire; when KIS dies at market close
    # (intraday-only WS contract) or fails OAuth refresh, Yahoo keeps
    # publishing real KRX closing prices instead of letting MockPublisher
    # take over with a 2024-era synthetic universe (the "SK Hynix 199K"
    # discrepancy the operator caught — real 2026-05 price is ~1.88M).
    #
    # Slow poll (60s) is intentional — KRX off-hours data only changes
    # at the next session open, and during hours KIS handles freshness.
    krx_yahoo_publisher_enabled: bool = False
    krx_yahoo_poll_interval_seconds: float = 60.0
    krx_yahoo_batch_size: int = 4
    krx_yahoo_symbol_suffix: str = ".KS"

    # ── Universe publisher (Sprint 5s+ — extended universe) ────────────
    # DB-backed Yahoo Finance publisher that polls EVERY ticker in
    # security_master that isn't already covered by KIS. With the
    # extended universe (~900 tickers across KOSPI 200 + KOSDAQ 150 +
    # S&P 500 + Nasdaq 100), this is what backfills tick coverage for
    # the long tail.
    #
    # Default OFF — the dedicated KIS / Us / Krx-Yahoo / Extra-Yahoo
    # publishers already cover the hand-curated 40-symbol Sprint 5s
    # universe. Operator flips UNIVERSE_PUBLISHER_ENABLED=true on the
    # production cluster once the extended seed is verified.
    #
    # Slow poll (60s) + larger batch (10) reflects the ~6× larger
    # symbol count vs UsPublisher. Full universe rotation:
    #   ceil(900/10) · 60s = ~90 minutes.
    # That's fine for backend persist + canvas reference data; live
    # trading symbols sit under KIS / Us with sub-second freshness.
    universe_publisher_enabled: bool = False
    universe_publisher_poll_interval_seconds: float = 60.0
    universe_publisher_batch_size: int = 10

    # ── Trading execution (Sprint 5g) ──────────────────────────────────
    # GLOBAL HARD SAFETY SWITCH. Default False — the OrderExecutor will
    # only emit shadow-trade logs and NEVER call the KIS order REST API.
    # MUST be hand-set on the production cluster (not in `.env.example`)
    # so that no committed config can ever flip a paper-trading deploy
    # into live trading by accident. Toggling this is the most consequential
    # config change in the system; treat any commit that touches it as
    # production-impact + audit-required.
    allow_live_orders: bool = False
    # Default fixed quantity — used directly by `position_sizer="fixed"`
    # AND as the lower bound for the linear sizer when not overridden
    # via `min_order_quantity`.
    default_order_quantity: int = 1

    # ── Sprint 5k position sizing ──────────────────────────────────────
    # `fixed`  — every order is `default_order_quantity` shares
    # `linear` — quantity = round(min + (max - min) × signal.confidence),
    #            with `min_order_confidence` floor (signals below that
    #            confidence are sized to 0 and skipped by the executor).
    position_sizer:        Literal["fixed", "linear"] = "fixed"
    min_order_quantity:    int   = 1
    max_order_quantity:    int   = 10
    min_order_confidence:  float = 0.0    # 0 = trade any non-HOLD signal

    # ── MacroAgent LLM provider (Sprint 5i — Open Q1 resolved) ─────────
    # `none` keeps the Sprint 5h safe-stub behavior (MacroAgent emits HOLD@0
    # without any external HTTP). Setting `openai` or `anthropic` activates
    # the real LLM call — the agent still falls back to HOLD@0 on any
    # third-party failure, so a provider outage cannot crash the pipeline.
    #
    # CRITICAL: `llm_api_key` is a SECRET. Like ALLOW_LIVE_ORDERS, it is
    # deliberately absent from `.env.example` so no committed file can
    # leak the key shape. Operators set it once on the deploy cluster.
    llm_provider: Literal["none", "openai", "anthropic"] = "none"
    llm_api_key:  str = ""
    # Empty `llm_model` lets the LLM client pick a sensible default per
    # provider (Haiku for Anthropic, gpt-4o-mini for OpenAI). Override
    # per-deploy for cost/latency tuning.
    llm_model:    str = ""

    # ── MacroAgent news provider (Sprint 5j + 5l) ──────────────────────
    # `none` keeps the MockNewsProvider stub from Sprint 5h. `google_rss`
    # activates the real Google News RSS fetcher (no API key needed).
    # Cache TTL caps the per-symbol fetch frequency — at 12 symbols × 600s
    # the upper bound is ~120 fetches/hour even if every tick triggered a
    # cache miss (which it never will once warmed up).
    news_provider:           Literal["none", "google_rss"] = "none"
    news_cache_ttl_seconds:  float = 600.0    # 10 minutes
    # Comma-separated locale list (Sprint 5l). Each locale gets its own
    # cached GoogleNewsRSSProvider; when more than one is configured, the
    # factory wraps them in a CompositeNewsProvider that merges + dedups
    # results. Default `en` matches the prior single-source behavior.
    # Korean equities benefit from `en,ko` because primary news is often
    # Korean-only while secondary analysis is English.
    news_locales:            str = "en"

    # ── Microsoft Entra ID (OIDC) ──────────────────────────────────────
    entra_tenant_id: str = ""
    entra_client_id: str = ""
    entra_audience: str = ""
    entra_issuer: str = Field(
        default="",
        description="Filled at runtime from tenant_id when blank",
    )

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @property
    def kis_subscribe_symbol_list(self) -> list[str]:
        return [s.strip() for s in self.kis_subscribe_symbols.split(",") if s.strip()]

    @property
    def us_subscribe_symbol_list(self) -> list[str]:
        return [s.strip() for s in self.us_subscribe_symbols.split(",") if s.strip()]

    @property
    def resolved_entra_issuer(self) -> str:
        if self.entra_issuer:
            return self.entra_issuer
        if self.entra_tenant_id:
            return f"https://login.microsoftonline.com/{self.entra_tenant_id}/v2.0"
        return ""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
