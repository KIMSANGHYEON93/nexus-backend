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

    # ── Trading execution (Sprint 5g) ──────────────────────────────────
    # GLOBAL HARD SAFETY SWITCH. Default False — the OrderExecutor will
    # only emit shadow-trade logs and NEVER call the KIS order REST API.
    # MUST be hand-set on the production cluster (not in `.env.example`)
    # so that no committed config can ever flip a paper-trading deploy
    # into live trading by accident. Toggling this is the most consequential
    # config change in the system; treat any commit that touches it as
    # production-impact + audit-required.
    allow_live_orders: bool = False
    # Fixed order size used until Sprint 5h adds confidence-driven sizing.
    # Per-symbol overrides come later; one global default is enough for
    # the executor to do its job without baking in opinions on size.
    default_order_quantity: int = 1

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
    def resolved_entra_issuer(self) -> str:
        if self.entra_issuer:
            return self.entra_issuer
        if self.entra_tenant_id:
            return f"https://login.microsoftonline.com/{self.entra_tenant_id}/v2.0"
        return ""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
