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
    def resolved_entra_issuer(self) -> str:
        if self.entra_issuer:
            return self.entra_issuer
        if self.entra_tenant_id:
            return f"https://login.microsoftonline.com/{self.entra_tenant_id}/v2.0"
        return ""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
