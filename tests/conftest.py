"""Shared test fixtures.

`pythonpath = ["."]` in pyproject.toml lets us import `src.*` without
having to install the package. The fixtures below stub out anything that
would otherwise reach across the network or hit a real Postgres / Redis.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import pytest


@pytest.fixture(autouse=True)
def _reset_root_logger():
    """Pytest itself attaches a LogCaptureHandler to root, and our
    `configure_logging()` deliberately strips all root handlers.
    Save/restore the handler list so tests don't fight the captureLog plugin.
    """
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    yield
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in saved_handlers:
        root.addHandler(h)
    root.setLevel(saved_level)


@pytest.fixture
def env_minimal(monkeypatch):
    """Set the bare-minimum env so `Settings()` can construct without
    the host's real .env leaking in. Tests that need richer env override
    on top of this fixture.

    Skips when pydantic_settings isn't installed (host without full
    requirements.txt — runs only on Docker / CI)."""
    pytest.importorskip("pydantic_settings", reason="full backend deps required")
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    # Clear any Entra-related env so dev-bypass is exercised by default.
    for var in ("ENTRA_TENANT_ID", "ENTRA_CLIENT_ID", "ENTRA_AUDIENCE"):
        monkeypatch.delenv(var, raising=False)
    # Drop the cached singleton so the next get_settings() picks up the patch.
    from src.core.config import get_settings
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def settings_dev_no_entra(env_minimal):
    """Settings instance with dev-bypass active (development env, blank Entra)."""
    from src.core.config import get_settings
    return get_settings()


@pytest.fixture
def settings_prod_no_entra(monkeypatch, env_minimal):
    """Production env without Entra config — dev-bypass MUST be denied."""
    monkeypatch.setenv("APP_ENV", "production")
    from src.core.config import get_settings
    get_settings.cache_clear()
    return get_settings()


@pytest.fixture
def settings_with_entra(monkeypatch, env_minimal):
    """Fully configured Entra environment for strict-validation tests."""
    monkeypatch.setenv("ENTRA_TENANT_ID", "11111111-1111-1111-1111-111111111111")
    monkeypatch.setenv("ENTRA_CLIENT_ID", "22222222-2222-2222-2222-222222222222")
    monkeypatch.setenv("ENTRA_AUDIENCE",  "22222222-2222-2222-2222-222222222222")
    from src.core.config import get_settings
    get_settings.cache_clear()
    return get_settings()
