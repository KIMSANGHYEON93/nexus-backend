"""Tests for the auth gate decision matrix in `core.security.get_current_user`.

The matrix has 9 cells — see the Sprint 4b verification report for the
full table. We pin every fail-closed branch here so a future refactor
can't silently widen the auth surface.

This module skips wholesale when the backend isn't fully installed
(host machine without pydantic_settings/fastapi/jose); it's exercised
end-to-end inside the Docker container and CI.
"""

from __future__ import annotations

import pytest

# Skip the whole module if any of the runtime deps it touches are absent.
pytest.importorskip("pydantic_settings", reason="full backend deps required")
pytest.importorskip("fastapi",           reason="full backend deps required")
pytest.importorskip("jose",              reason="full backend deps required")

from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from src.core.security import Principal, get_current_user


def _bearer(token: str = "x") -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


def _basic() -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Basic", credentials="x")


# ──────────────────────────────────────────────────────────────────────────
#  No credentials presented
# ──────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_dev_no_entra_no_creds_returns_anonymous(settings_dev_no_entra):
    principal = await get_current_user(creds=None, settings=settings_dev_no_entra)
    assert isinstance(principal, Principal)
    assert principal.subject == "anonymous"
    assert principal.tenant == "dev"


@pytest.mark.asyncio
async def test_prod_no_creds_raises_401(settings_prod_no_entra):
    with pytest.raises(HTTPException) as exc:
        await get_current_user(creds=None, settings=settings_prod_no_entra)
    assert exc.value.status_code == 401
    assert exc.value.headers is not None
    assert "Bearer" in exc.value.headers.get("WWW-Authenticate", "")


@pytest.mark.asyncio
async def test_dev_with_entra_config_no_creds_raises_401(settings_with_entra):
    """Once Entra is configured, even dev must enforce."""
    with pytest.raises(HTTPException) as exc:
        await get_current_user(creds=None, settings=settings_with_entra)
    assert exc.value.status_code == 401


# ──────────────────────────────────────────────────────────────────────────
#  Credentials presented but should fail closed before validation
# ──────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_wrong_scheme_raises_401(settings_with_entra):
    with pytest.raises(HTTPException) as exc:
        await get_current_user(creds=_basic(), settings=settings_with_entra)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_token_with_no_entra_config_returns_500(settings_dev_no_entra):
    """A token was presented but we cannot validate — fail CLOSED with 500."""
    with pytest.raises(HTTPException) as exc:
        await get_current_user(creds=_bearer(), settings=settings_dev_no_entra)
    assert exc.value.status_code == 500


@pytest.mark.asyncio
async def test_prod_token_with_no_entra_returns_500(settings_prod_no_entra):
    """Production with a token but no Entra config — same fail-closed posture."""
    with pytest.raises(HTTPException) as exc:
        await get_current_user(creds=_bearer(), settings=settings_prod_no_entra)
    assert exc.value.status_code == 500
