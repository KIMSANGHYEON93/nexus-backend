"""Microsoft Entra ID (OIDC / JWT) bearer token validation.

Validation pipeline:
    1. Bearer token extracted from `Authorization` header.
    2. JWT header parsed (no signature check yet) to read `kid`.
    3. JWKS fetched from Entra discovery, cached in-process by `kid`.
       — TTL: 1 hour. Cache miss for an unknown `kid` triggers exactly
         one re-fetch before failing (handles Microsoft's key rotation
         events without bricking live sessions).
    4. python-jose verifies signature + claims:
         • aud  matches ENTRA_AUDIENCE (or ENTRA_CLIENT_ID fallback)
         • iss  matches the issuer reported by the discovery doc
         • exp  not expired
         • nbf  not yet before
       Plus a manual `tid` check against ENTRA_TENANT_ID — defends
       single-tenant apps from cross-tenant token confusion even if
       Entra ever issues a token whose `aud` matches ours but `tid` doesn't.
    5. Subject precedence: `oid` (Entra object ID, immutable) preferred
       over `sub` (per-app pseudonym).
    6. Roles read from `roles` claim, scopes from `scp` (space-delimited).

Anonymous-bypass policy (development convenience only):
    If `app_env == 'development'` AND no Entra config is present
    (ENTRA_TENANT_ID OR ENTRA_CLIENT_ID is blank), an unauthenticated
    request returns an anonymous Principal and the gate logs a WARNING.
    In any other environment the gate raises 401.

Concurrency: the JWKS cache is guarded by `asyncio.Lock` so the first N
concurrent cold-start requests do not stampede the discovery endpoint.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Annotated, Any

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from .config import Settings, get_settings

logger = logging.getLogger(__name__)


# auto_error=False so we raise our own 401 with detailed messages instead
# of FastAPI's generic "Not authenticated".
_bearer_scheme = HTTPBearer(auto_error=False)


@dataclass
class Principal:
    """Authenticated identity attached to a request."""

    subject: str                                    # `oid` (preferred) or `sub`
    tenant: str                                     # `tid`
    roles: list[str] = field(default_factory=list)
    scopes: list[str] = field(default_factory=list)
    raw_token: str = ""
    claims: dict[str, Any] = field(default_factory=dict)


# ──────────────────────────────────────────────────────────────────────────
#  JWKS cache
# ──────────────────────────────────────────────────────────────────────────

class JwksCache:
    """In-process JWKS cache with TTL + kid-rotation refresh.

    Microsoft rotates Entra signing keys roughly every 24 h, with overlapping
    publication windows so old + new are served simultaneously. We cache
    aggressively (1 h TTL), and on any unknown-kid lookup we force a single
    refresh before declaring the token bad. That tolerates rotation while
    still hard-failing on truly forged tokens.
    """

    DEFAULT_TTL_SECONDS = 3600
    HTTP_TIMEOUT_SECONDS = 10.0

    def __init__(self, settings: Settings, ttl: int = DEFAULT_TTL_SECONDS) -> None:
        self._settings = settings
        self._ttl = ttl
        self._keys: dict[str, dict] = {}
        self._issuer: str = ""
        self._fetched_at: float = 0.0
        self._lock = asyncio.Lock()

    @property
    def issuer(self) -> str:
        """Issuer reported by the discovery doc, falling back to the
        configured value if we have not yet fetched."""
        return self._issuer or self._settings.resolved_entra_issuer

    def _is_fresh(self) -> bool:
        return bool(self._keys) and (time.monotonic() - self._fetched_at) < self._ttl

    async def get_key(self, kid: str) -> dict:
        """Return the JWK matching `kid`. Refreshes on miss exactly once."""
        if kid in self._keys and self._is_fresh():
            return self._keys[kid]

        async with self._lock:
            # Re-check after lock acquisition: another coroutine may have
            # finished the refresh while we were queued.
            if kid in self._keys and self._is_fresh():
                return self._keys[kid]
            await self._refresh()
            if kid not in self._keys:
                raise KeyError(f"kid {kid!r} not present in Entra JWKS")
            return self._keys[kid]

    async def _refresh(self) -> None:
        tenant = self._settings.entra_tenant_id
        if not tenant:
            raise RuntimeError("ENTRA_TENANT_ID not configured")

        oidc_url = (
            f"https://login.microsoftonline.com/{tenant}"
            "/v2.0/.well-known/openid-configuration"
        )
        async with httpx.AsyncClient(timeout=self.HTTP_TIMEOUT_SECONDS) as client:
            oidc_resp = await client.get(oidc_url)
            oidc_resp.raise_for_status()
            oidc = oidc_resp.json()

            jwks_uri = oidc.get("jwks_uri")
            if not jwks_uri:
                raise RuntimeError("OIDC discovery missing jwks_uri")

            jwks_resp = await client.get(jwks_uri)
            jwks_resp.raise_for_status()
            jwks = jwks_resp.json()

        self._keys = {k["kid"]: k for k in jwks.get("keys", []) if k.get("kid")}
        self._issuer = oidc.get("issuer", "") or self._settings.resolved_entra_issuer
        self._fetched_at = time.monotonic()
        logger.info(
            "Entra JWKS refreshed: %d keys, issuer=%s", len(self._keys), self._issuer,
        )


_cache_singleton: JwksCache | None = None


def _get_cache(settings: Settings) -> JwksCache:
    """Lazy process-wide singleton so we do not refetch JWKS per request."""
    global _cache_singleton
    if _cache_singleton is None:
        _cache_singleton = JwksCache(settings)
    return _cache_singleton


# ──────────────────────────────────────────────────────────────────────────
#  Token validation
# ──────────────────────────────────────────────────────────────────────────

def _401(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


async def _validate_token(token: str, settings: Settings) -> Principal:
    """Verify signature + claims; return Principal or raise HTTPException(401).

    Every failure branch emits one structured `auth_failed` warning so the
    operator can correlate the 401 the client received with the *reason*
    on the server side, while the response detail stays generic enough to
    avoid leaking internals to an attacker.
    """
    try:
        header = jwt.get_unverified_header(token)
    except JWTError as e:
        logger.warning(
            "auth failed: malformed JWT header",
            extra={"event": "auth_failed", "reason": "malformed_header", "error_type": type(e).__name__},
        )
        raise _401(f"Malformed JWT header: {e}") from e

    kid = header.get("kid")
    if not kid:
        logger.warning(
            "auth failed: JWT header missing 'kid'",
            extra={"event": "auth_failed", "reason": "missing_kid"},
        )
        raise _401("JWT header missing 'kid'")

    cache = _get_cache(settings)
    try:
        key = await cache.get_key(kid)
    except KeyError as e:
        logger.warning(
            "auth failed: kid not in JWKS",
            extra={"event": "auth_failed", "reason": "unknown_kid", "kid": kid},
        )
        raise _401(str(e)) from e
    except (httpx.HTTPError, RuntimeError) as e:
        logger.exception(
            "auth failed: JWKS lookup error",
            extra={"event": "auth_failed", "reason": "jwks_lookup", "error_type": type(e).__name__},
        )
        raise _401(f"JWKS lookup failed: {e}") from e

    expected_audience = settings.entra_audience or settings.entra_client_id
    expected_issuer = cache.issuer
    if not expected_audience or not expected_issuer:
        # Misconfiguration of the server — surface as 500 rather than 401
        # so the operator notices instead of blaming the client.
        logger.error(
            "auth misconfigured: audience or issuer empty",
            extra={
                "event": "auth_misconfigured",
                "audience_set": bool(expected_audience),
                "issuer_set":   bool(expected_issuer),
            },
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Entra audience/issuer not configured on the server",
        )

    try:
        claims = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            audience=expected_audience,
            issuer=expected_issuer,
            options={
                "verify_signature": True,
                "verify_aud":       True,
                "verify_iss":       True,
                "verify_exp":       True,
                "verify_nbf":       True,
            },
        )
    except JWTError as e:
        logger.warning(
            "auth failed: JWT decode rejected",
            extra={"event": "auth_failed", "reason": "decode", "error_type": type(e).__name__},
        )
        raise _401(f"JWT validation failed: {e}") from e

    # Single-tenant defense: even with a valid signature + correct aud, a
    # token from another tenant must be rejected. The discovery `issuer`
    # already binds us to the tenant in most cases, but we belt-and-brace.
    if settings.entra_tenant_id and claims.get("tid") != settings.entra_tenant_id:
        logger.warning(
            "auth failed: token tenant mismatch",
            extra={
                "event": "auth_failed",
                "reason": "tenant_mismatch",
                "expected_tid": settings.entra_tenant_id,
                "token_tid":    claims.get("tid", ""),
            },
        )
        raise _401("Token tenant ('tid') does not match this deployment")

    subject = claims.get("oid") or claims.get("sub")
    if not subject:
        logger.warning(
            "auth failed: JWT missing oid/sub",
            extra={"event": "auth_failed", "reason": "no_subject"},
        )
        raise _401("JWT missing both 'oid' and 'sub'")

    roles = claims.get("roles") or []
    scp = claims.get("scp", "")
    scopes = scp.split() if isinstance(scp, str) else list(scp or [])

    return Principal(
        subject=str(subject),
        tenant=str(claims.get("tid", "")),
        roles=list(roles),
        scopes=scopes,
        raw_token=token,
        claims=claims,
    )


# ──────────────────────────────────────────────────────────────────────────
#  FastAPI dependency
# ──────────────────────────────────────────────────────────────────────────

async def get_current_user(
    creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer_scheme)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> Principal:
    """Protect a route by adding `principal: Principal = Depends(get_current_user)`.

    Behavior:
      • Production / staging: bearer token is required; full validation;
        any failure raises 401.
      • Development with no Entra config (TENANT_ID or CLIENT_ID blank):
        unauthenticated requests return an anonymous Principal and the
        gate logs a WARNING. Set both env vars to enforce locally.
    """
    is_dev = settings.app_env == "development"
    has_entra_config = bool(settings.entra_tenant_id and settings.entra_client_id)

    if creds is None:
        if is_dev and not has_entra_config:
            logger.warning(
                "auth dev-bypass: anonymous principal granted",
                extra={"event": "auth_dev_bypass", "app_env": settings.app_env},
            )
            return Principal(subject="anonymous", tenant="dev")
        logger.warning(
            "auth failed: missing bearer token",
            extra={"event": "auth_failed", "reason": "missing_token"},
        )
        raise _401("Missing bearer token")

    if creds.scheme.lower() != "bearer":
        logger.warning(
            "auth failed: wrong auth scheme",
            extra={"event": "auth_failed", "reason": "wrong_scheme", "scheme": creds.scheme},
        )
        raise _401(f"Unsupported auth scheme: {creds.scheme!r}")

    if not has_entra_config:
        # A token was presented but we cannot validate it — fail closed.
        logger.error(
            "auth misconfigured: token presented but Entra not configured",
            extra={"event": "auth_misconfigured", "reason": "entra_not_configured"},
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Entra not configured but a bearer token was presented",
        )

    return await _validate_token(creds.credentials, settings)


# Backwards-compat alias retained so older routes (`/me`) keep working.
require_principal = get_current_user
