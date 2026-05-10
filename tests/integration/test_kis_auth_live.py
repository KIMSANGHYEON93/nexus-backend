"""LIVE KIS OAuth integration test — actually hits openapivts.koreainvestment.com.

Marked `integration` and skipped automatically when KIS_APP_KEY /
KIS_APP_SECRET are not present. CI never runs it; humans run it locally
(or on the DevContainer) after rotating the paper credentials.

Successful run logs `kis.auth.success` with the masked token to stderr;
the test assertions then prove the wire contract holds end-to-end.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

# Load .env from the project root so this script works whether pytest is
# invoked from `nexus-backend/` (normal) or from the repo root.
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parents[2] / ".env"
    if _env_path.is_file():
        load_dotenv(_env_path, override=False)
except ImportError:
    pass

from src.core.config import get_settings
from src.infrastructure.kis_client import KisClient, KisConnectionState

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (os.environ.get("KIS_APP_KEY") and os.environ.get("KIS_APP_SECRET")),
        reason="KIS_APP_KEY/KIS_APP_SECRET not set — skipping live OAuth integration test",
    ),
]


async def test_live_kis_authenticate_returns_real_token(caplog: pytest.LogCaptureFixture) -> None:
    """Hit the real KIS Paper OAuth endpoint and validate the token.

    Asserts:
      • POST /oauth2/tokenP returns 200 with a usable access_token
      • State machine lands on AUTHENTICATED
      • Token expiry is > 1h in the future (KIS issues 24h tokens)
      • `Bearer <token>` header is well-formed
      • Structured log line `kis.auth.success` was emitted with masked token
    """
    caplog.set_level(logging.INFO, logger="src.infrastructure.kis_client")
    get_settings.cache_clear()
    settings = get_settings()
    assert settings.kis_env == "paper", "live test must run against paper env"

    client = KisClient(settings)
    assert client.state is KisConnectionState.DISCONNECTED

    await client.authenticate()

    # ── State + token assertions ────────────────────────────────────────
    # Compare via .value to avoid mypy carrying the pre-call DISCONNECTED
    # narrowing across the authenticate() boundary.
    assert client.state.value == "authenticated"
    assert client.access_token is not None
    assert len(client.access_token) >= 20, "real KIS tokens are JWT-shaped (>>20 chars)"
    assert client.is_token_valid is True

    header = client.authorization_header()
    assert header.startswith("Bearer ")
    assert client.access_token in header

    # ── Expiry sanity ───────────────────────────────────────────────────
    assert client.token_expires_at is not None
    seconds_until_expiry = (client.token_expires_at - datetime.now(timezone.utc)).total_seconds()
    assert seconds_until_expiry > 3600, (
        f"KIS issues ~24h tokens but expiry is only {seconds_until_expiry:.0f}s out"
    )

    # ── Log proof — find the success record + verify it's masked ───────
    success_records = [r for r in caplog.records if r.message == "kis.auth.success"]
    assert success_records, "expected exactly one kis.auth.success log record"
    rec = success_records[0]
    masked = getattr(rec, "token_masked", None)
    assert masked is not None
    assert "..." in masked
    # The mask must NOT contain the full token body.
    assert client.access_token not in masked

    # ── Idempotency on a still-valid token — must NOT re-hit KIS.
    #    Critical: KIS rate-limits OAuth at "1분당 1회" (error EGW00133),
    #    so a chatty caller would self-DoS within seconds. Re-call here
    #    and confirm the token + expiry are byte-identical.
    token_before = client.access_token
    expiry_before = client.token_expires_at
    await client.authenticate()
    assert client.access_token == token_before, "second auth must short-circuit, not refresh"
    assert client.token_expires_at == expiry_before

    # ── Print a human-readable success line to stderr so the run output
    #    shows the user what happened. Token is masked.
    print(
        f"\n[KIS AUTH OK] env={settings.kis_env} "
        f"state={client.state.value} "
        f"header=Bearer {masked} "
        f"expires_at={client.token_expires_at.isoformat()} "
        f"({seconds_until_expiry/3600:.1f}h remaining)",
        file=sys.stderr,
    )
