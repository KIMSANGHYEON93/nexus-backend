"""One-shot live smoke test: hit KIS paper OAuth and print masked token.

Reads .env / shell env via Settings, calls `KisClient.authenticate()` once,
prints the resulting state + masked token + expiry. Not a pytest target —
CI must never touch live KIS. Run manually:

    python -m scripts.kis_auth_smoke

Exits 0 on success, 2 on `KisAuthError`, 1 on anything else.
"""

from __future__ import annotations

import asyncio
import sys

from src.core.config import get_settings
from src.infrastructure.kis_client import KisAuthError, KisClient


def _mask(token: str) -> str:
    """Show only the head + tail so logs/screenshots remain shareable."""
    if len(token) <= 12:
        return token[:2] + "…"
    return f"{token[:8]}…{token[-4:]}"


async def main() -> int:
    settings = get_settings()
    if not (settings.kis_app_key and settings.kis_app_secret):
        print("ERROR: KIS_APP_KEY / KIS_APP_SECRET missing.", file=sys.stderr)
        return 1

    client = KisClient(settings)
    print(f"→ KIS_ENV={settings.kis_env}, account={settings.kis_account_number}")
    print(f"→ initial state: {client.state.value}")

    try:
        await client.authenticate()
    except KisAuthError as exc:
        print(f"✗ KIS auth FAILED: {exc}", file=sys.stderr)
        print(f"→ state: {client.state.value}")
        await client.aclose()
        return 2

    token = client.access_token or ""
    expires = client.access_token_expires_at
    print(f"✓ KIS auth SUCCESS")
    print(f"  state:      {client.state.value}")
    print(f"  token:      Bearer {_mask(token)}")
    print(f"  expires_at: {expires.isoformat() if expires else '<none>'}")
    await client.aclose()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
