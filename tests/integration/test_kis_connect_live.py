"""LIVE KIS WebSocket integration test — actually opens a connection to
`ws://ops.koreainvestment.com:31000` and proves bidirectional traffic.

Marked `integration` and skipped automatically when KIS_APP_KEY /
KIS_APP_SECRET are not present.

Verification strategy:
  1. authenticate() — get access_token (REST)
  2. connect()      — get approval_key + open WebSocket
  3. Subscribe to Samsung 005930 H0STCNT0 (체결가) and assert we receive
     the SUBSCRIBE SUCCESS ACK frame within 5s. This proves bidirectional
     traffic, not just TCP open.
  4. close()        — clean shutdown.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import pytest

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
        reason="KIS_APP_KEY/KIS_APP_SECRET not set — skipping live WS integration test",
    ),
]


def _build_subscribe_frame(approval_key: str, tr_id: str, tr_key: str) -> str:
    """KIS WS subscribe frame — JSON, header carries the approval_key."""
    return json.dumps({
        "header": {
            "approval_key": approval_key,
            "custtype":     "P",         # P=개인, B=법인
            "tr_type":      "1",         # 1=등록, 2=해제
            "content-type": "utf-8",
        },
        "body": {
            "input": {
                "tr_id":  tr_id,
                "tr_key": tr_key,
            },
        },
    })


async def test_live_kis_websocket_handshake(caplog: pytest.LogCaptureFixture) -> None:
    """End-to-end: authenticate → connect → subscribe → SUBSCRIBE SUCCESS ACK.

    The subscribe ACK is KIS's confirmation that:
      • the approval_key was accepted at the WS layer
      • the tr_id is recognized
      • the channel is alive end-to-end
    Without it, a green WS open could mask a broken auth/subscribe path.
    """
    caplog.set_level(logging.INFO, logger="src.infrastructure.kis_client")
    get_settings.cache_clear()
    settings = get_settings()
    assert settings.kis_env == "paper"

    client = KisClient(settings)

    # ── Stage 1: REST auth ─────────────────────────────────────────────
    await client.authenticate()
    assert client.state.value == "authenticated"

    # ── Stage 2: Approval + WS open ────────────────────────────────────
    await client.connect()
    assert client.state.value == "connected"
    assert client.approval_key is not None
    assert client.is_ws_open is True
    assert client._ws is not None  # noqa: SLF001 — test introspection

    ws = client._ws  # noqa: SLF001

    # ── Stage 3: Subscribe to Samsung 005930 (체결가) and read ACK ──────
    # H0STCNT0 = realtime trade ticks. Smallest possible "is the channel
    # alive end-to-end" probe; KIS replies with a JSON ACK frame BEFORE
    # any tick data starts flowing.
    subscribe_frame = _build_subscribe_frame(
        approval_key=client.approval_key,
        tr_id="H0STCNT0",
        tr_key="005930",
    )
    await ws.send(subscribe_frame)

    # Read frames until we see the SUBSCRIBE SUCCESS ACK or timeout.
    # KIS may push a PINGPONG or similar before the subscribe ack;
    # accept any frame whose body's msg1 contains "SUBSCRIBE" or whose
    # msg_cd is OPSP0000 (subscribe success code).
    ack_payload: dict[str, Any] | None = None
    deadline = asyncio.get_event_loop().time() + 5.0
    while asyncio.get_event_loop().time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
        except asyncio.TimeoutError:
            continue
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        # Subscribe ACKs are JSON; tick frames start with "0|" or "1|".
        if not raw.startswith("{"):
            continue
        try:
            payload = json.loads(raw)
        except ValueError:
            continue
        if "body" in payload and isinstance(payload["body"], dict):
            ack_payload = payload
            break

    assert ack_payload is not None, "no JSON ACK frame received within 5s"
    body = ack_payload["body"]
    assert body.get("rt_cd") == "0", f"KIS replied with non-OK rt_cd: {body}"
    msg = body.get("msg1", "")
    msg_cd = body.get("msg_cd", "")
    assert "SUBSCRIBE SUCCESS" in msg or msg_cd == "OPSP0000", (
        f"expected SUBSCRIBE SUCCESS, got msg_cd={msg_cd!r} msg1={msg!r}"
    )

    # ── Print success line to stderr — masked credentials only ─────────
    from src.infrastructure.kis_client import _mask_token
    print(
        f"\n[KIS WS HANDSHAKE OK] env={settings.kis_env} "
        f"state={client.state.value} "
        f"approval_key={_mask_token(client.approval_key)} "
        f"subscribe_ack=msg_cd={msg_cd} msg1={msg!r}",
        file=sys.stderr,
    )

    # ── Stage 4: clean shutdown ────────────────────────────────────────
    await client.close()
    assert client.state is KisConnectionState.DISCONNECTED
    assert client.is_ws_open is False

    # Log proof — kis.ws.connected was emitted with masked approval_key.
    connected_records = [r for r in caplog.records if r.message == "kis.ws.connected"]
    assert connected_records, "expected kis.ws.connected log record"
    masked_log = getattr(connected_records[0], "approval_key_masked", None)
    assert masked_log is not None
    assert "..." in masked_log
    assert client.approval_key not in masked_log  # mask must not leak full key
