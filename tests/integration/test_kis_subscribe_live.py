"""LIVE end-to-end test: authenticate → connect → subscribe → stream → publish.

This is the integration proof for Sprint 5c Step 3. It exercises the full
data path with the real KIS Paper gateway and a real Redis-shaped capture
sink, but does NOT require KRX trading hours — the SUBSCRIBE SUCCESS ACK
is the deterministic "the pipeline is alive" signal that flows even on
weekends. Live tick frames are captured opportunistically: if KRX is in
session, they're asserted; if not, the test passes with an explicit note.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

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
from src.infrastructure.kis_publisher import _tick_to_wire
from src.infrastructure.redis_pubsub import CHANNEL_TICK

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (os.environ.get("KIS_APP_KEY") and os.environ.get("KIS_APP_SECRET")),
        reason="KIS_APP_KEY/KIS_APP_SECRET not set — skipping live subscribe test",
    ),
]


_KST = timezone(timedelta(hours=9))


def _is_krx_open(now_utc: datetime) -> bool:
    """KRX is open Mon–Fri, 09:00–15:30 KST. Used to decide whether the
    test should require live ticks vs accept ACK-only verification."""
    kst_now = now_utc.astimezone(_KST)
    if kst_now.weekday() >= 5:        # Sat / Sun
        return False
    minutes = kst_now.hour * 60 + kst_now.minute
    return 9 * 60 <= minutes <= 15 * 60 + 30


async def test_live_full_pipeline_subscribe_to_publish(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """End-to-end: subscribe Samsung 005930 → assert SUBSCRIBE ACK comes
    through the parser pipeline → opportunistically capture and validate
    a real tick if KRX is open.

    The capture sink stands in for Redis — same `(channel, payload)`
    signature `KisPublisher` uses. We don't need a real Redis daemon for
    this test because the whole point is proving that the KIS → parser →
    payload chain is byte-correct.
    """
    caplog.set_level(logging.INFO, logger="src.infrastructure.kis_client")
    get_settings.cache_clear()
    settings = get_settings()

    client = KisClient(settings)
    await client.authenticate()
    await client.connect()
    assert client.state is KisConnectionState.CONNECTED

    # Capture sink — what KisPublisher would send to Redis.
    captured: list[tuple[str, dict[str, object]]] = []

    async def consume_until(timeout_s: float, max_ticks: int = 5) -> None:
        """Pull from stream_ticks() for up to `timeout_s`, normalizing each
        Tick to wire format. Exits early once `max_ticks` arrive."""
        async def _pump() -> None:
            async for tick in client.stream_ticks():
                captured.append((CHANNEL_TICK, _tick_to_wire(tick)))
                if len(captured) >= max_ticks:
                    return
        try:
            await asyncio.wait_for(_pump(), timeout=timeout_s)
        except asyncio.TimeoutError:
            pass  # expected — outside trading hours, ACK-only is sufficient

    await client.subscribe(["005930"])
    await consume_until(timeout_s=4.0, max_ticks=3)

    # ── Subscribe ACK assertion (always — works on weekends) ───────────
    ack_records = [
        r for r in caplog.records if r.message == "kis.stream.subscribe_ack"
    ]
    assert ack_records, "no SUBSCRIBE ACK observed — KIS rejected the request?"
    ack = ack_records[0]
    assert getattr(ack, "msg_cd", "") == "OPSP0000"
    assert getattr(ack, "tr_id", "") == "H0STCNT0"

    # ── Tick assertion (only when KRX is open) ─────────────────────────
    krx_open = _is_krx_open(datetime.now(timezone.utc))
    if krx_open:
        assert captured, (
            "KRX is open but no ticks arrived in 4s — pipeline broken or symbol idle"
        )
        # Validate the first tick's wire shape exactly matches MockPublisher.
        _, payload = captured[0]
        assert set(payload.keys()) == {"symbol", "ts", "price", "volume", "side"}
        assert payload["symbol"] == "005930"
        assert isinstance(payload["price"], float)
        assert isinstance(payload["volume"], int)
        assert payload["side"] in ("buy", "sell")
        # JSON round-trip — what hits the wire to Redis.
        round_trip = json.loads(json.dumps(payload))
        assert round_trip == payload

    ack_msg_cd = getattr(ack, "msg_cd", "?") if ack else "?"
    print(
        f"\n[KIS PIPELINE OK] env={settings.kis_env} "
        f"subscribe_ack={ack_msg_cd} "
        f"krx_open={krx_open} "
        f"ticks_captured={len(captured)}",
        file=sys.stderr,
    )
    if captured:
        sample = captured[0][1]
        print(
            f"  sample tick → channel={CHANNEL_TICK} "
            f"symbol={sample['symbol']} price={sample['price']} "
            f"volume={sample['volume']} side={sample['side']} ts={sample['ts']}",
            file=sys.stderr,
        )

    await client.close()
    # Assert via .value to bypass mypy's narrowed-Literal carry-through from
    # the earlier `is CONNECTED` assertion. Equality check on .value is
    # equivalent at runtime.
    assert client.state.value == "disconnected"
