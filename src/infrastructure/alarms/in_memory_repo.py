"""In-memory `AlarmRepository` — Sprint scope dev backend.

Why in-memory: spec §3 explicitly defers persistence — the read surface is
the only thing in scope this sprint, and the alarm producer pipeline
isn't designed yet. An in-process store with a clean Protocol seam lets
us ship the HUD now and switch to Timescale/Redis Streams later without
touching the router.

Concurrency: a single `asyncio.Lock` serializes mutations. Reads take
snapshots (defensive list copy) so a concurrent `add` doesn't surface
half-written state to a polling client. Throughput is not a concern at
this layer — the alarm rate is operator-scale, not tick-scale.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Iterable

from ...domain.alarms.models import Alarm, Severity, Status
from ...domain.alarms.repository import AlarmListFilters

logger = logging.getLogger(__name__)


class InMemoryAlarmRepository:
    """Process-local list of `Alarm` objects, sorted newest-first on read.

    Satisfies the `AlarmRepository` protocol structurally — we deliberately
    don't inherit, so the protocol stays the one source of truth.
    """

    def __init__(self, alarms: Iterable[Alarm] | None = None) -> None:
        self._alarms: list[Alarm] = list(alarms or ())
        self._lock = asyncio.Lock()

    # ── Mutation helpers (not on the protocol — demo-only) ──────────────
    async def add(self, alarm: Alarm) -> None:
        """Append one alarm. The protocol's read methods sort on the way
        out, so insertion order does not matter — this keeps producers
        simple and avoids re-sorting hot inside the writer."""
        async with self._lock:
            self._alarms.append(alarm)

    async def replace_all(self, alarms: Iterable[Alarm]) -> None:
        """Atomically replace the entire backing list — used by tests to
        prime a known state without dragging in a fixture-builder."""
        async with self._lock:
            self._alarms = list(alarms)

    # ── Protocol surface ────────────────────────────────────────────────
    async def list(
        self,
        filters: AlarmListFilters,
    ) -> tuple[list[Alarm], int]:
        """Return `(page, total)`. `total` ignores `limit`; `page` honors it.

        Sort is `occurred_at DESC`, with `id` as a stable tiebreaker so
        the order is deterministic when two alarms share a timestamp
        (rare in practice but trivially reproducible in tests).
        """
        # Snapshot under the lock so a concurrent add can't surface a
        # partially-mutated list. Filtering / sorting happen outside the
        # lock — they're pure list operations on the snapshot.
        async with self._lock:
            snapshot = list(self._alarms)

        matched = [a for a in snapshot if _matches(a, filters)]
        matched.sort(key=lambda a: (a.occurred_at, a.id), reverse=True)
        total = len(matched)
        return matched[: filters.limit], total

    async def count_active(self) -> int:
        """Global active-count, ignoring all filters. Powers the HUD's
        "{n} UNACK" badge which the spec mandates is filter-independent."""
        async with self._lock:
            snapshot = list(self._alarms)
        return sum(1 for a in snapshot if a.status is Status.ACTIVE)


def _matches(alarm: Alarm, filters: AlarmListFilters) -> bool:
    """Return True iff the alarm passes every filter in `filters`."""
    if alarm.status not in filters.statuses:
        return False
    if filters.severities is not None and alarm.severity not in filters.severities:
        return False
    if filters.sources is not None and alarm.source not in filters.sources:
        return False
    if filters.since is not None and alarm.occurred_at < filters.since:
        return False
    return True


# ──────────────────────────────────────────────────────────────────────────
#  Seed data — wired into the router's DI factory at process start.
# ──────────────────────────────────────────────────────────────────────────
#
# Sources covered (spec §6 "비결정 사항"): trading-coordinator,
# kis-publisher, news-provider, system-monitor — 1-2 alarms each. Status
# spread reflects realistic operator state: most active, some
# acknowledged, one resolved (so frontend can render every lifecycle
# branch from a single bootstrap call).
#
# Timestamps are anchored to `_SEED_NOW` so the relative ordering is
# stable across processes (and reproducible in tests that monkey-patch
# this constant or call `seed_demo_alarms` with a custom anchor).

_SEED_NOW = datetime(2026, 5, 13, 8, 42, 11, 314_000, tzinfo=timezone.utc)


def seed_demo_alarms(now: datetime | None = None) -> list[Alarm]:
    """Build the canonical demo alarm fixture. Deterministic — same `now`
    in, same alarms out — so it's safe to call from a test."""
    anchor = now or _SEED_NOW

    return [
        # trading-coordinator — fresh anomaly, live and unacked.
        Alarm(
            id="01HXYR3K9P7M4Q8N2V5W6X7Y8Z",
            severity=Severity.ANOMALY,
            status=Status.ACTIVE,
            source="trading-coordinator",
            code="VOLATILITY_BREAKER_TRIPPED",
            title="VOLATILITY BREAKER TRIPPED",
            message="SMH 6σ in 30s window — fills paused.",
            entity_id="SMH",
            occurred_at=anchor,
            metadata={"symbol": "SMH", "sigma": 6.1},
        ),
        # trading-coordinator — older, ack'd but not resolved.
        Alarm(
            id="01HXYR3K9P7M4Q8N2V5W6X7Y92",
            severity=Severity.WARN,
            status=Status.ACKNOWLEDGED,
            source="trading-coordinator",
            code="COOLDOWN_GUARD_FIRED",
            title="COOLDOWN GUARD FIRED",
            message="005930 buy blocked — 47s of cooldown remain.",
            entity_id="005930",
            occurred_at=anchor.replace(minute=30, second=0, microsecond=0),
            acknowledged_at=anchor.replace(minute=32, second=18, microsecond=0),
            metadata={"symbol": "005930", "guard": "cooldown"},
        ),

        # kis-publisher — critical, channel down.
        Alarm(
            id="01HXYR3K9P7M4Q8N2V5W6X7YA1",
            severity=Severity.CRITICAL,
            status=Status.ACTIVE,
            source="kis-publisher",
            code="WEBSOCKET_DISCONNECTED",
            title="KIS WEBSOCKET DISCONNECTED",
            message="H0STCNT0 channel idle 12s — auto-reconnect in flight.",
            occurred_at=anchor.replace(minute=35, second=2, microsecond=0),
            metadata={"channel": "H0STCNT0", "idle_seconds": 12},
        ),

        # news-provider — info, resolved.
        Alarm(
            id="01HXYR3K9P7M4Q8N2V5W6X7YB7",
            severity=Severity.INFO,
            status=Status.RESOLVED,
            source="news-provider",
            code="FEED_BACKFILL_COMPLETE",
            title="FEED BACKFILL COMPLETE",
            message="Reuters tier-1 caught up — 38 stories ingested.",
            occurred_at=anchor.replace(hour=7, minute=10, second=0, microsecond=0),
            resolved_at=anchor.replace(hour=7, minute=12, second=44, microsecond=0),
            metadata={"feed": "reuters", "ingested": 38},
        ),

        # system-monitor — anomaly, unacked.
        Alarm(
            id="01HXYR3K9P7M4Q8N2V5W6X7YC4",
            severity=Severity.ANOMALY,
            status=Status.ACTIVE,
            source="system-monitor",
            code="REDIS_LATENCY_SPIKE",
            title="REDIS LATENCY SPIKE",
            message="p99 publish→consume 412ms over 60s — investigate broker.",
            occurred_at=anchor.replace(minute=40, second=55, microsecond=0),
            metadata={"p99_ms": 412, "window_seconds": 60},
        ),
        # system-monitor — warn, ack'd.
        Alarm(
            id="01HXYR3K9P7M4Q8N2V5W6X7YD0",
            severity=Severity.WARN,
            status=Status.ACKNOWLEDGED,
            source="system-monitor",
            code="DISK_USAGE_HIGH",
            title="DISK USAGE HIGH",
            message="ts-replicas volume at 81% — purge job scheduled.",
            occurred_at=anchor.replace(hour=6, minute=5, second=0, microsecond=0),
            acknowledged_at=anchor.replace(hour=6, minute=9, second=22, microsecond=0),
            metadata={"volume": "ts-replicas", "usage_pct": 81},
        ),
    ]


def build_seeded_repository(
    now: datetime | None = None,
) -> InMemoryAlarmRepository:
    """Factory used by the router's DI seam at process start.

    Tests construct their own empty repo with `InMemoryAlarmRepository()`
    and call `replace_all(...)` so they don't pick up the demo seed."""
    return InMemoryAlarmRepository(seed_demo_alarms(now=now))
