"""Unit tests for `InMemoryAlarmRepository` — filter / sort / limit / count.

The repository is async (lock-guarded) but the tests are sync-friendly via
pytest-asyncio's auto mode (configured in pyproject). No FastAPI / asyncpg
imports — this is purely the in-process backend.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.domain.alarms.models import Alarm, Severity, Status
from src.domain.alarms.repository import AlarmListFilters
from src.infrastructure.alarms.in_memory_repo import (
    InMemoryAlarmRepository,
    seed_demo_alarms,
)


_ANCHOR = datetime(2026, 5, 13, 12, 0, 0, tzinfo=timezone.utc)


def _alarm(
    *,
    id: str = "01-test",
    severity: Severity = Severity.INFO,
    status: Status = Status.ACTIVE,
    source: str = "trading-coordinator",
    occurred_at: datetime | None = None,
    acknowledged_at: datetime | None = None,
    resolved_at: datetime | None = None,
) -> Alarm:
    """Construct an Alarm with sensible defaults; only override what the
    test needs to set. Helper is typed so mypy --strict's
    disallow_untyped_calls stays happy."""
    return Alarm(
        id=id,
        severity=severity,
        status=status,
        source=source,
        code="TEST_CODE",
        title="TEST",
        message="msg",
        occurred_at=occurred_at or _ANCHOR,
        acknowledged_at=acknowledged_at,
        resolved_at=resolved_at,
    )


# ──────────────────────────────────────────────────────────────────────────
#  Empty repo
# ──────────────────────────────────────────────────────────────────────────

async def test_empty_repo_list_returns_no_rows():
    repo = InMemoryAlarmRepository()
    items, total = await repo.list(AlarmListFilters())
    assert items == []
    assert total == 0


async def test_empty_repo_count_active_is_zero():
    repo = InMemoryAlarmRepository()
    assert await repo.count_active() == 0


# ──────────────────────────────────────────────────────────────────────────
#  Sort — newest first by occurred_at, stable tiebreaker by id
# ──────────────────────────────────────────────────────────────────────────

async def test_list_sorts_newest_first():
    a1 = _alarm(id="01-old",   occurred_at=_ANCHOR - timedelta(minutes=10))
    a2 = _alarm(id="02-mid",   occurred_at=_ANCHOR - timedelta(minutes=5))
    a3 = _alarm(id="03-fresh", occurred_at=_ANCHOR)
    repo = InMemoryAlarmRepository([a1, a2, a3])

    items, _ = await repo.list(AlarmListFilters())
    assert [a.id for a in items] == ["03-fresh", "02-mid", "01-old"]


async def test_list_uses_id_tiebreaker_when_occurred_at_ties():
    same = _ANCHOR
    a1 = _alarm(id="aaa", occurred_at=same)
    a2 = _alarm(id="bbb", occurred_at=same)
    repo = InMemoryAlarmRepository([a1, a2])
    items, _ = await repo.list(AlarmListFilters())
    # `id` DESC tiebreaker → 'bbb' before 'aaa'
    assert [a.id for a in items] == ["bbb", "aaa"]


# ──────────────────────────────────────────────────────────────────────────
#  Status filter — default is ACTIVE only
# ──────────────────────────────────────────────────────────────────────────

async def test_default_filter_returns_only_active():
    repo = InMemoryAlarmRepository([
        _alarm(id="A", status=Status.ACTIVE),
        _alarm(id="B", status=Status.ACKNOWLEDGED,
               acknowledged_at=_ANCHOR + timedelta(seconds=1)),
        _alarm(id="C", status=Status.RESOLVED,
               resolved_at=_ANCHOR + timedelta(seconds=1)),
    ])
    items, total = await repo.list(AlarmListFilters())
    assert [a.id for a in items] == ["A"]
    assert total == 1


async def test_status_filter_accepts_multiple():
    repo = InMemoryAlarmRepository([
        _alarm(id="A", status=Status.ACTIVE,
               occurred_at=_ANCHOR),
        _alarm(id="B", status=Status.ACKNOWLEDGED,
               occurred_at=_ANCHOR - timedelta(seconds=1),
               acknowledged_at=_ANCHOR),
        _alarm(id="C", status=Status.RESOLVED,
               occurred_at=_ANCHOR - timedelta(seconds=2),
               resolved_at=_ANCHOR),
    ])
    items, total = await repo.list(
        AlarmListFilters(statuses=frozenset({Status.ACTIVE, Status.RESOLVED})),
    )
    assert {a.id for a in items} == {"A", "C"}
    assert total == 2


# ──────────────────────────────────────────────────────────────────────────
#  Severity / source / since filters
# ──────────────────────────────────────────────────────────────────────────

async def test_severity_filter_narrows_to_specified_levels():
    repo = InMemoryAlarmRepository([
        _alarm(id="info",  severity=Severity.INFO),
        _alarm(id="warn",  severity=Severity.WARN),
        _alarm(id="anom",  severity=Severity.ANOMALY),
        _alarm(id="crit",  severity=Severity.CRITICAL),
    ])
    items, total = await repo.list(AlarmListFilters(
        severities=frozenset({Severity.ANOMALY, Severity.CRITICAL}),
    ))
    assert {a.id for a in items} == {"anom", "crit"}
    assert total == 2


async def test_source_filter_exact_match_only():
    repo = InMemoryAlarmRepository([
        _alarm(id="tc", source="trading-coordinator"),
        _alarm(id="kp", source="kis-publisher"),
        _alarm(id="np", source="news-provider"),
    ])
    items, total = await repo.list(AlarmListFilters(
        sources=("trading-coordinator", "kis-publisher"),
    ))
    assert {a.id for a in items} == {"tc", "kp"}
    assert total == 2


async def test_since_filter_drops_older_alarms():
    cutoff = _ANCHOR - timedelta(minutes=10)
    repo = InMemoryAlarmRepository([
        _alarm(id="old",   occurred_at=_ANCHOR - timedelta(minutes=30)),
        _alarm(id="fresh", occurred_at=_ANCHOR - timedelta(minutes=5)),
    ])
    items, total = await repo.list(AlarmListFilters(since=cutoff))
    assert [a.id for a in items] == ["fresh"]
    assert total == 1


# ──────────────────────────────────────────────────────────────────────────
#  Limit — page size enforcement + total ignores limit
# ──────────────────────────────────────────────────────────────────────────

async def test_limit_caps_page_size_but_total_counts_all_matches():
    alarms = [
        _alarm(id=f"a{i:02d}", occurred_at=_ANCHOR - timedelta(seconds=i))
        for i in range(10)
    ]
    repo = InMemoryAlarmRepository(alarms)
    items, total = await repo.list(AlarmListFilters(limit=3))
    assert len(items) == 3
    assert total == 10
    # Newest three are a00 / a01 / a02
    assert [a.id for a in items] == ["a00", "a01", "a02"]


# ──────────────────────────────────────────────────────────────────────────
#  count_active is filter-independent
# ──────────────────────────────────────────────────────────────────────────

async def test_count_active_ignores_filter():
    repo = InMemoryAlarmRepository([
        _alarm(id="x", status=Status.ACTIVE, severity=Severity.INFO),
        _alarm(id="y", status=Status.ACTIVE, severity=Severity.CRITICAL),
        _alarm(id="z", status=Status.ACKNOWLEDGED,
               acknowledged_at=_ANCHOR + timedelta(seconds=1)),
    ])
    # The filter narrows to one severity, but count_active is global.
    items, _total = await repo.list(AlarmListFilters(
        severities=frozenset({Severity.CRITICAL}),
    ))
    assert len(items) == 1
    assert await repo.count_active() == 2


# ──────────────────────────────────────────────────────────────────────────
#  Mutation helpers
# ──────────────────────────────────────────────────────────────────────────

async def test_add_inserts_alarm_visible_on_next_list():
    repo = InMemoryAlarmRepository()
    await repo.add(_alarm(id="new"))
    items, total = await repo.list(AlarmListFilters())
    assert [a.id for a in items] == ["new"]
    assert total == 1


async def test_replace_all_wipes_previous_alarms():
    repo = InMemoryAlarmRepository([_alarm(id="old")])
    await repo.replace_all([_alarm(id="brand-new")])
    items, total = await repo.list(AlarmListFilters())
    assert [a.id for a in items] == ["brand-new"]
    assert total == 1


# ──────────────────────────────────────────────────────────────────────────
#  Seed fixture coverage
# ──────────────────────────────────────────────────────────────────────────

def test_seed_includes_every_required_source():
    """Spec §6 lists trading-coordinator / kis-publisher / news-provider /
    system-monitor as the expected demo source set. Seed must cover each."""
    seeded = seed_demo_alarms()
    sources = {a.source for a in seeded}
    assert sources == {
        "trading-coordinator",
        "kis-publisher",
        "news-provider",
        "system-monitor",
    }


def test_seed_is_deterministic_given_same_anchor():
    a = seed_demo_alarms(now=_ANCHOR)
    b = seed_demo_alarms(now=_ANCHOR)
    assert [x.id for x in a] == [x.id for x in b]
    assert [x.occurred_at for x in a] == [x.occurred_at for x in b]
