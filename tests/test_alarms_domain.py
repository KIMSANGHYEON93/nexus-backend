"""Unit tests for `src.domain.alarms` — pure model invariants.

These tests don't touch FastAPI, asyncio, or any repository — they
exercise the dataclass + enum constraints in `domain/alarms/models.py`
and the `AlarmListFilters` validation in `domain/alarms/repository.py`.

Mirrors `test_trading_portfolio.py` style: tiny construction helpers,
one assertion per test, descriptive names matching the spec's acceptance
checklist.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from src.domain.alarms.models import Alarm, Severity, Status
from src.domain.alarms.repository import (
    LIMIT_DEFAULT,
    LIMIT_MAX,
    LIMIT_MIN,
    AlarmListFilters,
)


def _ts(hour: int = 12, minute: int = 0) -> datetime:
    return datetime(2026, 5, 13, hour, minute, 0, tzinfo=timezone.utc)


def _minimal_active(**overrides: Any) -> Alarm:
    base: dict[str, Any] = dict(
        id="01HXYR-test",
        severity=Severity.INFO,
        status=Status.ACTIVE,
        source="trading-coordinator",
        code="TEST_CODE",
        title="TEST TITLE",
        message="Test message body.",
        occurred_at=_ts(12, 0),
    )
    base.update(overrides)
    return Alarm(**base)


# ──────────────────────────────────────────────────────────────────────────
#  Severity / Status enums — wire-string fidelity
# ──────────────────────────────────────────────────────────────────────────

def test_severity_enum_values_match_spec_wire_strings():
    """Spec §2 lists exactly these four severities on the JSON wire."""
    assert Severity.INFO.value     == "info"
    assert Severity.WARN.value     == "warn"
    assert Severity.ANOMALY.value  == "anomaly"
    assert Severity.CRITICAL.value == "critical"


def test_status_enum_values_match_spec_wire_strings():
    assert Status.ACTIVE.value       == "active"
    assert Status.ACKNOWLEDGED.value == "acknowledged"
    assert Status.RESOLVED.value     == "resolved"


# ──────────────────────────────────────────────────────────────────────────
#  Alarm — construction happy paths
# ──────────────────────────────────────────────────────────────────────────

def test_active_alarm_with_null_ack_resolved_is_valid():
    a = _minimal_active()
    assert a.status is Status.ACTIVE
    assert a.acknowledged_at is None
    assert a.resolved_at is None


def test_acknowledged_alarm_with_ack_timestamp_is_valid():
    a = _minimal_active(
        status=Status.ACKNOWLEDGED,
        acknowledged_at=_ts(12, 5),
    )
    assert a.status is Status.ACKNOWLEDGED
    assert a.acknowledged_at == _ts(12, 5)


def test_resolved_alarm_with_resolved_only_is_valid():
    """Spec allows resolved without an explicit ack step."""
    a = _minimal_active(
        status=Status.RESOLVED,
        resolved_at=_ts(12, 10),
    )
    assert a.status is Status.RESOLVED


def test_resolved_alarm_with_ack_then_resolved_is_valid():
    a = _minimal_active(
        status=Status.RESOLVED,
        acknowledged_at=_ts(12, 5),
        resolved_at=_ts(12, 10),
    )
    # Both must be present and ordered — `_minimal_active` enforces type at
    # construction but mypy still needs the explicit narrowing here.
    assert a.acknowledged_at is not None and a.resolved_at is not None
    assert a.acknowledged_at < a.resolved_at


# ──────────────────────────────────────────────────────────────────────────
#  Alarm — invariant violations (every line of the spec checklist)
# ──────────────────────────────────────────────────────────────────────────

def test_active_with_nonnull_ack_rejected():
    with pytest.raises(ValueError, match="active"):
        _minimal_active(acknowledged_at=_ts(12, 5))


def test_active_with_nonnull_resolved_rejected():
    with pytest.raises(ValueError, match="active"):
        _minimal_active(resolved_at=_ts(12, 5))


def test_acknowledged_without_ack_timestamp_rejected():
    with pytest.raises(ValueError, match="acknowledged"):
        _minimal_active(status=Status.ACKNOWLEDGED)


def test_acknowledged_with_resolved_rejected():
    with pytest.raises(ValueError, match="acknowledged"):
        _minimal_active(
            status=Status.ACKNOWLEDGED,
            acknowledged_at=_ts(12, 5),
            resolved_at=_ts(12, 10),
        )


def test_resolved_without_resolved_timestamp_rejected():
    with pytest.raises(ValueError, match="resolved"):
        _minimal_active(status=Status.RESOLVED)


def test_ack_before_occurred_rejected():
    with pytest.raises(ValueError, match="acknowledged_at"):
        _minimal_active(
            status=Status.ACKNOWLEDGED,
            occurred_at=_ts(12, 10),
            acknowledged_at=_ts(12, 5),
        )


def test_resolved_before_occurred_rejected():
    with pytest.raises(ValueError, match="resolved_at"):
        _minimal_active(
            status=Status.RESOLVED,
            occurred_at=_ts(12, 10),
            resolved_at=_ts(12, 5),
        )


def test_resolved_before_ack_rejected():
    with pytest.raises(ValueError, match="resolved_at"):
        _minimal_active(
            status=Status.RESOLVED,
            occurred_at=_ts(12, 0),
            acknowledged_at=_ts(12, 10),
            resolved_at=_ts(12, 5),
        )


def test_naive_occurred_at_rejected():
    naive = datetime(2026, 5, 13, 12, 0, 0)
    with pytest.raises(ValueError, match="timezone-aware"):
        _minimal_active(occurred_at=naive)


def test_empty_id_rejected():
    with pytest.raises(ValueError, match="id"):
        _minimal_active(id="")


def test_source_too_long_rejected():
    with pytest.raises(ValueError, match="source"):
        _minimal_active(source="x" * 65)


def test_title_too_long_rejected():
    with pytest.raises(ValueError, match="title"):
        _minimal_active(title="X" * 49)


def test_message_too_long_rejected():
    with pytest.raises(ValueError, match="message"):
        _minimal_active(message="x" * 241)


# ──────────────────────────────────────────────────────────────────────────
#  AlarmListFilters — validation
# ──────────────────────────────────────────────────────────────────────────

def test_default_filter_targets_active_only():
    f = AlarmListFilters()
    assert f.statuses == frozenset({Status.ACTIVE})
    assert f.severities is None
    assert f.sources is None
    assert f.since is None
    assert f.limit == LIMIT_DEFAULT


def test_filter_with_explicit_status_subset():
    f = AlarmListFilters(statuses=frozenset({Status.ACTIVE, Status.ACKNOWLEDGED}))
    assert Status.RESOLVED not in f.statuses


def test_filter_limit_below_min_rejected():
    with pytest.raises(ValueError, match="limit"):
        AlarmListFilters(limit=0)


def test_filter_limit_above_max_rejected():
    with pytest.raises(ValueError, match="limit"):
        AlarmListFilters(limit=LIMIT_MAX + 1)


def test_filter_empty_statuses_rejected():
    with pytest.raises(ValueError, match="statuses"):
        AlarmListFilters(statuses=frozenset())


def test_filter_empty_severities_set_rejected():
    with pytest.raises(ValueError, match="severities"):
        AlarmListFilters(severities=frozenset())


def test_filter_empty_sources_tuple_rejected():
    with pytest.raises(ValueError, match="sources"):
        AlarmListFilters(sources=())


def test_filter_naive_since_rejected():
    naive = datetime(2026, 5, 13, 0, 0, 0)
    with pytest.raises(ValueError, match="since"):
        AlarmListFilters(since=naive)


def test_filter_limit_min_accepted():
    f = AlarmListFilters(limit=LIMIT_MIN)
    assert f.limit == LIMIT_MIN


def test_filter_limit_max_accepted():
    f = AlarmListFilters(limit=LIMIT_MAX)
    assert f.limit == LIMIT_MAX


def test_filter_since_in_future_accepted():
    """No upper bound on `since` — domain doesn't care about clock skew."""
    f = AlarmListFilters(since=_ts(23, 59) + timedelta(days=1))
    assert f.since is not None
