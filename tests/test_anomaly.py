"""Tests for `domain/analysis/anomaly.zscore_anomaly`.

The function is the deterministic baseline that the LLM-based detector
will be A/B tested against later — pinning its output range and edge
cases here keeps the comparison fair across refactors.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.domain.analysis.anomaly import zscore_anomaly
from src.domain.market.models import Tick, TickSide

_BASE_TS = datetime(2026, 5, 9, 9, 0, 0, tzinfo=timezone.utc)


def _tick(price: float, ts_offset: int = 0) -> Tick:
    return Tick(
        symbol="005930",
        ts=_BASE_TS + timedelta(seconds=ts_offset),
        price=Decimal(str(price)),
        volume=100,
        side=TickSide.BUY,
    )


def test_returns_zero_for_too_few_samples():
    """The first 7 ticks must score 0 — the baseline window isn't filled yet."""
    for n in range(8):
        ticks = [_tick(100.0 + i * 0.01, ts_offset=i) for i in range(n)]
        assert zscore_anomaly(ticks) == 0.0


def test_flat_window_scores_near_zero():
    """A perfectly flat price stream should not produce phantom anomalies."""
    ticks = [_tick(100.0, ts_offset=i) for i in range(64)]
    score = zscore_anomaly(ticks)
    assert 0.0 <= score < 0.05


def test_score_is_bounded_in_unit_interval():
    """Even a 100σ outlier saturates around 1.0, never exceeds it."""
    ticks = [_tick(100.0, ts_offset=i) for i in range(63)]
    ticks.append(_tick(1_000_000.0, ts_offset=63))
    score = zscore_anomaly(ticks)
    assert 0.0 <= score <= 1.0


def test_three_sigma_lands_in_documented_band():
    """tanh(z/3) at z=3 ≈ 0.762 — pin the calibration so refactors can't
    silently shift the band the frontend reads."""
    import statistics
    base = [100.0 + i * 0.01 for i in range(63)]
    mu = statistics.mean(base)
    sigma = statistics.pstdev(base)
    spike = mu + 3 * sigma
    ticks = [_tick(p, ts_offset=i) for i, p in enumerate(base)]
    ticks.append(_tick(spike, ts_offset=63))
    score = zscore_anomaly(ticks)
    assert 0.70 < score < 0.80


def test_function_is_pure():
    """Calling repeatedly with the same input must return the same output —
    no hidden state, no time dependence."""
    ticks = [_tick(100.0 + (i % 5), ts_offset=i) for i in range(64)]
    s1 = zscore_anomaly(ticks)
    s2 = zscore_anomaly(ticks)
    assert s1 == s2
