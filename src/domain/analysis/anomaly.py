"""Anomaly detection — pure functions over domain types.

Phase 4 ships a deterministic baseline (z-score over a rolling window).
Phase 5 will route the same inputs through n8n → Ollama for model-based
scoring. Keep this module side-effect-free so it can be swapped, A/B
tested, or shadow-evaluated against the LLM path without touching
infrastructure.
"""

from __future__ import annotations

from collections.abc import Sequence
from statistics import mean, pstdev

from ..market.models import Tick


WINDOW = 64           # rolling sample size — power of two for cache friendliness
NOISE_FLOOR = 1e-9    # avoid divide-by-zero on flat windows


def zscore_anomaly(ticks: Sequence[Tick]) -> float:
    """Return an anomaly score in [0, 1] for the latest tick.

    The score is `tanh(|z| / 3)` so a 3σ deviation maps to ~0.76 and a
    6σ event saturates near 1.0 — matching the frontend's anomaly bands.
    """
    if len(ticks) < 8:
        return 0.0

    window = ticks[-WINDOW:]
    prices = [float(t.price) for t in window]
    mu = mean(prices)
    sigma = pstdev(prices) or NOISE_FLOOR
    z = abs(prices[-1] - mu) / sigma

    import math
    return math.tanh(z / 3.0)
