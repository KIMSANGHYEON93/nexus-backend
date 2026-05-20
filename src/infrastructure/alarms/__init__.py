"""Operator-alarm infrastructure — in-memory backend + seed data.

`InMemoryAlarmRepository` implements the `AlarmRepository` protocol from
`src.domain.alarms.repository`. The router only knows about the protocol,
so swapping this for a Timescale-backed adapter later is a one-line
change in the router's DI factory.

`build_seeded_repository()` returns a fully-seeded instance with one or
two alarms per source (`trading-coordinator`, `kis-publisher`,
`news-provider`, `system-monitor`) — sized so the HUD has something to
render in dev without us needing to wire a producer pipeline first.
"""

from .in_memory_repo import (
    InMemoryAlarmRepository,
    build_seeded_repository,
    seed_demo_alarms,
)

__all__ = [
    "InMemoryAlarmRepository",
    "build_seeded_repository",
    "seed_demo_alarms",
]
