"""Alarm repository contract — the domain side of the persistence seam.

The router speaks only to `AlarmRepository` (the Protocol below). The
concrete backend (in-memory today, possibly TimescaleDB or Redis Streams
tomorrow) lives in `src/infrastructure/alarms/`. Keeping the protocol in
domain lets us write pure unit tests against fakes that satisfy the
protocol without dragging in asyncpg.

This mirrors the `domain/market/repository.py` pattern (where the SQL
implementation is a concrete class) but uses a `Protocol` instead of a
concrete-class-as-interface — alarms have multiple plausible backends and
we want the domain to be explicit about the surface it depends on.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol, runtime_checkable

from .models import Alarm, Severity, Status


# Limits sourced from the spec (§2 query params): `limit` is 1..200.
LIMIT_MIN: int = 1
LIMIT_MAX: int = 200
LIMIT_DEFAULT: int = 50


@dataclass(frozen=True, slots=True)
class AlarmListFilters:
    """Filter set passed to `AlarmRepository.list`.

    All fields are optional / pre-clamped — the router validates query
    inputs, builds this dataclass, and hands it to the repo. The repo is
    free to assume the values are already sanitized:

      • `limit` is in [LIMIT_MIN, LIMIT_MAX] (clamped at construction).
      • `severities` / `sources`, if not None, are non-empty.
      • `statuses` is always non-empty (default `{Status.ACTIVE}` —
        the spec's `status` query defaults to "active").
      • `since` is timezone-aware (UTC) when set.

    These invariants are enforced in `__post_init__` so a malformed
    filter never reaches a backend.
    """

    statuses:   frozenset[Status]            = field(default_factory=lambda: frozenset({Status.ACTIVE}))
    severities: frozenset[Severity] | None   = None
    sources:    tuple[str, ...] | None       = None
    since:      datetime | None              = None
    limit:      int                          = LIMIT_DEFAULT

    def __post_init__(self) -> None:
        if not self.statuses:
            raise ValueError("AlarmListFilters.statuses must not be empty")
        if self.severities is not None and not self.severities:
            raise ValueError(
                "AlarmListFilters.severities, when set, must not be empty"
            )
        if self.sources is not None:
            if not self.sources:
                raise ValueError(
                    "AlarmListFilters.sources, when set, must not be empty"
                )
            for s in self.sources:
                if not s or len(s) > 64:
                    raise ValueError(
                        f"AlarmListFilters.sources entry length must be in [1, 64]: {s!r}"
                    )
        if self.limit < LIMIT_MIN or self.limit > LIMIT_MAX:
            raise ValueError(
                f"AlarmListFilters.limit {self.limit} out of allowed range "
                f"[{LIMIT_MIN}, {LIMIT_MAX}]"
            )
        if self.since is not None:
            if self.since.tzinfo is None or self.since.utcoffset() is None:
                raise ValueError(
                    "AlarmListFilters.since must be timezone-aware (UTC)"
                )


@runtime_checkable
class AlarmRepository(Protocol):
    """Read-side contract used by the v1 alarms router.

    The write-side (acknowledge / resolve / create) is out of scope for
    this spec — kept off the protocol so we don't lock in a shape before
    those endpoints are designed. The in-memory implementation may expose
    extra demo-seed helpers; they are not part of the contract.
    """

    async def list(
        self,
        filters: AlarmListFilters,
    ) -> tuple[list[Alarm], int]:
        """Return `(rows, total)` where:
          • `rows` is the page (newest first by `occurred_at`), already
            clamped to `filters.limit`.
          • `total` is the count of all rows matching the filter set,
            ignoring `limit`. Used to populate the HUD's counter.
        """

    async def count_active(self) -> int:
        """Global unacknowledged count — `status == active`, ignoring
        every filter. Powers the panel header's "{n} UNACK" badge,
        which by spec is independent of the visible page filters.
        """
