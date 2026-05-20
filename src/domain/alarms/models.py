"""Operator-alarm domain model — pure dataclasses + enums.

These are the **domain** shapes that flow through the alarm pipeline:
    (producer) → AlarmRepository → API → NEXUS OS HUD

The HTTP wire format (snake_case JSON) is rendered by the v1 DTO layer;
this module knows nothing about FastAPI or asyncpg. If the persistence
backend is ever swapped from in-process memory to Timescale or Redis
Streams, only `src/infrastructure/alarms/` changes — this file is stable.

Why a dataclass and not a Pydantic model: the rest of `src/domain/market/`
uses Pydantic for the boundary shapes, but the alarm domain has *invariants*
(status ↔ acknowledged_at/resolved_at + occurred ≤ acknowledged ≤ resolved)
that we want to enforce in one place, on every construction path. A
`__post_init__` validator on a frozen dataclass keeps the invariants in
the domain and out of the repository.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class Severity(str, Enum):
    """Operator-visible severity ladder, ascending priority.

    String enum so `Severity.ANOMALY.value == "anomaly"` lands as a plain
    string on the JSON wire without any custom serializer. Order matches
    the HUD glyph ladder (info < warn < anomaly < critical).
    """

    INFO     = "info"
    WARN     = "warn"
    ANOMALY  = "anomaly"
    CRITICAL = "critical"


class Status(str, Enum):
    """Alarm lifecycle — read-only on this surface (write-side TBD)."""

    ACTIVE       = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED     = "resolved"


# Field length / shape ceilings from the spec (§3 domain model). Keep them
# named constants so the repo and the tests can reference the same numbers
# without drifting.
_SOURCE_MAX_LEN  = 64
_CODE_MAX_LEN    = 64
_TITLE_MAX_LEN   = 48
_MESSAGE_MAX_LEN = 240


@dataclass(frozen=True, slots=True)
class Alarm:
    """One operator alarm, immutable once constructed.

    Invariants (enforced in `__post_init__`):
      • status==ACTIVE       ⇒ acknowledged_at is None AND resolved_at is None
      • status==ACKNOWLEDGED ⇒ acknowledged_at is not None AND resolved_at is None
      • status==RESOLVED     ⇒ resolved_at is not None
                              (acknowledged_at may stay None — the lifecycle
                              allows "active → resolved" with no explicit ack)
      • occurred_at ≤ acknowledged_at ≤ resolved_at (any present pair).

    `metadata` is optional source-specific extra context. The HUD renders
    its keys alpha-sorted in a mono grid; we keep it `dict[str, Any] | None`
    here because the schema is intentionally open at the domain edge.
    """

    id:              str
    severity:        Severity
    status:          Status
    source:          str
    code:            str
    title:           str
    message:         str
    occurred_at:     datetime
    entity_id:       str | None       = None
    acknowledged_at: datetime | None  = None
    resolved_at:     datetime | None  = None
    metadata:        dict[str, Any] | None = field(default=None)

    # ── Invariants ──────────────────────────────────────────────────────
    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("Alarm.id must be a non-empty string")
        _require_len("source",  self.source,  1, _SOURCE_MAX_LEN)
        _require_len("code",    self.code,    1, _CODE_MAX_LEN)
        _require_len("title",   self.title,   1, _TITLE_MAX_LEN)
        _require_len("message", self.message, 1, _MESSAGE_MAX_LEN)
        if self.entity_id is not None and not self.entity_id:
            raise ValueError("Alarm.entity_id, when set, must be non-empty")

        _require_aware("occurred_at",     self.occurred_at)
        if self.acknowledged_at is not None:
            _require_aware("acknowledged_at", self.acknowledged_at)
        if self.resolved_at is not None:
            _require_aware("resolved_at",     self.resolved_at)

        # Status ↔ timestamps coherence.
        if self.status is Status.ACTIVE:
            if self.acknowledged_at is not None or self.resolved_at is not None:
                raise ValueError(
                    "Alarm.status=='active' must have null acknowledged_at and resolved_at"
                )
        elif self.status is Status.ACKNOWLEDGED:
            if self.acknowledged_at is None:
                raise ValueError(
                    "Alarm.status=='acknowledged' requires acknowledged_at"
                )
            if self.resolved_at is not None:
                raise ValueError(
                    "Alarm.status=='acknowledged' must have null resolved_at"
                )
        elif self.status is Status.RESOLVED:
            if self.resolved_at is None:
                raise ValueError(
                    "Alarm.status=='resolved' requires resolved_at"
                )

        # Monotonic time order — occurred ≤ acknowledged ≤ resolved.
        if (
            self.acknowledged_at is not None
            and self.acknowledged_at < self.occurred_at
        ):
            raise ValueError("Alarm.acknowledged_at must be ≥ occurred_at")
        if self.resolved_at is not None:
            if self.resolved_at < self.occurred_at:
                raise ValueError("Alarm.resolved_at must be ≥ occurred_at")
            if (
                self.acknowledged_at is not None
                and self.resolved_at < self.acknowledged_at
            ):
                raise ValueError("Alarm.resolved_at must be ≥ acknowledged_at")


def _require_len(name: str, value: str, lo: int, hi: int) -> None:
    """Length-range guard — used by `Alarm.__post_init__` for string fields."""
    if not isinstance(value, str):
        raise TypeError(f"Alarm.{name} must be a string, got {type(value).__name__}")
    n = len(value)
    if n < lo or n > hi:
        raise ValueError(
            f"Alarm.{name} length {n} out of allowed range [{lo}, {hi}]"
        )


def _require_aware(name: str, value: datetime) -> None:
    """Reject naive datetimes — alarms are inherently global-clock events."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"Alarm.{name} must be timezone-aware (UTC)")
