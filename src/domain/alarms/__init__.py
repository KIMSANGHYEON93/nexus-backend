"""Operator-alarm domain package — pure models + repository protocol.

The router never reaches into this package's submodules to look at the
storage backend; it only sees the `AlarmRepository` protocol and the
`Alarm`/`Severity`/`Status` shapes. The in-memory implementation lives in
`src/infrastructure/alarms/` so this package stays free of any persistence
or framework imports (FastAPI / asyncpg / httpx forbidden).
"""

from .models import Alarm, Severity, Status
from .repository import AlarmListFilters, AlarmRepository

__all__ = [
    "Alarm",
    "AlarmListFilters",
    "AlarmRepository",
    "Severity",
    "Status",
]
