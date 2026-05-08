"""Structured JSON logging for NEXUS OS backend.

Design choices:
  • Stdlib `logging` only — no extra runtime deps. structlog is great but
    overkill for our shape; one custom Formatter + one Filter does the job.
  • request_id flows through `contextvars.ContextVar`, so any coroutine in
    the same task chain (route handler, repository, KIS adapter, anomaly
    detector) sees the same value without it being threaded through args.
  • The formatter pulls the ContextVar at format-time via a Filter, so the
    value is fresh even for log records emitted from background tasks
    spawned mid-request.

Module name caveat: this file is `src.core.logging`, not stdlib `logging`.
Inside this file `import logging` resolves to stdlib (Python 3 absolute
imports). Other modules use `from .logging import ...` and never collide.
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


# ──────────────────────────────────────────────────────────────────────────
#  Context propagation
# ──────────────────────────────────────────────────────────────────────────

REQUEST_ID_HEADER = "X-Request-ID"

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-",
)


# ──────────────────────────────────────────────────────────────────────────
#  Filter that injects request_id onto every record
# ──────────────────────────────────────────────────────────────────────────

class RequestIdFilter(logging.Filter):
    """Read the current request_id ContextVar and stamp it on the record.

    A Filter is preferred over reading the ContextVar inside the Formatter
    so the request_id becomes a first-class LogRecord attribute — accessible
    to any other Handler / Formatter the operator may add later.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        return True


# ──────────────────────────────────────────────────────────────────────────
#  JSON formatter
# ──────────────────────────────────────────────────────────────────────────

# Standard LogRecord attributes — anything in `record.__dict__` not in this
# set is treated as caller-supplied `extra={}` and merged into the payload.
_RESERVED_ATTRS: frozenset[str] = frozenset({
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "message", "asctime", "taskName",
})


class JsonFormatter(logging.Formatter):
    """Render LogRecords as single-line JSON.

    Always emits: timestamp (UTC ISO-8601, ms precision), level, logger,
    message, request_id. Caller-supplied `extra={...}` fields are merged in
    after the standard fields and never overwrite them. Exception traces
    are formatted into `exc_info` as a single string so log shippers can
    treat one log record as one line.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc)
                .isoformat(timespec="milliseconds"),
            "level":      record.levelname,
            "logger":     record.name,
            "message":    record.getMessage(),
            "request_id": getattr(record, "request_id", "-"),
        }

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack_info"] = record.stack_info

        # Merge `extra={...}` (anything non-reserved on the record).
        for key, value in record.__dict__.items():
            if key in _RESERVED_ATTRS or key in payload or key.startswith("_"):
                continue
            if key == "request_id":
                continue
            payload[key] = value

        # `default=str` is the safety net for Decimal, datetime, UUID, etc.
        return json.dumps(payload, ensure_ascii=False, default=str)


# ──────────────────────────────────────────────────────────────────────────
#  Public configure entrypoint
# ──────────────────────────────────────────────────────────────────────────

def configure_logging(level: str = "INFO") -> None:
    """Install JSON formatter + RequestIdFilter on the root logger.

    Idempotent — safe to call repeatedly (e.g. on hot-reload). Removes any
    handlers already attached so we don't double-emit. Uvicorn's own
    loggers are coerced to propagate to root so their access lines also
    flow through the JSON pipeline.
    """
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(RequestIdFilter())
    root.addHandler(handler)
    root.setLevel(level.upper())

    for noisy in ("uvicorn", "uvicorn.access", "uvicorn.error"):
        lg = logging.getLogger(noisy)
        # Drop uvicorn's bespoke handlers — we want everything to flow up.
        for h in list(lg.handlers):
            lg.removeHandler(h)
        lg.propagate = True
