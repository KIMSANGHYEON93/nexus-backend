"""RFC 7807 — Problem Details for HTTP APIs (`application/problem+json`).

Every non-2xx response from this service is rendered through a single
envelope so the frontend `Error Boundary` and any operator tooling can
parse them uniformly. The five RFC 7807 fields are present plus two
NEXUS extensions:

    type        — URI identifying the problem class (default `about:blank`)
    title       — short human-readable summary (always set)
    status      — HTTP status code (mirrors the response code)
    detail      — explanation specific to this occurrence (optional)
    instance    — URI for this specific occurrence (we use request.url.path)
    request_id  — correlation id pulled from the X-Request-ID ContextVar
    errors      — field-level validation diagnostics (only on 422)

`type` is intentionally a URI (even when it's a stable opaque string like
"https://nexus-os.local/problems/auth-failed") so a frontend can switch
on it without parsing English. Phase-5 will register real URIs that
return human docs when fetched.
"""

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


PROBLEM_MEDIA_TYPE = "application/problem+json"

# Stable problem-type URIs. Keep in sync with frontend error-handling
# switch statements; treat additions as a soft API contract change.
PROBLEM_TYPE_VALIDATION = "https://nexus-os.local/problems/validation-error"
PROBLEM_TYPE_AUTH       = "https://nexus-os.local/problems/auth-failed"
PROBLEM_TYPE_INTERNAL   = "https://nexus-os.local/problems/internal-error"


class ProblemDetail(BaseModel):
    """RFC 7807 problem object with NEXUS extensions."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    type:       str                = Field(default="about:blank")
    title:      str
    status:     int
    detail:     Optional[str]       = None
    instance:   Optional[str]       = None
    request_id: str                 = "-"
    errors:     Optional[list[dict[str, Any]]] = None
