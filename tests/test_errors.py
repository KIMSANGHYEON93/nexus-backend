"""Tests for the RFC 7807 `ProblemDetail` envelope.

The wire format is a contract relied on by the frontend Error Boundary.
A test failure here means a breaking change for `useMarketData` consumers.
"""

from __future__ import annotations

from src.core.errors import (
    PROBLEM_TYPE_INTERNAL,
    PROBLEM_TYPE_VALIDATION,
    ProblemDetail,
)


def test_minimum_fields_serialize():
    """Title + status are mandatory; everything else is optional."""
    p = ProblemDetail(title="Unauthorized", status=401, request_id="-")
    d = p.model_dump(exclude_none=True)
    assert d["title"] == "Unauthorized"
    assert d["status"] == 401
    assert d["request_id"] == "-"
    assert d["type"] == "about:blank"


def test_exclude_none_strips_optional_fields():
    """Null values must NOT appear on the wire — keeps payload small and
    avoids ambiguity between 'missing' and 'present-but-null' for clients."""
    p = ProblemDetail(title="X", status=400, request_id="r")
    d = p.model_dump(exclude_none=True)
    for absent in ("detail", "instance", "errors"):
        assert absent not in d


def test_full_401_envelope_shape():
    p = ProblemDetail(
        type="about:blank", title="Unauthorized", status=401,
        detail="Missing bearer token", instance="/v1/snapshot",
        request_id="trace-abc-123",
    )
    d = p.model_dump(exclude_none=True)
    assert d == {
        "type":       "about:blank",
        "title":      "Unauthorized",
        "status":     401,
        "detail":     "Missing bearer token",
        "instance":   "/v1/snapshot",
        "request_id": "trace-abc-123",
    }


def test_validation_envelope_carries_errors_array():
    errs = [
        {"loc": ["body", "email"], "msg": "invalid email", "type": "value_error.email"},
        {"loc": ["body", "age"],   "msg": "must be >= 0",  "type": "value_error.number.not_ge"},
    ]
    p = ProblemDetail(
        type=PROBLEM_TYPE_VALIDATION, title="Validation Error", status=422,
        detail="One or more request fields failed validation",
        instance="/v1/users", request_id="trace-xyz", errors=errs,
    )
    d = p.model_dump(exclude_none=True)
    assert d["type"] == PROBLEM_TYPE_VALIDATION
    assert d["status"] == 422
    assert d["errors"] == errs


def test_internal_envelope_uses_stable_type_uri():
    p = ProblemDetail(
        type=PROBLEM_TYPE_INTERNAL, title="Internal Server Error", status=500,
        detail="An unexpected error occurred. The incident has been logged.",
        instance="/v1/snapshot", request_id="trace-500",
    )
    d = p.model_dump(exclude_none=True)
    assert d["type"] == "https://nexus-os.local/problems/internal-error"
    assert d["status"] == 500


def test_extra_attributes_allowed_for_handler_extensions():
    """Custom handlers may attach context via Pydantic extra='allow'."""
    p = ProblemDetail(
        title="Conflict", status=409, request_id="r",
        # ConfigDict(extra='allow') accepts this through __init__ kwargs:
        retry_after_seconds=30,  # type: ignore[call-arg]
    )
    d = p.model_dump(exclude_none=True)
    assert d.get("retry_after_seconds") == 30
