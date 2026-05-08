"""Tests for src/core/logging.py.

Covers the contract relied on by every other layer:
  • request_id_var defaults to '-' and is reset cleanly across requests.
  • JSON schema always includes timestamp, level, logger, message, request_id.
  • Caller-supplied `extra={...}` merges in without overwriting standard fields.
  • `exc_info` from logger.exception() lands as a single string.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging

import pytest

from src.core.logging import (
    JsonFormatter,
    RequestIdFilter,
    configure_logging,
    request_id_var,
)


def _capture_to_buffer() -> tuple[io.StringIO, logging.Logger]:
    """Install our JSON formatter on root, redirect to a StringIO."""
    buf = io.StringIO()
    configure_logging("DEBUG")
    root = logging.getLogger()
    # Replace stream of our just-installed handler with the buffer.
    for h in root.handlers:
        h.stream = buf
    return buf, logging.getLogger("nexus.test")


def test_default_request_id_is_dash():
    assert request_id_var.get() == "-"


def test_json_schema_minimum_keys_present():
    buf, log = _capture_to_buffer()
    log.info("hello")
    line = buf.getvalue().strip()
    rec = json.loads(line)
    for key in ("timestamp", "level", "logger", "message", "request_id"):
        assert key in rec, f"missing key {key!r}"
    assert rec["level"] == "INFO"
    assert rec["logger"] == "nexus.test"
    assert rec["message"] == "hello"
    assert rec["request_id"] == "-"


def test_extra_fields_merge_without_overwriting_standard():
    buf, log = _capture_to_buffer()
    log.warning("auth event",
                extra={"event": "auth_failed", "level": "OVERRIDE_ATTEMPT"})
    rec = json.loads(buf.getvalue().strip())
    # extra={} ships through
    assert rec["event"] == "auth_failed"
    # but reserved standard fields are NOT overwritten by malicious extras
    assert rec["level"] == "WARNING"


def test_exc_info_renders_as_string():
    buf, log = _capture_to_buffer()
    try:
        raise ValueError("boom")
    except ValueError:
        log.exception("caught it")
    rec = json.loads(buf.getvalue().strip())
    assert "exc_info" in rec
    assert isinstance(rec["exc_info"], str)
    assert "ValueError" in rec["exc_info"]
    assert "boom" in rec["exc_info"]


@pytest.mark.asyncio
async def test_request_id_propagates_into_child_tasks():
    buf, log = _capture_to_buffer()

    async def child():
        log.info("from child")

    token = request_id_var.set("rid-parent")
    try:
        await asyncio.create_task(child())
    finally:
        request_id_var.reset(token)

    rec = json.loads(buf.getvalue().strip().splitlines()[-1])
    assert rec["request_id"] == "rid-parent"
    # And after reset, the var is back to default
    assert request_id_var.get() == "-"


def test_request_id_filter_stamps_records():
    """Even outside the helper, the filter alone must inject the var."""
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(RequestIdFilter())
    log = logging.getLogger("nexus.filter-only")
    log.handlers = [handler]
    log.setLevel("DEBUG")
    log.propagate = False  # bypass root so we only see this record

    token = request_id_var.set("scoped")
    try:
        log.info("scoped message")
    finally:
        request_id_var.reset(token)

    rec = json.loads(buf.getvalue().strip())
    assert rec["request_id"] == "scoped"
