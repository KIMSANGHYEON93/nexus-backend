"""Re-export shim — `Settings` lives in `src/core/config.py`.

Tests and external callers that do `from src.infrastructure.settings import Settings`
will resolve correctly via this one-liner re-export.  The canonical import path
remains `from src.core.config import Settings`.
"""

from ..core.config import Settings  # noqa: F401

__all__ = ["Settings"]
