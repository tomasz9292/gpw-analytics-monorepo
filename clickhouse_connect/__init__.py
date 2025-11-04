"""Stub clickhouse_connect client for offline tests."""
from __future__ import annotations

from typing import Any

__all__ = ["get_client"]


def get_client(*args: Any, **kwargs: Any) -> Any:
    raise RuntimeError("clickhouse_connect client is not available in this test environment")
