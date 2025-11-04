"""Minimal stub of pandas used only to satisfy imports during testing."""
from __future__ import annotations

class _PandasUnavailable:
    def __init__(self, *args, **kwargs):
        raise RuntimeError("pandas is unavailable in this testing environment")


DataFrame = _PandasUnavailable
Series = _PandasUnavailable


def to_datetime(*args, **kwargs):  # pragma: no cover - defensive stub
    raise RuntimeError("pandas is unavailable in this testing environment")
