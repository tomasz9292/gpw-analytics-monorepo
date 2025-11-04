from __future__ import annotations

import importlib
from pathlib import Path

_BACKEND_API = importlib.import_module("backend.api")

# Ensure submodules like ``api.ohlc_progress`` resolve to the backend implementation.
__path__ = [str(Path(__file__).resolve().parent.parent / "backend" / "api")]

__all__ = getattr(_BACKEND_API, "__all__", [])

def __getattr__(name: str):
    return getattr(_BACKEND_API, name)
