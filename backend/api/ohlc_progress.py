from __future__ import annotations

import threading
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field
from typing_extensions import Literal

from .stooq_ohlc import OhlcSyncResult


class OhlcSyncProgress(BaseModel):
    status: Literal["idle", "running", "success", "error"] = "idle"
    total_symbols: int = 0
    processed_symbols: int = 0
    inserted_rows: int = 0
    skipped_symbols: int = 0
    current_symbol: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    message: Optional[str] = None
    errors: List[str] = Field(default_factory=list)
    requested_as_admin: bool = False


class OhlcSyncProgressTracker:
    def __init__(self) -> None:
        self._state = OhlcSyncProgress()
        self._lock = threading.Lock()

    def snapshot(self) -> OhlcSyncProgress:
        with self._lock:
            return OhlcSyncProgress(**self._state.model_dump())

    def start(self, *, total_symbols: int, requested_as_admin: bool) -> None:
        now = datetime.utcnow()
        with self._lock:
            self._state = OhlcSyncProgress(
                status="running",
                total_symbols=max(0, total_symbols),
                processed_symbols=0,
                inserted_rows=0,
                skipped_symbols=0,
                current_symbol=None,
                started_at=now,
                finished_at=None,
                message=None,
                errors=[],
                requested_as_admin=requested_as_admin,
            )

    def update(
        self,
        *,
        processed_symbols: int,
        inserted_rows: int,
        skipped_symbols: int,
        current_symbol: Optional[str],
        errors: List[str],
    ) -> None:
        with self._lock:
            if self._state.status != "running":
                return
            self._state.processed_symbols = max(0, processed_symbols)
            self._state.inserted_rows = max(0, inserted_rows)
            self._state.skipped_symbols = max(0, skipped_symbols)
            self._state.current_symbol = current_symbol
            self._state.errors = list(errors)

    def finish(self, result: OhlcSyncResult) -> None:
        with self._lock:
            self._state = OhlcSyncProgress(
                status="success",
                total_symbols=max(0, result.symbols),
                processed_symbols=max(0, result.symbols),
                inserted_rows=max(0, result.inserted),
                skipped_symbols=max(0, result.skipped),
                current_symbol=None,
                started_at=result.started_at,
                finished_at=result.finished_at,
                message="Synchronizacja zakończona pomyślnie.",
                errors=list(result.errors),
                requested_as_admin=result.requested_as_admin,
            )

    def fail(self, message: str, *, errors: Optional[List[str]] = None) -> None:
        now = datetime.utcnow()
        with self._lock:
            previous_start = self._state.started_at
            self._state = OhlcSyncProgress(
                status="error",
                total_symbols=self._state.total_symbols,
                processed_symbols=self._state.processed_symbols,
                inserted_rows=self._state.inserted_rows,
                skipped_symbols=self._state.skipped_symbols,
                current_symbol=None,
                started_at=previous_start or now,
                finished_at=now,
                message=message,
                errors=list(errors) if errors else self._state.errors,
                requested_as_admin=self._state.requested_as_admin,
            )

    def reset(self) -> None:
        with self._lock:
            self._state = OhlcSyncProgress()


__all__ = ["OhlcSyncProgress", "OhlcSyncProgressTracker"]
