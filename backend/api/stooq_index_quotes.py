"""Utilities for downloading GPW index history from Stooq."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Sequence

from .stooq_ohlc import OhlcRow, StooqOhlcHarvester


INDEX_SYMBOL_ALIASES: Dict[str, Sequence[str]] = {
    "MWIG40": ("MWIG40", "MW40"),
    "SWIG80": ("SWIG80", "SW80"),
    "WIG20TR": ("WIG20TR",),
    "MWIG40TR": ("MWIG40TR",),
    "SWIG80TR": ("SWIG80TR",),
}


@dataclass(frozen=True)
class IndexQuoteRow:
    """Single daily observation for a GPW index."""

    index_code: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None


class StooqIndexQuoteHarvester:
    """Adapter exposing index-focused access to Stooq OHLC data."""

    def __init__(self, ohlc_harvester: Optional[StooqOhlcHarvester] = None) -> None:
        self.ohlc_harvester = ohlc_harvester or StooqOhlcHarvester()

    def fetch_history(self, index_symbol: str) -> List[IndexQuoteRow]:
        canonical = self._normalize_index_code(index_symbol)
        lookup_candidates = INDEX_SYMBOL_ALIASES.get(canonical, (canonical,))
        selected_rows: Optional[List[OhlcRow]] = None
        last_error: Optional[Exception] = None

        for candidate in lookup_candidates:
            try:
                rows = self.ohlc_harvester.fetch_history(candidate)
            except Exception as exc:  # pragma: no cover - defensive, network errors mocked in tests
                last_error = exc
                continue

            if rows:
                selected_rows = rows
                break

            # Remember the result to return if all candidates are empty.
            if selected_rows is None:
                selected_rows = rows

        if selected_rows is None:
            if last_error is not None:
                raise last_error
            return []

        return [
            IndexQuoteRow(
                index_code=canonical,
                date=row.date,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
            )
            for row in rows
        ]

    @staticmethod
    def _normalize_index_code(symbol: str) -> str:
        cleaned = symbol.strip().upper()
        if cleaned.endswith(".WA"):
            cleaned = cleaned[:-3]
        return cleaned


__all__ = ["IndexQuoteRow", "StooqIndexQuoteHarvester", "INDEX_SYMBOL_ALIASES"]
