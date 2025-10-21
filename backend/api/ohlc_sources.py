"""Additional OHLC data sources and a multi-source harvester for GPW tickers."""

from __future__ import annotations

import csv
import io
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from pydantic import BaseModel, PrivateAttr

from .company_ingestion import HttpRequestLog, SimpleHttpSession, _normalize_gpw_symbol
from .stooq_ohlc import OhlcRow, OhlcSyncResult, ProgressCallback
from .symbols import ALIASES_RAW_TO_WA


class OhlcSource(BaseModel):
    """Base class for OHLC data sources."""

    name: str

    def reset(self) -> None:  # pragma: no cover - default implementation
        """Clears any cached state before a new synchronization run."""

    def fetch_history(self, symbol: str) -> List[OhlcRow]:
        raise NotImplementedError

    def get_request_log(self) -> List[HttpRequestLog]:  # pragma: no cover - optional
        return []


class StooqOhlcSource(OhlcSource):
    """Adapter exposing :class:`StooqOhlcHarvester` as a generic source."""

    name: str = "stooq"
    _harvester: Any = PrivateAttr()

    def __init__(self, harvester: Optional[Any] = None) -> None:
        super().__init__()
        from .stooq_ohlc import StooqOhlcHarvester

        if harvester is None:
            harvester = StooqOhlcHarvester()
        self._harvester = harvester

    def reset(self) -> None:  # pragma: no cover - behaviour depends on HTTP client
        session = getattr(self._harvester, "session", None)
        if session and hasattr(session, "clear_history"):
            session.clear_history()

    def fetch_history(self, symbol: str) -> List[OhlcRow]:
        return self._harvester.fetch_history(symbol)

    def get_request_log(self) -> List[HttpRequestLog]:  # pragma: no cover - depends on HTTP client
        session = getattr(self._harvester, "session", None)
        if session and hasattr(session, "get_history"):
            history = session.get_history()
            return list(history)
        return []


class YahooFinanceOhlcSource(OhlcSource):
    """Downloads historical prices from Yahoo Finance."""

    name: str = "yahoo_finance"
    download_url_template: str = (
        "https://query1.finance.yahoo.com/v7/finance/download/{symbol}"
    )

    def __init__(self, session: Optional[Any] = None) -> None:
        super().__init__()
        if session is None:
            session = SimpleHttpSession(
                headers={
                    "Accept": "text/csv, */*;q=0.8",
                    "Referer": "https://finance.yahoo.com/",
                }
            )
            if hasattr(session, "headers"):
                # Yahoo finance does not require the custom GPW header.
                session.headers.pop("X-Requested-With", None)
        self._session = session

    def reset(self) -> None:  # pragma: no cover - behaviour depends on HTTP client
        if hasattr(self._session, "clear_history"):
            self._session.clear_history()

    def _resolve_symbol(self, symbol: str) -> str:
        alias = ALIASES_RAW_TO_WA.get(symbol)
        if alias:
            return alias
        if symbol.endswith(".WA"):
            return symbol
        return f"{symbol}.WA"

    def fetch_history(self, symbol: str) -> List[OhlcRow]:
        resolved = self._resolve_symbol(symbol)
        params = {
            "period1": "0",
            "period2": str(int(datetime.now(timezone.utc).timestamp())),
            "interval": "1d",
            "events": "history",
            "includeAdjustedClose": "true",
        }
        response = self._session.get(
            self.download_url_template.format(symbol=resolved),
            params=params,
        )
        if hasattr(response, "raise_for_status"):
            response.raise_for_status()
        text = response.text()
        if not text.strip():
            raise RuntimeError("Brak danych notowań z Yahoo Finance")

        rows: List[OhlcRow] = []
        reader = csv.DictReader(io.StringIO(text))
        for raw in reader:
            raw_date = (raw.get("Date") or "").strip()
            if not raw_date or raw_date.lower() == "date":
                continue
            try:
                parsed_date = date.fromisoformat(raw_date)
            except ValueError:
                continue
            open_price = _parse_float(raw.get("Open"))
            high_price = _parse_float(raw.get("High"))
            low_price = _parse_float(raw.get("Low"))
            close_price = _parse_float(raw.get("Close"))
            volume_value = _parse_float(raw.get("Volume"))
            if None in (open_price, high_price, low_price, close_price):
                continue
            rows.append(
                OhlcRow(
                    symbol=_normalize_gpw_symbol(symbol),
                    date=parsed_date,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume_value,
                )
            )
        return rows

    def get_request_log(self) -> List[HttpRequestLog]:  # pragma: no cover - depends on HTTP client
        if hasattr(self._session, "get_history"):
            history = self._session.get_history()
            return list(history)
        return []


def _merge_rows(preferred: OhlcRow, alternative: OhlcRow) -> OhlcRow:
    """Merges two OHLC rows preferring data from the primary source."""

    updates: Dict[str, Any] = {}
    for field in ("open", "high", "low", "close", "volume"):
        preferred_value = getattr(preferred, field)
        alternative_value = getattr(alternative, field)
        if preferred_value is None and alternative_value is not None:
            updates[field] = alternative_value
    if not updates:
        return preferred
    return preferred.model_copy(update=updates)


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned or cleaned in {"-", "null", "None"}:
            return None
        cleaned = cleaned.replace(" ", "").replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


class MultiSourceOhlcHarvester:
    """Combines multiple OHLC sources with deduplication and data enrichment."""

    def __init__(self, sources: Optional[Sequence[OhlcSource]] = None) -> None:
        self.sources: List[OhlcSource] = list(sources) if sources else [
            StooqOhlcSource(),
            YahooFinanceOhlcSource(),
        ]
        if not self.sources:
            raise ValueError("At least one OHLC source is required")

    def sync(
        self,
        *,
        ch_client: Any,
        table_name: str,
        symbols: Sequence[str],
        start_date: Optional[date] = None,
        truncate: bool = False,
        run_as_admin: bool = False,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> OhlcSyncResult:
        for source in self.sources:
            source.reset()

        started_at = datetime.utcnow()
        inserted = 0
        skipped = 0
        processed = 0
        errors: List[str] = []
        truncated = False
        total = len(symbols)

        def emit_progress(current_symbol: Optional[str] = None) -> None:
            if not progress_callback:
                return
            progress_callback(
                {
                    "processed": processed,
                    "total": total,
                    "inserted": inserted,
                    "skipped": skipped,
                    "errors": list(errors),
                    "current_symbol": current_symbol,
                }
            )

        if truncate:
            try:
                ch_client.command(f"TRUNCATE TABLE {table_name}")
                truncated = True
            except Exception as exc:  # pragma: no cover - depends on DB configuration
                errors.append(f"Nie udało się wyczyścić tabeli {table_name}: {exc}")
            finally:
                emit_progress()

        emit_progress()

        for raw_symbol in symbols:
            processed += 1
            symbol_errors: List[str] = []
            try:
                normalized_symbol = _normalize_gpw_symbol(raw_symbol)
            except Exception as exc:
                symbol_errors.append(str(exc))
                errors.extend(symbol_errors)
                emit_progress(current_symbol=str(raw_symbol))
                continue

            combined: Dict[date, OhlcRow] = {}
            for source in self.sources:
                try:
                    history = source.fetch_history(normalized_symbol)
                except Exception as exc:
                    symbol_errors.append(f"{normalized_symbol} [{source.name}]: {exc}")
                    continue
                for row in history:
                    canonical_row = OhlcRow(
                        symbol=normalized_symbol,
                        date=row.date,
                        open=row.open,
                        high=row.high,
                        low=row.low,
                        close=row.close,
                        volume=row.volume,
                    )
                    existing = combined.get(row.date)
                    if existing is None:
                        combined[row.date] = canonical_row
                    else:
                        combined[row.date] = _merge_rows(existing, canonical_row)

            if symbol_errors:
                errors.extend(symbol_errors)

            relevant_rows = [
                row
                for row in sorted(combined.values(), key=lambda item: item.date)
                if start_date is None or row.date >= start_date
            ]

            if not relevant_rows:
                skipped += 1
                emit_progress(current_symbol=normalized_symbol)
                continue

            payload = [
                [
                    row.symbol,
                    row.date,
                    row.open,
                    row.high,
                    row.low,
                    row.close,
                    row.volume,
                ]
                for row in relevant_rows
            ]

            try:
                ch_client.insert(
                    table=table_name,
                    data=payload,
                    column_names=[
                        "symbol",
                        "date",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                    ],
                )
            except Exception as exc:  # pragma: no cover - depends on DB configuration
                errors.append(f"{normalized_symbol}: nie udało się zapisać danych: {exc}")
                skipped += 1
                emit_progress(current_symbol=normalized_symbol)
                continue

            inserted += len(relevant_rows)
            emit_progress(current_symbol=normalized_symbol)

        finished_at = datetime.utcnow()
        request_log: List[HttpRequestLog] = []
        for source in self.sources:
            for entry in source.get_request_log():
                request_log.append(entry.model_copy(update={"source": source.name}))

        request_log.sort(key=lambda entry: entry.started_at)

        emit_progress()

        return OhlcSyncResult(
            symbols=processed,
            inserted=inserted,
            skipped=skipped,
            errors=errors,
            started_at=started_at,
            finished_at=finished_at,
            truncated=truncated,
            request_log=request_log,
            requested_as_admin=run_as_admin,
        )


__all__ = [
    "MultiSourceOhlcHarvester",
    "OhlcSource",
    "StooqOhlcSource",
    "YahooFinanceOhlcSource",
]

