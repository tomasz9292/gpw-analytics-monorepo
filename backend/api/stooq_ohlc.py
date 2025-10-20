"""Utilities for downloading and importing OHLC data from Stooq."""

from __future__ import annotations

import csv
import io
import unicodedata
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Sequence
from typing import Literal, TypedDict

from pydantic import BaseModel, Field

from .company_ingestion import HttpRequestLog, SimpleHttpSession, _normalize_gpw_symbol
from .symbols import to_stooq_symbol

STOOQ_OHLC_DOWNLOAD_URL = "https://stooq.pl/q/d/l/?s={symbol}&i=d"


def _normalize_header(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    cleaned = []
    for ch in normalized:
        if ch.isalnum():
            cleaned.append(ch.lower())
        elif ch.isspace():
            cleaned.append(" ")
    return " ".join("".join(cleaned).split())


_HEADER_ALIASES = {
    "data": "date",
    "date": "date",
    "otwarcie": "open",
    "open": "open",
    "najwyzszy": "high",
    "high": "high",
    "najnizszy": "low",
    "low": "low",
    "zamkniecie": "close",
    "close": "close",
    "wolumen": "volume",
    "volume": "volume",
    "obrot": "volume",
    "obrot wartosc": "turnover",
    "obrót": "volume",
}

_REQUIRED_FIELDS = {"date", "open", "high", "low", "close"}


class OhlcRow(BaseModel):
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None


class OhlcSyncResult(BaseModel):
    symbols: int = Field(0, description="Liczba symboli przetworzonych")
    inserted: int = Field(0, description="Łączna liczba wierszy zapisanych do bazy")
    skipped: int = Field(0, description="Liczba symboli pominiętych z powodu braku danych")
    errors: List[str] = Field(default_factory=list)
    started_at: datetime
    finished_at: datetime
    truncated: bool = False
    request_log: List[HttpRequestLog] = Field(default_factory=list)
    requested_as_admin: bool = Field(
        False, description="Czy synchronizacja została uruchomiona w trybie administratora"
    )
    sync_type: Literal["historical_prices"] = Field(
        "historical_prices", description="Rodzaj przeprowadzonej synchronizacji"
    )


class OhlcSyncProgressEvent(TypedDict):
    processed: int
    total: int
    inserted: int
    skipped: int
    errors: List[str]
    current_symbol: Optional[str]


ProgressCallback = Callable[[OhlcSyncProgressEvent], None]


class StooqOhlcHarvester:
    """Downloads daily OHLC data for GPW tickers from Stooq."""

    def __init__(
        self,
        session: Optional[Any] = None,
        download_url_template: str = STOOQ_OHLC_DOWNLOAD_URL,
    ) -> None:
        if session is None:
            session = SimpleHttpSession(
                headers={
                    "Accept": "text/csv, text/plain, */*;q=0.8",
                    "Referer": "https://stooq.pl/",
                }
            )
            # Stooq zwraca błąd 403 dla części niestandardowych nagłówków.
            if hasattr(session, "headers"):
                session.headers.pop("X-Requested-With", None)
        self.session = session
        self.download_url_template = download_url_template

    def _build_url(self, symbol: str) -> str:
        stooq_symbol = to_stooq_symbol(symbol)
        return self.download_url_template.format(symbol=stooq_symbol.lower())

    @staticmethod
    def _detect_delimiter(text: str) -> str:
        for line in text.splitlines():
            cleaned = line.strip()
            if not cleaned or cleaned.startswith("#"):
                continue
            if ";" in cleaned:
                return ";"
            if "," in cleaned:
                return ","
        return ","

    @classmethod
    def _parse_csv(cls, text: str) -> List[Dict[str, Any]]:
        stream = io.StringIO(text)
        delimiter = cls._detect_delimiter(text)
        reader = csv.reader(stream, delimiter=delimiter)
        header: Optional[List[str]] = None
        for row in reader:
            if not row:
                continue
            joined = "".join(row).strip()
            if not joined or joined.startswith("#"):
                continue
            header = row
            break
        if not header:
            return []
        normalized = [_normalize_header(value) for value in header]
        column_map: Dict[str, int] = {}
        for idx, norm in enumerate(normalized):
            key = _HEADER_ALIASES.get(norm)
            if key and key not in column_map:
                column_map[key] = idx
        if not _REQUIRED_FIELDS.issubset(column_map):
            return []

        rows: Dict[date, Dict[str, Any]] = {}
        for row in reader:
            if not row:
                continue
            if header and len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            date_idx = column_map["date"]
            raw_date = row[date_idx].strip() if date_idx < len(row) else ""
            if not raw_date:
                continue
            try:
                dt = date.fromisoformat(raw_date)
            except ValueError:
                continue
            parsed_row: Dict[str, Any] = {"date": dt}
            for field in ["open", "high", "low", "close", "volume", "turnover"]:
                idx = column_map.get(field)
                if idx is None or idx >= len(row):
                    continue
                parsed_value = _parse_float(row[idx])
                if parsed_value is not None:
                    parsed_row[field] = parsed_value
            if not all(field in parsed_row for field in _REQUIRED_FIELDS - {"date"}):
                continue
            rows[dt] = parsed_row
        return [rows[key] for key in sorted(rows)]

    def fetch_history(self, symbol: str) -> List[OhlcRow]:
        url = self._build_url(symbol)
        response = self.session.get(url)
        status_code = getattr(response, "status_code", None)
        raise_for_status = getattr(response, "raise_for_status", None)
        if callable(raise_for_status):
            raise_for_status()
        elif isinstance(status_code, int) and status_code >= 400:
            raise RuntimeError(f"HTTP {status_code}")
        document = response.text()
        parsed = self._parse_csv(document)
        if not parsed:
            raw_bytes: Optional[bytes]
            content = getattr(response, "content", None)
            if isinstance(content, (bytes, bytearray)):
                raw_bytes = bytes(content)
            elif callable(content):  # pragma: no cover - depends on HTTP client implementation
                try:
                    possible_bytes = content()
                except TypeError:  # pragma: no cover - defensive, unexpected signature
                    possible_bytes = None
                raw_bytes = bytes(possible_bytes) if possible_bytes is not None else None
            else:
                raw_bytes = None

            if raw_bytes:
                for encoding in ("utf-8-sig", "cp1250", "iso-8859-2"):
                    try:
                        fallback_document = raw_bytes.decode(encoding)
                    except Exception:
                        continue
                    parsed = self._parse_csv(fallback_document)
                    if parsed:
                        document = fallback_document
                        break
        if not parsed:
            raise RuntimeError("Brak danych notowań ze Stooq")
        normalized_symbol = _normalize_gpw_symbol(symbol)
        rows: List[OhlcRow] = []
        for item in parsed:
            rows.append(
                OhlcRow(
                    symbol=normalized_symbol,
                    date=item["date"],
                    open=float(item["open"]),
                    high=float(item["high"]),
                    low=float(item["low"]),
                    close=float(item["close"]),
                    volume=float(item.get("volume")) if item.get("volume") is not None else None,
                )
            )
        return rows

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
        supports_history = hasattr(self.session, "clear_history") and hasattr(
            self.session, "get_history"
        )
        if supports_history:
            self.session.clear_history()

        started_at = datetime.utcnow()
        inserted = 0
        errors: List[str] = []
        skipped = 0
        processed = 0
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
            current_symbol: Optional[str] = str(raw_symbol)
            try:
                normalized_symbol = _normalize_gpw_symbol(raw_symbol)
                current_symbol = normalized_symbol
            except Exception as exc:
                errors.append(str(exc))
                emit_progress(current_symbol=current_symbol)
                continue
            try:
                history = self.fetch_history(normalized_symbol)
            except Exception as exc:
                errors.append(f"{normalized_symbol}: {exc}")
                skipped += 1
                emit_progress(current_symbol=normalized_symbol)
                continue

            filtered_rows = [
                row
                for row in history
                if start_date is None or row.date >= start_date
            ]
            if not filtered_rows:
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
                for row in filtered_rows
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
            inserted += len(filtered_rows)
            emit_progress(current_symbol=normalized_symbol)

        finished_at = datetime.utcnow()
        request_log: List[HttpRequestLog] = []
        if supports_history:
            request_log = self.session.get_history()

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


def _parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned or cleaned in {"-", ""}:
            return None
        cleaned = cleaned.replace(" ", "").replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


__all__ = [
    "OhlcRow",
    "OhlcSyncResult",
    "StooqOhlcHarvester",
    "STOOQ_OHLC_DOWNLOAD_URL",
    "OhlcSyncProgressEvent",
    "ProgressCallback",
]
