# api/main.py
from __future__ import annotations

import csv
import io
import json
import os
import re
import statistics
import textwrap
import unicodedata
import zipfile
from datetime import date, datetime, timedelta, timezone
from math import isfinite, sqrt
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypedDict,
    Union,
)
from typing import Literal, cast
from uuid import uuid4

from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from bisect import bisect_right

import clickhouse_connect
import threading
from decimal import Decimal
from collections import OrderedDict
from fastapi import APIRouter, Depends, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.params import Query as QueryParam
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from .company_ingestion import (
    CompanyDataHarvester,
    CompanySyncProgress,
    CompanySyncResult,
    _normalize_gpw_symbol,
)
from .ohlc_progress import OhlcSyncProgress, OhlcSyncProgressTracker
from .ohlc_sources import MultiSourceOhlcHarvester
from .sector_classification_data import GPW_SECTOR_CLASSIFICATION
from .stooq_ohlc import OhlcSyncProgressEvent, OhlcSyncResult, _parse_float
from .symbols import (
    ALIASES_RAW_TO_WA,
    DEFAULT_OHLC_SYNC_SYMBOLS,
    normalize_input_symbol,
    pretty_symbol,
)
from .windows_agent import router as windows_agent_router

# =========================
# Konfiguracja / połączenie
# =========================

TABLE_OHLC = os.getenv("TABLE_OHLC", "ohlc")
TABLE_COMPANIES = os.getenv("TABLE_COMPANIES", "companies")
TABLE_INDEX_PORTFOLIOS = os.getenv("TABLE_INDEX_PORTFOLIOS", "index_portfolios")
TABLE_INDEX_HISTORY = os.getenv("TABLE_INDEX_HISTORY", "index_history")

DEFAULT_COMPANIES_TABLE_DDL = textwrap.dedent(
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE_COMPANIES} (
        symbol String,
        ticker String,
        code String,
        symbol_gpw LowCardinality(String),
        symbol_gpw_benchmark LowCardinality(Nullable(String)),
        symbol_stooq LowCardinality(Nullable(String)),
        symbol_yahoo LowCardinality(Nullable(String)),
        symbol_google LowCardinality(Nullable(String)),
        isin LowCardinality(Nullable(String)),
        name LowCardinality(Nullable(String)),
        company_name LowCardinality(Nullable(String)),
        full_name LowCardinality(Nullable(String)),
        short_name LowCardinality(Nullable(String)),
        sector LowCardinality(Nullable(String)),
        industry LowCardinality(Nullable(String)),
        country LowCardinality(Nullable(String)),
        headquarters LowCardinality(Nullable(String)),
        city LowCardinality(Nullable(String)),
        website LowCardinality(Nullable(String)),
        url LowCardinality(Nullable(String)),
        description LowCardinality(Nullable(String)),
        profile LowCardinality(Nullable(String)),
        logo LowCardinality(Nullable(String)),
        logo_url LowCardinality(Nullable(String)),
        image_url LowCardinality(Nullable(String)),
        employees Nullable(Int32),
        employee_count Nullable(Int32),
        founded Nullable(Int32),
        founded_year Nullable(Int32),
        established Nullable(Int32),
        listing_date LowCardinality(Nullable(String)),
        ipo_date LowCardinality(Nullable(String)),
        market_cap Nullable(Float64),
        shares_outstanding Nullable(Float64),
        book_value Nullable(Float64),
        revenue_ttm Nullable(Float64),
        net_income_ttm Nullable(Float64),
        ebitda_ttm Nullable(Float64),
        eps Nullable(Float64),
        pe_ratio Nullable(Float64),
        pb_ratio Nullable(Float64),
        dividend_yield Nullable(Float64),
        debt_to_equity Nullable(Float64),
        roe Nullable(Float64),
        roa Nullable(Float64),
        gross_margin Nullable(Float64),
        operating_margin Nullable(Float64),
        profit_margin Nullable(Float64),
        raw_payload String
    )
    ENGINE = MergeTree()
    ORDER BY symbol
    """
)

DEFAULT_OHLC_TABLE_DDL = textwrap.dedent(
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE_OHLC} (
        symbol LowCardinality(String),
        date Date,
        open Nullable(Float64),
        high Nullable(Float64),
        low Nullable(Float64),
        close Nullable(Float64),
        volume Nullable(Float64)
    )
    ENGINE = MergeTree()
    ORDER BY (symbol, date)
    """
)

DEFAULT_INDEX_PORTFOLIOS_DDL = textwrap.dedent(
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE_INDEX_PORTFOLIOS} (
        index_code LowCardinality(String),
        index_name Nullable(String),
        effective_date Date,
        symbol LowCardinality(String),
        symbol_base LowCardinality(String),
        company_name Nullable(String),
        weight Nullable(Float64),
        source LowCardinality(Nullable(String))
    )
    ENGINE = MergeTree()
    ORDER BY (index_code, effective_date, symbol)
    """
)

DEFAULT_INDEX_HISTORY_DDL = textwrap.dedent(
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE_INDEX_HISTORY} (
        index_code LowCardinality(String),
        index_name Nullable(String),
        date Date,
        value Nullable(Float64),
        change_pct Nullable(Float64),
        source LowCardinality(Nullable(String))
    )
    ENGINE = MergeTree()
    ORDER BY (index_code, date)
    """
)


def _ensure_index_tables(ch_client) -> None:
    ch_client.command(DEFAULT_INDEX_PORTFOLIOS_DDL)
    ch_client.command(
        f"ALTER TABLE {TABLE_INDEX_PORTFOLIOS} ADD COLUMN IF NOT EXISTS symbol_base LowCardinality(String) AFTER symbol"
    )
    ch_client.command(DEFAULT_INDEX_HISTORY_DDL)

_OHLC_IMPORT_REQUIRED_COLUMNS = ("symbol", "date", "open", "high", "low", "close")
_OHLC_IMPORT_OPTIONAL_COLUMNS = ("volume",)
_MAX_OHLC_IMPORT_ERRORS = 50

_MST_HEADER_ALIASES = {
    "data": "date",
    "date": "date",
    "dtyyyymmdd": "date",
    "czas": "time",
    "symbol": "symbol",
    "ticker": "symbol",
    "kod": "symbol",
    "spolka": "symbol",
    "hisopen": "open",
    "otwarcie": "open",
    "kurs_otwarcia": "open",
    "otw": "open",
    "open": "open",
    "hishigh": "high",
    "najwyzszy": "high",
    "kurs_max": "high",
    "max": "high",
    "high": "high",
    "hislow": "low",
    "najnizszy": "low",
    "kurs_min": "low",
    "min": "low",
    "low": "low",
    "hisclose": "close",
    "zamkniecie": "close",
    "kurs_zamkniecia": "close",
    "zamkn": "close",
    "close": "close",
    "vol": "volume",
    "wolumen": "volume",
    "wol": "volume",
    "wolumen_obrotu": "volume",
    "volume": "volume",
    "obrot": "turnover",
    "obrot_wartosciowy": "turnover",
    "obrotwartosciowy": "turnover",
}

_MST_REQUIRED_FIELDS = {"date", "open", "high", "low", "close"}

_MST_DEFAULT_COLUMN_ORDER = {
    "date": 0,
    "open": 1,
    "high": 2,
    "low": 3,
    "close": 4,
    "volume": 5,
}

_MST_DEFAULT_COLUMN_ORDER_WITH_SYMBOL = {
    "symbol": 0,
    "date": 1,
    "open": 2,
    "high": 3,
    "low": 4,
    "close": 5,
    "volume": 6,
}

ALLOWED_SCORE_METRICS = {"total_return", "volatility", "max_drawdown", "sharpe", "price_change"}


SHAREHOLDER_KEYWORDS = [
    "akcjonariat",
    "akcjonariusz",
    "akcjonariusze",
    "akcjon",
    "shareholder",
    "shareholders",
    "shareholderstructure",
    "ownership",
    "owner",
]

SHAREHOLDER_NAME_KEYWORDS = [
    "name",
    "akcjon",
    "shareholder",
    "holder",
    "entity",
    "podmiot",
]

SHAREHOLDER_STAKE_KEYWORDS = [
    "udz",
    "udzial",
    "udział",
    "stake",
    "share",
    "percent",
    "procent",
    "percentage",
    "pakiet",
]

COMPANY_SIZE_KEYWORDS = [
    "wielkosc",
    "wielkość",
    "companysize",
    "size",
    "capitalisation",
    "capitalization",
    "classification",
]

RAW_FACT_CANDIDATES: List[Dict[str, Iterable[str]]] = [
    {"label": "Segment", "keywords": ["segment"]},
    {"label": "Rynek", "keywords": ["market", "rynek"]},
    {"label": "Free float", "keywords": ["freefloat", "free float"]},
    {"label": "Kapitał zakładowy", "keywords": ["kapital", "capital", "sharecapital"]},
    {
        "label": "Liczba akcji",
        "keywords": ["liczbaakcji", "numberofshares", "sharesnumber", "sharescount"],
    },
]

INDEX_MEMBERSHIP_KEYWORDS = [
    "index",
    "indeks",
    "indexes",
    "indices",
    "indexmembership",
]


def _normalize_key(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value or "")
    cleaned = []
    for ch in normalized:
        if ch.isalnum():
            cleaned.append(ch.lower())
        elif ch.isspace():
            cleaned.append(" ")
        else:
            cleaned.append(" ")
    normalized_str = "".join(cleaned)
    return " ".join(normalized_str.split())


def _prettify_key(raw_key: str) -> str:
    normalized = unicodedata.normalize("NFD", raw_key or "")
    cleaned = re.sub(r"[_\s]+", " ", normalized)
    cleaned = re.sub(r"[\u0300-\u036f]", "", cleaned)
    cleaned = cleaned.strip()
    if not cleaned:
        return ""
    parts = [part.capitalize() for part in cleaned.split(" ") if part]
    return " ".join(parts)


def _normalize_import_column(raw_name: str) -> str:
    normalized = unicodedata.normalize("NFKD", raw_name or "")
    cleaned = []
    for char in normalized:
        if char.isalnum():
            cleaned.append(char.lower())
        elif char in {" ", "_", "-"}:
            cleaned.append("_")
    joined = "".join(cleaned)
    while "__" in joined:
        joined = joined.replace("__", "_")
    return joined.strip("_")


def _decode_uploaded_text(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1250", "iso-8859-2"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("Nie udało się zdekodować pliku. Użyj kodowania UTF-8.")


def _parse_ohlc_csv_payload(
    content: bytes,
) -> Tuple[List[List[Any]], int, List[str], int]:
    decoded = _decode_uploaded_text(content)
    reader = csv.DictReader(io.StringIO(decoded))
    if not reader.fieldnames:
        raise ValueError("Plik nie zawiera nagłówka.")

    column_map: Dict[str, str] = {}
    for raw_header in reader.fieldnames:
        normalized = _normalize_import_column(raw_header)
        if not normalized:
            continue
        column_map.setdefault(normalized, raw_header)

    missing = [name for name in _OHLC_IMPORT_REQUIRED_COLUMNS if name not in column_map]
    if missing:
        required = ", ".join(_OHLC_IMPORT_REQUIRED_COLUMNS)
        optional = ", ".join(_OHLC_IMPORT_OPTIONAL_COLUMNS)
        raise ValueError(
            "Niepoprawny nagłówek pliku. Wymagane kolumny: "
            f"{required}. Opcjonalnie: {optional}."
        )

    payload: List[List[Any]] = []
    skipped = 0
    errors: List[str] = []
    total_errors = 0
    seen: set[tuple[str, date]] = set()

    def register_error(message: str) -> None:
        nonlocal total_errors
        total_errors += 1
        if len(errors) < _MAX_OHLC_IMPORT_ERRORS:
            errors.append(message)

    for index, row in enumerate(reader, start=2):
        raw_symbol = (row.get(column_map["symbol"]) or "").strip()
        if not raw_symbol:
            skipped += 1
            register_error(f"Wiersz {index}: brak symbolu spółki.")
            continue
        try:
            symbol = _normalize_gpw_symbol(raw_symbol)
        except Exception as exc:
            skipped += 1
            register_error(f"Wiersz {index}: {exc}")
            continue

        raw_date = (row.get(column_map["date"]) or "").strip()
        if not raw_date:
            skipped += 1
            register_error(f"Wiersz {index}: brak daty notowania.")
            continue
        try:
            parsed_date = date.fromisoformat(raw_date)
        except ValueError:
            skipped += 1
            register_error(f"Wiersz {index}: niepoprawny format daty ({raw_date}).")
            continue

        open_value = _parse_float(row.get(column_map["open"]))
        high_value = _parse_float(row.get(column_map["high"]))
        low_value = _parse_float(row.get(column_map["low"]))
        close_value = _parse_float(row.get(column_map["close"]))
        missing_values = [
            name
            for name, value in (
                ("open", open_value),
                ("high", high_value),
                ("low", low_value),
                ("close", close_value),
            )
            if value is None
        ]
        if missing_values:
            skipped += 1
            missing_label = ", ".join(sorted(set(missing_values)))
            register_error(f"Wiersz {index}: brak danych w kolumnach: {missing_label}.")
            continue

        volume_value = None
        if "volume" in column_map:
            volume_value = _parse_float(row.get(column_map["volume"]))

        key = (symbol, parsed_date)
        if key in seen:
            skipped += 1
            register_error(
                f"Wiersz {index}: zduplikowany rekord {symbol} {parsed_date.isoformat()}."
            )
            continue
        seen.add(key)

        payload.append(
            [symbol, parsed_date, open_value, high_value, low_value, close_value, volume_value]
        )

    return payload, skipped, errors, total_errors


def _detect_mst_delimiter(text: str) -> str:
    for line in text.splitlines():
        cleaned = line.strip()
        if not cleaned or cleaned.startswith("#"):
            continue
        count_semicolons = cleaned.count(";")
        count_commas = cleaned.count(",")
        if count_semicolons >= 4:
            return ";"
        if count_commas >= 4:
            return ","
    return ";"


def _derive_symbol_from_mst_filename(filename: str) -> Optional[str]:
    base = os.path.basename(filename)
    stem, _ = os.path.splitext(base)
    candidate = stem.strip()
    if not candidate:
        return None

    lowered = candidate.lower()
    for suffix in ("_d", "_w", "_m", "_q", "_y"):
        if lowered.endswith(suffix):
            candidate = candidate[: -len(suffix)]
            break

    candidate = re.sub(r"[^0-9a-zA-Z]", "", candidate)
    if not candidate:
        return None

    return _resolve_import_symbol(candidate)


def _resolve_import_symbol(value: str) -> Optional[str]:
    normalized_input = normalize_input_symbol(value)
    if not normalized_input:
        return None

    alias = ALIASES_RAW_TO_WA.get(normalized_input)
    if alias:
        base = alias.split(".", 1)[0].strip()
        if base:
            try:
                return _normalize_gpw_symbol(base)
            except Exception:
                return None

    try:
        ticker = _normalize_gpw_symbol(normalized_input)
    except Exception:
        return None

    if not ticker.isalnum():
        return None

    return ticker


def _parse_mst_date(value: str) -> Optional[date]:
    cleaned = re.sub(r"[^0-9]", "", value or "")
    if len(cleaned) == 8:
        try:
            return date(int(cleaned[0:4]), int(cleaned[4:6]), int(cleaned[6:8]))
        except ValueError:
            return None
    if len(cleaned) == 6:
        try:
            year = int(cleaned[0:2])
            year += 2000 if year < 70 else 1900
            return date(year, int(cleaned[2:4]), int(cleaned[4:6]))
        except ValueError:
            return None
    return None


def _build_mst_column_map(row: Sequence[str]) -> Dict[str, int]:
    normalized = [_normalize_import_column(value) for value in row]
    column_map: Dict[str, int] = {}
    for idx, norm in enumerate(normalized):
        key = _MST_HEADER_ALIASES.get(norm)
        if key and key not in column_map:
            column_map[key] = idx
    if _MST_REQUIRED_FIELDS.issubset(column_map):
        return column_map
    return {}


def _parse_mst_file(
    content: bytes,
    *,
    default_symbol: Optional[str],
    display_name: str,
) -> Tuple[List[List[Any]], int, List[str], int]:
    text = _decode_uploaded_text(content)
    delimiter = _detect_mst_delimiter(text)
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)

    column_map: Dict[str, int] = {}
    payload: List[List[Any]] = []
    errors: List[str] = []
    total_errors = 0
    skipped = 0

    def register_error(message: str) -> None:
        nonlocal total_errors
        total_errors += 1
        if len(errors) < _MAX_OHLC_IMPORT_ERRORS:
            errors.append(f"{display_name}: {message}")

    has_header = False

    for index, row in enumerate(reader, start=1):
        if not row or not any(cell.strip() for cell in row):
            continue
        if not column_map:
            possible_map = _build_mst_column_map(row)
            if possible_map:
                column_map = possible_map
                has_header = True
                if "symbol" not in column_map and not default_symbol:
                    raise ValueError(
                        "brak kolumny z symbolem i nie udało się rozpoznać go z nazwy pliku"
                    )
                continue
            first_cell = row[0].strip() if row else ""
            if _parse_mst_date(first_cell) is not None:
                column_map = dict(_MST_DEFAULT_COLUMN_ORDER)
                if not default_symbol:
                    raise ValueError(
                        "nie udało się rozpoznać symbolu spółki z nazwy pliku"
                    )
            else:
                column_map = dict(_MST_DEFAULT_COLUMN_ORDER_WITH_SYMBOL)

        row_index = index

        max_index = max(column_map.values(), default=-1)
        if len(row) <= max_index:
            row = list(row) + [""] * (max_index + 1 - len(row))

        symbol_value: Optional[str] = None
        symbol_idx = column_map.get("symbol")
        if symbol_idx is not None and symbol_idx < len(row):
            symbol_value = _resolve_import_symbol(row[symbol_idx])
        if not symbol_value:
            symbol_value = default_symbol
        if not symbol_value:
            skipped += 1
            register_error(f"wiersz {row_index}: brak symbolu spółki.")
            continue

        date_idx = column_map.get("date")
        raw_date = row[date_idx].strip() if date_idx is not None and date_idx < len(row) else ""
        if not raw_date:
            skipped += 1
            register_error(f"wiersz {row_index}: brak daty notowania.")
            continue
        parsed_date = _parse_mst_date(raw_date)
        if parsed_date is None:
            skipped += 1
            register_error(
                f"wiersz {row_index}: niepoprawny format daty ({raw_date})."
            )
            continue

        numeric_values: Dict[str, Optional[float]] = {}
        missing_fields: List[str] = []
        for field in ["open", "high", "low", "close"]:
            idx = column_map.get(field)
            value = _parse_float(row[idx]) if idx is not None and idx < len(row) else None
            if value is None:
                missing_fields.append(field)
            numeric_values[field] = value
        if missing_fields:
            skipped += 1
            missing_label = ", ".join(sorted(set(missing_fields)))
            register_error(f"wiersz {row_index}: brak danych w kolumnach: {missing_label}.")
            continue

        volume_idx = column_map.get("volume")
        volume_value = (
            _parse_float(row[volume_idx])
            if volume_idx is not None and volume_idx < len(row)
            else None
        )

        payload.append(
            [
                symbol_value,
                parsed_date,
                numeric_values["open"],
                numeric_values["high"],
                numeric_values["low"],
                numeric_values["close"],
                volume_value,
            ]
        )

    if not has_header and not payload:
        # When there was no header and we couldn't parse any rows, report a clearer error.
        raise ValueError("Nie udało się odczytać danych z pliku MST.")

    return payload, skipped, errors, total_errors


def _parse_mst_archive_payload(
    content: bytes,
) -> Tuple[List[List[Any]], int, List[str], int]:
    try:
        archive = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile as exc:
        raise ValueError("Nie udało się odczytać archiwum ZIP.") from exc

    payload: List[List[Any]] = []
    skipped = 0
    errors: List[str] = []
    total_errors = 0
    seen: set[tuple[str, date]] = set()

    try:
        members = [
            info
            for info in archive.infolist()
            if not info.is_dir() and info.filename.lower().endswith(".mst")
        ]
        if not members:
            raise ValueError("Archiwum nie zawiera plików .MST z notowaniami.")

        for info in members:
            display_name = os.path.basename(info.filename) or info.filename
            default_symbol = _derive_symbol_from_mst_filename(display_name)
            try:
                with archive.open(info) as handle:
                    file_payload, file_skipped, file_errors, file_total_errors = _parse_mst_file(
                        handle.read(),
                        default_symbol=default_symbol,
                        display_name=display_name,
                    )
            except ValueError as exc:
                total_errors += 1
                if len(errors) < _MAX_OHLC_IMPORT_ERRORS:
                    errors.append(f"{display_name}: {exc}")
                skipped += 1
                continue

            total_errors += file_total_errors
            skipped += file_skipped
            for message in file_errors:
                if len(errors) < _MAX_OHLC_IMPORT_ERRORS:
                    errors.append(message)

            for row in file_payload:
                symbol_value, parsed_date = row[0], row[1]
                key = (symbol_value, parsed_date)
                if key in seen:
                    skipped += 1
                    continue
                seen.add(key)
                payload.append(row)
    finally:
        archive.close()

    return payload, skipped, errors, total_errors


def parse_mst_archive(content: bytes) -> Tuple[List[List[Any]], int, List[str], int]:
    """Parse a ZIP archive with Stooq MST files into normalized OHLC rows.

    This is a thin wrapper around the internal :func:`_parse_mst_archive_payload`
    helper that exposes MST parsing capabilities to other modules (for example,
    the desktop analytics agent) without duplicating the parsing logic.
    """

    return _parse_mst_archive_payload(content)


def _deduplicate_strings(values: Iterable[str], limit: Optional[int] = None) -> List[str]:
    seen: set[str] = set()
    output: List[str] = []
    for value in values:
        cleaned = re.sub(r"\s+", " ", value).strip()
        if not cleaned:
            continue
        normalized = cleaned.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        output.append(cleaned)
        if limit is not None and len(output) >= limit:
            break
    return output


def _collect_values_by_key_keywords(
    value: Any, keywords: Sequence[str], limit: Optional[int] = None
) -> List[Any]:
    if not isinstance(value, (dict, list)):
        return []

    normalized_keywords = [_normalize_key(keyword) for keyword in keywords]
    results: List[Any] = []
    stack: List[Tuple[Optional[str], Any]] = [(None, value)]

    while stack:
        current_key, current_value = stack.pop()
        if isinstance(current_value, dict):
            for child_key, child_value in current_value.items():
                normalized_key = _normalize_key(str(child_key))
                if any(keyword in normalized_key for keyword in normalized_keywords):
                    results.append(child_value)
                    if limit is not None and len(results) >= limit:
                        return results
                stack.append((child_key, child_value))
        elif isinstance(current_value, list):
            for item in current_value:
                stack.append((current_key, item))

    return results


def _split_shareholding_string(value: str) -> List[str]:
    without_html = re.sub(r"<br\s*/?>", "\n", value, flags=re.IGNORECASE)
    without_html = re.sub(r"<[^>]+>", " ", without_html)
    parts = re.split(r"[\n\r;•●▪·\u2022\u2023\u25CF\u25A0]+", without_html)
    cleaned: List[str] = []
    for part in parts:
        stripped = re.sub(r"^[\s•·\-–—\u2022\u2023\u25CF\u25A0]+", "", part)
        stripped = re.sub(r"\s+", " ", stripped).strip()
        if stripped:
            cleaned.append(stripped)
    return cleaned


def _flatten_shareholding_value(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return _split_shareholding_string(value)
    if isinstance(value, (int, float)):
        return [str(value)]
    if isinstance(value, bool):
        return ["Tak" if value else "Nie"]
    if isinstance(value, list):
        flattened: List[str] = []
        for item in value:
            flattened.extend(_flatten_shareholding_value(item))
        return flattened
    if isinstance(value, dict):
        name_parts: List[str] = []
        stake_parts: List[str] = []
        other_parts: List[str] = []

        for raw_key, child in value.items():
            key = _normalize_key(str(raw_key))
            child_values = _flatten_shareholding_value(child)
            if not child_values:
                continue
            if any(keyword in key for keyword in SHAREHOLDER_NAME_KEYWORDS):
                name_parts.extend(child_values)
                continue
            if any(keyword in key for keyword in SHAREHOLDER_STAKE_KEYWORDS):
                stake_parts.extend(child_values)
                continue
            label = _prettify_key(str(raw_key))
            other_parts.append(
                f"{label}: {', '.join(child_values)}".strip()
                if label
                else ", ".join(child_values)
            )

        combined: List[str] = []
        name_joined = re.sub(r"\s+", " ", " ".join(name_parts)).strip()
        stake_joined = re.sub(r"\s+", " ", " ".join(stake_parts)).strip()
        if name_joined or stake_joined:
            pieces = [part for part in [name_joined, stake_joined] if part]
            combined.append(" – ".join(pieces))
        combined.extend(part for part in other_parts if part)

        if not combined:
            fallback: List[str] = []
            for child in value.values():
                fallback.extend(_flatten_shareholding_value(child))
            fallback = [re.sub(r"\s+", " ", item).strip() for item in fallback if item]
            if fallback:
                combined.append(", ".join(fallback))
        return combined

    return []


def _flatten_generic_value(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        cleaned = re.sub(r"<br\s*/?>", " ", value, flags=re.IGNORECASE)
        cleaned = re.sub(r"<[^>]+>", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return [cleaned] if cleaned else []
    if isinstance(value, (int, float)):
        return [str(value)]
    if isinstance(value, bool):
        return ["Tak" if value else "Nie"]
    if isinstance(value, list):
        flattened: List[str] = []
        for item in value:
            flattened.extend(_flatten_generic_value(item))
        return _deduplicate_strings(flattened)
    if isinstance(value, dict):
        entries: List[str] = []
        for raw_key, child in value.items():
            child_values = _flatten_generic_value(child)
            if not child_values:
                continue
            label = _prettify_key(str(raw_key))
            if not label:
                entries.extend(child_values)
            elif len(child_values) == 1:
                entries.append(f"{label}: {child_values[0]}")
            else:
                entries.append(f"{label}: {', '.join(child_values)}")
        return entries
    return []


def _extract_stooq_insights(raw_payload: Any) -> Dict[str, Any]:
    payload: Any
    if isinstance(raw_payload, str):
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            return {}
    else:
        payload = raw_payload

    if not isinstance(payload, dict):
        return {}

    shareholding_values = _collect_values_by_key_keywords(payload, SHAREHOLDER_KEYWORDS)
    shareholding = _deduplicate_strings(
        [item for value in shareholding_values for item in _flatten_shareholding_value(value)],
        limit=20,
    )

    company_size_matches = _collect_values_by_key_keywords(
        payload, COMPANY_SIZE_KEYWORDS, limit=1
    )
    company_size_candidates = (
        _flatten_generic_value(company_size_matches[0]) if company_size_matches else []
    )
    company_size = company_size_candidates[0] if company_size_candidates else None

    facts: List[Dict[str, str]] = []
    for candidate in RAW_FACT_CANDIDATES:
        matches = _collect_values_by_key_keywords(payload, candidate["keywords"], limit=1)
        if not matches:
            continue
        flattened = _flatten_generic_value(matches[0])
        value = next((entry for entry in flattened if entry), None)
        if not value:
            continue
        facts.append({"label": str(candidate["label"]), "value": value})

    deduped_facts: List[Dict[str, str]] = []
    seen_facts: set[str] = set()
    for fact in facts:
        key = f"{fact['label']}|{fact['value']}".casefold()
        if key in seen_facts:
            continue
        seen_facts.add(key)
        deduped_facts.append(fact)

    index_values = _collect_values_by_key_keywords(payload, INDEX_MEMBERSHIP_KEYWORDS)
    index_entries = _deduplicate_strings(
        [item for value in index_values for item in _flatten_generic_value(value)],
        limit=20,
    )

    return {
        "shareholding": shareholding,
        "company_size": company_size,
        "facts": deduped_facts,
        "indices": index_entries,
    }


def _env_bool(name: str, default: bool = False) -> bool:
    """Pomocniczo odczytuje wartości bool z env."""

    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


# Wariant 1 – pełny URL (np. https://abc123.eu-west-1.aws.clickhouse.cloud:8443)
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "").strip()

# Wariant 2 – oddzielne pola. Działają także razem z URL, ale
# mogą nadpisywać wartości (np. inny user/hasło niż w URL).
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "").strip()
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "").strip()
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default").strip()
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default").strip()
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "").strip()

# Flagi TLS/SSL – przydają się na Render/Cloud.
CLICKHOUSE_SECURE = _env_bool("CLICKHOUSE_SECURE", default=True)
CLICKHOUSE_VERIFY = _env_bool("CLICKHOUSE_VERIFY", default=True)
CLICKHOUSE_CA = os.getenv("CLICKHOUSE_CA", "").strip()  # ścieżka do dodatkowego certyfikatu, opcjonalna

_INITIAL_CLICKHOUSE_SETTINGS = {
    "CLICKHOUSE_URL": CLICKHOUSE_URL,
    "CLICKHOUSE_HOST": CLICKHOUSE_HOST,
    "CLICKHOUSE_PORT": CLICKHOUSE_PORT,
    "CLICKHOUSE_DATABASE": CLICKHOUSE_DATABASE,
    "CLICKHOUSE_USER": CLICKHOUSE_USER,
    "CLICKHOUSE_PASSWORD": CLICKHOUSE_PASSWORD,
    "CLICKHOUSE_SECURE": CLICKHOUSE_SECURE,
    "CLICKHOUSE_VERIFY": CLICKHOUSE_VERIFY,
    "CLICKHOUSE_CA": CLICKHOUSE_CA,
}

_INITIAL_CLICKHOUSE_MODE: Literal["url", "manual"] = (
    "url" if CLICKHOUSE_URL else "manual"
)
_CLICKHOUSE_CONFIG_SOURCE: Literal["env", "override"] = "env"
_CLICKHOUSE_CONFIG_MODE: Literal["url", "manual"] = _INITIAL_CLICKHOUSE_MODE

# CORS – domyślnie pozwalamy wszystkim, ale można podać np. domenę z Vercel.
_cors_origins = os.getenv("CORS_ALLOW_ORIGINS", "*").strip()
if _cors_origins == "*":
    CORS_ALLOW_ORIGINS: List[str] = ["*"]
else:
    CORS_ALLOW_ORIGINS = [origin.strip() for origin in _cors_origins.split(",") if origin.strip()]


# Cache konfiguracji klienta ClickHouse + klienci per wątek
_CH_CLIENT_KWARGS = None
_CH_CLIENT_LOCK = threading.Lock()
_THREAD_LOCAL = threading.local()
_SYNC_LOCK = threading.Lock()
_SYNC_THREAD: Optional[threading.Thread] = None


class CompanySyncScheduleStatus(BaseModel):
    mode: Literal["idle", "once", "recurring"] = Field(
        default="idle", description="Tryb harmonogramu synchronizacji spółek"
    )
    next_run_at: Optional[datetime] = Field(
        default=None, description="Najbliższy zaplanowany termin synchronizacji"
    )
    recurring_interval_minutes: Optional[int] = Field(
        default=None, description="Interwał (w minutach) między kolejnymi synchronizacjami cyklicznymi"
    )
    recurring_start_at: Optional[datetime] = Field(
        default=None, description="Moment uruchomienia harmonogramu cyklicznego"
    )
    last_run_started_at: Optional[datetime] = Field(
        default=None, description="Czas uruchomienia ostatniej synchronizacji z harmonogramu"
    )
    last_run_finished_at: Optional[datetime] = Field(
        default=None, description="Czas zakończenia ostatniej synchronizacji z harmonogramu"
    )
    last_run_status: Literal["idle", "running", "success", "failed"] = Field(
        default="idle", description="Status ostatniej synchronizacji uruchomionej przez harmonogram"
    )


class CompanySyncScheduleRequest(BaseModel):
    mode: Literal["once", "recurring", "cancel"]
    scheduled_for: Optional[datetime] = Field(
        default=None,
        description="Data i czas jednorazowej synchronizacji (tryb once)",
    )
    interval_minutes: Optional[int] = Field(
        default=None,
        ge=5,
        le=7 * 24 * 60,
        description="Interwał w minutach między synchronizacjami cyklicznymi",
    )
    start_at: Optional[datetime] = Field(
        default=None,
        description="Początek harmonogramu cyklicznego (domyślnie natychmiast)",
    )

    @model_validator(mode="after")
    def validate_payload(self):  # type: ignore[override]
        if self.mode == "once":
            if not self.scheduled_for:
                raise ValueError("Należy podać datę jednorazowej synchronizacji")
        if self.mode == "recurring":
            if not self.interval_minutes:
                raise ValueError("Należy określić interwał dla synchronizacji cyklicznej")
        return self


_SCHEDULE_LOCK = threading.Lock()
_SCHEDULE_EVENT = threading.Event()
_SCHEDULE_THREAD: Optional[threading.Thread] = None
_SYNC_SCHEDULE_STATE = CompanySyncScheduleStatus()


class OhlcSyncScheduleStatus(BaseModel):
    mode: Literal["idle", "once", "recurring"] = Field(
        default="idle", description="Tryb harmonogramu synchronizacji notowań",
    )
    next_run_at: Optional[datetime] = Field(
        default=None, description="Najbliższy zaplanowany termin synchronizacji notowań",
    )
    recurring_interval_minutes: Optional[int] = Field(
        default=None, description="Interwał w minutach między kolejnymi synchronizacjami",
    )
    recurring_start_at: Optional[datetime] = Field(
        default=None, description="Planowany start harmonogramu cyklicznego",
    )
    last_run_started_at: Optional[datetime] = Field(
        default=None, description="Czas rozpoczęcia ostatniej synchronizacji z harmonogramu",
    )
    last_run_finished_at: Optional[datetime] = Field(
        default=None, description="Czas zakończenia ostatniej synchronizacji z harmonogramu",
    )
    last_run_status: Literal["idle", "running", "success", "failed"] = Field(
        default="idle", description="Status ostatniej synchronizacji z harmonogramu",
    )
    options: Optional["OhlcSyncRequest"] = Field(
        default=None,
        description="Parametry synchronizacji notowań używane przez harmonogram",
    )


class OhlcSyncScheduleRequest(BaseModel):
    mode: Literal["once", "recurring", "cancel"]
    scheduled_for: Optional[datetime] = Field(
        default=None,
        description="Data i czas jednorazowej synchronizacji notowań",
    )
    interval_minutes: Optional[int] = Field(
        default=None,
        ge=5,
        le=7 * 24 * 60,
        description="Interwał w minutach między synchronizacjami cyklicznymi",
    )
    start_at: Optional[datetime] = Field(
        default=None,
        description="Początek harmonogramu cyklicznego",
    )
    options: Optional["OhlcSyncRequest"] = Field(
        default=None,
        description="Parametry synchronizacji notowań wykonywanej przez harmonogram",
    )

    @model_validator(mode="after")
    def validate_payload(self):  # type: ignore[override]
        if self.mode == "once":
            if not self.scheduled_for:
                raise ValueError("Należy podać termin synchronizacji jednorazowej")
            if not self.options:
                raise ValueError("Należy określić parametry synchronizacji notowań")
        if self.mode == "recurring":
            if not self.interval_minutes:
                raise ValueError("Należy określić interwał synchronizacji cyklicznej")
            if not self.options:
                raise ValueError("Należy określić parametry synchronizacji notowań")
        return self


_OHLC_SCHEDULE_LOCK = threading.Lock()
_OHLC_SCHEDULE_EVENT = threading.Event()
_OHLC_SCHEDULE_THREAD: Optional[threading.Thread] = None
_OHLC_SCHEDULE_STATE: "OhlcSyncScheduleStatus"


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _snapshot_schedule_state() -> CompanySyncScheduleStatus:
    with _SCHEDULE_LOCK:
        return _SYNC_SCHEDULE_STATE.model_copy(deep=True)


def _notify_schedule_loop() -> None:
    _SCHEDULE_EVENT.set()


def _ensure_schedule_thread_running() -> None:
    global _SCHEDULE_THREAD
    if _SCHEDULE_THREAD and _SCHEDULE_THREAD.is_alive():
        return

    def _loop_wrapper() -> None:
        while True:
            with _SCHEDULE_LOCK:
                next_run = _SYNC_SCHEDULE_STATE.next_run_at

            if next_run is None:
                _SCHEDULE_EVENT.wait()
                _SCHEDULE_EVENT.clear()
                continue

            now = datetime.utcnow()
            wait_seconds = (next_run - now).total_seconds()
            if wait_seconds > 0:
                triggered = _SCHEDULE_EVENT.wait(timeout=min(wait_seconds, 60.0))
                if triggered:
                    _SCHEDULE_EVENT.clear()
                    continue

            started = _check_and_run_scheduled_job()
            if not started:
                _SCHEDULE_EVENT.wait(timeout=5.0)
                _SCHEDULE_EVENT.clear()

    _SCHEDULE_THREAD = threading.Thread(target=_loop_wrapper, daemon=True)
    _SCHEDULE_THREAD.start()


def _snapshot_ohlc_schedule_state() -> OhlcSyncScheduleStatus:
    with _OHLC_SCHEDULE_LOCK:
        return _OHLC_SCHEDULE_STATE.model_copy(deep=True)


def _notify_ohlc_schedule_loop() -> None:
    _OHLC_SCHEDULE_EVENT.set()


def _ensure_ohlc_schedule_thread_running() -> None:
    global _OHLC_SCHEDULE_THREAD
    if _OHLC_SCHEDULE_THREAD and _OHLC_SCHEDULE_THREAD.is_alive():
        return

    def _loop_wrapper() -> None:
        while True:
            with _OHLC_SCHEDULE_LOCK:
                next_run = _OHLC_SCHEDULE_STATE.next_run_at

            if next_run is None:
                _OHLC_SCHEDULE_EVENT.wait()
                _OHLC_SCHEDULE_EVENT.clear()
                continue

            now = datetime.utcnow()
            wait_seconds = (next_run - now).total_seconds()
            if wait_seconds > 0:
                triggered = _OHLC_SCHEDULE_EVENT.wait(timeout=min(wait_seconds, 60.0))
                if triggered:
                    _OHLC_SCHEDULE_EVENT.clear()
                    continue

            started = _check_and_run_ohlc_scheduled_job()
            if not started:
                _OHLC_SCHEDULE_EVENT.wait(timeout=5.0)
                _OHLC_SCHEDULE_EVENT.clear()

    _OHLC_SCHEDULE_THREAD = threading.Thread(target=_loop_wrapper, daemon=True)
    _OHLC_SCHEDULE_THREAD.start()


def _str_to_bool(value: str, default: bool) -> bool:
    low = value.strip().lower()
    if low in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if low in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _parse_clickhouse_url():
    """Zwraca słownik parametrów wyciągniętych z CLICKHOUSE_URL."""

    if not CLICKHOUSE_URL:
        return None

    u = urlparse(CLICKHOUSE_URL)
    query = parse_qs(u.query)

    def _query_last(*names: str) -> Optional[str]:
        for n in names:
            if n in query and query[n]:
                return query[n][-1]
        return None

    if u.scheme in {"http", "https"}:
        secure_default = u.scheme == "https"
    elif u.scheme in {"clickhouse", "clickhouses"}:
        secure_default = u.scheme == "clickhouses"
    else:
        raise RuntimeError(
            "CLICKHOUSE_URL must start with http(s):// or clickhouse(s)://, got: "
            f"{CLICKHOUSE_URL}"
        )

    host = u.hostname or ""
    if not host:
        raise RuntimeError("CLICKHOUSE_URL musi zawierać hosta")

    port_default = 8443 if secure_default else 8123
    port = u.port or port_default

    secure = secure_default
    secure_q = _query_last("secure", "ssl")
    if secure_q is not None:
        secure = _str_to_bool(secure_q, secure_default)

    verify = None
    verify_q = _query_last("verify", "check")
    if verify_q is not None:
        verify = _str_to_bool(verify_q, secure)

    username = u.username or _query_last("username", "user")
    password = u.password or _query_last("password", "pass")

    database = None
    if u.path and u.path != "/":
        database = u.path.lstrip("/")
    database_q = _query_last("database", "db")
    if database_q:
        database = database_q

    return {
        "host": host,
        "port": port,
        "secure": secure,
        "verify": verify,
        "username": username,
        "password": password,
        "database": database,
    }


def _mask_clickhouse_url(url: str) -> str:
    parsed = urlparse(url)
    username = parsed.username or ""
    password = parsed.password or ""
    hostname = parsed.hostname or ""
    port = parsed.port

    if username:
        userinfo = username
        if password:
            userinfo = f"{userinfo}:***"
        netloc = f"{userinfo}@{hostname}"
    else:
        netloc = hostname
    if port:
        netloc = f"{netloc}:{port}"

    sanitized = parsed._replace(netloc=netloc)
    if password:
        sanitized = sanitized._replace(path=parsed.path or "/")
    if parsed.query:
        query_params = parse_qs(parsed.query, keep_blank_values=True)
        masked_items = []
        for key, values in query_params.items():
            lowered = key.lower()
            if lowered in {"password", "pass"}:
                masked_items.append((key, "***"))
            else:
                for value in values:
                    masked_items.append((key, value))
        sanitized = sanitized._replace(query=urlencode(masked_items, doseq=True))
    return urlunparse(sanitized)


def _snapshot_clickhouse_settings() -> Dict[str, Any]:
    return {
        "CLICKHOUSE_URL": CLICKHOUSE_URL,
        "CLICKHOUSE_HOST": CLICKHOUSE_HOST,
        "CLICKHOUSE_PORT": CLICKHOUSE_PORT,
        "CLICKHOUSE_DATABASE": CLICKHOUSE_DATABASE,
        "CLICKHOUSE_USER": CLICKHOUSE_USER,
        "CLICKHOUSE_PASSWORD": CLICKHOUSE_PASSWORD,
        "CLICKHOUSE_SECURE": CLICKHOUSE_SECURE,
        "CLICKHOUSE_VERIFY": CLICKHOUSE_VERIFY,
        "CLICKHOUSE_CA": CLICKHOUSE_CA,
        "source": _CLICKHOUSE_CONFIG_SOURCE,
        "mode": _CLICKHOUSE_CONFIG_MODE,
    }


def _apply_clickhouse_settings(
    settings: Dict[str, Any],
    *,
    source: Literal["env", "override"],
    mode: Literal["url", "manual"],
) -> None:
    global CLICKHOUSE_URL, CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DATABASE
    global CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_SECURE, CLICKHOUSE_VERIFY
    global CLICKHOUSE_CA, _CLICKHOUSE_CONFIG_SOURCE, _CLICKHOUSE_CONFIG_MODE

    CLICKHOUSE_URL = str(settings.get("CLICKHOUSE_URL", "") or "").strip()
    CLICKHOUSE_HOST = str(settings.get("CLICKHOUSE_HOST", "") or "").strip()
    CLICKHOUSE_PORT = str(settings.get("CLICKHOUSE_PORT", "") or "").strip()
    CLICKHOUSE_DATABASE = str(
        settings.get("CLICKHOUSE_DATABASE", "default") or "default"
    ).strip()
    CLICKHOUSE_USER = str(settings.get("CLICKHOUSE_USER", "default") or "default").strip()
    CLICKHOUSE_PASSWORD = str(settings.get("CLICKHOUSE_PASSWORD", "") or "")
    CLICKHOUSE_SECURE = bool(settings.get("CLICKHOUSE_SECURE", True))
    CLICKHOUSE_VERIFY = bool(settings.get("CLICKHOUSE_VERIFY", True))
    CLICKHOUSE_CA = str(settings.get("CLICKHOUSE_CA", "") or "").strip()
    _CLICKHOUSE_CONFIG_SOURCE = source
    _CLICKHOUSE_CONFIG_MODE = mode


def _reset_clickhouse_client_cache() -> None:
    global _THREAD_LOCAL, _CH_CLIENT_KWARGS
    _CH_CLIENT_KWARGS = None
    client = getattr(_THREAD_LOCAL, "ch_client", None)
    if client is not None:
        try:
            client.close()
        except Exception:  # pragma: no cover - zależy od środowiska
            pass
    _THREAD_LOCAL = threading.local()


def _get_ch_client_kwargs():
    global _CH_CLIENT_KWARGS
    if _CH_CLIENT_KWARGS is not None:
        return _CH_CLIENT_KWARGS

    with _CH_CLIENT_LOCK:
        if _CH_CLIENT_KWARGS is not None:
            return _CH_CLIENT_KWARGS

        parsed = _parse_clickhouse_url()

        if parsed:
            host = parsed["host"]
            port = parsed["port"]
            secure = parsed["secure"]
            username = parsed.get("username") or CLICKHOUSE_USER
            password = parsed.get("password") or CLICKHOUSE_PASSWORD
            database = parsed.get("database") or CLICKHOUSE_DATABASE
            verify = (
                parsed["verify"]
                if parsed.get("verify") is not None
                else (CLICKHOUSE_VERIFY if secure else False)
            )
        else:
            host = CLICKHOUSE_HOST
            if not host:
                raise RuntimeError(
                    "Brak konfiguracji ClickHouse. Ustaw CLICKHOUSE_URL lub CLICKHOUSE_HOST"
                )
            try:
                port = int(CLICKHOUSE_PORT or (8443 if CLICKHOUSE_SECURE else 8123))
            except ValueError as exc:
                raise RuntimeError("CLICKHOUSE_PORT musi być liczbą całkowitą") from exc
            secure = CLICKHOUSE_SECURE
            username = CLICKHOUSE_USER
            password = CLICKHOUSE_PASSWORD
            database = CLICKHOUSE_DATABASE
            verify = CLICKHOUSE_VERIFY if secure else False

        interface = "https" if secure else "http"

        client_kwargs = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "database": database,
            "interface": interface,
            "secure": secure,
            "verify": verify,
        }

        if CLICKHOUSE_CA:
            client_kwargs["ca_cert"] = CLICKHOUSE_CA

        _CH_CLIENT_KWARGS = client_kwargs
        return _CH_CLIENT_KWARGS


def get_ch():
    client = getattr(_THREAD_LOCAL, "ch_client", None)
    if client is not None:
        return client

    client_kwargs = _get_ch_client_kwargs()
    client = clickhouse_connect.get_client(**client_kwargs)
    _THREAD_LOCAL.ch_client = client
    return client


# =========================
# FastAPI + CORS
# =========================

app = FastAPI(title="GPW Analytics API", version="0.1.0")
api_router = APIRouter()
OHLC_SYNC_PROGRESS_TRACKER = OhlcSyncProgressTracker()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://gpw-frontend.vercel.app",
        "http://localhost:3000",
        "*",  # opcjonalnie na czas testów
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@api_router.get("/ping")
def ping() -> str:
    return "pong"


class ClickHouseConfigRequest(BaseModel):
    reset: bool = Field(
        default=False,
        description="Czy przywrócić ustawienia środowiskowe ClickHouse",
    )
    mode: Literal["url", "manual"] = Field(
        default="url",
        description="Tryb konfiguracji: pełny adres URL lub ręczne parametry",
    )
    url: Optional[str] = Field(
        default=None,
        description="Adres połączenia ClickHouse (np. https://example:8443/db)",
    )
    host: Optional[str] = Field(
        default=None,
        description="Host ClickHouse w trybie ręcznym (np. srv.clickhouse.tech)",
    )
    port: Optional[int] = Field(
        default=None,
        ge=1,
        le=65535,
        description="Port ClickHouse w trybie ręcznym",
    )
    database: Optional[str] = Field(
        default=None,
        description="Nazwa bazy danych ClickHouse",
    )
    username: Optional[str] = Field(
        default=None,
        description="Login użytkownika ClickHouse",
    )
    password: Optional[str] = Field(
        default=None,
        description="Hasło użytkownika ClickHouse",
    )
    secure: Optional[bool] = Field(
        default=None,
        description="Czy użyć połączenia TLS/HTTPS w trybie ręcznym",
    )
    verify: Optional[bool] = Field(
        default=None,
        description="Czy weryfikować certyfikat TLS (ręczny tryb)",
    )
    ca: Optional[str] = Field(
        default=None,
        description="Ścieżka do dodatkowego certyfikatu CA (opcjonalnie)",
    )

    @model_validator(mode="after")
    def _normalize(cls, values: "ClickHouseConfigRequest") -> "ClickHouseConfigRequest":
        if values.reset:
            return values

        if values.url is not None:
            cleaned = values.url.strip()
            values.url = cleaned or None
        if values.host is not None:
            cleaned = values.host.strip()
            values.host = cleaned or None
        if values.database is not None:
            cleaned = values.database.strip()
            values.database = cleaned or None
        if values.username is not None:
            cleaned = values.username.strip()
            values.username = cleaned or None
        if values.ca is not None:
            cleaned = values.ca.strip()
            values.ca = cleaned or None

        if values.mode not in {"url", "manual"}:
            values.mode = "url"

        if values.mode == "url":
            if not values.url:
                raise ValueError("W trybie URL należy podać adres ClickHouse")
        else:
            if not values.host:
                raise ValueError("W trybie ręcznym należy podać hosta ClickHouse")

        return values


class ClickHouseConfigResponse(BaseModel):
    source: Literal["env", "override"] = Field(
        description="Źródło konfiguracji backendu: zmienne środowiskowe lub nadpisanie"
    )
    mode: Literal["url", "manual"] = Field(
        description="Tryb konfiguracji używany aktualnie przez backend"
    )
    url: Optional[str] = Field(
        default=None,
        description="Zanonimizowany adres URL ClickHouse (bez hasła)",
    )
    host: Optional[str] = Field(
        default=None,
        description="Host ClickHouse ustawiony w backendzie",
    )
    port: Optional[int] = Field(
        default=None,
        description="Port ClickHouse ustawiony w backendzie",
    )
    secure: bool = Field(description="Czy backend używa szyfrowanego połączenia")
    verify: Optional[bool] = Field(
        default=None,
        description="Czy backend weryfikuje certyfikat TLS (None = domyślnie)",
    )
    database: Optional[str] = Field(
        default=None,
        description="Nazwa bazy danych używanej przez backend",
    )
    username: Optional[str] = Field(
        default=None,
        description="Użytkownik ClickHouse ustawiony w backendzie",
    )
    has_password: bool = Field(
        description="Czy backend posiada skonfigurowane hasło do ClickHouse"
    )
    ca: Optional[str] = Field(
        default=None,
        description="Skonfigurowany dodatkowy certyfikat CA",
    )


def _build_clickhouse_config_response() -> ClickHouseConfigResponse:
    client_kwargs = _get_ch_client_kwargs()
    host = str(client_kwargs.get("host") or "").strip()
    port_value = client_kwargs.get("port")
    try:
        port = int(port_value) if port_value is not None else None
    except (TypeError, ValueError):
        port = None
    secure = bool(client_kwargs.get("secure", True))
    verify_value = client_kwargs.get("verify")
    verify = None if verify_value is None else bool(verify_value)
    username = client_kwargs.get("username") or None
    database = client_kwargs.get("database") or None
    password = client_kwargs.get("password") or None
    ca_cert = client_kwargs.get("ca_cert") or (CLICKHOUSE_CA or None)

    url_display = CLICKHOUSE_URL.strip() or None
    if url_display:
        url_display = _mask_clickhouse_url(url_display)

    return ClickHouseConfigResponse(
        source=_CLICKHOUSE_CONFIG_SOURCE,
        mode=_CLICKHOUSE_CONFIG_MODE,
        url=url_display,
        host=host or None,
        port=port,
        secure=secure,
        verify=verify,
        database=database,
        username=username,
        has_password=bool(password),
        ca=ca_cert,
    )


@api_router.get("/config/clickhouse", response_model=ClickHouseConfigResponse)
def get_clickhouse_config() -> ClickHouseConfigResponse:
    try:
        return _build_clickhouse_config_response()
    except Exception as exc:  # pragma: no cover - zależy od środowiska
        raise HTTPException(500, str(exc)) from exc


@api_router.post("/config/clickhouse", response_model=ClickHouseConfigResponse)
def update_clickhouse_config(payload: ClickHouseConfigRequest) -> ClickHouseConfigResponse:
    previous = _snapshot_clickhouse_settings()
    previous_settings = {
        key: previous[key]
        for key in _INITIAL_CLICKHOUSE_SETTINGS.keys()
    }
    previous_source: Literal["env", "override"] = previous["source"]
    previous_mode: Literal["url", "manual"] = previous["mode"]

    if payload.reset:
        settings = dict(_INITIAL_CLICKHOUSE_SETTINGS)
        target_source: Literal["env", "override"] = "env"
        target_mode: Literal["url", "manual"] = _INITIAL_CLICKHOUSE_MODE
    else:
        settings = dict(previous_settings)
        target_source = "override"
        target_mode = payload.mode
        if payload.mode == "url":
            settings["CLICKHOUSE_URL"] = payload.url or ""
            settings["CLICKHOUSE_HOST"] = payload.host or ""
            settings["CLICKHOUSE_PORT"] = (
                str(payload.port) if payload.port is not None else ""
            )
        else:
            settings["CLICKHOUSE_URL"] = ""
            settings["CLICKHOUSE_HOST"] = payload.host or ""
            settings["CLICKHOUSE_PORT"] = (
                str(payload.port) if payload.port is not None else ""
            )

        if payload.database is not None:
            settings["CLICKHOUSE_DATABASE"] = payload.database or ""
        if payload.username is not None:
            settings["CLICKHOUSE_USER"] = payload.username or ""
        if payload.password is not None:
            settings["CLICKHOUSE_PASSWORD"] = payload.password or ""
        if payload.secure is not None:
            settings["CLICKHOUSE_SECURE"] = bool(payload.secure)
        if payload.verify is not None:
            settings["CLICKHOUSE_VERIFY"] = bool(payload.verify)
        if payload.ca is not None:
            settings["CLICKHOUSE_CA"] = payload.ca or ""

    try:
        _apply_clickhouse_settings(settings, source=target_source, mode=target_mode)
        _reset_clickhouse_client_cache()
        _get_ch_client_kwargs()
    except Exception as exc:
        _apply_clickhouse_settings(
            previous_settings,
            source=previous_source,
            mode=previous_mode,
        )
        _reset_clickhouse_client_cache()
        raise HTTPException(400, str(exc)) from exc

    return _build_clickhouse_config_response()


class OhlcSyncRequest(BaseModel):
    symbols: Optional[List[str]] = Field(
        default=None,
        description="Lista symboli do synchronizacji (np. CDR lub CDR.WA)",
    )
    start: Optional[date] = Field(
        default=None,
        description="Najwcześniejsza data notowań w formacie YYYY-MM-DD",
    )
    truncate: bool = Field(
        default=False,
        description="Czy wyczyścić tabelę przed synchronizacją",
    )
    run_as_admin: bool = Field(
        default=False,
        description="Czy wykonać synchronizację w trybie administratora",
    )

    @field_validator("symbols", mode="before")
    @classmethod
    def _normalize_symbols(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            value = [value]
        symbols: List[str] = []
        for item in value:
            if item is None:
                continue
            normalized = normalize_input_symbol(str(item))
            if normalized:
                symbols.append(normalized)
        return symbols or None

    @field_validator("start", mode="before")
    @classmethod
    def _parse_start(cls, value):
        if value is None or isinstance(value, date):
            return value
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                return None
            try:
                return date.fromisoformat(cleaned)
            except ValueError as exc:
                raise ValueError("Data musi być w formacie YYYY-MM-DD") from exc
        raise ValueError("Niepoprawny format daty")


class OhlcImportResponse(BaseModel):
    inserted: int = Field(0, description="Liczba wierszy zapisanych do ClickHouse")
    skipped: int = Field(0, description="Liczba wierszy pominiętych (błędy lub duplikaty)")
    errors: List[str] = Field(default_factory=list, description="Lista komunikatów o błędach")


# Aktualizacja modeli zależnych po zdefiniowaniu OhlcSyncRequest
OhlcSyncScheduleStatus.model_rebuild()
OhlcSyncScheduleRequest.model_rebuild()

_OHLC_SCHEDULE_STATE = OhlcSyncScheduleStatus()


# =========================
# Dane o spółkach (mapowania + cache)
# =========================

COMPANY_SYMBOL_CANDIDATES = [
    "symbol",
    "symbol_gpw",
    "symbol_gpw_benchmark",
    "ticker",
    "code",
    "short_name",
    "symbol_stooq",
    "symbol_yahoo",
    "symbol_google",
    "company_symbol",
    "company_code",
]

TICKER_LIKE_PATTERN = re.compile(r"^[0-9A-Z]{1,8}(?:[._-][0-9A-Z]{1,8})?$")

COMPANY_NAME_CANDIDATES = [
    "symbol",
    "name",
    "company_name",
    "full_name",
    "nazwa",
]

CompanyFieldTarget = Tuple[str, str, str]


COMPANY_COLUMN_MAP: Dict[str, CompanyFieldTarget] = {
    # podstawowe informacje identyfikacyjne
    "symbol": ("company", "raw_symbol", "text"),
    "ticker": ("company", "raw_symbol", "text"),
    "code": ("company", "raw_symbol", "text"),
    "symbol_gpw": ("company", "symbol_gpw", "text"),
    "symbol_gpw_benchmark": ("company", "symbol_gpw_benchmark", "text"),
    "symbol_stooq": ("company", "symbol_stooq", "text"),
    "symbol_yahoo": ("company", "symbol_yahoo", "text"),
    "symbol_google": ("company", "symbol_google", "text"),
    "isin": ("company", "isin", "text"),
    "name": ("company", "name", "text"),
    "company_name": ("company", "name", "text"),
    "full_name": ("company", "name", "text"),
    "short_name": ("company", "raw_symbol", "text"),
    "sector": ("company", "sector", "text"),
    "industry": ("company", "industry", "text"),
    "branch": ("company", "industry", "text"),
    "country": ("company", "country", "text"),
    "region": ("company", "country", "text"),
    "headquarters": ("company", "headquarters", "text"),
    "city": ("company", "headquarters", "text"),
    "website": ("company", "website", "text"),
    "url": ("company", "website", "text"),
    "description": ("company", "description", "text"),
    "profile": ("company", "description", "text"),
    "long_description": ("company", "description", "text"),
    "about": ("company", "description", "text"),
    "logo": ("company", "logo_url", "text"),
    "logo_url": ("company", "logo_url", "text"),
    "image_url": ("company", "logo_url", "text"),
    "employees": ("company", "employees", "int"),
    "employee_count": ("company", "employees", "int"),
    "founded": ("company", "founded_year", "int"),
    "founded_year": ("company", "founded_year", "int"),
    "established": ("company", "founded_year", "int"),
    "listing_date": ("company", "listing_date", "date"),
    "ipo_date": ("company", "listing_date", "date"),
    # fundamenty (liczby)
    "market_cap": ("fundamentals", "market_cap", "float"),
    "marketcapitalization": ("fundamentals", "market_cap", "float"),
    "market_capitalization": ("fundamentals", "market_cap", "float"),
    "shares_outstanding": ("fundamentals", "shares_outstanding", "float"),
    "sharesoutstanding": ("fundamentals", "shares_outstanding", "float"),
    "shares": ("fundamentals", "shares_outstanding", "float"),
    "book_value": ("fundamentals", "book_value", "float"),
    "bookvalue": ("fundamentals", "book_value", "float"),
    "revenue": ("fundamentals", "revenue_ttm", "float"),
    "revenue_ttm": ("fundamentals", "revenue_ttm", "float"),
    "total_revenue": ("fundamentals", "revenue_ttm", "float"),
    "net_income": ("fundamentals", "net_income_ttm", "float"),
    "net_income_ttm": ("fundamentals", "net_income_ttm", "float"),
    "netincome": ("fundamentals", "net_income_ttm", "float"),
    "ebitda": ("fundamentals", "ebitda_ttm", "float"),
    "ebitda_ttm": ("fundamentals", "ebitda_ttm", "float"),
    "eps": ("fundamentals", "eps", "float"),
    "earnings_per_share": ("fundamentals", "eps", "float"),
    "pe_ratio": ("fundamentals", "pe_ratio", "float"),
    "price_earnings": ("fundamentals", "pe_ratio", "float"),
    "pb_ratio": ("fundamentals", "pb_ratio", "float"),
    "price_book": ("fundamentals", "pb_ratio", "float"),
    "dividend_yield": ("fundamentals", "dividend_yield", "float"),
    "dividend": ("fundamentals", "dividend_yield", "float"),
    "debt_to_equity": ("fundamentals", "debt_to_equity", "float"),
    "total_debt_to_equity": ("fundamentals", "debt_to_equity", "float"),
    "roa": ("fundamentals", "roa", "float"),
    "roe": ("fundamentals", "roe", "float"),
    "gross_margin": ("fundamentals", "gross_margin", "float"),
    "operating_margin": ("fundamentals", "operating_margin", "float"),
    "profit_margin": ("fundamentals", "profit_margin", "float"),
}

RAW_SYMBOL_PRIORITIES: Dict[str, int] = {
    "symbol": 500,
    "ticker": 400,
    "code": 300,
    "short_name": 200,
    "company_symbol": 150,
    "company_code": 100,
}

_COMPANY_COLUMNS_CACHE: Optional[List[str]] = None
_COMPANY_COLUMNS_LOCK = threading.Lock()
_COMPANY_SYMBOL_LOOKUP: Optional[Dict[str, str]] = None
_COMPANY_SYMBOL_LOOKUP_LOCK = threading.Lock()


def _is_unknown_table_error(exc: Exception) -> bool:
    code = getattr(exc, "code", None)
    if isinstance(code, int) and code == 60:
        return True
    message = str(exc)
    return "UNKNOWN_TABLE" in message or "does not exist" in message


def _create_companies_table_if_missing(ch_client) -> None:
    ch_client.command(DEFAULT_COMPANIES_TABLE_DDL)


def _describe_companies_table(ch_client):
    return ch_client.query(f"DESCRIBE TABLE {TABLE_COMPANIES}").result_rows


def _create_ohlc_table_if_missing(ch_client) -> None:
    ch_client.command(DEFAULT_OHLC_TABLE_DDL)


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip().replace(",", ".")
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(float(stripped))
        except ValueError:
            return None
    return None


def _coerce_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return date.fromisoformat(stripped).isoformat()
        except ValueError:
            return stripped
    return None


def _convert_clickhouse_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.hex()
    if isinstance(value, (list, tuple)):
        return [_convert_clickhouse_value(v) for v in value]
    return str(value)


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def _quote_sql_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def _get_company_columns(ch_client) -> List[str]:
    global _COMPANY_COLUMNS_CACHE
    if _COMPANY_COLUMNS_CACHE is not None:
        return _COMPANY_COLUMNS_CACHE

    with _COMPANY_COLUMNS_LOCK:
        if _COMPANY_COLUMNS_CACHE is not None:
            return _COMPANY_COLUMNS_CACHE

        try:
            rows = _describe_companies_table(ch_client)
        except Exception as exc:  # pragma: no cover - zależy od konfiguracji DB
            if not _is_unknown_table_error(exc):
                raise HTTPException(
                    500,
                    f"Nie udało się pobrać schematu tabeli {TABLE_COMPANIES}: {exc}",
                ) from exc

            try:
                _create_companies_table_if_missing(ch_client)
            except Exception as create_exc:  # pragma: no cover - środowisko DB
                raise HTTPException(
                    500,
                    f"Nie udało się utworzyć tabeli {TABLE_COMPANIES}: {create_exc}",
                ) from create_exc

            try:
                rows = _describe_companies_table(ch_client)
            except Exception as describe_exc:  # pragma: no cover - środowisko DB
                raise HTTPException(
                    500,
                    f"Nie udało się pobrać schematu tabeli {TABLE_COMPANIES} po utworzeniu: {describe_exc}",
                ) from describe_exc

        columns = [str(row[0]) for row in rows]
        if not columns:
            raise HTTPException(500, f"Tabela {TABLE_COMPANIES} nie ma zdefiniowanych kolumn")

        _COMPANY_COLUMNS_CACHE = columns
        return _COMPANY_COLUMNS_CACHE


def _ensure_company_benchmark_column(ch_client, columns: Optional[Sequence[str]] = None) -> str:
    global _COMPANY_COLUMNS_CACHE
    target = "symbol_gpw_benchmark"
    active_columns = list(columns) if columns is not None else _get_company_columns(ch_client)
    lowered_to_original = {col.lower(): col for col in active_columns}
    existing = lowered_to_original.get(target)
    if existing:
        return existing

    try:
        ch_client.command(
            f"ALTER TABLE {TABLE_COMPANIES} "
            "ADD COLUMN IF NOT EXISTS symbol_gpw_benchmark LowCardinality(Nullable(String)) "
            "AFTER symbol_gpw"
        )
    except Exception:
        ch_client.command(
            f"ALTER TABLE {TABLE_COMPANIES} "
            "ADD COLUMN IF NOT EXISTS symbol_gpw_benchmark LowCardinality(Nullable(String))"
        )

    _COMPANY_COLUMNS_CACHE = None
    refreshed = _get_company_columns(ch_client)
    lowered_to_original = {col.lower(): col for col in refreshed}
    return lowered_to_original.get(target, "symbol_gpw_benchmark")


def _find_company_symbol_column(columns: Sequence[str]) -> Optional[str]:
    lowered_to_original = {col.lower(): col for col in columns}
    for candidate in COMPANY_SYMBOL_CANDIDATES:
        existing = lowered_to_original.get(candidate)
        if existing:
            return existing
    return None


def _iter_symbol_aliases(value: str) -> Iterable[str]:
    cleaned = value.strip()
    if not cleaned:
        return

    variants: List[str] = [cleaned]
    upper = cleaned.upper()
    if upper.endswith(".WA"):
        trimmed = cleaned[:-3].strip()
        if trimmed:
            variants.append(trimmed)

    normalized = normalize_input_symbol(cleaned)
    if normalized:
        variants.append(normalized)
        pretty = pretty_symbol(normalized)
        if pretty:
            variants.append(pretty)
            if "." in pretty:
                base = pretty.split(".", 1)[0].strip()
                if base:
                    variants.append(base)

    seen: Set[str] = set()
    for variant in variants:
        candidate = variant.strip()
        if not candidate:
            continue
        upper_candidate = candidate.upper()
        if upper_candidate in seen:
            continue
        seen.add(upper_candidate)
        yield candidate


def _normalize_benchmark_symbol(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    normalized = re.sub(r"\s+", "", cleaned.upper())
    if normalized.endswith(".WA"):
        base = normalized[:-3].strip()
        if base:
            normalized = f"{base}.WA"
    return normalized


def _build_company_symbol_lookup(ch_client) -> Dict[str, str]:
    global _COMPANY_SYMBOL_LOOKUP
    if _COMPANY_SYMBOL_LOOKUP is not None:
        return _COMPANY_SYMBOL_LOOKUP

    with _COMPANY_SYMBOL_LOOKUP_LOCK:
        if _COMPANY_SYMBOL_LOOKUP is not None:
            return _COMPANY_SYMBOL_LOOKUP

        try:
            columns = _get_company_columns(ch_client)
        except HTTPException:
            columns = []

        if not columns:
            _COMPANY_SYMBOL_LOOKUP = {}
            return _COMPANY_SYMBOL_LOOKUP

        lowered_to_original = {col.lower(): col for col in columns}

        preferred_primary: List[str] = []
        for candidate in (
            "symbol",
            "symbol_gpw",
            "symbol_gpw_benchmark",
            "ticker",
            "code",
            "short_name",
        ):
            existing = lowered_to_original.get(candidate)
            if existing and existing not in preferred_primary:
                preferred_primary.append(existing)

        symbol_columns: List[str] = []
        for candidate in COMPANY_SYMBOL_CANDIDATES:
            existing = lowered_to_original.get(candidate)
            if existing and existing not in symbol_columns:
                symbol_columns.append(existing)

        name_columns: List[str] = []
        for candidate in COMPANY_NAME_CANDIDATES:
            existing = lowered_to_original.get(candidate)
            if existing and existing not in symbol_columns and existing not in name_columns:
                name_columns.append(existing)

        selected_columns = [*symbol_columns, *name_columns]
        if not selected_columns:
            _COMPANY_SYMBOL_LOOKUP = {}
            return _COMPANY_SYMBOL_LOOKUP

        select_clause = ", ".join(_quote_identifier(col) for col in selected_columns)
        sql = f"SELECT {select_clause} FROM {TABLE_COMPANIES}"

        try:
            result = ch_client.query(sql)
        except Exception:  # pragma: no cover - zależy od konfiguracji DB
            _COMPANY_SYMBOL_LOOKUP = {}
            return _COMPANY_SYMBOL_LOOKUP

        column_names = list(getattr(result, "column_names", []))
        try:
            rows = result.named_results()
        except AttributeError:
            rows = None

        if rows is None:
            rows = [
                {col: value for col, value in zip(column_names, row)}
                for row in getattr(result, "result_rows", [])
            ]

        lookup: Dict[str, str] = {}

        for row in rows:
            canonical: Optional[str] = None
            for column in preferred_primary or symbol_columns:
                value = row.get(column)
                text = str(_convert_clickhouse_value(value)).strip() if value is not None else ""
                if not text:
                    continue
                normalized = normalize_input_symbol(text)
                normalized_upper = normalized.strip().upper() if normalized else ""
                if not normalized_upper:
                    continue
                if not TICKER_LIKE_PATTERN.fullmatch(normalized_upper):
                    continue
                canonical = normalized
                break
            if not canonical:
                continue

            for column in selected_columns:
                value = row.get(column)
                text = str(_convert_clickhouse_value(value)).strip() if value is not None else ""
                if not text:
                    continue
                for alias in _iter_symbol_aliases(text):
                    key = alias.strip().upper()
                    if key:
                        lookup.setdefault(key, canonical)

            for alias in _iter_symbol_aliases(canonical):
                key = alias.strip().upper()
                if key:
                    lookup.setdefault(key, canonical)

        _COMPANY_SYMBOL_LOOKUP = lookup
        return _COMPANY_SYMBOL_LOOKUP

def _normalize_company_row(row: Dict[str, Any], symbol_column: str) -> Optional[Dict[str, Any]]:
    canonical: Dict[str, Any] = {
        "raw_symbol": None,
        "symbol_gpw": None,
        "symbol_gpw_benchmark": None,
        "symbol_stooq": None,
        "symbol_yahoo": None,
        "symbol_google": None,
        "name": None,
        "short_name": None,
        "isin": None,
        "sector": None,
        "industry": None,
        "country": None,
        "headquarters": None,
        "website": None,
        "description": None,
        "logo_url": None,
        "employees": None,
        "founded_year": None,
        "listing_date": None,
    }
    fundamentals: Dict[str, Optional[float]] = {}
    extra: Dict[str, Any] = {}
    raw_symbol_candidate: Optional[str] = None
    raw_symbol_priority = -1

    for column, raw_value in row.items():
        key = column.lower()
        converted_value = _convert_clickhouse_value(raw_value)
        mapping = COMPANY_COLUMN_MAP.get(key)

        if mapping is None:
            extra[column] = converted_value
            continue

        target_section, field_name, field_type = mapping

        if target_section == "company":
            if field_type == "text":
                if converted_value is None:
                    continue
                text_value = str(converted_value)
                if field_name == "raw_symbol":
                    candidate = text_value.strip()
                    if not candidate:
                        continue
                    priority = RAW_SYMBOL_PRIORITIES.get(key, 0)
                    if priority >= raw_symbol_priority:
                        raw_symbol_candidate = candidate
                        raw_symbol_priority = priority
                    continue
                canonical[field_name] = text_value
            elif field_type == "int":
                coerced = _coerce_int(converted_value)
                if coerced is not None:
                    canonical[field_name] = coerced
            elif field_type == "date":
                coerced_date = _coerce_date(raw_value)
                if coerced_date:
                    canonical[field_name] = coerced_date
            else:
                canonical[field_name] = converted_value
        else:
            # fundamentals -> zawsze liczby
            fundamentals[field_name] = _coerce_float(converted_value)

    if raw_symbol_candidate:
        canonical["raw_symbol"] = raw_symbol_candidate

    raw_symbol_value = canonical.get("raw_symbol")
    if not raw_symbol_value:
        fallback = row.get(symbol_column)
        if fallback is None:
            return None
        raw_symbol_value = str(_convert_clickhouse_value(fallback))
        canonical["raw_symbol"] = raw_symbol_value

    display_candidates = [
        canonical.get("symbol_gpw_benchmark"),
        canonical.get("symbol_gpw"),
        raw_symbol_value,
    ]
    for display_candidate in display_candidates:
        if not display_candidate:
            continue
        display_text = str(display_candidate).strip()
        if not display_text:
            continue
        pretty = pretty_symbol(display_text)
        canonical["symbol"] = pretty or display_text
        break
    else:
        fallback_pretty = pretty_symbol(str(raw_symbol_value))
        canonical["symbol"] = fallback_pretty or str(raw_symbol_value)

    if not canonical.get("symbol_gpw"):
        canonical["symbol_gpw"] = canonical["raw_symbol"]
    canonical["fundamentals"] = fundamentals

    insights = _extract_stooq_insights(extra.get("raw_payload"))
    if insights:
        shareholding = insights.get("shareholding")
        if isinstance(shareholding, list) and shareholding:
            extra.setdefault("stooq_shareholding", shareholding)
        company_size = insights.get("company_size")
        if isinstance(company_size, str) and company_size.strip():
            extra.setdefault("stooq_company_size", company_size.strip())
        facts = insights.get("facts")
        if isinstance(facts, list) and facts:
            extra.setdefault("stooq_facts", facts)
        indices = insights.get("indices")
        if isinstance(indices, list) and indices:
            extra.setdefault("stooq_indices", indices)

    canonical["extra"] = extra
    canonical["raw"] = {col: _convert_clickhouse_value(val) for col, val in row.items()}

    return canonical


class _CompanyNameLookupEntry(TypedDict, total=False):
    raw_symbol: str
    symbol: str
    name: Optional[str]
    names: List[str]


def _build_company_name_lookup(ch_client) -> Dict[str, _CompanyNameLookupEntry]:
    columns = _get_company_columns(ch_client)
    lowered_to_original = {col.lower(): col for col in columns}

    symbol_column: Optional[str] = None
    for candidate in COMPANY_SYMBOL_CANDIDATES:
        existing = lowered_to_original.get(candidate)
        if existing:
            symbol_column = existing
            break

    if not symbol_column:
        return {}

    name_columns: List[str] = []
    for candidate in COMPANY_NAME_CANDIDATES:
        existing = lowered_to_original.get(candidate)
        if existing and existing not in name_columns:
            name_columns.append(existing)

    # Jeśli nie mamy dodatkowych kolumn z nazwą, zapytanie ograniczy się do symbolu.
    selected_columns: List[str] = []
    seen: Set[str] = set()
    for column in [symbol_column, *name_columns]:
        if column and column not in seen:
            selected_columns.append(column)
            seen.add(column)

    if not selected_columns:
        return {}

    select_clause = ", ".join(_quote_identifier(col) for col in selected_columns)
    sql = f"SELECT {select_clause} FROM {TABLE_COMPANIES}"

    try:
        result = ch_client.query(sql)
    except Exception:  # pragma: no cover - zależy od konfiguracji DB
        return {}

    column_names = list(getattr(result, "column_names", []))
    try:
        rows = result.named_results()
    except AttributeError:
        rows = None

    if rows is None:
        rows = [
            {col: value for col, value in zip(column_names, row)}
            for row in getattr(result, "result_rows", [])
        ]

    lookup: Dict[str, _CompanyNameLookupEntry] = {}

    for row in rows:
        symbol_value = row.get(symbol_column)
        if symbol_value is None:
            continue

        raw_symbol = str(_convert_clickhouse_value(symbol_value)).strip()
        if not raw_symbol:
            continue

        normalized_raw = normalize_input_symbol(raw_symbol)
        if not normalized_raw:
            continue

        pretty = pretty_symbol(normalized_raw)
        base = pretty.split(".", 1)[0] if "." in pretty else pretty

        resolved_names: List[str] = []
        for column in name_columns:
            value = row.get(column)
            text = str(_convert_clickhouse_value(value)).strip()
            if text:
                resolved_names.append(text)

        deduplicated_names: List[str] = []
        seen_names: Set[str] = set()
        for candidate in resolved_names:
            cleaned = candidate.strip()
            if not cleaned:
                continue
            upper = cleaned.upper()
            if upper in seen_names:
                continue
            seen_names.add(upper)
            deduplicated_names.append(cleaned)

        symbol_keys: Set[str] = {
            normalized_raw,
            pretty,
            base,
            raw_symbol,
        }
        for alias_column in ("short_name", "ticker", "code"):
            alias_value = row.get(alias_column)
            alias_text = str(_convert_clickhouse_value(alias_value)).strip()
            if not alias_text:
                continue
            symbol_keys.add(alias_text)
            normalized_alias = normalize_input_symbol(alias_text)
            if normalized_alias:
                symbol_keys.add(normalized_alias)
                symbol_keys.add(pretty_symbol(normalized_alias))
        normalized_symbol_keys = {
            key.strip().upper() for key in symbol_keys if key and key.strip()
        }

        preferred_name: Optional[str] = None
        for candidate in deduplicated_names:
            cleaned_candidate = candidate.strip()
            if not cleaned_candidate:
                continue
            candidate_upper = cleaned_candidate.upper()
            normalized_candidate = normalize_input_symbol(cleaned_candidate)
            normalized_candidate_upper = (
                normalized_candidate.strip().upper()
                if normalized_candidate
                else ""
            )
            pretty_candidate_upper = (
                pretty_symbol(normalized_candidate).strip().upper()
                if normalized_candidate
                else ""
            )
            if candidate_upper in normalized_symbol_keys:
                continue
            if normalized_candidate_upper and normalized_candidate_upper in normalized_symbol_keys:
                continue
            if pretty_candidate_upper and pretty_candidate_upper in normalized_symbol_keys:
                continue
            preferred_name = candidate
            break
        if preferred_name is None and deduplicated_names:
            preferred_name = max(
                deduplicated_names,
                key=lambda value: (len(value or ""), value),
            )

        entry: _CompanyNameLookupEntry = {
            "raw_symbol": normalized_raw,
            "symbol": pretty,
            "name": preferred_name,
            "names": deduplicated_names,
        }

        alias_keys: Set[str] = {
            normalized_raw,
            pretty,
            base,
            raw_symbol,
        }

        for alias in deduplicated_names:
            alias_keys.add(alias)

        for key in alias_keys:
            cleaned = key.strip().upper()
            if cleaned:
                lookup.setdefault(cleaned, entry)

    return lookup


# =========================
# MODELE
# =========================

class QuoteRow(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class DataCollectionItem(BaseModel):
    symbol: str
    raw: str
    quotes: List[QuoteRow] = Field(default_factory=list)


class SectorClassificationEntry(BaseModel):
    code: str
    name: str
    parent_code: Optional[str] = Field(default=None, description="Kod nadrzędnej kategorii")


class CompanySyncJobStatus(BaseModel):
    job_id: Optional[str] = Field(default=None, description="Identyfikator bieżącego zadania")
    status: Literal["idle", "running", "completed", "failed"] = Field(
        default="idle", description="Aktualny stan synchronizacji"
    )
    stage: Literal["idle", "fetching", "harvesting", "inserting", "finished", "failed"] = Field(
        default="idle", description="Faza procesu synchronizacji"
    )
    total: Optional[int] = Field(
        default=None, description="Całkowita liczba spółek oczekująca na przetworzenie"
    )
    processed: int = Field(0, description="Liczba rekordów z listy GPW, które zostały przetworzone")
    synced: int = Field(0, description="Liczba spółek zapisanych lub przygotowanych do zapisu")
    failed: int = Field(0, description="Liczba błędów napotkanych podczas synchronizacji")
    started_at: Optional[datetime] = Field(
        default=None, description="Moment rozpoczęcia synchronizacji"
    )
    finished_at: Optional[datetime] = Field(
        default=None, description="Moment zakończenia synchronizacji"
    )
    current_symbol: Optional[str] = Field(
        default=None, description="Symbol spółki przetwarzanej w ostatnim kroku"
    )
    message: Optional[str] = Field(default=None, description="Dodatkowy komunikat statusowy")
    errors: List[str] = Field(default_factory=list, description="Lista napotkanych błędów")
    result: Optional[CompanySyncResult] = Field(
        default=None, description="Pełne podsumowanie ostatniej synchronizacji"
    )


_SYNC_STATE = CompanySyncJobStatus()


def _start_sync_job(limit: Optional[int], *, message: str) -> str:
    global _SYNC_STATE, _SYNC_THREAD
    job_id = str(uuid4())
    started_at = datetime.utcnow()
    _SYNC_STATE = CompanySyncJobStatus(
        job_id=job_id,
        status="running",
        stage="fetching",
        total=None,
        processed=0,
        synced=0,
        failed=0,
        started_at=started_at,
        finished_at=None,
        current_symbol=None,
        message=message,
        errors=[],
        result=None,
    )
    _SYNC_THREAD = threading.Thread(
        target=_run_company_sync_job,
        args=(job_id, limit),
        daemon=True,
    )
    _SYNC_THREAD.start()
    return job_id


def _check_and_run_scheduled_job(now: Optional[datetime] = None) -> bool:
    if now is None:
        now = datetime.utcnow()

    with _SCHEDULE_LOCK:
        next_run = _SYNC_SCHEDULE_STATE.next_run_at
        mode = _SYNC_SCHEDULE_STATE.mode
        interval = _SYNC_SCHEDULE_STATE.recurring_interval_minutes

    if next_run is None or next_run > now:
        return False

    with _SYNC_LOCK:
        if _SYNC_STATE.status == "running":
            return False
        _start_sync_job(limit=None, message="Planowana synchronizacja spółek")
        started_at = _SYNC_STATE.started_at or now

    with _SCHEDULE_LOCK:
        _SYNC_SCHEDULE_STATE.last_run_started_at = started_at
        _SYNC_SCHEDULE_STATE.last_run_status = "running"
        if mode == "once":
            _SYNC_SCHEDULE_STATE.mode = "idle"
            _SYNC_SCHEDULE_STATE.next_run_at = None
            _SYNC_SCHEDULE_STATE.recurring_interval_minutes = None
            _SYNC_SCHEDULE_STATE.recurring_start_at = None
        elif mode == "recurring" and interval:
            _SYNC_SCHEDULE_STATE.next_run_at = started_at + timedelta(minutes=interval)
        _notify_schedule_loop()

    return True


def _check_and_run_ohlc_scheduled_job(now: Optional[datetime] = None) -> bool:
    if now is None:
        now = datetime.utcnow()

    with _OHLC_SCHEDULE_LOCK:
        next_run = _OHLC_SCHEDULE_STATE.next_run_at
        mode = _OHLC_SCHEDULE_STATE.mode
        interval = _OHLC_SCHEDULE_STATE.recurring_interval_minutes
        options = (
            _OHLC_SCHEDULE_STATE.options.model_copy(deep=True)
            if _OHLC_SCHEDULE_STATE.options
            else None
        )

    if next_run is None or next_run > now or options is None:
        return False

    snapshot = OHLC_SYNC_PROGRESS_TRACKER.snapshot()
    if snapshot.status == "running":
        return False

    try:
        payload = OhlcSyncRequest.model_validate(options.model_dump())
    except Exception:
        return False

    schedule_mode: Optional[str]
    if mode in {"once", "recurring"}:
        schedule_mode = mode
    else:
        schedule_mode = None

    started_at = datetime.utcnow()
    with _OHLC_SCHEDULE_LOCK:
        _OHLC_SCHEDULE_STATE.last_run_started_at = started_at
        _OHLC_SCHEDULE_STATE.last_run_status = "running"
        if mode == "once":
            _OHLC_SCHEDULE_STATE.mode = "idle"
            _OHLC_SCHEDULE_STATE.next_run_at = None
            _OHLC_SCHEDULE_STATE.recurring_interval_minutes = None
            _OHLC_SCHEDULE_STATE.recurring_start_at = None
        elif mode == "recurring" and interval:
            _OHLC_SCHEDULE_STATE.next_run_at = started_at + timedelta(minutes=interval)
            if _OHLC_SCHEDULE_STATE.recurring_start_at is None:
                _OHLC_SCHEDULE_STATE.recurring_start_at = started_at
        _notify_ohlc_schedule_loop()

    thread = threading.Thread(
        target=_run_ohlc_sync_in_background,
        args=(payload,),
        kwargs={"schedule_mode": schedule_mode},
        daemon=True,
    )
    thread.start()

    return True


class CompanyFundamentals(BaseModel):
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    book_value: Optional[float] = None
    revenue_ttm: Optional[float] = None
    net_income_ttm: Optional[float] = None
    ebitda_ttm: Optional[float] = None
    eps: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    dividend_yield: Optional[float] = None
    debt_to_equity: Optional[float] = None
    roa: Optional[float] = None
    roe: Optional[float] = None
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    profit_margin: Optional[float] = None


class CompanyProfile(BaseModel):
    symbol: str
    raw_symbol: str
    symbol_gpw: Optional[str] = None
    symbol_gpw_benchmark: Optional[str] = None
    symbol_stooq: Optional[str] = None
    symbol_yahoo: Optional[str] = None
    symbol_google: Optional[str] = None
    name: Optional[str] = None
    short_name: Optional[str] = None
    isin: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    country: Optional[str] = None
    headquarters: Optional[str] = None
    website: Optional[str] = None
    description: Optional[str] = None
    logo_url: Optional[str] = None
    employees: Optional[int] = None
    founded_year: Optional[int] = None
    listing_date: Optional[str] = None
    fundamentals: CompanyFundamentals = Field(default_factory=CompanyFundamentals)
    extra: Dict[str, Any] = Field(default_factory=dict)
    raw: Dict[str, Any] = Field(default_factory=dict)


class CompanyBenchmarkSymbolUpdateRequest(BaseModel):
    symbol: str = Field(..., description="Symbol spółki w tabeli companies")
    benchmark_symbol: Optional[str] = Field(
        default=None,
        description="Symbol GPW Benchmark przypisany do spółki (np. CDR.WA)",
    )

    @field_validator("symbol")
    @classmethod
    def _validate_symbol(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Symbol spółki nie może być pusty")
        return cleaned

    @field_validator("benchmark_symbol")
    @classmethod
    def _validate_benchmark_symbol(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None


class PortfolioPoint(BaseModel):
    date: str
    value: float


class PortfolioStats(BaseModel):
    cagr: float
    max_drawdown: float
    volatility: float
    sharpe: float
    last_value: float
    total_return: Optional[float] = None
    turnover: Optional[float] = None
    trades: Optional[float] = None
    initial_value: Optional[float] = None
    final_value: Optional[float] = None
    fees: Optional[float] = None


class PortfolioTrade(BaseModel):
    symbol: str
    action: Optional[str] = None
    weight_change: Optional[float] = None
    value_change: Optional[float] = None
    target_weight: Optional[float] = None
    shares_change: Optional[float] = None
    price: Optional[float] = None
    shares_after: Optional[float] = None
    note: Optional[str] = None


class PortfolioRebalanceEvent(BaseModel):
    date: str
    reason: Optional[str] = None
    turnover: Optional[float] = None
    trades: Optional[List[PortfolioTrade]] = None


class PortfolioAllocation(BaseModel):
    symbol: str
    target_weight: float
    raw: Optional[str] = None
    realized_weight: Optional[float] = None
    return_pct: Optional[float] = None
    contribution_pct: Optional[float] = None
    value: Optional[float] = None


class PortfolioResp(BaseModel):
    equity: List[PortfolioPoint]
    stats: PortfolioStats
    allocations: Optional[List[PortfolioAllocation]] = None
    rebalances: Optional[List[PortfolioRebalanceEvent]] = None


class PortfolioScoreItem(BaseModel):
    symbol: str
    raw: str
    score: float


class LinearClampedScoring(BaseModel):
    type: Literal["linear_clamped"]
    worst: float
    best: float

    @model_validator(mode="after")
    def _validate_bounds(self):  # type: ignore[override]
        if not isfinite(self.worst) or not isfinite(self.best):
            raise ValueError("worst and best must be finite numbers")
        if self.best < self.worst:
            raise ValueError("best must be greater than or equal to worst")
        return self


class ScoreComponent(BaseModel):
    lookback_days: int = Field(..., ge=1, le=3650)
    metric: str = Field(..., description="Typ metryki score'u (np. total_return)")
    weight: float = Field(..., gt=0)
    direction: str = Field("desc", pattern="^(asc|desc)$")
    min_value: Optional[float] = Field(default=None)
    max_value: Optional[float] = Field(default=None)
    scoring: Optional[LinearClampedScoring] = None
    normalize: Literal["none", "percentile"] = "none"

    @field_validator("metric")
    @classmethod
    def _validate_metric(cls, value: str) -> str:
        if value not in ALLOWED_SCORE_METRICS:
            raise ValueError(f"metric must be one of {sorted(ALLOWED_SCORE_METRICS)}")
        return value


SCORE_PRESETS: Dict[str, List[ScoreComponent]] = {
    # Ranking jakościowy używany w demie frontendu.
    "quality_score": [
        ScoreComponent(lookback_days=252, metric="total_return", weight=40, direction="desc"),
        ScoreComponent(lookback_days=126, metric="total_return", weight=25, direction="desc"),
        ScoreComponent(lookback_days=252, metric="max_drawdown", weight=20, direction="asc"),
        ScoreComponent(lookback_days=63, metric="volatility", weight=15, direction="asc"),
    ],
}


class UniverseFilters(BaseModel):
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    prefixes: Optional[List[str]] = None
    indices: Optional[List[str]] = None

    @field_validator("include", "exclude", "prefixes", mode="before")
    @classmethod
    def _ensure_list(cls, value):
        if value is None:
            return value
        if isinstance(value, str):
            return [value]
        return list(value)

    @field_validator("include", "exclude", "prefixes")
    @classmethod
    def _cleanup(cls, value):
        if value is None:
            return value
        cleaned: List[str] = []
        for item in value:
            cleaned_item = item.strip()
            if not cleaned_item:
                raise ValueError("filter values must not be empty")
            cleaned.append(cleaned_item)
        return cleaned

    @field_validator("indices", mode="before")
    @classmethod
    def _ensure_indices_list(cls, value):
        if value is None:
            return value
        if isinstance(value, str):
            return [value]
        return list(value)

    @field_validator("indices")
    @classmethod
    def _cleanup_indices(cls, value):
        if value is None:
            return value
        cleaned: List[str] = []
        for item in value:
            cleaned_item = item.strip().upper()
            if not cleaned_item:
                raise ValueError("filter values must not be empty")
            parts = [part.strip() for part in re.split(r"[+&]", cleaned_item) if part.strip()]
            if not parts:
                raise ValueError("filter values must not be empty")
            cleaned.extend(parts)
        return cleaned


class ManualPortfolioConfig(BaseModel):
    symbols: List[str] = Field(..., min_length=1)
    weights: Optional[List[float]] = None

    @model_validator(mode="after")
    def _validate_weights(self):
        if self.weights is not None and len(self.weights) != len(self.symbols):
            raise ValueError("Liczba wag musi odpowiadać liczbie symboli")
        return self


class AutoSelectionConfig(BaseModel):
    top_n: int = Field(..., ge=1, le=5000)
    components: List[ScoreComponent] = Field(..., min_length=1)
    filters: Optional[UniverseFilters] = None
    weighting: str = Field("equal", pattern="^(equal|score)$")
    direction: str = Field("desc", pattern="^(asc|desc)$")
    min_score: Optional[float] = Field(default=None)
    max_score: Optional[float] = Field(default=None)


class BacktestPortfolioRequest(BaseModel):
    start: date = Field(default=date(2015, 1, 1))
    end: Optional[date] = Field(default=None)
    rebalance: str = Field("monthly", pattern="^(none|monthly|quarterly|yearly)$")
    initial_capital: float = Field(default=10000.0, gt=0)
    fee_pct: float = Field(default=0.0, ge=0.0)
    threshold_pct: float = Field(default=0.0, ge=0.0)
    benchmark: Optional[str] = Field(default=None)
    manual: Optional[ManualPortfolioConfig] = None
    auto: Optional[AutoSelectionConfig] = None

    @model_validator(mode="after")
    def _validate_mode(self):
        if self.manual and self.auto:
            raise ValueError("Wybierz tylko jeden tryb: manual lub auto")
        if not self.manual and not self.auto:
            raise ValueError("Wymagany jest tryb manual lub auto")
        return self


class RangeDescriptor(BaseModel):
    min: float
    max: float
    step: Optional[float] = None
    default: Optional[float] = None


class ComponentDescriptor(BaseModel):
    metric: str
    label: str
    description: str
    lookback_days: RangeDescriptor
    weight: RangeDescriptor


class AutoSelectionDescriptor(BaseModel):
    top_n: RangeDescriptor
    weighting_modes: List[str]
    components: List[ComponentDescriptor]
    filters: Dict[str, str]


class ManualSelectionDescriptor(BaseModel):
    description: str
    weights: str


class BacktestPortfolioTooling(BaseModel):
    start: str
    rebalance_modes: List[str]
    manual: ManualSelectionDescriptor
    auto: AutoSelectionDescriptor


class PortfolioScoreRequest(BaseModel):
    auto: AutoSelectionConfig


class ScoreRulePayload(BaseModel):
    metric: str
    weight: float | None = None
    direction: str | None = Field(None, pattern="^(asc|desc)$")
    lookback_days: int | None = Field(None, ge=5, le=3650)
    lookback: int | None = Field(None, ge=5, le=3650)
    min_value: float | None = None
    max_value: float | None = None
    scoring: LinearClampedScoring | None = None
    normalize: Literal["none", "percentile"] | None = None


class ScorePreviewRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    rules: List[ScoreRulePayload] = Field(..., min_length=1)
    limit: Optional[int] = Field(None, ge=1, le=5000)
    universe: Optional[List[str]] = None
    sort: Optional[str] = Field(None, pattern="^(asc|desc)$")
    as_of: Optional[date] = Field(default=None)

    @field_validator("universe", mode="before")
    @classmethod
    def _normalize_universe(cls, value):
        if value is None:
            return value
        if isinstance(value, str):
            tokens = [token.strip() for token in value.split(",") if token.strip()]
            return tokens
        return list(value)


class ScorePreviewRow(BaseModel):
    symbol: str
    raw: str
    score: float
    rank: int
    metrics: Dict[str, float]


class ScorePreviewMissingRow(BaseModel):
    symbol: str
    raw: str
    reason: str


class ScorePreviewResponse(BaseModel):
    name: Optional[str] = None
    as_of: str
    universe_count: int
    rows: List[ScorePreviewRow]
    missing: List[ScorePreviewMissingRow] = Field(default_factory=list)
    meta: Dict[str, object]


class IndexConstituentResponse(BaseModel):
    symbol: str
    raw_symbol: Optional[str] = None
    symbol_base: Optional[str] = None
    company_name: Optional[str] = None
    weight: Optional[float] = None


class IndexPortfolioSnapshotResponse(BaseModel):
    index_code: str
    index_name: Optional[str] = None
    effective_date: str
    constituents: List[IndexConstituentResponse]


class IndexPortfoliosResponse(BaseModel):
    portfolios: List[IndexPortfolioSnapshotResponse]


class IndexHistoryPointResponse(BaseModel):
    date: str
    value: Optional[float] = None
    change_pct: Optional[float] = None


class IndexHistorySeriesResponse(BaseModel):
    index_code: str
    index_name: Optional[str] = None
    points: List[IndexHistoryPointResponse]


class IndexHistoryResponse(BaseModel):
    items: List[IndexHistorySeriesResponse]


class IndexListItemResponse(BaseModel):
    code: str
    name: Optional[str] = None


class IndexListResponse(BaseModel):
    items: List[IndexListItemResponse]


class BenchmarkSymbolResponse(BaseModel):
    symbol: str
    symbol_base: Optional[str] = None
    indices: List[str] = Field(default_factory=list)
    company_name: Optional[str] = None


class BenchmarkSymbolListResponse(BaseModel):
    items: List[BenchmarkSymbolResponse]


class UniverseCandidateResponseItem(BaseModel):
    symbol: str
    name: Optional[str] = None
    isin: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None


class UniverseCandidateListResponse(BaseModel):
    total: int
    items: List[UniverseCandidateResponseItem]


# =========================
# =========================
# /companies – dane o spółkach
# =========================


def _snapshot_sync_state() -> CompanySyncJobStatus:
    with _SYNC_LOCK:
        return _SYNC_STATE.model_copy(deep=True)


def _update_sync_state_from_progress(job_id: str, progress: CompanySyncProgress) -> None:
    with _SYNC_LOCK:
        if _SYNC_STATE.job_id != job_id:
            return
        _SYNC_STATE.stage = progress.stage
        if progress.total is not None:
            _SYNC_STATE.total = progress.total
        _SYNC_STATE.processed = progress.processed
        _SYNC_STATE.synced = progress.synced
        _SYNC_STATE.failed = progress.failed
        _SYNC_STATE.current_symbol = progress.current_symbol
        if progress.message:
            _SYNC_STATE.message = progress.message


def _run_company_sync_job(job_id: str, limit: Optional[int]) -> None:
    global _SYNC_THREAD
    try:
        ch = get_ch()
        columns = _get_company_columns(ch)
        harvester = CompanyDataHarvester()
        result = harvester.sync(
            ch_client=ch,
            table_name=TABLE_COMPANIES,
            columns=columns,
            limit=limit,
            progress_callback=lambda progress: _update_sync_state_from_progress(job_id, progress),
        )
        with _SYNC_LOCK:
            if _SYNC_STATE.job_id == job_id:
                _SYNC_STATE.status = "completed"
                _SYNC_STATE.stage = "finished"
                _SYNC_STATE.finished_at = result.finished_at
                if _SYNC_STATE.total is None:
                    _SYNC_STATE.total = result.fetched
                _SYNC_STATE.processed = max(_SYNC_STATE.processed, result.fetched)
                _SYNC_STATE.synced = result.synced
                _SYNC_STATE.failed = result.failed
                _SYNC_STATE.errors = list(result.errors)
                _SYNC_STATE.result = result
                if not _SYNC_STATE.message:
                    _SYNC_STATE.message = "Synchronizacja zakończona"
        with _SCHEDULE_LOCK:
            if _SYNC_SCHEDULE_STATE.last_run_status == "running":
                finished_at = result.finished_at or datetime.utcnow()
                _SYNC_SCHEDULE_STATE.last_run_finished_at = finished_at
                _SYNC_SCHEDULE_STATE.last_run_status = "success"
        _notify_schedule_loop()
    except Exception as exc:  # pragma: no cover - zależy od środowiska uruch.
        with _SYNC_LOCK:
            if _SYNC_STATE.job_id == job_id:
                _SYNC_STATE.status = "failed"
                _SYNC_STATE.stage = "failed"
                _SYNC_STATE.finished_at = datetime.utcnow()
                _SYNC_STATE.message = str(exc)
                existing_errors = list(_SYNC_STATE.errors)
                existing_errors.append(str(exc))
                _SYNC_STATE.errors = existing_errors
        with _SCHEDULE_LOCK:
            if _SYNC_SCHEDULE_STATE.last_run_status == "running":
                _SYNC_SCHEDULE_STATE.last_run_finished_at = datetime.utcnow()
                _SYNC_SCHEDULE_STATE.last_run_status = "failed"
        _notify_schedule_loop()
    finally:
        with _SYNC_LOCK:
            _SYNC_THREAD = None


@api_router.post("/companies/sync/background", response_model=CompanySyncJobStatus)
def start_company_sync(
    limit: Optional[int] = Query(default=None, ge=1, le=5000),
) -> CompanySyncJobStatus:
    with _SYNC_LOCK:
        if _SYNC_STATE.status == "running":
            raise HTTPException(409, "Synchronizacja spółek jest już w toku")
        _start_sync_job(limit, message="Rozpoczęto synchronizację spółek")
        return _SYNC_STATE.model_copy(deep=True)


@api_router.get("/companies/sync/status", response_model=CompanySyncJobStatus)
def company_sync_status() -> CompanySyncJobStatus:
    return _snapshot_sync_state()


@api_router.get("/companies/sync/schedule", response_model=CompanySyncScheduleStatus)
def company_sync_schedule() -> CompanySyncScheduleStatus:
    return _snapshot_schedule_state()


@api_router.post("/companies/sync/schedule", response_model=CompanySyncScheduleStatus)
def update_company_sync_schedule(payload: CompanySyncScheduleRequest) -> CompanySyncScheduleStatus:
    now = datetime.utcnow()

    if payload.mode == "cancel":
        with _SCHEDULE_LOCK:
            _SYNC_SCHEDULE_STATE.mode = "idle"
            _SYNC_SCHEDULE_STATE.next_run_at = None
            _SYNC_SCHEDULE_STATE.recurring_interval_minutes = None
            _SYNC_SCHEDULE_STATE.recurring_start_at = None
        _notify_schedule_loop()
        return _snapshot_schedule_state()

    if payload.mode == "once":
        scheduled_for = _normalize_datetime(payload.scheduled_for)  # type: ignore[arg-type]
        if scheduled_for <= now:
            raise HTTPException(400, "Termin jednorazowej synchronizacji musi być w przyszłości")
        with _SCHEDULE_LOCK:
            _SYNC_SCHEDULE_STATE.mode = "once"
            _SYNC_SCHEDULE_STATE.next_run_at = scheduled_for
            _SYNC_SCHEDULE_STATE.recurring_interval_minutes = None
            _SYNC_SCHEDULE_STATE.recurring_start_at = None
        _ensure_schedule_thread_running()
        _notify_schedule_loop()
        return _snapshot_schedule_state()

    interval = payload.interval_minutes or 0
    if interval <= 0:
        raise HTTPException(400, "Interwał synchronizacji musi być dodatni")

    start_at_source = payload.start_at or (now + timedelta(minutes=interval))
    start_at = _normalize_datetime(start_at_source)
    if start_at <= now:
        start_at = now + timedelta(seconds=5)

    with _SCHEDULE_LOCK:
        _SYNC_SCHEDULE_STATE.mode = "recurring"
        _SYNC_SCHEDULE_STATE.recurring_interval_minutes = interval
        _SYNC_SCHEDULE_STATE.recurring_start_at = start_at
        _SYNC_SCHEDULE_STATE.next_run_at = start_at
    _ensure_schedule_thread_running()
    _notify_schedule_loop()
    return _snapshot_schedule_state()


@api_router.post("/companies/sync", response_model=CompanySyncResult)
def sync_companies(
    limit: Optional[int] = Query(default=None, ge=1, le=5000),
    run_as_admin: bool = Query(
        default=False, description="Czy wykonać synchronizację w trybie administratora"
    ),
):
    ch = get_ch()
    columns = _get_company_columns(ch)
    harvester = CompanyDataHarvester()
    run_as_admin_value = (
        bool(run_as_admin.default)
        if isinstance(run_as_admin, QueryParam)
        else bool(run_as_admin)
    )
    result = harvester.sync(
        ch_client=ch,
        table_name=TABLE_COMPANIES,
        columns=columns,
        limit=limit,
        run_as_admin=run_as_admin_value,
    )
    return result


@api_router.get("/companies", response_model=List[CompanyProfile])
def list_companies(
    q: Optional[str] = Query(
        default=None, description="Fragment symbolu, nazwy, branży lub ISIN spółki."
    ),
    limit: int = Query(default=500, ge=1, le=5000),
):
    ch = get_ch()

    columns = _get_company_columns(ch)
    lowered_to_original = {col.lower(): col for col in columns}

    symbol_column = None
    for candidate in COMPANY_SYMBOL_CANDIDATES:
        existing = lowered_to_original.get(candidate)
        if existing:
            symbol_column = existing
            break

    if not symbol_column:
        raise HTTPException(
            500,
            f"Tabela {TABLE_COMPANIES} musi zawierać kolumnę z symbolem (np. symbol lub ticker)",
        )

    searchable_columns = [symbol_column]
    for candidate in COMPANY_NAME_CANDIDATES + ["isin", "sector", "industry"]:
        existing = lowered_to_original.get(candidate)
        if existing and existing not in searchable_columns:
            searchable_columns.append(existing)

    where_clause = ""
    params: Dict[str, Any] = {"limit": limit}
    if q:
        params["q"] = q
        conditions = [
            f"positionCaseInsensitive({_quote_identifier(col)}, %(q)s) > 0"
            for col in searchable_columns
        ]
        where_clause = " WHERE " + " OR ".join(conditions)

    order_expr = _quote_identifier(symbol_column)
    sql = (
        f"SELECT * FROM {TABLE_COMPANIES}{where_clause} "
        f"ORDER BY {order_expr} LIMIT %(limit)s"
    )

    try:
        result = ch.query(sql, parameters=params)
    except Exception as exc:  # pragma: no cover - zależy od konfiguracji DB
        raise HTTPException(500, f"Nie udało się pobrać danych spółek: {exc}") from exc

    column_names = list(result.column_names)
    output: List[CompanyProfile] = []

    for row in result.result_rows:
        raw_row = {col: value for col, value in zip(column_names, row)}
        normalized = _normalize_company_row(raw_row, symbol_column)
        if not normalized:
            continue

        fundamentals_payload = normalized.pop("fundamentals", {})
        fundamentals_model = CompanyFundamentals(**fundamentals_payload)
        profile = CompanyProfile(fundamentals=fundamentals_model, **normalized)
        output.append(profile)

    return output


@api_router.get("/companies/{symbol}", response_model=CompanyProfile)
def get_company_profile(symbol: str) -> CompanyProfile:
    ch = get_ch()
    columns = _get_company_columns(ch)
    lowered_to_original = {col.lower(): col for col in columns}

    symbol_column = None
    for candidate in COMPANY_SYMBOL_CANDIDATES:
        existing = lowered_to_original.get(candidate)
        if existing:
            symbol_column = existing
            break

    if not symbol_column:
        raise HTTPException(
            500,
            f"Tabela {TABLE_COMPANIES} musi zawierać kolumnę z symbolem (np. symbol lub ticker)",
        )

    raw_symbol = normalize_input_symbol(symbol)
    sql = (
        f"SELECT * FROM {TABLE_COMPANIES} "
        f"WHERE upper({_quote_identifier(symbol_column)}) = %(symbol)s "
        f"LIMIT 1"
    )
    params = {"symbol": raw_symbol.upper()}

    try:
        result = ch.query(sql, parameters=params)
    except Exception as exc:  # pragma: no cover - zależy od konfiguracji DB
        raise HTTPException(500, f"Nie udało się pobrać danych spółki: {exc}") from exc

    if not result.result_rows:
        raise HTTPException(404, f"Nie znaleziono spółki o symbolu {symbol}")

    column_names = list(result.column_names)
    raw_row = {col: value for col, value in zip(column_names, result.result_rows[0])}
    normalized = _normalize_company_row(raw_row, symbol_column)
    if not normalized:
        raise HTTPException(404, f"Nie znaleziono spółki o symbolu {symbol}")

    fundamentals_payload = normalized.pop("fundamentals", {})
    fundamentals_model = CompanyFundamentals(**fundamentals_payload)
    profile = CompanyProfile(fundamentals=fundamentals_model, **normalized)
    return profile


@api_router.post("/companies/benchmark-symbol", response_model=CompanyProfile)
def update_company_benchmark_symbol(
    payload: CompanyBenchmarkSymbolUpdateRequest,
) -> CompanyProfile:
    ch = get_ch()
    columns = _get_company_columns(ch)
    symbol_column = _find_company_symbol_column(columns)
    if not symbol_column:
        raise HTTPException(
            500,
            f"Tabela {TABLE_COMPANIES} musi zawierać kolumnę z symbolem (np. symbol lub ticker)",
        )

    benchmark_column = _ensure_company_benchmark_column(ch, columns)
    normalized_symbol = normalize_input_symbol(payload.symbol)
    if not normalized_symbol:
        raise HTTPException(400, "Symbol spółki nie może być pusty")

    try:
        existing_profile = get_company_profile(payload.symbol)
    except HTTPException as exc:
        if exc.status_code == 404:
            raise HTTPException(404, f"Nie znaleziono spółki o symbolu {payload.symbol}") from exc
        raise

    raw_symbol = existing_profile.raw_symbol.strip()
    if not raw_symbol:
        raise HTTPException(500, "Nie udało się ustalić symbolu spółki w bazie")

    benchmark_symbol = _normalize_benchmark_symbol(payload.benchmark_symbol)
    value_expr = "NULL" if benchmark_symbol is None else _quote_sql_string(benchmark_symbol)
    where_expr = _quote_sql_string(raw_symbol)
    update_sql = (
        f"ALTER TABLE {TABLE_COMPANIES} UPDATE {_quote_identifier(benchmark_column)} = {value_expr} "
        f"WHERE {_quote_identifier(symbol_column)} = {where_expr}"
    )

    try:
        ch.command(update_sql)
    except Exception as exc:  # pragma: no cover - zależy od konfiguracji DB
        raise HTTPException(
            500,
            f"Nie udało się zaktualizować symbolu GPW Benchmark: {exc}",
        ) from exc

    global _COMPANY_SYMBOL_LOOKUP
    _COMPANY_SYMBOL_LOOKUP = None

    return get_company_profile(raw_symbol)


# =========================
# /symbols – lista tickerów
# =========================

@api_router.get("/symbols")
def symbols(
    q: Optional[str] = Query(default=None, description="fragment symbolu"),
    limit: int = Query(default=200, ge=1, le=2000),
):
    """
    Zwraca listę symboli:
    - symbol: ładny ticker (np. CDR.WA)
    - raw: surowy symbol w bazie (np. CDPROJEKT)
    - dodatkowe pola identyfikacyjne (ticker, code, isin, nazwy) jeśli dostępne
    """
    ch = get_ch()
    if q:
        rows = ch.query(
            f"""
            SELECT DISTINCT symbol
            FROM {TABLE_OHLC}
            WHERE positionCaseInsensitive(symbol, %(q)s) > 0
            ORDER BY symbol
            LIMIT %(limit)s
            """,
            parameters={"q": q, "limit": limit},
        ).result_rows
    else:
        rows = ch.query(
            f"""
            SELECT DISTINCT symbol
            FROM {TABLE_OHLC}
            ORDER BY symbol
            LIMIT %(limit)s
            """,
            parameters={"limit": limit},
        ).result_rows

    if not rows:
        return []

    normalized_lookup: Dict[str, Dict[str, Optional[str]]] = {}
    output_rows = []
    lookup_keys: Set[str] = set()

    for r in rows:
        raw = str(r[0])
        pretty = pretty_symbol(raw)
        normalized = normalize_input_symbol(raw)
        output_rows.append({"symbol": pretty, "raw": raw})
        if normalized:
            lookup_keys.add(normalized.upper())
        lookup_keys.add(raw.upper())
        lookup_keys.add(pretty.upper())

    # Spróbuj wzbogacić wynik o dane z tabeli spółek, jeśli są dostępne.
    try:
        columns = _get_company_columns(ch)
    except HTTPException:
        columns = []

    lowered_to_original = {col.lower(): col for col in columns}

    symbol_column: Optional[str] = None
    for candidate in COMPANY_SYMBOL_CANDIDATES:
        existing = lowered_to_original.get(candidate)
        if existing:
            symbol_column = existing
            break

    detail_candidates = [
        "ticker",
        "code",
        "isin",
        "name",
        "company_name",
        "full_name",
        "short_name",
    ]

    selected_columns: List[str] = []
    if symbol_column:
        selected_columns.append(symbol_column)
        for candidate in detail_candidates:
            existing = lowered_to_original.get(candidate)
            if existing and existing not in selected_columns:
                selected_columns.append(existing)

    if symbol_column and len(selected_columns) > 1 and lookup_keys:
        select_clause = ", ".join(_quote_identifier(col) for col in selected_columns)
        sql = (
            f"SELECT {select_clause} FROM {TABLE_COMPANIES} "
            f"WHERE upper({_quote_identifier(symbol_column)}) IN %(symbols)s"
        )
        try:
            result = ch.query(sql, parameters={"symbols": tuple(lookup_keys)})
        except Exception:
            result = None

        if result is not None:
            column_names = list(getattr(result, "column_names", []))
            for row in getattr(result, "result_rows", []):
                raw_row = {col: value for col, value in zip(column_names, row)}
                symbol_value = raw_row.get(symbol_column)
                if symbol_value is None:
                    continue
                normalized = normalize_input_symbol(str(_convert_clickhouse_value(symbol_value)))
                if not normalized:
                    continue
                key = normalized.upper()
                entry: Dict[str, Optional[str]] = {}
                for candidate in detail_candidates:
                    column = lowered_to_original.get(candidate)
                    if not column:
                        continue
                    value = raw_row.get(column)
                    text = str(_convert_clickhouse_value(value)).strip() if value is not None else ""
                    if text:
                        entry[candidate] = text
                if entry:
                    normalized_lookup[key] = entry

    ticker_like_pattern = re.compile(r"^[0-9A-Z]{1,8}(?:[._-][0-9A-Z]{1,8})?$")

    def _clean_text(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    for entry in output_rows:
        normalized = normalize_input_symbol(entry["raw"])
        if normalized:
            enriched = normalized_lookup.get(normalized.upper())
            if enriched:
                entry.update(enriched)

        symbol_value = _clean_text(entry.get("symbol")) or ""
        raw_value = _clean_text(entry.get("raw")) or ""
        symbol_upper = symbol_value.upper()
        raw_upper = raw_value.upper()

        preferred: Optional[str] = None
        candidates = (
            entry.get("display"),
            entry.get("ticker"),
            entry.get("code"),
            entry.get("short_name"),
        )
        for candidate in candidates:
            cleaned = _clean_text(candidate)
            if not cleaned:
                continue
            normalized_candidate = cleaned.upper()
            if normalized_candidate in {symbol_upper, raw_upper}:
                continue
            if not ticker_like_pattern.fullmatch(normalized_candidate):
                continue
            preferred = cleaned
            break

        entry["display"] = preferred or symbol_value or raw_value

    return output_rows


# =========================
# /quotes – notowania OHLC
# =========================


def _http_exception_message(exc: HTTPException) -> str:
    detail = exc.detail
    if isinstance(detail, str):
        return detail
    if isinstance(detail, dict):
        for key in ("error", "message", "detail"):
            value = detail.get(key)
            if isinstance(value, str) and value.strip():
                return value
        try:
            return json.dumps(detail, ensure_ascii=False)
        except Exception:  # pragma: no cover - ostrożność
            return str(detail)
    if isinstance(detail, (list, tuple)):
        return "; ".join(str(item) for item in detail if item)
    return str(detail or exc)


@api_router.post("/ohlc/import", response_model=OhlcImportResponse)
async def import_ohlc_file(file: UploadFile = File(...)) -> OhlcImportResponse:
    try:
        content = await file.read()
    finally:
        await file.close()

    if not content:
        raise HTTPException(400, "Przesłany plik jest pusty.")

    buffer = io.BytesIO(content)
    buffer.seek(0)
    try:
        if zipfile.is_zipfile(buffer):
            payload, skipped, errors, total_errors = _parse_mst_archive_payload(content)
        else:
            payload, skipped, errors, total_errors = _parse_ohlc_csv_payload(content)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

    if not payload:
        if errors:
            raise HTTPException(400, "Brak poprawnych wierszy w pliku – sprawdź komunikaty błędów.")
        raise HTTPException(400, "Plik nie zawiera poprawnych danych OHLC.")

    if total_errors > len(errors):
        errors.append(f"… (pominięto {total_errors - len(errors)} kolejnych błędów)")

    try:
        ch = get_ch()
    except Exception as exc:
        raise HTTPException(500, f"Nie udało się nawiązać połączenia z ClickHouse: {exc}") from exc

    try:
        _create_ohlc_table_if_missing(ch)
    except Exception as exc:
        raise HTTPException(500, f"Nie udało się przygotować tabeli notowań: {exc}") from exc

    inserted = 0
    batch_size = 10_000
    for start in range(0, len(payload), batch_size):
        chunk = payload[start : start + batch_size]
        try:
            ch.insert(
                table=TABLE_OHLC,
                data=chunk,
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
        except Exception as exc:
            raise HTTPException(500, f"Nie udało się zapisać danych do ClickHouse: {exc}") from exc
        inserted += len(chunk)

    return OhlcImportResponse(inserted=inserted, skipped=skipped, errors=errors)


def _perform_ohlc_sync(
    payload: OhlcSyncRequest,
    *,
    schedule_mode: Optional[Literal["once", "recurring"]] = None,
) -> OhlcSyncResult:
    try:
        ch = get_ch()
    except Exception as exc:  # pragma: no cover - zależy od konfiguracji DB
        message = f"Nie udało się połączyć z bazą ClickHouse: {exc}"
        OHLC_SYNC_PROGRESS_TRACKER.fail(message)
        raise HTTPException(500, message) from exc

    try:
        _create_ohlc_table_if_missing(ch)
    except Exception as exc:  # pragma: no cover - zależy od konfiguracji DB
        message = f"Nie udało się przygotować tabeli notowań: {exc}"
        OHLC_SYNC_PROGRESS_TRACKER.fail(message)
        raise HTTPException(500, message) from exc

    if payload.truncate and not payload.run_as_admin:
        message = "Czyszczenie tabeli wymaga uprawnień administratora"
        OHLC_SYNC_PROGRESS_TRACKER.fail(message)
        raise HTTPException(403, message)

    if payload.symbols:
        symbols = payload.symbols
    else:
        symbols = _collect_all_company_symbols(ch)
        if not symbols:
            symbols = list(DEFAULT_OHLC_SYNC_SYMBOLS)

    if not symbols:
        message = "Brak symboli do synchronizacji"
        OHLC_SYNC_PROGRESS_TRACKER.fail(message)
        raise HTTPException(400, message)

    deduplicated: List[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        normalized = normalize_input_symbol(raw_symbol)
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        deduplicated.append(normalized)

    if not deduplicated:
        message = "Brak poprawnych symboli do synchronizacji"
        OHLC_SYNC_PROGRESS_TRACKER.fail(message)
        raise HTTPException(400, message)

    OHLC_SYNC_PROGRESS_TRACKER.start(
        total_symbols=len(deduplicated),
        requested_as_admin=payload.run_as_admin,
    )

    harvester = MultiSourceOhlcHarvester()

    def handle_progress(event: OhlcSyncProgressEvent) -> None:
        OHLC_SYNC_PROGRESS_TRACKER.update(
            processed_symbols=event["processed"],
            inserted_rows=event["inserted"],
            skipped_symbols=event["skipped"],
            current_symbol=event.get("current_symbol"),
            errors=event["errors"],
        )

    try:
        result = harvester.sync(
            ch_client=ch,
            table_name=TABLE_OHLC,
            symbols=deduplicated,
            start_date=payload.start,
            truncate=payload.truncate,
            run_as_admin=payload.run_as_admin,
            progress_callback=handle_progress,
        )
    except HTTPException as exc:
        OHLC_SYNC_PROGRESS_TRACKER.fail(_http_exception_message(exc))
        if schedule_mode:
            with _OHLC_SCHEDULE_LOCK:
                if _OHLC_SCHEDULE_STATE.last_run_status == "running":
                    _OHLC_SCHEDULE_STATE.last_run_finished_at = datetime.utcnow()
                    _OHLC_SCHEDULE_STATE.last_run_status = "failed"
            _notify_ohlc_schedule_loop()
        raise
    except Exception as exc:
        message = f"Nieoczekiwany błąd synchronizacji notowań: {exc}"
        OHLC_SYNC_PROGRESS_TRACKER.fail(message)
        if schedule_mode:
            with _OHLC_SCHEDULE_LOCK:
                if _OHLC_SCHEDULE_STATE.last_run_status == "running":
                    _OHLC_SCHEDULE_STATE.last_run_finished_at = datetime.utcnow()
                    _OHLC_SCHEDULE_STATE.last_run_status = "failed"
            _notify_ohlc_schedule_loop()
        raise HTTPException(500, message) from exc

    OHLC_SYNC_PROGRESS_TRACKER.finish(result)
    if schedule_mode:
        with _OHLC_SCHEDULE_LOCK:
            if _OHLC_SCHEDULE_STATE.last_run_status == "running":
                finished_at = result.finished_at or datetime.utcnow()
                _OHLC_SCHEDULE_STATE.last_run_finished_at = finished_at
                _OHLC_SCHEDULE_STATE.last_run_status = "success"
        _notify_ohlc_schedule_loop()
    return result


@api_router.post("/ohlc/sync", response_model=OhlcSyncResult)
def sync_ohlc(payload: OhlcSyncRequest) -> OhlcSyncResult:
    return _perform_ohlc_sync(payload)


def _run_ohlc_sync_in_background(
    payload: OhlcSyncRequest,
    *,
    schedule_mode: Optional[Literal["once", "recurring"]] = None,
) -> None:
    try:
        _perform_ohlc_sync(payload, schedule_mode=schedule_mode)
    except HTTPException:
        # Błąd został już zapisany w trackerze – nie ponownie podnosimy wyjątku.
        return


@api_router.post("/ohlc/sync/background", status_code=202)
def sync_ohlc_background(payload: OhlcSyncRequest):
    snapshot = OHLC_SYNC_PROGRESS_TRACKER.snapshot()
    if snapshot.status == "running":
        raise HTTPException(409, "Synchronizacja notowań jest już w toku")

    payload_copy = OhlcSyncRequest.model_validate(payload.model_dump())

    thread = threading.Thread(
        target=_run_ohlc_sync_in_background,
        args=(payload_copy,),
        daemon=True,
    )
    thread.start()

    return {"status": "accepted"}


@api_router.get("/ohlc/sync/progress", response_model=OhlcSyncProgress)
def sync_ohlc_progress() -> OhlcSyncProgress:
    return OHLC_SYNC_PROGRESS_TRACKER.snapshot()


@api_router.get("/ohlc/sync/schedule", response_model=OhlcSyncScheduleStatus)
def ohlc_sync_schedule() -> OhlcSyncScheduleStatus:
    return _snapshot_ohlc_schedule_state()


@api_router.post("/ohlc/sync/schedule", response_model=OhlcSyncScheduleStatus)
def update_ohlc_sync_schedule(payload: OhlcSyncScheduleRequest) -> OhlcSyncScheduleStatus:
    now = datetime.utcnow()

    if payload.mode == "cancel":
        with _OHLC_SCHEDULE_LOCK:
            _OHLC_SCHEDULE_STATE.mode = "idle"
            _OHLC_SCHEDULE_STATE.next_run_at = None
            _OHLC_SCHEDULE_STATE.recurring_interval_minutes = None
            _OHLC_SCHEDULE_STATE.recurring_start_at = None
            _OHLC_SCHEDULE_STATE.options = None
        _notify_ohlc_schedule_loop()
        return _snapshot_ohlc_schedule_state()

    if payload.mode == "once":
        scheduled_for = _normalize_datetime(payload.scheduled_for)  # type: ignore[arg-type]
        if scheduled_for <= now:
            raise HTTPException(400, "Termin jednorazowej synchronizacji musi być w przyszłości")
        options = payload.options.model_copy(deep=True) if payload.options else None
        if options is None:
            raise HTTPException(400, "Brak konfiguracji synchronizacji notowań")
        with _OHLC_SCHEDULE_LOCK:
            _OHLC_SCHEDULE_STATE.mode = "once"
            _OHLC_SCHEDULE_STATE.next_run_at = scheduled_for
            _OHLC_SCHEDULE_STATE.recurring_interval_minutes = None
            _OHLC_SCHEDULE_STATE.recurring_start_at = None
            _OHLC_SCHEDULE_STATE.options = options
        _ensure_ohlc_schedule_thread_running()
        _notify_ohlc_schedule_loop()
        return _snapshot_ohlc_schedule_state()

    interval = payload.interval_minutes or 0
    if interval <= 0:
        raise HTTPException(400, "Interwał synchronizacji musi być dodatni")
    options = payload.options.model_copy(deep=True) if payload.options else None
    if options is None:
        raise HTTPException(400, "Brak konfiguracji synchronizacji notowań")

    start_at_source = payload.start_at or (now + timedelta(minutes=interval))
    start_at = _normalize_datetime(start_at_source)
    if start_at <= now:
        start_at = now + timedelta(seconds=5)

    with _OHLC_SCHEDULE_LOCK:
        _OHLC_SCHEDULE_STATE.mode = "recurring"
        _OHLC_SCHEDULE_STATE.recurring_interval_minutes = interval
        _OHLC_SCHEDULE_STATE.recurring_start_at = start_at
        _OHLC_SCHEDULE_STATE.next_run_at = start_at
        _OHLC_SCHEDULE_STATE.options = options
    _ensure_ohlc_schedule_thread_running()
    _notify_ohlc_schedule_loop()
    return _snapshot_ohlc_schedule_state()


def _resolve_symbol_for_quotes(ch_client, value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return ""

    symbol_lookup = _build_company_symbol_lookup(ch_client)
    if symbol_lookup:
        direct = symbol_lookup.get(cleaned.upper())
        if direct:
            return direct

        for alias in _iter_symbol_aliases(cleaned):
            key = alias.strip().upper()
            if not key:
                continue
            resolved = symbol_lookup.get(key)
            if resolved:
                return resolved

    normalized = normalize_input_symbol(cleaned)
    if normalized:
        if symbol_lookup:
            lookup_key = normalized.strip().upper()
            if lookup_key:
                resolved = symbol_lookup.get(lookup_key)
                if resolved:
                    return resolved
        return normalized

    return ""


@api_router.get("/quotes", response_model=List[QuoteRow])
def quotes(symbol: str, start: Optional[str] = None):
    """
    Zwraca notowania OHLC dla symbolu od wskazanej daty.
    Obsługuje zarówno 'CDR.WA' jak i 'CDPROJEKT'.
    """
    ch = get_ch()
    raw_symbol = _resolve_symbol_for_quotes(ch, symbol)
    if not raw_symbol:
        raise HTTPException(400, "symbol must not be empty")

    try:
        dt = date.fromisoformat(start) if start else date(2015, 1, 1)
    except Exception:
        raise HTTPException(400, "start must be in format YYYY-MM-DD")

    rows = ch.query(
        f"""
        SELECT toString(date) as date, open, high, low, close, volume
        FROM {TABLE_OHLC}
        WHERE symbol = %(sym)s AND date >= %(dt)s
        ORDER BY date
        """,
        parameters={"sym": raw_symbol, "dt": dt},
    ).named_results()

    out: List[QuoteRow] = []
    for r in rows:
        out.append(
            QuoteRow(
                date=str(r["date"]),
                open=float(r["open"]),
                high=float(r["high"]),
                low=float(r["low"]),
                close=float(r["close"]),
                volume=float(r["volume"]),
            )
        )
    return out


@api_router.get("/data-collection", response_model=List[DataCollectionItem])
def collect_data(
    symbols: List[str] = Query(
        ...,
        description="Lista symboli, dla których mają zostać pobrane notowania",
    ),
    start: Optional[str] = Query(
        default=None, description="Początek zakresu w formacie YYYY-MM-DD"
    ),
    end: Optional[str] = Query(
        default=None, description="Koniec zakresu w formacie YYYY-MM-DD"
    ),
):
    """Zwraca listę notowań dla wielu spółek jednocześnie."""

    if not symbols:
        raise HTTPException(400, "symbols must not be empty")

    try:
        start_dt = date.fromisoformat(start) if start else None
    except ValueError as exc:
        raise HTTPException(400, "start must be in format YYYY-MM-DD") from exc

    try:
        end_dt = date.fromisoformat(end) if end else None
    except ValueError as exc:
        raise HTTPException(400, "end must be in format YYYY-MM-DD") from exc

    if start_dt and end_dt and end_dt < start_dt:
        raise HTTPException(400, "end must not be earlier than start")

    items: List[DataCollectionItem] = []
    for raw_input in symbols:
        raw_symbol = normalize_input_symbol(raw_input)
        if not raw_symbol:
            raise HTTPException(400, "symbol must not be empty")

        rows = quotes(symbol=raw_input, start=start)
        if end_dt:
            rows = [
                row for row in rows if date.fromisoformat(row.date) <= end_dt
            ]

        items.append(
            DataCollectionItem(
                symbol=pretty_symbol(raw_symbol),
                raw=raw_symbol,
                quotes=rows,
            )
        )

    return items


@api_router.get("/sectors/classification", response_model=List[SectorClassificationEntry])
def list_sector_classification() -> List[SectorClassificationEntry]:
    """Zwraca hierarchiczną klasyfikację sektorową GPW."""

    return [SectorClassificationEntry(**item) for item in GPW_SECTOR_CLASSIFICATION]


# =========================
# /backtest/portfolio
# =========================

def _fetch_close_series(
    ch_client,
    raw_symbol: str,
    start: date,
    end: Optional[date] = None,
) -> List[Tuple[str, float]]:
    """
    Pobiera (date, close) dla symbolu od daty start (opcjonalnie do daty końcowej).
    """

    where_clause = "symbol = %(sym)s AND date >= %(dt_start)s"
    params: Dict[str, object] = {"sym": raw_symbol, "dt_start": start}
    if end is not None:
        where_clause += " AND date <= %(dt_end)s"
        params["dt_end"] = end

    rows = ch_client.query(
        f"""
        SELECT toString(date) AS date, close
        FROM {TABLE_OHLC}
        WHERE {where_clause}
        ORDER BY date
        """,
        parameters=params,
    ).result_rows
    return [(str(d), float(c)) for (d, c) in rows]


def _fetch_close_history(ch_client, raw_symbol: str) -> List[Tuple[str, float]]:
    rows = ch_client.query(
        f"""
        SELECT toString(date) AS date, close
        FROM {TABLE_OHLC}
        WHERE symbol = %(sym)s
        ORDER BY date
        """,
        parameters={"sym": raw_symbol},
    ).result_rows
    return [(str(d), float(c)) for (d, c) in rows]


def _collect_close_history_bulk(
    ch_client,
    symbols: Sequence[str],
    components: Sequence[ScoreComponent],
    *,
    as_of: Optional[date] = None,
) -> Dict[str, List[Tuple[str, float]]]:
    """Fetches close history for a batch of symbols using a single ClickHouse query."""

    unique_symbols = []
    seen = set()
    for sym in symbols:
        if sym and sym not in seen:
            seen.add(sym)
            unique_symbols.append(sym)

    if not unique_symbols or not components:
        return {sym: [] for sym in unique_symbols}

    max_lookback = max(component.lookback_days for component in components)
    # Add a small buffer to account for non-trading days and ensure we have
    # sufficient history for metrics relying on older prices.
    window = int(max_lookback + 30)

    params: Dict[str, object] = {"symbols": tuple(unique_symbols), "window": window}
    date_filter = ""
    if as_of is not None:
        params["as_of"] = as_of.isoformat()
        date_filter = " AND date <= %(as_of)s"

    rows = ch_client.query(
        f"""
        WITH latest AS (
            SELECT symbol, max(date) AS last_date
            FROM {TABLE_OHLC}
            WHERE symbol IN %(symbols)s{date_filter}
            GROUP BY symbol
        )
        SELECT o.symbol, toString(o.date) AS date, o.close
        FROM {TABLE_OHLC} AS o
        INNER JOIN latest AS l ON o.symbol = l.symbol
        WHERE o.symbol IN %(symbols)s
          AND o.date <= l.last_date
          AND o.date >= addDays(l.last_date, -%(window)s)
        ORDER BY o.symbol, o.date
        """,
        parameters=params,
    ).result_rows

    history: Dict[str, List[Tuple[str, float]]] = {sym: [] for sym in unique_symbols}
    for raw_symbol, raw_date, close in rows:
        history.setdefault(str(raw_symbol), []).append((str(raw_date), float(close)))

    # Ensure all requested symbols are present in the mapping, even if no rows
    # were returned for them.
    for sym in unique_symbols:
        history.setdefault(sym, [])

    return history


def _ensure_date(value: object) -> date:
    """Przekształca różne reprezentacje daty na ``datetime.date``."""

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return datetime.fromisoformat(value).date()
    raise ValueError(f"Unsupported date value: {value!r}")


def _prepare_metric_series(
    closes: List[Tuple[str, float]]
) -> List[Tuple[date, float]]:
    """Konwertuje listę (data, close) na format dogodny do obliczania metryk."""

    prepared: List[Tuple[date, float]] = []
    append = prepared.append
    for raw_date, raw_close in closes:
        try:
            dt = _ensure_date(raw_date)
        except ValueError:
            continue
        try:
            close = float(raw_close)
        except (TypeError, ValueError):
            continue
        append((dt, close))
    return prepared


def _slice_closes_window(
    closes: Sequence[Tuple[date, float]], lookback_days: int
) -> List[Tuple[date, float]]:
    if not closes:
        return []

    last_dt, _ = closes[-1]
    min_dt = last_dt - timedelta(days=lookback_days)

    window: List[Tuple[date, float]] = []
    for dt, close in closes:
        if dt < min_dt or close <= 0:
            continue
        window.append((dt, close))

    return window


def _compute_metric_value(
    closes: Sequence[Tuple[date, float]], metric: str, lookback_days: int, *, diagnose: bool = False
) -> Optional[float]:
    if not closes:
        if diagnose:
            raise ScoreComputationError(
                "Brak danych notowań do obliczenia metryk score."
            )
        return None

    if metric in {"total_return", "price_change"}:
        last_dt, last_close = closes[-1]
        if last_close <= 0:
            if diagnose:
                raise ScoreComputationError(
                    "Ostatnia cena zamknięcia jest niepoprawna lub niedostępna."
                )
            return None
        target_dt = last_dt - timedelta(days=lookback_days)
        base_close = None
        for dt, close in reversed(closes):
            if dt <= target_dt:
                if close > 0:
                    base_close = close
                break
        if base_close is None or base_close <= 0:
            if diagnose:
                raise ScoreComputationError(
                    (
                        "Brak danych sprzed {days} dni do wyliczenia metryki "
                        "{metric}."
                    ).format(days=lookback_days, metric=metric)
                )
            return None
        result = (last_close / base_close) - 1.0
        if metric == "price_change":
            return result * 100.0
        return result

    window = _slice_closes_window(closes, lookback_days)
    if len(window) < 2:
        if diagnose:
            raise ScoreComputationError(
                (
                    "Za mało notowań ({count}) do obliczenia metryki {metric} "
                    "w horyzoncie {days} dni."
                ).format(count=len(window), metric=metric, days=lookback_days)
            )
        return None

    returns: List[float] = []
    for (_, prev_close), (_, next_close) in zip(window, window[1:]):
        if prev_close <= 0:
            continue
        returns.append(next_close / prev_close - 1.0)

    if metric == "volatility":
        if len(returns) < 2:
            if diagnose:
                raise ScoreComputationError(
                    "Za mało prawidłowych stóp zwrotu do obliczenia zmienności."
                )
            return None
        return statistics.pstdev(returns)

    if metric == "max_drawdown":
        peak = window[0][1]
        max_dd = 0.0
        for _, price in window:
            if price > peak:
                peak = price
            drawdown = price / peak - 1.0
            if drawdown < max_dd:
                max_dd = drawdown
        return abs(max_dd)

    if metric == "sharpe":
        if len(returns) < 2:
            if diagnose:
                raise ScoreComputationError(
                    "Za mało prawidłowych stóp zwrotu do obliczenia Sharpe'a."
                )
            return None
        avg = statistics.mean(returns)
        stdev = statistics.pstdev(returns)
        if stdev <= 1e-12:
            if diagnose:
                raise ScoreComputationError(
                    "Zmienność w badanym okresie jest zbyt niska do obliczenia Sharpe'a."
                )
            return None
        return (avg / stdev) * sqrt(252)

    if diagnose:
        raise ScoreComputationError(f"Metryka {metric} nie jest obsługiwana.")
    return None


class ScoreComputationError(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _normalize_component_score(value: float, component: ScoreComponent) -> float:
    scoring = component.scoring
    if scoring and scoring.type == "linear_clamped":
        worst = scoring.worst
        best = scoring.best
        if best == worst:
            if component.direction == "asc":
                return 1.0 if value < best else 0.0
            return 1.0 if value > best else 0.0

        if value <= worst:
            score = 0.0
        elif value >= best:
            score = 1.0
        else:
            score = (value - worst) / (best - worst)

        if component.direction == "asc":
            return 1.0 - score
        return score

    min_value = component.min_value
    max_value = component.max_value
    if (
        min_value is not None
        and max_value is not None
        and isfinite(min_value)
        and isfinite(max_value)
        and max_value > min_value
    ):
        if component.direction == "asc":
            ratio = (max_value - value) / (max_value - min_value)
        else:
            ratio = (value - min_value) / (max_value - min_value)
        if ratio <= 0.0:
            return 0.0
        if ratio >= 1.0:
            return 1.0
        return ratio

    direction = -1.0 if component.direction == "asc" else 1.0
    return direction * value


def _evaluate_components_for_prepared(
    closes: Sequence[Tuple[date, float]],
    components: List[ScoreComponent],
    *,
    include_metrics: bool = False,
) -> Optional[Tuple[List[float], Optional[Dict[str, float]]]]:
    return _evaluate_components_internal(
        closes,
        components,
        include_metrics=include_metrics,
        diagnose_failure=False,
    )


def _evaluate_components_internal(
    closes: Sequence[Tuple[date, float]],
    components: List[ScoreComponent],
    *,
    include_metrics: bool,
    diagnose_failure: bool,
) -> Optional[Tuple[List[float], Optional[Dict[str, float]]]]:
    if not closes:
        if diagnose_failure:
            raise ScoreComputationError("Brak notowań dla wybranej spółki.")
        return None

    metrics: Optional[Dict[str, float]] = {} if include_metrics else None
    adjusted_scores: List[float] = []

    for comp in components:
        try:
            value = _compute_metric_value(
                closes,
                comp.metric,
                comp.lookback_days,
                diagnose=diagnose_failure,
            )
        except ScoreComputationError:
            if diagnose_failure:
                raise
            return None
        if value is None:
            if diagnose_failure:
                raise ScoreComputationError(
                    (
                        "Nie udało się obliczyć metryki {metric} w horyzoncie {days} dni."
                    ).format(metric=comp.metric, days=comp.lookback_days)
                )
            return None

        if metrics is not None:
            key = f"{comp.metric}_{comp.lookback_days}"
            metrics[key] = value

        adjusted = _normalize_component_score(value, comp)
        adjusted_scores.append(adjusted)

    return adjusted_scores, metrics


def _calculate_score_from_prepared(
    closes: Sequence[Tuple[date, float]],
    components: List[ScoreComponent],
    include_metrics: bool = False,
) -> Optional[Tuple[float, Dict[str, float]] | float]:
    evaluated = _evaluate_components_for_prepared(
        closes, components, include_metrics=include_metrics
    )
    if evaluated is None:
        return None

    adjusted_scores, metrics = evaluated
    total_weight = sum(comp.weight for comp in components)
    if total_weight <= 0:
        return None

    weighted = 0.0
    for idx, comp in enumerate(components):
        weighted += comp.weight * adjusted_scores[idx]

    score = weighted / total_weight
    if include_metrics:
        return score, metrics or {}
    return score


def _calculate_symbol_score(
    ch_client,
    raw_symbol: str,
    components: List[ScoreComponent],
    include_metrics: bool = False,
    *,
    preloaded_closes: Optional[List[Tuple[str, float]]] = None,
) -> Optional[Tuple[float, Dict[str, float]] | float]:
    if preloaded_closes is not None:
        closes_raw = preloaded_closes
    else:
        closes_raw = _fetch_close_history(ch_client, raw_symbol)
    closes = _prepare_metric_series(closes_raw)
    if not closes:
        return None

    return _calculate_score_from_prepared(
        closes, components, include_metrics=include_metrics
    )


def _collect_all_company_symbols(ch_client) -> Optional[List[str]]:
    company_symbols: set[str] = set()
    try:
        columns = _get_company_columns(ch_client)
    except HTTPException:
        columns = None

    if columns:
        symbol_lookup: Optional[Dict[str, str]] = _build_company_symbol_lookup(ch_client)
        symbol_column = _find_company_symbol_column(columns)
        if symbol_column:
            sql = (
                f"SELECT DISTINCT {_quote_identifier(symbol_column)} "
                f"FROM {TABLE_COMPANIES} "
                f"ORDER BY {_quote_identifier(symbol_column)}"
            )

            try:
                result = ch_client.query(sql)
            except Exception:  # pragma: no cover - zależy od konfiguracji DB
                result = None
            else:
                for row in result.result_rows:
                    if not row:
                        continue
                    raw_value = row[0]
                    if raw_value is None:
                        continue
                    text = str(raw_value)
                    canonical: Optional[str] = None
                    if symbol_lookup:
                        lookup_key = text.strip().upper()
                        if lookup_key:
                            canonical = symbol_lookup.get(lookup_key)
                        if not canonical:
                            normalized_key = normalize_input_symbol(text).strip().upper()
                            if normalized_key:
                                canonical = symbol_lookup.get(normalized_key)
                    if not canonical:
                        canonical = normalize_input_symbol(text)
                    if canonical and not TICKER_LIKE_PATTERN.fullmatch(canonical):
                        canonical = ""
                    if canonical:
                        company_symbols.add(canonical)

    ohlc_symbols: set[str] = set()
    try:
        ohlc_result = ch_client.query(
            f"SELECT DISTINCT symbol FROM {TABLE_OHLC} ORDER BY symbol"
        )
    except Exception:  # pragma: no cover - zależy od konfiguracji DB
        ohlc_result = None
    else:
        for row in ohlc_result.result_rows:
            if not row:
                continue
            raw_value = row[0]
            if raw_value is None:
                continue
            normalized = normalize_input_symbol(str(raw_value))
            if normalized:
                ohlc_symbols.add(normalized)

    combined = sorted(company_symbols | ohlc_symbols)
    if combined:
        return combined
    if company_symbols:
        return sorted(company_symbols)
    if ohlc_symbols:
        return sorted(ohlc_symbols)
    return None


def _sanitize_index_code(value: str) -> str:
    cleaned = "".join(ch for ch in value.upper() if ch.isalnum() or ch in {"_", "-"})
    return cleaned


def _normalize_index_member_symbol(
    symbol_raw: object,
    symbol_base_raw: object = None,
    *,
    symbol_lookup: Optional[Dict[str, str]] = None,
) -> str:
    """Return the normalized GPW ticker for index membership entries."""

    candidates: List[str] = []

    for source in (symbol_raw, symbol_base_raw):
        if not source:
            continue
        text = str(source).strip()
        if not text:
            continue
        candidates.append(text)
        if text.upper().endswith(".WA"):
            stripped = text[:-3].strip()
            if stripped:
                candidates.append(stripped)

    for candidate in candidates:
        cleaned = candidate.strip()
        if not cleaned:
            continue
        if symbol_lookup:
            for alias in _iter_symbol_aliases(cleaned):
                key = alias.strip().upper()
                if key:
                    resolved = symbol_lookup.get(key)
                    if resolved:
                        return resolved

        normalized = normalize_input_symbol(cleaned)
        if normalized:
            if symbol_lookup:
                lookup_key = normalized.strip().upper()
                if lookup_key:
                    resolved = symbol_lookup.get(lookup_key)
                    if resolved:
                        return resolved
            return normalized

    return ""


def _collect_latest_index_membership(
    ch_client, index_codes: Iterable[str], *, as_of: Optional[date] = None
) -> Dict[str, List[str]]:
    cleaned_codes = [_sanitize_index_code(code) for code in index_codes if _sanitize_index_code(code)]
    if not cleaned_codes:
        return {}
    if as_of is not None:
        timeline_map, _ = _fetch_index_portfolio_history_map(ch_client, cleaned_codes)
        membership: Dict[str, List[str]] = {}
        for code, entries in timeline_map.items():
            latest: Optional[Set[str]] = None
            for entry_date, members in entries:
                if entry_date > as_of:
                    break
                latest = members
            if latest:
                membership[code] = sorted(latest)
        return membership
    _ensure_index_tables(ch_client)
    symbol_lookup = _build_company_symbol_lookup(ch_client)
    in_clause = ", ".join(f"'{code}'" for code in cleaned_codes)
    inner_filter = f"WHERE upper(index_code) IN ({in_clause})"
    outer_filter = f"WHERE upper(p.index_code) IN ({in_clause})"
    query = f"""
        WITH latest AS (
            SELECT index_code, max(effective_date) AS max_date
            FROM {TABLE_INDEX_PORTFOLIOS}
            {inner_filter}
            GROUP BY index_code
        )
        SELECT
            p.index_code,
            p.symbol,
            p.symbol_base
        FROM {TABLE_INDEX_PORTFOLIOS} AS p
        INNER JOIN latest AS l
            ON p.index_code = l.index_code AND p.effective_date = l.max_date
        {outer_filter}
        ORDER BY p.index_code, p.symbol
    """
    try:
        rows = ch_client.query(query).named_results()
    except AttributeError:
        rows = None
    if rows is None:
        rows = [
            {"index_code": row[0], "symbol": row[1], "symbol_base": row[2] if len(row) > 2 else None}
            for row in ch_client.query(query).result_rows
        ]

    membership: Dict[str, List[str]] = {}
    for row in rows:
        if isinstance(row, dict):
            code_raw = row.get("index_code")
            symbol_raw = row.get("symbol")
            symbol_base_raw = row.get("symbol_base")
        else:
            code_raw = row[0]
            symbol_raw = row[1]
            symbol_base_raw = row[2] if len(row) > 2 else None
        if not code_raw or not symbol_raw:
            continue
        code = str(code_raw).upper()
        normalized_symbol = _normalize_index_member_symbol(
            symbol_raw,
            symbol_base_raw,
            symbol_lookup=symbol_lookup,
        )
        if not normalized_symbol:
            continue
        bucket = membership.setdefault(code, [])
        if normalized_symbol not in bucket:
            bucket.append(normalized_symbol)
    return membership


def _fetch_latest_index_portfolios(
    ch_client, index_codes: Optional[Iterable[str]] = None
) -> List[Dict[str, Any]]:
    cleaned: List[str] = []
    if index_codes is not None:
        cleaned = [_sanitize_index_code(code) for code in index_codes if _sanitize_index_code(code)]
    _ensure_index_tables(ch_client)
    if cleaned:
        in_clause = ", ".join(f"'{code}'" for code in cleaned)
        inner_filter = f"WHERE upper(index_code) IN ({in_clause})"
        outer_filter = f"WHERE upper(p.index_code) IN ({in_clause})"
    else:
        inner_filter = ""
        outer_filter = ""

    query = f"""
        WITH latest AS (
            SELECT index_code, max(effective_date) AS max_date
            FROM {TABLE_INDEX_PORTFOLIOS}
            {inner_filter}
            GROUP BY index_code
        )
        SELECT
            p.index_code,
            p.index_name,
            p.effective_date,
            p.symbol,
            p.symbol_base,
            p.company_name,
            p.weight
        FROM {TABLE_INDEX_PORTFOLIOS} AS p
        INNER JOIN latest AS l
            ON p.index_code = l.index_code AND p.effective_date = l.max_date
        {outer_filter}
        ORDER BY p.index_code, p.symbol
    """

    try:
        rows = ch_client.query(query).named_results()
    except AttributeError:
        rows = None
    if rows is None:
        rows = [
            {
                "index_code": row[0],
                "index_name": row[1],
                "effective_date": row[2],
                "symbol": row[3],
                "symbol_base": row[4],
                "company_name": row[5],
                "weight": row[6],
            }
            for row in ch_client.query(query).result_rows
        ]
    normalized_rows: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            normalized_rows.append(dict(row))
            continue
        try:
            normalized_rows.append(dict(row))
            continue
        except (TypeError, ValueError):
            pass
        if isinstance(row, (list, tuple)):
            normalized_rows.append(
                {
                    "index_code": row[0],
                    "index_name": row[1],
                    "effective_date": row[2],
                    "symbol": row[3],
                    "symbol_base": row[4] if len(row) > 4 else None,
                    "company_name": row[5] if len(row) > 5 else None,
                    "weight": row[6] if len(row) > 6 else None,
                }
            )
            continue
        normalized_rows.append({
            "index_code": getattr(row, "index_code", None),
            "index_name": getattr(row, "index_name", None),
            "effective_date": getattr(row, "effective_date", None),
            "symbol": getattr(row, "symbol", None),
            "symbol_base": getattr(row, "symbol_base", None),
            "company_name": getattr(row, "company_name", None),
            "weight": getattr(row, "weight", None),
        })
    return normalized_rows


def _fetch_index_history_rows(
    ch_client,
    index_codes: Optional[Iterable[str]] = None,
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> List[Dict[str, Any]]:
    cleaned: List[str] = []
    if index_codes is not None:
        cleaned = [_sanitize_index_code(code) for code in index_codes if _sanitize_index_code(code)]
    _ensure_index_tables(ch_client)
    conditions: List[str] = []
    if cleaned:
        in_clause = ", ".join(f"'{code}'" for code in cleaned)
        conditions.append(f"upper(index_code) IN ({in_clause})")
    params: Dict[str, Any] = {}
    if start is not None:
        conditions.append("date >= %(start)s")
        params["start"] = start
    if end is not None:
        conditions.append("date <= %(end)s")
        params["end"] = end
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"""
        SELECT index_code, index_name, date, value, change_pct
        FROM {TABLE_INDEX_HISTORY}
        {where_clause}
        ORDER BY index_code, date
    """
    try:
        rows = ch_client.query(query, parameters=params).named_results()
    except AttributeError:
        rows = None
    if rows is None:
        rows = [
            {
                "index_code": row[0],
                "index_name": row[1],
                "date": row[2],
                "value": row[3],
                "change_pct": row[4],
            }
            for row in ch_client.query(query, parameters=params).result_rows
        ]
    return rows


def _fetch_index_portfolio_history_map(
    ch_client,
    index_codes: Iterable[str],
) -> Tuple[Dict[str, List[Tuple[date, Set[str]]]], Dict[str, Optional[str]]]:
    cleaned = [_sanitize_index_code(code) for code in index_codes if _sanitize_index_code(code)]
    if not cleaned:
        return {}, {}

    _ensure_index_tables(ch_client)
    symbol_lookup = _build_company_symbol_lookup(ch_client)
    in_clause = ", ".join(f"'{code}'" for code in cleaned)
    query = f"""
        SELECT index_code, index_name, effective_date, symbol, symbol_base
        FROM {TABLE_INDEX_PORTFOLIOS}
        WHERE upper(index_code) IN ({in_clause})
        ORDER BY index_code, effective_date, symbol
    """

    try:
        rows = ch_client.query(query).named_results()
    except AttributeError:
        rows = None
    if rows is None:
        rows = [
            {
                "index_code": row[0],
                "index_name": row[1],
                "effective_date": row[2],
                "symbol": row[3],
                "symbol_base": row[4] if len(row) > 4 else None,
            }
            for row in ch_client.query(query).result_rows
        ]

    timeline: Dict[str, Dict[date, Set[str]]] = {}
    names: Dict[str, Optional[str]] = {}

    for row in rows:
        if isinstance(row, dict):
            code_raw = row.get("index_code")
            name_raw = row.get("index_name")
            effective_raw = row.get("effective_date")
            symbol_raw = row.get("symbol")
            symbol_base_raw = row.get("symbol_base")
        else:
            code_raw, name_raw, effective_raw, symbol_raw, *rest = row
            symbol_base_raw = rest[0] if rest else None

        if not code_raw or not symbol_raw:
            continue

        code = str(code_raw).upper()
        symbol = _normalize_index_member_symbol(
            symbol_raw,
            symbol_base_raw,
            symbol_lookup=symbol_lookup,
        )
        if not symbol:
            continue

        effective = effective_raw
        if isinstance(effective, datetime):
            effective = effective.date()
        elif isinstance(effective, str):
            try:
                effective = date.fromisoformat(effective)
            except ValueError:
                continue
        if not isinstance(effective, date):
            continue

        bucket = timeline.setdefault(code, {})
        members = bucket.setdefault(effective, set())
        members.add(symbol)

        current_name = str(name_raw).strip() if name_raw else None
        if code not in names or (current_name and not names.get(code)):
            names[code] = current_name

    prepared: Dict[str, List[Tuple[date, Set[str]]]] = {}
    for code, mapping in timeline.items():
        sorted_items = sorted(mapping.items(), key=lambda item: item[0])
        prepared[code] = [(dt, set(members)) for dt, members in sorted_items]

    return prepared, names


def _collect_index_membership_union(
    ch_client,
    index_codes: Iterable[str],
    *,
    as_of: Optional[date] = None,
) -> Dict[str, List[str]]:
    timeline_map, _ = _fetch_index_portfolio_history_map(ch_client, index_codes)
    membership: Dict[str, List[str]] = {}
    for code, entries in timeline_map.items():
        seen: Set[str] = set()
        ordered: List[str] = []
        for entry_date, members in entries:
            if as_of is not None and entry_date > as_of:
                break
            for sym in sorted(members):
                if sym in seen:
                    continue
                seen.add(sym)
                ordered.append(sym)
        membership[code] = ordered
    return membership


def _list_candidate_symbols(
    ch_client,
    filters: Optional[UniverseFilters],
    *,
    include_index_history: bool = False,
    as_of: Optional[date] = None,
) -> List[str]:
    symbols = _collect_all_company_symbols(ch_client)
    if symbols is None:
        rows = ch_client.query(
            f"""
            SELECT DISTINCT symbol
            FROM {TABLE_OHLC}
            ORDER BY symbol
            """
        ).result_rows
        normalized: List[str] = []
        seen: Set[str] = set()
        for row in rows:
            if not row:
                continue
            raw_value = row[0]
            if raw_value is None:
                continue
            normalized_symbol = normalize_input_symbol(str(raw_value))
            if not normalized_symbol or normalized_symbol in seen:
                continue
            seen.add(normalized_symbol)
            normalized.append(normalized_symbol)
        symbols = normalized

    if not filters:
        return symbols

    includes = None
    if filters.include:
        includes = {normalize_input_symbol(sym) for sym in filters.include}
        includes = {sym for sym in includes if sym}
        if not includes:
            raise HTTPException(400, "Lista include nie zawiera poprawnych symboli")

    indices_whitelist = None
    if filters.indices:
        if include_index_history:
            membership_map = _collect_index_membership_union(
                ch_client, filters.indices, as_of=as_of
            )
        else:
            membership_map = _collect_latest_index_membership(
                ch_client, filters.indices, as_of=as_of
            )
        aggregated: Set[str] = set()
        for members in membership_map.values():
            aggregated.update(members)
        if not aggregated:
            raise HTTPException(404, "Brak spółek w wybranych indeksach")
        indices_whitelist = aggregated

    excludes = set()
    if filters.exclude:
        excludes = {normalize_input_symbol(sym) for sym in filters.exclude}
        excludes = {sym for sym in excludes if sym}

    prefixes = None
    if filters.prefixes:
        prefixes = [p.strip().upper() for p in filters.prefixes if p.strip()]

    filtered: List[str] = []
    for sym in symbols:
        if indices_whitelist is not None and sym not in indices_whitelist:
            continue
        if includes and sym not in includes:
            continue
        if sym in excludes:
            continue
        if prefixes and not any(sym.startswith(pref) for pref in prefixes):
            continue
        filtered.append(sym)

    return filtered


class CandidateSymbolMetadata(TypedDict, total=False):
    name: str
    isin: str
    sector: str
    industry: str


def _collect_candidate_metadata(
    ch_client,
    symbols: Sequence[str],
) -> Dict[str, CandidateSymbolMetadata]:
    if not symbols:
        return {}

    try:
        columns = _get_company_columns(ch_client)
    except Exception:
        return {}

    symbol_column = _find_company_symbol_column(columns)
    if not symbol_column:
        return {}

    lowered = {col.lower(): col for col in columns}
    field_candidates = {
        "name": COMPANY_NAME_CANDIDATES,
        "isin": ["isin"],
        "sector": ["sector"],
        "industry": ["industry", "branch"],
    }

    resolved_columns: Dict[str, str] = {}
    for field, candidates in field_candidates.items():
        for candidate in candidates:
            existing = lowered.get(candidate)
            if existing and existing != symbol_column:
                resolved_columns[field] = existing
                break

    if not resolved_columns:
        return {}

    selected_columns = [symbol_column, *resolved_columns.values()]

    lookup_keys: Set[str] = set()
    for symbol in symbols:
        if not symbol:
            continue
        lookup_keys.add(symbol.upper())
        normalized = normalize_input_symbol(symbol)
        if normalized:
            lookup_keys.add(normalized.upper())

    if not lookup_keys:
        return {}

    sql = (
        f"SELECT {', '.join(_quote_identifier(col) for col in selected_columns)} "
        f"FROM {TABLE_COMPANIES} "
        f"WHERE upper({_quote_identifier(symbol_column)}) IN %(symbols)s"
    )

    try:
        result = ch_client.query(sql, parameters={"symbols": tuple(lookup_keys)})
    except Exception:
        return {}

    column_names = list(getattr(result, "column_names", []))
    metadata: Dict[str, CandidateSymbolMetadata] = {}

    for row in getattr(result, "result_rows", []):
        row_dict = {col: value for col, value in zip(column_names, row)}
        raw_symbol = row_dict.get(symbol_column)
        if raw_symbol is None:
            continue
        raw_text = str(_convert_clickhouse_value(raw_symbol)).strip()
        if not raw_text:
            continue
        normalized = normalize_input_symbol(raw_text)
        key = (normalized or raw_text).upper()

        entry: CandidateSymbolMetadata = {}
        for field, column in resolved_columns.items():
            value = row_dict.get(column)
            if value is None:
                continue
            text = str(_convert_clickhouse_value(value)).strip()
            if text:
                entry[field] = text

        if not entry:
            continue

        metadata[key] = entry
        metadata.setdefault(raw_text.upper(), entry)

    return metadata


RankedScoreEntry = Tuple[str, float] | Tuple[str, float, Dict[str, float]]
RankedScoreList = List[RankedScoreEntry]


def _rank_symbols_by_score(
    ch_client,
    candidates: List[str],
    components: List[ScoreComponent],
    include_metrics: bool = False,
    *,
    as_of: Optional[date] = None,
    collect_failures: bool = False,
) -> Union[RankedScoreList, Tuple[RankedScoreList, Dict[str, str]]]:
    history_map = _collect_close_history_bulk(
        ch_client, candidates, components, as_of=as_of
    )
    evaluated: Dict[str, Dict[str, object]] = {}
    failures: Dict[str, str] = {} if collect_failures else {}
    for sym in candidates:
        closes_raw = history_map.get(sym)
        if not closes_raw:
            if collect_failures:
                failures[sym] = "Brak notowań spełniających kryteria zapytania."
            continue
        closes_prepared = _prepare_metric_series(closes_raw or [])
        if not closes_prepared:
            if collect_failures:
                failures[sym] = "Brak prawidłowych danych notowań."
            continue
        try:
            result = _evaluate_components_internal(
                closes_prepared,
                components,
                include_metrics=include_metrics,
                diagnose_failure=collect_failures,
            )
        except ScoreComputationError as exc:
            if collect_failures:
                failures[sym] = exc.reason
            continue
        if result is None:
            if collect_failures and sym not in failures:
                failures[sym] = "Brak wymaganych danych do obliczenia metryk score."
            continue
        adjusted_scores, metrics = result
        evaluated[sym] = {
            "scores": adjusted_scores,
        }
        if include_metrics and metrics is not None:
            evaluated[sym]["metrics"] = metrics

    if not evaluated:
        return []

    total_weight = sum(comp.weight for comp in components)
    if total_weight <= 0:
        return []

    for idx, component in enumerate(components):
        if component.normalize != "percentile":
            continue
        values = [
            (sym, float(cast(List[float], data["scores"])[idx]))
            for sym, data in evaluated.items()
        ]
        if not values:
            continue
        sorted_values = sorted(values, key=lambda item: item[1])
        n = len(sorted_values)
        if n == 1:
            sym, _ = sorted_values[0]
            cast(List[float], evaluated[sym]["scores"])[idx] = 1.0
            continue
        for rank, (sym, _) in enumerate(sorted_values):
            percentile = rank / (n - 1)
            cast(List[float], evaluated[sym]["scores"])[idx] = percentile

    ranked: List[Tuple[str, float] | Tuple[str, float, Dict[str, float]]] = []
    for sym, data in evaluated.items():
        scores = cast(List[float], data["scores"])
        weighted = 0.0
        for idx, component in enumerate(components):
            weighted += component.weight * scores[idx]
        score = weighted / total_weight
        if include_metrics:
            metrics = cast(Dict[str, float], data.get("metrics", {}))
            ranked.append((sym, score, metrics))
        else:
            ranked.append((sym, score))

    ranked.sort(key=lambda item: item[1], reverse=True)
    if collect_failures:
        return ranked, failures
    return ranked


def _parse_metric_identifier(metric: str, rule: ScoreRulePayload) -> Tuple[str, int]:
    cleaned = metric.strip().lower()
    cleaned = cleaned.replace(" ", "_")

    lookback_hint = rule.lookback_days or rule.lookback
    for sep in (":", "@", "/", "-"):
        if sep in cleaned:
            head, tail = cleaned.rsplit(sep, 1)
            if tail.isdigit():
                lookback_hint = int(tail)
                cleaned = head
                break

    if lookback_hint is None:
        parts = cleaned.split("_")
        if parts and parts[-1].isdigit():
            lookback_hint = int(parts[-1])
            cleaned = "_".join(parts[:-1])

    if lookback_hint is None:
        lookback_hint = 252

    if cleaned not in ALLOWED_SCORE_METRICS:
        raise HTTPException(400, f"Nieznana metryka score: {metric}")

    return cleaned, lookback_hint


def _build_components_from_rules(rules: List[ScoreRulePayload]) -> List[ScoreComponent]:
    components: List[ScoreComponent] = []
    for rule in rules:
        metric_name, lookback = _parse_metric_identifier(rule.metric, rule)
        weight = float(rule.weight or 1.0)
        direction = rule.direction or "desc"
        try:
            component = ScoreComponent(
                metric=metric_name,
                lookback_days=lookback,
                weight=weight,
                direction=direction,
                min_value=rule.min_value,
                max_value=rule.max_value,
                scoring=rule.scoring,
                normalize=rule.normalize or "none",
            )
        except ValidationError as exc:
            raise HTTPException(400, exc.errors()) from exc
        components.append(component)
    if not components:
        raise HTTPException(400, "Lista reguł score nie może być pusta")
    return components


def _build_filters_from_universe(universe: ScorePreviewRequest["universe"]) -> Optional[UniverseFilters]:
    if not universe:
        return None

    if isinstance(universe, str):
        tokens = [token.strip() for token in re.split(r"[\s,;]+", universe) if token.strip()]
    else:
        tokens: List[str] = []
        for item in universe:
            if isinstance(item, str):
                cleaned = item.strip()
            else:
                cleaned = str(item).strip()
            if cleaned:
                tokens.append(cleaned)

    if not tokens:
        return None

    include_tokens: List[str] = []
    index_tokens: List[str] = []
    for token in tokens:
        lowered = token.lower()
        if lowered.startswith("index:") or lowered.startswith("indeks:") or lowered.startswith("idx:"):
            _, _, tail = token.partition(":")
            raw_value = tail.strip()
            parts = [part.strip() for part in re.split(r"[+&]", raw_value) if part.strip()]
            for part in parts:
                candidate = _sanitize_index_code(part)
                if candidate:
                    index_tokens.append(candidate)
            continue
        include_tokens.append(token)

    payload: Dict[str, object] = {}
    if include_tokens:
        payload["include"] = include_tokens
    if index_tokens:
        payload["indices"] = index_tokens

    if not payload:
        return None

    try:
        return UniverseFilters(**payload)
    except ValidationError as exc:  # pragma: no cover - delegacja błędu walidacji
        raise HTTPException(400, "Niepoprawny format wszechświata") from exc


DEFAULT_SCORE_PREVIEW_LIMIT = 5000


def _build_auto_config_from_preview(req: ScorePreviewRequest) -> AutoSelectionConfig:
    components = _build_components_from_rules(req.rules)
    limit = req.limit if req.limit is not None else DEFAULT_SCORE_PREVIEW_LIMIT
    top_n = max(1, min(limit, DEFAULT_SCORE_PREVIEW_LIMIT))
    filters = _build_filters_from_universe(req.universe)
    return AutoSelectionConfig(
        top_n=top_n,
        components=components,
        filters=filters,
        weighting="equal",
        direction="asc" if req.sort == "asc" else "desc",
    )


def _rebalance_dates(dates: List[str], freq: str) -> List[str]:
    """
    Zwraca listę dat rebalansingu (YYYY-MM-DD) dla equity kroczonej dziennie.
    freq: 'none' | 'monthly' | 'quarterly' | 'yearly'
    """
    if freq == "none":
        return []

    result: List[str] = []
    last_key = None

    for ds in dates:
        y, m, _ = ds.split("-")
        key = None
        if freq == "monthly":
            key = f"{y}-{m}"
        elif freq == "quarterly":
            q = (int(m) - 1) // 3 + 1
            key = f"{y}-Q{q}"
        elif freq == "yearly":
            key = y
        else:
            break

        if key != last_key:
            result.append(ds)
            last_key = key

    return result


def _compute_backtest(
    closes_map: Dict[str, List[Tuple[str, float]]],
    weights_pct: List[float],
    start: date,
    rebalance: str,
    *,
    end: Optional[date] = None,
    initial_capital: float = 10000.0,
    fee_pct: float = 0.0,
    threshold_pct: float = 0.0,
    cash_weight: float = 0.0,
    dynamic_allocator: Optional[
        Callable[[str, List[str]], Tuple[Dict[str, float], float, Optional[str]]]
    ] = None,
) -> Tuple[List[PortfolioPoint], PortfolioStats, List[PortfolioRebalanceEvent]]:
    """
    Prosty backtest na dziennych close'ach z rebalancingiem.
    Obsługuje okresy przed debiutem spółki poprzez normalizację wag
    wśród dostępnych składników i wymusza rebalans w dniu wejścia na giełdę.
    """

    _ = fee_pct  # parametr zarezerwowany na przyszłe uwzględnienie kosztów transakcyjnych

    start_iso = start.isoformat()
    end_iso = end.isoformat() if end is not None else None
    all_dates = sorted(
        {
            d
            for series in closes_map.values()
            for (d, _) in series
            if d >= start_iso and (end_iso is None or d <= end_iso)
        }
    )
    if not all_dates:
        raise HTTPException(404, "Brak wspólnych notowań")

    close_dicts: Dict[str, Dict[str, float]] = {
        sym: {d: c for (d, c) in series} for sym, series in closes_map.items()
    }

    first_dates: Dict[str, str] = {sym: series[0][0] for sym, series in closes_map.items() if series}
    last_prices: Dict[str, Optional[float]] = {sym: None for sym in closes_map.keys()}

    base_weights: Dict[str, float] = {}
    for sym, weight in zip(closes_map.keys(), weights_pct):
        base_weights[sym] = max(float(weight or 0.0), 0.0)

    default_cash_weight = min(max(float(cash_weight), 0.0), 1.0)
    investable_limit = max(0.0, 1.0 - default_cash_weight)
    investable_total = sum(base_weights.values())
    if investable_limit <= 0:
        for sym in base_weights:
            base_weights[sym] = 0.0
        investable_total = 0.0
    elif investable_total > investable_limit and investable_total > 0:
        scale = investable_limit / investable_total
        for sym in base_weights:
            base_weights[sym] *= scale
        investable_total = investable_limit

    active_cash_weight = default_cash_weight

    rebal_dates = set(_rebalance_dates(all_dates, rebalance))

    equity: List[PortfolioPoint] = []
    rebalances: List[PortfolioRebalanceEvent] = []
    shares: Dict[str, float] = {sym: 0.0 for sym in closes_map.keys()}

    def _to_ratio(value: float) -> float:
        return value / 100.0 if abs(value) > 1 else value

    portfolio_initial = initial_capital if initial_capital > 0 else 1.0
    portfolio_value = portfolio_initial
    cash_value = portfolio_initial
    threshold_ratio = max(_to_ratio(threshold_pct), 0.0)
    fee_ratio = max(_to_ratio(fee_pct), 0.0)
    total_turnover_ratio = 0.0
    total_trades = 0
    total_fees_paid = 0.0

    for ds in all_dates:
        prices_today: Dict[str, float] = {}
        newly_available: List[str] = []

        for sym, price_map in close_dicts.items():
            px = price_map.get(ds)
            if px is not None:
                prices_today[sym] = px
                last_prices[sym] = px
                if first_dates.get(sym) == ds:
                    newly_available.append(sym)
            elif shares.get(sym, 0.0) > 0 and last_prices.get(sym) is not None:
                prices_today[sym] = last_prices[sym]  # podtrzymaj ostatni kurs

        if not prices_today:
            continue

        # wartość portfela przed ewentualnym rebalancingiem
        if equity:
            portfolio_value = cash_value
            for sym, qty in shares.items():
                if qty <= 0:
                    continue
                price = prices_today.get(sym)
                if price is None:
                    price = last_prices.get(sym)
                if price is None:
                    continue
                portfolio_value += qty * price
            if portfolio_value <= 0:
                portfolio_value = portfolio_initial

        portfolio_value_before = portfolio_value
        prev_shares = dict(shares)
        is_first_point = not equity
        scheduled_rebalance = ds in rebal_dates
        should_rebalance = is_first_point or scheduled_rebalance or bool(newly_available)

        trades: List[PortfolioTrade] = []
        turnover_abs = 0.0
        portfolio_value_after_fees: Optional[float] = None
        scale_factor = 1.0
        trade_records: List[Tuple[str, PortfolioTrade]] = []
        target_cash_value = cash_value
        cash_trade_note: Optional[str] = None

        if should_rebalance:
            # symbole, które mogą uczestniczyć w rebalansingu
            available_syms = list(prices_today.keys())

            prev_cash_weight = (
                cash_value / portfolio_value_before if portfolio_value_before > 0 else active_cash_weight
            )

            if dynamic_allocator is not None:
                weights_update, cash_candidate, note = dynamic_allocator(ds, available_syms)
                cash_trade_note = note
                for sym in base_weights.keys():
                    base_weights[sym] = max(float(weights_update.get(sym, 0.0)), 0.0)
                active_cash_weight = min(max(float(cash_candidate), 0.0), 1.0)
            else:
                active_cash_weight = default_cash_weight
                cash_trade_note = None

            symbols_with_weight = [sym for sym in available_syms if base_weights.get(sym, 0.0) > 0]
            if not symbols_with_weight:
                symbols_with_weight = available_syms

            investable_target = sum(base_weights.values())
            weight_sum = sum(base_weights.get(sym, 0.0) for sym in symbols_with_weight)
            targets: Dict[str, float] = {}
            if weight_sum > 0 and investable_target > 0:
                scale = investable_target / weight_sum
                for sym in symbols_with_weight:
                    targets[sym] = base_weights.get(sym, 0.0) * scale
            else:
                equal = (
                    investable_target / len(symbols_with_weight)
                    if symbols_with_weight
                    else 0.0
                )
                for sym in symbols_with_weight:
                    targets[sym] = equal

            for sym in available_syms:
                targets.setdefault(sym, 0.0)

            target_cash_value = portfolio_value_before * active_cash_weight

            for sym in available_syms:
                price = prices_today[sym]
                current_qty = shares.get(sym, 0.0)
                current_value = current_qty * price
                current_weight = (
                    current_value / portfolio_value_before if portfolio_value_before > 0 else 0.0
                )
                target_weight = targets.get(sym, 0.0)
                target_value = portfolio_value_before * target_weight
                delta_value = target_value - current_value

                if threshold_ratio > 0 and abs(target_weight - current_weight) < threshold_ratio:
                    shares[sym] = current_qty
                    continue

                if abs(delta_value) > 1e-9:
                    action = "buy" if delta_value > 0 else "sell"
                    target_qty = target_value / price if price > 0 else 0.0
                    shares_delta = target_qty - current_qty
                    shares[sym] = target_qty
                    turnover_abs += abs(delta_value)
                    note = None
                    if sym in newly_available:
                        note = "Dołączono do portfela (debiut notowań)"
                    trade = PortfolioTrade(
                        symbol=pretty_symbol(sym),
                        action=action,
                        weight_change=target_weight - current_weight,
                        value_change=delta_value,
                        target_weight=target_weight,
                        shares_change=shares_delta,
                        price=price,
                        shares_after=target_qty,
                        note=note,
                    )
                    trades.append(trade)
                    trade_records.append((sym, trade))
                else:
                    if price > 0:
                        shares[sym] = current_value / price

            cash_delta = target_cash_value - cash_value
            cash_weight_change = active_cash_weight - prev_cash_weight
            if (
                cash_trade_note is not None
                or abs(cash_delta) > 1e-9
                or abs(cash_weight_change) > 1e-9
            ):
                trades.append(
                    PortfolioTrade(
                        symbol="Wolne środki",
                        action="hold" if abs(cash_delta) <= 1e-9 else ("buy" if cash_delta > 0 else "sell"),
                        value_change=cash_delta if abs(cash_delta) > 1e-9 else None,
                        target_weight=active_cash_weight,
                        weight_change=cash_weight_change,
                        note=cash_trade_note,
                    )
                )

            if trades:
                reasons: List[str] = []
                if is_first_point:
                    reasons.append("Start portfela")
                if newly_available:
                    joined = ", ".join(pretty_symbol(sym) for sym in newly_available)
                    reasons.append(f"Nowe spółki: {joined}")
                if scheduled_rebalance and not is_first_point:
                    reasons.append("Planowy rebalansing")

                turnover = (
                    turnover_abs / portfolio_value_before if portfolio_value_before > 0 else 0.0
                )
                total_turnover_ratio += turnover
                total_trades += len(trades)
                if fee_ratio > 0 and portfolio_value_before > 0 and turnover_abs > 0:
                    fees_cost = min(portfolio_value_before, turnover_abs * fee_ratio)
                    if fees_cost > 0:
                        scale_factor = max(0.0, 1.0 - fees_cost / portfolio_value_before)
                        if scale_factor < 1.0:
                            for sym_key in list(shares.keys()):
                                shares[sym_key] *= scale_factor
                            cash_value *= scale_factor
                            target_cash_value *= scale_factor
                            for raw_sym, trade in trade_records:
                                prev_qty = prev_shares.get(raw_sym, 0.0)
                                actual_after = shares.get(raw_sym, 0.0)
                                trade.shares_after = actual_after
                                trade.shares_change = actual_after - prev_qty
                                price_for_value = prices_today.get(raw_sym)
                                if price_for_value is None:
                                    price_for_value = last_prices.get(raw_sym)
                                if price_for_value is not None:
                                    trade.value_change = trade.shares_change * price_for_value
                        total_fees_paid += fees_cost
                        portfolio_value_after_fees = portfolio_value_before * scale_factor
                rebalances.append(
                    PortfolioRebalanceEvent(
                        date=ds,
                        reason=" • ".join(reasons) if reasons else None,
                        turnover=turnover,
                            trades=trades,
                        )
                    )

            cash_value = target_cash_value

        # aktualizacja wartości portfela po ewentualnym rebalansingu
        portfolio_value = cash_value
        for sym, qty in shares.items():
            if qty <= 0:
                continue
            price = prices_today.get(sym)
            if price is None:
                price = last_prices.get(sym)
            if price is None:
                continue
            portfolio_value += qty * price

        if portfolio_value <= 0:
            if portfolio_value_after_fees is not None:
                portfolio_value = portfolio_value_after_fees
            else:
                portfolio_value = portfolio_initial

        equity.append(PortfolioPoint(date=ds, value=portfolio_value))

    if not equity:
        raise HTTPException(404, "Brak notowań do zbudowania portfela")

    if len(equity) >= 1:
        first_v = equity[0].value
        last_v = equity[-1].value
        days = max(
            1,
            (
                datetime.fromisoformat(equity[-1].date)
                - datetime.fromisoformat(equity[0].date)
            ).days,
        )
        years = days / 365.25
        ratio = last_v / first_v if first_v > 0 else 1.0
        cagr = ratio ** (1 / years) - 1 if years > 0 else 0.0

        peak = -1e9
        max_dd = 0.0
        values = [pt.value for pt in equity]
        for v in values:
            peak = max(peak, v)
            dd = (v / peak) - 1.0
            if dd < max_dd:
                max_dd = dd

        import statistics

        rets: List[float] = []
        for a, b in zip(values, values[1:]):
            if a > 0:
                rets.append(b / a - 1.0)
        vol_daily = statistics.pstdev(rets) if len(rets) > 1 else 0.0
        vol_annual = vol_daily * (252 ** 0.5)

        sharpe = (cagr - 0.0) / vol_annual if vol_annual > 1e-12 else 0.0

        total_return = ratio - 1 if first_v > 0 else 0.0
        stats = PortfolioStats(
            cagr=cagr,
            max_drawdown=max_dd,
            volatility=vol_annual,
            sharpe=sharpe,
            last_value=ratio,
            initial_value=first_v,
            final_value=last_v,
            total_return=total_return,
            turnover=total_turnover_ratio,
            trades=float(total_trades),
            fees=total_fees_paid,
        )
    else:
        last_v = equity[-1].value
        ratio = 1.0
        stats = PortfolioStats(
            cagr=0.0,
            max_drawdown=0.0,
            volatility=0.0,
            sharpe=0.0,
            last_value=ratio,
            initial_value=last_v,
            final_value=last_v,
            total_return=0.0,
            turnover=total_turnover_ratio,
            trades=float(total_trades),
            fees=total_fees_paid,
        )

    return equity, stats, rebalances


def _run_backtest(req: BacktestPortfolioRequest) -> PortfolioResp:
    dt_start = req.start
    ch = get_ch()

    allocations: List[PortfolioAllocation] = []
    cash_weight = 0.0
    dynamic_allocator: Optional[
        Callable[[str, List[str]], Tuple[Dict[str, float], float, Optional[str]]]
    ] = None

    if req.manual:
        raw_syms: List[str] = []
        for s in req.manual.symbols:
            raw = normalize_input_symbol(s)
            if not raw:
                raise HTTPException(400, "Symbol nie może być pusty")
            raw_syms.append(raw)

        raw_weights = list(req.manual.weights) if req.manual.weights else [1.0] * len(raw_syms)
        total_raw_weights = sum(raw_weights)
        if total_raw_weights > 0:
            weights_list = [weight / total_raw_weights for weight in raw_weights]
        elif raw_syms:
            equal = 1.0 / len(raw_syms)
            weights_list = [equal] * len(raw_syms)
        else:
            weights_list = []

        for raw_sym, weight in zip(raw_syms, weights_list):
            allocations.append(
                PortfolioAllocation(
                    symbol=pretty_symbol(raw_sym),
                    raw=raw_sym,
                    target_weight=weight,
                )
            )
    else:
        assert req.auto is not None
        candidates = _list_candidate_symbols(
            ch, req.auto.filters, include_index_history=True
        )
        if not candidates:
            raise HTTPException(404, "Brak symboli do oceny")

        components = req.auto.components
        max_lookback = max(comp.lookback_days for comp in components)
        buffer_days = max_lookback + 30
        fetch_start = dt_start - timedelta(days=buffer_days)

        prepared_series: Dict[str, List[Tuple[date, float]]] = {}
        prepared_dates: Dict[str, List[date]] = {}
        closes_ordered: "OrderedDict[str, List[Tuple[str, float]]]" = OrderedDict()

        for sym in candidates:
            series_full = _fetch_close_series(ch, sym, fetch_start, req.end)
            if not series_full:
                continue
            prepared = _prepare_metric_series(series_full)
            if not prepared:
                continue
            prepared_series[sym] = prepared
            prepared_dates[sym] = [dt for (dt, _) in prepared]

            trimmed = [(d, c) for (d, c) in series_full if d >= dt_start.isoformat()]
            if trimmed:
                closes_ordered[sym] = trimmed

        if not closes_ordered:
            raise HTTPException(404, "Brak danych historycznych po filtrach score")

        index_membership_resolver: Optional[Callable[[str], Set[str]]] = None
        if req.auto.filters and req.auto.filters.indices:
            timeline_map, _ = _fetch_index_portfolio_history_map(
                ch, req.auto.filters.indices
            )
            prepared_timelines: Dict[str, Tuple[List[date], List[Set[str]]]] = {}
            for code, entries in timeline_map.items():
                if not entries:
                    continue
                dates_sorted = [entry_date for entry_date, _ in entries]
                member_sets = [set(members) for _, members in entries]
                prepared_timelines[code] = (dates_sorted, member_sets)

            if prepared_timelines:
                def _membership_union_for_date(ds: str) -> Set[str]:
                    dt_current = date.fromisoformat(ds)
                    union: Set[str] = set()
                    for dates_sorted, member_sets in prepared_timelines.values():
                        idx = bisect_right(dates_sorted, dt_current) - 1
                        if idx >= 0:
                            union.update(member_sets[idx])
                    return union

                index_membership_resolver = _membership_union_for_date

        all_symbols = list(closes_ordered.keys())
        investable_ratio = max(0.0, min(1.0, 1.0 - float(cash_weight)))
        min_score = req.auto.min_score
        max_score = req.auto.max_score
        direction_desc = req.auto.direction != "asc"

        def _allocate_for_date(
            ds: str, available_syms: List[str]
        ) -> Tuple[Dict[str, float], float, Optional[str]]:
            dt_current = date.fromisoformat(ds)
            scored: List[Tuple[str, float]] = []
            eligible_syms = list(available_syms)
            if index_membership_resolver is not None:
                allowed_today = index_membership_resolver(ds)
                if allowed_today:
                    eligible_syms = [sym for sym in available_syms if sym in allowed_today]
                else:
                    eligible_syms = []

            for sym in eligible_syms:
                prepared = prepared_series.get(sym)
                if not prepared:
                    continue
                dates_cache = prepared_dates[sym]
                idx = bisect_right(dates_cache, dt_current)
                if idx == 0:
                    continue
                subset = prepared[:idx]
                score_value = _calculate_score_from_prepared(subset, components)
                if score_value is None:
                    continue
                if isinstance(score_value, tuple):
                    score_number = float(score_value[0])
                else:
                    score_number = float(score_value)
                scored.append((sym, score_number))

            if not scored:
                weights = {sym: 0.0 for sym in all_symbols}
                return weights, 1.0, "Wolne środki do transakcji"

            scored.sort(key=lambda item: item[1], reverse=direction_desc)

            if min_score is not None:
                scored = [item for item in scored if item[1] >= min_score]
            if max_score is not None:
                scored = [item for item in scored if item[1] <= max_score]

            if not scored:
                weights = {sym: 0.0 for sym in all_symbols}
                return weights, 1.0, "Wolne środki do transakcji"

            selected = scored[: req.auto.top_n]
            if not selected:
                weights = {sym: 0.0 for sym in all_symbols}
                return weights, 1.0, "Wolne środki do transakcji"

            weights = {sym: 0.0 for sym in all_symbols}
            slots = max(req.auto.top_n, 1)
            selected_ratio = min(len(selected), slots) / slots
            actual_investable = investable_ratio * selected_ratio

            if req.auto.weighting == "score":
                raw_values = [score for _, score in selected]
                total_raw = sum(raw_values)
                if total_raw > 0:
                    for sym, score in selected:
                        weights[sym] = actual_investable * (score / total_raw)
                else:
                    equal = actual_investable / len(selected)
                    for sym, _ in selected:
                        weights[sym] = equal
            else:
                equal = actual_investable / len(selected)
                for sym, _ in selected:
                    weights[sym] = equal

            cash_weight_local = max(0.0, 1.0 - actual_investable)
            note: Optional[str]
            if cash_weight_local > 0:
                note = (
                    "Niewykorzystane sloty (część środków pozostaje w gotówce)"
                    if len(selected) < req.auto.top_n
                    else "Wolne środki do transakcji"
                )
            else:
                note = None
            return weights, cash_weight_local, note

        weights_list = [0.0] * len(closes_ordered)
        raw_syms = list(closes_ordered.keys())

        dynamic_allocator = _allocate_for_date

    closes_map: Dict[str, List[Tuple[str, float]]] = {}
    for rs in raw_syms:
        if req.manual:
            series = _fetch_close_series(ch, rs, dt_start, req.end)
            if not series:
                raise HTTPException(404, f"Brak danych historycznych dla {rs}")
            closes_map[rs] = series
        else:
            assert req.auto is not None
            series = closes_ordered.get(rs)
            if not series:
                series = []
            closes_map[rs] = series

    equity, stats, rebalances = _compute_backtest(
        closes_map,
        weights_list,
        dt_start,
        req.rebalance,
        end=req.end,
        initial_capital=req.initial_capital,
        fee_pct=req.fee_pct,
        threshold_pct=req.threshold_pct,
        cash_weight=cash_weight,
        dynamic_allocator=dynamic_allocator,
    )
    return PortfolioResp(
        equity=equity,
        stats=stats,
        allocations=allocations or None,
        rebalances=rebalances or None,
    )


def _compute_portfolio_score(req: PortfolioScoreRequest) -> List[PortfolioScoreItem]:
    if not req.auto:
        raise HTTPException(400, "Endpoint score wspiera jedynie tryb auto")

    ch = get_ch()
    candidates = _list_candidate_symbols(ch, req.auto.filters)
    if not candidates:
        raise HTTPException(404, "Brak symboli do oceny")

    ranked = _rank_symbols_by_score(ch, candidates, req.auto.components)
    if req.auto.direction == "asc":
        ranked = list(reversed(ranked))
    if not ranked:
        raise HTTPException(404, "Brak symboli ze wszystkimi wymaganymi danymi")

    min_score = req.auto.min_score
    if min_score is not None:
        ranked = [item for item in ranked if item[1] >= min_score]
    max_score = req.auto.max_score
    if max_score is not None:
        ranked = [item for item in ranked if item[1] <= max_score]

    top = ranked[: req.auto.top_n]
    return [
        PortfolioScoreItem(symbol=pretty_symbol(sym), raw=sym, score=score)
        for sym, score in top
    ]


def _run_score_preview(req: ScorePreviewRequest) -> ScorePreviewResponse:
    auto_config = _build_auto_config_from_preview(req)
    ch = get_ch()

    as_of_date = req.as_of or date.today()

    candidates = _list_candidate_symbols(ch, auto_config.filters, as_of=as_of_date)
    if not candidates:
        raise HTTPException(404, "Brak symboli do oceny")

    ranked_result = _rank_symbols_by_score(
        ch,
        candidates,
        auto_config.components,
        include_metrics=True,
        as_of=as_of_date,
        collect_failures=True,
    )
    if isinstance(ranked_result, tuple):
        ranked, failures = ranked_result
    else:
        ranked = ranked_result
        failures = {}
    if not ranked and not failures:
        raise HTTPException(404, "Brak symboli ze wszystkimi wymaganymi danymi")

    prepared: List[Dict[str, object]] = []
    for sym, score, metrics in ranked:  # type: ignore[misc]
        prepared.append(
            {
                "symbol": pretty_symbol(sym),
                "raw": sym,
                "score": score,
                "metrics": metrics,
            }
        )

    if req.sort == "asc":
        prepared.sort(key=lambda item: item["score"])  # type: ignore[index]
    else:
        prepared.sort(key=lambda item: item["score"], reverse=True)  # type: ignore[index]

    limit = req.limit if req.limit is not None else len(ranked)
    if limit:
        prepared = prepared[:limit]

    rows = [
        ScorePreviewRow(
            symbol=item["symbol"],
            raw=item["raw"],
            score=float(item["score"]),
            metrics=dict(item["metrics"]),
            rank=idx + 1,
        )
        for idx, item in enumerate(prepared)
    ]

    as_of = as_of_date.isoformat()
    meta: Dict[str, object] = {
        "name": req.name,
        "as_of": as_of,
        "universe_count": len(candidates),
    }

    missing_rows = [
        ScorePreviewMissingRow(symbol=pretty_symbol(sym), raw=sym, reason=reason)
        for sym, reason in sorted(failures.items())
    ]

    return ScorePreviewResponse(
        name=req.name,
        as_of=as_of,
        universe_count=len(candidates),
        rows=rows,
        missing=missing_rows,
        meta=meta,
    )


@api_router.post("/score/preview", response_model=ScorePreviewResponse)
def score_preview(req: ScorePreviewRequest):
    return _run_score_preview(req)


@api_router.post("/scores/preview", response_model=ScorePreviewResponse)
def scores_preview(req: ScorePreviewRequest):
    return _run_score_preview(req)


def _parse_backtest_get(
    mode: str = Query(
        default="manual",
        description="Wybierz tryb budowy portfela: manual lub auto.",
    ),
    start: str = Query(
        default=date(2015, 1, 1).isoformat(),
        description="Początek backtestu w formacie YYYY-MM-DD.",
    ),
    rebalance: str = Query(
        default="monthly",
        description="Częstotliwość rebalancingu (none, monthly, quarterly, yearly).",
    ),
    symbols: Optional[List[str]] = Query(
        default=None,
        description="Lista symboli GPW (powtarzalny parametr) dla trybu manual.",
    ),
    weights: Optional[List[str]] = Query(
        default=None,
        description="Lista wag odpowiadająca kolejności symboli (powtarzalny parametr).",
    ),
    top_n: Optional[int] = Query(
        default=None,
        description="Liczba spółek do wyboru w trybie auto.",
    ),
    min_score: Optional[float] = Query(
        default=None,
        description="Minimalna wartość score wymagana, aby spółka trafiła do portfela.",
    ),
    max_score: Optional[float] = Query(
        default=None,
        description="Maksymalna wartość score dla spółek w portfelu.",
    ),
    weighting: str = Query(
        default="equal",
        description="Strategia wag w trybie auto: equal lub score.",
    ),
    components: Optional[List[str]] = Query(
        default=None,
        description=(
            "Lista komponentów score'u. Każdy element może być JSON-em lub zapisem "
            "lookback:metric:weight (np. 252:total_return:5)."
        ),
    ),
    score: Optional[str] = Query(
        default=None,
        description="Nazwa predefiniowanego score'u (np. quality_score) dla trybu score.",
    ),
    direction: str = Query(
        default="desc",
        description="Sortowanie rankingu (desc lub asc).",
    ),
    filters_include: Optional[List[str]] = Query(
        default=None,
        description="Filtr: bierz pod uwagę tylko wskazane symbole.",
    ),
    filters_exclude: Optional[List[str]] = Query(
        default=None,
        description="Filtr: pomiń wskazane symbole.",
    ),
    filters_prefixes: Optional[List[str]] = Query(
        default=None,
        description="Filtr: ogranicz do symboli zaczynających się od prefiksów.",
    ),
) -> BacktestPortfolioRequest:
    try:
        start_dt = date.fromisoformat(start)
    except ValueError as exc:  # pragma: no cover - defensywne
        raise HTTPException(400, "Parametr start musi być w formacie YYYY-MM-DD") from exc

    payload: Dict[str, object] = {"start": start_dt, "rebalance": rebalance}

    def _split_csv(values: Optional[List[str]]) -> List[str]:
        if not values:
            return []
        collected: List[str] = []
        for raw in values:
            if raw is None:
                continue
            parts = [part.strip() for part in raw.split(",")]
            for part in parts:
                if part:
                    collected.append(part)
        return collected

    def _parse_components(raw_components: Optional[List[str]]) -> List[Dict[str, object]]:
        parsed: List[Dict[str, object]] = []
        if not raw_components:
            return parsed
        for raw in raw_components:
            raw_value = raw.strip()
            if not raw_value:
                continue
            try:
                loaded = json.loads(raw_value)
            except json.JSONDecodeError:
                parts = raw_value.split(":")
                if len(parts) != 3:
                    raise HTTPException(
                        400,
                        "Komponent musi być JSON-em lub mieć format lookback:metric:weight",
                    )
                lookback_str, metric, weight_str = parts
                try:
                    lookback_days = int(lookback_str)
                    weight_val = int(weight_str)
                except ValueError as exc:
                    raise HTTPException(
                        400, "Lookback i weight muszą być liczbami całkowitymi"
                    ) from exc
                comp_data = {
                    "lookback_days": lookback_days,
                    "metric": metric,
                    "weight": weight_val,
                }
            else:
                if not isinstance(loaded, dict):
                    raise HTTPException(400, "JSON komponentu musi być obiektem")
                comp_data = loaded
            parsed.append(comp_data)
        return parsed

    mode_normalized = mode.strip().lower()
    direction_normalized = direction.strip().lower()
    if direction_normalized not in {"asc", "desc"}:
        raise HTTPException(400, "Parametr direction musi przyjmować wartości asc lub desc")

    if not isinstance(min_score, (type(None), int, float)):
        min_score = getattr(min_score, "default", min_score)
    if not isinstance(max_score, (type(None), int, float)):
        max_score = getattr(max_score, "default", max_score)

    if mode_normalized == "manual":
        parsed_symbols = _split_csv(symbols)
        if not parsed_symbols:
            raise HTTPException(400, "Tryb manual wymaga co najmniej jednego symbolu")
        manual_payload: Dict[str, object] = {"symbols": parsed_symbols}
        parsed_weights = _split_csv(weights)
        if parsed_weights:
            try:
                manual_payload["weights"] = [float(item) for item in parsed_weights]
            except ValueError as exc:
                raise HTTPException(400, "Wagi muszą być liczbami") from exc
        payload["manual"] = manual_payload
    elif mode_normalized == "auto":
        if top_n is None:
            raise HTTPException(400, "Tryb auto wymaga parametru top_n")
        parsed_components = _parse_components(components)
        if not parsed_components:
            raise HTTPException(400, "Lista komponentów nie może być pusta")

        auto_payload: Dict[str, object] = {
            "top_n": top_n,
            "weighting": weighting,
            "components": parsed_components,
            "direction": direction_normalized,
        }

        if min_score is not None:
            auto_payload["min_score"] = float(min_score)
        if max_score is not None:
            auto_payload["max_score"] = float(max_score)

        if filters_include or filters_exclude or filters_prefixes:
            filters_payload: Dict[str, List[str]] = {}
            if filters_include:
                filters_payload["include"] = list(filters_include)
            if filters_exclude:
                filters_payload["exclude"] = list(filters_exclude)
            if filters_prefixes:
                filters_payload["prefixes"] = list(filters_prefixes)
            auto_payload["filters"] = filters_payload

        payload["auto"] = auto_payload
    elif mode_normalized == "score":
        parsed_components = _parse_components(components)
        if not parsed_components:
            if not score or not score.strip():
                raise HTTPException(
                    400, "Tryb score wymaga parametru score lub listy komponentów"
                )
            score_key = score.strip().lower()
            preset_components = SCORE_PRESETS.get(score_key)
            if not preset_components:
                raise HTTPException(404, f"Nieznany score: {score}")
            parsed_components = [comp.model_dump() for comp in preset_components]

        top_value = top_n or max(len(parsed_components), 1)

        auto_payload = {
            "top_n": top_value,
            "weighting": weighting,
            "components": parsed_components,
            "direction": direction_normalized,
        }

        if min_score is not None:
            auto_payload["min_score"] = float(min_score)
        if max_score is not None:
            auto_payload["max_score"] = float(max_score)

        if filters_include or filters_exclude or filters_prefixes:
            filters_payload = {}
            if filters_include:
                filters_payload["include"] = list(filters_include)
            if filters_exclude:
                filters_payload["exclude"] = list(filters_exclude)
            if filters_prefixes:
                filters_payload["prefixes"] = list(filters_prefixes)
            auto_payload["filters"] = filters_payload

        payload["auto"] = auto_payload
    else:
        raise HTTPException(
            400, "Parametr mode musi przyjmować wartości manual, auto lub score"
        )

    try:
        return BacktestPortfolioRequest.model_validate(payload)
    except ValidationError as exc:
        raise HTTPException(422, exc.errors()) from exc


@api_router.get("/backtest/portfolio", response_model=PortfolioResp)
def backtest_portfolio_get(req: BacktestPortfolioRequest = Depends(_parse_backtest_get)):
    """GET-owy wariant backtestu portfela.

    Umożliwia szybkie testy z poziomu przeglądarki, np.:

    ``/backtest/portfolio?mode=manual&symbols=CDR.WA&symbols=PKN.WA&start=2023-01-01``

    ``/backtest/portfolio?mode=auto&top_n=3&components=252:total_return:5``
    """

    return _run_backtest(req)


@api_router.post("/backtest/portfolio", response_model=PortfolioResp)
def backtest_portfolio(req: BacktestPortfolioRequest):
    """Backtest portfela na bazie kursów zamknięcia.

    Endpoint obsługuje zarówno klasyczny POST (JSON), jak i wariant GET opisany
    w dokumentacji wyżej. Tryb ``manual`` przyjmuje listę symboli oraz opcjonalne
    wagi, a tryb ``auto`` – konfigurację komponentów score'u wraz z filtrami
    wszechświata i sposobem ważenia (``equal`` lub ``score``).
    """

    return _run_backtest(req)


@api_router.post("/backtest/portfolio/score", response_model=List[PortfolioScoreItem])
def backtest_portfolio_score(req: PortfolioScoreRequest):
    """Zwraca ranking spółek na podstawie konfiguracji trybu auto."""

    return _compute_portfolio_score(req)


@api_router.get("/backtest/portfolio/tooling", response_model=BacktestPortfolioTooling)
def backtest_portfolio_tooling():
    """Zwraca metadane pomagające zbudować formularz do backtestów.

    Ułatwia frontendom przygotowanie list rozwijanych i opisów pól, tak aby
    użytkownicy mogli szybciej złożyć poprawny request ``/backtest/portfolio``.
    """

    return BacktestPortfolioTooling(
        start=date(2015, 1, 1).isoformat(),
        rebalance_modes=["none", "monthly", "quarterly", "yearly"],
        manual=ManualSelectionDescriptor(
            description="Podaj listę symboli w formacie GPW (np. CDR.WA).",
            weights=(
                "Opcjonalna lista wag – musi odpowiadać kolejności symboli. "
                "Brak oznacza równy podział."
            ),
        ),
        auto=AutoSelectionDescriptor(
            top_n=RangeDescriptor(min=1, max=5000, step=1, default=5),
            weighting_modes=["equal", "score"],
            components=[
                ComponentDescriptor(
                    metric="total_return",
                    label="Skumulowana stopa zwrotu",
                    description=(
                        "Porównuje cenę końcową z wartością sprzed okresu "
                        "lookback i normalizuje wynik (0-200%)."
                    ),
                    lookback_days=RangeDescriptor(min=1, max=3650, step=1, default=252),
                    weight=RangeDescriptor(min=1, max=10, step=1, default=5),
                )
            ],
            filters={
                "include": "Lista symboli do rozważenia (priorytet nad prefixami).",
                "exclude": "Symbole, które zostaną pominięte (po normalizacji).",
                "prefixes": "Rozważaj tylko tickery zaczynające się od podanych prefiksów.",
            },
        ),
    )


@api_router.get("/indices/portfolios", response_model=IndexPortfoliosResponse)
def list_index_portfolios(codes: Optional[List[str]] = Query(default=None)) -> IndexPortfoliosResponse:
    ch = get_ch()
    rows = _fetch_latest_index_portfolios(ch, codes)
    company_lookup = _build_company_name_lookup(ch)
    grouped: Dict[str, Dict[str, object]] = {}
    for row in rows:
        if isinstance(row, dict):
            code_raw = row.get("index_code")
            name_raw = row.get("index_name")
            date_raw = row.get("effective_date")
            symbol_display_raw = row.get("symbol")
            symbol_base_raw = row.get("symbol_base")
            company_raw = row.get("company_name")
            weight_raw = row.get("weight")
        else:
            if len(row) >= 7:
                (
                    code_raw,
                    name_raw,
                    date_raw,
                    symbol_display_raw,
                    symbol_base_raw,
                    company_raw,
                    weight_raw,
                ) = row
            else:
                code_raw, name_raw, date_raw, symbol_display_raw, company_raw, weight_raw = row
                symbol_base_raw = None
        if not code_raw or not (symbol_display_raw or symbol_base_raw):
            continue
        code = str(code_raw).upper()
        display_symbol = (
            str(symbol_display_raw).strip().upper() if symbol_display_raw else ""
        )
        base_symbol_candidate = (
            str(symbol_base_raw).strip().upper() if symbol_base_raw else ""
        )
        primary_candidate = display_symbol or base_symbol_candidate
        normalized_primary = (
            normalize_input_symbol(primary_candidate) if primary_candidate else ""
        )
        normalized_base = (
            normalize_input_symbol(base_symbol_candidate) if base_symbol_candidate else ""
        )
        symbol = normalized_primary or normalized_base
        if not symbol:
            continue
        pretty_candidate = display_symbol or pretty_symbol(symbol)
        pretty = pretty_candidate.strip().upper() if pretty_candidate else symbol
        pretty_base = pretty.split(".", 1)[0] if "." in pretty else pretty
        base_symbol = normalized_base or symbol
        effective_date = date_raw
        if isinstance(effective_date, datetime):
            effective_date = effective_date.date()
        if isinstance(effective_date, date):
            effective_iso = effective_date.isoformat()
        else:
            effective_iso = str(effective_date)
        snapshot = grouped.setdefault(
            code,
            {
                "index_code": code,
                "index_name": (str(name_raw).strip() if name_raw else None),
                "effective_date": effective_iso,
                "constituents": [],
            },
        )
        snapshot["effective_date"] = effective_iso
        company_name = str(company_raw).strip() if company_raw else None
        if company_name == "":
            company_name = None
        lookup_keys = [
            symbol,
            base_symbol,
            pretty,
            pretty_base,
            str(symbol_display_raw).strip() if symbol_display_raw else None,
            str(symbol_base_raw).strip() if symbol_base_raw else None,
            company_name,
        ]
        resolved_entry: Optional[_CompanyNameLookupEntry] = None
        for key in lookup_keys:
            if not key:
                continue
            resolved_entry = company_lookup.get(str(key).strip().upper())
            if resolved_entry:
                break
        if resolved_entry:
            resolved_name = resolved_entry.get("name")
            if resolved_name:
                normalized_existing = (company_name or "").strip().upper()
                if not normalized_existing or normalized_existing in {
                    symbol,
                    pretty.upper(),
                    pretty_base.upper(),
                    str(symbol_display_raw).strip().upper() if symbol_display_raw else "",
                }:
                    company_name = resolved_name
        weight = float(weight_raw) if weight_raw is not None else None
        constituents = snapshot["constituents"]  # type: ignore[index]
        constituents.append(
            IndexConstituentResponse(
                symbol=pretty,
                raw_symbol=symbol,
                symbol_base=base_symbol,
                company_name=company_name,
                weight=weight,
            )
        )
    portfolios = [
        IndexPortfolioSnapshotResponse(
            index_code=data["index_code"],
            index_name=data.get("index_name"),
            effective_date=str(data.get("effective_date")),
            constituents=[entry for entry in data["constituents"]],  # type: ignore[index]
        )
        for data in grouped.values()
    ]
    portfolios.sort(key=lambda item: item.index_code)
    for portfolio in portfolios:
        portfolio.constituents.sort(key=lambda item: item.symbol)
    return IndexPortfoliosResponse(portfolios=portfolios)


@api_router.get("/indices/benchmark/symbols", response_model=BenchmarkSymbolListResponse)
def list_benchmark_symbols(
    q: Optional[str] = Query(
        default=None,
        description="Fragment symbolu, nazwy spółki lub kodu indeksu",
    ),
    limit: int = Query(default=1000, ge=1, le=10000),
) -> BenchmarkSymbolListResponse:
    ch = get_ch()
    _ensure_index_tables(ch)

    conditions: List[str] = []
    params: Dict[str, Any] = {"limit": limit}
    if q:
        params["q"] = q
        conditions.append("positionCaseInsensitive(symbol, %(q)s) > 0")
        conditions.append("positionCaseInsensitive(coalesce(symbol_base, ''), %(q)s) > 0")
        conditions.append("positionCaseInsensitive(coalesce(company_name, ''), %(q)s) > 0")
        conditions.append("positionCaseInsensitive(index_code, %(q)s) > 0")

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " OR ".join(conditions)

    query = f"""
        SELECT
            symbol,
            anyHeavy(symbol_base) AS symbol_base,
            groupUniqArray(index_code) AS indices,
            anyHeavy(company_name) AS company_name
        FROM {TABLE_INDEX_PORTFOLIOS}
        {where_clause}
        GROUP BY symbol
        ORDER BY symbol
        LIMIT %(limit)s
    """

    try:
        result = ch.query(query, parameters=params)
    except Exception as exc:  # pragma: no cover - zależy od konfiguracji DB
        raise HTTPException(500, f"Nie udało się pobrać symboli GPW Benchmark: {exc}") from exc

    column_names = list(result.column_names)
    items: List[BenchmarkSymbolResponse] = []

    for row in result.result_rows:
        payload = {col: value for col, value in zip(column_names, row)}
        symbol_raw = _convert_clickhouse_value(payload.get("symbol"))
        if not symbol_raw:
            continue
        symbol_text = str(symbol_raw)
        indices_raw = _convert_clickhouse_value(payload.get("indices"))
        if isinstance(indices_raw, (list, tuple)):
            indices_list = [
                str(item)
                for item in indices_raw
                if item is not None and str(item).strip()
            ]
        else:
            indices_list = []
        item = BenchmarkSymbolResponse(
            symbol=symbol_text,
            symbol_base=_convert_clickhouse_value(payload.get("symbol_base")) or None,
            indices=indices_list,
            company_name=_convert_clickhouse_value(payload.get("company_name")) or None,
        )
        items.append(item)

    return BenchmarkSymbolListResponse(items=items)


@api_router.get("/universe/candidates", response_model=UniverseCandidateListResponse)
def list_universe_candidates(
    universe: List[str] = Query(
        ..., description="Filtry uniwersum, np. index:WIG40 lub isin:PLLOTOS00025",
    ),
    with_company_info: bool = Query(
        default=True,
        description="Czy dołączyć podstawowe informacje o spółkach z tabeli companies",
    ),
    include_index_history: bool = Query(
        default=True,
        description="Uwzględnij historyczne składy indeksów przy filtrowaniu po index:",
    ),
) -> UniverseCandidateListResponse:
    if not universe:
        raise HTTPException(400, "Podaj co najmniej jeden filtr uniwersum.")

    filters = _build_filters_from_universe(universe)
    if filters is None:
        raise HTTPException(400, "Nie znaleziono poprawnych filtrów uniwersum.")

    ch = get_ch()
    symbols = _list_candidate_symbols(
        ch,
        filters,
        include_index_history=include_index_history,
    )

    metadata_lookup: Dict[str, CandidateSymbolMetadata] = {}
    if with_company_info:
        metadata_lookup = _collect_candidate_metadata(ch, symbols)

    items: List[UniverseCandidateResponseItem] = []
    for symbol in symbols:
        normalized = normalize_input_symbol(symbol) or symbol
        entry = metadata_lookup.get(normalized.upper()) or metadata_lookup.get(symbol.upper()) or {}
        items.append(
            UniverseCandidateResponseItem(
                symbol=symbol,
                name=entry.get("name"),
                isin=entry.get("isin"),
                sector=entry.get("sector"),
                industry=entry.get("industry"),
            )
        )

    return UniverseCandidateListResponse(total=len(symbols), items=items)


@api_router.get("/indices/list", response_model=IndexListResponse)
def list_indices(
    q: Optional[str] = Query(default=None, description="Fragment kodu lub nazwy indeksu"),
    limit: int = Query(default=200, ge=1, le=2000),
) -> IndexListResponse:
    ch = get_ch()
    _ensure_index_tables(ch)
    params: Dict[str, Any] = {"limit": limit}
    where_clause = ""
    if q:
        params["q"] = q
        where_clause = (
            " WHERE (positionCaseInsensitive(index_code, %(q)s) > 0"
            " OR (index_name IS NOT NULL AND positionCaseInsensitive(index_name, %(q)s) > 0))"
        )
    query = f"""
        SELECT index_code, anyLast(index_name) AS index_name
        FROM {TABLE_INDEX_PORTFOLIOS}
        {where_clause}
        GROUP BY index_code
        ORDER BY index_code
        LIMIT %(limit)s
    """
    try:
        rows = ch.query(query, parameters=params).named_results()
    except AttributeError:
        rows = None
    if rows is None:
        rows = [
            {"index_code": row[0], "index_name": row[1]}
            for row in ch.query(query, parameters=params).result_rows
        ]

    items: List[IndexListItemResponse] = []
    for row in rows:
        if isinstance(row, dict):
            code_raw = row.get("index_code")
            name_raw = row.get("index_name")
        else:
            code_raw, name_raw = row
        if not code_raw:
            continue
        code = _sanitize_index_code(str(code_raw))
        if not code:
            continue
        name = str(name_raw).strip() if name_raw else None
        items.append(IndexListItemResponse(code=code, name=name or None))

    items.sort(key=lambda item: item.code)
    return IndexListResponse(items=items)


@api_router.get("/indices/history", response_model=IndexHistoryResponse)
def list_index_history(
    codes: Optional[List[str]] = Query(default=None),
    start: Optional[str] = Query(default=None, description="Początek zakresu (YYYY-MM-DD)"),
    end: Optional[str] = Query(default=None, description="Koniec zakresu (YYYY-MM-DD)"),
) -> IndexHistoryResponse:
    ch = get_ch()
    start_dt: Optional[date] = None
    end_dt: Optional[date] = None
    if start:
        try:
            start_dt = date.fromisoformat(start)
        except ValueError as exc:
            raise HTTPException(400, "start must be in format YYYY-MM-DD") from exc
    if end:
        try:
            end_dt = date.fromisoformat(end)
        except ValueError as exc:
            raise HTTPException(400, "end must be in format YYYY-MM-DD") from exc
    if start_dt and end_dt and end_dt < start_dt:
        raise HTTPException(400, "end must not be earlier than start")

    rows = _fetch_index_history_rows(ch, codes, start=start_dt, end=end_dt)
    grouped: Dict[str, Dict[str, object]] = {}
    for row in rows:
        if isinstance(row, dict):
            code_raw = row.get("index_code")
            name_raw = row.get("index_name")
            date_raw = row.get("date")
            value_raw = row.get("value")
            change_raw = row.get("change_pct")
        else:
            code_raw, name_raw, date_raw, value_raw, change_raw = row
        if not code_raw:
            continue
        code = str(code_raw).upper()
        series = grouped.setdefault(
            code,
            {
                "index_code": code,
                "index_name": (str(name_raw).strip() if name_raw else None),
                "points": [],
            },
        )
        point_date = date_raw
        if isinstance(point_date, datetime):
            point_date = point_date.date()
        if isinstance(point_date, date):
            date_iso = point_date.isoformat()
        else:
            date_iso = str(point_date)
        value = float(value_raw) if value_raw is not None else None
        change = float(change_raw) if change_raw is not None else None
        points = series["points"]  # type: ignore[index]
        points.append(IndexHistoryPointResponse(date=date_iso, value=value, change_pct=change))
    items = [
        IndexHistorySeriesResponse(
            index_code=data["index_code"],
            index_name=data.get("index_name"),
            points=[point for point in data["points"]],  # type: ignore[index]
        )
        for data in grouped.values()
    ]
    items.sort(key=lambda item: item.index_code)
    for entry in items:
        entry.points.sort(key=lambda point: point.date)
    return IndexHistoryResponse(items=items)


api_router.include_router(windows_agent_router)
app.include_router(api_router)
app.include_router(api_router, prefix="/api/admin")
