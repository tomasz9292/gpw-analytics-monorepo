# api/main.py
from __future__ import annotations

import io
import json
import os
import re
import statistics
import textwrap
import unicodedata
from datetime import date, datetime, timedelta, timezone
from math import sqrt
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from typing import Literal
from uuid import uuid4

from urllib.parse import parse_qs, urlparse

import clickhouse_connect
import threading
from decimal import Decimal
from fastapi import Depends, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from .company_ingestion import CompanyDataHarvester, CompanySyncProgress, CompanySyncResult
from .sector_classification_data import GPW_SECTOR_CLASSIFICATION

# =========================
# Konfiguracja / połączenie
# =========================

TABLE_OHLC = os.getenv("TABLE_OHLC", "ohlc")
TABLE_COMPANIES = os.getenv("TABLE_COMPANIES", "companies")

DEFAULT_COMPANIES_TABLE_DDL = textwrap.dedent(
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE_COMPANIES} (
        symbol String,
        ticker String,
        code String,
        isin Nullable(String),
        name Nullable(String),
        company_name Nullable(String),
        full_name Nullable(String),
        short_name Nullable(String),
        sector Nullable(String),
        industry Nullable(String),
        country Nullable(String),
        headquarters Nullable(String),
        city Nullable(String),
        website Nullable(String),
        url Nullable(String),
        description Nullable(String),
        profile Nullable(String),
        logo Nullable(String),
        logo_url Nullable(String),
        image_url Nullable(String),
        employees Nullable(Int32),
        employee_count Nullable(Int32),
        founded Nullable(Int32),
        founded_year Nullable(Int32),
        established Nullable(Int32),
        listing_date Nullable(String),
        ipo_date Nullable(String),
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

ALLOWED_SCORE_METRICS = {"total_return", "volatility", "max_drawdown", "sharpe"}


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


@app.get("/ping")
def ping() -> str:
    return "pong"


# =========================
# Aliasy RAW -> .WA
# =========================

# Dodawaj wg potrzeb.
ALIASES_RAW_TO_WA: Dict[str, str] = {
    "CDPROJEKT": "CDR.WA",
    "PKNORLEN": "PKN.WA",
    "PEKAO": "PEO.WA",
    "KGHM": "KGH.WA",
    "PGE": "PGE.WA",
    "ALLEGRO": "ALE.WA",
    "DINOPL": "DNP.WA",
    "LPP": "LPP.WA",
    "ORANGEPL": "OPL.WA",
    "MERCATOR": "MRC.WA",
    # ...
}

# odwrotna mapa .WA -> RAW (wygodna do normalizacji wejścia)
ALIASES_WA_TO_RAW: Dict[str, str] = {wa.lower(): raw for raw, wa in ALIASES_RAW_TO_WA.items()}


def pretty_symbol(raw: str) -> str:
    """
    Zwraca 'ładny' ticker z sufiksem .WA jeśli znamy alias; w p.p. zwraca raw.
    """
    return ALIASES_RAW_TO_WA.get(raw, raw)


def normalize_input_symbol(s: str) -> str:
    """
    Dla wejścia użytkownika zwraca surowy symbol (RAW) używany w bazie.
    Obsługuje zarówno 'CDR.WA' jak i 'CDPROJEKT'.

    W praktyce użytkownicy często wpisują tickery małymi literami albo z
    sufiksem .WA dla spółek z GPW.  Funkcja stara się więc:
    - przywrócić RAW z mapy aliasów, jeśli go znamy,
    - w przeciwnym razie, gdy ticker wygląda jak "XYZ.WA", uciąć sufiks i
      zwrócić bazowy symbol,
    - w ostateczności zwrócić wejście spójne wielkościowo (UPPER).
    """

    cleaned = s.strip()
    if not cleaned:
        return ""

    maybe = ALIASES_WA_TO_RAW.get(cleaned.lower())
    if maybe:
        return maybe

    if "." in cleaned:
        base = cleaned.split(".", 1)[0].strip()
        if base:
            return base.upper()

    return cleaned.upper()


# =========================
# Dane o spółkach (mapowania + cache)
# =========================

COMPANY_SYMBOL_CANDIDATES = [
    "symbol",
    "ticker",
    "code",
    "company_symbol",
    "company_code",
]

COMPANY_NAME_CANDIDATES = [
    "name",
    "company_name",
    "full_name",
    "short_name",
]

CompanyFieldTarget = Tuple[str, str, str]


COMPANY_COLUMN_MAP: Dict[str, CompanyFieldTarget] = {
    # podstawowe informacje identyfikacyjne
    "symbol": ("company", "raw_symbol", "text"),
    "ticker": ("company", "raw_symbol", "text"),
    "code": ("company", "raw_symbol", "text"),
    "isin": ("company", "isin", "text"),
    "name": ("company", "name", "text"),
    "company_name": ("company", "name", "text"),
    "full_name": ("company", "name", "text"),
    "short_name": ("company", "short_name", "text"),
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

_COMPANY_COLUMNS_CACHE: Optional[List[str]] = None
_COMPANY_COLUMNS_LOCK = threading.Lock()


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


def _find_company_symbol_column(columns: Sequence[str]) -> Optional[str]:
    lowered_to_original = {col.lower(): col for col in columns}
    for candidate in COMPANY_SYMBOL_CANDIDATES:
        existing = lowered_to_original.get(candidate)
        if existing:
            return existing
    return None


def _normalize_company_row(row: Dict[str, Any], symbol_column: str) -> Optional[Dict[str, Any]]:
    canonical: Dict[str, Any] = {
        "raw_symbol": None,
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
                canonical[field_name] = str(converted_value)
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

    raw_symbol_value = canonical.get("raw_symbol")
    if not raw_symbol_value:
        fallback = row.get(symbol_column)
        if fallback is None:
            return None
        raw_symbol_value = str(_convert_clickhouse_value(fallback))
        canonical["raw_symbol"] = raw_symbol_value

    canonical["symbol"] = pretty_symbol(str(raw_symbol_value))
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


class PortfolioResp(BaseModel):
    equity: List[PortfolioPoint]
    stats: PortfolioStats
    rebalances: Optional[List[PortfolioRebalanceEvent]] = None


class PortfolioScoreItem(BaseModel):
    symbol: str
    raw: str
    score: float


class ScoreComponent(BaseModel):
    lookback_days: int = Field(..., ge=1, le=3650)
    metric: str = Field(..., description="Typ metryki score'u (np. total_return)")
    weight: float = Field(..., gt=0)
    direction: str = Field("desc", pattern="^(asc|desc)$")

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


class ManualPortfolioConfig(BaseModel):
    symbols: List[str] = Field(..., min_length=1)
    weights: Optional[List[float]] = None

    @model_validator(mode="after")
    def _validate_weights(self):
        if self.weights is not None and len(self.weights) != len(self.symbols):
            raise ValueError("Liczba wag musi odpowiadać liczbie symboli")
        return self


class AutoSelectionConfig(BaseModel):
    top_n: int = Field(..., ge=1, le=100)
    components: List[ScoreComponent] = Field(..., min_length=1)
    filters: Optional[UniverseFilters] = None
    weighting: str = Field("equal", pattern="^(equal|score)$")
    direction: str = Field("desc", pattern="^(asc|desc)$")


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


class ScorePreviewRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    rules: List[ScoreRulePayload] = Field(..., min_length=1)
    limit: Optional[int] = Field(None, ge=1, le=100)
    universe: Optional[List[str]] = None
    sort: Optional[str] = Field(None, pattern="^(asc|desc)$")

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


class ScorePreviewResponse(BaseModel):
    name: Optional[str] = None
    as_of: str
    universe_count: int
    rows: List[ScorePreviewRow]
    meta: Dict[str, object]


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


@app.post("/companies/sync/background", response_model=CompanySyncJobStatus)
def start_company_sync(
    limit: Optional[int] = Query(default=None, ge=1, le=5000),
) -> CompanySyncJobStatus:
    with _SYNC_LOCK:
        if _SYNC_STATE.status == "running":
            raise HTTPException(409, "Synchronizacja spółek jest już w toku")
        _start_sync_job(limit, message="Rozpoczęto synchronizację spółek")
        return _SYNC_STATE.model_copy(deep=True)


@app.get("/companies/sync/status", response_model=CompanySyncJobStatus)
def company_sync_status() -> CompanySyncJobStatus:
    return _snapshot_sync_state()


@app.get("/companies/sync/schedule", response_model=CompanySyncScheduleStatus)
def company_sync_schedule() -> CompanySyncScheduleStatus:
    return _snapshot_schedule_state()


@app.post("/companies/sync/schedule", response_model=CompanySyncScheduleStatus)
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


@app.post("/companies/sync", response_model=CompanySyncResult)
def sync_companies(limit: Optional[int] = Query(default=None, ge=1, le=5000)):
    ch = get_ch()
    columns = _get_company_columns(ch)
    harvester = CompanyDataHarvester()
    result = harvester.sync(
        ch_client=ch,
        table_name=TABLE_COMPANIES,
        columns=columns,
        limit=limit,
    )
    return result


@app.get("/companies", response_model=List[CompanyProfile])
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


@app.get("/companies/{symbol}", response_model=CompanyProfile)
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


# =========================
# /symbols – lista tickerów
# =========================

@app.get("/symbols")
def symbols(
    q: Optional[str] = Query(default=None, description="fragment symbolu"),
    limit: int = Query(default=200, ge=1, le=2000),
):
    """
    Zwraca listę symboli:
    - symbol: ładny ticker (np. CDR.WA)
    - raw: surowy symbol w bazie (np. CDPROJEKT)
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

    out = []
    for r in rows:
        raw = str(r[0])
        out.append({"symbol": pretty_symbol(raw), "raw": raw})
    return out


# =========================
# /quotes – notowania OHLC
# =========================

@app.get("/quotes", response_model=List[QuoteRow])
def quotes(symbol: str, start: Optional[str] = None):
    """
    Zwraca notowania OHLC dla symbolu od wskazanej daty.
    Obsługuje zarówno 'CDR.WA' jak i 'CDPROJEKT'.
    """
    raw_symbol = normalize_input_symbol(symbol)
    if not raw_symbol:
        raise HTTPException(400, "symbol must not be empty")

    try:
        dt = date.fromisoformat(start) if start else date(2015, 1, 1)
    except Exception:
        raise HTTPException(400, "start must be in format YYYY-MM-DD")

    ch = get_ch()
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


@app.get("/data-collection", response_model=List[DataCollectionItem])
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


@app.get("/sectors/classification", response_model=List[SectorClassificationEntry])
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

    rows = ch_client.query(
        f"""
        WITH latest AS (
            SELECT symbol, max(date) AS last_date
            FROM {TABLE_OHLC}
            WHERE symbol IN %(symbols)s
            GROUP BY symbol
        )
        SELECT o.symbol, toString(o.date) AS date, o.close
        FROM {TABLE_OHLC} AS o
        INNER JOIN latest AS l ON o.symbol = l.symbol
        WHERE o.symbol IN %(symbols)s
          AND o.date >= addDays(l.last_date, -%(window)s)
        ORDER BY o.symbol, o.date
        """,
        parameters={"symbols": tuple(unique_symbols), "window": window},
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
    closes: Sequence[Tuple[date, float]], metric: str, lookback_days: int
) -> Optional[float]:
    if not closes:
        return None

    if metric == "total_return":
        last_dt, last_close = closes[-1]
        if last_close <= 0:
            return None
        target_dt = last_dt - timedelta(days=lookback_days)
        base_close = None
        for dt, close in reversed(closes):
            if dt <= target_dt:
                if close > 0:
                    base_close = close
                break
        if base_close is None or base_close <= 0:
            return None
        return (last_close / base_close) - 1.0

    window = _slice_closes_window(closes, lookback_days)
    if len(window) < 2:
        return None

    returns: List[float] = []
    for (_, prev_close), (_, next_close) in zip(window, window[1:]):
        if prev_close <= 0:
            continue
        returns.append(next_close / prev_close - 1.0)

    if metric == "volatility":
        if len(returns) < 2:
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
            return None
        avg = statistics.mean(returns)
        stdev = statistics.pstdev(returns)
        if stdev <= 1e-12:
            return None
        return (avg / stdev) * sqrt(252)

    return None


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

    total_weight = 0.0
    weighted = 0.0
    metrics: Dict[str, float] = {}

    for comp in components:
        value = _compute_metric_value(closes, comp.metric, comp.lookback_days)
        if value is None:
            return None

        key = f"{comp.metric}_{comp.lookback_days}"
        metrics[key] = value

        direction = -1.0 if comp.direction == "asc" else 1.0
        weighted += comp.weight * direction * value
        total_weight += comp.weight

    if total_weight <= 0:
        return None

    score = weighted / total_weight
    if include_metrics:
        return score, metrics
    return score


def _collect_all_company_symbols(ch_client) -> Optional[List[str]]:
    company_symbols: set[str] = set()
    try:
        columns = _get_company_columns(ch_client)
    except HTTPException:
        columns = None

    if columns:
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
                    normalized = normalize_input_symbol(str(raw_value))
                    if normalized:
                        company_symbols.add(normalized)

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


def _list_candidate_symbols(ch_client, filters: Optional[UniverseFilters]) -> List[str]:
    symbols = _collect_all_company_symbols(ch_client)
    if symbols is None:
        rows = ch_client.query(
            f"""
            SELECT DISTINCT symbol
            FROM {TABLE_OHLC}
            ORDER BY symbol
            """
        ).result_rows
        symbols = [str(r[0]) for r in rows]

    if not filters:
        return symbols

    includes = None
    if filters.include:
        includes = {normalize_input_symbol(sym) for sym in filters.include}
        includes = {sym for sym in includes if sym}
        if not includes:
            raise HTTPException(400, "Lista include nie zawiera poprawnych symboli")

    excludes = set()
    if filters.exclude:
        excludes = {normalize_input_symbol(sym) for sym in filters.exclude}
        excludes = {sym for sym in excludes if sym}

    prefixes = None
    if filters.prefixes:
        prefixes = [p.strip().upper() for p in filters.prefixes if p.strip()]

    filtered: List[str] = []
    for sym in symbols:
        if includes and sym not in includes:
            continue
        if sym in excludes:
            continue
        if prefixes and not any(sym.startswith(pref) for pref in prefixes):
            continue
        filtered.append(sym)

    return filtered


def _rank_symbols_by_score(
    ch_client,
    candidates: List[str],
    components: List[ScoreComponent],
    include_metrics: bool = False,
) -> List[Tuple[str, float] | Tuple[str, float, Dict[str, float]]]:
    history_map = _collect_close_history_bulk(ch_client, candidates, components)
    ranked: List[Tuple[str, float] | Tuple[str, float, Dict[str, float]]] = []
    for sym in candidates:
        result = _calculate_symbol_score(
            ch_client,
            sym,
            components,
            include_metrics=include_metrics,
            preloaded_closes=history_map.get(sym),
        )
        if result is None:
            continue
        if include_metrics:
            score, metrics = result  # type: ignore[misc]
            ranked.append((sym, score, metrics))
        else:
            ranked.append((sym, result))  # type: ignore[arg-type]

    ranked.sort(key=lambda item: item[1], reverse=True)
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
            )
        except ValidationError as exc:
            raise HTTPException(400, exc.errors()) from exc
        components.append(component)
    if not components:
        raise HTTPException(400, "Lista reguł score nie może być pusta")
    return components


def _build_auto_config_from_preview(req: ScorePreviewRequest) -> AutoSelectionConfig:
    components = _build_components_from_rules(req.rules)
    top_n = req.limit or len(components)
    filters = None
    if req.universe:
        filters = UniverseFilters(include=req.universe)
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

    tot = sum(weights_pct) or 1.0
    base_weights: Dict[str, float] = {}
    for sym, weight in zip(closes_map.keys(), weights_pct):
        base_weights[sym] = (weight or 0.0) / tot

    rebal_dates = set(_rebalance_dates(all_dates, rebalance))

    equity: List[PortfolioPoint] = []
    rebalances: List[PortfolioRebalanceEvent] = []
    shares: Dict[str, float] = {sym: 0.0 for sym in closes_map.keys()}

    def _to_ratio(value: float) -> float:
        return value / 100.0 if abs(value) > 1 else value

    portfolio_initial = initial_capital if initial_capital > 0 else 1.0
    portfolio_value = portfolio_initial
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
            portfolio_value = 0.0
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

        if should_rebalance:
            # symbole, które mogą uczestniczyć w rebalansingu
            available_syms = list(prices_today.keys())
            symbols_with_weight = [sym for sym in available_syms if base_weights.get(sym, 0.0) > 0]
            if not symbols_with_weight:
                symbols_with_weight = available_syms

            weight_sum = sum(base_weights.get(sym, 0.0) for sym in symbols_with_weight)
            targets: Dict[str, float] = {}
            if weight_sum > 0:
                for sym in symbols_with_weight:
                    targets[sym] = base_weights.get(sym, 0.0) / weight_sum
            else:
                equal = 1.0 / len(symbols_with_weight) if symbols_with_weight else 0.0
                for sym in symbols_with_weight:
                    targets[sym] = equal

            for sym in available_syms:
                targets.setdefault(sym, 0.0)

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

        # aktualizacja wartości portfela po ewentualnym rebalansingu
        portfolio_value = 0.0
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

    if req.manual:
        raw_syms: List[str] = []
        for s in req.manual.symbols:
            raw = normalize_input_symbol(s)
            if not raw:
                raise HTTPException(400, "Symbol nie może być pusty")
            raw_syms.append(raw)

        weights_list = list(req.manual.weights) if req.manual.weights else [1.0] * len(raw_syms)
    else:
        assert req.auto is not None
        candidates = _list_candidate_symbols(ch, req.auto.filters)
        if not candidates:
            raise HTTPException(404, "Brak symboli do oceny")

        ranked = _rank_symbols_by_score(ch, candidates, req.auto.components)
        if req.auto.direction == "asc":
            ranked = list(reversed(ranked))
        if not ranked:
            raise HTTPException(404, "Brak symboli ze wszystkimi wymaganymi danymi")

        top = ranked[: req.auto.top_n]
        raw_syms = [sym for sym, _ in top]
        if not raw_syms:
            raise HTTPException(404, "Brak symboli po filtrach")

        if req.auto.weighting == "score":
            weights_list = [score for _, score in top]
            if not any(weights_list):
                weights_list = [1.0] * len(top)
        else:
            weights_list = [1.0] * len(top)

    closes_map: Dict[str, List[Tuple[str, float]]] = {}
    for rs in raw_syms:
        series = _fetch_close_series(ch, rs, dt_start, req.end)
        if not series:
            raise HTTPException(404, f"Brak danych historycznych dla {rs}")
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
    )
    return PortfolioResp(
        equity=equity,
        stats=stats,
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

    top = ranked[: req.auto.top_n]
    return [
        PortfolioScoreItem(symbol=pretty_symbol(sym), raw=sym, score=score)
        for sym, score in top
    ]


def _run_score_preview(req: ScorePreviewRequest) -> ScorePreviewResponse:
    auto_config = _build_auto_config_from_preview(req)
    ch = get_ch()

    candidates = _list_candidate_symbols(ch, auto_config.filters)
    if not candidates:
        raise HTTPException(404, "Brak symboli do oceny")

    ranked = _rank_symbols_by_score(
        ch, candidates, auto_config.components, include_metrics=True
    )
    if not ranked:
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

    limit = req.limit or auto_config.top_n
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

    as_of = date.today().isoformat()
    meta: Dict[str, object] = {
        "name": req.name,
        "as_of": as_of,
        "universe_count": len(candidates),
    }

    return ScorePreviewResponse(
        name=req.name,
        as_of=as_of,
        universe_count=len(candidates),
        rows=rows,
        meta=meta,
    )


@app.post("/score/preview", response_model=ScorePreviewResponse)
def score_preview(req: ScorePreviewRequest):
    return _run_score_preview(req)


@app.post("/scores/preview", response_model=ScorePreviewResponse)
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


@app.get("/backtest/portfolio", response_model=PortfolioResp)
def backtest_portfolio_get(req: BacktestPortfolioRequest = Depends(_parse_backtest_get)):
    """GET-owy wariant backtestu portfela.

    Umożliwia szybkie testy z poziomu przeglądarki, np.:

    ``/backtest/portfolio?mode=manual&symbols=CDR.WA&symbols=PKN.WA&start=2023-01-01``

    ``/backtest/portfolio?mode=auto&top_n=3&components=252:total_return:5``
    """

    return _run_backtest(req)


@app.post("/backtest/portfolio", response_model=PortfolioResp)
def backtest_portfolio(req: BacktestPortfolioRequest):
    """Backtest portfela na bazie kursów zamknięcia.

    Endpoint obsługuje zarówno klasyczny POST (JSON), jak i wariant GET opisany
    w dokumentacji wyżej. Tryb ``manual`` przyjmuje listę symboli oraz opcjonalne
    wagi, a tryb ``auto`` – konfigurację komponentów score'u wraz z filtrami
    wszechświata i sposobem ważenia (``equal`` lub ``score``).
    """

    return _run_backtest(req)


@app.post("/backtest/portfolio/score", response_model=List[PortfolioScoreItem])
def backtest_portfolio_score(req: PortfolioScoreRequest):
    """Zwraca ranking spółek na podstawie konfiguracji trybu auto."""

    return _compute_portfolio_score(req)


@app.get("/backtest/portfolio/tooling", response_model=BacktestPortfolioTooling)
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
            top_n=RangeDescriptor(min=1, max=100, step=1, default=5),
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
