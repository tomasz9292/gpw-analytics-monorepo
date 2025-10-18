from __future__ import annotations

import json
from http.cookiejar import CookieJar
from html import unescape as html_unescape
from html.parser import HTMLParser
import re
from xml.etree import ElementTree
from datetime import date, datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence
from typing import Literal
from urllib.error import URLError
from urllib.parse import urlencode, urlparse, parse_qs, urlsplit, urlunsplit
from urllib.request import HTTPCookieProcessor, Request, build_opener
from pydantic import BaseModel, Field

GPW_COMPANY_PROFILES_URL = "https://www.gpw.pl/ajaxindex.php"
GPW_COMPANY_PROFILES_FALLBACK_URL = "https://www.gpw.pl/restapi/GPWCompanyProfiles"
STOOQ_COMPANY_CATALOG_URL = "https://stooq.pl/t/?i=513"
STOOQ_COMPANY_PROFILE_URL = "https://stooq.pl/q/p/?s={symbol}"
YAHOO_QUOTE_SUMMARY_URL = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
YAHOO_MODULES = (
    "price,assetProfile,summaryDetail,defaultKeyStatistics,financialData"
)

GOOGLE_FINANCE_QUOTE_URL = "https://www.google.com/finance/quote/{symbol}"


class SimpleHttpResponse:
    def __init__(self, status_code: int, body: bytes) -> None:
        self.status_code = status_code
        self._body = body

    def json(self) -> Dict[str, Any]:
        """Zwraca sparsowaną odpowiedź JSON z zabezpieczeniami na typowe błędy."""

        decoded = self._body.decode("utf-8-sig", errors="replace")
        stripped = decoded.strip()
        if not stripped:
            raise RuntimeError("Pusta odpowiedź serwera (oczekiwano JSON)")

        for strict in (True, False):
            try:
                return json.loads(stripped, strict=strict)
            except json.JSONDecodeError:
                continue

        xml_detail = _extract_xml_error_detail(stripped)
        if xml_detail:
            detail = f" (serwer zwrócił komunikat: {xml_detail})"
        else:
            snippet = " ".join(stripped.split())[:200]
            detail = f" (fragment: {snippet})" if snippet else ""
        raise RuntimeError(f"Niepoprawna odpowiedź JSON{detail}")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    @property
    def content(self) -> bytes:
        return self._body

    def text(self, encoding: str = "utf-8", errors: str = "replace") -> str:
        return self._body.decode(encoding, errors=errors)


class HttpRequestLog(BaseModel):
    url: str
    params: Dict[str, Any] = Field(default_factory=dict)
    started_at: datetime = Field(default_factory=datetime.utcnow)
    finished_at: Optional[datetime] = None
    status_code: Optional[int] = None
    error: Optional[str] = None


class SimpleHttpSession:
    """Minimalna sesja HTTP ze wsparciem nagłówków wymaganych przez GPW."""

    DEFAULT_HEADERS: Dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.7,en;q=0.6",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.gpw.pl/",
        "Connection": "keep-alive",
    }

    def __init__(
        self,
        headers: Optional[Dict[str, str]] = None,
        *,
        opener: Optional[Any] = None,
    ) -> None:
        self.headers = dict(self.DEFAULT_HEADERS)
        if headers:
            self.headers.update(headers)
        self.history: List[HttpRequestLog] = []
        self.cookie_jar = CookieJar()
        self._opener = opener or build_opener(HTTPCookieProcessor(self.cookie_jar))

    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 15,
    ) -> SimpleHttpResponse:
        if params:
            query = urlencode(params, doseq=True)
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{query}"
        log_entry = HttpRequestLog(url=url, params=params or {})
        self.history.append(log_entry)
        try:
            request = Request(url, headers=self.headers)
            with self._opener.open(request, timeout=timeout) as response:  # type: ignore[arg-type]
                status = getattr(response, "status", 200)
                body = response.read()
            log_entry.status_code = status
            log_entry.finished_at = datetime.utcnow()
            return SimpleHttpResponse(status_code=status, body=body)
        except Exception as exc:
            log_entry.error = str(exc)
            log_entry.finished_at = datetime.utcnow()
            raise

    def clear_history(self) -> None:
        self.history.clear()

    def get_history(self) -> List["HttpRequestLog"]:
        return list(self.history)


def _clean_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _merge_missing_fields(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    for key, value in source.items():
        if key == "raw_payload":
            continue
        if _is_blank(target.get(key)) and not _is_blank(value):
            target[key] = value


def _merge_company_rows(existing: Dict[str, Any], new_data: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(existing)
    for key, value in new_data.items():
        if key == "raw_payload":
            merged[key] = value
            continue
        if not _is_blank(value):
            merged[key] = value
        elif key not in merged:
            merged[key] = value
    return merged


def _quote_sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _extract_xml_error_detail(document: str) -> Optional[str]:
    cleaned = document.lstrip("\ufeff").strip()
    if not cleaned.startswith("<"):
        return None
    try:
        root = ElementTree.fromstring(cleaned)
    except ElementTree.ParseError:
        return None

    if root.tag.lower() == "html":
        return None

    def _local_name(tag: str) -> str:
        """Return the element tag name without any XML namespace."""

        if "}" in tag:
            return tag.rsplit("}", 1)[-1]
        return tag

    def _collect_texts(tag: str) -> List[str]:
        target = tag.lower()
        values: List[str] = []
        for element in root.iter():
            element_tag = _local_name(element.tag).lower()
            if element_tag != target:
                continue
            text = " ".join(" ".join(element.itertext()).split())
            if text and text not in values:
                values.append(text)
        return values

    def _truncate(value: str) -> str:
        return value if len(value) <= 200 else f"{value[:197]}..."

    status_texts = _collect_texts("status")
    status_text = status_texts[0] if status_texts else None

    detail_tags = (
        "message",
        "error",
        "title",
        "description",
        "details",
        "detail",
        "reason",
        "statusdetails",
        "statusdetail",
    )
    detail_values: List[str] = []
    for tag in detail_tags:
        for value in _collect_texts(tag):
            if value not in detail_values and value != status_text:
                detail_values.append(value)

    if status_text and detail_values:
        combined = f"{status_text} – {'; '.join(detail_values)}"
        return _truncate(combined)
    if status_text:
        return _truncate(status_text)
    if detail_values:
        return _truncate("; ".join(detail_values))

    collected = [" ".join(text.split()) for text in root.itertext()]
    summary = " ".join(filter(None, collected))[:200]
    return summary or None


def _normalize_gpw_symbol(value: str) -> str:
    """Zwraca surowy symbol GPW z walidacją sufiksu."""

    normalized = value.strip().upper()
    if not normalized:
        raise RuntimeError("Pusty symbol spółki")

    if "." in normalized:
        if normalized.endswith(".WA"):
            normalized = normalized.rsplit(".", 1)[0]
        else:
            raise RuntimeError(f"Symbol spoza GPW: {normalized}")

    return normalized


def _clean_website(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    parsed = urlparse(url if "://" in url else f"https://{url}")
    if not parsed.netloc:
        return None
    scheme = "https"
    return f"{scheme}://{parsed.netloc}{parsed.path or ''}"


def _extract_domain(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    parsed = urlparse(url)
    if not parsed.netloc:
        return None
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _logo_url_from_website(website: Optional[str]) -> Optional[str]:
    domain = _extract_domain(website)
    if not domain:
        return None
    return f"https://logo.clearbit.com/{domain}"


def _clean_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            return int(float(cleaned.replace(",", ".")))
        except ValueError:
            return None
    return None


def _clean_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", ".")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    if isinstance(value, dict) and "raw" in value:
        return _clean_float(value.get("raw"))
    return None


def _clean_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d", "%Y.%m.%d"):
            try:
                return datetime.strptime(cleaned, fmt).date().isoformat()
            except ValueError:
                continue
        if len(cleaned) == 4 and cleaned.isdigit():
            return f"{cleaned}-01-01"
    return None


def _value_from_path(data: Dict[str, Any], *path: str) -> Any:
    current: Any = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
        if current is None:
            return None
    if isinstance(current, dict) and "raw" in current:
        return current.get("raw")
    return current


class CompanySyncProgress(BaseModel):
    stage: Literal["fetching", "harvesting", "inserting", "finished", "failed"]
    total: Optional[int] = Field(None, description="Szacowana liczba spółek do przetworzenia")
    processed: int = Field(0, description="Liczba rekordów przetworzonych z listy GPW")
    synced: int = Field(0, description="Liczba spółek przygotowanych do synchronizacji")
    failed: int = Field(0, description="Liczba błędów napotkanych podczas przetwarzania")
    current_symbol: Optional[str] = Field(
        None, description="Symbol spółki przetwarzanej w ostatnim kroku"
    )
    message: Optional[str] = Field(
        None, description="Dodatkowy komunikat dla interfejsu użytkownika"
    )


class CompanySyncResult(BaseModel):
    fetched: int = Field(..., description="Liczba spółek pobranych z listy GPW")
    synced: int = Field(..., description="Liczba spółek wstawionych do bazy")
    failed: int = Field(..., description="Liczba spółek z błędami podczas synchronizacji")
    errors: List[str] = Field(default_factory=list)
    started_at: datetime = Field(..., description="Czas rozpoczęcia synchronizacji")
    finished_at: datetime = Field(..., description="Czas zakończenia synchronizacji")
    request_log: List[HttpRequestLog] = Field(
        default_factory=list,
        description="Historia zapytań HTTP wykonanych podczas synchronizacji",
    )
    requested_as_admin: bool = Field(
        False, description="Czy synchronizacja została wywołana w trybie administratora"
    )
    sync_type: Literal["company_info"] = Field(
        "company_info", description="Rodzaj przeprowadzonej synchronizacji"
    )


def _extract_company_rows(payload: Any) -> List[Dict[str, Any]]:
    """Wydobywa listę słowników z różnych wariantów odpowiedzi GPW."""

    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]

    if isinstance(payload, dict):
        candidate_keys = (
            "data",
            "content",
            "items",
            "results",
            "records",
            "rows",
            "companies",
        )
        for key in candidate_keys:
            rows = payload.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]

        # Niektóre odpowiedzi mogą być mapą symbol -> dane
        if all(isinstance(value, dict) for value in payload.values()):
            return [value for value in payload.values() if isinstance(value, dict)]

    return []


class _HtmlTableState:
    def __init__(self) -> None:
        self.rows: List[List[str]] = []
        self.current_row: Optional[List[str]] = None
        self.current_cell: Optional[List[str]] = None
        self.cell_depth: int = 0

    def start_row(self) -> None:
        self.current_row = []

    def end_row(self) -> None:
        if self.current_row is None:
            return
        cleaned = [" ".join(cell.split()) for cell in self.current_row]
        if any(cleaned):
            self.rows.append(cleaned)
        self.current_row = None

    def start_cell(self) -> None:
        if self.current_row is None:
            self.current_row = []
        self.current_cell = []
        self.cell_depth = 1

    def append_data(self, data: str) -> None:
        if self.current_cell is None:
            return
        self.current_cell.append(data)

    def end_cell(self) -> None:
        if self.current_cell is None or self.current_row is None:
            self.current_cell = None
            self.cell_depth = 0
            return
        text = "".join(self.current_cell)
        text = " ".join(text.split())
        self.current_row.append(text)
        self.current_cell = None
        self.cell_depth = 0


class _HtmlTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._stack: List[_HtmlTableState] = []
        self.tables: List[List[List[str]]] = []

    def handle_starttag(self, tag: str, attrs: Any) -> None:  # type: ignore[override]
        if tag == "table":
            self._stack.append(_HtmlTableState())
            return

        if not self._stack:
            return

        current = self._stack[-1]
        if tag == "tr":
            current.start_row()
            return
        if tag in {"td", "th"}:
            current.start_cell()
            return
        if tag == "br":
            current.append_data("\n")
            return
        if current.cell_depth > 0:
            current.cell_depth += 1

    def handle_endtag(self, tag: str) -> None:  # type: ignore[override]
        if not self._stack:
            return

        current = self._stack[-1]
        if tag in {"td", "th"}:
            current.end_cell()
            return
        if tag == "tr":
            current.end_row()
            return
        if tag == "table":
            finished = self._stack.pop()
            if finished.rows:
                self.tables.append(finished.rows)
            return
        if current.cell_depth > 0:
            current.cell_depth -= 1

    def handle_data(self, data: str) -> None:  # type: ignore[override]
        if not self._stack:
            return
        current = self._stack[-1]
        if current.cell_depth > 0 or current.current_cell is not None:
            current.append_data(data)

    def handle_entityref(self, name: str) -> None:  # type: ignore[override]
        from html import unescape

        self.handle_data(unescape(f"&{name};"))

    def handle_charref(self, name: str) -> None:  # type: ignore[override]
        from html import unescape

        self.handle_data(unescape(f"&#{name};"))


def _normalize_stooq_header(value: str) -> Optional[str]:
    normalized = " ".join(value.strip().casefold().split())
    if not normalized:
        return None
    mapping = {
        "symbol": "symbol",
        "ticker": "symbol",
        "kod": "symbol",
        "kod gpw": "symbol",
        "nazwa": "name",
        "nazwa spółki": "name",
        "nazwa spolki": "name",
        "spółka": "name",
        "spolka": "name",
        "nazwa skrócona": "short_name",
        "nazwa skrocona": "short_name",
        "isin": "isin",
        "sektor": "sector",
        "branża": "industry",
        "branza": "industry",
        "segment": "segment",
        "rynek": "market",
        "indeks": "index",
        "kraj": "country",
    }
    return mapping.get(normalized)


def _iter_stooq_catalog_urls(base_url: str) -> List[str]:
    parsed = urlsplit(base_url)
    base_query = parse_qs(parsed.query, keep_blank_values=True)

    if "l" in base_query:
        return [base_url]

    normalized_query = {key: list(values) for key, values in base_query.items()}
    normalized_query.setdefault("v", ["0"])

    urls: List[str] = []
    for suffix in [None, "2", "3", "4", "5"]:
        query = {key: list(values) for key, values in normalized_query.items()}
        if suffix is None:
            query.pop("l", None)
        else:
            query["l"] = [suffix]
        query_string = urlencode(query, doseq=True)
        urls.append(
            urlunsplit(
                (
                    parsed.scheme,
                    parsed.netloc,
                    parsed.path,
                    query_string,
                    parsed.fragment,
                )
            )
        )
    return urls


def _extract_stooq_company_rows(document: str) -> List[Dict[str, Any]]:
    parser = _HtmlTableParser()
    parser.feed(document)
    parser.close()

    for table in parser.tables:
        if not table or len(table) < 2:
            continue
        header = table[0]
        normalized = [_normalize_stooq_header(cell) for cell in header]
        if normalized.count("symbol") != 1 or "name" not in normalized:
            continue
        index_map = {key: idx for idx, key in enumerate(normalized) if key}
        symbol_index = index_map.get("symbol")
        name_index = index_map.get("name")
        if symbol_index is None or name_index is None:
            continue

        results: Dict[str, Dict[str, Any]] = {}
        for raw_row in table[1:]:
            if len(raw_row) < len(header):
                raw_row = raw_row + [""] * (len(header) - len(raw_row))
            symbol = _clean_string(raw_row[symbol_index]) if symbol_index < len(raw_row) else None
            name = _clean_string(raw_row[name_index]) if name_index < len(raw_row) else None
            if not symbol or not name:
                continue

            short_name_value = None
            short_index = index_map.get("short_name")
            if short_index is not None and short_index < len(raw_row):
                short_name_value = _clean_string(raw_row[short_index])

            row: Dict[str, Any] = {
                "stockTicker": symbol.upper(),
                "companyName": name,
                "shortName": short_name_value or name,
            }

            def _assign(key: str, field: str) -> None:
                idx = index_map.get(key)
                if idx is None or idx >= len(raw_row):
                    return
                value = _clean_string(raw_row[idx])
                if value:
                    row[field] = value

            _assign("isin", "isin")
            _assign("sector", "sectorName")
            _assign("industry", "subsectorName")
            _assign("market", "market")
            _assign("segment", "segment")
            _assign("index", "index")
            _assign("country", "country")

            results[row["stockTicker"]] = row

        if results:
            return list(results.values())

    return []


def _normalize_stooq_profile_label(value: str) -> Optional[str]:
    cleaned = " ".join(value.strip().casefold().rstrip(":").split())
    if not cleaned:
        return None
    mapping = {
        "nazwa": "company_name",
        "nazwa spółki": "company_name",
        "nazwa spolki": "company_name",
        "spółka": "company_name",
        "spolka": "company_name",
        "symbol": "symbol",
        "ticker": "symbol",
        "skrót": "short_name",
        "skrot": "short_name",
        "isin": "isin",
        "sektor": "sector",
        "branża": "industry",
        "branza": "industry",
        "segment": "segment",
        "rynek": "market",
        "indeks": "index",
        "kraj": "country",
        "miasto": "city",
        "miejscowość": "city",
        "adres": "address",
        "siedziba": "address",
        "strona www": "website",
        "strona internetowa": "website",
        "www": "website",
        "profil": "profile",
        "opis": "profile",
        "opis działalności": "profile",
        "opis dzialalnosci": "profile",
        "zatrudnienie": "employees",
        "liczba pracowników": "employees",
        "liczba pracownikow": "employees",
        "pracownicy": "employees",
        "data debiutu": "ipo_date",
        "debiut": "ipo_date",
        "data pierwszych notowań": "ipo_date",
        "data pierwszych notowan": "ipo_date",
        "data pierwszego notowania": "ipo_date",
        "data założenia": "founded",
        "data zalozenia": "founded",
        "rok założenia": "founded",
        "rok zalozenia": "founded",
        "powstanie": "founded",
    }
    return mapping.get(cleaned)


def _parse_stooq_profile_document(document: str) -> Dict[str, Any]:
    parser = _HtmlTableParser()
    parser.feed(document)
    parser.close()

    raw_fields: Dict[str, str] = {}
    normalized_fields: Dict[str, Any] = {}

    for table in parser.tables:
        if not table:
            continue
        for row in table:
            if len(row) < 2:
                continue
            label = _clean_string(row[0])
            if not label:
                continue
            values = [
                part for part in (_clean_string(cell) for cell in row[1:]) if part is not None
            ]
            if not values:
                continue
            value = "\n".join(values)
            raw_fields[label] = value
            normalized = _normalize_stooq_profile_label(label)
            if not normalized:
                continue
            if normalized == "website":
                parsed_value = _clean_website(value)
            elif normalized == "employees":
                parsed_value = _clean_int(value)
            elif normalized == "ipo_date":
                parsed_value = _clean_date(value)
            elif normalized == "founded":
                parsed_value = _clean_int(value)
            else:
                parsed_value = _clean_string(value)
            if parsed_value is None:
                continue
            existing = normalized_fields.get(normalized)
            if existing is None or existing == "":
                normalized_fields[normalized] = parsed_value

    result: Dict[str, Any] = {"raw_fields": raw_fields}

    if "company_name" in normalized_fields:
        result["companyName"] = normalized_fields["company_name"]
    if "symbol" in normalized_fields:
        result["stockTicker"] = normalized_fields["symbol"]
    if "short_name" in normalized_fields:
        result["shortName"] = normalized_fields["short_name"]
    if "isin" in normalized_fields:
        result["isin"] = normalized_fields["isin"]
    if "sector" in normalized_fields:
        result["sectorName"] = normalized_fields["sector"]
    if "industry" in normalized_fields:
        result["subsectorName"] = normalized_fields["industry"]
    if "segment" in normalized_fields:
        result["segment"] = normalized_fields["segment"]
    if "market" in normalized_fields:
        result["market"] = normalized_fields["market"]
    if "index" in normalized_fields:
        result["index"] = normalized_fields["index"]
    if "country" in normalized_fields:
        result["country"] = normalized_fields["country"]
    if "city" in normalized_fields:
        result["city"] = normalized_fields["city"]
    if "address" in normalized_fields:
        result["address"] = normalized_fields["address"]
    if "website" in normalized_fields:
        result["website"] = normalized_fields["website"]
    if "profile" in normalized_fields:
        result["profile"] = normalized_fields["profile"]
    if "employees" in normalized_fields:
        result["employees"] = normalized_fields["employees"]
    if "ipo_date" in normalized_fields:
        date_value = normalized_fields["ipo_date"]
        if date_value:
            result["listing_date"] = date_value
            result["ipo_date"] = date_value
    if "founded" in normalized_fields:
        founded_value = normalized_fields["founded"]
        if founded_value:
            result["founded"] = founded_value
            result["founded_year"] = founded_value
            result["established"] = founded_value

    return result


_JSON_SCRIPT_RE = re.compile(
    r"<script[^>]+type=\"application/(?:json|ld\+json)\"[^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)


def _normalize_json_key(key: str) -> str:
    return "".join(ch for ch in key.lower() if ch.isalnum())


def _iter_key_values(payload: Any) -> Iterable[tuple[str, Any]]:
    stack = [payload]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            for key, value in current.items():
                yield key, value
                stack.append(value)
        elif isinstance(current, list):
            stack.extend(current)


def _search_value(payloads: Iterable[Any], aliases: Iterable[str]) -> Any:
    alias_set = {_normalize_json_key(alias) for alias in aliases}
    for payload in payloads:
        for key, value in _iter_key_values(payload):
            if _normalize_json_key(str(key)) not in alias_set:
                continue
            if isinstance(value, dict):
                for candidate_key in ("raw", "value", "amount", "text", "data"):
                    candidate = value.get(candidate_key)
                    if candidate is not None:
                        return candidate
            return value
    return None


_SCALED_NUMBER_RE = re.compile(
    r"^(-?\d+(?:[\.,]\d+)?)(k|m|b|t|tys|mln|mld)?([a-z]{1,4})?$",
    re.IGNORECASE,
)


def _parse_scaled_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, dict):
        for key in ("raw", "value", "amount"):
            if key in value and value[key] is not None:
                parsed = _parse_scaled_number(value[key])
                if parsed is not None:
                    return parsed
        return None
    if isinstance(value, str):
        cleaned = value.strip().replace("\xa0", " ")
        if not cleaned:
            return None
        percent = False
        if cleaned.endswith("%"):
            percent = True
            cleaned = cleaned[:-1]
        cleaned = cleaned.replace(" ", "")
        cleaned = cleaned.replace(",", ".")
        match = _SCALED_NUMBER_RE.match(cleaned)
        if not match:
            try:
                return float(cleaned)
            except ValueError:
                return None
        number = float(match.group(1))
        suffix = (match.group(2) or "").lower()
        multiplier = {
            "": 1.0,
            "k": 1e3,
            "tys": 1e3,
            "m": 1e6,
            "mln": 1e6,
            "b": 1e9,
            "mld": 1e9,
            "t": 1e12,
        }.get(suffix, 1.0)
        result = number * multiplier
        if percent:
            result /= 100
        return result
    return None


def _extract_google_json_payloads(document: str) -> List[Any]:
    payloads: List[Any] = []
    for match in _JSON_SCRIPT_RE.finditer(document):
        raw = html_unescape(match.group(1)).strip()
        if raw.startswith("<!--") and raw.endswith("-->"):
            raw = raw[4:-3].strip()
        if raw.startswith(")]}'"):
            raw = raw[4:]
        if not raw:
            continue
        for candidate in (raw, raw.replace("\u2028", ""), raw.replace("\u2029", "")):
            try:
                payloads.append(json.loads(candidate))
                break
            except json.JSONDecodeError:
                continue
    return payloads


def _flatten_payloads(payloads: Iterable[Any]) -> List[Any]:
    flattened: List[Any] = []
    for payload in payloads:
        flattened.append(payload)
        if isinstance(payload, list):
            flattened.extend(_flatten_payloads(payload))
        elif isinstance(payload, dict):
            flattened.extend(_flatten_payloads(payload.values()))
    return flattened


def _parse_google_finance_document(document: str) -> Dict[str, Any]:
    payloads = _extract_google_json_payloads(document)
    if not payloads:
        raise RuntimeError(
            "Nie udało się odnaleźć danych JSON w odpowiedzi Google Finance"
        )

    flattened = _flatten_payloads(payloads)
    metrics: Dict[str, Any] = {}

    def _set_metric(name: str, aliases: Iterable[str], *, parser: Callable[[Any], Any]) -> None:
        value = _search_value(flattened, aliases)
        if value is None:
            return
        parsed = parser(value)
        if parsed is None:
            return
        metrics[name] = parsed

    _set_metric("last_price", ["lastPrice", "regularMarketLastPrice", "price"], parser=_parse_scaled_number)
    _set_metric("price_change", ["priceChange", "change", "regularMarketChange"], parser=_parse_scaled_number)
    _set_metric(
        "price_change_percent",
        ["priceChangePercent", "changePercent", "regularMarketChangePercent"],
        parser=_parse_scaled_number,
    )
    _set_metric("previous_close", ["previousClose", "prevClose"], parser=_parse_scaled_number)
    _set_metric("open", ["open", "openingPrice"], parser=_parse_scaled_number)
    _set_metric("day_low", ["dayLow", "low", "lowPrice"], parser=_parse_scaled_number)
    _set_metric("day_high", ["dayHigh", "high", "highPrice"], parser=_parse_scaled_number)
    _set_metric(
        "year_low",
        ["fiftyTwoWeekLow", "52WeekLow", "yearLow"],
        parser=_parse_scaled_number,
    )
    _set_metric(
        "year_high",
        ["fiftyTwoWeekHigh", "52WeekHigh", "yearHigh"],
        parser=_parse_scaled_number,
    )
    _set_metric("market_cap", ["marketCap", "marketCapitalization"], parser=_parse_scaled_number)
    _set_metric("pe_ratio", ["peRatio", "pe"], parser=_parse_scaled_number)
    _set_metric("dividend_yield", ["dividendYield"], parser=_parse_scaled_number)
    _set_metric("eps", ["eps", "earningsPerShare"], parser=_parse_scaled_number)
    _set_metric("volume", ["volume", "regularMarketVolume"], parser=_parse_scaled_number)
    _set_metric("average_volume", ["averageVolume", "avgVolume"], parser=_parse_scaled_number)
    _set_metric("beta", ["beta"], parser=_parse_scaled_number)

    def _string_parser(value: Any) -> Optional[str]:
        return _clean_string(value)

    _set_metric("currency", ["currency", "currencyCode", "priceCurrency"], parser=_string_parser)
    _set_metric("exchange", ["exchange", "exchangeCode"], parser=_string_parser)
    _set_metric("ceo", ["ceo"], parser=_string_parser)
    _set_metric("headquarters", ["headquarters", "headquarterLocation"], parser=_string_parser)

    def _int_parser(value: Any) -> Optional[int]:
        parsed = _parse_scaled_number(value)
        if parsed is None:
            return None
        return int(parsed)

    _set_metric("employees", ["employees", "numberOfEmployees", "fullTimeEmployees"], parser=_int_parser)
    _set_metric("founded_year", ["foundedYear", "founded", "foundedDate"], parser=_int_parser)

    structured_data: List[Any] = []
    for payload in payloads:
        if isinstance(payload, dict) and payload.get("@context"):
            structured_data.append(payload)
        elif isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict) and item.get("@context"):
                    structured_data.append(item)

    return {
        "metrics": metrics,
        "structured_data": structured_data,
        "json_payloads": payloads,
    }


class CompanyDataHarvester:
    """Pobiera dane o spółkach z darmowych źródeł i zapisuje do ClickHouse."""

    def __init__(
        self,
        session: Optional[Any] = None,
        gpw_url: str = GPW_COMPANY_PROFILES_URL,
        *,
        gpw_fallback_url: Optional[str] = GPW_COMPANY_PROFILES_FALLBACK_URL,
        gpw_stooq_url: Optional[str] = STOOQ_COMPANY_CATALOG_URL,
        stooq_profile_url_template: Optional[str] = STOOQ_COMPANY_PROFILE_URL,
        yahoo_url_template: Optional[str] = None,
        google_url_template: Optional[str] = None,
    ) -> None:
        self.session = session or SimpleHttpSession()
        self.gpw_url = gpw_url
        self.gpw_fallback_url = gpw_fallback_url
        self.gpw_stooq_url = gpw_stooq_url
        self.stooq_profile_url_template = stooq_profile_url_template
        self.yahoo_url_template = yahoo_url_template
        self.google_url_template = google_url_template
        self._yahoo_crumb: Optional[str] = None
        parsed_yahoo_url = urlparse(self.yahoo_url_template) if self.yahoo_url_template else None
        self._yahoo_crumb_url: Optional[str]
        if parsed_yahoo_url and parsed_yahoo_url.scheme and parsed_yahoo_url.netloc:
            self._yahoo_crumb_url = (
                f"{parsed_yahoo_url.scheme}://{parsed_yahoo_url.netloc}/v1/test/getcrumb"
            )
        else:
            self._yahoo_crumb_url = None

    # ---------------------------
    # HTTP helpers
    # ---------------------------

    def _get(self, url: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            response = self.session.get(url, params=params, timeout=15)
        except URLError as exc:  # pragma: no cover - zależy od środowiska uruch.
            raise RuntimeError(f"Błąd połączenia z {url}: {exc}") from exc
        except Exception as exc:  # pragma: no cover - obrona
            raise RuntimeError(f"Nie udało się pobrać {url}: {exc}") from exc
        response.raise_for_status()
        return response.json()

    def _get_text(self, url: str, *, params: Optional[Dict[str, Any]] = None) -> str:
        try:
            response = self.session.get(url, params=params, timeout=15)
        except URLError as exc:  # pragma: no cover - zależy od środowiska uruch.
            raise RuntimeError(f"Błąd połączenia z {url}: {exc}") from exc
        except Exception as exc:  # pragma: no cover - obrona
            raise RuntimeError(f"Nie udało się pobrać {url}: {exc}") from exc
        response.raise_for_status()
        text_getter = getattr(response, "text", None)
        if callable(text_getter):
            return text_getter()  # type: ignore[call-arg]
        content = getattr(response, "content", None)
        if isinstance(content, bytes):
            return content.decode("utf-8", errors="replace")
        raise RuntimeError(f"Brak treści w odpowiedzi z {url}")

    # ---------------------------
    # Fetchers
    # ---------------------------

    def fetch_gpw_profiles(
        self,
        *,
        limit: Optional[int] = None,
        page_size: int = 200,
    ) -> List[Dict[str, Any]]:
        try:
            return self._fetch_gpw_profiles_legacy(limit=limit, page_size=page_size)
        except RuntimeError as exc:
            last_error: Exception = exc
            fallback_tried = False
            if self.gpw_fallback_url and self._should_try_gpw_fallback(exc):
                fallback_tried = True
                try:
                    return self._fetch_gpw_profiles_fallback(limit=limit, page_size=page_size)
                except RuntimeError as fallback_exc:
                    last_error = fallback_exc
            if self.gpw_stooq_url:
                try:
                    return self._fetch_gpw_profiles_stooq(limit=limit)
                except RuntimeError as stooq_exc:
                    last_error = stooq_exc
            if not fallback_tried and self.gpw_fallback_url and self._should_try_gpw_fallback(last_error):
                return self._fetch_gpw_profiles_fallback(limit=limit, page_size=page_size)
            raise last_error

    def _fetch_gpw_profiles_legacy(
        self,
        *,
        limit: Optional[int],
        page_size: int,
    ) -> List[Dict[str, Any]]:
        start = 0
        collected: List[Dict[str, Any]] = []
        while True:
            params = {
                "action": "GPWCompanyProfiles",
                "start": start,
                "limit": page_size,
            }
            payload = self._get(self.gpw_url, params=params)
            rows = _extract_company_rows(payload)
            collected.extend(rows)
            if limit is not None and len(collected) >= limit:
                return collected[:limit]
            if not rows or len(rows) < page_size:
                break
            start += len(rows)
        return collected

    def _fetch_gpw_profiles_fallback(
        self,
        *,
        limit: Optional[int],
        page_size: int,
    ) -> List[Dict[str, Any]]:
        if not self.gpw_fallback_url:
            raise RuntimeError("Brak alternatywnego adresu GPW do pobrania danych")

        page = 0
        collected: List[Dict[str, Any]] = []
        while True:
            params = {"page": page, "size": page_size}
            payload = self._get(self.gpw_fallback_url, params=params)
            rows = _extract_company_rows(payload)
            if not rows:
                break
            collected.extend(rows)
            if limit is not None and len(collected) >= limit:
                return collected[:limit]
            if len(rows) < page_size:
                break
            page += 1
        return collected

    def _fetch_gpw_profiles_stooq(
        self,
        *,
        limit: Optional[int],
    ) -> List[Dict[str, Any]]:
        if not self.gpw_stooq_url:
            raise RuntimeError("Brak adresu katalogu spółek na Stooq")

        collected: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for url in _iter_stooq_catalog_urls(self.gpw_stooq_url):
            document = self._get_text(url)
            rows = _extract_stooq_company_rows(document)
            for row in rows:
                ticker = row.get("stockTicker")
                if not ticker or ticker in seen:
                    continue
                seen.add(ticker)
                collected.append(row)
                if limit is not None and len(collected) >= limit:
                    return collected[:limit]

        if not collected:
            raise RuntimeError("Nie udało się odczytać danych spółek ze Stooq")

        return collected

    def _should_try_gpw_fallback(self, exc: Exception) -> bool:
        message = str(exc)
        indicators = (
            "HandlerMappingException",
            "Brak dopasowania akcji",
        )
        return any(indicator in message for indicator in indicators)

    def fetch_yahoo_summary(self, raw_symbol: str) -> Dict[str, Any]:
        if not self.yahoo_url_template:
            raise RuntimeError("Pobieranie danych z Yahoo Finance jest wyłączone")
        normalized = _normalize_gpw_symbol(raw_symbol)
        symbol = f"{normalized}.WA"
        payload = self._fetch_yahoo_payload(symbol)
        result = (((payload or {}).get("quoteSummary") or {}).get("result") or [])
        if not result:
            raise RuntimeError(f"Brak danych fundamentalnych dla {symbol}")
        return result[0]

    def _fetch_yahoo_payload(self, symbol: str) -> Dict[str, Any]:
        if not self.yahoo_url_template:
            raise RuntimeError("Pobieranie danych z Yahoo Finance jest wyłączone")
        url = self.yahoo_url_template.format(symbol=symbol)
        base_params: Dict[str, Any] = {"modules": YAHOO_MODULES}
        params = dict(base_params)
        if self._yahoo_crumb:
            params["crumb"] = self._yahoo_crumb
        try:
            return self._get(url, params=params)
        except RuntimeError as exc:
            if "HTTP 401" not in str(exc):
                raise
            self._yahoo_crumb = None
            if not self._refresh_yahoo_crumb():
                raise RuntimeError(
                    f"Brak autoryzacji Yahoo dla {symbol}: nie udało się pobrać tokenu dostępu"
                ) from exc
        retry_params = dict(base_params)
        retry_params["crumb"] = self._yahoo_crumb
        try:
            return self._get(url, params=retry_params)
        except RuntimeError as exc:
            raise RuntimeError(f"Brak autoryzacji Yahoo dla {symbol}: {exc}") from exc

    def _refresh_yahoo_crumb(self) -> bool:
        if not self._yahoo_crumb_url:
            return False
        try:
            crumb = self._get_text(self._yahoo_crumb_url).strip()
        except Exception:
            self._yahoo_crumb = None
            return False
        if not crumb:
            self._yahoo_crumb = None
            return False
        self._yahoo_crumb = crumb
        return True

    def fetch_google_overview(self, raw_symbol: str) -> Dict[str, Any]:
        if not self.google_url_template:
            raise RuntimeError("Pobieranie danych z Google Finance jest wyłączone")
        normalized = _normalize_gpw_symbol(raw_symbol)
        symbol = f"{normalized}:WSE"
        url = self.google_url_template.format(symbol=symbol)
        document = self._get_text(url, params={"hl": "pl"})
        parsed = _parse_google_finance_document(document)
        parsed.update(
            {
                "url": url,
                "symbol": symbol,
                "retrieved_at": datetime.utcnow().isoformat(),
            }
        )
        return parsed

    def fetch_stooq_profile(self, raw_symbol: str) -> Dict[str, Any]:
        if not self.stooq_profile_url_template:
            raise RuntimeError("Pobieranie profili ze Stooq jest wyłączone")
        normalized = _normalize_gpw_symbol(raw_symbol)
        symbol_param = normalized.lower()
        url = self.stooq_profile_url_template.format(symbol=symbol_param)
        document = self._get_text(url)
        parsed = _parse_stooq_profile_document(document)
        raw_fields = parsed.get("raw_fields") if isinstance(parsed, dict) else None
        if not raw_fields:
            raise RuntimeError(f"Brak danych profilu Stooq dla {normalized}")
        parsed.setdefault("url", url)
        parsed.setdefault("symbol", normalized)
        parsed.setdefault("stockTicker", normalized)
        parsed.setdefault("retrieved_at", datetime.utcnow().isoformat())
        return parsed

    # ---------------------------
    # Normalizacja
    # ---------------------------

    def _extract_symbol(self, row: Dict[str, Any]) -> str:
        for key in ("stockTicker", "ticker", "symbol", "code"):
            value = _clean_string(row.get(key))
            if value:
                return _normalize_gpw_symbol(value)
        raise RuntimeError("Rekord GPW nie zawiera symbolu spółki")

    def build_row(
        self,
        base: Dict[str, Any],
        fundamentals: Optional[Dict[str, Any]],
        google: Optional[Dict[str, Any]] = None,
        stooq: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        raw_symbol = self._extract_symbol(base)
        asset_profile = fundamentals.get("assetProfile") if fundamentals else None
        price_info = fundamentals.get("price") if fundamentals else None
        summary_detail = fundamentals.get("summaryDetail") if fundamentals else None
        default_stats = fundamentals.get("defaultKeyStatistics") if fundamentals else None
        financial_data = fundamentals.get("financialData") if fundamentals else None
        stooq_data = stooq or {}

        company_name = (
            _clean_string(base.get("companyName"))
            or _clean_string(price_info.get("longName") if price_info else None)
            or _clean_string(stooq_data.get("companyName"))
            or _clean_string(base.get("shortName"))
        )
        short_name = (
            _clean_string(base.get("shortName"))
            or _clean_string(price_info.get("shortName") if price_info else None)
            or _clean_string(stooq_data.get("shortName"))
            or company_name
        )

        website = _clean_website(
            _clean_string(
                (asset_profile or {}).get("website")
                or base.get("website")
                or base.get("www")
                or base.get("url")
                or stooq_data.get("website")
            )
        )

        description = (
            _clean_string((asset_profile or {}).get("longBusinessSummary"))
            or _clean_string(base.get("profile"))
            or _clean_string(base.get("description"))
            or _clean_string(stooq_data.get("profile"))
        )

        industry = (
            _clean_string((asset_profile or {}).get("industry"))
            or _clean_string(base.get("subsectorName"))
            or _clean_string(base.get("industry"))
            or _clean_string(stooq_data.get("subsectorName"))
            or _clean_string(stooq_data.get("industry"))
        )
        sector = (
            _clean_string((asset_profile or {}).get("sector"))
            or _clean_string(base.get("sectorName"))
            or _clean_string(base.get("sector"))
            or _clean_string(stooq_data.get("sectorName"))
            or _clean_string(stooq_data.get("sector"))
        )
        country = (
            _clean_string((asset_profile or {}).get("country"))
            or _clean_string(base.get("country"))
            or _clean_string(base.get("countryName"))
            or _clean_string(stooq_data.get("country"))
        )
        city = _clean_string(
            (asset_profile or {}).get("city")
            or base.get("city")
            or stooq_data.get("city")
        )
        state = _clean_string((asset_profile or {}).get("state") or base.get("state"))

        if city and country:
            headquarters = f"{city}, {country}" if not state else f"{city}, {state}, {country}"
        elif country:
            headquarters = country
        else:
            headquarters = _clean_string(stooq_data.get("address")) or city

        listing_date = _clean_date(
            base.get("firstQuotation")
            or base.get("firstQuotationDate")
            or base.get("firstListingDate")
            or base.get("ipoDate")
            or stooq_data.get("listing_date")
            or stooq_data.get("ipo_date")
        )

        founded_year = _clean_int(
            base.get("founded")
            or base.get("foundedYear")
            or base.get("established")
            or base.get("startYear")
            or stooq_data.get("founded")
            or stooq_data.get("founded_year")
            or stooq_data.get("established")
        )

        logo_url = _logo_url_from_website(website)

        row: Dict[str, Any] = {
            "symbol": raw_symbol,
            "ticker": raw_symbol,
            "code": raw_symbol,
            "isin": _clean_string(base.get("isin"))
            or _clean_string(stooq_data.get("isin")),
            "name": company_name,
            "company_name": company_name,
            "full_name": company_name,
            "short_name": short_name,
            "sector": sector,
            "industry": industry,
            "country": country,
            "headquarters": headquarters,
            "city": city,
            "website": website,
            "url": website,
            "description": description,
            "profile": description,
            "logo": logo_url,
            "logo_url": logo_url,
            "image_url": logo_url,
            "employees": _clean_int((asset_profile or {}).get("fullTimeEmployees")),
            "employee_count": _clean_int((asset_profile or {}).get("fullTimeEmployees")),
            "founded": founded_year,
            "founded_year": founded_year,
            "established": founded_year,
            "listing_date": listing_date,
            "ipo_date": listing_date,
        }

        shares_outstanding = _clean_float(
            _value_from_path(default_stats or {}, "sharesOutstanding")
            or _value_from_path(price_info or {}, "sharesOutstanding")
            or _value_from_path(summary_detail or {}, "sharesOutstanding")
        )

        share_price = _clean_float(
            _value_from_path(price_info or {}, "regularMarketPrice")
            or _value_from_path(financial_data or {}, "currentPrice")
            or _value_from_path(price_info or {}, "regularMarketPreviousClose")
            or _value_from_path(summary_detail or {}, "regularMarketPreviousClose")
            or _value_from_path(summary_detail or {}, "previousClose")
        )

        market_cap = _clean_float(
            _value_from_path(default_stats or {}, "marketCap")
            or _value_from_path(price_info or {}, "marketCap")
            or _value_from_path(summary_detail or {}, "marketCap")
        )

        if (
            share_price is None
            and market_cap is not None
            and shares_outstanding not in (None, 0)
        ):
            share_price = market_cap / shares_outstanding

        if (
            share_price not in (None, 0)
            and shares_outstanding not in (None, 0)
        ):
            market_cap = share_price * shares_outstanding

        book_value_per_share = _clean_float(
            _value_from_path(default_stats or {}, "bookValue")
            or _value_from_path(summary_detail or {}, "bookValue")
            or _value_from_path(financial_data or {}, "bookValue")
        )

        total_equity = _clean_float(
            _value_from_path(financial_data or {}, "totalStockholderEquity")
            or _value_from_path(financial_data or {}, "totalShareholderEquity")
            or _value_from_path(financial_data or {}, "totalEquity")
        )

        book_value_total = total_equity
        if (
            book_value_total is None
            and book_value_per_share is not None
            and shares_outstanding is not None
        ):
            book_value_total = book_value_per_share * shares_outstanding

        eps = _clean_float(_value_from_path(default_stats or {}, "trailingEps"))

        row.update(
            {
                "market_cap": market_cap,
                "shares_outstanding": shares_outstanding,
                "book_value": book_value_total,
                "revenue_ttm": _clean_float(_value_from_path(financial_data or {}, "totalRevenue")),
                "net_income_ttm": _clean_float(
                    _value_from_path(financial_data or {}, "netIncomeToCommon")
                ),
                "ebitda_ttm": _clean_float(_value_from_path(financial_data or {}, "ebitda")),
                "eps": eps,
                "pe_ratio": _clean_float(_value_from_path(summary_detail or {}, "trailingPE")),
                "pb_ratio": _clean_float(_value_from_path(summary_detail or {}, "priceToBook")),
                "dividend_yield": _clean_float(
                    _value_from_path(summary_detail or {}, "dividendYield")
                ),
                "debt_to_equity": _clean_float(
                    _value_from_path(financial_data or {}, "debtToEquity")
                ),
                "roe": _clean_float(_value_from_path(financial_data or {}, "returnOnEquity")),
                "roa": _clean_float(_value_from_path(financial_data or {}, "returnOnAssets")),
                "gross_margin": _clean_float(_value_from_path(financial_data or {}, "grossMargins")),
                "operating_margin": _clean_float(
                    _value_from_path(financial_data or {}, "operatingMargins")
                ),
                "profit_margin": _clean_float(
                    _value_from_path(financial_data or {}, "profitMargins")
                ),
            }
        )

        net_income_ttm = row.get("net_income_ttm")
        pb_ratio_computed: Optional[float] = None
        if (
            market_cap not in (None, 0)
            and book_value_total not in (None, 0)
        ):
            pb_ratio_computed = market_cap / book_value_total
        elif (
            share_price not in (None, 0)
            and book_value_per_share not in (None, 0)
        ):
            pb_ratio_computed = share_price / book_value_per_share

        if pb_ratio_computed is not None:
            row["pb_ratio"] = pb_ratio_computed

        pe_ratio_computed: Optional[float] = None
        if (
            share_price not in (None, 0)
            and eps not in (None, 0)
        ):
            pe_ratio_computed = share_price / eps
        elif (
            market_cap not in (None, 0)
            and net_income_ttm not in (None, 0)
        ):
            pe_ratio_computed = market_cap / net_income_ttm

        if pe_ratio_computed is not None:
            row["pe_ratio"] = pe_ratio_computed

        google_metrics = ((google or {}).get("metrics") or {}) if google else {}

        def _metric_value(name: str) -> Any:
            if not google_metrics:
                return None
            return google_metrics.get(name)

        def _assign_from_google(field: str, metric_name: str) -> None:
            if row.get(field) is not None:
                return
            value = _metric_value(metric_name)
            if value is not None:
                row[field] = value

        _assign_from_google("market_cap", "market_cap")
        _assign_from_google("pe_ratio", "pe_ratio")
        _assign_from_google("dividend_yield", "dividend_yield")
        _assign_from_google("eps", "eps")
        _assign_from_google("employees", "employees")
        _assign_from_google("employee_count", "employees")
        _assign_from_google("headquarters", "headquarters")

        founded_metric = _metric_value("founded_year")
        if founded_metric is not None:
            founded_int = _clean_int(founded_metric)
            if founded_int:
                if row.get("founded") is None:
                    row["founded"] = founded_int
                if row.get("founded_year") is None:
                    row["founded_year"] = founded_int
                if row.get("established") is None:
                    row["established"] = founded_int

        stooq_employees = _clean_int(stooq_data.get("employees")) if stooq_data else None
        if stooq_employees is not None:
            if row.get("employees") is None:
                row["employees"] = stooq_employees
            if row.get("employee_count") is None:
                row["employee_count"] = stooq_employees

        payload = {"gpw": base, "yahoo": fundamentals, "google": google, "stooq": stooq}
        row["raw_payload"] = json.dumps(payload, ensure_ascii=False)
        return row

    # ---------------------------
    # Baza danych
    # ---------------------------

    def _load_existing_rows(
        self,
        ch_client: Any,
        table_name: str,
        symbols: Sequence[str],
    ) -> Dict[str, Dict[str, Any]]:
        if not symbols or not hasattr(ch_client, "query"):
            return {}

        placeholders = ", ".join(_quote_sql_literal(symbol) for symbol in symbols if symbol)
        if not placeholders:
            return {}

        query = f"SELECT * FROM {table_name} WHERE symbol IN ({placeholders})"
        result = ch_client.query(query)
        columns = getattr(result, "column_names", None)
        rows = getattr(result, "result_rows", None)
        if not columns or rows is None:
            return {}

        existing: Dict[str, Dict[str, Any]] = {}
        for values in rows:
            record = dict(zip(columns, values))
            symbol = record.get("symbol")
            if not symbol:
                continue
            current = existing.get(symbol)
            if current is None:
                existing[symbol] = record
            else:
                _merge_missing_fields(current, record)
        return existing

    def _delete_existing_symbols(
        self,
        ch_client: Any,
        table_name: str,
        symbols: Sequence[str],
    ) -> None:
        if not symbols or not hasattr(ch_client, "command"):
            return

        placeholders = ", ".join(_quote_sql_literal(symbol) for symbol in symbols if symbol)
        if not placeholders:
            return

        ch_client.command(
            f"ALTER TABLE {table_name} DELETE WHERE symbol IN ({placeholders})"
        )

    # ---------------------------
    # Synchronizacja
    # ---------------------------

    def sync(
        self,
        *,
        ch_client: Any,
        table_name: str,
        columns: Sequence[str],
        limit: Optional[int] = None,
        progress_callback: Optional[Callable[[CompanySyncProgress], None]] = None,
        run_as_admin: bool = False,
    ) -> CompanySyncResult:
        supports_history = hasattr(self.session, "clear_history") and hasattr(
            self.session, "get_history"
        )
        if supports_history:
            self.session.clear_history()
        started_at = datetime.utcnow()
        total_count: Optional[int] = None
        processed_count = 0
        deduplicated_count = 0
        failed_count = 0

        def emit(stage: Literal["fetching", "harvesting", "inserting", "finished", "failed"], *, message: Optional[str] = None, current_symbol: Optional[str] = None) -> None:
            if not progress_callback:
                return
            progress_callback(
                CompanySyncProgress(
                    stage=stage,
                    total=total_count,
                    processed=processed_count,
                    synced=deduplicated_count,
                    failed=failed_count,
                    current_symbol=current_symbol,
                    message=message,
                )
            )

        emit("fetching", message="Pobieranie listy spółek GPW")
        base_rows = self.fetch_gpw_profiles(limit=limit)
        total_count = len(base_rows)
        emit(
            "harvesting",
            message=f"Znaleziono {total_count} rekordów do pobrania" if total_count else "Brak spółek do pobrania",
        )
        deduplicated: Dict[str, Dict[str, Any]] = {}
        errors: List[str] = []

        for base in base_rows:
            processed_count += 1
            try:
                symbol = self._extract_symbol(base)
            except Exception as exc:  # pragma: no cover - safeguard
                errors.append(str(exc))
                failed_count = len(errors)
                emit(
                    "harvesting",
                    message=str(exc),
                )
                continue

            if symbol in deduplicated:
                emit(
                    "harvesting",
                    message=f"Pomijanie duplikatu {symbol}",
                    current_symbol=symbol,
                )
                continue

            fundamentals: Optional[Dict[str, Any]] = None
            google_data: Optional[Dict[str, Any]] = None
            stooq_data: Optional[Dict[str, Any]] = None
            if self.yahoo_url_template:
                try:
                    fundamentals = self.fetch_yahoo_summary(symbol)
                except Exception as exc:  # pragma: no cover - network/API specific
                    errors.append(f"{symbol} [Yahoo]: {exc}")
                    failed_count = len(errors)
            if self.google_url_template:
                try:
                    google_data = self.fetch_google_overview(symbol)
                except Exception as exc:  # pragma: no cover - network/API specific
                    errors.append(f"{symbol} [Google]: {exc}")
                    failed_count = len(errors)
            if self.stooq_profile_url_template:
                try:
                    stooq_data = self.fetch_stooq_profile(symbol)
                except Exception as exc:  # pragma: no cover - network/API specific
                    errors.append(f"{symbol} [Stooq]: {exc}")
                    failed_count = len(errors)
            row = self.build_row(base, fundamentals, google_data, stooq_data)
            deduplicated[symbol] = row
            deduplicated_count = len(deduplicated)
            emit(
                "harvesting",
                message=f"Przetworzono {deduplicated_count} spółek",
                current_symbol=symbol,
            )

        normalized_rows = list(deduplicated.values())
        final_rows: List[Dict[str, Any]] = []
        synced = 0

        existing_rows: Dict[str, Dict[str, Any]] = {}
        if normalized_rows:
            symbols_for_lookup = sorted(deduplicated.keys())
            try:
                existing_rows = self._load_existing_rows(
                    ch_client, table_name, symbols_for_lookup
                )
            except Exception as exc:  # pragma: no cover - zależy od konfiguracji DB
                errors.append(f"Nie udało się odczytać istniejących spółek: {exc}")
                failed_count = len(errors)
            else:
                if existing_rows:
                    try:
                        self._delete_existing_symbols(
                            ch_client, table_name, sorted(existing_rows.keys())
                        )
                    except Exception as exc:  # pragma: no cover - zależy od konfiguracji DB
                        errors.append(f"Nie udało się usunąć duplikatów: {exc}")
                        failed_count = len(errors)

        for row in normalized_rows:
            symbol = row.get("symbol")
            if symbol and symbol in existing_rows:
                merged = _merge_company_rows(existing_rows[symbol], row)
            else:
                merged = row
            final_rows.append(merged)

        usable_columns = [
            column for column in columns if any(row.get(column) is not None for row in final_rows)
        ]
        emit(
            "inserting",
            message="Zapisywanie danych w bazie",
        )
        if final_rows and usable_columns:
            data = [[row.get(column) for column in usable_columns] for row in final_rows]
            ch_client.insert(
                table=table_name,
                data=data,
                column_names=list(usable_columns),
            )
            synced = len(final_rows)
            deduplicated_count = synced

        finished_at = datetime.utcnow()
        request_log: List[HttpRequestLog] = []
        if supports_history:
            request_log = self.session.get_history()

        failed_count = len(errors)
        processed_count = max(processed_count, total_count or 0)
        emit(
            "finished",
            message="Synchronizacja zakończona",
        )

        return CompanySyncResult(
            fetched=len(base_rows),
            synced=synced,
            failed=len(errors),
            errors=errors,
            started_at=started_at,
            finished_at=finished_at,
            request_log=request_log,
            requested_as_admin=run_as_admin,
        )


