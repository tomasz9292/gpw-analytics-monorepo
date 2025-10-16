from __future__ import annotations

import json
from xml.etree import ElementTree
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Sequence
from typing import Literal
from urllib.error import URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
from pydantic import BaseModel, Field

GPW_COMPANY_PROFILES_URL = "https://www.gpw.pl/ajaxindex.php"
YAHOO_QUOTE_SUMMARY_URL = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
YAHOO_MODULES = (
    "price,assetProfile,summaryDetail,defaultKeyStatistics,financialData"
)


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

    def __init__(self, headers: Optional[Dict[str, str]] = None) -> None:
        self.headers = dict(self.DEFAULT_HEADERS)
        if headers:
            self.headers.update(headers)
        self.history: List[HttpRequestLog] = []

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
            with urlopen(request, timeout=timeout) as response:  # type: ignore[arg-type]
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


class CompanyDataHarvester:
    """Pobiera dane o spółkach z darmowych źródeł i zapisuje do ClickHouse."""

    def __init__(
        self,
        session: Optional[Any] = None,
        gpw_url: str = GPW_COMPANY_PROFILES_URL,
        yahoo_url_template: str = YAHOO_QUOTE_SUMMARY_URL,
    ) -> None:
        self.session = session or SimpleHttpSession()
        self.gpw_url = gpw_url
        self.yahoo_url_template = yahoo_url_template

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

    # ---------------------------
    # Fetchers
    # ---------------------------

    def fetch_gpw_profiles(
        self,
        *,
        limit: Optional[int] = None,
        page_size: int = 200,
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
            rows = payload.get("data") or []
            collected.extend(rows)
            if limit is not None and len(collected) >= limit:
                return collected[:limit]
            if not rows or len(rows) < page_size:
                break
            start += len(rows)
        return collected

    def fetch_yahoo_summary(self, raw_symbol: str) -> Dict[str, Any]:
        symbol = raw_symbol if "." in raw_symbol else f"{raw_symbol}.WA"
        url = self.yahoo_url_template.format(symbol=symbol)
        params = {"modules": YAHOO_MODULES}
        payload = self._get(url, params=params)
        result = (((payload or {}).get("quoteSummary") or {}).get("result") or [])
        if not result:
            raise RuntimeError(f"Brak danych fundamentalnych dla {symbol}")
        return result[0]

    # ---------------------------
    # Normalizacja
    # ---------------------------

    def _extract_symbol(self, row: Dict[str, Any]) -> str:
        for key in ("stockTicker", "ticker", "symbol", "code"):
            value = _clean_string(row.get(key))
            if value:
                return value.upper()
        raise RuntimeError("Rekord GPW nie zawiera symbolu spółki")

    def build_row(
        self,
        base: Dict[str, Any],
        fundamentals: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        raw_symbol = self._extract_symbol(base)
        asset_profile = fundamentals.get("assetProfile") if fundamentals else None
        price_info = fundamentals.get("price") if fundamentals else None
        summary_detail = fundamentals.get("summaryDetail") if fundamentals else None
        default_stats = fundamentals.get("defaultKeyStatistics") if fundamentals else None
        financial_data = fundamentals.get("financialData") if fundamentals else None

        company_name = (
            _clean_string(base.get("companyName"))
            or _clean_string(price_info.get("longName") if price_info else None)
            or _clean_string(base.get("shortName"))
        )
        short_name = (
            _clean_string(base.get("shortName"))
            or _clean_string(price_info.get("shortName") if price_info else None)
            or company_name
        )

        website = _clean_website(
            _clean_string(
                (asset_profile or {}).get("website")
                or base.get("website")
                or base.get("www")
                or base.get("url")
            )
        )

        description = (
            _clean_string((asset_profile or {}).get("longBusinessSummary"))
            or _clean_string(base.get("profile"))
            or _clean_string(base.get("description"))
        )

        industry = (
            _clean_string((asset_profile or {}).get("industry"))
            or _clean_string(base.get("subsectorName"))
            or _clean_string(base.get("industry"))
        )
        sector = (
            _clean_string((asset_profile or {}).get("sector"))
            or _clean_string(base.get("sectorName"))
            or _clean_string(base.get("sector"))
        )
        country = (
            _clean_string((asset_profile or {}).get("country"))
            or _clean_string(base.get("country"))
            or _clean_string(base.get("countryName"))
        )
        city = _clean_string((asset_profile or {}).get("city") or base.get("city"))
        state = _clean_string((asset_profile or {}).get("state") or base.get("state"))

        if city and country:
            headquarters = f"{city}, {country}" if not state else f"{city}, {state}, {country}"
        elif country:
            headquarters = country
        else:
            headquarters = city

        listing_date = _clean_date(
            base.get("firstQuotation")
            or base.get("firstQuotationDate")
            or base.get("firstListingDate")
            or base.get("ipoDate")
        )

        founded_year = _clean_int(
            base.get("founded")
            or base.get("foundedYear")
            or base.get("established")
            or base.get("startYear")
        )

        logo_url = _logo_url_from_website(website)

        row: Dict[str, Any] = {
            "symbol": raw_symbol,
            "ticker": raw_symbol,
            "code": raw_symbol,
            "isin": _clean_string(base.get("isin")),
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

        market_cap = _clean_float(
            _value_from_path(default_stats or {}, "marketCap")
            or _value_from_path(price_info or {}, "marketCap")
        )
        eps = _clean_float(_value_from_path(default_stats or {}, "trailingEps"))

        row.update(
            {
                "market_cap": market_cap,
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

        payload = {"gpw": base, "yahoo": fundamentals}
        row["raw_payload"] = json.dumps(payload, ensure_ascii=False)
        return row

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

            fundamentals: Dict[str, Any] = {}
            try:
                fundamentals = self.fetch_yahoo_summary(symbol)
            except Exception as exc:  # pragma: no cover - network/API specific
                errors.append(f"{symbol}: {exc}")
                failed_count = len(errors)
            row = self.build_row(base, fundamentals)
            deduplicated[symbol] = row
            deduplicated_count = len(deduplicated)
            emit(
                "harvesting",
                message=f"Przetworzono {deduplicated_count} spółek",
                current_symbol=symbol,
            )

        normalized_rows = list(deduplicated.values())
        synced = 0
        usable_columns = [
            column for column in columns if any(row.get(column) is not None for row in normalized_rows)
        ]
        emit(
            "inserting",
            message="Zapisywanie danych w bazie",
        )
        if normalized_rows and usable_columns:
            data = [[row.get(column) for column in usable_columns] for row in normalized_rows]
            ch_client.insert(
                table=table_name,
                data=data,
                column_names=list(usable_columns),
            )
            synced = len(normalized_rows)
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
        )


