"""Utilities for ingesting index compositions from GPW Benchmark."""

from __future__ import annotations

import html
import io
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

try:  # pragma: no cover - optional dependency
    import pdfplumber
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    pdfplumber = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    from bs4 import BeautifulSoup
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    BeautifulSoup = None  # type: ignore[assignment]

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LOGGER = logging.getLogger(__name__)


_DATE_CANDIDATE_KEYS = (
    "effective_date",
    "effectiveDate",
    "date",
    "validFrom",
    "valid_from",
    "fromDate",
    "from",
    "since",
    "updatedAt",
)

_WEIGHT_CANDIDATE_KEYS = (
    "weight",
    "weightPct",
    "weight_pct",
    "percent",
    "percentage",
    "share",
    "participation",
)

_SYMBOL_CANDIDATE_KEYS = (
    "ticker",
    "symbol",
    "code",
    "short",
    "shortName",
    "short_name",
    "isin",
)

_NAME_CANDIDATE_KEYS = (
    "name",
    "company",
    "companyName",
    "company_name",
    "label",
)

_INDEX_CODE_KEYS = (
    "code",
    "symbol",
    "index",
    "indexCode",
    "index_code",
    "ticker",
    "shortName",
    "short_name",
    "slug",
    "id",
)


@dataclass(frozen=True)
class IndexPortfolioRecord:
    index_code: str
    index_name: Optional[str]
    effective_date: date
    symbol: str
    company_name: Optional[str]
    weight: Optional[float]


@dataclass(frozen=True)
class IndexHistoryRecord:
    index_code: str
    index_name: Optional[str]
    date: date
    value: Optional[float]
    change_pct: Optional[float]


def _as_date(value: Any) -> Optional[date]:
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(float(value)).date()
        except (OSError, OverflowError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d.%m.%Y"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
    return None


def _parse_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        cleaned = cleaned.replace("%", "")
        cleaned = cleaned.replace("\xa0", " ")
        cleaned = cleaned.replace(" ", "")
        cleaned = cleaned.replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _parse_weight(value: Any) -> Optional[float]:
    parsed = _parse_float(value)
    if parsed is None:
        return None
    if parsed > 1.0:
        return parsed / 100.0
    return parsed


def _normalize_symbol(value: str) -> Optional[str]:
    cleaned = value.strip().upper()
    if not cleaned:
        return None
    if cleaned.endswith(".WA"):
        return cleaned
    if len(cleaned) > 12:
        return cleaned
    return f"{cleaned}.WA"


class GpwBenchmarkHarvester:
    """Client extracting index portfolios from gpwbenchmark.pl."""

    BASE_URL = "https://gpwbenchmark.pl"
    API_INDEXES = "/api/indexes?lang=pl"

    SESSION_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pl,en;q=0.9",
        "Referer": "https://gpwbenchmark.pl/",
    }

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self.session = session or requests.Session()
        self._configure_session()

    def _configure_session(self) -> None:
        """Configure the underlying requests session with sane defaults."""

        # RemoteDisconnected errors started appearing when the GPW Benchmark
        # backend began aggressively closing connections.  Using urllib3
        # retries ensures that we transparently recover from these transient
        # connection drops without surfacing an error to the user.
        retry = Retry(
            total=3,
            connect=5,
            read=5,
            backoff_factor=0.5,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=None,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    # ------------------------------------------------------------------
    def fetch(self) -> Tuple[List[IndexPortfolioRecord], List[IndexHistoryRecord]]:
        """Download and normalise data from GPW Benchmark."""

        root_payload = self._load_index_payload()
        portfolio_records: Dict[Tuple[str, date, str], IndexPortfolioRecord] = {}
        history_records: Dict[Tuple[str, date], IndexHistoryRecord] = {}

        for entry in root_payload:
            index_code = self._extract_index_code(entry)
            if not index_code:
                continue
            index_name = self._extract_index_name(entry)

            self._merge_portfolios(
                portfolio_records,
                self._extract_portfolios_for_index(entry, index_code, index_name),
            )
            self._merge_history(
                history_records,
                self._extract_history_for_index(entry, index_code, index_name),
            )

            missing_portfolios = not any(
                key[0] == index_code for key in portfolio_records.keys()
            )
            missing_history = not any(key[0] == index_code for key in history_records.keys())
            if missing_portfolios or missing_history:
                detail_payload = self._load_index_detail(entry)
                if detail_payload:
                    if missing_portfolios:
                        self._merge_portfolios(
                            portfolio_records,
                            self._extract_portfolios_for_index(
                                detail_payload, index_code, index_name
                            ),
                        )
                    if missing_history:
                        self._merge_history(
                            history_records,
                            self._extract_history_for_index(
                                detail_payload, index_code, index_name
                            ),
                        )
            else:
                detail_payload = None

            archive_source = detail_payload or entry
            self._merge_portfolios(
                portfolio_records,
                self._load_index_portfolio_archive(
                    archive_source, index_code, index_name
                ),
            )
            self._merge_history(
                history_records,
                self._load_index_history_archive(archive_source, index_code, index_name),
            )

        if not portfolio_records:
            self._merge_portfolios(
                portfolio_records,
                self._load_portfolios_from_pdf_archive(),
            )

        portfolios_sorted = sorted(
            portfolio_records.values(),
            key=lambda record: (record.index_code, record.effective_date, record.symbol),
        )
        history_sorted = sorted(
            history_records.values(), key=lambda record: (record.index_code, record.date)
        )

        return portfolios_sorted, history_sorted

    # ------------------------------------------------------------------
    @staticmethod
    def _merge_portfolios(
        destination: Dict[Tuple[str, date, str], IndexPortfolioRecord],
        records: Iterable[IndexPortfolioRecord],
    ) -> None:
        for record in records or []:
            key = (record.index_code, record.effective_date, record.symbol)
            if key not in destination:
                destination[key] = record

    @staticmethod
    def _merge_history(
        destination: Dict[Tuple[str, date], IndexHistoryRecord],
        records: Iterable[IndexHistoryRecord],
    ) -> None:
        for record in records or []:
            key = (record.index_code, record.date)
            if key not in destination:
                destination[key] = record

    # ------------------------------------------------------------------
    def _load_index_payload(self) -> List[Dict[str, Any]]:
        try:
            response = self.session.get(
                urljoin(self.BASE_URL, self.API_INDEXES), headers=self.SESSION_HEADERS, timeout=20
            )
            response.raise_for_status()
            data = response.json()
        except Exception as exc:  # noqa: BLE001 - we want to fall back to HTML parsing
            LOGGER.warning("GPW Benchmark API JSON failed: %s", exc)
            html_payload = self._load_html_page()
            data = self._extract_json_from_html(html_payload)

        if isinstance(data, dict):
            candidates = []
            for key in ("items", "indexes", "indices", "data", "results"):
                value = data.get(key)
                if isinstance(value, list):
                    candidates = value
                    break
            if not candidates and data:
                candidates = [data]
        elif isinstance(data, list):
            candidates = data
        else:
            candidates = []

        parsed: List[Dict[str, Any]] = [item for item in candidates if isinstance(item, dict)]
        if parsed:
            return parsed

        # The public site started embedding the payload deeper inside the
        # server-rendered JSON (e.g. within dehydrated React query state).
        # When the top-level keys above are missing we attempt to discover any
        # nested dictionaries that resemble index descriptors so that the rest
        # of the extraction pipeline can still operate.
        return self._discover_index_entries(data)

    # ------------------------------------------------------------------
    def _discover_index_entries(self, payload: Any) -> List[Dict[str, Any]]:
        if payload is None:
            return []
        results: Dict[Tuple[str, Optional[str], Optional[str]], Dict[str, Any]] = {}
        stack: List[Any] = [payload]
        visited: set[int] = set()
        while stack:
            current = stack.pop()
            current_id = id(current)
            if current_id in visited:
                continue
            visited.add(current_id)
            if isinstance(current, dict):
                if self._looks_like_index_entry(current):
                    code = self._extract_index_code(current)
                    slug = self._extract_index_slug(current)
                    name = self._extract_index_name(current)
                    key = (code or "", slug, name)
                    if key not in results:
                        results[key] = current
                for value in current.values():
                    stack.append(value)
            elif isinstance(current, list):
                for item in current:
                    stack.append(item)
        return list(results.values())

    @staticmethod
    def _looks_like_index_entry(payload: Dict[str, Any]) -> bool:
        if not isinstance(payload, dict):
            return False
        code = GpwBenchmarkHarvester._extract_index_code(payload)
        if not code:
            return False
        slug = GpwBenchmarkHarvester._extract_index_slug(payload)
        name = GpwBenchmarkHarvester._extract_index_name(payload)
        # Some containers are lightweight and only expose the code, but those
        # are rarely relevant.  Requiring either a slug or a readable name
        # keeps false positives (e.g. company dictionaries) to a minimum.
        return bool(slug or name)

    def _load_html_page(self) -> str:
        response = self.session.get(
            urljoin(self.BASE_URL, "/historyczne-portfele-indeksow"),
            headers={**self.SESSION_HEADERS, "Accept": "text/html"},
            timeout=20,
        )
        response.raise_for_status()
        return response.text

    @staticmethod
    def _extract_json_from_html(payload: str) -> Any:
        patterns = [
            re.compile(r"<script[^>]*id=\"__NEXT_DATA__\"[^>]*>(.*?)</script>", re.S),
            re.compile(r"window\.__NUXT__\s*=\s*(\{.*?\});", re.S),
            re.compile(r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\});", re.S),
        ]
        for pattern in patterns:
            match = pattern.search(payload)
            if not match:
                continue
            raw = html.unescape(match.group(1))
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                continue
        return {}

    def _load_index_detail(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        slug = self._extract_index_slug(payload)
        if not slug:
            return None
        try:
            response = self.session.get(
                urljoin(self.BASE_URL, f"/api/indexes/{slug}?lang=pl"),
                headers=self.SESSION_HEADERS,
                timeout=20,
            )
            response.raise_for_status()
            detail = response.json()
            if isinstance(detail, dict):
                return detail
        except Exception as exc:  # noqa: BLE001 - logging only
            LOGGER.debug("Failed to load detail for %s: %s", slug, exc)
        return None

    def _load_index_portfolio_archive(
        self, payload: Optional[Dict[str, Any]], index_code: str, index_name: Optional[str]
    ) -> List[IndexPortfolioRecord]:
        slug = self._extract_index_slug(payload or {})
        if not slug:
            return []
        endpoints = [
            f"/api/indexes/{slug}/portfolios?lang=pl&period=ALL",
            f"/api/indexes/{slug}/portfolios?lang=pl&period=all",
            f"/api/indexes/{slug}/portfolio?lang=pl&period=ALL",
        ]
        for endpoint in endpoints:
            try:
                response = self.session.get(
                    urljoin(self.BASE_URL, endpoint),
                    headers=self.SESSION_HEADERS,
                    timeout=20,
                )
                response.raise_for_status()
            except Exception as exc:  # noqa: BLE001 - logging only
                LOGGER.debug("Failed to load portfolio archive for %s using %s: %s", slug, endpoint, exc)
                continue
            try:
                data = response.json()
            except ValueError:
                LOGGER.debug("Portfolio archive %s for %s did not return JSON", endpoint, slug)
                continue
            container: Dict[str, Any]
            if isinstance(data, list):
                container = {"portfolios": data}
            elif isinstance(data, dict):
                container = data
            else:
                continue
            records = self._extract_portfolios_for_index(container, index_code, index_name)
            if records:
                return records
        return []

    # ------------------------------------------------------------------
    def _load_portfolios_from_pdf_archive(self) -> List[IndexPortfolioRecord]:
        if BeautifulSoup is None or pdfplumber is None:
            LOGGER.warning(
                "Skipping GPW Benchmark PDF archive fallback due to missing dependencies"
            )
            return []
        try:
            html_payload = self._load_html_page()
        except Exception as exc:  # noqa: BLE001 - logging only
            LOGGER.warning("GPW Benchmark PDF archive listing failed: %s", exc)
            return []

        links = self._discover_pdf_links(html_payload)
        if not links:
            return []

        records: Dict[Tuple[str, date, str], IndexPortfolioRecord] = {}
        for revision_date, index_code, index_name, pdf_url in links:
            try:
                response = self.session.get(
                    pdf_url,
                    headers={**self.SESSION_HEADERS, "Accept": "application/pdf"},
                    timeout=30,
                )
                response.raise_for_status()
            except Exception as exc:  # noqa: BLE001 - logging only
                LOGGER.debug("Failed to download GPW Benchmark PDF %s: %s", pdf_url, exc)
                continue
            try:
                entries = self._parse_pdf_portfolio(
                    response.content, revision_date, index_code, index_name
                )
            except Exception as exc:  # noqa: BLE001 - logging only
                LOGGER.debug("Failed to parse GPW Benchmark PDF %s: %s", pdf_url, exc)
                continue
            for record in entries:
                key = (record.index_code, record.effective_date, record.symbol)
                if key not in records:
                    records[key] = record
        return list(records.values())

    # ------------------------------------------------------------------
    def _discover_pdf_links(
        self, html_payload: str
    ) -> List[Tuple[date, str, Optional[str], str]]:
        soup = BeautifulSoup(html_payload, "html.parser")
        results: List[Tuple[date, str, Optional[str], str]] = []
        for anchor in soup.find_all("a"):
            text = (anchor.text or "").strip().lower()
            if "pobierz" not in text:
                continue
            href = anchor.get("href")
            if not href or not href.lower().endswith(".pdf"):
                continue
            if href.startswith("/"):
                url = urljoin(self.BASE_URL, href)
            elif href.startswith("http"):
                url = href
            else:
                url = urljoin(self.BASE_URL + "/", href)
            filename = url.split("/")[-1]
            index_code = self._normalize_index_from_filename(filename)
            revision_date = self._revision_date_from_filename(filename)
            if not index_code or not revision_date:
                continue
            results.append((revision_date, index_code, index_code, url))
        results.sort(key=lambda item: (item[0], item[1]), reverse=True)
        return results

    # ------------------------------------------------------------------
    def _parse_pdf_portfolio(
        self,
        pdf_bytes: bytes,
        revision_date: date,
        index_code: str,
        index_name: Optional[str],
    ) -> List[IndexPortfolioRecord]:
        entries: List[IndexPortfolioRecord] = []
        if pdfplumber is None:
            return []

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_entries = self._parse_pdf_page(page)
                for isin, ticker, company_name, weight_pct in page_entries:
                    weight = _parse_weight(weight_pct)
                    if weight is None:
                        continue
                    normalized_symbol = _normalize_symbol(ticker)
                    if not normalized_symbol:
                        continue
                    entries.append(
                        IndexPortfolioRecord(
                            index_code=index_code,
                            index_name=index_name,
                            effective_date=revision_date,
                            symbol=normalized_symbol,
                            company_name=company_name,
                            weight=weight,
                        )
                    )
                if page_entries:
                    continue
                text_entries = self._parse_pdf_text(page)
                for isin, ticker, company_name, weight_pct in text_entries:
                    weight = _parse_weight(weight_pct)
                    if weight is None:
                        continue
                    normalized_symbol = _normalize_symbol(ticker)
                    if not normalized_symbol:
                        continue
                    entries.append(
                        IndexPortfolioRecord(
                            index_code=index_code,
                            index_name=index_name,
                            effective_date=revision_date,
                            symbol=normalized_symbol,
                            company_name=company_name,
                            weight=weight,
                        )
                    )
        return entries

    # ------------------------------------------------------------------
    @staticmethod
    def _parse_pdf_page(page: pdfplumber.page.Page) -> List[Tuple[str, str, Optional[str], str]]:
        try:
            tables = page.extract_tables() or []
        except Exception:  # noqa: BLE001 - fall back to text extraction
            tables = []
        results: List[Tuple[str, str, Optional[str], str]] = []
        for table in tables:
            if not table:
                continue
            header = [cell.strip().lower() if isinstance(cell, str) else "" for cell in table[0]]
            header_joined = " ".join(header)
            if "kod" not in header_joined or "udział" not in header_joined:
                continue
            for row in table[1:]:
                if not row:
                    continue
                cells = [cell.strip() if isinstance(cell, str) else "" for cell in row]
                isin = next((cell for cell in cells if re.match(r"^[A-Z]{2}[A-Z0-9]{10,}$", cell)), "")
                if not isin:
                    continue
                try:
                    isin_index = cells.index(isin)
                except ValueError:
                    continue
                ticker = ""
                company_name: Optional[str] = None
                if isin_index + 1 < len(cells):
                    ticker = cells[isin_index + 1].replace(" ", "")
                if isin_index + 2 < len(cells):
                    company_name = cells[isin_index + 2] or None
                weight_candidates = [cell for cell in cells if re.search(r"\d+[,.]\d+", cell)]
                if not weight_candidates:
                    continue
                weight_text = weight_candidates[-1]
                weight_text = weight_text.replace("\xa0", " ").replace(" ", "")
                weight_text = weight_text.replace(",", ".")
                results.append((isin, ticker, company_name, weight_text))
        return results

    # ------------------------------------------------------------------
    @staticmethod
    def _parse_pdf_text(page: pdfplumber.page.Page) -> List[Tuple[str, str, Optional[str], str]]:
        try:
            text = page.extract_text() or ""
        except Exception:  # noqa: BLE001 - treat as empty page
            text = ""
        results: List[Tuple[str, str, Optional[str], str]] = []
        for line in text.splitlines():
            cleaned = re.sub(r"\s+", " ", line.strip())
            match = re.search(
                r"([A-Z]{2}[A-Z0-9]{9,})\s+([A-Z0-9\.]+)\s+(.*?)\s+(\d+[,.]\d{2,})$",
                cleaned,
            )
            if not match:
                continue
            isin, ticker, company_name, weight_text = match.groups()
            ticker = ticker.replace(" ", "")
            company_name = company_name.strip() or None
            weight_text = weight_text.replace(" ", "").replace("\xa0", "").replace(",", ".")
            results.append((isin, ticker, company_name, weight_text))
        return results

    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_index_from_filename(filename: str) -> Optional[str]:
        match = re.search(r"_(WIG30|WIG20|sWIG80|mWIG40|WIG)\.pdf$", filename, re.IGNORECASE)
        if not match:
            return None
        value = match.group(1)
        mapping = {
            "wig30": "WIG30",
            "wig20": "WIG20",
            "swig80": "sWIG80",
            "mwig40": "mWIG40",
            "wig": "WIG",
        }
        return mapping.get(value.lower(), value)

    @staticmethod
    def _revision_date_from_filename(filename: str) -> Optional[date]:
        match = re.search(r"(\d{4})_(\d{2})_(\d{2})_", filename)
        if not match:
            return None
        try:
            return datetime.strptime("-".join(match.groups()), "%Y-%m-%d").date()
        except ValueError:
            return None

    def _load_index_history_archive(
        self, payload: Optional[Dict[str, Any]], index_code: str, index_name: Optional[str]
    ) -> List[IndexHistoryRecord]:
        slug = self._extract_index_slug(payload or {})
        if not slug:
            return []
        endpoints = [
            f"/api/indexes/{slug}/history?lang=pl&period=ALL",
            f"/api/indexes/{slug}/history?lang=pl&period=all",
            f"/api/indexes/{slug}/history?lang=pl&range=ALL",
        ]
        for endpoint in endpoints:
            try:
                response = self.session.get(
                    urljoin(self.BASE_URL, endpoint),
                    headers=self.SESSION_HEADERS,
                    timeout=20,
                )
                response.raise_for_status()
            except Exception as exc:  # noqa: BLE001 - logging only
                LOGGER.debug("Failed to load history archive for %s using %s: %s", slug, endpoint, exc)
                continue
            try:
                data = response.json()
            except ValueError:
                LOGGER.debug("History archive %s for %s did not return JSON", endpoint, slug)
                continue
            container: Dict[str, Any]
            if isinstance(data, list):
                container = {"history": data}
            elif isinstance(data, dict):
                container = data
            else:
                continue
            records = self._extract_history_for_index(container, index_code, index_name)
            if records:
                return records
        return []

    @staticmethod
    def _extract_index_code(payload: Dict[str, Any]) -> Optional[str]:
        for key in _INDEX_CODE_KEYS:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().upper()
        nested = payload.get("index") or payload.get("meta") or {}
        if isinstance(nested, dict):
            return GpwBenchmarkHarvester._extract_index_code(nested)
        return None

    @staticmethod
    def _extract_index_name(payload: Dict[str, Any]) -> Optional[str]:
        for key in ("name", "title", "label", "indexName", "index_name"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        nested = payload.get("index") or payload.get("meta") or {}
        if isinstance(nested, dict):
            return GpwBenchmarkHarvester._extract_index_name(nested)
        return None

    @staticmethod
    def _extract_index_slug(payload: Dict[str, Any]) -> Optional[str]:
        for key in ("slug", "path", "id", "code", "symbol"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        url_value = None
        for key in ("url", "href", "link", "permalink"):
            candidate = payload.get(key)
            if isinstance(candidate, str) and candidate.strip():
                url_value = candidate.strip()
                break
        if url_value:
            url_value = url_value.strip("/")
            if "/" in url_value:
                url_value = url_value.rsplit("/", 1)[-1]
            return url_value
        return None

    # ------------------------------------------------------------------
    def _extract_portfolios_for_index(
        self, payload: Dict[str, Any], index_code: str, index_name: Optional[str]
    ) -> List[IndexPortfolioRecord]:
        containers = self._collect_candidate_containers(
            payload,
            {"portfolios", "portfolio", "indexPortfolios", "portfolioHistory"},
        )
        results: List[IndexPortfolioRecord] = []
        for container in containers:
            snapshots = self._normalise_snapshot_container(container)
            for snapshot in snapshots:
                effective_date = self._pick_date(snapshot)
                if effective_date is None:
                    continue
                constituents = self._pick_constituents(snapshot)
                if not constituents:
                    continue
                for entry in constituents:
                    symbol = None
                    for key in _SYMBOL_CANDIDATE_KEYS:
                        candidate = entry.get(key)
                        if isinstance(candidate, str) and candidate.strip():
                            symbol = candidate
                            break
                    if not symbol:
                        continue
                    normalized_symbol = _normalize_symbol(symbol)
                    if not normalized_symbol:
                        continue
                    company_name = None
                    for key in _NAME_CANDIDATE_KEYS:
                        candidate = entry.get(key)
                        if isinstance(candidate, str) and candidate.strip():
                            company_name = candidate.strip()
                            break
                    weight = None
                    for key in _WEIGHT_CANDIDATE_KEYS:
                        if key in entry:
                            weight = _parse_weight(entry.get(key))
                            break
                    results.append(
                        IndexPortfolioRecord(
                            index_code=index_code,
                            index_name=index_name,
                            effective_date=effective_date,
                            symbol=normalized_symbol,
                            company_name=company_name,
                            weight=weight,
                        )
                    )
        return results

    def _extract_history_for_index(
        self, payload: Dict[str, Any], index_code: str, index_name: Optional[str]
    ) -> List[IndexHistoryRecord]:
        containers = self._collect_candidate_containers(payload, {"history", "values", "quotes"})
        results: List[IndexHistoryRecord] = []
        for container in containers:
            records = self._normalise_snapshot_container(container)
            for record in records:
                record_date = self._pick_date(record)
                if record_date is None:
                    continue
                value = None
                for key in ("value", "close", "indexValue", "level", "points"):
                    if key in record:
                        value = _parse_float(record.get(key))
                        break
                change_pct = None
                for key in ("changePct", "change_pct", "change", "pct"):
                    if key in record:
                        change_pct = _parse_float(record.get(key))
                        if change_pct is not None and change_pct > 1.0:
                            change_pct /= 100.0
                        break
                results.append(
                    IndexHistoryRecord(
                        index_code=index_code,
                        index_name=index_name,
                        date=record_date,
                        value=value,
                        change_pct=change_pct,
                    )
                )
        return results

    # ------------------------------------------------------------------
    @staticmethod
    def _collect_candidate_containers(
        payload: Dict[str, Any], target_keys: Iterable[str]
    ) -> List[Any]:
        keys = {key.lower() for key in target_keys}
        queue: List[Any] = [payload]
        containers: List[Any] = []
        while queue:
            current = queue.pop()
            if isinstance(current, dict):
                for key, value in current.items():
                    if key.lower() in keys and value is not None:
                        containers.append(value)
                    queue.append(value)
            elif isinstance(current, list):
                queue.extend(current)
        return containers

    @staticmethod
    def _normalise_snapshot_container(container: Any) -> List[Dict[str, Any]]:
        if isinstance(container, list):
            return [item for item in container if isinstance(item, dict)]
        if isinstance(container, dict):
            snapshots: List[Dict[str, Any]] = []
            for value in container.values():
                if isinstance(value, dict):
                    snapshots.append(value)
                elif isinstance(value, list):
                    snapshots.extend([item for item in value if isinstance(item, dict)])
            return snapshots
        return []

    @staticmethod
    def _pick_date(payload: Dict[str, Any]) -> Optional[date]:
        for key in _DATE_CANDIDATE_KEYS:
            if key in payload:
                parsed = _as_date(payload.get(key))
                if parsed:
                    return parsed
        return None

    @staticmethod
    def _pick_constituents(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        for key in (
            "companies",
            "components",
            "composition",
            "members",
            "portfolio",
            "constituents",
        ):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []

