"""Utilities for ingesting index compositions from GPW Benchmark."""

from __future__ import annotations

import html
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import requests


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
    }

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self.session = session or requests.Session()

    # ------------------------------------------------------------------
    def fetch(self) -> Tuple[List[IndexPortfolioRecord], List[IndexHistoryRecord]]:
        """Download and normalise data from GPW Benchmark."""

        root_payload = self._load_index_payload()
        portfolio_records: List[IndexPortfolioRecord] = []
        history_records: List[IndexHistoryRecord] = []

        for entry in root_payload:
            index_code = self._extract_index_code(entry)
            if not index_code:
                continue
            index_name = self._extract_index_name(entry)

            portfolio_records.extend(
                self._extract_portfolios_for_index(entry, index_code, index_name)
            )
            history_records.extend(
                self._extract_history_for_index(entry, index_code, index_name)
            )

            missing_portfolios = not any(
                record.index_code == index_code for record in portfolio_records
            )
            missing_history = not any(
                record.index_code == index_code for record in history_records
            )
            if missing_portfolios or missing_history:
                detail_payload = self._load_index_detail(entry)
                if detail_payload:
                    if missing_portfolios:
                        portfolio_records.extend(
                            self._extract_portfolios_for_index(
                                detail_payload, index_code, index_name
                            )
                        )
                    if missing_history:
                        history_records.extend(
                            self._extract_history_for_index(
                                detail_payload, index_code, index_name
                            )
                        )

        return portfolio_records, history_records

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

        parsed: List[Dict[str, Any]] = []
        for item in candidates:
            if isinstance(item, dict):
                parsed.append(item)
        return parsed

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

    @staticmethod
    def _extract_index_code(payload: Dict[str, Any]) -> Optional[str]:
        for key in ("code", "symbol", "index", "indexCode", "index_code", "ticker"):
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
        return None

    # ------------------------------------------------------------------
    def _extract_portfolios_for_index(
        self, payload: Dict[str, Any], index_code: str, index_name: Optional[str]
    ) -> List[IndexPortfolioRecord]:
        containers = self._collect_candidate_containers(payload, {"portfolios", "portfolio"})
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
        for key in ("companies", "components", "composition", "members", "portfolio"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []

