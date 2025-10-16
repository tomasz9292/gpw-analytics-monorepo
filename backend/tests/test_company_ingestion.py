from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api import main
from api.company_ingestion import CompanyDataHarvester, CompanySyncResult


class FakeResponse:
    def __init__(self, payload: Dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> Dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses: List[FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: List[Dict[str, Any]] = []

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[int] = None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        if not self._responses:
            raise AssertionError("Brak przygotowanych odpowiedzi testowych")
        return self._responses.pop(0)


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.insert_calls: List[Dict[str, Any]] = []

    def insert(self, *, table: str, data: List[List[Any]], column_names: List[str]) -> None:
        self.insert_calls.append({"table": table, "data": data, "columns": column_names})


GPW_FIXTURE = {
    "success": True,
    "data": [
        {
            "stockTicker": "CDR",
            "isin": "PLCDPRO00015",
            "companyName": "CD PROJEKT SPÓŁKA AKCYJNA",
            "shortName": "CD PROJEKT",
            "sectorName": "IT",
            "subsectorName": "Gry komputerowe",
            "country": "Polska",
            "city": "Warszawa",
            "firstQuotationDate": "2010-10-28",
            "www": "www.cdprojekt.com",
            "profile": "CD PROJEKT produkuje gry.",
        },
        {
            "stockTicker": "PKN",
            "isin": "PLPKN0000018",
            "companyName": "PKN ORLEN S.A.",
            "shortName": "PKN ORLEN",
            "sectorName": "Paliwa",
            "country": "Polska",
            "city": "Płock",
            "firstQuotationDate": "1999-11-26",
            "www": "https://www.orlen.pl",
            "profile": "PKN Orlen jest koncernem paliwowym.",
        },
    ],
}

YAHOO_CDR = {
    "quoteSummary": {
        "result": [
            {
                "price": {
                    "longName": "CD PROJEKT S.A.",
                    "shortName": "CD PROJEKT",
                    "marketCap": {"raw": 45000000000},
                },
                "assetProfile": {
                    "industry": "Electronic Gaming & Multimedia",
                    "sector": "Communication Services",
                    "country": "Poland",
                    "city": "Warsaw",
                    "longBusinessSummary": "Szczegółowy opis z Yahoo.",
                    "website": "https://www.cdprojekt.com",
                    "fullTimeEmployees": 1220,
                },
                "summaryDetail": {
                    "trailingPE": {"raw": 21.5},
                    "priceToBook": {"raw": 5.2},
                    "dividendYield": {"raw": 0.012},
                },
                "defaultKeyStatistics": {
                    "marketCap": {"raw": 45000000000},
                    "trailingEps": {"raw": 12.34},
                },
                "financialData": {
                    "totalRevenue": {"raw": 2000000000},
                    "netIncomeToCommon": {"raw": 900000000},
                    "ebitda": {"raw": 1000000000},
                    "returnOnEquity": {"raw": 0.25},
                    "returnOnAssets": {"raw": 0.12},
                    "grossMargins": {"raw": 0.6},
                    "operatingMargins": {"raw": 0.4},
                    "profitMargins": {"raw": 0.35},
                },
            }
        ],
        "error": None,
    }
}

YAHOO_PKN = {
    "quoteSummary": {
        "result": [
            {
                "price": {
                    "longName": "PKN ORLEN SPÓŁKA AKCYJNA",
                },
                "assetProfile": {
                    "industry": "Oil & Gas Refining & Marketing",
                    "sector": "Energy",
                    "country": "Poland",
                    "city": "Płock",
                    "website": "http://www.orlen.pl",
                    "fullTimeEmployees": 40000,
                },
                "summaryDetail": {},
                "defaultKeyStatistics": {
                    "marketCap": {"raw": 80000000000},
                },
                "financialData": {},
            }
        ],
        "error": None,
    }
}


def test_harvester_sync_inserts_expected_rows():
    session = FakeSession(
        [
            FakeResponse(GPW_FIXTURE),
            FakeResponse(YAHOO_CDR),
            FakeResponse(YAHOO_PKN),
        ]
    )
    harvester = CompanyDataHarvester(session=session)
    fake_client = FakeClickHouseClient()

    columns = [
        "symbol",
        "name",
        "short_name",
        "isin",
        "sector",
        "industry",
        "country",
        "headquarters",
        "website",
        "description",
        "logo_url",
        "employees",
        "listing_date",
        "market_cap",
        "revenue_ttm",
        "net_income_ttm",
        "ebitda_ttm",
        "eps",
        "pe_ratio",
        "pb_ratio",
        "dividend_yield",
        "roe",
        "roa",
        "gross_margin",
        "operating_margin",
        "profit_margin",
        "raw_payload",
    ]

    result = harvester.sync(
        ch_client=fake_client,
        table_name="companies",
        columns=columns,
    )

    assert result == CompanySyncResult(fetched=2, synced=2, failed=0, errors=[])
    assert len(fake_client.insert_calls) == 1
    insert_call = fake_client.insert_calls[0]
    assert insert_call["table"] == "companies"
    used_columns = insert_call["columns"]
    assert "symbol" in used_columns
    assert "market_cap" in used_columns

    rows = [dict(zip(used_columns, row)) for row in insert_call["data"]]
    first = rows[0]
    assert first["symbol"] == "CDR"
    assert first["website"] == "https://www.cdprojekt.com"
    assert first["logo_url"] == "https://logo.clearbit.com/cdprojekt.com"
    assert pytest.approx(first["market_cap"], rel=1e-6) == 45000000000
    payload = json.loads(first["raw_payload"])
    assert payload["gpw"]["stockTicker"] == "CDR"
    assert payload["yahoo"]["assetProfile"]["industry"] == "Electronic Gaming & Multimedia"

    second = rows[1]
    assert second["symbol"] == "PKN"
    assert second["website"] == "https://www.orlen.pl"
    assert second["description"].startswith("PKN Orlen")


def test_companies_sync_endpoint(monkeypatch):
    fake_stats = CompanySyncResult(fetched=5, synced=3, failed=1, errors=["PKN: timeout"])
    harvester_instances: List[Any] = []

    class StubHarvester:
        def __init__(self):
            self.calls: List[Dict[str, Any]] = []

        def sync(self, *, ch_client: Any, table_name: str, columns: List[str], limit: Optional[int] = None):
            self.calls.append(
                {
                    "ch_client": ch_client,
                    "table_name": table_name,
                    "columns": columns,
                    "limit": limit,
                }
            )
            return fake_stats

    def harvester_factory():
        instance = StubHarvester()
        harvester_instances.append(instance)
        return instance

    fake_clickhouse = object()
    monkeypatch.setattr(main, "CompanyDataHarvester", harvester_factory)
    monkeypatch.setattr(main, "get_ch", lambda: fake_clickhouse)
    monkeypatch.setattr(main, "_get_company_columns", lambda _client: ["symbol", "name"])

    result = main.sync_companies(limit=50)

    assert result == fake_stats
    assert harvester_instances, "Harvester nie został utworzony"
    call = harvester_instances[0].calls[0]
    assert call["ch_client"] is fake_clickhouse
    assert call["table_name"] == main.TABLE_COMPANIES
    assert call["columns"] == ["symbol", "name"]
    assert call["limit"] == 50


