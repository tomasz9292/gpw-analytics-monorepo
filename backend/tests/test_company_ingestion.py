from __future__ import annotations

import json
from datetime import datetime, timedelta
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional
from types import SimpleNamespace
import sys
import time

import pytest
from fastapi import HTTPException

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api import main
from api.company_ingestion import (
    CompanyDataHarvester,
    CompanySyncProgress,
    CompanySyncResult,
    HttpRequestLog,
)


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
        self._history: List[HttpRequestLog] = []

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[int] = None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        entry = HttpRequestLog(url=url, params=params or {})
        self._history.append(entry)
        if not self._responses:
            entry.error = "Brak przygotowanych odpowiedzi testowych"
            entry.finished_at = datetime.utcnow()
            raise AssertionError(entry.error)
        response = self._responses.pop(0)
        entry.status_code = response.status_code
        entry.finished_at = datetime.utcnow()
        return response

    def clear_history(self) -> None:
        self._history.clear()

    def get_history(self) -> List[HttpRequestLog]:
        return list(self._history)


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.insert_calls: List[Dict[str, Any]] = []

    def insert(self, *, table: str, data: List[List[Any]], column_names: List[str]) -> None:
        self.insert_calls.append({"table": table, "data": data, "columns": column_names})


class FakeUnknownTableError(Exception):
    code = 60

    def __init__(self) -> None:
        super().__init__(
            "Code: 60. DB::Exception: Table default.companies does not exist. (UNKNOWN_TABLE)"
        )


def reset_sync_globals() -> None:
    main._SYNC_STATE = main.CompanySyncJobStatus()
    main._SYNC_THREAD = None
    main._SYNC_SCHEDULE_STATE = main.CompanySyncScheduleStatus()
    main._SCHEDULE_THREAD = None
    main._SCHEDULE_EVENT = threading.Event()


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

    assert result.fetched == 2
    assert result.synced == 2
    assert result.failed == 0
    assert result.errors == []
    assert result.started_at <= result.finished_at
    assert len(result.request_log) == 3
    assert all(entry.finished_at is not None for entry in result.request_log)
    assert result.request_log[0].url.startswith("https://www.gpw.pl")
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


def test_harvester_sync_reports_progress_events():
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
        "raw_payload",
    ]

    events: List[CompanySyncProgress] = []

    result = harvester.sync(
        ch_client=fake_client,
        table_name="companies",
        columns=columns,
        progress_callback=lambda evt: events.append(evt),
    )

    assert events, "Powinny być emitowane zdarzenia postępu"
    assert events[0].stage == "fetching"
    assert events[-1].stage == "finished"
    assert events[-1].synced == result.synced
    harvesting_events = [evt for evt in events if evt.stage == "harvesting"]
    assert harvesting_events, "Brak zdarzeń etapu harvestingu"
    assert harvesting_events[-1].synced == result.synced


def test_companies_sync_endpoint(monkeypatch):
    now = datetime.utcnow()
    fake_stats = CompanySyncResult(
        fetched=5,
        synced=3,
        failed=1,
        errors=["PKN: timeout"],
        started_at=now,
        finished_at=now,
        request_log=[],
    )
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


def test_companies_sync_background_endpoint(monkeypatch):
    reset_sync_globals()
    now = datetime.utcnow()
    fake_stats = CompanySyncResult(
        fetched=2,
        synced=2,
        failed=0,
        errors=[],
        started_at=now,
        finished_at=now,
        request_log=[],
    )

    class StubHarvester:
        def sync(
            self,
            *,
            ch_client: Any,
            table_name: str,
            columns: List[str],
            limit: Optional[int] = None,
            progress_callback=None,
        ) -> CompanySyncResult:
            if progress_callback:
                progress_callback(
                    CompanySyncProgress(
                        stage="harvesting",
                        total=2,
                        processed=1,
                        synced=1,
                        failed=0,
                        current_symbol="AAA",
                        message="Przetwarzanie AAA",
                    )
                )
            time.sleep(0.05)
            return fake_stats

    monkeypatch.setattr(main, "CompanyDataHarvester", lambda: StubHarvester())
    monkeypatch.setattr(main, "get_ch", lambda: object())
    monkeypatch.setattr(main, "_get_company_columns", lambda _client: ["symbol", "name"])
    reset_sync_globals()

    first_status = main.start_company_sync()
    assert first_status.status == "running"
    assert first_status.job_id

    with pytest.raises(HTTPException) as conflict_exc:
        main.start_company_sync()
    assert conflict_exc.value.status_code == 409

    final_status = None
    for _ in range(20):
        current = main.company_sync_status()
        if current.status != "running":
            final_status = current
            break
        time.sleep(0.05)

    assert final_status is not None
    assert final_status.status == "completed"
    assert final_status.result is not None
    assert final_status.result.synced == fake_stats.synced
    assert final_status.total == fake_stats.fetched
    assert final_status.processed >= fake_stats.fetched
    assert final_status.errors == []

    main._SYNC_STATE = main.CompanySyncJobStatus()
    main._SYNC_THREAD = None


def test_get_company_columns_creates_table_when_missing(monkeypatch):
    class FakeClient:
        def __init__(self) -> None:
            self._describe_calls = 0
            self.command_calls: List[str] = []

        def query(self, sql: str):
            self._describe_calls += 1
            if self._describe_calls == 1:
                raise FakeUnknownTableError()
            assert sql.startswith("DESCRIBE TABLE")
            return SimpleNamespace(result_rows=[("symbol",), ("name",)])

        def command(self, sql: str) -> None:
            self.command_calls.append(sql)

    client = FakeClient()
    previous_cache = main._COMPANY_COLUMNS_CACHE
    main._COMPANY_COLUMNS_CACHE = None
    try:
        columns = main._get_company_columns(client)
    finally:
        main._COMPANY_COLUMNS_CACHE = previous_cache

    assert columns == ["symbol", "name"]
    assert client._describe_calls == 2
    assert client.command_calls
    assert "CREATE TABLE IF NOT EXISTS" in client.command_calls[0]


def test_get_company_profile_endpoint(monkeypatch):
    class FakeResult:
        def __init__(self, columns: List[str], rows: List[tuple[Any, ...]]):
            self.column_names = columns
            self.result_rows = rows

    class FakeClickHouse:
        def query(self, sql: str, parameters: Optional[Dict[str, Any]] = None):
            assert "LIMIT 1" in sql
            assert parameters is not None
            return FakeResult(
                ["symbol", "name", "isin", "market_cap", "raw_payload"],
                [
                    (
                        "CDR",
                        "CD PROJEKT",
                        "PLCDPRO00015",
                        123.0,
                        json.dumps({"gpw": {"stockTicker": "CDR"}}),
                    )
                ],
            )

    monkeypatch.setattr(main, "get_ch", lambda: FakeClickHouse())
    monkeypatch.setattr(
        main,
        "_get_company_columns",
        lambda _client: ["symbol", "name", "isin", "market_cap", "raw_payload"],
    )

    profile = main.get_company_profile("CDR")
    assert profile.symbol.startswith("CDR")
    assert profile.raw_symbol == "CDR"
    assert profile.fundamentals.market_cap == 123.0


def test_get_company_profile_not_found(monkeypatch):
    class EmptyResult:
        def __init__(self):
            self.column_names = ["symbol"]
            self.result_rows: List[tuple[Any, ...]] = []

    class FakeClickHouse:
        def query(self, sql: str, parameters: Optional[Dict[str, Any]] = None):
            return EmptyResult()

    monkeypatch.setattr(main, "get_ch", lambda: FakeClickHouse())
    monkeypatch.setattr(main, "_get_company_columns", lambda _client: ["symbol"])

    with pytest.raises(HTTPException) as exc:
        main.get_company_profile("NONEXISTENT")
    assert exc.value.status_code == 404


def test_schedule_once_configuration(monkeypatch):
    reset_sync_globals()
    monkeypatch.setattr(main, "_ensure_schedule_thread_running", lambda: None)
    target = datetime.utcnow() + timedelta(minutes=15)
    request = main.CompanySyncScheduleRequest(mode="once", scheduled_for=target)
    response = main.update_company_sync_schedule(request)
    assert response.mode == "once"
    assert response.next_run_at is not None
    assert abs((response.next_run_at - target).total_seconds()) < 2
    with main._SCHEDULE_LOCK:
        assert main._SYNC_SCHEDULE_STATE.mode == "once"
        assert main._SYNC_SCHEDULE_STATE.next_run_at == response.next_run_at


def test_schedule_recurring_configuration(monkeypatch):
    reset_sync_globals()
    monkeypatch.setattr(main, "_ensure_schedule_thread_running", lambda: None)
    start_at = datetime.utcnow() + timedelta(minutes=10)
    request = main.CompanySyncScheduleRequest(
        mode="recurring",
        interval_minutes=90,
        start_at=start_at,
    )
    response = main.update_company_sync_schedule(request)
    assert response.mode == "recurring"
    assert response.recurring_interval_minutes == 90
    assert response.next_run_at is not None
    assert abs((response.next_run_at - start_at).total_seconds()) < 2
    with main._SCHEDULE_LOCK:
        assert main._SYNC_SCHEDULE_STATE.mode == "recurring"
        assert main._SYNC_SCHEDULE_STATE.recurring_interval_minutes == 90


def test_schedule_cancel_clears_state(monkeypatch):
    reset_sync_globals()
    monkeypatch.setattr(main, "_ensure_schedule_thread_running", lambda: None)
    future = datetime.utcnow() + timedelta(minutes=5)
    main.update_company_sync_schedule(
        main.CompanySyncScheduleRequest(mode="once", scheduled_for=future)
    )
    response = main.update_company_sync_schedule(
        main.CompanySyncScheduleRequest(mode="cancel")
    )
    assert response.mode == "idle"
    assert response.next_run_at is None
    with main._SCHEDULE_LOCK:
        assert main._SYNC_SCHEDULE_STATE.mode == "idle"
        assert main._SYNC_SCHEDULE_STATE.next_run_at is None


def test_check_and_run_scheduled_job_triggers_once(monkeypatch):
    reset_sync_globals()
    calls: List[Dict[str, Any]] = []

    def fake_start_sync_job(limit: Optional[int], *, message: str) -> str:
        calls.append({"limit": limit, "message": message})
        main._SYNC_STATE = main.CompanySyncJobStatus(
            job_id="scheduled-job",
            status="running",
            stage="fetching",
            total=None,
            processed=0,
            synced=0,
            failed=0,
            started_at=datetime.utcnow(),
            finished_at=None,
            current_symbol=None,
            message=message,
            errors=[],
            result=None,
        )
        main._SYNC_THREAD = None
        return "scheduled-job"

    monkeypatch.setattr(main, "_start_sync_job", fake_start_sync_job)
    monkeypatch.setattr(main, "_notify_schedule_loop", lambda: None)
    with main._SCHEDULE_LOCK:
        main._SYNC_SCHEDULE_STATE.mode = "once"
        main._SYNC_SCHEDULE_STATE.next_run_at = datetime.utcnow() - timedelta(seconds=1)

    started = main._check_and_run_scheduled_job(datetime.utcnow())

    assert started is True
    assert calls and calls[0]["message"] == "Planowana synchronizacja spółek"
    with main._SCHEDULE_LOCK:
        assert main._SYNC_SCHEDULE_STATE.mode == "idle"
        assert main._SYNC_SCHEDULE_STATE.next_run_at is None
        assert main._SYNC_SCHEDULE_STATE.last_run_status == "running"
        assert main._SYNC_SCHEDULE_STATE.last_run_started_at is not None


def test_check_and_run_scheduled_job_triggers_recurring_sets_next(monkeypatch):
    reset_sync_globals()
    calls: List[Dict[str, Any]] = []

    def fake_start_sync_job(limit: Optional[int], *, message: str) -> str:
        calls.append({"limit": limit, "message": message})
        main._SYNC_STATE = main.CompanySyncJobStatus(
            job_id="recurring-job",
            status="running",
            stage="fetching",
            total=None,
            processed=0,
            synced=0,
            failed=0,
            started_at=datetime.utcnow(),
            finished_at=None,
            current_symbol=None,
            message=message,
            errors=[],
            result=None,
        )
        main._SYNC_THREAD = None
        return "recurring-job"

    monkeypatch.setattr(main, "_start_sync_job", fake_start_sync_job)
    monkeypatch.setattr(main, "_notify_schedule_loop", lambda: None)
    now = datetime.utcnow()
    with main._SCHEDULE_LOCK:
        main._SYNC_SCHEDULE_STATE.mode = "recurring"
        main._SYNC_SCHEDULE_STATE.recurring_interval_minutes = 45
        main._SYNC_SCHEDULE_STATE.next_run_at = now - timedelta(seconds=2)

    started = main._check_and_run_scheduled_job(now)

    assert started is True
    assert calls and calls[-1]["message"] == "Planowana synchronizacja spółek"
    with main._SCHEDULE_LOCK:
        assert main._SYNC_SCHEDULE_STATE.mode == "recurring"
        assert main._SYNC_SCHEDULE_STATE.next_run_at is not None
        assert main._SYNC_SCHEDULE_STATE.next_run_at > now


def test_schedule_completion_updates_success(monkeypatch):
    reset_sync_globals()
    monkeypatch.setattr(main, "_notify_schedule_loop", lambda: None)

    class DummyHarvester:
        def sync(
            self,
            *,
            ch_client: Any,
            table_name: str,
            columns: List[str],
            limit: Optional[int] = None,
            progress_callback=None,
        ) -> CompanySyncResult:
            now = datetime.utcnow()
            if progress_callback:
                progress_callback(
                    CompanySyncProgress(
                        stage="fetching",
                        total=2,
                        processed=0,
                        synced=0,
                        failed=0,
                        current_symbol=None,
                        message="Start",
                    )
                )
                progress_callback(
                    CompanySyncProgress(
                        stage="finished",
                        total=2,
                        processed=2,
                        synced=2,
                        failed=0,
                        current_symbol=None,
                        message="Koniec",
                    )
                )
            return CompanySyncResult(
                fetched=2,
                synced=2,
                failed=0,
                errors=[],
                started_at=now,
                finished_at=now + timedelta(seconds=1),
                request_log=[],
            )

    monkeypatch.setattr(main, "CompanyDataHarvester", DummyHarvester)
    monkeypatch.setattr(main, "get_ch", lambda: object())
    monkeypatch.setattr(main, "_get_company_columns", lambda _client: ["symbol"])

    main._SYNC_STATE = main.CompanySyncJobStatus(
        job_id="scheduled",
        status="running",
        stage="fetching",
        total=None,
        processed=0,
        synced=0,
        failed=0,
        started_at=datetime.utcnow(),
        finished_at=None,
        current_symbol=None,
        message="Planowana synchronizacja spółek",
        errors=[],
        result=None,
    )
    main._SYNC_THREAD = None
    with main._SCHEDULE_LOCK:
        main._SYNC_SCHEDULE_STATE.last_run_status = "running"
        main._SYNC_SCHEDULE_STATE.last_run_started_at = datetime.utcnow()

    main._run_company_sync_job("scheduled", None)

    with main._SCHEDULE_LOCK:
        assert main._SYNC_SCHEDULE_STATE.last_run_status == "success"
        assert main._SYNC_SCHEDULE_STATE.last_run_finished_at is not None


def test_schedule_completion_failure_updates_state(monkeypatch):
    reset_sync_globals()
    monkeypatch.setattr(main, "_notify_schedule_loop", lambda: None)

    class FailingHarvester:
        def sync(
            self,
            *,
            ch_client: Any,
            table_name: str,
            columns: List[str],
            limit: Optional[int] = None,
            progress_callback=None,
        ) -> CompanySyncResult:
            raise RuntimeError("boom")

    monkeypatch.setattr(main, "CompanyDataHarvester", FailingHarvester)
    monkeypatch.setattr(main, "get_ch", lambda: object())
    monkeypatch.setattr(main, "_get_company_columns", lambda _client: ["symbol"])

    main._SYNC_STATE = main.CompanySyncJobStatus(
        job_id="failing",
        status="running",
        stage="fetching",
        total=None,
        processed=0,
        synced=0,
        failed=0,
        started_at=datetime.utcnow(),
        finished_at=None,
        current_symbol=None,
        message="Planowana synchronizacja spółek",
        errors=[],
        result=None,
    )
    main._SYNC_THREAD = None
    with main._SCHEDULE_LOCK:
        main._SYNC_SCHEDULE_STATE.last_run_status = "running"
        main._SYNC_SCHEDULE_STATE.last_run_started_at = datetime.utcnow()

    main._run_company_sync_job("failing", None)

    with main._SCHEDULE_LOCK:
        assert main._SYNC_SCHEDULE_STATE.last_run_status == "failed"
        assert main._SYNC_SCHEDULE_STATE.last_run_finished_at is not None

