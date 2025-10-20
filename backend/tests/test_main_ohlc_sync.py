import sys
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

import pytest
from fastapi import HTTPException

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.main import OhlcSyncRequest, sync_ohlc  # noqa: E402
from api import main as main_module  # noqa: E402
from api.symbols import DEFAULT_OHLC_SYNC_SYMBOLS  # noqa: E402
from api.stooq_ohlc import OhlcSyncResult  # noqa: E402


class FakeClickHouse:
    def __init__(self) -> None:
        self.commands: List[str] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def query(self, sql: str):
        raise AssertionError("Query should not be called in this scenario")


class FakeHarvester:
    def __init__(self) -> None:
        self.captured_symbols: Optional[List[str]] = None

    def sync(
        self,
        *,
        ch_client: Any,
        table_name: str,
        symbols: List[str],
        start_date: Optional[datetime] = None,
        truncate: bool = False,
        run_as_admin: bool = False,
        progress_callback=None,
    ) -> OhlcSyncResult:
        self.captured_symbols = list(symbols)
        now = datetime.utcnow()
        return OhlcSyncResult(
            symbols=len(symbols),
            inserted=0,
            skipped=0,
            errors=[],
            started_at=now,
            finished_at=now,
            truncated=truncate,
            request_log=[],
            requested_as_admin=run_as_admin,
        )


@pytest.fixture()
def fake_harvester(monkeypatch: pytest.MonkeyPatch) -> FakeHarvester:
    harvester = FakeHarvester()
    monkeypatch.setattr(main_module, "StooqOhlcHarvester", lambda: harvester)
    return harvester


@pytest.fixture()
def fake_clickhouse(monkeypatch: pytest.MonkeyPatch) -> FakeClickHouse:
    client = FakeClickHouse()
    monkeypatch.setattr(main_module, "get_ch", lambda: client)
    monkeypatch.setattr(main_module, "_create_ohlc_table_if_missing", lambda ch: None)
    monkeypatch.setattr(main_module, "_collect_all_company_symbols", lambda ch: None)
    return client


def test_sync_ohlc_uses_default_symbol_list_when_database_empty(
    fake_clickhouse: FakeClickHouse, fake_harvester: FakeHarvester
):
    request = OhlcSyncRequest()

    result = sync_ohlc(request)

    assert fake_harvester.captured_symbols == list(DEFAULT_OHLC_SYNC_SYMBOLS)
    assert result.symbols == len(DEFAULT_OHLC_SYNC_SYMBOLS)
    assert result.errors == []
    assert fake_clickhouse.commands == []


def test_sync_ohlc_background_starts_thread(monkeypatch: pytest.MonkeyPatch):
    main_module.OHLC_SYNC_PROGRESS_TRACKER.reset()

    captured: List[OhlcSyncRequest] = []

    def fake_perform(payload: OhlcSyncRequest) -> None:
        captured.append(payload)

    monkeypatch.setattr(main_module, "_perform_ohlc_sync", fake_perform)

    def fake_thread(target, args=(), kwargs=None, daemon=False):
        assert daemon is True
        assert not kwargs

        class DummyThread:
            def start(self_nonlocal):
                target(*args)

        return DummyThread()

    monkeypatch.setattr(main_module.threading, "Thread", fake_thread)

    request = OhlcSyncRequest(symbols=["CDR"])
    response = main_module.sync_ohlc_background(request)

    assert response == {"status": "accepted"}
    assert captured, "Background sync should invoke the worker"
    assert captured[0].symbols == ["CDR"]


def test_sync_ohlc_background_rejects_when_running():
    tracker = main_module.OHLC_SYNC_PROGRESS_TRACKER
    tracker.reset()
    tracker.start(total_symbols=1, requested_as_admin=False)

    request = OhlcSyncRequest()

    with pytest.raises(HTTPException) as exc:
        main_module.sync_ohlc_background(request)

    assert exc.value.status_code == 409
    tracker.reset()
