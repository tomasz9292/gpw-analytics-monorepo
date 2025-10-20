import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, List, Optional

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.company_ingestion import HttpRequestLog  # noqa: E402
from api.ohlc_sources import MultiSourceOhlcHarvester  # noqa: E402
from api.stooq_ohlc import OhlcRow  # noqa: E402


class FakeSource:
    def __init__(
        self,
        name: str,
        rows: Optional[List[OhlcRow]] = None,
        *,
        error: Optional[str] = None,
        logs: Optional[List[HttpRequestLog]] = None,
    ) -> None:
        self.name = name
        self._rows = rows or []
        self._error = error
        self._logs = logs or []
        self.reset_called = 0

    def reset(self) -> None:
        self.reset_called += 1

    def fetch_history(self, symbol: str) -> List[OhlcRow]:
        if self._error:
            raise RuntimeError(self._error)
        return list(self._rows)

    def get_request_log(self) -> List[HttpRequestLog]:
        return [log.model_copy(update={}) for log in self._logs]


class FakeClickHouse:
    def __init__(self) -> None:
        self.insert_calls: List[dict[str, Any]] = []
        self.command_calls: List[str] = []

    def command(self, sql: str) -> None:
        self.command_calls.append(sql)

    def insert(self, *, table: str, data: List[List[Any]], column_names: List[str]) -> None:
        self.insert_calls.append({
            "table": table,
            "data": data,
            "column_names": column_names,
        })


def test_multi_source_merges_rows_and_logs():
    base_row = OhlcRow(
        symbol="CDR",
        date=date(2024, 1, 2),
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        volume=None,
    )
    enriched_row = base_row.model_copy(update={"volume": 12345.0})
    primary_log = HttpRequestLog(url="https://primary", params={"s": "CDR"})
    secondary_log = HttpRequestLog(url="https://secondary", params={"s": "CDR"})

    sources = [
        FakeSource("primary", rows=[base_row], logs=[primary_log]),
        FakeSource("secondary", rows=[enriched_row], logs=[secondary_log]),
    ]

    harvester = MultiSourceOhlcHarvester(sources=sources)
    client = FakeClickHouse()

    result = harvester.sync(
        ch_client=client,
        table_name="ohlc",
        symbols=["CDR"],
        start_date=None,
        truncate=False,
        run_as_admin=False,
    )

    assert result.inserted == 1
    assert result.skipped == 0
    assert client.insert_calls[0]["data"][0][-1] == pytest.approx(12345.0)
    assert {log.source for log in result.request_log} == {"primary", "secondary"}


def test_multi_source_handles_partial_failures():
    good_row = OhlcRow(
        symbol="PKN",
        date=date(2024, 1, 3),
        open=100.0,
        high=101.0,
        low=99.5,
        close=100.5,
        volume=5000,
    )

    sources = [
        FakeSource("primary", rows=[good_row]),
        FakeSource("secondary", rows=None, error="timeout"),
    ]

    harvester = MultiSourceOhlcHarvester(sources=sources)
    client = FakeClickHouse()

    result = harvester.sync(
        ch_client=client,
        table_name="ohlc",
        symbols=["PKN"],
        start_date=None,
        truncate=False,
        run_as_admin=False,
    )

    assert result.inserted == 1
    assert any("timeout" in error for error in result.errors)
    assert result.skipped == 0


def test_multi_source_marks_symbol_as_skipped_when_all_sources_fail():
    sources = [
        FakeSource("primary", rows=None, error="404"),
        FakeSource("secondary", rows=None, error="500"),
    ]
    harvester = MultiSourceOhlcHarvester(sources=sources)
    client = FakeClickHouse()

    result = harvester.sync(
        ch_client=client,
        table_name="ohlc",
        symbols=["XYZ"],
        start_date=None,
        truncate=False,
        run_as_admin=False,
    )

    assert result.inserted == 0
    assert result.skipped == 1
    assert len(result.errors) == 2


def test_multi_source_sorts_request_logs_by_timestamp():
    base_row = OhlcRow(
        symbol="CDR",
        date=date(2024, 1, 2),
        open=10.0,
        high=11.0,
        low=9.0,
        close=10.5,
        volume=1000.0,
    )

    log_a = HttpRequestLog(url="https://primary", started_at=datetime(2024, 1, 2, 12, 0, 5))
    log_b = HttpRequestLog(url="https://secondary", started_at=datetime(2024, 1, 2, 12, 0, 1))

    sources = [
        FakeSource("primary", rows=[base_row], logs=[log_a]),
        FakeSource("secondary", rows=[base_row], logs=[log_b]),
    ]

    harvester = MultiSourceOhlcHarvester(sources=sources)
    client = FakeClickHouse()

    result = harvester.sync(
        ch_client=client,
        table_name="ohlc",
        symbols=["CDR"],
        start_date=None,
        truncate=False,
        run_as_admin=False,
    )

    assert [entry.url for entry in result.request_log] == [
        "https://secondary",
        "https://primary",
    ]
