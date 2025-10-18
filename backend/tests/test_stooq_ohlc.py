import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.company_ingestion import HttpRequestLog
from api.stooq_ohlc import OhlcSyncResult, StooqOhlcHarvester


class FakeResponse:
    def __init__(self, text: str) -> None:
        self._text = text

    def text(self) -> str:
        return self._text


class FakeSession:
    def __init__(self, payloads: List[str]) -> None:
        self._payloads = list(payloads)
        self.calls: List[str] = []
        self._history: List[HttpRequestLog] = []

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15):
        self.calls.append(url)
        entry = HttpRequestLog(url=url, params=params or {})
        self._history.append(entry)
        if not self._payloads:
            raise AssertionError("Brak przygotowanych odpowiedzi testowych")
        return FakeResponse(self._payloads.pop(0))

    def clear_history(self) -> None:
        self._history.clear()

    def get_history(self) -> List[HttpRequestLog]:
        return list(self._history)


class FakeClickHouse:
    def __init__(self) -> None:
        self.command_calls: List[str] = []
        self.insert_calls: List[Dict[str, Any]] = []

    def command(self, sql: str) -> None:
        self.command_calls.append(sql)

    def insert(self, *, table: str, data: List[List[Any]], column_names: List[str]) -> None:
        self.insert_calls.append({
            "table": table,
            "data": data,
            "column_names": column_names,
        })


CSV_SAMPLE = """Data,Otwarcie,Najwyzszy,Najnizszy,Zamkniecie,Wolumen\n2024-01-02,10,11,9,10.5,12345\n2024-01-03,10.5,11.5,10.2,11,23456\n"""


def test_parse_csv_returns_sorted_rows():
    parsed = StooqOhlcHarvester._parse_csv(CSV_SAMPLE)
    assert [row["date"] for row in parsed] == [date(2024, 1, 2), date(2024, 1, 3)]
    assert parsed[0]["open"] == pytest.approx(10.0)
    assert parsed[1]["volume"] == pytest.approx(23456.0)


def test_fetch_history_normalizes_symbol_and_returns_rows():
    session = FakeSession([CSV_SAMPLE])
    harvester = StooqOhlcHarvester(session=session)

    rows = harvester.fetch_history("CDR.WA")

    assert [row.date for row in rows] == [date(2024, 1, 2), date(2024, 1, 3)]
    assert all(row.symbol == "CDR" for row in rows)
    assert session.calls[0].endswith("s=cdr&i=d")


def test_sync_truncates_and_inserts_filtered_rows():
    session = FakeSession([CSV_SAMPLE])
    harvester = StooqOhlcHarvester(session=session)
    client = FakeClickHouse()

    result = harvester.sync(
        ch_client=client,
        table_name="ohlc",
        symbols=["CDR"],
        start_date=date(2024, 1, 3),
        truncate=True,
        run_as_admin=True,
    )

    assert isinstance(result, OhlcSyncResult)
    assert result.symbols == 1
    assert result.inserted == 1
    assert result.skipped == 0
    assert result.truncated is True
    assert result.requested_as_admin is True
    assert result.sync_type == "historical_prices"
    assert client.command_calls == ["TRUNCATE TABLE ohlc"]
    assert len(client.insert_calls) == 1
    inserted = client.insert_calls[0]
    assert inserted["table"] == "ohlc"
    assert inserted["column_names"] == [
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    assert inserted["data"] == [["CDR", date(2024, 1, 3), 10.5, 11.5, 10.2, 11.0, 23456.0]]
