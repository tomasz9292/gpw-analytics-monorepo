from __future__ import annotations

from pathlib import Path
import sys
from typing import List

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api import main as main_module  # noqa: E402


class FakeResult:
    def __init__(self, rows: List[tuple]) -> None:
        self.result_rows = rows


class FakeClickHouse:
    def __init__(self) -> None:
        self.queries: List[str] = []

    def query(self, sql: str) -> FakeResult:
        self.queries.append(sql)
        if f"FROM {main_module.TABLE_COMPANIES}" in sql:
            return FakeResult([
                ("CD PROJEKT",),
                ("PKN ORLEN",),
                ("ACME INC",),
            ])
        if f"FROM {main_module.TABLE_OHLC}" in sql:
            return FakeResult([])
        raise AssertionError(f"Unexpected query: {sql!r}")


def test_collect_all_company_symbols_uses_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main_module, "_COMPANY_COLUMNS_CACHE", ["symbol"], raising=False)
    monkeypatch.setattr(main_module, "_COMPANY_SYMBOL_LOOKUP", None, raising=False)
    monkeypatch.setattr(
        main_module,
        "_build_company_symbol_lookup",
        lambda ch: {
            "CD PROJEKT": "CDR",
            "PKN ORLEN": "PKN",
            "CDR": "CDR",
            "PKN": "PKN",
        },
    )

    client = FakeClickHouse()

    symbols = main_module._collect_all_company_symbols(client)

    assert symbols == ["CDR", "PKN"], symbols
    assert all(" " not in sym for sym in symbols)
