from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Iterable, List

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import api.main as api_main  # noqa: E402
from api.main import _collect_index_membership_union, _collect_latest_index_membership  # noqa: E402


class _DummyResult:
    def __init__(self, rows: Iterable[dict | List[object]]):
        self._rows = list(rows)

    def named_results(self):  # pragma: no cover - used implicitly in tests
        return self._rows

    @property
    def result_rows(self):  # pragma: no cover - fallback path
        return self._rows


class _DummyClient:
    def __init__(self, rows: Iterable[dict | List[object]]):
        self._rows = list(rows)

    def command(self, *_args, **_kwargs):  # pragma: no cover - no-op
        return None

    def query(self, *_args, **_kwargs):  # pragma: no cover - returns static rows
        return _DummyResult(self._rows)


def _mock_symbol_lookup(_client):
    return {
        "AMICA": "AMC",
        "AMICA.WA": "AMC",
        "AMC": "AMC",
        "ACTION": "ACTION",
        "ACTION.WA": "ACTION",
        "BUMECH": "BMC",
        "BUMECH.WA": "BMC",
        "BMC": "BMC",
    }


def test_collect_latest_index_membership_prefers_symbol_base(monkeypatch: pytest.MonkeyPatch):
    api_main._COMPANY_SYMBOL_LOOKUP = None
    monkeypatch.setattr(api_main, "_build_company_symbol_lookup", _mock_symbol_lookup)
    client = _DummyClient(
        [
            {"index_code": "SWIG80", "symbol": "AMICA.WA", "symbol_base": "AMC"},
            {"index_code": "SWIG80", "symbol": "ACTION.WA", "symbol_base": None},
        ]
    )

    result = _collect_latest_index_membership(client, ["swig80"])

    assert result == {"SWIG80": ["AMC", "ACTION"]}


def test_collect_index_membership_union_handles_history(monkeypatch: pytest.MonkeyPatch):
    api_main._COMPANY_SYMBOL_LOOKUP = None
    monkeypatch.setattr(api_main, "_build_company_symbol_lookup", _mock_symbol_lookup)
    client = _DummyClient(
        [
            {
                "index_code": "SWIG80",
                "index_name": "sWIG80",
                "effective_date": "2024-01-01",
                "symbol": "AMICA.WA",
                "symbol_base": "AMC",
            },
            {
                "index_code": "SWIG80",
                "index_name": "sWIG80",
                "effective_date": "2024-01-01",
                "symbol": "ACTION.WA",
                "symbol_base": None,
            },
            {
                "index_code": "SWIG80",
                "index_name": "sWIG80",
                "effective_date": "2024-02-01",
                "symbol": "AMICA.WA",
                "symbol_base": "AMC",
            },
            {
                "index_code": "SWIG80",
                "index_name": "sWIG80",
                "effective_date": "2024-02-01",
                "symbol": "BUMECH.WA",
                "symbol_base": "BMC",
            },
        ]
    )

    result = _collect_index_membership_union(client, ["SWIG80"])

    assert result == {"SWIG80": ["ACTION", "AMC", "BMC"]}


def test_collect_latest_index_membership_as_of_filters_future(monkeypatch: pytest.MonkeyPatch):
    api_main._COMPANY_SYMBOL_LOOKUP = None
    monkeypatch.setattr(api_main, "_build_company_symbol_lookup", _mock_symbol_lookup)
    client = _DummyClient(
        [
            {
                "index_code": "SWIG80",
                "index_name": "sWIG80",
                "effective_date": "2024-01-01",
                "symbol": "ACTION.WA",
                "symbol_base": None,
            },
            {
                "index_code": "SWIG80",
                "index_name": "sWIG80",
                "effective_date": "2024-02-01",
                "symbol": "BUMECH.WA",
                "symbol_base": "BMC",
            },
        ]
    )

    result = _collect_latest_index_membership(
        client, ["SWIG80"], as_of=date(2024, 1, 15)
    )

    assert result == {"SWIG80": ["ACTION"]}


def test_collect_index_membership_union_limits_by_as_of(monkeypatch: pytest.MonkeyPatch):
    api_main._COMPANY_SYMBOL_LOOKUP = None
    monkeypatch.setattr(api_main, "_build_company_symbol_lookup", _mock_symbol_lookup)
    client = _DummyClient(
        [
            {
                "index_code": "SWIG80",
                "index_name": "sWIG80",
                "effective_date": "2024-01-01",
                "symbol": "ACTION.WA",
                "symbol_base": None,
            },
            {
                "index_code": "SWIG80",
                "index_name": "sWIG80",
                "effective_date": "2024-02-01",
                "symbol": "BUMECH.WA",
                "symbol_base": "BMC",
            },
            {
                "index_code": "SWIG80",
                "index_name": "sWIG80",
                "effective_date": "2024-03-01",
                "symbol": "AMICA.WA",
                "symbol_base": "AMC",
            },
        ]
    )

    result = _collect_index_membership_union(
        client, ["SWIG80"], as_of=date(2024, 2, 15)
    )

    assert result == {"SWIG80": ["ACTION", "BMC"]}
