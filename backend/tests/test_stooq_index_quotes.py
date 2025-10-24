import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.stooq_index_quotes import INDEX_SYMBOL_ALIASES, StooqIndexQuoteHarvester
from api.stooq_ohlc import OhlcRow


class FakeOhlcHarvester:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def fetch_history(self, symbol):
        self.calls.append(symbol)
        response = self.responses[symbol]
        if isinstance(response, Exception):
            raise response
        return response


def _make_row(symbol: str, value: float) -> OhlcRow:
    return OhlcRow(
        symbol=symbol,
        date=date(2024, 1, 1),
        open=value,
        high=value,
        low=value,
        close=value,
        volume=None,
    )


def test_fetch_history_prefers_primary_alias():
    responses = {
        "WIG20": [_make_row("WIG20", 1.0)],
    }
    harvester = StooqIndexQuoteHarvester(ohlc_harvester=FakeOhlcHarvester(responses))

    rows = harvester.fetch_history("WIG20")

    assert [row.index_code for row in rows] == ["WIG20"]
    assert rows[0].close == 1.0


def test_fetch_history_falls_back_to_legacy_symbol():
    responses = {
        "MWIG40": [],
        "MW40": [_make_row("MW40", 2.0)],
    }
    fake_harvester = FakeOhlcHarvester(responses)
    harvester = StooqIndexQuoteHarvester(ohlc_harvester=fake_harvester)

    rows = harvester.fetch_history("MWIG40")

    assert [row.index_code for row in rows] == ["MWIG40"]
    assert rows[0].close == 2.0
    assert fake_harvester.calls == ["MWIG40", "MW40"]


def test_fetch_history_raises_last_error_when_all_candidates_fail():
    error = RuntimeError("boom")
    responses = {
        candidate: error for candidate in INDEX_SYMBOL_ALIASES["MWIG40"]
    }
    fake_harvester = FakeOhlcHarvester(responses)
    harvester = StooqIndexQuoteHarvester(ohlc_harvester=fake_harvester)

    with pytest.raises(RuntimeError, match="boom"):
        harvester.fetch_history("MWIG40")

