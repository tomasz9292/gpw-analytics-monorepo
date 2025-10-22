from datetime import date, timedelta
from pathlib import Path
import sys

import pytest
from typing import List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api import main


class FakeResult:
    def __init__(self, rows, columns=None):
        self.result_rows = rows
        self._columns = columns or []

    def named_results(self):
        if not self._columns:
            raise AssertionError("named_results requested without column metadata")
        out = []
        for row in self.result_rows:
            out.append({name: value for name, value in zip(self._columns, row)})
        return out


class FakeClickHouse:
    def __init__(self, data):
        self.data = {symbol: list(rows) for symbol, rows in data.items()}
        self.queries: List[str] = []

    def query(self, sql, parameters=None):
        parameters = parameters or {}
        normalized_sql = " ".join(sql.split())
        self.queries.append(normalized_sql)

        if "SELECT DISTINCT symbol" in normalized_sql:
            rows = [(sym,) for sym in sorted(self.data.keys())]
            return FakeResult(rows)

        if (
            "WITH latest AS" in normalized_sql
            and "symbol IN %(symbols)s" in normalized_sql
            and "addDays" in normalized_sql
        ):
            symbols = parameters.get("symbols") or ()
            window = int(parameters.get("window", 0))
            rows = []
            for sym in symbols:
                history = self.data.get(sym, [])
                if not history:
                    continue
                last_date = max(date.fromisoformat(ds) for ds, _ in history)
                cutoff = (last_date - timedelta(days=window)).isoformat()
                for ds, close in history:
                    if ds >= cutoff:
                        rows.append((sym, ds, close))
            return FakeResult(rows)

        if "SELECT toString(date) as date, open, high, low, close, volume" in normalized_sql:
            symbol = parameters.get("sym")
            start = parameters.get("dt")
            if isinstance(start, date):
                start_str = start.isoformat()
            else:
                start_str = str(start)
            rows = [
                (
                    ds,
                    float(close),
                    float(close),
                    float(close),
                    float(close),
                    0.0,
                )
                for (ds, close) in self.data.get(symbol, [])
                if ds >= start_str
            ]
            return FakeResult(
                rows,
                columns=["date", "open", "high", "low", "close", "volume"],
            )

        if "WHERE symbol = %(sym)s AND date >= %(dt_start)s" in normalized_sql or (
            "WHERE symbol = %(sym)s AND date >= %(dt)s" in normalized_sql
        ):
            symbol = parameters.get("sym")
            start = parameters.get("dt_start")
            if start is None:
                start = parameters.get("dt")
            if isinstance(start, date):
                start_str = start.isoformat()
            else:
                start_str = str(start)
            rows = [
                (ds, close)
                for (ds, close) in self.data.get(symbol, [])
                if ds >= start_str
            ]
            return FakeResult(rows)

        if "WHERE symbol = %(sym)s" in normalized_sql:
            symbol = parameters.get("sym")
            rows = self.data.get(symbol, [])
            return FakeResult(rows)

        raise AssertionError(f"Unexpected query: {sql}")


def test_rank_symbols_with_multiple_components():
    data = {
        "AAA": [
            ("2023-01-01", 100.0),
            ("2023-01-02", 105.0),
            ("2023-01-03", 110.0),
            ("2023-01-04", 120.0),
            ("2023-01-05", 130.0),
        ],
        "BBB": [
            ("2023-01-01", 100.0),
            ("2023-01-02", 101.0),
            ("2023-01-03", 102.0),
            ("2023-01-04", 103.0),
            ("2023-01-05", 104.0),
        ],
        "CCC": [
            ("2023-01-01", 50.0),
            ("2023-01-02", 55.0),
            ("2023-01-03", 70.0),
            ("2023-01-04", 90.0),
            ("2023-01-05", 120.0),
        ],
    }
    fake = FakeClickHouse(data)

    components = [
        main.ScoreComponent(lookback_days=2, metric="total_return", weight=4),
        main.ScoreComponent(lookback_days=4, metric="total_return", weight=6),
    ]

    ranked = main._rank_symbols_by_score(fake, list(data.keys()), components)
    ordered = [sym for sym, _ in ranked]
    assert ordered[:3] == ["CCC", "AAA", "BBB"]

    history_queries = [q for q in fake.queries if "symbol IN %(symbols)s" in q]
    assert history_queries, "expected bulk history query to be used"
    assert all("WHERE symbol = %(sym)s" not in q for q in fake.queries)


def test_backtest_portfolio_auto_uses_dynamic_scores(monkeypatch):
    data = {
        "AAA": [
            ("2023-01-02", 100.0),
            ("2023-01-03", 101.0),
            ("2023-01-04", 102.0),
            ("2023-01-31", 110.0),
            ("2023-02-01", 109.0),
            ("2023-02-02", 108.0),
            ("2023-02-28", 107.0),
            ("2023-03-01", 105.0),
        ],
        "BBB": [
            ("2023-01-02", 100.0),
            ("2023-01-03", 99.0),
            ("2023-01-04", 98.0),
            ("2023-01-31", 97.0),
            ("2023-02-01", 100.0),
            ("2023-02-02", 105.0),
            ("2023-02-28", 108.0),
            ("2023-03-01", 112.0),
        ],
    }
    fake = FakeClickHouse(data)
    monkeypatch.setattr(main, "get_ch", lambda: fake)

    request = main.BacktestPortfolioRequest(
        start=date(2023, 1, 3),
        rebalance="monthly",
        auto=main.AutoSelectionConfig(
            top_n=1,
            components=[
                main.ScoreComponent(lookback_days=1, metric="total_return", weight=1)
            ],
            weighting="equal",
            filters=main.UniverseFilters(include=["AAA", "BBB"]),
        ),
    )

    result = main.backtest_portfolio(request)

    assert result.allocations is None
    assert result.rebalances is not None
    dates = [event.date for event in result.rebalances]
    assert dates[:2] == ["2023-01-03", "2023-02-01"]

    first_event = result.rebalances[0]
    assert first_event.trades is not None
    trade_symbols = [trade.symbol for trade in first_event.trades]
    assert any(symbol.startswith("AAA") for symbol in trade_symbols)
    cash_trade = [trade for trade in first_event.trades if trade.symbol == "Wolne środki"][0]
    assert cash_trade.target_weight == pytest.approx(0.5, rel=1e-6)
    assert cash_trade.note == "Wolne środki do transakcji"

    second_event = result.rebalances[1]
    assert second_event.trades is not None
    actions = {
        trade.symbol: trade.action
        for trade in second_event.trades
        if trade.symbol != "Wolne środki"
    }
    assert any(sym.startswith("AAA") and action == "sell" for sym, action in actions.items())
    assert any(sym.startswith("BBB") and action == "buy" for sym, action in actions.items())


def test_backtest_portfolio_auto_thresholds_leave_cash(monkeypatch):
    data = {
        "AAA": [
            ("2023-01-02", 100.0),
            ("2023-01-03", 100.5),
            ("2023-01-04", 101.0),
        ],
        "BBB": [
            ("2023-01-02", 80.0),
            ("2023-01-03", 80.1),
            ("2023-01-04", 80.2),
        ],
    }
    fake = FakeClickHouse(data)
    monkeypatch.setattr(main, "get_ch", lambda: fake)

    request = main.BacktestPortfolioRequest(
        start=date(2023, 1, 3),
        rebalance="none",
        auto=main.AutoSelectionConfig(
            top_n=2,
            components=[
                main.ScoreComponent(lookback_days=1, metric="total_return", weight=1)
            ],
            weighting="equal",
            min_score=0.02,
            max_score=0.5,
            filters=main.UniverseFilters(include=["AAA", "BBB"]),
        ),
    )

    result = main.backtest_portfolio(request)

    assert result.allocations is None
    assert result.rebalances is not None
    event = result.rebalances[0]
    assert event.trades is not None
    # only cash trade should be present because thresholds filtered out all symbols
    assert [trade.symbol for trade in event.trades] == ["Wolne środki"]
    assert event.trades[0].note == "Wolne środki do transakcji"


def test_portfolio_score_returns_top_n(monkeypatch):
    data = {
        "AAA": [
            ("2023-01-01", 100.0),
            ("2023-01-02", 105.0),
            ("2023-01-03", 110.0),
            ("2023-01-04", 115.0),
        ],
        "BBB": [
            ("2023-01-01", 50.0),
            ("2023-01-02", 51.0),
            ("2023-01-03", 52.0),
            ("2023-01-04", 53.0),
        ],
        "CCC": [
            ("2023-01-01", 40.0),
            ("2023-01-02", 60.0),
            ("2023-01-03", 80.0),
            ("2023-01-04", 120.0),
        ],
    }

    fake = FakeClickHouse(data)
    monkeypatch.setattr(main, "get_ch", lambda: fake)

    request = main.PortfolioScoreRequest(
        auto=main.AutoSelectionConfig(
            top_n=2,
            components=[
                main.ScoreComponent(lookback_days=2, metric="total_return", weight=5)
            ],
            weighting="equal",
        )
    )

    result = main.backtest_portfolio_score(request)

    assert [item.raw for item in result] == ["CCC", "AAA"]
    assert all(item.symbol.endswith(".WA") or item.symbol == item.raw for item in result)


def test_portfolio_score_respects_min_score(monkeypatch):
    monkeypatch.setattr(main, "get_ch", lambda: object())
    monkeypatch.setattr(main, "_list_candidate_symbols", lambda ch, filters: ["AAA", "BBB", "CCC"])
    monkeypatch.setattr(
        main,
        "_rank_symbols_by_score",
        lambda *args, **kwargs: [("AAA", 1.0), ("BBB", 0.4), ("CCC", 0.3)],
    )

    request = main.PortfolioScoreRequest(
        auto=main.AutoSelectionConfig(
            top_n=3,
            components=[main.ScoreComponent(lookback_days=2, metric="total_return", weight=5)],
            weighting="equal",
            min_score=0.5,
        )
    )

    result = main.backtest_portfolio_score(request)

    assert [item.raw for item in result] == ["AAA"]


def test_portfolio_score_respects_max_score(monkeypatch):
    monkeypatch.setattr(main, "get_ch", lambda: object())
    monkeypatch.setattr(main, "_list_candidate_symbols", lambda ch, filters: ["AAA", "BBB", "CCC"])
    monkeypatch.setattr(
        main,
        "_rank_symbols_by_score",
        lambda *args, **kwargs: [("AAA", 1.0), ("BBB", 0.6), ("CCC", 0.2)],
    )

    request = main.PortfolioScoreRequest(
        auto=main.AutoSelectionConfig(
            top_n=3,
            components=[main.ScoreComponent(lookback_days=2, metric="total_return", weight=5)],
            weighting="equal",
            max_score=0.5,
        )
    )

    result = main.backtest_portfolio_score(request)

    assert [item.raw for item in result] == ["CCC"]


def test_score_preview_returns_metrics(monkeypatch):
    data = {
        "AAA": [
            ("2023-01-01", 100.0),
            ("2023-01-02", 110.0),
            ("2023-01-03", 120.0),
            ("2023-01-04", 140.0),
            ("2023-01-05", 160.0),
        ],
        "BBB": [
            ("2023-01-01", 50.0),
            ("2023-01-02", 55.0),
            ("2023-01-03", 60.0),
            ("2023-01-04", 62.0),
            ("2023-01-05", 63.0),
        ],
    }

    fake = FakeClickHouse(data)
    monkeypatch.setattr(main, "get_ch", lambda: fake)

    request = main.ScorePreviewRequest(
        name="demo",
        rules=[
            main.ScoreRulePayload(metric="total_return_4", weight=2, direction="desc"),
            main.ScoreRulePayload(metric="volatility_4", weight=1, direction="asc"),
        ],
        limit=1,
    )

    response = main.score_preview(request)

    assert response.meta["universe_count"] == 2
    assert len(response.rows) == 1
    row = response.rows[0]
    assert "total_return_4" in row.metrics
    assert "volatility_4" in row.metrics


def test_collect_data_returns_filtered_quotes(monkeypatch):
    data = {
        "AAA": [
            ("2023-01-01", 100.0),
            ("2023-01-02", 105.0),
            ("2023-01-03", 110.0),
        ],
        "BBB": [
            ("2023-01-01", 200.0),
            ("2023-01-02", 195.0),
            ("2023-01-03", 190.0),
            ("2023-01-04", 185.0),
        ],
    }

    fake = FakeClickHouse(data)
    monkeypatch.setattr(main, "get_ch", lambda: fake)

    response = main.collect_data(
        symbols=["AAA", "BBB"], start="2023-01-02", end="2023-01-03"
    )

    assert [item.raw for item in response] == ["AAA", "BBB"]
    assert [q.date for q in response[0].quotes] == ["2023-01-02", "2023-01-03"]
    assert [q.close for q in response[0].quotes] == [105.0, 110.0]
    assert [q.date for q in response[1].quotes] == ["2023-01-02", "2023-01-03"]
    assert [q.close for q in response[1].quotes] == [195.0, 190.0]


def test_parse_backtest_get_accepts_comma_separated_values():
    req = main._parse_backtest_get(
        mode="manual",
        start="2023-01-01",
        rebalance="monthly",
        symbols=["AAA,BBB"],
        weights=["0.6,0.4"],
        top_n=None,
        weighting="equal",
        components=None,
        score=None,
        direction="desc",
        filters_include=None,
        filters_exclude=None,
        filters_prefixes=None,
    )

    assert req.manual is not None
    assert req.manual.symbols == ["AAA", "BBB"]
    assert req.manual.weights == [pytest.approx(0.6), pytest.approx(0.4)]


def test_parse_backtest_get_supports_score_mode():
    req = main._parse_backtest_get(
        mode="score",
        start="2023-01-01",
        rebalance="monthly",
        symbols=None,
        weights=None,
        top_n=3,
        weighting="score",
        components=None,
        score="quality_score",
        direction="asc",
        filters_include=None,
        filters_exclude=None,
        filters_prefixes=None,
    )

    assert req.auto is not None
    assert req.auto.top_n == 3
    assert req.auto.direction == "asc"
    assert len(req.auto.components) > 0


def test_portfolio_score_respects_ascending_direction(monkeypatch):
    data = {
        "AAA": [
            ("2023-01-01", 100.0),
            ("2023-01-02", 110.0),
            ("2023-01-03", 120.0),
            ("2023-01-04", 130.0),
        ],
        "BBB": [
            ("2023-01-01", 100.0),
            ("2023-01-02", 99.0),
            ("2023-01-03", 98.0),
            ("2023-01-04", 97.0),
        ],
        "CCC": [
            ("2023-01-01", 50.0),
            ("2023-01-02", 50.0),
            ("2023-01-03", 50.0),
            ("2023-01-04", 50.0),
        ],
    }

    fake = FakeClickHouse(data)
    monkeypatch.setattr(main, "get_ch", lambda: fake)

    request = main.PortfolioScoreRequest(
        auto=main.AutoSelectionConfig(
            top_n=1,
            components=[
                main.ScoreComponent(lookback_days=2, metric="total_return", weight=5)
            ],
            weighting="equal",
            direction="asc",
        )
    )

    result = main.backtest_portfolio_score(request)

    assert result[0].raw == "BBB"
