from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

import pandas as pd


@dataclass
class PortfolioSimulationConfig:
    top_n: int = 5
    initial_cash: float = 100_000.0


@dataclass
class PortfolioSimulationResult:
    initial_value: float
    final_value: float
    return_pct: float
    annualized_return_pct: float | None
    max_drawdown_pct: float | None
    daily_values: List[dict]

    def as_dict(self) -> dict:
        return {
            "initial_value": float(self.initial_value),
            "final_value": float(self.final_value),
            "return_pct": float(self.return_pct),
            "annualized_return_pct": float(self.annualized_return_pct) if self.annualized_return_pct is not None else None,
            "max_drawdown_pct": float(self.max_drawdown_pct) if self.max_drawdown_pct is not None else None,
            "daily_values": self.daily_values,
        }


def _initial_prices(pivot: pd.DataFrame, symbols: Sequence[str]) -> pd.Series:
    prices = {}
    for symbol in symbols:
        series = pivot[symbol].dropna()
        if series.empty:
            raise ValueError(f"No price history available for symbol {symbol}")
        prices[symbol] = float(series.iloc[0])
    return pd.Series(prices, dtype=float)


def _shares(initial_cash: float, initial_prices: pd.Series) -> pd.Series:
    allocation = initial_cash / len(initial_prices)
    return (allocation / initial_prices).astype(float)


def _compute_annualized_return(values: pd.Series) -> float | None:
    if values.empty:
        return None
    start = values.index[0]
    end = values.index[-1]
    delta_days = (end - start).days
    if delta_days <= 0:
        return None
    total_return = float(values.iloc[-1] / values.iloc[0])
    return total_return ** (365.0 / delta_days) - 1.0


def _compute_max_drawdown(values: pd.Series) -> float | None:
    if values.empty:
        return None
    running_max = values.cummax()
    drawdowns = (values - running_max) / running_max
    return float(drawdowns.min()) if not drawdowns.empty else None


def simulate_equal_weight_portfolio(
    ohlc: pd.DataFrame,
    symbols: Sequence[str],
    initial_cash: float,
) -> PortfolioSimulationResult:
    if not len(symbols):
        raise ValueError("At least one symbol must be selected for simulation")

    subset = ohlc[ohlc["symbol"].isin(symbols)].copy()
    if subset.empty:
        raise ValueError("No OHLC data available for the selected symbols")

    subset["date"] = pd.to_datetime(subset["date"])
    subset.sort_values(["date", "symbol"], inplace=True)

    pivot = subset.pivot_table(index="date", columns="symbol", values="close", aggfunc="last")
    pivot = pivot.sort_index().ffill()

    initial_prices = _initial_prices(pivot, symbols)
    weights = _shares(initial_cash, initial_prices)

    portfolio_values = pivot[symbols].fillna(method="ffill").mul(weights, axis=1).sum(axis=1)
    portfolio_values = portfolio_values.dropna()

    if portfolio_values.empty:
        raise ValueError("Unable to compute portfolio value time series")

    initial_value = float(portfolio_values.iloc[0])
    final_value = float(portfolio_values.iloc[-1])
    return_pct = (final_value / initial_value) - 1.0 if initial_value else 0.0
    annualized_return = _compute_annualized_return(portfolio_values)
    max_drawdown = _compute_max_drawdown(portfolio_values)

    daily_values = [
        {"date": idx.date().isoformat(), "value": float(value)}
        for idx, value in portfolio_values.items()
    ]

    return PortfolioSimulationResult(
        initial_value=initial_value,
        final_value=final_value,
        return_pct=return_pct,
        annualized_return_pct=annualized_return,
        max_drawdown_pct=max_drawdown,
        daily_values=daily_values,
    )
