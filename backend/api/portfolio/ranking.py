from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Mapping, Optional

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class RankingFeature:
    """Metadata describing a ranking feature."""

    name: str
    description: str
    compute: Callable[[pd.DataFrame], pd.Series]


class RankingFeatureRegistry:
    """Container holding available ranking features."""

    def __init__(self, features: Iterable[RankingFeature]):
        self._features: Dict[str, RankingFeature] = {feature.name: feature for feature in features}

    def get(self, name: str) -> RankingFeature:
        try:
            return self._features[name]
        except KeyError as exc:
            raise KeyError(f"Unknown ranking feature: {name}") from exc

    def names(self) -> List[str]:
        return list(self._features.keys())

    def items(self) -> Iterable[RankingFeature]:
        return self._features.values()


def _feature_momentum(ohlc: pd.DataFrame) -> pd.Series:
    grouped = ohlc.groupby("symbol")
    momentum = grouped["close"].apply(lambda series: (series.iloc[-1] / series.iloc[0]) - 1 if len(series) > 1 else 0.0)
    return momentum.astype(float)


def _feature_volatility(ohlc: pd.DataFrame) -> pd.Series:
    grouped = ohlc.groupby("symbol")

    def _vol(series: pd.Series) -> float:
        returns = series.pct_change().dropna()
        if not len(returns):
            return 0.0
        return returns.std()

    raw_volatility = grouped["close"].apply(_vol).astype(float)
    # Lower volatility is better – convert to a decreasing penalty.
    safe_volatility = raw_volatility.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return 1.0 / (1.0 + safe_volatility)


def _feature_average_volume(ohlc: pd.DataFrame) -> pd.Series:
    if "volume" not in ohlc.columns:
        return pd.Series({symbol: 0.0 for symbol in ohlc["symbol"].unique()}, dtype=float)

    grouped = ohlc.groupby("symbol")
    volume = grouped["volume"].mean().astype(float)
    safe_volume = volume.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return safe_volume


AVAILABLE_FEATURES = RankingFeatureRegistry(
    [
        RankingFeature(
            name="momentum",
            description="Total return between the first and last observation.",
            compute=_feature_momentum,
        ),
        RankingFeature(
            name="volatility",
            description="Inverse of the standard deviation of daily returns (higher is better).",
            compute=_feature_volatility,
        ),
        RankingFeature(
            name="average_volume",
            description="Average traded volume over the analysed window.",
            compute=_feature_average_volume,
        ),
    ]
)


@dataclass
class RankingComputationResult:
    scores: pd.Series
    feature_values: Dict[str, pd.Series]
    normalized_feature_values: Dict[str, pd.Series]

    def top(self, n: int) -> pd.Series:
        return self.scores.sort_values(ascending=False).head(n)

    def as_serializable(self) -> List[Dict[str, object]]:
        payload: List[Dict[str, object]] = []
        for symbol, score in self.scores.sort_values(ascending=False).items():
            entry: Dict[str, object] = {"symbol": symbol, "score": float(score)}
            entry["features"] = {
                name: float(values.get(symbol, np.nan))
                for name, values in self.feature_values.items()
            }
            entry["features_normalized"] = {
                name: float(values.get(symbol, np.nan))
                for name, values in self.normalized_feature_values.items()
            }
            payload.append(entry)
        return payload


def _normalize(series: pd.Series) -> pd.Series:
    cleaned = series.replace([np.inf, -np.inf], np.nan).dropna()
    if cleaned.empty:
        return pd.Series({index: 0.0 for index in series.index}, dtype=float)

    min_value = float(cleaned.min())
    max_value = float(cleaned.max())
    if math.isclose(max_value, min_value):
        return pd.Series({index: 1.0 for index in series.index}, dtype=float)

    normalized = (series - min_value) / (max_value - min_value)
    normalized = normalized.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return normalized.astype(float)


def _normalize_weights(weights: Mapping[str, float], feature_names: Iterable[str]) -> Dict[str, float]:
    selected = {name: weights.get(name, 0.0) for name in feature_names}
    total = sum(value for value in selected.values() if value is not None)
    if total <= 0:
        equal_weight = 1.0 / max(len(selected), 1)
        return {name: equal_weight for name in selected}
    return {name: float(value) / total for name, value in selected.items()}


def compute_ranking_scores(
    ohlc: pd.DataFrame,
    feature_weights: Mapping[str, float],
    selected_features: Optional[Iterable[str]] = None,
) -> RankingComputationResult:
    if ohlc.empty:
        raise ValueError("OHLC dataset is empty – cannot compute ranking scores.")

    feature_names = list(selected_features) if selected_features is not None else AVAILABLE_FEATURES.names()
    weights = _normalize_weights(feature_weights, feature_names)

    feature_values: Dict[str, pd.Series] = {}
    normalized_feature_values: Dict[str, pd.Series] = {}
    for name in feature_names:
        feature = AVAILABLE_FEATURES.get(name)
        raw_values = feature.compute(ohlc).astype(float)
        feature_values[name] = raw_values
        normalized_feature_values[name] = _normalize(raw_values)

    composite_score = pd.Series(0.0, index=next(iter(feature_values.values())).index)
    for name, weight in weights.items():
        normalized = normalized_feature_values.get(name)
        if normalized is None:
            continue
        composite_score = composite_score.add(normalized * float(weight), fill_value=0.0)

    composite_score = composite_score.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return RankingComputationResult(
        scores=composite_score.astype(float),
        feature_values=feature_values,
        normalized_feature_values=normalized_feature_values,
    )
