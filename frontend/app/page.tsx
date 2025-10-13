"use client";

import React, { useMemo, useState, useEffect, useId, useCallback, useRef } from "react";
import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    Tooltip,
    ResponsiveContainer,
    CartesianGrid,
    Area,
    AreaChart,
    Legend,
    ReferenceLine,
    ReferenceDot,
    Brush,
} from "recharts";
import type { TooltipContentProps } from "recharts";
import type { CategoricalChartFunc } from "recharts/types/chart/types";
import type { MouseHandlerDataParam } from "recharts/types/synchronisation/types";
import type { BrushStartEndIndex } from "recharts/types/context/brushUpdateContext";

/** =========================
 *  API base (proxy w next.config.mjs)
 *  ========================= */
const API = "/api";

const removeUndefined = (obj: Record<string, unknown>) =>
    Object.fromEntries(
        Object.entries(obj).filter(([, value]) => value !== undefined && value !== null)
    );

const findScoreMetric = (value: string): ScoreMetricOption | undefined =>
    SCORE_METRIC_OPTIONS.find((option) => option.value === value);

type ScoreComponentRequest = {
    metric: ScoreMetricOption["backendMetric"];
    lookback_days: number;
    weight: number;
    direction: "asc" | "desc";
    label?: string;
};

const buildScoreComponents = (rules: ScoreBuilderRule[]): ScoreComponentRequest[] =>
    rules.reduce<ScoreComponentRequest[]>((acc, rule) => {
        const option = findScoreMetric(rule.metric);
        if (!option) return acc;

        const weightNumeric = Number(rule.weight);
        if (!Number.isFinite(weightNumeric) || weightNumeric <= 0) return acc;

        const direction = rule.direction === "asc" || rule.direction === "desc"
            ? rule.direction
            : option.defaultDirection;

        acc.push({
            metric: option.backendMetric,
            lookback_days: option.lookback,
            weight: Number(weightNumeric),
            direction,
            label: option.label,
        });
        return acc;
    }, []);

const createRuleId = () =>
    `rule-${Math.random().toString(36).slice(2, 8)}-${Date.now().toString(36)}`;

type ScoreMetricOption = {
    value: string;
    label: string;
    backendMetric: "total_return" | "volatility" | "max_drawdown" | "sharpe";
    lookback: number;
    defaultDirection: "asc" | "desc";
    description?: string;
};

const SCORE_METRIC_OPTIONS: ScoreMetricOption[] = [
    {
        value: "total_return_63",
        label: "Zwrot 3M (63 dni)",
        backendMetric: "total_return",
        lookback: 63,
        defaultDirection: "desc",
        description: "Zmiana ceny za ostatnie ~3 miesiące.",
    },
    {
        value: "total_return_126",
        label: "Zwrot 6M (126 dni)",
        backendMetric: "total_return",
        lookback: 126,
        defaultDirection: "desc",
        description: "Momentum półroczne na kursie zamknięcia.",
    },
    {
        value: "total_return_252",
        label: "Zwrot 12M (252 dni)",
        backendMetric: "total_return",
        lookback: 252,
        defaultDirection: "desc",
        description: "Roczna stopa zwrotu liczona na bazie kursów zamknięcia.",
    },
    {
        value: "max_drawdown_252",
        label: "Maksymalne obsunięcie 12M",
        backendMetric: "max_drawdown",
        lookback: 252,
        defaultDirection: "asc",
        description: "Najgłębsze obsunięcie kapitału w ostatnim roku (im mniejsze, tym lepiej).",
    },
    {
        value: "volatility_63",
        label: "Zmienność 3M",
        backendMetric: "volatility",
        lookback: 63,
        defaultDirection: "asc",
        description: "Odchylenie standardowe dziennych stóp zwrotu (~3 miesiące).",
    },
    {
        value: "sharpe_252",
        label: "Sharpe 12M",
        backendMetric: "sharpe",
        lookback: 252,
        defaultDirection: "desc",
        description: "Współczynnik Sharpe'a liczony na danych dziennych (ostatni rok).",
    },
];

const getDefaultScoreRules = (): ScoreBuilderRule[] => {
    const picks: { option: ScoreMetricOption; weight: number }[] = [
        { option: SCORE_METRIC_OPTIONS[2], weight: 40 },
        { option: SCORE_METRIC_OPTIONS[1], weight: 25 },
        { option: SCORE_METRIC_OPTIONS[3], weight: 20 },
        { option: SCORE_METRIC_OPTIONS[4], weight: 15 },
    ].filter((item) => item.option);

    return picks.map(({ option, weight }) => ({
        id: createRuleId(),
        metric: option.value,
        label: option.label,
        weight,
        direction: option.defaultDirection,
        transform: "raw",
    }));
};

/** =========================
 *  Typy danych
 *  ========================= */
type SymbolRow = { symbol: string; name: string };

type Row = {
    date: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
};

type RowSMA = Row & { sma?: number | null };
type RowRSI = Row & { rsi: number | null };
type PriceChartPoint = RowSMA & { change: number; changePct: number };

type ChartPeriod = 90 | 180 | 365 | 1825 | "max";

const DAY_MS = 24 * 3600 * 1000;

const computeStartISOForPeriod = (period: ChartPeriod): string => {
    if (period === "max") {
        return "1990-01-01";
    }
    const startDate = new Date(Date.now() - period * DAY_MS);
    return startDate.toISOString().slice(0, 10);
};

type Rebalance = "none" | "monthly" | "quarterly" | "yearly";

type BacktestOptions = {
    start: string;
    end?: string;
    rebalance: Rebalance;
    initialCapital?: number;
    feePct?: number;
    thresholdPct?: number;
    benchmark?: string | null;
};

type ScorePortfolioOptions = BacktestOptions & {
    score: string;
    universe?: string | string[] | null;
    limit?: number;
    weighting?: string | null;
    direction?: "asc" | "desc" | null;
    minScore?: number | null;
    maxScore?: number | null;
};

type ScoreBuilderRule = {
    id: string;
    metric: string;
    label?: string | null;
    weight: number;
    direction: "asc" | "desc";
    min?: string;
    max?: string;
    transform?: "raw" | "zscore" | "percentile" | "";
};

type ScorePreviewRulePayload = {
    metric: string;
    weight: number;
    direction: "asc" | "desc";
    label?: string;
};

type ScorePreviewRequest = {
    name?: string;
    description?: string;
    rules: ScorePreviewRulePayload[];
    limit?: number;
    universe?: string | string[] | null;
    sort?: "asc" | "desc" | null;
};

type ScorePreviewRow = {
    symbol: string;
    name?: string;
    score?: number;
    weight?: number;
    rank?: number;
    metrics?: Record<string, number>;
};

type ScorePreviewMeta = {
    asOf?: string;
    totalUniverse?: number;
    runId?: string;
    requestId?: string;
    name?: string;
};

type ScorePreviewResult = {
    rows: ScorePreviewRow[];
    meta: ScorePreviewMeta;
};

type PortfolioPoint = { date: string; value: number };
type PortfolioStats = {
    cagr: number;
    max_drawdown: number;
    volatility: number;
    sharpe: number;
    last_value: number;
    total_return?: number;
    best_year?: number;
    worst_year?: number;
    turnover?: number;
    trades?: number;
    final_value?: number;
    initial_value?: number;
};

type PortfolioAllocation = {
    symbol: string;
    target_weight: number;
    realized_weight?: number;
    return_pct?: number;
    contribution_pct?: number;
    value?: number;
};

type PortfolioRebalanceTrade = {
    symbol: string;
    action?: string;
    weight_change?: number;
    value_change?: number;
    target_weight?: number;
};

type PortfolioRebalanceEvent = {
    date: string;
    reason?: string;
    turnover?: number;
    trades?: PortfolioRebalanceTrade[];
};

type PortfolioResp = {
    equity: PortfolioPoint[];
    stats: PortfolioStats;
    allocations?: PortfolioAllocation[];
    rebalances?: PortfolioRebalanceEvent[];
    benchmark?: PortfolioPoint[];
};

/** =========================
 *  API helpers
 *  ========================= */
async function searchSymbols(q: string): Promise<SymbolRow[]> {
    const r = await fetch(`${API}/symbols?q=${encodeURIComponent(q)}`);
    if (!r.ok) throw new Error(`API /symbols ${r.status}`);
    return r.json();
}

async function fetchQuotes(symbol: string, start = "2015-01-01"): Promise<Row[]> {
    const r = await fetch(
        `${API}/quotes?symbol=${encodeURIComponent(symbol)}&start=${encodeURIComponent(
            start
        )}`
    );
    if (!r.ok) throw new Error(`API /quotes ${r.status}`);
    return r.json();
}

async function backtestPortfolio(
    symbols: string[],
    weightsPct: number[],
    options: BacktestOptions
): Promise<PortfolioResp> {
    const { start, rebalance } = options;

    const prepared = symbols
        .map((symbol, idx) => ({
            symbol: symbol?.trim(),
            weight: Number(weightsPct[idx]),
        }))
        .filter((row) => row.symbol && Number.isFinite(row.weight) && (row.weight as number) > 0);

    if (!prepared.length) {
        throw new Error(
            "Dodaj co najmniej jedną spółkę z wagą większą od zera, aby uruchomić symulację."
        );
    }

    const rawWeights = prepared.map((row) =>
        Number(row.weight) > 0 ? (row.weight as number) : 0
    );
    const totalWeight = rawWeights.reduce((sum, weight) => sum + (Number(weight) || 0), 0);
    const safeTotal = totalWeight === 0 ? 1 : totalWeight;
    const weightsRatio = rawWeights.map((weight) => (Number(weight) || 0) / safeTotal);

    const manualPayload = removeUndefined({
        symbols: prepared.map((row) => row.symbol as string),
        weights: weightsRatio,
    });

    const payload = removeUndefined({
        start,
        rebalance,
        manual: manualPayload,
    });

    const response = await fetch(`/api/backtest/portfolio`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
    });

    if (!response.ok) {
        let message = "";
        try {
            message = await response.text();
        } catch {
            // ignore
        }
        throw new Error(message || `API /backtest/portfolio ${response.status}`);
    }

    const json = await response.json();
    return normalizePortfolioResponse(json);
}


async function backtestPortfolioByScore(
    options: ScorePortfolioOptions,
    components: ScoreComponentRequest[]
): Promise<PortfolioResp> {
    const {
        score,
        universe = null,
        limit,
        weighting,
        direction,
        minScore,
        maxScore,
        start,
        end,
        rebalance,
        initialCapital,
        feePct,
        thresholdPct,
        benchmark,
    } = options;

    if (!components.length) {
        throw new Error("Skonfiguruj ranking score, aby uruchomić symulację.");
    }

    const limitCandidate =
        typeof limit === "number" && Number.isFinite(limit) && limit > 0
            ? Math.floor(limit)
            : undefined;

    const previewRules: ScorePreviewRulePayload[] = components.map((component) => ({
        metric: `${component.metric}_${component.lookback_days}`,
        weight: component.weight,
        direction: component.direction,
        label: component.label,
    }));

    const previewPayload: ScorePreviewRequest = {
        name: score && score.trim() ? score.trim() : undefined,
        rules: previewRules,
        limit: limitCandidate,
        universe,
        sort: direction ?? undefined,
    };

    const preview = await previewScoreRanking(previewPayload);

    let rows = preview.rows.slice();
    if (typeof minScore === "number") {
        rows = rows.filter((row) => row.score === undefined || row.score >= minScore);
    }
    if (typeof maxScore === "number") {
        rows = rows.filter((row) => row.score === undefined || row.score <= maxScore);
    }

    const overallDirection = direction === "asc" ? "asc" : "desc";
    rows.sort((a, b) => {
        const aScore = a.score ?? 0;
        const bScore = b.score ?? 0;
        return overallDirection === "asc" ? aScore - bScore : bScore - aScore;
    });

    const finalLimit = limitCandidate ?? rows.length;
    const topRows = rows.slice(0, finalLimit);
    if (!topRows.length) {
        throw new Error("Brak spółek spełniających kryteria score.");
    }

    const rawWeights =
        weighting === "score"
            ? topRows.map((row) => Math.max(row.score ?? 0, 0))
            : topRows.map(() => 1);

    const totalWeight = rawWeights.reduce(
        (acc, value) => acc + (Number.isFinite(value) ? (value as number) : 0),
        0
    );

    const normalizedWeights =
        totalWeight > 0
            ? rawWeights.map((value) =>
                  Number.isFinite(value) ? ((value as number) / totalWeight) || 0 : 0
              )
            : topRows.map(() => 1 / topRows.length);

    const symbols = topRows.map((row) => row.symbol);

    return backtestPortfolio(symbols, normalizedWeights, {
        start,
        end,
        rebalance,
        initialCapital,
        feePct,
        thresholdPct,
        benchmark,
    });
}


const parseNumber = (value: unknown): number | undefined => {
    if (value === null || value === undefined) return undefined;
    const num = Number(value);
    return Number.isFinite(num) ? num : undefined;
};

const getProp = (obj: unknown, key: string): unknown => {
    if (!obj || typeof obj !== "object") return undefined;
    return (obj as Record<string, unknown>)[key];
};

const pickNumber = (sources: unknown[], keys: string[]): number | undefined => {
    for (const source of sources) {
        if (!source || typeof source !== "object") continue;
        for (const key of keys) {
            const candidate = parseNumber(getProp(source, key));
            if (candidate !== undefined) {
                return candidate;
            }
        }
    }
    return undefined;
};

const normalizeEquity = (raw: unknown): PortfolioPoint[] => {
    if (!Array.isArray(raw)) return [];
    return raw
        .map((point) => {
            if (!point) return null;
            if (Array.isArray(point)) {
                if (point.length < 2) return null;
                const [dateCandidate, valueCandidate] = point;
                const value = parseNumber(valueCandidate);
                if (value === undefined) return null;
                return {
                    date: String(dateCandidate),
                    value,
                };
            }
            if (typeof point === "object") {
                const dateCandidate =
                    (point as Record<string, unknown>).date ??
                    (point as Record<string, unknown>).timestamp ??
                    (point as Record<string, unknown>).period ??
                    (point as Record<string, unknown>).time;
                const value =
                    pickNumber([point], ["value", "portfolio", "equity", "balance", "portfolio_value"]);
                if (!dateCandidate || value === undefined) return null;
                return {
                    date: String(dateCandidate),
                    value,
                };
            }
            return null;
        })
        .filter((item): item is PortfolioPoint => Boolean(item));
};

function normalizePortfolioResponse(raw: unknown): PortfolioResp {
    const equitySource =
        getProp(raw, "equity") ??
        getProp(raw, "equity_curve") ??
        getProp(raw, "portfolio") ??
        getProp(raw, "history") ??
        getProp(raw, "values");

    const equity = normalizeEquity(equitySource);

    const statsSources = [
        getProp(raw, "stats"),
        getProp(raw, "statistics"),
        getProp(raw, "metrics"),
        getProp(raw, "summary"),
        raw,
    ];

    const cagr = pickNumber(statsSources, ["cagr", "annualized_return", "annual_return", "cagr_pct"]) ?? 0;
    const maxDrawdown =
        pickNumber(statsSources, ["max_drawdown", "max_dd", "max_drawdown_pct", "worst_drawdown"]) ?? 0;
    const volatility =
        pickNumber(statsSources, ["volatility", "stdev", "std_dev", "std", "annualized_volatility"]) ?? 0;
    const sharpe = pickNumber(statsSources, ["sharpe", "sharpe_ratio"]) ?? 0;
    const lastValue =
        pickNumber(statsSources, ["last_value", "final_value", "ending_value", "last_equity"]) ??
        (equity.length ? equity[equity.length - 1].value : 0);

    const stats: PortfolioStats = {
        cagr,
        max_drawdown: maxDrawdown,
        volatility,
        sharpe,
        last_value: lastValue,
        total_return: pickNumber(statsSources, ["total_return", "return", "cumulative_return", "total_pct"]),
        best_year: pickNumber(statsSources, ["best_year", "best_year_return", "best_annual_return"]),
        worst_year: pickNumber(statsSources, ["worst_year", "worst_year_return", "worst_annual_return"]),
        turnover: pickNumber(statsSources, ["turnover", "turnover_pct", "turnover_ratio"]),
        trades: pickNumber(statsSources, ["trades", "transaction_count", "trades_count"]),
        final_value: pickNumber(statsSources, ["final_value", "ending_value", "last_value"]),
        initial_value: pickNumber(statsSources, ["initial_value", "starting_value", "start_value"]),
    };

    const benchmarkSource =
        getProp(raw, "benchmark") ??
        getProp(raw, "benchmark_equity") ??
        getProp(raw, "benchmark_history") ??
        getProp(raw, "benchmark_curve") ??
        getProp(raw, "benchmark_values");

    const benchmark = normalizeEquity(benchmarkSource);

    const allocationSource =
        getProp(raw, "allocations") ??
        getProp(raw, "allocation") ??
        getProp(raw, "allocation_summary") ??
        getProp(raw, "allocations_summary") ??
        getProp(raw, "breakdown");

    const allocationRaw = Array.isArray(allocationSource) ? allocationSource : [];

    const normalizedAllocations = allocationRaw.reduce<PortfolioAllocation[]>((acc, item) => {
        if (!item || typeof item !== "object") return acc;
        const record = item as Record<string, unknown>;
        const symbolRaw = record.symbol ?? record.ticker ?? record.name ?? record.asset ?? record.instrument;
        if (!symbolRaw) return acc;

        const normalized: PortfolioAllocation = {
            symbol: String(symbolRaw),
            target_weight: pickNumber([record], ["target_weight", "target", "weight", "allocation"]) ?? 0,
        };

        const realized = pickNumber(
            [record],
            ["realized_weight", "actual_weight", "avg_weight", "average_weight"]
        );
        if (realized !== undefined) normalized.realized_weight = realized;

        const returnPct = pickNumber([record], ["return_pct", "return", "total_return", "performance"]);
        if (returnPct !== undefined) normalized.return_pct = returnPct;

        const contribution = pickNumber(
            [record],
            ["contribution_pct", "contribution", "contrib", "contribution_share"]
        );
        if (contribution !== undefined) normalized.contribution_pct = contribution;

        const value = pickNumber([record], ["value", "ending_value", "final_value", "amount"]);
        if (value !== undefined) normalized.value = value;

        acc.push(normalized);
        return acc;
    }, []);

    const allocations: PortfolioAllocation[] | undefined = normalizedAllocations.length
        ? normalizedAllocations
        : undefined;

    const rebalanceSource =
        getProp(raw, "rebalances") ??
        getProp(raw, "rebalance_events") ??
        getProp(raw, "rebalance_log") ??
        getProp(raw, "events");

    const rebalanceRaw = Array.isArray(rebalanceSource) ? rebalanceSource : [];

    const normalizedRebalances = rebalanceRaw.reduce<PortfolioRebalanceEvent[]>((acc, event) => {
        if (!event || typeof event !== "object") return acc;
        const record = event as Record<string, unknown>;

        const dateCandidate =
            record.date ?? record.event_date ?? record.timestamp ?? record.time ?? record.period;
        if (!dateCandidate) return acc;

        const tradesRaw =
            (Array.isArray(record.trades) && record.trades) ||
            (Array.isArray(record.orders) && record.orders) ||
            (Array.isArray(record.moves) && record.moves) ||
            (Array.isArray(record.actions) && record.actions) ||
            [];

        const trades = tradesRaw.reduce<PortfolioRebalanceTrade[]>((tradeAcc, trade) => {
            if (!trade || typeof trade !== "object") return tradeAcc;
            const tradeRecord = trade as Record<string, unknown>;

            const symbolRaw =
                tradeRecord.symbol ?? tradeRecord.ticker ?? tradeRecord.asset ?? tradeRecord.name;
            if (!symbolRaw) return tradeAcc;

            const normalizedTrade: PortfolioRebalanceTrade = {
                symbol: String(symbolRaw),
            };

            const actionRaw = tradeRecord.action ?? tradeRecord.type ?? tradeRecord.side;
            if (actionRaw !== undefined) normalizedTrade.action = String(actionRaw);

            const weightChange = pickNumber(
                [tradeRecord],
                ["weight_change", "delta_weight", "weight", "change"]
            );
            if (weightChange !== undefined) normalizedTrade.weight_change = weightChange;

            const valueChange = pickNumber(
                [tradeRecord],
                ["value_change", "delta_value", "value", "amount"]
            );
            if (valueChange !== undefined) normalizedTrade.value_change = valueChange;

            const targetWeight = pickNumber(
                [tradeRecord],
                ["target_weight", "new_weight", "weight_after"]
            );
            if (targetWeight !== undefined) normalizedTrade.target_weight = targetWeight;

            tradeAcc.push(normalizedTrade);
            return tradeAcc;
        }, []);

        const normalizedEvent: PortfolioRebalanceEvent = {
            date: String(dateCandidate),
        };

        const reasonRaw = record.reason ?? record.note ?? record.description;
        if (reasonRaw !== undefined) normalizedEvent.reason = String(reasonRaw);

        const turnover = pickNumber(
            [record],
            ["turnover", "turnover_pct", "turnover_ratio", "turnover_percentage"]
        );
        if (turnover !== undefined) normalizedEvent.turnover = turnover;

        if (trades.length) {
            normalizedEvent.trades = trades;
        }

        acc.push(normalizedEvent);
        return acc;
    }, []);

    const rebalances: PortfolioRebalanceEvent[] | undefined = normalizedRebalances.length
        ? normalizedRebalances
        : undefined;

    return {
        equity,
        stats,
        allocations,
        rebalances,
        benchmark: benchmark.length ? benchmark : undefined,
    };
}

const normalizeScoreRankingResponse = (raw: unknown): ScorePreviewResult => {
    const rows: ScorePreviewRow[] = [];
    const collections: unknown[] = [];

    if (Array.isArray(raw)) {
        collections.push(raw);
    }

    const rankingSources = [
        getProp(raw, "items"),
        getProp(raw, "results"),
        getProp(raw, "ranking"),
        getProp(raw, "rows"),
        getProp(raw, "data"),
        getProp(raw, "list"),
        getProp(raw, "scores"),
    ];

    for (const candidate of rankingSources) {
        if (Array.isArray(candidate)) {
            collections.push(candidate);
        }
    }

    let ranking: unknown[] = [];
    for (const candidate of collections) {
        if (Array.isArray(candidate) && candidate.length > 0) {
            ranking = candidate;
            break;
        }
    }

    ranking.forEach((entry, index) => {
        if (!entry) return;

        if (Array.isArray(entry)) {
            let symbol = "";
            let name: string | undefined;
            const numbers: number[] = [];
            const metricBuckets: Record<string, number>[] = [];

            entry.forEach((value) => {
                if (typeof value === "string") {
                    if (!symbol) symbol = value;
                    else if (!name) name = value;
                } else if (typeof value === "number") {
                    numbers.push(value);
                } else if (value && typeof value === "object" && !Array.isArray(value)) {
                    const bucket: Record<string, number> = {};
                    Object.entries(value as Record<string, unknown>).forEach(([key, v]) => {
                        const numeric = parseNumber(v);
                        if (numeric !== undefined) {
                            bucket[key] = numeric;
                        }
                    });
                    if (Object.keys(bucket).length) {
                        metricBuckets.push(bucket);
                    }
                }
            });

            if (!symbol) return;

            const row: ScorePreviewRow = { symbol };
            if (name) row.name = name;
            if (numbers.length > 0) row.score = numbers[0];
            if (numbers.length > 1) row.weight = numbers[1];
            if (numbers.length > 2 && Number.isInteger(numbers[2])) {
                row.rank = numbers[2];
            }
            if (!row.rank) {
                row.rank = index + 1;
            }
            if (metricBuckets.length) {
                row.metrics = Object.assign({}, ...metricBuckets);
            }
            rows.push(row);
            return;
        }

        if (typeof entry === "object") {
            const record = entry as Record<string, unknown>;
            const symbolRaw =
                record.symbol ??
                record.ticker ??
                record.code ??
                record.asset ??
                record.instrument ??
                record.company_symbol;
            if (!symbolRaw) return;

            const row: ScorePreviewRow = { symbol: String(symbolRaw) };
            const nameCandidate =
                record.name ??
                record.company ??
                record.title ??
                record.label ??
                record.security;
            if (nameCandidate) row.name = String(nameCandidate);

            const score = pickNumber(
                [record, getProp(record, "score")],
                ["score", "value", "total", "points", "final_score", "composite", "ranking"]
            );
            if (score !== undefined) row.score = score;

            const weight = pickNumber(
                [record],
                [
                    "weight",
                    "allocation",
                    "target_weight",
                    "weight_pct",
                    "weight_percentage",
                    "weight_percent",
                    "position_weight",
                ]
            );
            if (weight !== undefined) row.weight = weight;

            const rank = pickNumber(
                [record],
                ["rank", "position", "order", "index", "place", "ranking_position"]
            );
            if (rank !== undefined) row.rank = rank;

            const metricSources = [
                getProp(record, "metrics"),
                getProp(record, "components"),
                getProp(record, "values"),
                getProp(record, "details"),
                getProp(record, "factors"),
            ];

            const metrics: Record<string, number> = {};
            metricSources.forEach((source) => {
                if (!source || typeof source !== "object" || Array.isArray(source)) return;
                Object.entries(source as Record<string, unknown>).forEach(([key, value]) => {
                    const numeric = parseNumber(value);
                    if (numeric !== undefined) {
                        metrics[key] = numeric;
                    }
                });
            });

            if (Object.keys(metrics).length) {
                row.metrics = metrics;
            }

            if (!row.rank) {
                row.rank = index + 1;
            }

            rows.push(row);
        }
    });

    rows.forEach((row, idx) => {
        if (typeof row.rank !== "number" || Number.isNaN(row.rank)) {
            row.rank = idx + 1;
        }
    });

    const metaSources = [getProp(raw, "meta"), getProp(raw, "summary"), raw];

    const meta: ScorePreviewMeta = {};
    const asOfCandidate =
        getProp(raw, "as_of") ??
        getProp(raw, "asAt") ??
        getProp(raw, "date") ??
        getProp(getProp(raw, "meta"), "as_of") ??
        getProp(getProp(raw, "meta"), "date");
    if (asOfCandidate) meta.asOf = String(asOfCandidate);

    const nameCandidate =
        getProp(raw, "name") ??
        getProp(raw, "title") ??
        getProp(raw, "score_name") ??
        getProp(raw, "rule_name");
    if (nameCandidate) meta.name = String(nameCandidate);

    const runIdCandidate =
        getProp(raw, "run_id") ??
        getProp(raw, "id") ??
        getProp(raw, "request_id") ??
        getProp(getProp(raw, "meta"), "run_id");
    if (runIdCandidate) meta.runId = String(runIdCandidate);

    const requestIdCandidate =
        getProp(raw, "request_id") ?? getProp(getProp(raw, "meta"), "request_id");
    if (requestIdCandidate) meta.requestId = String(requestIdCandidate);

    const totalUniverse = pickNumber(metaSources, [
        "universe_count",
        "universe_size",
        "total",
        "count",
        "universe",
        "available",
    ]);
    if (totalUniverse !== undefined) meta.totalUniverse = totalUniverse;

    return { rows, meta };
};

async function previewScoreRanking(payload: ScorePreviewRequest): Promise<ScorePreviewResult> {
    if (!payload.rules.length) {
        throw new Error("Dodaj co najmniej jedną metrykę scoringową.");
    }

    const prepared = removeUndefined({
        name: payload.name,
        description: payload.description,
        rules: payload.rules.map((rule) => ({
            metric: rule.metric,
            weight: rule.weight,
            direction: rule.direction,
            label: rule.label,
        })),
        limit: payload.limit,
        universe: payload.universe ?? undefined,
        sort: payload.sort ?? undefined,
    });

    const response = await fetch("/api/score/preview", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(prepared),
    });

    if (!response.ok) {
        throw new Error(`API /score/preview ${response.status}`);
    }

    const json = await response.json();
    return normalizeScoreRankingResponse(json);
}


/** =========================
 *  Obliczenia: SMA / RSI
 *  ========================= */
function sma(rows: Row[], w = 20): RowSMA[] {
    const out: RowSMA[] = rows.map((d, i) => {
        if (i < w - 1) return { ...d, sma: null };
        const slice = rows.slice(i - w + 1, i + 1);
        const avg = slice.reduce((a, b) => a + b.close, 0) / w;
        return { ...d, sma: Number(avg.toFixed(2)) };
    });
    return out;
}

function rsi(rows: Row[], p = 14): RowRSI[] {
    let g = 0,
        l = 0;
    const out: RowRSI[] = rows.map((d, i) => {
        if (i === 0) return { ...d, rsi: null };
        const diff = d.close - rows[i - 1].close;
        const gain = Math.max(diff, 0),
            loss = Math.max(-diff, 0);
        if (i <= p) {
            g += gain;
            l += loss;
            return { ...d, rsi: null };
        }
        if (i === p + 1) {
            g = g / p;
            l = l / p;
        } else {
            g = (g * (p - 1) + gain) / p;
            l = (l * (p - 1) + loss) / p;
        }
        const rs = l === 0 ? 100 : g / (l || 1e-9);
        return { ...d, rsi: Number((100 - 100 / (1 + rs)).toFixed(2)) };
    });
    return out;
}

/** =========================
 *  UI helpers
 *  ========================= */
const Card = ({
    title,
    right,
    children,
}: {
    title?: string;
    right?: React.ReactNode;
    children: React.ReactNode;
}) => (
    <div className="bg-surface rounded-2xl shadow-sm border border-soft">
        {(title || right) && (
            <div className="px-4 md:px-6 py-3 border-b border-soft flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                {title && <div className="font-semibold text-primary sm:flex-1">{title}</div>}
                {right && (
                    <div className="flex flex-wrap gap-2 w-full sm:w-auto sm:justify-end">{right}</div>
                )}
            </div>
        )}
        <div className="p-4 md:p-6">{children}</div>
    </div>
);

const Section = ({
    id,
    kicker,
    title,
    description,
    actions,
    children,
}: {
    id: string;
    kicker?: string;
    title: string;
    description?: string;
    actions?: React.ReactNode;
    children: React.ReactNode;
}) => (
    <section id={id} className="scroll-mt-28">
        <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
            <div className="space-y-2">
                {kicker && (
                    <span className="text-xs uppercase tracking-[0.35em] text-subtle">
                        {kicker}
                    </span>
                )}
                <div className="space-y-1">
                    <h2 className="text-2xl md:text-3xl font-semibold text-primary">{title}</h2>
                    {description && (
                        <p className="text-sm text-muted max-w-2xl">{description}</p>
                    )}
                </div>
            </div>
            {actions && <div className="flex flex-wrap gap-2">{actions}</div>}
        </div>
        <div className="mt-8">{children}</div>
    </section>
);

const SectionNav = ({
    items,
}: {
    items: { href: string; label: string }[];
}) => {
    if (!items.length) return null;
    return (
        <nav className="flex flex-wrap gap-2 text-sm">
            {items.map((item) => (
                <a
                    key={item.href}
                    href={item.href}
                    className="px-3 py-1 rounded-full border border-white/20 bg-white/10 text-white/80 hover:text-white hover:border-white/40 transition"
                >
                    {item.label}
                </a>
            ))}
        </nav>
    );
};

const Chip = ({
    active,
    onClick,
    children,
    className,
}: {
    active?: boolean;
    onClick?: () => void;
    children: React.ReactNode;
    className?: string;
}) => (
    <button
        onClick={onClick}
        className={`inline-flex items-center justify-center rounded-full px-3 py-1 text-sm text-center border transition ${
            active
                ? "bg-primary text-white border-[var(--color-primary)]"
                : "bg-surface text-muted border-soft hover:border-[var(--color-primary)] hover:text-primary"
        } ${className ?? ""}`}
    >
        {children}
    </button>
);

const inputBaseClasses =
    "rounded-xl border border-soft bg-surface px-3 py-2 text-neutral focus:outline-none focus:border-[var(--color-tech)] focus:ring-2 focus:ring-[rgba(52,152,219,0.15)]";

const toRatio = (value: number) => {
    const abs = Math.abs(value);
    if (abs === 0) return 0;
    return abs > 1 ? value / 100 : value;
};

const formatPercent = (value: number, fractionDigits = 2) =>
    `${(toRatio(value) * 100).toFixed(fractionDigits)}%`;

function PortfolioStatsGrid({ stats }: { stats: PortfolioStats }) {
    const config: { key: keyof PortfolioStats; label: string; format?: (value: number) => string }[] = [
        { key: "cagr", label: "CAGR" },
        { key: "total_return", label: "Łączna stopa zwrotu" },
        { key: "max_drawdown", label: "Max DD" },
        { key: "volatility", label: "Vol" },
        { key: "sharpe", label: "Sharpe", format: (v) => v.toFixed(2) },
        { key: "turnover", label: "Obrót" },
        { key: "trades", label: "Transakcje", format: (v) => v.toFixed(0) },
        { key: "best_year", label: "Najlepszy rok" },
        { key: "worst_year", label: "Najgorszy rok" },
        { key: "initial_value", label: "Wartość startowa", format: (v) => v.toFixed(2) },
        { key: "last_value", label: "Końcowa wartość", format: (v) => v.toFixed(2) },
        { key: "final_value", label: "Wartość końcowa", format: (v) => v.toFixed(2) },
    ];

    const items = config
        .map((item) => {
            const value = stats[item.key];
            if (typeof value !== "number" || Number.isNaN(value)) {
                return null;
            }
            const formatter = item.format ?? ((v: number) => formatPercent(v));
            return {
                label: item.label,
                display: formatter(value),
            };
        })
        .filter(Boolean) as { label: string; display: string }[];

    if (!items.length) return null;

    return (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            {items.map((item) => (
                <div key={item.label}>
                    <div className="text-subtle">{item.label}</div>
                    <div className="text-lg font-semibold">{item.display}</div>
                </div>
            ))}
        </div>
    );
}

function AllocationTable({ allocations }: { allocations: PortfolioAllocation[] }) {
    if (!allocations.length) return null;

    return (
        <div className="space-y-2">
            <div className="text-sm text-muted font-medium">Podsumowanie pozycji</div>
            <div className="overflow-x-auto">
                <table className="min-w-full text-sm">
                    <thead className="text-left text-subtle">
                        <tr className="border-b border-soft">
                            <th className="py-2 pr-4 font-medium">Symbol</th>
                            <th className="py-2 pr-4 font-medium">Waga docelowa</th>
                            <th className="py-2 pr-4 font-medium">Śr. waga</th>
                            <th className="py-2 pr-4 font-medium">Zwrot</th>
                            <th className="py-2 pr-4 font-medium">Kontrybucja</th>
                            <th className="py-2 pr-4 font-medium">Wartość końcowa</th>
                        </tr>
                    </thead>
                    <tbody>
                        {allocations.map((row, idx) => (
                            <tr key={`${row.symbol}-${idx}`} className="border-b border-soft">
                                <td className="py-2 pr-4 font-medium text-primary">{row.symbol}</td>
                                <td className="py-2 pr-4">{formatPercent(row.target_weight)}</td>
                                <td className="py-2 pr-4">
                                    {typeof row.realized_weight === "number"
                                        ? formatPercent(row.realized_weight)
                                        : "—"}
                                </td>
                                <td className="py-2 pr-4">
                                    {typeof row.return_pct === "number"
                                        ? formatPercent(row.return_pct)
                                        : "—"}
                                </td>
                                <td className="py-2 pr-4">
                                    {typeof row.contribution_pct === "number"
                                        ? formatPercent(row.contribution_pct)
                                        : "—"}
                                </td>
                                <td className="py-2 pr-4">
                                    {typeof row.value === "number" ? row.value.toFixed(2) : "—"}
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

function RebalanceTimeline({ events }: { events: PortfolioRebalanceEvent[] }) {
    if (!events.length) return null;

    return (
        <div className="space-y-3">
            <div className="text-sm text-muted font-medium">Harmonogram rebalansingu</div>
            <div className="space-y-3">
                {events.map((event, idx) => (
                    <div key={`${event.date}-${idx}`} className="rounded-xl border border-soft bg-soft-surface p-4">
                        <div className="flex flex-wrap items-center justify-between gap-2">
                            <div className="text-sm font-semibold text-primary">{event.date}</div>
                            <div className="text-xs text-subtle">
                                {event.reason || "Planowy rebalansing"}
                                {typeof event.turnover === "number"
                                    ? ` • obrót ${formatPercent(event.turnover, 1)}`
                                    : ""}
                            </div>
                        </div>
                        {event.trades && event.trades.length > 0 && (
                            <div className="mt-3 overflow-x-auto">
                                <table className="min-w-full text-xs md:text-sm">
                                    <thead className="text-left text-subtle">
                                        <tr>
                                            <th className="py-1 pr-3 font-medium">Spółka</th>
                                            <th className="py-1 pr-3 font-medium">Akcja</th>
                                            <th className="py-1 pr-3 font-medium">Zmiana wagi</th>
                                            <th className="py-1 pr-3 font-medium">Zmiana wartości</th>
                                            <th className="py-1 pr-3 font-medium">Waga docelowa</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {event.trades.map((trade, tradeIdx) => (
                                            <tr key={`${trade.symbol}-${tradeIdx}`} className="border-t border-soft">
                                                <td className="py-1 pr-3 font-medium text-primary">{trade.symbol}</td>
                                                <td className="py-1 pr-3 capitalize">{trade.action ?? "—"}</td>
                                                <td className="py-1 pr-3">
                                                    {typeof trade.weight_change === "number"
                                                        ? formatPercent(trade.weight_change, 1)
                                                        : "—"}
                                                </td>
                                                <td className="py-1 pr-3">
                                                    {typeof trade.value_change === "number"
                                                        ? trade.value_change.toFixed(2)
                                                        : "—"}
                                                </td>
                                                <td className="py-1 pr-3">
                                                    {typeof trade.target_weight === "number"
                                                        ? formatPercent(trade.target_weight, 1)
                                                        : "—"}
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        )}
                    </div>
                ))}
            </div>
        </div>
    );
}

function ScoreRankingTable({ rows }: { rows: ScorePreviewRow[] }) {
    if (!rows.length) return null;

    const metricKeys = Array.from(
        new Set(rows.flatMap((row) => Object.keys(row.metrics ?? {})))
    ).slice(0, 4);

    return (
        <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
                <thead className="text-left text-subtle">
                    <tr className="border-b border-soft">
                        <th className="py-2 pr-4 font-medium">Pozycja</th>
                        <th className="py-2 pr-4 font-medium">Spółka</th>
                        <th className="py-2 pr-4 font-medium">Score</th>
                        <th className="py-2 pr-4 font-medium">Waga</th>
                        {metricKeys.map((key) => (
                            <th key={key} className="py-2 pr-4 font-medium capitalize">
                                {key.replace(/_/g, " ")}
                            </th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {rows.map((row, idx) => (
                        <tr key={`${row.symbol}-${idx}`} className="border-b border-soft">
                            <td className="py-2 pr-4 font-medium text-subtle">#{row.rank ?? idx + 1}</td>
                            <td className="py-2 pr-4">
                                <div className="font-semibold text-primary">{row.symbol}</div>
                                {row.name && <div className="text-xs text-subtle">{row.name}</div>}
                            </td>
                            <td className="py-2 pr-4">
                                {typeof row.score === "number" ? row.score.toFixed(2) : "—"}
                            </td>
                            <td className="py-2 pr-4">
                                {typeof row.weight === "number" ? formatPercent(row.weight) : "—"}
                            </td>
                            {metricKeys.map((key) => (
                                <td key={key} className="py-2 pr-4">
                                    {row.metrics && typeof row.metrics[key] === "number"
                                        ? row.metrics[key].toFixed(2)
                                        : "—"}
                                </td>
                            ))}
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

function Watchlist({
    items,
    current,
    onPick,
    onRemove,
}: {
    items: string[];
    current: string | null;
    onPick: (s: string) => void;
    onRemove: (s: string) => void;
}) {
    if (!items.length) {
        return (
            <div className="text-sm text-subtle">
                Dodaj spółkę powyżej, aby zbudować własną listę obserwacyjną.
            </div>
        );
    }

    return (
        <div className="flex flex-wrap gap-2">
            {items.map((s) => (
                <div key={s} className="group flex items-center gap-1">
                    <Chip active={s === current} onClick={() => onPick(s)}>
                        {s}
                    </Chip>
                    <button
                        type="button"
                        onClick={() => onRemove(s)}
                        className={[
                            "opacity-0 transition-opacity",
                            "group-hover:opacity-100 group-focus-within:opacity-100 focus-visible:opacity-100",
                            "text-xl leading-none text-subtle hover:text-negative focus-visible:text-negative",
                            "px-1",
                        ].join(" ")}
                        aria-label={`Usuń ${s} z listy`}
                    >
                        ×
                    </button>
                </div>
            ))}
        </div>
    );
}

function Stats({ data }: { data: Row[] }) {
    if (!data.length) return null;
    const close = data[data.length - 1].close;
    const min = Math.min(...data.map((d) => d.close));
    const max = Math.max(...data.map((d) => d.close));
    const first = data[0].close;
    const ch = close - first;
    const chPct = (ch / first) * 100;
    return (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div>
                <div className="text-subtle">Kurs</div>
                <div className="text-xl font-semibold">{close.toFixed(2)}</div>
            </div>
            <div>
                <div className="text-subtle">Zmiana (okres)</div>
                <div
                    className={`text-xl font-semibold ${ch >= 0 ? "text-accent" : "text-negative"
                        }`}
                >
                    {ch.toFixed(2)} ({chPct.toFixed(1)}%)
                </div>
            </div>
            <div>
                <div className="text-subtle">Max</div>
                <div className="text-xl font-semibold">{max.toFixed(2)}</div>
            </div>
            <div>
                <div className="text-subtle">Min</div>
                <div className="text-xl font-semibold">{min.toFixed(2)}</div>
            </div>
        </div>
    );
}

/** =========================
 *  Komponent: Autosuggest
 *  ========================= */
function TickerAutosuggest({
    onPick,
    placeholder = "Dodaj ticker (np. CDR.WA)",
    inputClassName = "",
}: {
    onPick: (symbol: string) => void;
    placeholder?: string;
    inputClassName?: string;
}) {
    const [q, setQ] = useState("");
    const [list, setList] = useState<SymbolRow[]>([]);
    const [open, setOpen] = useState(false);
    const [idx, setIdx] = useState(-1);
    const [loading, setLoading] = useState(false);

    // debounce
    useEffect(() => {
        if (!q.trim()) {
            setList([]);
            setOpen(false);
            setIdx(-1);
            return;
        }
        setLoading(true);
        const h = setTimeout(async () => {
            try {
                const rows = await searchSymbols(q.trim());
                setList(rows);
                setOpen(true);
                setIdx(rows.length ? 0 : -1);
            } finally {
                setLoading(false);
            }
        }, 200);
        return () => clearTimeout(h);
    }, [q]);

    function choose(s: string) {
        onPick(s);
        setQ("");
        setList([]);
        setOpen(false);
        setIdx(-1);
    }

    function onKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
        if (!open || !list.length) return;
        if (e.key === "ArrowDown") {
            e.preventDefault();
            setIdx((i) => Math.min(i + 1, list.length - 1));
        } else if (e.key === "ArrowUp") {
            e.preventDefault();
            setIdx((i) => Math.max(i - 1, 0));
        } else if (e.key === "Enter") {
            e.preventDefault();
            if (idx >= 0) choose(list[idx].symbol);
        } else if (e.key === "Escape") {
            setOpen(false);
        }
    }

    return (
        <div className="relative">
            <input
                value={q}
                onChange={(e) => setQ(e.target.value)}
                onFocus={() => list.length && setOpen(true)}
                onKeyDown={onKeyDown}
                placeholder={placeholder}
                className={[
                    inputBaseClasses,
                    inputClassName || "w-56",
                ]
                    .filter(Boolean)
                    .join(" ")}
            />
            {open && (
                <div className="absolute z-20 mt-1 w-full rounded-xl border border-soft bg-surface shadow-lg max-h-72 overflow-auto">
                    {loading && (
                        <div className="px-3 py-2 text-sm text-subtle">Szukam…</div>
                    )}
                    {!loading && list.length === 0 && (
                        <div className="px-3 py-2 text-sm text-subtle">Brak wyników</div>
                    )}
                    {!loading &&
                        list.map((row, i) => (
                            <button
                                key={row.symbol}
                                onMouseDown={(e) => {
                                    e.preventDefault();
                                    choose(row.symbol);
                                }}
                                className={`w-full text-left px-3 py-2 text-sm hover:bg-[#EEF3F7] ${i === idx ? "bg-[#E3ECF5]" : ""
                                    }`}
                            >
                                <div className="flex items-center justify-between">
                                    <span className="font-medium">{row.symbol}</span>
                                    <span className="text-subtle">{row.name}</span>
                                </div>
                            </button>
                        ))}
                </div>
            )}
        </div>
    );
}

/** =========================
 *  Wykresy
 *  ========================= */
function ChartTooltipContent({
    active,
    payload,
    label,
    priceFormatter,
    percentFormatter,
    dateFormatter,
    showSMA,
}: TooltipContentProps<number, string> & {
    priceFormatter: Intl.NumberFormat;
    percentFormatter: Intl.NumberFormat;
    dateFormatter: Intl.DateTimeFormat;
    showSMA: boolean;
}) {
    const point = active && payload?.length ? (payload[0]?.payload as PriceChartPoint) : null;

    if (!active || !point || !label) return null;

    const formattedDate = dateFormatter.format(new Date(label));
    const isZeroChange = Math.abs(point.change) < 1e-10;
    const changeColor = isZeroChange
        ? "text-subtle"
        : point.change > 0
            ? "text-accent"
            : "text-negative";
    const changeSign = point.change > 0 ? "+" : point.change < 0 ? "-" : "";
    const changeAbs = priceFormatter.format(Math.abs(point.change));
    const changePct = percentFormatter.format(Math.abs(point.changePct));
    const changeText = isZeroChange ? priceFormatter.format(0) : `${changeSign}${changeAbs}`;
    const changePctText = isZeroChange ? percentFormatter.format(0) : `${changeSign}${changePct}`;

    return (
        <div className="rounded-xl border border-soft bg-white/95 px-4 py-3 text-xs shadow-xl backdrop-blur">
            <div className="font-medium uppercase tracking-wide text-subtle">{formattedDate}</div>
            <div className="mt-2 text-lg font-semibold text-neutral">
                {priceFormatter.format(point.close)}
            </div>
            <div className={`mt-1 font-semibold ${changeColor}`}>
                {changeText} ({changePctText}%)
            </div>
            {showSMA && typeof point.sma === "number" && (
                <div className="mt-2 text-[11px] text-muted">SMA 20: {priceFormatter.format(point.sma)}</div>
            )}
        </div>
    );
}

function PriceChart({
    rows,
    showArea,
    showSMA,
    brushDataRows,
    brushRange,
    onBrushChange,
}: {
    rows: RowSMA[];
    showArea: boolean;
    showSMA: boolean;
    brushDataRows?: RowSMA[];
    brushRange?: BrushStartEndIndex | null;
    onBrushChange?: (range: BrushStartEndIndex) => void;
}) {
    const gradientId = useId();
    const priceFormatter = useMemo(
        () =>
            new Intl.NumberFormat("pl-PL", {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
            }),
        []
    );
    const percentFormatter = useMemo(
        () =>
            new Intl.NumberFormat("pl-PL", {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
            }),
        []
    );
    const axisDateFormatter = useMemo(
        () =>
            new Intl.DateTimeFormat("pl-PL", {
                month: "short",
                year: "numeric",
            }),
        []
    );
    const tooltipDateFormatter = useMemo(
        () =>
            new Intl.DateTimeFormat("pl-PL", {
                day: "numeric",
                month: "long",
                year: "numeric",
            }),
        []
    );

    const chartData: PriceChartPoint[] = useMemo(() => {
        if (!rows.length) return [];
        const base = rows[0].close || 0;
        return rows.map((row) => {
            const change = row.close - base;
            const changePct = base !== 0 ? (change / base) * 100 : 0;
            return { ...row, change, changePct };
        });
    }, [rows]);

    const brushChartData: PriceChartPoint[] | null = useMemo(() => {
        if (!brushDataRows?.length) return null;
        const base = brushDataRows[0].close || 0;
        return brushDataRows.map((row) => {
            const change = row.close - base;
            const changePct = base !== 0 ? (change / base) * 100 : 0;
            return { ...row, change, changePct };
        });
    }, [brushDataRows]);

    const showBrushControls = Boolean(brushChartData && brushChartData.length > 1 && onBrushChange);

    const latestPoint = chartData.at(-1) ?? null;
    const isGrowing =
        (latestPoint?.close ?? 0) >= (chartData[0]?.close ?? latestPoint?.close ?? 0);
    const primaryColor = isGrowing ? "#1DB954" : "#EA4335";
    const strokeColor = isGrowing ? "#0B8F47" : "#C5221F";
    const axisTickFormatter = useCallback(
        (value: string | number) => {
            if (typeof value !== "string" && typeof value !== "number") return String(value ?? "");
            return axisDateFormatter.format(new Date(value));
        },
        [axisDateFormatter]
    );

    const brushDateFormatter = useMemo(
        () =>
            new Intl.DateTimeFormat("pl-PL", {
                year: "numeric",
            }),
        []
    );

    const brushTickFormatter = useCallback(
        (value: string | number) => {
            if (typeof value !== "string" && typeof value !== "number") return "";
            return brushDateFormatter.format(new Date(value));
        },
        [brushDateFormatter]
    );

    const yTickFormatter = useCallback(
        (value: number) => priceFormatter.format(value),
        [priceFormatter]
    );

    const handleBrushUpdate = useCallback(
        (range: BrushStartEndIndex) => {
            onBrushChange?.(range);
        },
        [onBrushChange]
    );

    type ChartMouseState = MouseHandlerDataParam & {
        activePayload?: Array<{ payload?: PriceChartPoint }>;
        chartX?: number;
        chartY?: number;
    };

    type SelectionPoint = {
        point: PriceChartPoint;
        x: number;
    };

    const [selection, setSelection] = useState<{
        start: SelectionPoint;
        end: SelectionPoint;
    } | null>(null);
    const [isSelecting, setIsSelecting] = useState(false);

    const chartContainerRef = useRef<HTMLDivElement | null>(null);
    const [containerWidth, setContainerWidth] = useState(0);

    useEffect(() => {
        const node = chartContainerRef.current;
        if (!node) return;

        const updateSize = () => {
            const width = node.getBoundingClientRect().width;
            if (Number.isFinite(width)) {
                setContainerWidth(width);
            }
        };

        updateSize();

        if (typeof ResizeObserver === "undefined") {
            window.addEventListener("resize", updateSize);
            return () => {
                window.removeEventListener("resize", updateSize);
            };
        }

        const observer = new ResizeObserver(() => updateSize());
        observer.observe(node);

        return () => {
            observer.disconnect();
        };
    }, []);

    const getPointFromState = useCallback(
        (state: MouseHandlerDataParam): SelectionPoint | null => {
            const payload = (state as ChartMouseState)?.activePayload?.[0]?.payload;
            const chartX = (state as ChartMouseState)?.chartX;

            if (
                payload &&
                typeof payload === "object" &&
                "close" in payload &&
                typeof chartX === "number"
            ) {
                return { point: payload as PriceChartPoint, x: chartX };
            }
            return null;
        },
        []
    );

    const updateSelectionEnd = useCallback((nextPoint: SelectionPoint) => {
        setSelection((current) => (current ? { start: current.start, end: nextPoint } : current));
    }, []);

    const handleChartMouseDown = useCallback<CategoricalChartFunc>(
        (state) => {
            if (!state) return;
            const point = getPointFromState(state as MouseHandlerDataParam);
            if (!point) return;
            setSelection({ start: point, end: point });
            setIsSelecting(true);
        },
        [getPointFromState]
    );

    const handleChartMouseMove = useCallback<CategoricalChartFunc>(
        (state) => {
            if (!state) return;
            if (!isSelecting && !selection) return;
            const point = getPointFromState(state as MouseHandlerDataParam);
            if (!point) return;
            updateSelectionEnd(point);
        },
        [getPointFromState, isSelecting, selection, updateSelectionEnd]
    );

    const handleChartMouseUp = useCallback<CategoricalChartFunc>(
        (state) => {
            if (!isSelecting) return;
            if (!state) return;
            const point = getPointFromState(state as MouseHandlerDataParam);
            if (point) {
                updateSelectionEnd(point);
            }
            setIsSelecting(false);
        },
        [getPointFromState, isSelecting, updateSelectionEnd]
    );

    const handleChartMouseLeave = useCallback(() => {
        setIsSelecting(false);
    }, []);

    useEffect(() => {
        setSelection(null);
        setIsSelecting(false);
    }, [rows]);

    const selectionStart = selection?.start ?? null;
    const selectionEnd = selection?.end ?? null;
    const selectionStartPoint = selectionStart?.point ?? null;
    const selectionEndPoint = selectionEnd?.point ?? null;
    const hasSelection = Boolean(selectionStart && selectionEnd);
    const selectionChange =
        hasSelection && selectionStartPoint && selectionEndPoint
            ? selectionEndPoint.close - selectionStartPoint.close
            : 0;
    const selectionBase = selectionStartPoint?.close ?? 0;
    const selectionPct = selectionBase !== 0 ? (selectionChange / selectionBase) * 100 : 0;
    const selectionIsZero = Math.abs(selectionChange) < 1e-10;
    const selectionSign = selectionChange > 0 ? "+" : selectionChange < 0 ? "-" : "";
    const selectionClass = selectionIsZero
        ? "text-subtle"
        : selectionChange > 0
            ? "text-accent"
            : "text-negative";
    const selectionChangeText = selectionIsZero
        ? priceFormatter.format(0)
        : `${selectionSign}${priceFormatter.format(Math.abs(selectionChange))}`;
    const selectionPctText = selectionIsZero
        ? percentFormatter.format(0)
        : `${selectionSign}${percentFormatter.format(Math.abs(selectionPct))}`;
    const selectionStartLabel = selectionStartPoint
        ? tooltipDateFormatter.format(new Date(selectionStartPoint.date))
        : "";
    const selectionEndLabel = selectionEndPoint
        ? tooltipDateFormatter.format(new Date(selectionEndPoint.date))
        : "";
    const selectionStartPrice = selectionStartPoint ? priceFormatter.format(selectionStartPoint.close) : "";
    const selectionEndPrice = selectionEndPoint ? priceFormatter.format(selectionEndPoint.close) : "";
    const selectionColor = selectionIsZero
        ? "#94A3B8"
        : selectionChange > 0
            ? "#1DB954"
            : "#EA4335";
    const tooltipLeft = selectionEnd?.x ?? null;
    const tooltipLeftClamped =
        tooltipLeft !== null && containerWidth > 0
            ? Math.min(Math.max(tooltipLeft, 72), containerWidth - 72)
            : tooltipLeft;

    const priceLine = (
        <Line
            type="monotone"
            dataKey="close"
            stroke={strokeColor}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 4, strokeWidth: 2, stroke: "#fff" }}
            isAnimationActive={false}
        />
    );
    const smaLine =
        showSMA && (
            <Line
                type="monotone"
                dataKey="sma"
                stroke="#0A2342"
                strokeWidth={1.5}
                dot={false}
                strokeDasharray="4 4"
                isAnimationActive={false}
            />
        );

    return (
        <div className="space-y-4">
            <div ref={chartContainerRef} className="relative h-80">
                {hasSelection && selectionStartPoint && selectionEndPoint && tooltipLeftClamped !== null && (
                    <div
                        className="pointer-events-none absolute top-3 z-10 max-w-xs -translate-x-1/2 rounded-lg border border-soft bg-white/95 px-3 py-2 text-xs shadow backdrop-blur"
                        style={{ left: tooltipLeftClamped }}
                    >
                        <div className={`font-semibold ${selectionClass}`}>
                            {selectionChangeText} ({selectionPctText}%)
                        </div>
                        <div className="mt-1 space-y-1 text-[11px] text-muted">
                            <div className="flex items-center justify-between gap-3">
                                <span>Start</span>
                                <span className="font-medium text-foreground">{selectionStartPrice}</span>
                            </div>
                            <div className="flex items-center justify-between gap-3">
                                <span>Koniec</span>
                                <span className="font-medium text-foreground">{selectionEndPrice}</span>
                            </div>
                            <div>
                                {selectionStartLabel}
                                {selectionStartLabel && selectionEndLabel ? " → " : ""}
                                {selectionEndLabel}
                            </div>
                        </div>
                    </div>
                )}
                <ResponsiveContainer width="100%" height="100%">
                    {showArea ? (
                        <AreaChart
                            data={chartData}
                            margin={{ top: 10, right: 16, left: 0, bottom: 0 }}
                            onMouseDown={handleChartMouseDown}
                            onMouseMove={handleChartMouseMove}
                            onMouseUp={handleChartMouseUp}
                            onMouseLeave={handleChartMouseLeave}
                        >
                            <defs>
                                <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="0%" stopColor={primaryColor} stopOpacity={0.3} />
                                    <stop offset="95%" stopColor={primaryColor} stopOpacity={0} />
                                </linearGradient>
                            </defs>
                            <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" vertical={false} />
                            <XAxis
                                dataKey="date"
                                tick={{ fontSize: 11 }}
                                tickMargin={10}
                                axisLine={false}
                                tickLine={false}
                                minTickGap={24}
                                tickFormatter={axisTickFormatter}
                            />
                            <YAxis
                                width={70}
                                tick={{ fontSize: 11 }}
                                axisLine={false}
                                tickLine={false}
                                tickFormatter={yTickFormatter}
                                domain={["auto", "auto"]}
                            />
                            <Tooltip<number, string>
                                cursor={{ stroke: strokeColor, strokeOpacity: 0.2, strokeWidth: 1 }}
                                content={(tooltipProps) => (
                                    <ChartTooltipContent
                                        {...tooltipProps}
                                        priceFormatter={priceFormatter}
                                        percentFormatter={percentFormatter}
                                        dateFormatter={tooltipDateFormatter}
                                        showSMA={Boolean(showSMA)}
                                    />
                                )}
                                wrapperStyle={{ outline: "none" }}
                                position={{ y: 24 }}
                            />
                            <Area
                                type="monotone"
                                dataKey="close"
                                stroke={strokeColor}
                                strokeWidth={2}
                                fill={`url(#${gradientId})`}
                                fillOpacity={1}
                                isAnimationActive={false}
                            />
                            {hasSelection && selectionStartPoint && selectionEndPoint && (
                                <>
                                    <ReferenceLine x={selectionStartPoint.date} stroke="#CBD5F0" strokeDasharray="4 4" />
                                    <ReferenceLine x={selectionEndPoint.date} stroke="#CBD5F0" strokeDasharray="4 4" />
                                    <ReferenceDot
                                        x={selectionStartPoint.date}
                                        y={selectionStartPoint.close}
                                        r={4}
                                        fill={selectionColor}
                                        stroke="#ffffff"
                                        strokeWidth={2}
                                    />
                                    <ReferenceDot
                                        x={selectionEndPoint.date}
                                        y={selectionEndPoint.close}
                                        r={4}
                                        fill={selectionColor}
                                        stroke="#ffffff"
                                        strokeWidth={2}
                                    />
                                </>
                            )}
                            {priceLine}
                            {smaLine}
                        </AreaChart>
                    ) : (
                        <LineChart
                            data={chartData}
                            margin={{ top: 10, right: 16, left: 0, bottom: 0 }}
                            onMouseDown={handleChartMouseDown}
                            onMouseMove={handleChartMouseMove}
                            onMouseUp={handleChartMouseUp}
                            onMouseLeave={handleChartMouseLeave}
                        >
                            <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" vertical={false} />
                            <XAxis
                                dataKey="date"
                                tick={{ fontSize: 11 }}
                                tickMargin={10}
                                axisLine={false}
                                tickLine={false}
                                minTickGap={24}
                                tickFormatter={axisTickFormatter}
                            />
                            <YAxis
                                width={70}
                                tick={{ fontSize: 11 }}
                                axisLine={false}
                                tickLine={false}
                                tickFormatter={yTickFormatter}
                                domain={["auto", "auto"]}
                            />
                            <Tooltip<number, string>
                                cursor={{ stroke: strokeColor, strokeOpacity: 0.2, strokeWidth: 1 }}
                                content={(tooltipProps) => (
                                    <ChartTooltipContent
                                        {...tooltipProps}
                                        priceFormatter={priceFormatter}
                                        percentFormatter={percentFormatter}
                                        dateFormatter={tooltipDateFormatter}
                                        showSMA={Boolean(showSMA)}
                                    />
                                )}
                                wrapperStyle={{ outline: "none" }}
                                position={{ y: 24 }}
                            />
                            {hasSelection && selectionStartPoint && selectionEndPoint && (
                                <>
                                    <ReferenceLine x={selectionStartPoint.date} stroke="#CBD5F0" strokeDasharray="4 4" />
                                    <ReferenceLine x={selectionEndPoint.date} stroke="#CBD5F0" strokeDasharray="4 4" />
                                    <ReferenceDot
                                        x={selectionStartPoint.date}
                                        y={selectionStartPoint.close}
                                        r={4}
                                        fill={selectionColor}
                                        stroke="#ffffff"
                                        strokeWidth={2}
                                    />
                                    <ReferenceDot
                                        x={selectionEndPoint.date}
                                        y={selectionEndPoint.close}
                                        r={4}
                                        fill={selectionColor}
                                        stroke="#ffffff"
                                        strokeWidth={2}
                                    />
                                </>
                            )}
                            {priceLine}
                            {smaLine}
                        </LineChart>
                    )}
                </ResponsiveContainer>
            </div>
            {showBrushControls && brushChartData && (
                <div className="h-24 rounded-lg border border-soft bg-surface px-2 py-2">
                    <ResponsiveContainer width="100%" height="100%">
                        <AreaChart data={brushChartData} margin={{ top: 4, right: 16, left: 0, bottom: 0 }}>
                            <XAxis
                                dataKey="date"
                                tickFormatter={brushTickFormatter}
                                tick={{ fontSize: 10, fill: "#64748B" }}
                                axisLine={false}
                                tickLine={false}
                                minTickGap={32}
                            />
                            <YAxis hide domain={["auto", "auto"]} />
                            <Area
                                type="monotone"
                                dataKey="close"
                                stroke={strokeColor}
                                fill={primaryColor}
                                fillOpacity={0.15}
                                isAnimationActive={false}
                                dot={false}
                            />
                            <Brush
                                dataKey="date"
                                height={22}
                                travellerWidth={10}
                                stroke="#2563EB"
                                fill="#E2E8F0"
                                startIndex={brushRange?.startIndex}
                                endIndex={brushRange?.endIndex}
                                onChange={handleBrushUpdate}
                                onDragEnd={handleBrushUpdate}
                            />
                        </AreaChart>
                    </ResponsiveContainer>
                </div>
            )}
        </div>
    );
}

function RsiChart({ rows }: { rows: RowRSI[] }) {
    return (
        <div className="h-40">
            <ResponsiveContainer width="100%" height="100%">
                <LineChart data={rows}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#BDC3C7" />
                    <XAxis dataKey="date" tick={{ fontSize: 12 }} tickMargin={8} />
                    <YAxis domain={[0, 100]} tick={{ fontSize: 12 }} width={40} />
                    <Tooltip />
                    <Line type="monotone" dataKey="rsi" stroke="#0A2342" dot={false} />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}

/** =========================
 *  Strona główna
 *  ========================= */
export default function Page() {
    const [watch, setWatch] = useState<string[]>([]);
    const [symbol, setSymbol] = useState<string | null>(null);
    const [period, setPeriod] = useState<ChartPeriod>(365);
    const [area, setArea] = useState(true);
    const [smaOn, setSmaOn] = useState(true);

    const [rows, setRows] = useState<Row[]>([]);
    const [allRows, setAllRows] = useState<Row[]>([]);
    const [brushRange, setBrushRange] = useState<BrushStartEndIndex | null>(null);
    const [loading, setLoading] = useState(false);
    const [err, setErr] = useState("");

    // Score builder
    const [scoreRules, setScoreRules] = useState<ScoreBuilderRule[]>(() => getDefaultScoreRules());
    const [scoreNameInput, setScoreNameInput] = useState("custom_quality");
    const [scoreDescription, setScoreDescription] = useState("Ranking jakościowy – przykład");
    const [scoreLimit, setScoreLimit] = useState(10);
    const [scoreSort, setScoreSort] = useState<"asc" | "desc">("desc");
    const [scoreUniverse, setScoreUniverse] = useState("WIG20.WA");
    const [scoreAsOf, setScoreAsOf] = useState(() => new Date().toISOString().slice(0, 10));
    const [scoreMinMcap, setScoreMinMcap] = useState("");
    const [scoreMinTurnover, setScoreMinTurnover] = useState("");
    const [scoreResults, setScoreResults] = useState<ScorePreviewResult | null>(null);
    const [scoreLoading, setScoreLoading] = useState(false);
    const [scoreError, setScoreError] = useState("");

    const scoreComponents = useMemo(() => buildScoreComponents(scoreRules), [scoreRules]);
    const scoreTotalWeight = scoreComponents.reduce((acc, component) => acc + component.weight, 0);
    const scoreLimitInvalid = !Number.isFinite(scoreLimit) || scoreLimit <= 0;
    const scoreDisabled = scoreLoading || scoreLimitInvalid || !scoreComponents.length;

    // Portfel
    const [pfMode, setPfMode] = useState<"manual" | "score">("manual");
    const [pfRows, setPfRows] = useState<{ symbol: string; weight: number }[]>([
        { symbol: "CDR.WA", weight: 40 },
        { symbol: "ORLEN.WA", weight: 30 },
        { symbol: "PKO.WA", weight: 30 },
    ]);
    const [pfStart, setPfStart] = useState("2015-01-01");
    const [pfEnd, setPfEnd] = useState(() => new Date().toISOString().slice(0, 10));
    const [pfInitial, setPfInitial] = useState(10000);
    const [pfFee, setPfFee] = useState(0);
    const [pfThreshold, setPfThreshold] = useState(0);
    const [pfBenchmark, setPfBenchmark] = useState<string | null>(null);
    const [pfLastBenchmark, setPfLastBenchmark] = useState<string | null>(null);
    const [pfFreq, setPfFreq] = useState<Rebalance>("monthly");
    const [pfRes, setPfRes] = useState<PortfolioResp | null>(null);
    const [pfScoreName, setPfScoreName] = useState("quality_score");
    const [pfScoreLimit, setPfScoreLimit] = useState(10);
    const [pfScoreWeighting, setPfScoreWeighting] = useState("equal");
    const [pfScoreDirection, setPfScoreDirection] = useState<"asc" | "desc">("desc");
    const [pfScoreUniverse, setPfScoreUniverse] = useState("");
    const [pfScoreMin, setPfScoreMin] = useState("");
    const [pfScoreMax, setPfScoreMax] = useState("");
    const pfTotal = pfRows.reduce((a, b) => a + (Number(b.weight) || 0), 0);
    const pfRangeInvalid = pfStart > pfEnd;
    const [pfLoading, setPfLoading] = useState(false);
    const [pfErr, setPfErr] = useState("");
    const pfHasInvalidWeights = pfRows.some((row) => Number(row.weight) < 0 || Number.isNaN(Number(row.weight)));
    const pfHasMissingSymbols = pfRows.some(
        (row) => Number(row.weight) > 0 && (!row.symbol || !row.symbol.trim())
    );
    const pfHasValidPositions = pfRows.some((row) => row.symbol && Number(row.weight) > 0);
    const pfDisableManualSimulation =
        pfLoading ||
        pfInitial <= 0 ||
        pfRangeInvalid ||
        pfHasInvalidWeights ||
        pfHasMissingSymbols ||
        !pfHasValidPositions;
    const pfScoreNameInvalid = !pfScoreName.trim();
    const pfScoreLimitInvalid = !Number.isFinite(pfScoreLimit) || pfScoreLimit <= 0;
    const pfDisableScoreSimulation =
        pfLoading || pfInitial <= 0 || pfRangeInvalid || pfScoreNameInvalid || pfScoreLimitInvalid;
    const pfDisableSimulation =
        pfMode === "manual" ? pfDisableManualSimulation : pfDisableScoreSimulation;

    // Quotes loader
    useEffect(() => {
        let live = true;
        if (!symbol) {
            setRows([]);
            setAllRows([]);
            setBrushRange(null);
            setErr("");
            setLoading(false);
            return;
        }

        (async () => {
            try {
                setLoading(true);
                setErr("");
                const startISO = computeStartISOForPeriod(period);
                const data = await fetchQuotes(symbol, startISO);
                if (live) {
                    setAllRows(data);
                    setRows(data);
                    setBrushRange(null);
                }
            } catch (e: unknown) {
                const message = e instanceof Error ? e.message : String(e);
                if (live) {
                    setErr(message);
                    setRows([]);
                    setAllRows([]);
                    setBrushRange(null);
                }
            } finally {
                if (live) setLoading(false);
            }
        })();
        return () => {
            live = false;
        };
    }, [symbol, period]);

    const withSma: RowSMA[] = useMemo(
        () => (smaOn ? sma(rows, 20) : rows.map((r) => ({ ...r, sma: undefined }))),
        [rows, smaOn]
    );
    const withRsi: RowRSI[] = useMemo(() => rsi(rows, 14), [rows]);

    const brushRows: RowSMA[] = useMemo(
        () => allRows.map((row) => ({ ...row, sma: null })),
        [allRows]
    );

    const handleBrushSelectionChange = useCallback(
        (range: BrushStartEndIndex) => {
            if (period !== "max" || !allRows.length) return;
            const safeStart = Math.max(0, Math.min(range.startIndex, allRows.length - 1));
            const safeEnd = Math.max(safeStart, Math.min(range.endIndex, allRows.length - 1));
            setBrushRange((current) => {
                if (current && current.startIndex === safeStart && current.endIndex === safeEnd) {
                    return current;
                }
                return { startIndex: safeStart, endIndex: safeEnd };
            });
            setRows(allRows.slice(safeStart, safeEnd + 1));
        },
        [allRows, period]
    );

    const symbolLabel = symbol ?? "—";
    const navItems = [
        { href: "#watchlist", label: "Watchlista" },
        { href: "#analysis", label: "Analiza techniczna" },
        { href: "#score", label: "Ranking score" },
        { href: "#portfolio", label: "Symulacja portfela" },
    ];

    const pfChartData = useMemo(() => {
        if (!pfRes) return [];

        const merged = new Map<
            string,
            { date: string; portfolio?: number; benchmark?: number }
        >();

        for (const point of pfRes.equity) {
            merged.set(point.date, { date: point.date, portfolio: point.value });
        }

        const benchmarkSeries = pfRes.benchmark ?? [];
        for (const point of benchmarkSeries) {
            const existing = merged.get(point.date);
            if (existing) {
                existing.benchmark = point.value;
            } else {
                merged.set(point.date, { date: point.date, benchmark: point.value });
            }
        }

        return Array.from(merged.values()).sort((a, b) => a.date.localeCompare(b.date));
    }, [pfRes]);

    const parseUniverseValue = (value: string): string | string[] | null => {
        const trimmed = value.trim();
        if (!trimmed) return null;
        const tokens = trimmed
            .split(/[\s,;]+/)
            .map((token) => token.trim())
            .filter(Boolean);
        if (!tokens.length) return null;
        return tokens.length === 1 ? tokens[0] : tokens;
    };

    const addScoreRule = () => {
        const defaultOption = SCORE_METRIC_OPTIONS[0];
        setScoreRules((prev) => [
            ...prev,
            {
                id: createRuleId(),
                metric: defaultOption?.value ?? "",
                weight: 10,
                direction: defaultOption?.defaultDirection ?? "desc",
                transform: "raw",
                label: defaultOption?.label,
            },
        ]);
    };

    const removeScoreRule = (id: string) => {
        setScoreRules((prev) => prev.filter((rule) => rule.id !== id));
    };

    const resetScoreBuilder = () => {
        setScoreRules(getDefaultScoreRules());
        setScoreNameInput("custom_quality");
        setScoreDescription("Ranking jakościowy – przykład");
        setScoreLimit(10);
        setScoreSort("desc");
        setScoreUniverse("WIG20.WA");
        setScoreAsOf(new Date().toISOString().slice(0, 10));
        setScoreMinMcap("");
        setScoreMinTurnover("");
        setScoreResults(null);
        setScoreError("");
    };

    const handleScorePreview = async () => {
        try {
            setScoreError("");
            setScoreLoading(true);
            setScoreResults(null);

            const componentsForRequest = scoreComponents;
            if (!componentsForRequest.length) {
                throw new Error("Dodaj co najmniej jedną metrykę score z dodatnią wagą.");
            }

            const rulePayload: ScorePreviewRulePayload[] = componentsForRequest.map((component) => ({
                metric: `${component.metric}_${component.lookback_days}`,
                weight: component.weight,
                direction: component.direction,
                label: component.label,
            }));

            const limitValue = !scoreLimitInvalid && Number.isFinite(scoreLimit)
                ? Math.floor(Number(scoreLimit))
                : undefined;

            const payload: ScorePreviewRequest = {
                name: scoreNameInput.trim() || undefined,
                description: scoreDescription.trim() || undefined,
                rules: rulePayload,
                limit: limitValue,
                universe: parseUniverseValue(scoreUniverse),
                sort: scoreSort,
            };

            const result = await previewScoreRanking(payload);
            setScoreResults(result);
        } catch (e: unknown) {
            const message = e instanceof Error ? e.message : String(e);
            setScoreError(message);
        } finally {
            setScoreLoading(false);
        }
    };

    const runPortfolioSimulation = async () => {
        try {
            setPfErr("");
            setPfLoading(true);
            setPfRes(null);

            if (pfMode === "manual") {
                const symbols = pfRows.map((r2) => r2.symbol);
                const weights = pfRows.map((r2) => Number(r2.weight));
                const res = await backtestPortfolio(symbols, weights, {
                    start: pfStart,
                    end: pfEnd,
                    rebalance: pfFreq,
                    initialCapital: pfInitial,
                    feePct: pfFee,
                    thresholdPct: pfThreshold,
                    benchmark: pfBenchmark,
                });
                setPfRes(res);
            } else {
                const parseOptionalNumber = (value: string): number | null => {
                    if (!value.trim()) return null;
                    const numeric = Number(value);
                    return Number.isFinite(numeric) ? numeric : null;
                };

                const universeCandidates = pfScoreUniverse
                    .split(/[\s,;]+/)
                    .map((item) => item.trim())
                    .filter(Boolean);
                const universeValue =
                    universeCandidates.length === 0
                        ? null
                        : universeCandidates.length === 1
                        ? universeCandidates[0]
                        : universeCandidates;

                const componentsForScore = scoreComponents;
                if (!componentsForScore.length) {
                    throw new Error("Skonfiguruj ranking score, aby uruchomić symulację.");
                }

                const res = await backtestPortfolioByScore(
                    {
                        score: pfScoreName.trim(),
                        limit: pfScoreLimitInvalid ? undefined : Math.floor(pfScoreLimit),
                        weighting: pfScoreWeighting,
                        direction: pfScoreDirection,
                        universe: universeValue,
                        minScore: parseOptionalNumber(pfScoreMin),
                        maxScore: parseOptionalNumber(pfScoreMax),
                        start: pfStart,
                        end: pfEnd,
                        rebalance: pfFreq,
                        initialCapital: pfInitial,
                        feePct: pfFee,
                        thresholdPct: pfThreshold,
                        benchmark: pfBenchmark,
                    },
                    componentsForScore
                );
                setPfRes(res);
            }

            setPfLastBenchmark(pfBenchmark);
        } catch (e: unknown) {
            const message = e instanceof Error ? e.message : String(e);
            setPfErr(message);
        } finally {
            setPfLoading(false);
        }
    };

    const removeFromWatch = (sym: string) => {
        setWatch((prev) => {
            if (!prev.includes(sym)) {
                return prev;
            }
            const next = prev.filter((item) => item !== sym);
            if (symbol === sym) {
                setSymbol(next.length ? next[0] : null);
            }
            return next;
        });
    };

    return (
        <div className="min-h-screen bg-page text-neutral">
            <header className="border-b border-soft bg-primary text-white">
                <div className="max-w-6xl mx-auto px-4 md:px-8 py-10 space-y-8">
                    <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
                        <div className="space-y-3">
                            <span className="text-xs uppercase tracking-[0.35em] text-white/70">Panel demo</span>
                            <h1 className="text-3xl md:text-4xl font-bold text-white">Analityka Rynków</h1>
                            <p className="max-w-2xl text-white/80">
                                Zbieraj notowania, konfiguruj score i sprawdzaj portfel w jednym miejscu połączonym z
                                backendem.
                            </p>
                        </div>
                        <div className="flex items-center gap-3 self-start md:self-auto">
                            <button className="px-4 py-2 rounded-xl border border-white/40 text-white hover:bg-white/10">
                                Zaloguj
                            </button>
                            <button className="px-4 py-2 rounded-xl bg-accent text-primary transition hover:bg-[#27AE60]">
                                Utwórz konto
                            </button>
                        </div>
                    </div>
                    <SectionNav items={navItems} />
                </div>
            </header>

            <main className="max-w-6xl mx-auto px-4 md:px-8 py-12 space-y-16">
                <Section
                    id="watchlist"
                    kicker="Krok 1"
                    title="Monitoruj swoje spółki"
                    description="Dodawaj tickery z GPW, aby szybko przełączać wykresy oraz sekcje analityczne na stronie."
                    actions={
                        <TickerAutosuggest
                            onPick={(sym) => {
                                setWatch((w) => (w.includes(sym) ? w : [sym, ...w]));
                                setSymbol(sym);
                            }}
                        />
                    }
                >
                    <Card>
                        <div className="space-y-4">
                            <p className="text-sm text-muted">
                                Kliknij na ticker, aby przełączyć moduły poniżej. Usuń zbędne pozycje przyciskiem ×.
                            </p>
                            <Watchlist
                                items={watch}
                                current={symbol}
                                onPick={(sym) => setSymbol(sym)}
                                onRemove={removeFromWatch}
                            />
                        </div>
                    </Card>
                </Section>

                <Section
                    id="analysis"
                    kicker="Krok 2"
                    title="Analiza techniczna i kontekst"
                    description="Przeglądaj kluczowe statystyki, wykres cenowy oraz wskaźniki momentum, a obok miej szybki podgląd fundamentów."
                >
                    <div className="grid md:grid-cols-3 gap-6">
                        <div className="md:col-span-2 space-y-6">
                            <Card
                                title={symbol ? `${symbol} – wykres cenowy` : "Wykres cenowy"}
                                right={
                                    <>
                                        <Chip active={period === 90} onClick={() => setPeriod(90)}>
                                            3M
                                        </Chip>
                                        <Chip active={period === 180} onClick={() => setPeriod(180)}>
                                            6M
                                        </Chip>
                                        <Chip active={period === 365} onClick={() => setPeriod(365)}>
                                            1R
                                        </Chip>
                                        <Chip active={period === 1825} onClick={() => setPeriod(1825)}>
                                            5L
                                        </Chip>
                                        <Chip active={period === "max"} onClick={() => setPeriod("max")}>
                                            MAX
                                        </Chip>
                                        <Chip active={area} onClick={() => setArea(!area)}>
                                            Area
                                        </Chip>
                                        <Chip active={smaOn} onClick={() => setSmaOn(!smaOn)}>
                                            SMA 20
                                        </Chip>
                                    </>
                                }
                            >
                                {!symbol ? (
                                    <div className="p-6 text-sm text-subtle">
                                        Dodaj spółkę do listy obserwacyjnej, aby zobaczyć wykres.
                                    </div>
                                ) : loading ? (
                                    <div className="p-6 text-sm text-subtle">
                                        Ładowanie danych z API…
                                    </div>
                                ) : rows.length ? (
                                    <>
                                        <Stats data={rows} />
                                        <div className="h-2" />
                                        <PriceChart
                                            rows={withSma}
                                            showArea={area}
                                            showSMA={smaOn}
                                            brushDataRows={period === "max" ? brushRows : undefined}
                                            brushRange={period === "max" ? brushRange : null}
                                            onBrushChange={
                                                period === "max" ? handleBrushSelectionChange : undefined
                                            }
                                        />
                                    </>
                                ) : (
                                    <div className="p-6 text-sm text-subtle">
                                        Brak danych do wyświetlenia
                                    </div>
                                )}
                                {err && symbol && (
                                    <div className="mt-3 text-sm text-negative">Błąd: {err}</div>
                                )}
                            </Card>

                            <Card title="RSI (14)">
                                {!symbol ? (
                                    <div className="p-6 text-sm text-subtle">
                                        Dodaj spółkę, aby zobaczyć wskaźnik RSI.
                                    </div>
                                ) : (
                                    <RsiChart rows={withRsi} />
                                )}
                            </Card>
                        </div>

                        <div className="space-y-6">
                            <Card title={`Fundamenty – ${symbolLabel}`}>
                                <div className="text-sm text-subtle">
                                    {symbol
                                        ? "Dane przykładowe — podłączymy realne API fundamentów w kolejnym kroku."
                                        : "Dodaj spółkę, aby zobaczyć sekcję fundamentów."}
                                </div>
                                {symbol && (
                                    <div className="mt-4 grid grid-cols-1 sm:grid-cols-2 gap-y-2 sm:gap-x-4 text-sm">
                                        <div className="text-subtle">Kapitalizacja</div>
                                        <div>$—</div>
                                        <div className="text-subtle">P/E (TTM)</div>
                                        <div>—</div>
                                        <div className="text-subtle">Przychody</div>
                                        <div>—</div>
                                        <div className="text-subtle">Marża netto</div>
                                        <div>—</div>
                                    </div>
                                )}
                            </Card>

                            <Card title="Skaner (demo)" right={<Chip active>Beta</Chip>}>
                                <ul className="text-sm list-disc pl-5 space-y-1">
                                    <li>Wysoki wolumen vs 20-sesyjna średnia</li>
                                    <li>RSI &lt; 30 (wyprzedanie)</li>
                                    <li>Przebicie SMA50 od dołu</li>
                                    <li>Nowe 52-tygodniowe maksimum</li>
                                </ul>
                                <p className="text-xs text-subtle mt-3">
                                    Podmienimy na realny backend skanera.
                                </p>
                            </Card>
                        </div>
                    </div>
                </Section>

                <Section
                    id="score"
                    kicker="Krok 3"
                    title="Konfigurator score"
                    description="Skonfiguruj zasady rankingu i pobierz wynik z backendu jednym kliknięciem."
                >
                    <Card title="Konfigurator score" right={<Chip active>Nowość</Chip>}>
                        <div className="space-y-5 text-sm">
                            <div className="grid gap-4 md:grid-cols-2">
                                <label className="flex flex-col gap-2">
                                    <span className="text-muted text-xs uppercase tracking-wide">
                                        Nazwa score
                                    </span>
                                    <input
                                        type="text"
                                        value={scoreNameInput}
                                        onChange={(e) => setScoreNameInput(e.target.value)}
                                        className={inputBaseClasses}
                                        placeholder="np. custom_quality"
                                    />
                                </label>
                                <label className="flex flex-col gap-2">
                                    <span className="text-muted text-xs uppercase tracking-wide">
                                        Opis (opcjonalnie)
                                    </span>
                                    <input
                                        type="text"
                                        value={scoreDescription}
                                        onChange={(e) => setScoreDescription(e.target.value)}
                                        className={inputBaseClasses}
                                        placeholder="Krótka nazwa w raporcie"
                                    />
                                </label>
                            </div>
                            <div className="grid gap-4 md:grid-cols-2">
                                <label className="flex flex-col gap-2">
                                    <span className="text-muted text-xs uppercase tracking-wide">
                                        Universe / filtr
                                    </span>
                                    <input
                                        type="text"
                                        value={scoreUniverse}
                                        onChange={(e) => setScoreUniverse(e.target.value)}
                                        className={inputBaseClasses}
                                        placeholder="np. WIG20.WA, WIG40.WA"
                                    />
                                </label>
                                <label className="flex flex-col gap-2">
                                    <span className="text-muted text-xs uppercase tracking-wide">
                                        Liczba spółek
                                    </span>
                                    <input
                                        type="number"
                                        min={1}
                                        step={1}
                                        value={scoreLimit}
                                        onChange={(e) => setScoreLimit(Number(e.target.value))}
                                        className={inputBaseClasses}
                                    />
                                </label>
                            </div>
                            <div className="grid gap-4 md:grid-cols-2">
                                <label className="flex flex-col gap-2">
                                    <span className="text-muted text-xs uppercase tracking-wide">
                                        Sortowanie
                                    </span>
                                    <div className="flex gap-2">
                                        <Chip active={scoreSort === "desc"} onClick={() => setScoreSort("desc")}>
                                            Najwyższe score
                                        </Chip>
                                        <Chip active={scoreSort === "asc"} onClick={() => setScoreSort("asc")}>
                                            Najniższe score
                                        </Chip>
                                    </div>
                                </label>
                                <label className="flex flex-col gap-2">
                                    <span className="text-muted text-xs uppercase tracking-wide">
                                        Data (as of)
                                    </span>
                                    <input
                                        type="date"
                                        value={scoreAsOf}
                                        onChange={(e) => setScoreAsOf(e.target.value)}
                                        className={inputBaseClasses}
                                    />
                                </label>
                            </div>
                            <div className="grid gap-4 md:grid-cols-2">
                                <label className="flex flex-col gap-2">
                                    <span className="text-muted text-xs uppercase tracking-wide">
                                        Min kapitalizacja (mln)
                                    </span>
                                    <input
                                        type="number"
                                        value={scoreMinMcap}
                                        onChange={(e) => setScoreMinMcap(e.target.value)}
                                        className={inputBaseClasses}
                                    />
                                </label>
                                <label className="flex flex-col gap-2">
                                    <span className="text-muted text-xs uppercase tracking-wide">
                                        Min obrót (mln)
                                    </span>
                                    <input
                                        type="number"
                                        value={scoreMinTurnover}
                                        onChange={(e) => setScoreMinTurnover(e.target.value)}
                                        className={inputBaseClasses}
                                    />
                                </label>
                            </div>
                            <div className="space-y-4">
                                <div className="flex items-center justify-between flex-wrap gap-3">
                                    <div>
                                        <div className="text-sm font-medium text-primary">Zasady rankingu</div>
                                        <div className="text-xs text-subtle">
                                            Dobierz metryki, kierunek i wagi. Każde kliknięcie od razu aktualizuje payload
                                            wysyłany do backendu.
                                        </div>
                                    </div>
                                    <button
                                        type="button"
                                        onClick={addScoreRule}
                                        className="px-3 py-2 rounded-xl border border-dashed border-soft text-sm text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                    >
                                        Dodaj metrykę
                                    </button>
                                </div>
                                <div className="space-y-4">
                                    {scoreRules.map((rule, idx) => (
                                        <div
                                            key={rule.id}
                                            className="relative rounded-2xl border border-soft bg-soft-surface p-4"
                                        >
                                            <button
                                                type="button"
                                                onClick={() => removeScoreRule(rule.id)}
                                                className="absolute right-4 top-4 inline-flex items-center justify-center rounded-lg border border-soft px-2 py-1 text-xs text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                            >
                                                Usuń
                                            </button>
                                            <div className="space-y-3 pt-8 md:pt-3">
                                                <div className="grid gap-3 md:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
                                                        <label className="flex flex-col gap-2">
                                                            <span className="text-xs uppercase tracking-wide text-muted">
                                                                Metryka
                                                            </span>
                                                            <select
                                                                value={rule.metric}
                                                                onChange={(e) =>
                                                                    setScoreRules((prev) =>
                                                                        prev.map((r) =>
                                                                            r.id === rule.id
                                                                                ? {
                                                                                      ...r,
                                                                                      metric: e.target.value,
                                                                                      label:
                                                                                          SCORE_METRIC_OPTIONS.find(
                                                                                              (option) =>
                                                                                                  option.value ===
                                                                                                  e.target.value
                                                                                          )?.label ?? r.label,
                                                                                      direction:
                                                                                          SCORE_METRIC_OPTIONS.find(
                                                                                              (option) =>
                                                                                                  option.value ===
                                                                                                  e.target.value
                                                                                          )?.defaultDirection ??
                                                                                          r.direction,
                                                                                  }
                                                                                : r
                                                                        )
                                                                    )
                                                                }
                                                                className={inputBaseClasses}
                                                            >
                                                                <option value="">Wybierz…</option>
                                                                {SCORE_METRIC_OPTIONS.map((option) => (
                                                                    <option key={option.value} value={option.value}>
                                                                        {option.label}
                                                                    </option>
                                                                ))}
                                                            </select>
                                                        </label>
                                                        <label className="flex flex-col gap-2">
                                                            <span className="text-xs uppercase tracking-wide text-muted">
                                                                Waga
                                                            </span>
                                                            <input
                                                                type="number"
                                                                value={rule.weight}
                                                                onChange={(e) =>
                                                                    setScoreRules((prev) =>
                                                                        prev.map((r) =>
                                                                            r.id === rule.id
                                                                                ? { ...r, weight: Number(e.target.value) }
                                                                                : r
                                                                        )
                                                                    )
                                                                }
                                                                className={inputBaseClasses}
                                                            />
                                                        </label>
                                                    </div>
                                                    <div className="grid gap-3 md:grid-cols-3">
                                                        <div>
                                                            <span className="text-xs uppercase tracking-wide text-muted">
                                                                Kierunek
                                                            </span>
                                                            <div className="mt-1 grid grid-cols-2 gap-2">
                                                                <Chip
                                                                    active={rule.direction === "desc"}
                                                                    onClick={() =>
                                                                        setScoreRules((prev) =>
                                                                            prev.map((r) =>
                                                                                r.id === rule.id
                                                                                    ? { ...r, direction: "desc" }
                                                                                    : r
                                                                            )
                                                                        )
                                                                    }
                                                                    className="w-full"
                                                                >
                                                                    Więcej = lepiej
                                                                </Chip>
                                                                <Chip
                                                                    active={rule.direction === "asc"}
                                                                    onClick={() =>
                                                                        setScoreRules((prev) =>
                                                                            prev.map((r) =>
                                                                                r.id === rule.id
                                                                                    ? { ...r, direction: "asc" }
                                                                                    : r
                                                                            )
                                                                        )
                                                                    }
                                                                    className="w-full"
                                                                >
                                                                    Mniej = lepiej
                                                                </Chip>
                                                            </div>
                                                        </div>
                                                        <label className="flex flex-col gap-2">
                                                            <span className="text-xs uppercase tracking-wide text-muted">
                                                                Minimalna wartość
                                                            </span>
                                                            <input
                                                                type="number"
                                                                value={rule.min ?? ""}
                                                                onChange={(e) =>
                                                                    setScoreRules((prev) =>
                                                                        prev.map((r) =>
                                                                            r.id === rule.id
                                                                                ? { ...r, min: e.target.value }
                                                                                : r
                                                                        )
                                                                    )
                                                                }
                                                                className={inputBaseClasses}
                                                            />
                                                        </label>
                                                        <label className="flex flex-col gap-2">
                                                            <span className="text-xs uppercase tracking-wide text-muted">
                                                                Maksymalna wartość
                                                            </span>
                                                            <input
                                                                type="number"
                                                                value={rule.max ?? ""}
                                                                onChange={(e) =>
                                                                    setScoreRules((prev) =>
                                                                        prev.map((r) =>
                                                                            r.id === rule.id
                                                                                ? { ...r, max: e.target.value }
                                                                                : r
                                                                        )
                                                                    )
                                                                }
                                                                className={inputBaseClasses}
                                                            />
                                                        </label>
                                                    </div>
                                                    <div className="grid gap-3 md:grid-cols-2">
                                                        <label className="flex flex-col gap-2">
                                                            <span className="text-xs uppercase tracking-wide text-muted">
                                                                Normalizacja
                                                            </span>
                                                            <select
                                                                value={rule.transform ?? "raw"}
                                                                onChange={(e) =>
                                                                    setScoreRules((prev) =>
                                                                        prev.map((r) =>
                                                                            r.id === rule.id
                                                                                ? {
                                                                                      ...r,
                                                                                      transform: e.target
                                                                                          .value as ScoreBuilderRule["transform"],
                                                                                  }
                                                                                : r
                                                                        )
                                                                    )
                                                                }
                                                                className={inputBaseClasses}
                                                            >
                                                                <option value="raw">Bez zmian</option>
                                                                <option value="zscore">Z-score</option>
                                                                <option value="percentile">Percentyl</option>
                                                            </select>
                                                        </label>
                                                        <div className="text-xs text-subtle">
                                                            Metryki korzystają z danych cenowych (zwroty, zmienność,
                                                            Sharpe). Wagi są skalowane automatycznie.
                                                        </div>
                                                    </div>
                                                </div>
                                            {idx === scoreRules.length - 1 && (
                                                <div className="mt-3 text-xs text-subtle">
                                                    Zmieniaj wagi i parametry, aby zobaczyć wpływ na ranking.
                                                </div>
                                            )}
                                        </div>
                                    ))}
                                </div>
                                <div className="text-xs text-subtle">
                                    Suma wag: <b>{scoreTotalWeight.toFixed(1)}</b>. Wartość względna — backend normalizuje
                                    je przy obliczeniach.
                                </div>
                            </div>
                            <div className="flex flex-wrap items-center gap-3">
                                <button
                                    type="button"
                                    onClick={handleScorePreview}
                                    disabled={scoreDisabled}
                                    className="px-4 py-2 rounded-xl bg-accent text-primary transition hover:bg-[#27AE60] disabled:opacity-50"
                                >
                                    {scoreLoading ? "Łączenie…" : "Przelicz ranking"}
                                </button>
                                <button
                                    type="button"
                                    onClick={resetScoreBuilder}
                                    className="px-4 py-2 rounded-xl border border-soft text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                >
                                    Resetuj
                                </button>
                                {!scoreComponents.length && !scoreLoading && (
                                    <div className="text-xs text-negative">
                                        Dodaj co najmniej jedną metrykę z wagą różną od zera.
                                    </div>
                                )}
                                {scoreLimitInvalid && !scoreLoading && (
                                    <div className="text-xs text-negative">Liczba spółek musi być dodatnia.</div>
                                )}
                            </div>
                            {scoreError && <div className="text-sm text-negative">Błąd: {scoreError}</div>}
                            {scoreResults ? (
                                <div className="space-y-4">
                                    <div className="text-xs text-subtle">
                                        {[
                                            scoreResults.meta.name ? `Score: ${scoreResults.meta.name}` : null,
                                            scoreResults.meta.asOf ? `Stan na ${scoreResults.meta.asOf}` : null,
                                            typeof scoreResults.meta.totalUniverse === "number"
                                                ? `Universe: ${scoreResults.meta.totalUniverse}`
                                                : null,
                                            scoreResults.meta.runId ? `ID: ${scoreResults.meta.runId}` : null,
                                            scoreResults.meta.requestId ? `Request: ${scoreResults.meta.requestId}` : null,
                                        ]
                                            .filter(Boolean)
                                            .join(" • ")}
                                    </div>
                                    <ScoreRankingTable rows={scoreResults.rows} />
                                </div>
                            ) : (
                                <div className="text-xs text-subtle">
                                    {scoreLoading
                                        ? "Łączenie z backendem…"
                                        : "Zdefiniuj zasady i kliknij \"Przelicz ranking\", aby pobrać wyniki."}
                                </div>
                            )}
                        </div>
                    </Card>
                </Section>

                <Section
                    id="portfolio"
                    kicker="Krok 4"
                    title="Portfel – symulacja i rebalansing"
                    description="Porównaj strategie z realnymi wagami lub rankingiem score, w tym statystyki, wykres i log rebalansingu."
                >
                    <Card>
                        <div className="space-y-8">
                            <div className="space-y-4">
                                <div className="flex flex-wrap gap-2">
                                    <Chip active={pfMode === "manual"} onClick={() => setPfMode("manual")}>
                                        Własne wagi
                                    </Chip>
                                    <Chip active={pfMode === "score"} onClick={() => setPfMode("score")}>
                                        Automatycznie wg score
                                    </Chip>
                                </div>
                                <p className="text-sm text-muted">
                                    Wybierz tryb konfiguracji portfela. Wariant score wykorzysta parametry zdefiniowane
                                    powyżej lub dowolną istniejącą nazwę rankingu z backendu.
                                </p>
                            </div>
                            <div className="grid gap-6 md:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
                                <div className="space-y-3">
                                    {pfMode === "manual" ? (
                                        <>
                                            <div className="text-sm font-medium text-neutral">Skład portfela</div>
                                            {pfRows.map((r, i) => (
                                                <div
                                                    key={i}
                                                    className="flex flex-wrap items-center gap-3 rounded-xl border border-soft bg-soft-surface px-3 py-3"
                                                >
                                                    <div className="flex-1 min-w-[12rem]">
                                                        <TickerAutosuggest
                                                            onPick={(sym) => {
                                                                setPfRows((rows) =>
                                                                    rows.map((x, idx) =>
                                                                        idx === i
                                                                            ? { ...x, symbol: sym }
                                                                            : x
                                                                    )
                                                                );
                                                            }}
                                                            placeholder={r.symbol || "Symbol"}
                                                            inputClassName="w-full"
                                                        />
                                                    </div>
                                                    <div className="flex flex-wrap items-center gap-2 md:flex-nowrap">
                                                        <input
                                                            type="number"
                                                            min={0}
                                                            max={100}
                                                            step={1}
                                                            value={r.weight}
                                                            onChange={(e) =>
                                                                setPfRows((rows) =>
                                                                    rows.map((x, idx) =>
                                                                        idx === i
                                                                            ? {
                                                                                  ...x,
                                                                                  weight: Number(e.target.value),
                                                                              }
                                                                            : x
                                                                    )
                                                                )
                                                            }
                                                            className={`${inputBaseClasses} w-24 md:w-20`}
                                                        />
                                                        <span className="text-sm text-subtle">%</span>
                                                        <button
                                                            onClick={() =>
                                                                setPfRows((rows) => rows.filter((_, idx) => idx !== i))
                                                            }
                                                            className="px-2 py-1 text-xs rounded-lg border border-soft text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                                        >
                                                            Usuń
                                                        </button>
                                                    </div>
                                                </div>
                                            ))}
                                            <button
                                                onClick={() =>
                                                    setPfRows((rows) => [
                                                        ...rows,
                                                        { symbol: "", weight: 0 },
                                                    ])
                                                }
                                                className="px-3 py-2 rounded-xl border border-dashed border-soft text-sm text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                            >
                                                Dodaj pozycję
                                            </button>
                                            <div className="text-xs text-subtle">
                                                Suma wag: {pfTotal.toFixed(1)}% (normalizujemy do 100%).
                                            </div>
                                        </>
                                    ) : (
                                        <div className="space-y-3">
                                            <div className="grid gap-3 md:grid-cols-2">
                                                <label className="flex flex-col gap-2">
                                                    <span className="text-xs uppercase tracking-wide text-muted">
                                                        Score
                                                    </span>
                                                    <input
                                                        type="text"
                                                        value={pfScoreName}
                                                        onChange={(e) => setPfScoreName(e.target.value)}
                                                        className={inputBaseClasses}
                                                        placeholder="np. quality_score"
                                                    />
                                                </label>
                                                <label className="flex flex-col gap-2">
                                                    <span className="text-xs uppercase tracking-wide text-muted">
                                                        Limit spółek
                                                    </span>
                                                    <input
                                                        type="number"
                                                        min={1}
                                                        step={1}
                                                        value={pfScoreLimit}
                                                        onChange={(e) => setPfScoreLimit(Number(e.target.value))}
                                                        className={inputBaseClasses}
                                                    />
                                                </label>
                                            </div>
                                            <div className="grid gap-3 md:grid-cols-3">
                                                <label className="flex flex-col gap-2">
                                                    <span className="text-xs uppercase tracking-wide text-muted">
                                                        Wagi
                                                    </span>
                                                    <select
                                                        value={pfScoreWeighting}
                                                        onChange={(e) => setPfScoreWeighting(e.target.value)}
                                                        className={inputBaseClasses}
                                                    >
                                                        <option value="equal">Równe</option>
                                                        <option value="score">Proporcjonalne do score</option>
                                                        <option value="volatility_inverse">Odwrotność zmienności</option>
                                                    </select>
                                                </label>
                                                <div>
                                                    <span className="text-xs uppercase tracking-wide text-muted">
                                                        Kierunek
                                                    </span>
                                                    <div className="mt-1 flex flex-wrap gap-2">
                                                        <Chip
                                                            active={pfScoreDirection === "desc"}
                                                            onClick={() => setPfScoreDirection("desc")}
                                                        >
                                                            Najwyższy score
                                                        </Chip>
                                                        <Chip
                                                            active={pfScoreDirection === "asc"}
                                                            onClick={() => setPfScoreDirection("asc")}
                                                        >
                                                            Najniższy score
                                                        </Chip>
                                                    </div>
                                                </div>
                                                <label className="flex flex-col gap-2">
                                                    <span className="text-xs uppercase tracking-wide text-muted">
                                                        Universe / filtr
                                                    </span>
                                                    <input
                                                        type="text"
                                                        value={pfScoreUniverse}
                                                        onChange={(e) => setPfScoreUniverse(e.target.value)}
                                                        className={inputBaseClasses}
                                                        placeholder="np. WIG20.WA"
                                                    />
                                                </label>
                                            </div>
                                            <div className="grid gap-3 md:grid-cols-2">
                                                <label className="flex flex-col gap-2">
                                                    <span className="text-xs uppercase tracking-wide text-muted">
                                                        Min score
                                                    </span>
                                                    <input
                                                        type="number"
                                                        value={pfScoreMin}
                                                        onChange={(e) => setPfScoreMin(e.target.value)}
                                                        className={inputBaseClasses}
                                                    />
                                                </label>
                                                <label className="flex flex-col gap-2">
                                                    <span className="text-xs uppercase tracking-wide text-muted">
                                                        Max score
                                                    </span>
                                                    <input
                                                        type="number"
                                                        value={pfScoreMax}
                                                        onChange={(e) => setPfScoreMax(e.target.value)}
                                                        className={inputBaseClasses}
                                                    />
                                                </label>
                                            </div>
                                        </div>
                                    )}
                                </div>
                                <div className="space-y-4">
                                    <div className="grid gap-3 md:grid-cols-2">
                                        <label className="flex flex-col gap-2 rounded-2xl border border-soft bg-white/80 px-4 py-3 shadow-sm">
                                            <span className="text-muted">Data startu</span>
                                            <input
                                                type="date"
                                                value={pfStart}
                                                onChange={(e) => setPfStart(e.target.value)}
                                                className={inputBaseClasses}
                                            />
                                        </label>
                                        <label className="flex flex-col gap-2 rounded-2xl border border-soft bg-white/80 px-4 py-3 shadow-sm">
                                            <span className="text-muted">Data końca</span>
                                            <input
                                                type="date"
                                                value={pfEnd}
                                                onChange={(e) => setPfEnd(e.target.value)}
                                                className={inputBaseClasses}
                                            />
                                        </label>
                                        <label className="flex flex-col gap-2 rounded-2xl border border-soft bg-white/80 px-4 py-3 shadow-sm">
                                            <span className="text-muted">Rebalansing</span>
                                            <select
                                                value={pfFreq}
                                                onChange={(e) => setPfFreq(e.target.value as Rebalance)}
                                                className={inputBaseClasses}
                                            >
                                                <option value="monthly">Miesięcznie</option>
                                                <option value="quarterly">Kwartalnie</option>
                                                <option value="yearly">Rocznie</option>
                                                <option value="none">Bez rebalansingu</option>
                                            </select>
                                        </label>
                                        <label className="flex flex-col gap-2 rounded-2xl border border-soft bg-white/80 px-4 py-3 shadow-sm">
                                            <span className="text-muted">Kapitał początkowy</span>
                                            <input
                                                type="number"
                                                min={0}
                                                step={100}
                                                value={pfInitial}
                                                onChange={(e) => setPfInitial(Number(e.target.value))}
                                                className={inputBaseClasses}
                                            />
                                        </label>
                                        <label className="flex flex-col gap-2 rounded-2xl border border-soft bg-white/80 px-4 py-3 shadow-sm">
                                            <span className="text-muted">Koszt transakcyjny (%)</span>
                                            <input
                                                type="number"
                                                min={0}
                                                step={0.1}
                                                value={pfFee}
                                                onChange={(e) => setPfFee(Number(e.target.value))}
                                                className={inputBaseClasses}
                                            />
                                        </label>
                                        <label className="flex flex-col gap-2 rounded-2xl border border-soft bg-white/80 px-4 py-3 shadow-sm">
                                            <span className="text-muted">Próg rebalansingu (%)</span>
                                            <input
                                                type="number"
                                                min={0}
                                                step={0.1}
                                                value={pfThreshold}
                                                onChange={(e) => setPfThreshold(Number(e.target.value))}
                                                className={inputBaseClasses}
                                            />
                                        </label>
                                    </div>
                                    <div className="space-y-2">
                                        <span className="text-sm text-muted">Benchmark (opcjonalnie)</span>
                                        <div className="flex flex-wrap items-center gap-2">
                                            <TickerAutosuggest
                                                onPick={(sym) => setPfBenchmark(sym)}
                                                placeholder="Dodaj benchmark (np. WIG20.WA)"
                                                inputClassName="w-60"
                                            />
                                            {pfBenchmark && (
                                                <div className="flex items-center gap-2 text-sm text-muted">
                                                    <span className="font-medium text-primary">{pfBenchmark}</span>
                                                    <button
                                                        onClick={() => setPfBenchmark(null)}
                                                        className="px-2 py-1 text-xs rounded-lg border border-soft text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                                    >
                                                        Wyczyść
                                                    </button>
                                                </div>
                                            )}
                                            {!pfBenchmark && (
                                                <span className="text-xs text-subtle">
                                                    Wykres porówna portfel z wybranym benchmarkiem.
                                                </span>
                                            )}
                                        </div>
                                    </div>
                                    {pfRangeInvalid && (
                                        <div className="text-xs text-negative">
                                            Data końca musi być późniejsza niż data startu.
                                        </div>
                                    )}
                                    <button
                                        disabled={pfDisableSimulation}
                                        onClick={runPortfolioSimulation}
                                        className="w-full md:w-auto px-4 py-2 rounded-xl bg-accent text-primary transition hover:bg-[#27AE60] disabled:opacity-50"
                                    >
                                        {pfLoading
                                            ? "Liczenie…"
                                            : pfMode === "manual"
                                            ? "Symuluj portfel"
                                            : "Symuluj wg score"}
                                    </button>
                                    {pfMode === "manual" ? (
                                        <>
                                            {pfHasInvalidWeights && (
                                                <div className="text-xs text-negative">
                                                    Wagi muszą być liczbami większymi lub równymi zero.
                                                </div>
                                            )}
                                            {pfHasMissingSymbols && (
                                                <div className="text-xs text-negative">
                                                    Uzupełnij symbole dla pozycji z dodatnią wagą.
                                                </div>
                                            )}
                                            {!pfHasValidPositions &&
                                                !pfHasMissingSymbols &&
                                                !pfHasInvalidWeights && (
                                                    <div className="text-xs text-negative">
                                                        Dodaj co najmniej jedną spółkę z wagą większą od zera.
                                                    </div>
                                                )}
                                        </>
                                    ) : (
                                        <>
                                            {pfScoreNameInvalid && (
                                                <div className="text-xs text-negative">
                                                    Podaj nazwę score, aby zbudować portfel.
                                                </div>
                                            )}
                                            {pfScoreLimitInvalid && (
                                                <div className="text-xs text-negative">
                                                    Wybierz dodatnią liczbę spółek w rankingu.
                                                </div>
                                            )}
                                        </>
                                    )}
                                    {pfErr && (
                                        <div className="text-sm text-negative">Błąd: {pfErr}</div>
                                    )}
                                </div>
                            </div>

                            <div className="space-y-4">
                                {!pfRes ? (
                                    <div className="text-sm text-muted">
                                        {pfMode === "manual"
                                            ? "Skonfiguruj portfel (symbole + wagi), wybierz datę startu i rebalansing, potem uruchom symulację."
                                            : "Podaj score i parametry rankingu, ustaw zakres dat i uruchom symulację."}
                                    </div>
                                ) : (
                                    <>
                                        <PortfolioStatsGrid stats={pfRes.stats} />
                                        {pfRes.allocations && pfRes.allocations.length > 0 && (
                                            <div className="mt-6">
                                                <AllocationTable allocations={pfRes.allocations} />
                                            </div>
                                        )}
                                        <div className="h-72">
                                            <ResponsiveContainer width="100%" height="100%">
                                                <LineChart data={pfChartData}>
                                                    <CartesianGrid strokeDasharray="3 3" stroke="#BDC3C7" />
                                                    <XAxis dataKey="date" tick={{ fontSize: 12 }} tickMargin={8} />
                                                    <YAxis tick={{ fontSize: 12 }} width={60} />
                                                    <Tooltip
                                                        formatter={(value: number) =>
                                                            typeof value === "number" ? value.toFixed(2) : value
                                                        }
                                                    />
                                                    <Legend verticalAlign="top" height={36} />
                                                    <Line
                                                        type="monotone"
                                                        dataKey="portfolio"
                                                        name="Portfel"
                                                        stroke="#0A2342"
                                                        dot={false}
                                                    />
                                                    {pfRes.benchmark && pfRes.benchmark.length > 0 && (
                                                        <Line
                                                            type="monotone"
                                                            dataKey="benchmark"
                                                            name="Benchmark"
                                                            stroke="#3498DB"
                                                            strokeDasharray="4 2"
                                                            dot={false}
                                                        />
                                                    )}
                                                </LineChart>
                                            </ResponsiveContainer>
                                        </div>
                                        {pfRes.rebalances && pfRes.rebalances.length > 0 && (
                                            <div className="mt-6">
                                                <RebalanceTimeline events={pfRes.rebalances} />
                                            </div>
                                        )}
                                        <div className="text-xs text-subtle mt-2 space-y-1">
                                            <div>
                                                Zakres: {pfStart} → {pfEnd}. Rebalansing: {pfFreq}
                                                {pfThreshold > 0 ? ` (próg ${pfThreshold.toFixed(1)}%)` : ""}.
                                                Koszt transakcyjny: {pfFee.toFixed(2)}%.
                                            </div>
                                            <div>
                                                Wartość początkowa:
                                                {typeof pfRes.stats.initial_value === "number"
                                                    ? ` ${pfRes.stats.initial_value.toFixed(2)}`
                                                    : ` ${pfInitial.toFixed(2)}`} {" "}• Wagi są normalizowane do 100%.
                                            </div>
                                            <div>
                                                {pfRes.benchmark && pfRes.benchmark.length > 0
                                                    ? `Benchmark: ${pfLastBenchmark ?? "dostarczony w odpowiedzi"}.`
                                                    : "Bez benchmarku."}
                                            </div>
                                        </div>
                                    </>
                                )}
                            </div>
                        </div>
                    </Card>
                </Section>

                <footer className="pt-6 text-center text-sm text-subtle">
                    © {new Date().getFullYear()} Analityka Rynków • MVP
                </footer>
            </main>
        </div>
    );
}
