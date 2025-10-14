"use client";

import React, { useMemo, useState, useEffect, useId, useCallback, useRef } from "react";
import Image from "next/image";
import Script from "next/script";
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
    ReferenceLine,
    ReferenceDot,
    Brush,
} from "recharts";
import type { TooltipContentProps } from "recharts";
import type { CategoricalChartFunc } from "recharts/types/chart/types";
import type { MouseHandlerDataParam } from "recharts/types/synchronisation/types";
import type { BrushStartEndIndex } from "recharts/types/context/brushUpdateContext";

declare global {
    interface Window {
        google?: {
            accounts: {
                id: {
                    initialize: (options: {
                        client_id: string;
                        callback: (response: { credential?: string | undefined }) => void;
                        ux_mode?: "popup" | "redirect";
                        auto_select?: boolean;
                        cancel_on_tap_outside?: boolean;
                    }) => void;
                    prompt: () => void;
                    disableAutoSelect?: () => void;
                };
            };
        };
    }
}

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

const createTemplateId = () =>
    `tpl-${Math.random().toString(36).slice(2, 8)}-${Date.now().toString(36)}`;

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

const SCORE_UNIVERSE_FALLBACK: string[] = [
    "CDR.WA",
    "PKN.WA",
    "PEO.WA",
    "KGH.WA",
    "PGE.WA",
    "ALE.WA",
    "DNP.WA",
    "LPP.WA",
    "OPL.WA",
    "MRC.WA",
];

const SCORE_TEMPLATE_STORAGE_KEY = "gpw_score_templates_v1";

const resolveUniverseWithFallback = (
    universe: ScorePreviewRequest["universe"],
    fallback?: string[]
): ScorePreviewRequest["universe"] => {
    if (typeof universe === "string" && universe.trim()) {
        return universe.trim();
    }
    if (Array.isArray(universe)) {
        const cleaned = universe
            .map((item) => (typeof item === "string" ? item.trim() : ""))
            .filter(Boolean);
        if (cleaned.length) {
            return cleaned;
        }
    }
    if (fallback && fallback.length) {
        return [...fallback];
    }
    return undefined;
};

const toTemplateRule = (rule: ScoreBuilderRule): ScoreTemplateRule => ({
    metric: rule.metric,
    weight: Number(rule.weight) || 0,
    direction: rule.direction === "asc" ? "asc" : "desc",
    label: rule.label ?? null,
    transform: rule.transform ?? "raw",
});

const fromTemplateRules = (rules: ScoreTemplateRule[]): ScoreBuilderRule[] =>
    rules.map((rule) => ({
        id: createRuleId(),
        metric: rule.metric,
        weight: Number(rule.weight) || 0,
        direction: rule.direction === "asc" ? "asc" : "desc",
        label: rule.label ?? findScoreMetric(rule.metric)?.label ?? undefined,
        transform: rule.transform ?? "raw",
    }));

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

const GOOGLE_CLIENT_ID = process.env.NEXT_PUBLIC_GOOGLE_CLIENT_ID ?? "";
const DEFAULT_WATCHLIST = ["CDR.WA", "PKN.WA", "PKOBP"];

type ScoreDraftState = {
    name: string;
    description: string;
    limit: number;
    sort: "asc" | "desc";
    universe: string;
    minMcap: string;
    minTurnover: string;
    asOf: string;
    rules: ScoreBuilderRule[];
};

type PortfolioDraftState = {
    mode: "manual" | "score";
    rows: { symbol: string; weight: number }[];
    start: string;
    end: string;
    initial: number;
    fee: number;
    threshold: number;
    benchmark: string | null;
    frequency: Rebalance;
    score: {
        name: string;
        limit: number;
        weighting: string;
        direction: "asc" | "desc";
        universe: string;
        min: string;
        max: string;
    };
    comparisons: string[];
};

type AuthUser = {
    id: string;
    email: string | null;
    name: string | null;
    picture: string | null;
    provider: "google";
    createdAt: string;
    updatedAt: string;
};

type PersistedPreferences = {
    watchlist: string[];
    scoreTemplates: ScoreTemplate[];
    scoreDraft: ScoreDraftState;
    portfolioDraft: PortfolioDraftState;
};

type PublicUserProfile = {
    user: AuthUser;
    preferences: PersistedPreferences;
};

const getDefaultScoreDraft = (): ScoreDraftState => ({
    name: "custom_quality",
    description: "Ranking jakościowy – przykład",
    limit: 10,
    sort: "desc",
    universe: "",
    minMcap: "",
    minTurnover: "",
    asOf: new Date().toISOString().slice(0, 10),
    rules: getDefaultScoreRules(),
});

const getDefaultPortfolioDraft = (): PortfolioDraftState => ({
    mode: "manual",
    rows: [
        { symbol: "CDR.WA", weight: 40 },
        { symbol: "PKN.WA", weight: 30 },
        { symbol: "PKOBP", weight: 30 },
    ],
    start: "2015-01-01",
    end: new Date().toISOString().slice(0, 10),
    initial: 10000,
    fee: 0,
    threshold: 0,
    benchmark: null,
    frequency: "monthly",
    score: {
        name: "quality_score",
        limit: 10,
        weighting: "equal",
        direction: "desc",
        universe: "",
        min: "",
        max: "",
    },
    comparisons: [],
});

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
type ComparisonValueKey = `${string}__pct` | `${string}__close`;
type PriceChartComparisonPoint =
    PriceChartPoint & Partial<Record<ComparisonValueKey, number | null>>;

type ChartPeriod = 90 | 180 | 365 | 1825 | "max";

const PERIOD_OPTIONS: { label: string; value: ChartPeriod }[] = [
    { label: "3M", value: 90 },
    { label: "6M", value: 180 },
    { label: "1R", value: 365 },
    { label: "5L", value: 1825 },
    { label: "MAX", value: "max" },
];

const DAY_MS = 24 * 3600 * 1000;

const computeStartISOForPeriod = (period: ChartPeriod): string => {
    if (period === "max") {
        return "1990-01-01";
    }
    const startDate = new Date(Date.now() - period * DAY_MS);
    return startDate.toISOString().slice(0, 10);
};

const computeVisibleRangeForRows = (
    rows: Row[],
    period: ChartPeriod
): { start: string; end: string } | null => {
    if (!rows.length) return null;
    const end = rows[rows.length - 1]?.date;
    const start = rows[0]?.date;
    if (!end || !start) return null;
    if (period === "max") {
        return { start, end };
    }
    const endDate = new Date(end);
    if (Number.isNaN(endDate.getTime())) {
        return { start, end };
    }
    const candidateStart = new Date(endDate.getTime() - period * DAY_MS)
        .toISOString()
        .slice(0, 10);
    const matchedStart = rows.find((row) => row.date >= candidateStart)?.date ?? start;
    return { start: matchedStart, end };
};

const COMPARISON_COLORS = [
    "#2563EB",
    "#F59E0B",
    "#8B5CF6",
    "#EC4899",
    "#0EA5E9",
    "#16A34A",
    "#F97316",
];

const MAX_COMPARISONS = COMPARISON_COLORS.length;

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

type ScoreTemplateRule = {
    metric: string;
    weight: number;
    direction: "asc" | "desc";
    label?: string | null;
    transform?: "raw" | "zscore" | "percentile" | "";
};

type ScoreTemplate = {
    id: string;
    title: string;
    name?: string;
    description?: string;
    rules: ScoreTemplateRule[];
    limit: number;
    sort: "asc" | "desc";
    universe: string;
    minMcap: string;
    minTurnover: string;
    createdAt: string;
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
    shares_change?: number;
    price?: number;
    shares_after?: number;
    note?: string;
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

const portfolioPointsToRows = (points: PortfolioPoint[]): Row[] =>
    points.map((point) => ({
        date: point.date,
        open: point.value,
        high: point.value,
        low: point.value,
        close: point.value,
        volume: 0,
    }));

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
    const { start, end, rebalance, initialCapital, feePct, thresholdPct, benchmark } =
        options;

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
        end,
        rebalance,
        initial_capital: initialCapital,
        fee_pct: feePct,
        threshold_pct: thresholdPct,
        benchmark: benchmark?.trim() ? benchmark.trim() : undefined,
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
    components: ScoreComponentRequest[],
    fallbackUniverse?: string[]
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

    const resolvedUniverse = resolveUniverseWithFallback(universe, fallbackUniverse);

    const previewPayload: ScorePreviewRequest = {
        name: score && score.trim() ? score.trim() : undefined,
        rules: previewRules,
        limit: limitCandidate,
        universe: resolvedUniverse,
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

            const price = pickNumber(
                [tradeRecord],
                ["price", "trade_price", "execution_price", "share_price"]
            );
            if (price !== undefined) normalizedTrade.price = price;

            const sharesChange = pickNumber(
                [tradeRecord],
                [
                    "shares_change",
                    "shares_delta",
                    "delta_shares",
                    "quantity_change",
                    "shares",
                ]
            );
            if (sharesChange !== undefined) normalizedTrade.shares_change = sharesChange;

            const sharesAfter = pickNumber(
                [tradeRecord],
                [
                    "shares_after",
                    "quantity_after",
                    "position_size",
                    "target_quantity",
                ]
            );
            if (sharesAfter !== undefined) normalizedTrade.shares_after = sharesAfter;

            const targetWeight = pickNumber(
                [tradeRecord],
                ["target_weight", "new_weight", "weight_after"]
            );
            if (targetWeight !== undefined) normalizedTrade.target_weight = targetWeight;

            const noteRaw =
                tradeRecord.note ?? tradeRecord.comment ?? tradeRecord.info ?? tradeRecord.details;
            if (noteRaw !== undefined) normalizedTrade.note = String(noteRaw);

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

type DashboardView = "analysis" | "score" | "portfolio";
type NavItem = {
    href: string;
    label: string;
    key?: DashboardView;
    icon?: React.ComponentType<{ className?: string }>;
};

const IconSparkline = ({ className }: { className?: string }) => (
    <svg
        viewBox="0 0 24 24"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        className={className}
    >
        <path
            d="M4 18H20"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
        <path
            d="M5 13.5L9.5 9L12.5 12L19 5"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
    </svg>
);

const IconTrophy = ({ className }: { className?: string }) => (
    <svg
        viewBox="0 0 24 24"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        className={className}
    >
        <path
            d="M8 4H16V7.5C16 9.433 14.433 11 12.5 11H11.5C9.567 11 8 9.433 8 7.5V4Z"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
        <path
            d="M6 4H4V6C4 7.657 5.343 9 7 9"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
        <path
            d="M18 4H20V6C20 7.657 18.657 9 17 9"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
        <path
            d="M12 11V15"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
        />
        <path
            d="M9 20H15"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
        <path
            d="M9.5 15H14.5V18.5H9.5V15Z"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
    </svg>
);

const IconPie = ({ className }: { className?: string }) => (
    <svg
        viewBox="0 0 24 24"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        className={className}
    >
        <path
            d="M12 3V12H21"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
        <path
            d="M19 12.5C18.8491 15.6537 16.1537 18.3491 13 18.5C9.41015 18.6745 6.32551 15.5899 6.5 12C6.65094 8.84634 9.34634 6.15094 12.5 6"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
    </svg>
);

const SectionNav = ({ items }: { items: NavItem[] }) => {
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

const SidebarToggleGlyph = ({ className }: { className?: string }) => (
    <svg
        viewBox="0 0 20 20"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        className={className}
        aria-hidden
    >
        <rect
            x="2.75"
            y="2.75"
            width="14.5"
            height="14.5"
            rx="3"
            stroke="currentColor"
            strokeWidth="1.5"
        />
        <path
            d="M7.25 2.75V17.25"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
        />
        <path
            d="M10.75 6.75H14.25"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
        />
        <path
            d="M10.75 10.25H14.25"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
        />
        <path
            d="M10.75 13.75H14.25"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
        />
    </svg>
);

const SidebarNav = ({
    items,
    activeKey,
    collapsed,
    onNavigate,
}: {
    items: NavItem[];
    activeKey?: DashboardView;
    collapsed?: boolean;
    onNavigate?: () => void;
}) => {
    if (!items.length) return null;
    return (
        <nav className={`space-y-1.5 ${collapsed ? "text-[11px]" : "text-sm"}`}>
            {items.map((item) => {
                const active = item.key && item.key === activeKey;
                const Icon = item.icon;
                return (
                    <a
                        key={item.href}
                        href={item.href}
                        aria-label={collapsed ? item.label : undefined}
                        className={`group relative flex items-center overflow-hidden rounded-xl border border-transparent px-3 py-2 transition ${
                            collapsed ? "justify-center" : "gap-3"
                        } ${
                            active
                                ? "bg-white/10 text-white shadow-[0_0_0_1px_rgba(255,255,255,0.08)]"
                                : "text-white/70 hover:border-white/10 hover:text-white hover:bg-white/5"
                        }`}
                        title={item.label}
                        onClick={() => onNavigate?.()}
                        aria-current={active ? "page" : undefined}
                    >
                        {active && (
                            <span
                                aria-hidden
                                className={`absolute left-2 top-1/2 h-7 w-1 -translate-y-1/2 rounded-full bg-[#10a37f] ${
                                    collapsed ? "left-1 h-6" : ""
                                }`}
                            />
                        )}
                        {Icon ? (
                            <span
                                aria-hidden
                                className={`relative z-10 inline-flex items-center justify-center rounded-lg transition ${
                                    collapsed
                                        ? "h-12 w-12 bg-white/5"
                                        : "h-10 w-10 bg-white/10 group-hover:bg-white/15"
                                } ${active ? "text-white" : "text-white/60 group-hover:text-white"}`}
                            >
                                <Icon className="h-5 w-5" />
                            </span>
                        ) : collapsed ? (
                            <span aria-hidden className="font-semibold">
                                {item.label.charAt(0).toUpperCase()}
                            </span>
                        ) : null}
                        {!collapsed && (
                            <span className="relative z-10 font-medium">{item.label}</span>
                        )}
                        {!collapsed && (
                            <svg
                                viewBox="0 0 24 24"
                                fill="none"
                                xmlns="http://www.w3.org/2000/svg"
                                className={`relative z-10 h-4 w-4 transition ${
                                    active ? "text-white" : "text-white/40 group-hover:text-white/70"
                                }`}
                                aria-hidden
                            >
                                <path
                                    d="M9 5L16 12L9 19"
                                    stroke="currentColor"
                                    strokeWidth="2"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                />
                            </svg>
                        )}
                        {active && collapsed && <span className="sr-only">(aktywny)</span>}
                    </a>
                );
            })}
        </nav>
    );
};

const SidebarContent = ({
    collapsed,
    navItems,
    activeKey,
    onStartAnalysis,
    isAuthenticated,
    authUser,
    profileLoading,
    authLoading,
    handleLogout,
    openAuthDialog,
    authError,
    profileError,
    googleClientId,
    onNavigate,
    onToggleCollapse,
}: {
    collapsed?: boolean;
    navItems: NavItem[];
    activeKey?: DashboardView;
    onStartAnalysis: () => void;
    isAuthenticated: boolean;
    authUser: AuthUser | null;
    profileLoading: boolean;
    authLoading: boolean;
    handleLogout: () => void;
    openAuthDialog: (mode: "login" | "signup") => void;
    authError: string | null;
    profileError: string | null;
    googleClientId: string;
    onNavigate?: () => void;
    onToggleCollapse?: () => void;
}) => {
    const sectionPadding = collapsed ? "px-3" : "px-5";
    const navSpacing = collapsed ? "mt-4" : "mt-6";
    const headerSpacing = collapsed ? "space-y-4" : "space-y-5";
    const collapseToggleLabel = collapsed ? "Otwórz pasek boczny" : "Zamknij pasek boczny";
    const toggleTooltipClass =
        "pointer-events-none absolute left-full top-1/2 z-20 ml-3 -translate-y-1/2 whitespace-nowrap rounded-lg bg-[#1a1c23] px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.28em] text-white opacity-0 transition-opacity duration-150 group-hover:opacity-100 group-focus-visible:opacity-100";
    const renderBrandBadge = () => (
        <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-gradient-to-br from-[#10a37f] via-[#0f5d4a] to-[#0b3d2d] text-sm font-semibold">
            GA
        </div>
    );
    const renderExpandedHeader = () => (
        <div className="flex w-full items-center gap-3">
            <div className="group relative">{renderBrandBadge()}</div>
            <div className="leading-tight">
                <p className="text-[11px] font-semibold uppercase tracking-[0.28em] text-white/50">
                    GPW Analytics
                </p>
                <p className="text-base font-semibold text-white">Panel demo</p>
            </div>
        </div>
    );
    const renderExpandedToggle = () => (
        <button
            type="button"
            onClick={onToggleCollapse}
            className="group relative flex h-10 w-10 items-center justify-center rounded-xl border border-white/15 bg-white/5 text-white/70 transition hover:border-white/30 hover:bg-white/10 hover:text-white focus:outline-none focus-visible:ring-2 focus-visible:ring-white/40 focus-visible:ring-offset-2 focus-visible:ring-offset-[#0f1014]"
            aria-label={collapseToggleLabel}
            aria-expanded={!collapsed}
        >
            <SidebarToggleGlyph className="h-4 w-4" />
            <span className={toggleTooltipClass}>
                {collapseToggleLabel}
            </span>
        </button>
    );
    const headerAlignment = collapsed
        ? "justify-center"
        : onToggleCollapse
        ? "justify-between"
        : "justify-start";
    return (
        <div className="flex h-full flex-col bg-[#0f1014] text-white">
            <div className={`${sectionPadding} ${headerSpacing} pt-6`}>
                <div className={`flex items-center ${headerAlignment} gap-3`}>
                    {collapsed ? (
                        onToggleCollapse ? (
                            <button
                                type="button"
                                onClick={onToggleCollapse}
                                className="group relative flex h-12 w-12 items-center justify-center rounded-xl bg-gradient-to-br from-[#10a37f] via-[#0f5d4a] to-[#0b3d2d] text-sm font-semibold text-white transition focus:outline-none focus-visible:ring-2 focus-visible:ring-white/40 focus-visible:ring-offset-2 focus-visible:ring-offset-[#0f1014]"
                                aria-label={collapseToggleLabel}
                                aria-expanded={!collapsed}
                            >
                                <span className="pointer-events-none select-none transition-opacity duration-150 group-hover:opacity-0 group-focus-visible:opacity-0">
                                    GA
                                </span>
                                <span className="pointer-events-none absolute inset-0 flex items-center justify-center opacity-0 transition-opacity duration-150 group-hover:opacity-100 group-focus-visible:opacity-100">
                                    <SidebarToggleGlyph className="h-5 w-5 text-white" />
                                </span>
                                <span className={toggleTooltipClass}>
                                    {collapseToggleLabel}
                                </span>
                            </button>
                        ) : (
                            renderBrandBadge()
                        )
                    ) : (
                        <>
                            {renderExpandedHeader()}
                            {onToggleCollapse ? renderExpandedToggle() : null}
                        </>
                    )}
                </div>
                <button
                    type="button"
                    onClick={onStartAnalysis}
                    className={
                        collapsed
                            ? "group flex h-12 w-12 items-center justify-center rounded-xl border border-white/15 bg-white/5 text-white transition hover:border-white/25 hover:bg-white/10"
                            : "group w-full rounded-2xl bg-gradient-to-r from-[#10a37f] via-[#0f7f66] to-[#0b5a45] px-4 py-3 text-sm font-semibold text-white shadow-[0_10px_35px_rgba(16,163,127,0.35)] transition hover:shadow-[0_12px_40px_rgba(16,163,127,0.55)]"
                    }
                >
                    {collapsed ? (
                        <span className="text-2xl leading-none">+</span>
                    ) : (
                        <span className="flex items-center justify-center gap-2">
                            <span className="flex h-5 w-5 items-center justify-center rounded-full bg-white/20 text-base leading-none">
                                +
                            </span>
                            <span>Nowa analiza</span>
                        </span>
                    )}
                </button>
            </div>
            <div className={`flex-1 overflow-y-auto pb-6 ${sectionPadding} ${navSpacing}`}>
                <div className="space-y-3">
                    {!collapsed && (
                        <p className="text-[11px] font-semibold uppercase tracking-[0.32em] text-white/40">
                            Nawigacja
                        </p>
                    )}
                    <SidebarNav
                        items={navItems}
                        activeKey={activeKey}
                        collapsed={collapsed}
                        onNavigate={onNavigate}
                    />
                </div>
            </div>
            <div
                className={`border-t border-white/10 ${sectionPadding} ${
                    collapsed ? "py-5" : "py-6"
                } text-sm`}
            >
                {isAuthenticated ? (
                    <button
                        type="button"
                        onClick={handleLogout}
                        disabled={authLoading}
                        className={`group w-full rounded-2xl border border-white/10 bg-white/5 text-left transition hover:border-white/20 hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60 ${
                            collapsed ? "p-2" : "px-4 py-3"
                        }`}
                    >
                        <div
                            className={`flex ${
                                collapsed ? "flex-col items-center gap-3" : "items-center gap-3"
                            }`}
                        >
                            {authUser?.picture ? (
                                <Image
                                    src={authUser.picture}
                                    alt="Avatar"
                                    width={40}
                                    height={40}
                                    className="h-10 w-10 rounded-full border border-white/20 object-cover"
                                    unoptimized
                                />
                            ) : (
                                <div className="flex h-10 w-10 items-center justify-center rounded-full border border-white/15 bg-white/10 text-sm font-semibold">
                                    {(authUser?.name ?? authUser?.email ?? "U").charAt(0).toUpperCase()}
                                </div>
                            )}
                            {!collapsed && (
                                <div className="flex-1">
                                    <p className="font-semibold text-white">
                                        {authUser?.name ?? authUser?.email ?? "Użytkownik Google"}
                                    </p>
                                    {authUser?.email ? (
                                        <p className="text-xs text-white/60">{authUser.email}</p>
                                    ) : null}
                                    <p className="text-[11px] uppercase tracking-wider text-white/40">
                                        {profileLoading ? "Zapisywanie ustawień..." : "Konto Google"}
                                    </p>
                                </div>
                            )}
                            <svg
                                viewBox="0 0 24 24"
                                fill="none"
                                xmlns="http://www.w3.org/2000/svg"
                                className={`h-4 w-4 text-white/40 transition group-hover:text-white ${
                                    collapsed ? "" : "self-start"
                                }`}
                                aria-hidden
                            >
                                <path
                                    d="M9 5L16 12L9 19"
                                    stroke="currentColor"
                                    strokeWidth="2"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                />
                            </svg>
                            <span className="sr-only">Wyloguj</span>
                        </div>
                    </button>
                ) : (
                    <div
                        className={`space-y-3 ${
                            collapsed ? "text-center text-[11px]" : "text-sm"
                        } text-white/70`}
                    >
                        <button
                            className={`w-full rounded-2xl border border-white/15 bg-white/5 font-semibold text-white transition hover:border-white/25 hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60 ${
                                collapsed ? "py-2" : "px-4 py-3"
                            }`}
                            onClick={() => openAuthDialog("login")}
                            disabled={authLoading}
                        >
                            {collapsed ? "Zaloguj" : "Zaloguj się"}
                        </button>
                        <button
                            className={`w-full rounded-2xl bg-white font-semibold text-[#0f172a] shadow transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60 ${
                                collapsed ? "py-2" : "px-4 py-3"
                            }`}
                            onClick={() => openAuthDialog("signup")}
                            disabled={authLoading}
                        >
                            {collapsed ? "Rejestracja" : "Załóż konto"}
                        </button>
                        <p className={collapsed ? "text-[10px]" : "text-xs"}>
                            Historia ustawień jest zapisywana w Twoim koncie Google.
                        </p>
                        {!googleClientId && (
                            <p className={`text-amber-200 ${collapsed ? "text-[10px]" : "text-xs"}`}>
                                Ustaw zmienną NEXT_PUBLIC_GOOGLE_CLIENT_ID, aby włączyć logowanie.
                            </p>
                        )}
                    </div>
                )}
                {(authError || profileError) && (
                    <p className={`mt-4 text-xs text-rose-200 ${collapsed ? "text-center" : ""}`}>
                        {authError ?? profileError}
                    </p>
                )}
            </div>
        </div>
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
    "rounded-xl border border-soft bg-surface px-3 py-2 text-neutral focus:outline-none focus:border-[var(--color-tech)] focus:ring-2 focus:ring-[rgba(52,152,219,0.15)] disabled:cursor-not-allowed disabled:opacity-60";

const toRatio = (value: number) => {
    const abs = Math.abs(value);
    if (abs === 0) return 0;
    return abs > 1 ? value / 100 : value;
};

const formatPercent = (value: number, fractionDigits = 2) =>
    `${(toRatio(value) * 100).toFixed(fractionDigits)}%`;

const formatNumber = (value: number, fractionDigits = 2) =>
    value.toLocaleString("pl-PL", {
        minimumFractionDigits: fractionDigits,
        maximumFractionDigits: fractionDigits,
    });

const formatSignedNumber = (value: number, fractionDigits = 2) => {
    if (Math.abs(value) < 1e-9) {
        return formatNumber(0, fractionDigits);
    }
    const sign = value > 0 ? "+" : "-";
    return `${sign}${formatNumber(Math.abs(value), fractionDigits)}`;
};

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

type RebalanceTimelineItem = {
    event: PortfolioRebalanceEvent;
    date: Date | null;
    value?: number;
    change?: number;
    changePct?: number;
};

type RebalanceTimelineGroup = {
    year: string;
    items: RebalanceTimelineItem[];
};

function RebalanceTimeline({
    events,
    equity,
}: {
    events: PortfolioRebalanceEvent[];
    equity: PortfolioPoint[];
}) {
    const [selected, setSelected] = useState<RebalanceTimelineItem | null>(null);

    const dateFormatter = useMemo(
        () =>
            new Intl.DateTimeFormat("en-US", {
                month: "long",
                day: "numeric",
                year: "numeric",
            }),
        []
    );

    const groups = useMemo<RebalanceTimelineGroup[]>(() => {
        if (!events.length) return [];

        const parseDate = (value?: string): Date | null => {
            if (!value) return null;
            const direct = new Date(value);
            if (!Number.isNaN(direct.getTime())) return direct;
            const normalized = new Date(`${value}T00:00:00`);
            if (!Number.isNaN(normalized.getTime())) return normalized;
            return null;
        };

        const equityPoints = equity
            .map((point) => {
                const date = parseDate(point.date);
                if (!date) return null;
                const key = point.date.slice(0, 10);
                return { ...point, date, key };
            })
            .filter(Boolean) as { date: Date; key: string; value: number }[];

        equityPoints.sort((a, b) => a.date.getTime() - b.date.getTime());

        const valueByKey = new Map<string, number>();
        equityPoints.forEach((point) => {
            if (!valueByKey.has(point.key)) {
                valueByKey.set(point.key, point.value);
            }
        });

        const enriched = events.map((event) => {
            const date = parseDate(event.date);
            const key = event.date?.slice(0, 10);
            let value = key ? valueByKey.get(key) : undefined;

            if (value === undefined && date) {
                let candidateValue: number | undefined;
                let candidateTime = -Infinity;
                equityPoints.forEach((point) => {
                    const time = point.date.getTime();
                    if (time <= date.getTime() && time > candidateTime) {
                        candidateTime = time;
                        candidateValue = point.value;
                    }
                });
                value = candidateValue;
            }

            return { event, date, value };
        });

        enriched.sort((a, b) => {
            const timeA = a.date ? a.date.getTime() : Number.POSITIVE_INFINITY;
            const timeB = b.date ? b.date.getTime() : Number.POSITIVE_INFINITY;
            return timeA - timeB;
        });

        let previousValue: number | undefined;
        const grouped = new Map<string, RebalanceTimelineItem[]>();

        enriched.forEach((item) => {
            const { event, date, value } = item;

            let change: number | undefined;
            let changePct: number | undefined;

            if (value !== undefined && previousValue !== undefined) {
                change = value - previousValue;
                if (Math.abs(previousValue) > 1e-9) {
                    changePct = change / previousValue;
                }
            }

            if (value !== undefined) {
                previousValue = value;
            }

            const yearKey = date ? String(date.getFullYear()) : "Pozostałe";
            if (!grouped.has(yearKey)) {
                grouped.set(yearKey, []);
            }
            grouped.get(yearKey)!.push({ event, date, value, change, changePct });
        });

        const sortedGroups = Array.from(grouped.entries())
            .map(([year, items]) => ({ year, items }))
            .sort((a, b) => {
                const aNum = Number(a.year);
                const bNum = Number(b.year);
                const aIsNum = Number.isFinite(aNum);
                const bIsNum = Number.isFinite(bNum);
                if (aIsNum && bIsNum) return aNum - bNum;
                if (aIsNum) return -1;
                if (bIsNum) return 1;
                return a.year.localeCompare(b.year);
            });

        return sortedGroups;
    }, [equity, events]);

    useEffect(() => {
        if (!selected) return;
        const onKeyDown = (event: KeyboardEvent) => {
            if (event.key === "Escape") {
                setSelected(null);
            }
        };
        window.addEventListener("keydown", onKeyDown);
        const previousOverflow = document.body.style.overflow;
        document.body.style.overflow = "hidden";
        return () => {
            window.removeEventListener("keydown", onKeyDown);
            document.body.style.overflow = previousOverflow;
        };
    }, [selected]);

    if (!groups.length) return null;

    const closeModal = () => setSelected(null);

    return (
        <div className="space-y-4">
            <div className="text-sm text-muted font-medium">Historia transakcji</div>
            <div className="space-y-6">
                {groups.map((group) => (
                    <div key={group.year} className="space-y-3">
                        <div className="text-lg font-semibold text-primary">{group.year}</div>
                        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                            {group.items.map((item, idx) => {
                                const { event, value, change, changePct, date } = item;
                                const formattedDate = date ? dateFormatter.format(date) : event.date;
                                const changeClass =
                                    changePct === undefined
                                        ? "text-subtle"
                                        : changePct > 0
                                            ? "text-accent"
                                            : changePct < 0
                                                ? "text-negative"
                                                : "text-subtle";
                                const hasChange = changePct !== undefined;
                                const changeText = hasChange
                                    ? `${formatSignedNumber(change ?? 0)} (${formatPercent(changePct, 2)})`
                                    : null;
                                const turnoverText =
                                    typeof event.turnover === "number"
                                        ? `Obrót ${formatPercent(event.turnover, 1)}`
                                        : null;

                                return (
                                    <button
                                        key={`${event.date}-${idx}`}
                                        type="button"
                                        onClick={() => setSelected(item)}
                                        className="group w-full rounded-2xl border border-soft bg-soft-surface/70 p-5 text-left shadow-sm transition hover:-translate-y-0.5 hover:border-[var(--color-primary)] hover:bg-white hover:shadow-lg focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-primary)]/40"
                                    >
                                        <div className="flex items-start justify-between gap-3">
                                            <div className="text-sm font-semibold text-primary">{formattedDate}</div>
                                            {changeText && (
                                                <div className={`text-xs font-semibold ${changeClass}`}>
                                                    {changeText}
                                                </div>
                                            )}
                                        </div>
                                        <div className="mt-3 text-2xl font-semibold text-neutral">
                                            {typeof value === "number" ? formatNumber(value, 2) : "—"}
                                        </div>
                                        {event.reason && (
                                            <div className="mt-2 line-clamp-2 text-xs text-muted">{event.reason}</div>
                                        )}
                                        <div className="mt-3 flex flex-wrap items-center gap-3 text-xs text-subtle">
                                            <span>{event.trades?.length ?? 0} trans.</span>
                                            {turnoverText && <span>• {turnoverText}</span>}
                                        </div>
                                        <div className="mt-4 inline-flex items-center gap-1 text-sm font-semibold text-primary transition group-hover:gap-2">
                                            <span>Transakcje</span>
                                            <span aria-hidden>→</span>
                                        </div>
                                    </button>
                                );
                            })}
                        </div>
                    </div>
                ))}
            </div>

            {selected && (
                <div className="fixed inset-0 z-40 flex items-center justify-center px-4 py-8">
                    <div className="absolute inset-0 bg-black/50 backdrop-blur-sm" onClick={closeModal} />
                    <div className="relative z-50 max-h-[90vh] w-full max-w-5xl overflow-hidden rounded-3xl bg-white p-6 shadow-2xl">
                        <div className="flex flex-wrap items-start justify-between gap-4">
                            <div>
                                <div className="text-xs uppercase tracking-wide text-muted">{selected.event.date}</div>
                                <div className="mt-1 text-2xl font-semibold text-neutral">
                                    {typeof selected.value === "number"
                                        ? formatNumber(selected.value, 2)
                                        : "—"}
                                </div>
                                {selected.changePct !== undefined && (
                                    <div
                                        className={`mt-2 text-sm font-semibold ${
                                            selected.changePct > 0
                                                ? "text-accent"
                                                : selected.changePct < 0
                                                    ? "text-negative"
                                                    : "text-subtle"
                                        }`}
                                    >
                                        {formatSignedNumber(selected.change ?? 0)} (
                                        {formatPercent(selected.changePct, 2)})
                                    </div>
                                )}
                                {selected.event.reason && (
                                    <div className="mt-3 text-sm text-muted">{selected.event.reason}</div>
                                )}
                                {typeof selected.event.turnover === "number" && (
                                    <div className="mt-1 text-xs text-subtle">
                                        Obrót: {formatPercent(selected.event.turnover, 2)}
                                    </div>
                                )}
                            </div>
                            <button
                                type="button"
                                onClick={closeModal}
                                className="rounded-full border border-soft px-3 py-1 text-sm font-medium text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                            >
                                Zamknij
                            </button>
                        </div>
                        <div className="mt-6 overflow-x-auto">
                            {selected.event.trades && selected.event.trades.length > 0 ? (
                                <table className="min-w-full text-xs md:text-sm">
                                    <thead className="text-left text-subtle">
                                        <tr>
                                            <th className="py-2 pr-4 font-medium">Spółka</th>
                                            <th className="py-2 pr-4 font-medium">Akcja</th>
                                            <th className="py-2 pr-4 font-medium">Wartość</th>
                                            <th className="py-2 pr-4 font-medium">Cena</th>
                                            <th className="py-2 pr-4 font-medium">Zmiana akcji</th>
                                            <th className="py-2 pr-4 font-medium">Doc. akcji</th>
                                            <th className="py-2 pr-4 font-medium">Waga docelowa</th>
                                            <th className="py-2 pr-4 font-medium">Notatka</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {selected.event.trades.map((trade, tradeIdx) => (
                                            <tr key={`${trade.symbol}-${tradeIdx}`} className="border-t border-soft">
                                                <td className="py-2 pr-4 font-semibold text-primary">{trade.symbol}</td>
                                                <td className="py-2 pr-4 capitalize">{trade.action ?? "—"}</td>
                                                <td className="py-2 pr-4">
                                                    {typeof trade.value_change === "number"
                                                        ? formatSignedNumber(trade.value_change, 2)
                                                        : "—"}
                                                </td>
                                                <td className="py-2 pr-4">
                                                    {typeof trade.price === "number"
                                                        ? formatNumber(trade.price, 2)
                                                        : "—"}
                                                </td>
                                                <td className="py-2 pr-4">
                                                    {typeof trade.shares_change === "number"
                                                        ? formatSignedNumber(trade.shares_change, 2)
                                                        : "—"}
                                                </td>
                                                <td className="py-2 pr-4">
                                                    {typeof trade.shares_after === "number"
                                                        ? formatNumber(trade.shares_after, 2)
                                                        : "—"}
                                                </td>
                                                <td className="py-2 pr-4">
                                                    {typeof trade.target_weight === "number"
                                                        ? formatPercent(trade.target_weight, 2)
                                                        : "—"}
                                                </td>
                                                <td className="py-2 pr-4 text-subtle">{trade.note ?? "—"}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            ) : (
                                <div className="rounded-2xl border border-soft bg-soft-surface p-4 text-sm text-muted">
                                    Brak szczegółów transakcji dla tego dnia.
                                </div>
                            )}
                        </div>
                    </div>
                </div>
            )}
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
        <div className="grid grid-cols-2 gap-4 text-sm md:grid-cols-[1fr_1.5fr_.75fr_.75fr]">
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
    disabled = false,
}: {
    onPick: (symbol: string) => void;
    placeholder?: string;
    inputClassName?: string;
    disabled?: boolean;
}) {
    const [q, setQ] = useState("");
    const [list, setList] = useState<SymbolRow[]>([]);
    const [open, setOpen] = useState(false);
    const [idx, setIdx] = useState(-1);
    const [loading, setLoading] = useState(false);

    // debounce
    useEffect(() => {
        if (disabled) {
            setList([]);
            setOpen(false);
            setIdx(-1);
            return;
        }
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
    }, [disabled, q]);

    useEffect(() => {
        if (!disabled) return;
        setQ("");
    }, [disabled]);

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
                onFocus={() => !disabled && list.length && setOpen(true)}
                onKeyDown={onKeyDown}
                placeholder={placeholder}
                disabled={disabled}
                aria-disabled={disabled}
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

type ComparisonSeries = {
    symbol: string;
    label?: string;
    color: string;
    rows: Row[];
};

function PriceChart({
    rows,
    showArea,
    showSMA,
    brushDataRows,
    brushRange,
    onBrushChange,
    primarySymbol,
    comparisonSeries,
}: {
    rows: RowSMA[];
    showArea: boolean;
    showSMA: boolean;
    brushDataRows?: RowSMA[];
    brushRange?: BrushStartEndIndex | null;
    onBrushChange?: (range: BrushStartEndIndex) => void;
    primarySymbol?: string | null;
    comparisonSeries?: ComparisonSeries[];
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

    const comparisonDescriptors = useMemo(() => {
        if (!comparisonSeries?.length) return [];
        return comparisonSeries.map((series) => {
            const map = new Map<string, Row>();
            for (const row of series.rows) {
                map.set(row.date, row);
            }
            return {
                ...series,
                start: series.rows[0]?.close ?? 0,
                map,
                hasData: series.rows.length > 0,
            };
        });
    }, [comparisonSeries]);

    const chartData: PriceChartComparisonPoint[] = useMemo(() => {
        if (!rows.length) return [];
        const base = rows[0].close || 0;
        return rows.map((row) => {
            const change = row.close - base;
            const changePct = base !== 0 ? (change / base) * 100 : 0;
            const merged: PriceChartComparisonPoint = {
                ...row,
                change,
                changePct,
            };
            for (const descriptor of comparisonDescriptors) {
                const match = descriptor.map.get(row.date) ?? null;
                const pctKey = `${descriptor.symbol}__pct` as ComparisonValueKey;
                const closeKey = `${descriptor.symbol}__close` as ComparisonValueKey;
                if (match) {
                    const diff = match.close - (descriptor.start || 0);
                    merged[pctKey] = descriptor.start !== 0 ? (diff / descriptor.start) * 100 : 0;
                    merged[closeKey] = match.close;
                } else {
                    merged[pctKey] = null;
                    merged[closeKey] = null;
                }
            }
            return merged;
        });
    }, [comparisonDescriptors, rows]);

    const brushChartData: PriceChartPoint[] | null = useMemo(() => {
        if (!brushDataRows?.length) return null;
        const base = brushDataRows[0].close || 0;
        return brushDataRows.map((row) => {
            const change = row.close - base;
            const changePct = base !== 0 ? (change / base) * 100 : 0;
            return { ...row, change, changePct };
        });
    }, [brushDataRows]);

    const hasComparisons = comparisonDescriptors.some((descriptor) => descriptor.hasData);

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
        (value: number) =>
            hasComparisons ? `${percentFormatter.format(value)}%` : priceFormatter.format(value),
        [hasComparisons, percentFormatter, priceFormatter]
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
    const [hoverPoint, setHoverPoint] = useState<SelectionPoint | null>(null);
    const lastKnownPointRef = useRef<SelectionPoint | null>(null);
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
            const point =
                getPointFromState(state as MouseHandlerDataParam) ??
                lastKnownPointRef.current;
            if (!point) return;
            setSelection({ start: point, end: point });
            setIsSelecting(true);
            lastKnownPointRef.current = point;
        },
        [getPointFromState]
    );

    const handleChartMouseMove = useCallback<CategoricalChartFunc>(
        (state) => {
            if (!state) {
                setHoverPoint(null);
                return;
            }
            const point = getPointFromState(state as MouseHandlerDataParam);
            if (!point) return;
            setHoverPoint(point);
            lastKnownPointRef.current = point;
            if (isSelecting || selection) {
                updateSelectionEnd(point);
            }
        },
        [getPointFromState, isSelecting, selection, updateSelectionEnd]
    );

    const handleChartMouseUp = useCallback<CategoricalChartFunc>(
        (state) => {
            if (!isSelecting) return;
            const point = state
                ? getPointFromState(state as MouseHandlerDataParam)
                : null;
            const nextPoint = point ?? lastKnownPointRef.current;
            if (nextPoint) {
                updateSelectionEnd(nextPoint);
                lastKnownPointRef.current = nextPoint;
            }
            setIsSelecting(false);
        },
        [getPointFromState, isSelecting, updateSelectionEnd]
    );

    const handleChartMouseLeave = useCallback(() => {
        setIsSelecting(false);
        setHoverPoint(null);
    }, []);

    useEffect(() => {
        setSelection(null);
        setHoverPoint(null);
        setIsSelecting(false);
        lastKnownPointRef.current = null;
    }, [rows]);

    const selectionStart = selection?.start ?? null;
    const selectionEnd = selection?.end ?? null;
    const selectionStartPoint = selectionStart?.point ?? null;
    const selectionEndPoint = selectionEnd?.point ?? null;
    const hoverSelectionPoint = hoverPoint?.point ?? null;
    const explicitSelection = Boolean(selectionStart && selectionEnd);
    const baseStartPoint = rows.length ? rows[0] : null;
    const anchorPoint = selectionStartPoint ?? baseStartPoint;
    const targetPoint = selectionEndPoint ?? hoverSelectionPoint;
    const hasComparisonRange = Boolean(anchorPoint && targetPoint);
    const selectionChange =
        anchorPoint && targetPoint ? targetPoint.close - anchorPoint.close : 0;
    const selectionBase = anchorPoint?.close ?? 0;
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
    const selectionStartLabel = anchorPoint
        ? tooltipDateFormatter.format(new Date(anchorPoint.date))
        : "";
    const selectionEndLabel = targetPoint
        ? tooltipDateFormatter.format(new Date(targetPoint.date))
        : "";
    const selectionStartPrice = anchorPoint ? priceFormatter.format(anchorPoint.close) : "";
    const selectionEndPrice = targetPoint ? priceFormatter.format(targetPoint.close) : "";
    const selectionColor = selectionIsZero
        ? "#94A3B8"
        : selectionChange > 0
            ? "#1DB954"
            : "#EA4335";
    const tooltipLeft = selectionEnd?.x ?? hoverPoint?.x ?? null;
    const tooltipLeftClamped =
        tooltipLeft !== null && containerWidth > 0
            ? Math.min(Math.max(tooltipLeft, 72), containerWidth - 72)
            : tooltipLeft;

    type SummaryItem = {
        key: string;
        symbol: string;
        label: string;
        price: number;
        change: number;
        changePct: number;
        color: string;
    };

    const summaryItems: SummaryItem[] = useMemo(() => {
        const items: SummaryItem[] = [];
        if (primarySymbol && rows.length) {
            const first = rows[0].close;
            const last = rows[rows.length - 1].close;
            items.push({
                key: primarySymbol,
                symbol: primarySymbol,
                label: primarySymbol,
                price: last,
                change: last - first,
                changePct: first !== 0 ? ((last - first) / first) * 100 : 0,
                color: strokeColor,
            });
        }
        for (const descriptor of comparisonDescriptors) {
            if (!descriptor.rows.length) continue;
            const first = descriptor.rows[0].close;
            const last = descriptor.rows[descriptor.rows.length - 1].close;
            items.push({
                key: descriptor.symbol,
                symbol: descriptor.symbol,
                label: descriptor.label ?? descriptor.symbol,
                price: last,
                change: last - first,
                changePct: first !== 0 ? ((last - first) / first) * 100 : 0,
                color: descriptor.color,
            });
        }
        return items;
    }, [comparisonDescriptors, primarySymbol, rows, strokeColor]);

    const baseDataKey = hasComparisons ? "changePct" : "close";
    const effectiveShowArea = showArea && !hasComparisons;
    const effectiveShowSma = showSMA && !hasComparisons;

    const priceLine = (
        <Line
            type="monotone"
            dataKey={baseDataKey}
            stroke={strokeColor}
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 4, strokeWidth: 2, stroke: "#fff" }}
            connectNulls
            isAnimationActive={false}
        />
    );
    const smaLine =
        effectiveShowSma && (
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
    const comparisonLines = hasComparisons
        ? comparisonDescriptors
              .filter((descriptor) => descriptor.hasData)
              .map((descriptor) => (
                <Line
                    key={descriptor.symbol}
                    type="monotone"
                    dataKey={`${descriptor.symbol}__pct`}
                    stroke={descriptor.color}
                    strokeWidth={2}
                    dot={false}
                    connectNulls
                    isAnimationActive={false}
                />
            ))
        : null;

    return (
        <div className="space-y-4">
            <div ref={chartContainerRef} className="relative h-80">
                {hasComparisonRange && tooltipLeftClamped !== null && (
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
                    {effectiveShowArea ? (
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
                            {explicitSelection && selectionStartPoint && selectionEndPoint && (
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
                            {explicitSelection && selectionStartPoint && selectionEndPoint && (
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
                            {comparisonLines}
                            {smaLine}
                        </LineChart>
                    )}
                </ResponsiveContainer>
            </div>
            {summaryItems.length > 0 && (
                <div className="rounded-lg border border-soft bg-white/90 p-4 text-sm shadow-sm">
                    <div className="mb-3 text-xs font-semibold uppercase tracking-wide text-muted">
                        Porównanie wyników
                    </div>
                    <div className="space-y-2">
                        {summaryItems.map((item) => {
                            const changeIsZero = Math.abs(item.change) < 1e-10;
                            const changeSign = item.change > 0 ? "+" : item.change < 0 ? "-" : "";
                            const changeColor = changeIsZero
                                ? "text-subtle"
                                : item.change > 0
                                    ? "text-accent"
                                    : "text-negative";
                            const changeValue = changeIsZero
                                ? priceFormatter.format(0)
                                : `${changeSign}${priceFormatter.format(Math.abs(item.change))}`;
                            const changePctValue = changeIsZero
                                ? percentFormatter.format(0)
                                : `${changeSign}${percentFormatter.format(Math.abs(item.changePct))}`;
                            return (
                                <div
                                    key={item.key}
                                    className="flex flex-wrap items-center gap-x-6 gap-y-2"
                                >
                                    <div className="flex min-w-[120px] items-center gap-2">
                                        <span
                                            className="h-2.5 w-2.5 rounded-full"
                                            style={{ backgroundColor: item.color }}
                                        />
                                        <span className="font-semibold text-neutral">{item.label}</span>
                                    </div>
                                    <div className="text-muted">
                                        {priceFormatter.format(item.price)}
                                    </div>
                                    <div className={`font-medium ${changeColor}`}>
                                        {changeValue}
                                    </div>
                                    <div className={`font-medium ${changeColor}`}>
                                        {changePctValue}%
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}
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
    return <AnalyticsDashboard view="analysis" />;
}

export function AnalyticsDashboard({ view }: { view: DashboardView }) {
    const defaultScoreDraft = useMemo(() => getDefaultScoreDraft(), []);
    const defaultPortfolioDraft = useMemo(() => getDefaultPortfolioDraft(), []);

    const [authUser, setAuthUser] = useState<AuthUser | null>(null);
    const [authLoading, setAuthLoading] = useState(true);
    const [authError, setAuthError] = useState<string | null>(null);
    const [profileLoading, setProfileLoading] = useState(false);
    const [profileError, setProfileError] = useState<string | null>(null);
    const [profileHydrated, setProfileHydrated] = useState(false);
    const [googleLoaded, setGoogleLoaded] = useState(false);
    const googleInitializedRef = useRef(false);
    const lastSavedPreferencesRef = useRef<string | null>(null);
    const [authDialogOpen, setAuthDialogOpen] = useState(false);
    const [authDialogMode, setAuthDialogMode] = useState<"login" | "signup">("login");
    const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
    const [sidebarMobileOpen, setSidebarMobileOpen] = useState(false);

    const [watch, setWatch] = useState<string[]>(() => [...DEFAULT_WATCHLIST]);
    const [symbol, setSymbol] = useState<string | null>(DEFAULT_WATCHLIST[0] ?? null);
    const [period, setPeriod] = useState<ChartPeriod>(365);
    const [area, setArea] = useState(true);
    const [smaOn, setSmaOn] = useState(true);

    const [rows, setRows] = useState<Row[]>([]);
    const [allRows, setAllRows] = useState<Row[]>([]);
    const [brushRange, setBrushRange] = useState<BrushStartEndIndex | null>(null);
    const [comparisonSymbols, setComparisonSymbols] = useState<string[]>([]);
    const [comparisonAllRows, setComparisonAllRows] = useState<Record<string, Row[]>>({});
    const [comparisonErrors, setComparisonErrors] = useState<Record<string, string>>({});
    const [loading, setLoading] = useState(false);
    const [err, setErr] = useState("");

    useEffect(() => {
        if (typeof document === "undefined") {
            return;
        }
        const { body } = document;
        const previous = body.style.overflow;
        if (sidebarMobileOpen) {
            body.style.overflow = "hidden";
            return () => {
                body.style.overflow = previous;
            };
        }
        body.style.overflow = previous;
        return () => {
            body.style.overflow = previous;
        };
    }, [sidebarMobileOpen]);

    // Score builder
    const [scoreRules, setScoreRules] = useState<ScoreBuilderRule[]>(() => defaultScoreDraft.rules);
    const [scoreNameInput, setScoreNameInput] = useState(defaultScoreDraft.name);
    const [scoreDescription, setScoreDescription] = useState(defaultScoreDraft.description);
    const [scoreLimit, setScoreLimit] = useState(defaultScoreDraft.limit);
    const [scoreSort, setScoreSort] = useState<"asc" | "desc">(defaultScoreDraft.sort);
    const [scoreUniverse, setScoreUniverse] = useState(defaultScoreDraft.universe);
    const [scoreUniverseFallback, setScoreUniverseFallback] = useState<string[]>(
        () => [...SCORE_UNIVERSE_FALLBACK]
    );
    const [scoreAsOf, setScoreAsOf] = useState(defaultScoreDraft.asOf);
    const [scoreMinMcap, setScoreMinMcap] = useState(defaultScoreDraft.minMcap);
    const [scoreMinTurnover, setScoreMinTurnover] = useState(defaultScoreDraft.minTurnover);
    const [scoreResults, setScoreResults] = useState<ScorePreviewResult | null>(null);
    const [scoreLoading, setScoreLoading] = useState(false);
    const [scoreError, setScoreError] = useState("");
    const [scoreTemplates, setScoreTemplates] = useState<ScoreTemplate[]>([]);
    const [editingTemplateId, setEditingTemplateId] = useState<string | null>(null);
    const [scoreTemplateFeedback, setScoreTemplateFeedback] = useState<
        { type: "success" | "error"; message: string } | null
    >(null);
    const templatesHydratedRef = useRef(false);

    const scoreComponents = useMemo(() => buildScoreComponents(scoreRules), [scoreRules]);
    const scoreTotalWeight = scoreComponents.reduce((acc, component) => acc + component.weight, 0);
    const scoreLimitInvalid = !Number.isFinite(scoreLimit) || scoreLimit <= 0;
    const scoreDisabled = scoreLoading || scoreLimitInvalid || !scoreComponents.length;
    const editingTemplate = useMemo(
        () =>
            editingTemplateId
                ? scoreTemplates.find((tpl) => tpl.id === editingTemplateId) ?? null
                : null,
        [editingTemplateId, scoreTemplates]
    );

    const isAuthenticated = Boolean(authUser);
    useEffect(() => {
        if (isAuthenticated) {
            setAuthDialogOpen(false);
        }
    }, [isAuthenticated]);
    const useLocalTemplates = !isAuthenticated;

    const openAuthDialog = useCallback((mode: "login" | "signup") => {
        setAuthDialogMode(mode);
        setAuthDialogOpen(true);
        setAuthError(null);
        setProfileError(null);
    }, []);

    const closeAuthDialog = useCallback(() => {
        setAuthDialogOpen(false);
    }, []);

    const resetToDefaults = useCallback(() => {
        const freshScoreDraft = getDefaultScoreDraft();
        const freshPortfolioDraft = getDefaultPortfolioDraft();
        setWatch([...DEFAULT_WATCHLIST]);
        setSymbol(DEFAULT_WATCHLIST[0] ?? null);
        setScoreTemplates([]);
        setScoreRules(freshScoreDraft.rules.map((rule) => ({ ...rule })));
        setScoreNameInput(freshScoreDraft.name);
        setScoreDescription(freshScoreDraft.description);
        setScoreLimit(freshScoreDraft.limit);
        setScoreSort(freshScoreDraft.sort);
        setScoreUniverse(freshScoreDraft.universe);
        setScoreAsOf(freshScoreDraft.asOf);
        setScoreMinMcap(freshScoreDraft.minMcap);
        setScoreMinTurnover(freshScoreDraft.minTurnover);
        setPfMode(freshPortfolioDraft.mode);
        setPfRows(freshPortfolioDraft.rows.map((row) => ({ ...row })));
        setPfStart(freshPortfolioDraft.start);
        setPfEnd(freshPortfolioDraft.end);
        setPfInitial(freshPortfolioDraft.initial);
        setPfFee(freshPortfolioDraft.fee);
        setPfThreshold(freshPortfolioDraft.threshold);
        setPfBenchmark(freshPortfolioDraft.benchmark);
        setPfFreq(freshPortfolioDraft.frequency);
        setPfScoreName(freshPortfolioDraft.score.name);
        setPfScoreLimit(freshPortfolioDraft.score.limit);
        setPfScoreWeighting(freshPortfolioDraft.score.weighting);
        setPfScoreDirection(freshPortfolioDraft.score.direction);
        setPfScoreUniverse(freshPortfolioDraft.score.universe);
        setPfScoreMin(freshPortfolioDraft.score.min);
        setPfScoreMax(freshPortfolioDraft.score.max);
        setPfComparisonSymbols([...freshPortfolioDraft.comparisons]);
        lastSavedPreferencesRef.current = null;
        setProfileHydrated(false);
        setProfileLoading(false);
        setAuthLoading(false);
    }, []);

    const hydrateFromPreferences = useCallback(
        (preferences: PersistedPreferences) => {
            const safeWatch = preferences.watchlist.length
                ? preferences.watchlist
                : [...DEFAULT_WATCHLIST];
            setWatch(safeWatch);
            setSymbol((prev) => (prev && safeWatch.includes(prev) ? prev : safeWatch[0] ?? null));
            setScoreTemplates(
                preferences.scoreTemplates.map((tpl) => ({
                    ...tpl,
                    rules: tpl.rules.map((rule) => ({ ...rule })),
                }))
            );

            const incomingScoreDraft = preferences.scoreDraft ?? defaultScoreDraft;
            const scoreRulesSource =
                incomingScoreDraft.rules && incomingScoreDraft.rules.length
                    ? incomingScoreDraft.rules
                    : defaultScoreDraft.rules;
            setScoreRules(scoreRulesSource.map((rule) => ({ ...rule })));
            setScoreNameInput(
                incomingScoreDraft.name?.trim().length
                    ? incomingScoreDraft.name
                    : defaultScoreDraft.name
            );
            setScoreDescription(
                incomingScoreDraft.description?.trim().length
                    ? incomingScoreDraft.description
                    : defaultScoreDraft.description
            );
            setScoreLimit(
                incomingScoreDraft.limit && incomingScoreDraft.limit > 0
                    ? incomingScoreDraft.limit
                    : defaultScoreDraft.limit
            );
            setScoreSort(incomingScoreDraft.sort === "asc" ? "asc" : "desc");
            setScoreUniverse(incomingScoreDraft.universe ?? "");
            setScoreAsOf(incomingScoreDraft.asOf || defaultScoreDraft.asOf);
            setScoreMinMcap(incomingScoreDraft.minMcap ?? "");
            setScoreMinTurnover(incomingScoreDraft.minTurnover ?? "");

            const incomingPortfolio = preferences.portfolioDraft ?? defaultPortfolioDraft;
            const portfolioRowsSource =
                incomingPortfolio.rows && incomingPortfolio.rows.length
                    ? incomingPortfolio.rows
                    : defaultPortfolioDraft.rows;
            setPfMode(incomingPortfolio.mode === "score" ? "score" : "manual");
            setPfRows(portfolioRowsSource.map((row) => ({ ...row })));
            setPfStart(incomingPortfolio.start || defaultPortfolioDraft.start);
            setPfEnd(incomingPortfolio.end || defaultPortfolioDraft.end);
            setPfInitial(
                incomingPortfolio.initial && incomingPortfolio.initial > 0
                    ? incomingPortfolio.initial
                    : defaultPortfolioDraft.initial
            );
            setPfFee(
                typeof incomingPortfolio.fee === "number"
                    ? incomingPortfolio.fee
                    : defaultPortfolioDraft.fee
            );
            setPfThreshold(
                typeof incomingPortfolio.threshold === "number"
                    ? incomingPortfolio.threshold
                    : defaultPortfolioDraft.threshold
            );
            setPfBenchmark(incomingPortfolio.benchmark ?? null);
            setPfFreq(
                incomingPortfolio.frequency === "none"
                    ? "none"
                    : incomingPortfolio.frequency === "quarterly"
                    ? "quarterly"
                    : incomingPortfolio.frequency === "yearly"
                    ? "yearly"
                    : "monthly"
            );
            const scoreSection = incomingPortfolio.score ?? defaultPortfolioDraft.score;
            setPfScoreName(scoreSection.name || defaultPortfolioDraft.score.name);
            setPfScoreLimit(
                scoreSection.limit && scoreSection.limit > 0
                    ? scoreSection.limit
                    : defaultPortfolioDraft.score.limit
            );
            setPfScoreWeighting(scoreSection.weighting || defaultPortfolioDraft.score.weighting);
            setPfScoreDirection(scoreSection.direction === "asc" ? "asc" : "desc");
            setPfScoreUniverse(scoreSection.universe ?? "");
            setPfScoreMin(scoreSection.min ?? "");
            setPfScoreMax(scoreSection.max ?? "");
            setPfComparisonSymbols(
                scoreSection && incomingPortfolio.comparisons?.length
                    ? Array.from(
                          new Set(
                              incomingPortfolio.comparisons.map((item) =>
                                  (item ?? "").toString().trim().toUpperCase()
                              )
                          )
                      ).filter((item) => item)
                    : [...defaultPortfolioDraft.comparisons]
            );

            const snapshot: PersistedPreferences = {
                watchlist: safeWatch,
                scoreTemplates: preferences.scoreTemplates.map((tpl) => ({
                    ...tpl,
                    rules: tpl.rules.map((rule) => ({ ...rule })),
                })),
                scoreDraft: {
                    name: incomingScoreDraft.name || defaultScoreDraft.name,
                    description:
                        incomingScoreDraft.description || defaultScoreDraft.description,
                    limit:
                        incomingScoreDraft.limit && incomingScoreDraft.limit > 0
                            ? incomingScoreDraft.limit
                            : defaultScoreDraft.limit,
                    sort: incomingScoreDraft.sort === "asc" ? "asc" : "desc",
                    universe: incomingScoreDraft.universe ?? "",
                    minMcap: incomingScoreDraft.minMcap ?? "",
                    minTurnover: incomingScoreDraft.minTurnover ?? "",
                    asOf: incomingScoreDraft.asOf || defaultScoreDraft.asOf,
                    rules: scoreRulesSource.map((rule) => ({ ...rule })),
                },
                portfolioDraft: {
                    mode: incomingPortfolio.mode === "score" ? "score" : "manual",
                    rows: portfolioRowsSource.map((row) => ({ ...row })),
                    start: incomingPortfolio.start || defaultPortfolioDraft.start,
                    end: incomingPortfolio.end || defaultPortfolioDraft.end,
                    initial:
                        incomingPortfolio.initial && incomingPortfolio.initial > 0
                            ? incomingPortfolio.initial
                            : defaultPortfolioDraft.initial,
                    fee:
                        typeof incomingPortfolio.fee === "number"
                            ? incomingPortfolio.fee
                            : defaultPortfolioDraft.fee,
                    threshold:
                        typeof incomingPortfolio.threshold === "number"
                            ? incomingPortfolio.threshold
                            : defaultPortfolioDraft.threshold,
                    benchmark: incomingPortfolio.benchmark ?? null,
                    frequency:
                        incomingPortfolio.frequency === "none"
                            ? "none"
                            : incomingPortfolio.frequency === "quarterly"
                            ? "quarterly"
                            : incomingPortfolio.frequency === "yearly"
                            ? "yearly"
                            : "monthly",
                    score: {
                        name: scoreSection.name || defaultPortfolioDraft.score.name,
                        limit:
                            scoreSection.limit && scoreSection.limit > 0
                                ? scoreSection.limit
                                : defaultPortfolioDraft.score.limit,
                        weighting: scoreSection.weighting || defaultPortfolioDraft.score.weighting,
                        direction: scoreSection.direction === "asc" ? "asc" : "desc",
                        universe: scoreSection.universe ?? "",
                        min: scoreSection.min ?? "",
                        max: scoreSection.max ?? "",
                    },
                    comparisons:
                        incomingPortfolio.comparisons?.length
                            ? Array.from(
                                  new Set(
                                      incomingPortfolio.comparisons.map((item) =>
                                          (item ?? "").toString().trim().toUpperCase()
                                      )
                                  )
                              ).filter((item) => item)
                            : [...defaultPortfolioDraft.comparisons],
                },
            };
            lastSavedPreferencesRef.current = JSON.stringify(snapshot);
            setProfileHydrated(true);
        },
        [defaultPortfolioDraft, defaultScoreDraft]
    );

    const fetchProfile = useCallback(async () => {
        setAuthLoading(true);
        setProfileLoading(true);
        try {
            const response = await fetch("/api/account/profile", { cache: "no-store" });
            if (!response.ok) {
                if (response.status === 401) {
                    setAuthUser(null);
                    resetToDefaults();
                    lastSavedPreferencesRef.current = null;
                    return null;
                }
                const text = await response.text();
                throw new Error(
                    text?.trim()
                        ? `Nie udało się pobrać profilu: ${text.trim()}`
                        : `Nie udało się pobrać profilu (status ${response.status})`
                );
            }
            const data = (await response.json()) as PublicUserProfile;
            setAuthUser(data.user);
            setAuthError(null);
            setProfileError(null);
            hydrateFromPreferences(data.preferences);
            return data;
        } catch (error: unknown) {
            const message = error instanceof Error ? error.message : String(error);
            setAuthError(message);
            setProfileError(message);
            return null;
        } finally {
            setAuthLoading(false);
            setProfileLoading(false);
        }
    }, [hydrateFromPreferences, resetToDefaults]);

    useEffect(() => {
        void fetchProfile();
    }, [fetchProfile]);

    const handleGoogleCredential = useCallback(
        async (credential: string | undefined) => {
            if (!credential) {
                setAuthError("Nie otrzymano tokenu Google.");
                return;
            }
            setAuthError(null);
            setProfileError(null);
            try {
                const response = await fetch("/api/auth/google", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ credential }),
                });
                const payload = (await response
                    .json()
                    .catch(() => null)) as { user?: AuthUser; error?: string } | null;
                if (!response.ok || !payload?.user) {
                    throw new Error(payload?.error ?? "Logowanie przez Google nie powiodło się.");
                }
                setAuthUser(payload.user);
                setProfileHydrated(false);
                await fetchProfile();
            } catch (error: unknown) {
                const message = error instanceof Error ? error.message : String(error);
                setAuthError(message);
            }
        },
        [fetchProfile]
    );

    const initializeGoogle = useCallback(() => {
        if (googleInitializedRef.current) {
            return Boolean(window.google?.accounts?.id);
        }
        if (!googleLoaded || !GOOGLE_CLIENT_ID) {
            return false;
        }
        const googleApi = window.google?.accounts?.id;
        if (!googleApi) {
            return false;
        }
        googleApi.initialize({
            client_id: GOOGLE_CLIENT_ID,
            callback: (response) => {
                void handleGoogleCredential(response?.credential);
            },
            ux_mode: "popup",
            auto_select: false,
            cancel_on_tap_outside: true,
        });
        googleInitializedRef.current = true;
        return true;
    }, [googleLoaded, handleGoogleCredential]);

    useEffect(() => {
        if (!googleLoaded) return;
        void initializeGoogle();
    }, [googleLoaded, initializeGoogle]);

    const handleSignInClick = useCallback(() => {
        const ready = initializeGoogle();
        if (!ready) {
            setAuthError("Logowanie Google nie jest dostępne. Sprawdź konfigurację klienta.");
            return false;
        }
        window.google?.accounts?.id?.prompt();
        return true;
    }, [initializeGoogle]);

    const triggerGoogleAuth = useCallback(() => {
        const success = handleSignInClick();
        if (success) {
            setAuthDialogOpen(false);
        }
        return success;
    }, [handleSignInClick]);

    const handleLogout = useCallback(async () => {
        try {
            await fetch("/api/auth/logout", { method: "POST" });
        } catch {
            // ignoruj błędy wylogowania
        }
        setAuthUser(null);
        resetToDefaults();
        setAuthError(null);
        setProfileError(null);
        window.google?.accounts?.id?.disableAutoSelect?.();
    }, [resetToDefaults]);

    useEffect(() => {
        if (typeof window === "undefined") return;
        if (!useLocalTemplates) {
            templatesHydratedRef.current = false;
            return;
        }
        templatesHydratedRef.current = false;
        try {
            const stored = window.localStorage.getItem(SCORE_TEMPLATE_STORAGE_KEY);
            if (!stored) {
                setScoreTemplates([]);
                return;
            }
            const parsed: unknown = JSON.parse(stored);
            if (!Array.isArray(parsed)) {
                setScoreTemplates([]);
                return;
            }
            const normalized = (parsed
                .map((item) => {
                    if (!item || typeof item !== "object") return null;
                    const candidate = item as Partial<ScoreTemplate>;
                    if (typeof candidate.id !== "string") return null;
                    const title =
                        typeof candidate.title === "string" && candidate.title.trim()
                            ? candidate.title.trim()
                            : candidate.id;
                    const rulesSource = Array.isArray(candidate.rules)
                        ? candidate.rules
                        : [];
                    const rules = (rulesSource
                        .map((rule) => {
                            if (!rule || typeof rule !== "object") return null;
                            const asRule = rule as Partial<ScoreTemplateRule>;
                            if (typeof asRule.metric !== "string") return null;
                            const transform =
                                asRule.transform === "zscore"
                                    ? "zscore"
                                    : asRule.transform === "percentile"
                                    ? "percentile"
                                    : "raw";
                            return {
                                metric: asRule.metric,
                                weight: Number(asRule.weight) || 0,
                                direction: asRule.direction === "asc" ? "asc" : "desc",
                                label:
                                    typeof asRule.label === "string"
                                        ? asRule.label
                                        : asRule.label === null
                                        ? null
                                        : undefined,
                                transform,
                            } satisfies ScoreTemplateRule;
                        })
                        .filter(Boolean)) as ScoreTemplateRule[];

                    return {
                        id: candidate.id,
                        title,
                        name:
                            typeof candidate.name === "string" && candidate.name.trim()
                                ? candidate.name.trim()
                                : undefined,
                        description:
                            typeof candidate.description === "string" && candidate.description.trim()
                                ? candidate.description.trim()
                                : undefined,
                        rules,
                        limit:
                            typeof candidate.limit === "number" && candidate.limit > 0
                                ? Math.floor(candidate.limit)
                                : Number(candidate.limit) > 0
                                ? Math.floor(Number(candidate.limit))
                                : 10,
                        sort: candidate.sort === "asc" ? "asc" : "desc",
                        universe:
                            typeof candidate.universe === "string" ? candidate.universe : "",
                        minMcap:
                            typeof candidate.minMcap === "string" ? candidate.minMcap : "",
                        minTurnover:
                            typeof candidate.minTurnover === "string"
                                ? candidate.minTurnover
                                : "",
                        createdAt:
                            typeof candidate.createdAt === "string"
                                ? candidate.createdAt
                                : new Date().toISOString(),
                    } satisfies ScoreTemplate;
                })
                .filter(Boolean)) as ScoreTemplate[];
            setScoreTemplates(normalized);
        } catch {
            // ignoruj uszkodzone dane
        } finally {
            templatesHydratedRef.current = true;
        }
    }, [useLocalTemplates]);

    useEffect(() => {
        if (typeof window === "undefined") return;
        if (!useLocalTemplates) return;
        if (!templatesHydratedRef.current) return;
        try {
            window.localStorage.setItem(
                SCORE_TEMPLATE_STORAGE_KEY,
                JSON.stringify(scoreTemplates)
            );
        } catch {
            // ignoruj brak miejsca w localStorage
        }
    }, [scoreTemplates, useLocalTemplates]);

    useEffect(() => {
        if (!editingTemplateId) return;
        if (!scoreTemplates.some((tpl) => tpl.id === editingTemplateId)) {
            setEditingTemplateId(null);
        }
    }, [editingTemplateId, scoreTemplates]);

    useEffect(() => {
        let active = true;
        const loadDefaultUniverse = async () => {
            try {
                const response = await fetch("/api/symbols?limit=50");
                if (!response.ok) {
                    return;
                }
                const data: unknown = await response.json();
                if (!Array.isArray(data)) {
                    return;
                }
                const extracted = data
                    .map((item) => {
                        if (!item || typeof item !== "object") return null;
                        const symbol = (item as { symbol?: unknown }).symbol;
                        if (typeof symbol !== "string") return null;
                        const normalized = symbol.trim().toUpperCase();
                        return normalized ? normalized : null;
                    })
                    .filter((sym): sym is string => Boolean(sym));
                if (!extracted.length || !active) {
                    return;
                }
                setScoreUniverseFallback((prev) => {
                    const merged = new Set<string>([...prev, ...extracted]);
                    return Array.from(merged).slice(0, 100);
                });
            } catch {
                // ignoruj chwilowe błędy sieciowe – fallback pozostanie statyczny
            }
        };
        void loadDefaultUniverse();
        return () => {
            active = false;
        };
    }, []);

    // Portfel
    const [pfMode, setPfMode] = useState<"manual" | "score">(defaultPortfolioDraft.mode);
    const [pfRows, setPfRows] = useState<{ symbol: string; weight: number }[]>(() =>
        defaultPortfolioDraft.rows.map((row) => ({ ...row }))
    );
    const [pfStart, setPfStart] = useState(defaultPortfolioDraft.start);
    const [pfEnd, setPfEnd] = useState(defaultPortfolioDraft.end);
    const [pfInitial, setPfInitial] = useState(defaultPortfolioDraft.initial);
    const [pfFee, setPfFee] = useState(defaultPortfolioDraft.fee);
    const [pfThreshold, setPfThreshold] = useState(defaultPortfolioDraft.threshold);
    const [pfBenchmark, setPfBenchmark] = useState<string | null>(defaultPortfolioDraft.benchmark);
    const [pfLastBenchmark, setPfLastBenchmark] = useState<string | null>(null);
    const [pfFreq, setPfFreq] = useState<Rebalance>(defaultPortfolioDraft.frequency);
    const [pfRes, setPfRes] = useState<PortfolioResp | null>(null);
    const [pfBrushRange, setPfBrushRange] = useState<BrushStartEndIndex | null>(null);
    const [pfTimelineOpen, setPfTimelineOpen] = useState(false);
    const [pfScoreName, setPfScoreName] = useState(defaultPortfolioDraft.score.name);
    const [pfScoreLimit, setPfScoreLimit] = useState(defaultPortfolioDraft.score.limit);
    const [pfScoreWeighting, setPfScoreWeighting] = useState(defaultPortfolioDraft.score.weighting);
    const [pfScoreDirection, setPfScoreDirection] = useState<"asc" | "desc">(
        defaultPortfolioDraft.score.direction
    );
    const [pfScoreUniverse, setPfScoreUniverse] = useState(defaultPortfolioDraft.score.universe);
    const [pfScoreMin, setPfScoreMin] = useState(defaultPortfolioDraft.score.min);
    const [pfScoreMax, setPfScoreMax] = useState(defaultPortfolioDraft.score.max);
    const [pfComparisonSymbols, setPfComparisonSymbols] = useState<string[]>(
        () => [...defaultPortfolioDraft.comparisons]
    );
    const [pfComparisonAllRows, setPfComparisonAllRows] = useState<Record<string, Row[]>>({});
    const [pfComparisonErrors, setPfComparisonErrors] = useState<Record<string, string>>({});
    const [pfPeriod, setPfPeriod] = useState<ChartPeriod>("max");
    const pfTotal = pfRows.reduce((a, b) => a + (Number(b.weight) || 0), 0);
    const pfRangeInvalid = pfStart > pfEnd;
    const [pfLoading, setPfLoading] = useState(false);
    const [pfErr, setPfErr] = useState("");
    const [pfSelectedTemplateId, setPfSelectedTemplateId] = useState<string | null>(null);
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

    const preferencesObject = useMemo<PersistedPreferences>(() => {
        const sanitizedWatch = Array.from(
            new Set(watch.map((item) => item.trim().toUpperCase()).filter((item) => Boolean(item)))
        );
        const sanitizedTemplates = scoreTemplates.map((tpl) => ({
            ...tpl,
            rules: tpl.rules.map((rule) => ({ ...rule })),
        }));
        const scoreDraft: ScoreDraftState = {
            name: scoreNameInput,
            description: scoreDescription,
            limit: scoreLimit,
            sort: scoreSort,
            universe: scoreUniverse,
            minMcap: scoreMinMcap,
            minTurnover: scoreMinTurnover,
            asOf: scoreAsOf,
            rules: scoreRules.map((rule) => ({ ...rule })),
        };
        const portfolioDraft: PortfolioDraftState = {
            mode: pfMode,
            rows: pfRows.map((row) => ({ ...row })),
            start: pfStart,
            end: pfEnd,
            initial: pfInitial,
            fee: pfFee,
            threshold: pfThreshold,
            benchmark: pfBenchmark ?? null,
            frequency: pfFreq,
            score: {
                name: pfScoreName,
                limit: pfScoreLimit,
                weighting: pfScoreWeighting,
                direction: pfScoreDirection,
                universe: pfScoreUniverse,
                min: pfScoreMin,
                max: pfScoreMax,
            },
            comparisons: Array.from(
                new Set(
                    pfComparisonSymbols
                        .map((item) => item.trim().toUpperCase())
                        .filter((item) => Boolean(item))
                )
            ),
        };
        return {
            watchlist: sanitizedWatch,
            scoreTemplates: sanitizedTemplates,
            scoreDraft,
            portfolioDraft,
        };
    }, [
        pfBenchmark,
        pfComparisonSymbols,
        pfEnd,
        pfFee,
        pfFreq,
        pfInitial,
        pfMode,
        pfRows,
        pfScoreDirection,
        pfScoreLimit,
        pfScoreMax,
        pfScoreMin,
        pfScoreName,
        pfScoreUniverse,
        pfScoreWeighting,
        pfStart,
        pfThreshold,
        scoreAsOf,
        scoreDescription,
        scoreLimit,
        scoreMinMcap,
        scoreMinTurnover,
        scoreNameInput,
        scoreRules,
        scoreSort,
        scoreTemplates,
        scoreUniverse,
        watch,
    ]);

    const preferencesJson = useMemo(() => JSON.stringify(preferencesObject), [preferencesObject]);

    useEffect(() => {
        if (!isAuthenticated || !profileHydrated) return;
        if (!preferencesJson) return;
        if (lastSavedPreferencesRef.current === preferencesJson) return;

        const controller = new AbortController();
        const timeout = setTimeout(() => {
            void (async () => {
                try {
                    const response = await fetch("/api/account/profile", {
                        method: "PUT",
                        headers: { "Content-Type": "application/json" },
                        body: preferencesJson,
                        signal: controller.signal,
                    });
                    if (!response.ok) {
                        const text = await response.text();
                        throw new Error(
                            text?.trim()
                                ? `Nie udało się zapisać profilu: ${text.trim()}`
                                : `Nie udało się zapisać profilu (status ${response.status})`
                        );
                    }
                    const data = (await response.json()) as PublicUserProfile;
                    setAuthUser(data.user);
                    lastSavedPreferencesRef.current = preferencesJson;
                    setProfileError(null);
                } catch (error: unknown) {
                    if (error instanceof DOMException && error.name === "AbortError") {
                        return;
                    }
                    const message = error instanceof Error ? error.message : String(error);
                    setProfileError(message);
                    lastSavedPreferencesRef.current = null;
                }
            })();
        }, 750);

        return () => {
            clearTimeout(timeout);
            controller.abort();
        };
    }, [isAuthenticated, preferencesJson, profileHydrated]);

    useEffect(() => {
        if (!pfSelectedTemplateId) return;
        if (!scoreTemplates.some((tpl) => tpl.id === pfSelectedTemplateId)) {
            setPfSelectedTemplateId(null);
        }
    }, [pfSelectedTemplateId, scoreTemplates]);

    useEffect(() => {
        if (!pfSelectedTemplateId) return;
        const tpl = scoreTemplates.find((item) => item.id === pfSelectedTemplateId);
        if (!tpl) return;
        setPfScoreName(tpl.name?.trim() ? tpl.name.trim() : tpl.title);
        setPfScoreDirection(tpl.sort);
        setPfScoreLimit(tpl.limit);
        setPfScoreUniverse(tpl.universe ?? "");
    }, [pfSelectedTemplateId, scoreTemplates]);

    useEffect(() => {
        if (pfPeriod !== "max") {
            setPfBrushRange(null);
        }
    }, [pfPeriod]);

    useEffect(() => {
        setPfBrushRange(null);
    }, [pfRes]);

    useEffect(() => {
        setPfTimelineOpen(false);
    }, [pfRes]);

    const pfPortfolioAllRows = useMemo<Row[]>(
        () => (pfRes ? portfolioPointsToRows(pfRes.equity) : []),
        [pfRes]
    );

    const pfBrushRows = useMemo<RowSMA[]>(
        () => pfPortfolioAllRows.map((row) => ({ ...row, sma: null })),
        [pfPortfolioAllRows]
    );

    const pfVisibleRange = useMemo(() => {
        const baseRange = computeVisibleRangeForRows(pfPortfolioAllRows, pfPeriod);
        if (!baseRange) return null;
        if (pfPeriod !== "max") {
            return baseRange;
        }
        if (!pfBrushRange || !pfPortfolioAllRows.length) {
            return baseRange;
        }
        const total = pfPortfolioAllRows.length;
        const safeStart = Math.max(0, Math.min(pfBrushRange.startIndex, total - 1));
        const safeEnd = Math.max(safeStart, Math.min(pfBrushRange.endIndex, total - 1));
        const startDate = pfPortfolioAllRows[safeStart]?.date ?? baseRange.start;
        const endDate = pfPortfolioAllRows[safeEnd]?.date ?? baseRange.end;
        return { start: startDate, end: endDate };
    }, [pfBrushRange, pfPeriod, pfPortfolioAllRows]);

    const pfPortfolioVisibleRows = useMemo<Row[]>(() => {
        if (!pfVisibleRange) return [];
        return pfPortfolioAllRows.filter(
            (row) => row.date >= pfVisibleRange.start && row.date <= pfVisibleRange.end
        );
    }, [pfPortfolioAllRows, pfVisibleRange]);

    const pfPortfolioRowsWithSma = useMemo<RowSMA[]>(
        () => pfPortfolioVisibleRows.map((row) => ({ ...row, sma: null })),
        [pfPortfolioVisibleRows]
    );

    const pfBenchmarkAllRows = useMemo<Row[]>(
        () => (pfRes?.benchmark?.length ? portfolioPointsToRows(pfRes.benchmark) : []),
        [pfRes]
    );

    const pfBenchmarkVisibleRows = useMemo<Row[]>(() => {
        if (!pfVisibleRange) return [];
        return pfBenchmarkAllRows.filter(
            (row) => row.date >= pfVisibleRange.start && row.date <= pfVisibleRange.end
        );
    }, [pfBenchmarkAllRows, pfVisibleRange]);

    const pfBenchmarkSeries = useMemo<ComparisonSeries | null>(() => {
        if (!pfBenchmarkVisibleRows.length) return null;
        const label = pfLastBenchmark ?? "Benchmark";
        return {
            symbol: label,
            label,
            color: "#2563EB",
            rows: pfBenchmarkVisibleRows,
        };
    }, [pfBenchmarkVisibleRows, pfLastBenchmark]);

    // Quotes loader
    useEffect(() => {
        let live = true;
        if (!symbol) {
            setRows([]);
            setAllRows([]);
            setBrushRange(null);
            setComparisonAllRows({});
            setComparisonErrors({});
            setComparisonSymbols([]);
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

    useEffect(() => {
        if (!symbol) return;
        const normalized = symbol.toUpperCase();
        setComparisonSymbols((prev) => prev.filter((sym) => sym !== normalized));
    }, [symbol]);

    useEffect(() => {
        let live = true;
        if (!symbol || comparisonSymbols.length === 0) {
            setComparisonAllRows({});
            setComparisonErrors({});
            return () => {
                live = false;
            };
        }

        const startISO = computeStartISOForPeriod(period);

        (async () => {
            const results = await Promise.allSettled(
                comparisonSymbols.map((sym) =>
                    fetchQuotes(sym, startISO).then((data) => ({ symbol: sym, data }))
                )
            );
            if (!live) return;

            const nextAll: Record<string, Row[]> = {};
            const nextErrors: Record<string, string> = {};

            results.forEach((result, idx) => {
                const sym = comparisonSymbols[idx];
                if (!sym) return;
                if (result.status === "fulfilled") {
                    nextAll[sym] = result.value.data;
                } else {
                    const reason = result.reason;
                    const message =
                        reason instanceof Error
                            ? reason.message
                            : typeof reason === "string"
                                ? reason
                                : `Nie udało się pobrać danych dla ${sym}`;
                    nextErrors[sym] = message;
                }
            });

            setComparisonAllRows(nextAll);
            setComparisonErrors(nextErrors);
        })();

        return () => {
            live = false;
        };
    }, [comparisonSymbols, period, symbol]);

    const withSma: RowSMA[] = useMemo(
        () => (smaOn ? sma(rows, 20) : rows.map((r) => ({ ...r, sma: undefined }))),
        [rows, smaOn]
    );
    const withRsi: RowRSI[] = useMemo(() => rsi(rows, 14), [rows]);

    const visibleComparisonRows = useMemo(() => {
        if (!rows.length) return {} as Record<string, Row[]>;
        const startDate = rows[0].date;
        const endDate = rows[rows.length - 1].date;
        const next: Record<string, Row[]> = {};
        for (const sym of comparisonSymbols) {
            const series = comparisonAllRows[sym];
            if (!series?.length) continue;
            next[sym] = series.filter((row) => row.date >= startDate && row.date <= endDate);
        }
        return next;
    }, [comparisonAllRows, comparisonSymbols, rows]);

    const comparisonSeriesForChart: ComparisonSeries[] = useMemo(
        () =>
            comparisonSymbols.map((sym, idx) => ({
                symbol: sym,
                label: sym,
                color: COMPARISON_COLORS[idx % COMPARISON_COLORS.length],
                rows: visibleComparisonRows[sym] ?? [],
            })),
        [comparisonSymbols, visibleComparisonRows]
    );

    const comparisonColorMap = useMemo(() => {
        const map: Record<string, string> = {};
        for (const series of comparisonSeriesForChart) {
            map[series.symbol] = series.color;
        }
        return map;
    }, [comparisonSeriesForChart]);

    const brushRows: RowSMA[] = useMemo(
        () => allRows.map((row) => ({ ...row, sma: null })),
        [allRows]
    );

    const handleAddComparison = useCallback(
        (candidate: string) => {
            const normalized = candidate.trim().toUpperCase();
            if (!normalized || !symbol) return;
            if (normalized === symbol.toUpperCase()) return;
            setComparisonSymbols((prev) => {
                if (prev.includes(normalized) || prev.length >= MAX_COMPARISONS) {
                    return prev;
                }
                return [...prev, normalized];
            });
        },
        [symbol]
    );

    const handleRemoveComparison = useCallback((sym: string) => {
        setComparisonSymbols((prev) => prev.filter((item) => item !== sym));
        setComparisonAllRows((prev) => {
            if (!(sym in prev)) return prev;
            const next = { ...prev };
            delete next[sym];
            return next;
        });
        setComparisonErrors((prev) => {
            if (!(sym in prev)) return prev;
            const next = { ...prev };
            delete next[sym];
            return next;
        });
    }, []);

    const handleAddPfComparison = useCallback(
        (candidate: string) => {
            const normalized = candidate.trim().toUpperCase();
            if (!normalized) return;
            if (pfLastBenchmark && normalized === pfLastBenchmark.toUpperCase()) return;
            setPfComparisonSymbols((prev) => {
                if (prev.includes(normalized) || prev.length >= MAX_COMPARISONS) {
                    return prev;
                }
                return [...prev, normalized];
            });
        },
        [pfLastBenchmark]
    );

    const handleRemovePfComparison = useCallback((sym: string) => {
        setPfComparisonSymbols((prev) => prev.filter((item) => item !== sym));
        setPfComparisonAllRows((prev) => {
            if (!(sym in prev)) return prev;
            const next = { ...prev };
            delete next[sym];
            return next;
        });
        setPfComparisonErrors((prev) => {
            if (!(sym in prev)) return prev;
            const next = { ...prev };
            delete next[sym];
            return next;
        });
    }, []);

    const handlePfBrushSelectionChange = useCallback(
        (range: BrushStartEndIndex) => {
            if (pfPeriod !== "max" || !pfPortfolioAllRows.length) return;
            const total = pfPortfolioAllRows.length;
            const safeStart = Math.max(0, Math.min(range.startIndex, total - 1));
            const safeEnd = Math.max(safeStart, Math.min(range.endIndex, total - 1));
            setPfBrushRange((current) => {
                if (
                    current &&
                    current.startIndex === safeStart &&
                    current.endIndex === safeEnd
                ) {
                    return current;
                }
                return { startIndex: safeStart, endIndex: safeEnd };
            });
        },
        [pfPeriod, pfPortfolioAllRows]
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
    const handleStartAnalysis = useCallback(() => {
        setSidebarMobileOpen(false);
        if (!isAuthenticated) {
            openAuthDialog("signup");
            return;
        }
        if (typeof window !== "undefined") {
            window.scrollTo({ top: 0, behavior: "smooth" });
        }
    }, [isAuthenticated, openAuthDialog]);
    const navItems: NavItem[] = [
        {
            href: view === "analysis" ? "#analysis" : "/",
            label: "Analiza techniczna",
            key: "analysis",
            icon: IconSparkline,
        },
        {
            href: view === "score" ? "#score" : "/ranking-score",
            label: "Ranking score",
            key: "score",
            icon: IconTrophy,
        },
        {
            href: view === "portfolio" ? "#portfolio" : "/symulator-portfela",
            label: "Symulacja portfela",
            key: "portfolio",
            icon: IconPie,
        },
    ];

    const comparisonErrorEntries = useMemo(
        () => Object.entries(comparisonErrors),
        [comparisonErrors]
    );
    const comparisonLimitReached = comparisonSymbols.length >= MAX_COMPARISONS;

    useEffect(() => {
        let live = true;
        if (!pfRes || !pfRes.equity.length || pfComparisonSymbols.length === 0) {
            setPfComparisonAllRows({});
            setPfComparisonErrors({});
            return () => {
                live = false;
            };
        }

        const startISO = pfRes.equity[0]?.date ?? pfStart;

        (async () => {
            const results = await Promise.allSettled(
                pfComparisonSymbols.map((sym) =>
                    fetchQuotes(sym, startISO).then((data) => ({ symbol: sym, data }))
                )
            );

            if (!live) return;

            const nextAll: Record<string, Row[]> = {};
            const nextErrors: Record<string, string> = {};

            results.forEach((result, idx) => {
                const sym = pfComparisonSymbols[idx];
                if (!sym) return;
                if (result.status === "fulfilled") {
                    nextAll[sym] = result.value.data;
                } else {
                    const reason = result.reason;
                    const message =
                        reason instanceof Error
                            ? reason.message
                            : typeof reason === "string"
                                ? reason
                                : `Nie udało się pobrać danych dla ${sym}`;
                    nextErrors[sym] = message;
                }
            });

            setPfComparisonAllRows(nextAll);
            setPfComparisonErrors(nextErrors);
        })();

        return () => {
            live = false;
        };
    }, [pfComparisonSymbols, pfRes, pfStart]);

    const pfComparisonVisibleRows = useMemo(() => {
        if (!pfVisibleRange) return {} as Record<string, Row[]>;
        const next: Record<string, Row[]> = {};
        for (const sym of pfComparisonSymbols) {
            const series = pfComparisonAllRows[sym];
            if (!series?.length) continue;
            next[sym] = series.filter(
                (row) => row.date >= pfVisibleRange.start && row.date <= pfVisibleRange.end
            );
        }
        return next;
    }, [pfComparisonAllRows, pfComparisonSymbols, pfVisibleRange]);

    const pfComparisonSeriesForChart = useMemo<ComparisonSeries[]>(() => {
        const series: ComparisonSeries[] = [];
        if (pfBenchmarkSeries) {
            series.push(pfBenchmarkSeries);
        }
        const offset = pfBenchmarkSeries ? 1 : 0;
        pfComparisonSymbols.forEach((sym, idx) => {
            series.push({
                symbol: sym,
                label: sym,
                color: COMPARISON_COLORS[(idx + offset) % COMPARISON_COLORS.length],
                rows: pfComparisonVisibleRows[sym] ?? [],
            });
        });
        return series;
    }, [pfBenchmarkSeries, pfComparisonSymbols, pfComparisonVisibleRows]);

    const pfComparisonColorMap = useMemo(() => {
        const map: Record<string, string> = {};
        for (const series of pfComparisonSeriesForChart) {
            map[series.symbol] = series.color;
        }
        return map;
    }, [pfComparisonSeriesForChart]);

    const pfComparisonLimitReached = pfComparisonSymbols.length >= MAX_COMPARISONS;

    const pfComparisonErrorEntries = useMemo(
        () => Object.entries(pfComparisonErrors),
        [pfComparisonErrors]
    );

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
        setScoreUniverse("");
        setScoreAsOf(new Date().toISOString().slice(0, 10));
        setScoreMinMcap("");
        setScoreMinTurnover("");
        setScoreResults(null);
        setScoreError("");
        setEditingTemplateId(null);
        setScoreTemplateFeedback(null);
    };

    const handleSaveScoreTemplate = (
        mode: "update" | "new" = editingTemplateId ? "update" : "new"
    ) => {
        setScoreTemplateFeedback(null);
        const sanitizedRules = scoreRules
            .map(toTemplateRule)
            .filter((rule) => typeof rule.metric === "string" && rule.metric.trim());
        if (!sanitizedRules.length) {
            setScoreTemplateFeedback({
                type: "error",
                message: "Dodaj co najmniej jedną metrykę, aby zapisać szablon.",
            });
            return;
        }

        const templateTitle = scoreNameInput.trim()
            ? scoreNameInput.trim()
            : `Szablon ${scoreTemplates.length + 1}`;
        const limitValue =
            typeof scoreLimit === "number" && Number.isFinite(scoreLimit) && scoreLimit > 0
                ? Math.floor(scoreLimit)
                : 10;

        const baseTemplate = {
            title: templateTitle,
            name: scoreNameInput.trim() || undefined,
            description: scoreDescription.trim() || undefined,
            rules: sanitizedRules,
            limit: limitValue,
            sort: scoreSort === "asc" ? "asc" : "desc",
            universe: scoreUniverse,
            minMcap: scoreMinMcap,
            minTurnover: scoreMinTurnover,
        } satisfies Omit<ScoreTemplate, "id" | "createdAt">;

        if (mode === "update" && editingTemplateId) {
            const existing = scoreTemplates.find((tpl) => tpl.id === editingTemplateId);
            if (existing) {
                setScoreTemplates((prev) =>
                    prev.map((tpl) =>
                        tpl.id === editingTemplateId ? { ...existing, ...baseTemplate } : tpl
                    )
                );
                setScoreTemplateFeedback({
                    type: "success",
                    message: `Zaktualizowano szablon „${templateTitle}”.`,
                });
                return;
            }
        }

        const newTemplate: ScoreTemplate = {
            ...baseTemplate,
            id: createTemplateId(),
            createdAt: new Date().toISOString(),
        };

        setScoreTemplates((prev) => [...prev, newTemplate]);
        setEditingTemplateId(newTemplate.id);
        setScoreTemplateFeedback({
            type: "success",
            message: `Zapisano szablon „${templateTitle}”.`,
        });
    };

    const handleApplyScoreTemplate = (template: ScoreTemplate) => {
        setScoreRules(fromTemplateRules(template.rules));
        setScoreNameInput(template.name ?? template.title);
        setScoreDescription(template.description ?? "");
        setScoreLimit(template.limit);
        setScoreSort(template.sort);
        setScoreUniverse(template.universe ?? "");
        setScoreMinMcap(template.minMcap ?? "");
        setScoreMinTurnover(template.minTurnover ?? "");
        setScoreResults(null);
        setScoreError("");
        setEditingTemplateId(template.id);
        setScoreTemplateFeedback({
            type: "success",
            message: `Załadowano szablon „${template.title}”.`,
        });
    };

    const handleDeleteScoreTemplate = (id: string) => {
        const template = scoreTemplates.find((tpl) => tpl.id === id);
        setScoreTemplates((prev) => prev.filter((tpl) => tpl.id !== id));
        setScoreTemplateFeedback({
            type: "success",
            message: template
                ? `Usunięto szablon „${template.title}”.`
                : "Usunięto szablon.",
        });
    };

    const handleScorePreview = async () => {
        try {
            setScoreError("");
            setScoreTemplateFeedback(null);
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

            const parsedUniverse = parseUniverseValue(scoreUniverse);
            const resolvedUniverse = resolveUniverseWithFallback(
                parsedUniverse,
                scoreUniverseFallback
            );

            const payload: ScorePreviewRequest = {
                name: scoreNameInput.trim() || undefined,
                description: scoreDescription.trim() || undefined,
                rules: rulePayload,
                limit: limitValue,
                universe: resolvedUniverse,
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

                const selectedTemplate = pfSelectedTemplateId
                    ? scoreTemplates.find((tpl) => tpl.id === pfSelectedTemplateId)
                    : null;
                const componentsForScore = selectedTemplate
                    ? buildScoreComponents(fromTemplateRules(selectedTemplate.rules))
                    : scoreComponents;
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
                    componentsForScore,
                    scoreUniverseFallback
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
        const normalized = sym.trim().toUpperCase();
        if (!normalized) return;
        setWatch((prev) => {
            if (!prev.includes(normalized)) {
                return prev;
            }
            const next = prev.filter((item) => item !== normalized);
            if (symbol === normalized) {
                setSymbol(next.length ? next[0] : null);
            }
            return next;
        });
    };

    const authDialogSectionLabel = authDialogMode === "login" ? "Logowanie" : "Rejestracja";
    const authDialogHeading =
        authDialogMode === "login"
            ? "Wróć do zapisanych ustawień"
            : "Załóż konto i synchronizuj konfiguracje";
    const authDialogCtaLabel = authDialogMode === "login" ? "Zaloguj się" : "Załóż konto";
    return (
        <div className="flex min-h-screen bg-page text-neutral">
            <Script
                src="https://accounts.google.com/gsi/client"
                strategy="afterInteractive"
                async
                defer
                onLoad={() => setGoogleLoaded(true)}
                onError={() => setAuthError("Nie udało się wczytać logowania Google.")}
            />
            {sidebarMobileOpen && (
                <div
                    className="fixed inset-0 z-40 bg-black/70 backdrop-blur-sm lg:hidden"
                    onClick={() => setSidebarMobileOpen(false)}
                />
            )}
            <div
                className={`fixed inset-y-0 left-0 z-50 w-80 transform border-r border-white/10 bg-[#0f1014] text-white shadow-[0_20px_50px_rgba(0,0,0,0.45)] transition-transform duration-300 ease-in-out lg:hidden ${
                    sidebarMobileOpen ? "translate-x-0" : "-translate-x-full"
                }`}
                role="dialog"
                aria-modal="true"
            >
                <div className="relative flex h-full flex-col">
                    <SidebarContent
                        collapsed={false}
                        navItems={navItems}
                        activeKey={view}
                        onStartAnalysis={handleStartAnalysis}
                        isAuthenticated={isAuthenticated}
                        authUser={authUser}
                        profileLoading={profileLoading}
                        authLoading={authLoading}
                        handleLogout={handleLogout}
                        openAuthDialog={openAuthDialog}
                        authError={authError}
                        profileError={profileError}
                        googleClientId={GOOGLE_CLIENT_ID}
                        onNavigate={() => setSidebarMobileOpen(false)}
                    />
                    <button
                        type="button"
                        onClick={() => setSidebarMobileOpen(false)}
                        className="absolute right-4 top-4 inline-flex h-9 w-9 items-center justify-center rounded-full border border-white/20 text-lg text-white/70 transition hover:border-white/40 hover:text-white"
                        aria-label="Zamknij menu"
                    >
                        ×
                    </button>
                </div>
            </div>
            <aside
                className={`hidden lg:flex ${sidebarCollapsed ? "lg:w-20" : "lg:w-[280px]"} flex-col border-r border-white/10 bg-[#0f1014] text-white lg:sticky lg:top-0 lg:h-screen lg:flex-shrink-0`}
            >
                <SidebarContent
                    collapsed={sidebarCollapsed}
                    navItems={navItems}
                    activeKey={view}
                    onStartAnalysis={handleStartAnalysis}
                    isAuthenticated={isAuthenticated}
                    authUser={authUser}
                    profileLoading={profileLoading}
                    authLoading={authLoading}
                    handleLogout={handleLogout}
                    openAuthDialog={openAuthDialog}
                    authError={authError}
                    profileError={profileError}
                    googleClientId={GOOGLE_CLIENT_ID}
                    onToggleCollapse={() => setSidebarCollapsed((prev) => !prev)}
                />
            </aside>
            <div className="flex min-h-screen flex-1 flex-col">
                <header className="text-white lg:hidden">
                    <div className="mx-auto w-full max-w-6xl px-4 py-4 md:px-8 md:py-6">
                        <div className="mb-4 flex items-center justify-between lg:mb-0">
                            <div className="flex items-center gap-3">
                                <button
                                    type="button"
                                    onClick={() => setSidebarMobileOpen(true)}
                                    className="inline-flex h-10 w-10 items-center justify-center rounded-full border border-white/20 text-white transition hover:border-white/40 hover:text-white lg:hidden"
                                    aria-label="Otwórz menu"
                                    aria-expanded={sidebarMobileOpen}
                                >
                                    <svg
                                        viewBox="0 0 24 24"
                                        fill="none"
                                        xmlns="http://www.w3.org/2000/svg"
                                        className="h-5 w-5"
                                    >
                                        <path
                                            d="M4 6H20M4 12H20M4 18H20"
                                            stroke="currentColor"
                                            strokeWidth="2"
                                            strokeLinecap="round"
                                            strokeLinejoin="round"
                                        />
                                    </svg>
                                </button>
                                <span className="text-sm font-semibold text-white lg:hidden">GPW Analytics</span>
                            </div>
                        </div>
                        <div className="mt-6 space-y-4">
                            {isAuthenticated ? (
                                <div className="flex items-center gap-3 rounded-2xl bg-white/10 p-4">
                                    {authUser?.picture ? (
                                        <Image
                                            src={authUser.picture}
                                            alt="Avatar"
                                            width={40}
                                            height={40}
                                            className="h-10 w-10 rounded-full border border-white/30 object-cover"
                                            unoptimized
                                        />
                                    ) : (
                                        <div className="flex h-10 w-10 items-center justify-center rounded-full border border-white/20 bg-white/10 text-sm font-semibold">
                                            {(authUser?.name ?? authUser?.email ?? "U").charAt(0).toUpperCase()}
                                        </div>
                                    )}
                                    <div className="flex-1 text-sm">
                                        <p className="font-semibold text-white">
                                            {authUser?.name ?? authUser?.email ?? "Użytkownik Google"}
                                        </p>
                                        {authUser?.email ? (
                                            <p className="text-xs text-white/60">{authUser.email}</p>
                                        ) : null}
                                        <p className="text-[11px] uppercase tracking-wider text-white/40">
                                            {profileLoading ? "Zapisywanie ustawień..." : "Konto Google"}
                                        </p>
                                    </div>
                                    <button
                                        className="rounded-lg border border-white/20 px-3 py-2 text-xs font-semibold text-white transition hover:bg-white/10"
                                        onClick={handleLogout}
                                        disabled={authLoading}
                                    >
                                        Wyloguj
                                    </button>
                                </div>
                            ) : (
                                <div className="space-y-3">
                                    <div className="flex flex-col gap-2 sm:flex-row">
                                        <button
                                            className="flex-1 rounded-lg border border-white/20 px-4 py-2 text-sm font-semibold text-white/80 transition hover:bg-white/10 hover:text-white disabled:cursor-not-allowed disabled:opacity-60"
                                            onClick={() => openAuthDialog("login")}
                                            disabled={authLoading}
                                        >
                                            Zaloguj się
                                        </button>
                                        <button
                                            className="flex-1 rounded-lg bg-white px-4 py-2 text-sm font-semibold text-primary shadow-lg shadow-black/10 transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60"
                                            onClick={() => openAuthDialog("signup")}
                                            disabled={authLoading}
                                        >
                                            Załóż konto
                                        </button>
                                    </div>
                                    <p className="text-xs text-white/70">
                                        Historia ustawień jest zapisywana w Twoim koncie Google.
                                    </p>
                                    {!GOOGLE_CLIENT_ID && (
                                        <p className="text-[11px] text-amber-200">
                                            Ustaw zmienną NEXT_PUBLIC_GOOGLE_CLIENT_ID, aby włączyć logowanie.
                                        </p>
                                    )}
                                </div>
                            )}
                            {(authError || profileError) && (
                                <p className="text-xs text-rose-200">
                                    {authError ?? profileError}
                                </p>
                            )}
                        </div>
                        <div className="mt-6 lg:hidden">
                            <SectionNav items={navItems} />
                        </div>
                    </div>
                </header>

                {!isAuthenticated && authDialogOpen && (
                <div
                    className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/70 px-4 py-6"
                    role="dialog"
                    aria-modal="true"
                    aria-labelledby="auth-dialog-title"
                    onClick={closeAuthDialog}
                >
                    <div
                        className="w-full max-w-lg rounded-3xl bg-white text-slate-900 shadow-2xl"
                        onClick={(event) => event.stopPropagation()}
                    >
                        <div className="flex items-start justify-between border-b border-slate-100 px-6 py-5">
                            <div className="space-y-1">
                                <p className="text-xs font-semibold uppercase tracking-[0.35em] text-slate-400">
                                    {authDialogSectionLabel}
                                </p>
                                <h2 id="auth-dialog-title" className="text-xl font-semibold text-slate-900">
                                    {authDialogHeading}
                                </h2>
                            </div>
                            <button
                                type="button"
                                onClick={closeAuthDialog}
                                className="flex h-9 w-9 items-center justify-center rounded-full border border-slate-200 text-lg text-slate-400 transition hover:border-slate-300 hover:text-slate-600"
                                aria-label="Zamknij okno logowania"
                            >
                                ×
                            </button>
                        </div>
                        <div className="space-y-6 px-6 py-6">
                            <div className="space-y-3">
                                <button
                                    type="button"
                                    disabled
                                    className="flex w-full items-center justify-center gap-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-semibold text-slate-400"
                                >
                                    Kontynuuj przez Facebook
                                </button>
                                <button
                                    type="button"
                                    disabled
                                    className="flex w-full items-center justify-center gap-3 rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-semibold text-slate-400"
                                >
                                    Kontynuuj przez Apple
                                </button>
                                <button
                                    type="button"
                                    onClick={triggerGoogleAuth}
                                    className="flex w-full items-center justify-center gap-3 rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm font-semibold text-primary shadow hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
                                    disabled={authLoading || !GOOGLE_CLIENT_ID}
                                >
                                    {authLoading ? "Ładowanie logowania..." : "Kontynuuj przez konto Google"}
                                </button>
                            </div>
                            <div className="flex items-center gap-3 text-slate-400">
                                <span className="h-px flex-1 bg-slate-200" />
                                <span className="text-xs uppercase tracking-[0.3em]">lub</span>
                                <span className="h-px flex-1 bg-slate-200" />
                            </div>
                            <div className="flex items-center gap-2 rounded-full bg-slate-100 p-1 text-sm font-semibold text-slate-500">
                                <button
                                    type="button"
                                    onClick={() => setAuthDialogMode("login")}
                                    className={`flex-1 rounded-full px-4 py-2 transition ${
                                        authDialogMode === "login"
                                            ? "bg-white text-slate-900 shadow"
                                            : "hover:text-slate-700"
                                    }`}
                                >
                                    Zaloguj się
                                </button>
                                <button
                                    type="button"
                                    onClick={() => setAuthDialogMode("signup")}
                                    className={`flex-1 rounded-full px-4 py-2 transition ${
                                        authDialogMode === "signup"
                                            ? "bg-white text-slate-900 shadow"
                                            : "hover:text-slate-700"
                                    }`}
                                >
                                    Załóż konto
                                </button>
                            </div>
                            <div className="space-y-4">
                                <label className="flex flex-col gap-2 text-sm font-medium text-slate-700">
                                    <span>E-mail</span>
                                    <input
                                        type="email"
                                        placeholder="adres@email.com"
                                        className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-slate-500 shadow-inner"
                                        disabled
                                    />
                                </label>
                                <label className="flex flex-col gap-2 text-sm font-medium text-slate-700">
                                    <span>Hasło</span>
                                    <input
                                        type="password"
                                        placeholder="••••••••"
                                        className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-slate-500 shadow-inner"
                                        disabled
                                    />
                                </label>
                                <button
                                    type="button"
                                    className="w-full rounded-xl bg-primary px-4 py-3 text-sm font-semibold text-white opacity-50"
                                    disabled
                                >
                                    {authDialogCtaLabel}
                                </button>
                                <p className="text-xs text-slate-500">
                                    Obsługujemy obecnie logowanie przez Google. Formularz e-mailowy będzie dostępny wkrótce.
                                </p>
                                {(authError || profileError) && (
                                    <p className="text-sm text-red-500">{authError ?? profileError}</p>
                                )}
                                {!GOOGLE_CLIENT_ID && (
                                    <p className="text-xs text-amber-500">
                                        Dodaj identyfikator klienta Google, aby aktywować przycisk logowania.
                                    </p>
                                )}
                            </div>
                        </div>
                    </div>
                </div>
            )}

            <main className="flex-1 overflow-y-auto">
                <div className="mx-auto w-full max-w-6xl px-4 py-8 md:px-8 md:py-12 space-y-16">
                    {view === "analysis" && (
                        <Section
                            id="analysis"
                        title="Analiza techniczna i kontekst"
                        description="Dodawaj tickery z GPW do listy obserwacyjnej i analizuj wykres wraz z kluczowymi statystykami, wskaźnikami momentum oraz podglądem fundamentów."
                        actions={
                            <TickerAutosuggest
                                onPick={(sym) => {
                                    const normalized = sym.trim().toUpperCase();
                                    if (!normalized) return;
                                    setWatch((w) => (w.includes(normalized) ? w : [normalized, ...w]));
                                    setSymbol(normalized);
                                }}
                            />
                        }
                    >
                        <div className="space-y-10">
                            <Card>
                                <div className="space-y-4">
                                    <h3 className="text-lg font-semibold text-primary">
                                        Monitoruj swoje spółki
                                    </h3>
                                    <p className="text-sm text-muted">
                                        Kliknij na ticker, aby przełączyć moduły analizy poniżej. Usuń zbędne pozycje przyciskiem ×.
                                    </p>
                                    <Watchlist
                                        items={watch}
                                        current={symbol}
                                        onPick={(sym) => setSymbol(sym)}
                                        onRemove={removeFromWatch}
                                    />
                                </div>
                            </Card>

                            <div className="grid md:grid-cols-3 gap-6">
                                <div className="md:col-span-2 space-y-6">
                                    <Card
                                        title={symbol ? `${symbol} – wykres cenowy` : "Wykres cenowy"}
                                        right={
                                            <>
                                                {PERIOD_OPTIONS.map(({ label, value }) => (
                                                    <Chip
                                                        key={value}
                                                        active={period === value}
                                                        onClick={() => setPeriod(value)}
                                                    >
                                                        {label}
                                                    </Chip>
                                                ))}
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
                                                {symbol && (
                                                    <div className="rounded-lg border border-dashed border-soft/70 bg-white/60 p-3">
                                                        <div className="flex flex-wrap items-center gap-3">
                                                            <div className="text-xs font-semibold uppercase tracking-wide text-muted">
                                                                Porównania
                                                            </div>
                                                            <TickerAutosuggest
                                                                onPick={handleAddComparison}
                                                                placeholder={
                                                                    comparisonLimitReached
                                                                        ? "Osiągnięto limit porównań"
                                                                        : "Dodaj spółkę do porównania"
                                                                }
                                                                inputClassName="w-56"
                                                                disabled={comparisonLimitReached}
                                                            />
                                                            {comparisonLimitReached && (
                                                                <span className="text-[11px] text-subtle">
                                                                    Maksymalnie {MAX_COMPARISONS} spółek.
                                                                </span>
                                                            )}
                                                        </div>
                                                        <div className="mt-3 space-y-2">
                                                            {comparisonSymbols.length ? (
                                                                <div className="flex flex-wrap gap-2">
                                                                    {comparisonSymbols.map((sym) => {
                                                                        const color = comparisonColorMap[sym] ?? "#475569";
                                                                        return (
                                                                            <span
                                                                                key={sym}
                                                                                className="inline-flex items-center gap-2 rounded-full border border-soft bg-white/80 px-3 py-1 text-xs font-medium text-neutral shadow-sm"
                                                                            >
                                                                                <span
                                                                                    className="h-2.5 w-2.5 rounded-full"
                                                                                    style={{ backgroundColor: color }}
                                                                                />
                                                                                {sym}
                                                                                <button
                                                                                    type="button"
                                                                                    onClick={() => handleRemoveComparison(sym)}
                                                                                    className="text-subtle transition hover:text-negative focus-visible:text-negative"
                                                                                    aria-label={`Usuń ${sym} z porównań`}
                                                                                >
                                                                                    ×
                                                                                </button>
                                                                            </span>
                                                                        );
                                                                    })}
                                                                </div>
                                                            ) : (
                                                                <p className="text-xs text-subtle">
                                                                    Dodaj spółkę, aby porównać zachowanie kursu z innymi instrumentami.
                                                                </p>
                                                            )}
                                                            {comparisonErrorEntries.map(([sym, message]) => (
                                                                <div key={sym} className="text-xs text-negative">
                                                                    {sym}: {message}
                                                                </div>
                                                            ))}
                                                        </div>
                                                    </div>
                                                )}
                                                <PriceChart
                                                    rows={withSma}
                                                    showArea={area}
                                                    showSMA={smaOn}
                                                    brushDataRows={period === "max" ? brushRows : undefined}
                                                    brushRange={period === "max" ? brushRange : null}
                                                    onBrushChange={
                                                        period === "max" ? handleBrushSelectionChange : undefined
                                                    }
                                                    primarySymbol={symbol}
                                                    comparisonSeries={comparisonSeriesForChart}
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
                        </div>
                        </Section>
                    )}

                    {view === "score" && (
                        <Section
                            id="score"
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
                                    <span className="text-xs text-subtle">
                                        Pozostaw puste, aby automatycznie użyć listy
                                        {" "}
                                        {scoreUniverseFallback.length}
                                        {" "}
                                        najpłynniejszych spółek GPW z backendu.
                                    </span>
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
                                    onClick={() => handleSaveScoreTemplate()}
                                    className="px-4 py-2 rounded-xl bg-primary text-white transition hover:opacity-90"
                                >
                                    {editingTemplate
                                        ? "Zapisz zmiany w szablonie"
                                        : "Zapisz szablon"}
                                </button>
                                {editingTemplate && (
                                    <button
                                        type="button"
                                        onClick={() => handleSaveScoreTemplate("new")}
                                        className="px-4 py-2 rounded-xl border border-soft text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                    >
                                        Zapisz jako nowy
                                    </button>
                                )}
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
                            {editingTemplate && (
                                <div className="text-xs text-subtle">
                                    Edytujesz szablon „{editingTemplate.title}”.
                                </div>
                            )}
                            {scoreTemplateFeedback && (
                                <div
                                    className={`text-xs ${
                                        scoreTemplateFeedback.type === "error"
                                            ? "text-negative"
                                            : "text-primary"
                                    }`}
                                >
                                    {scoreTemplateFeedback.message}
                                </div>
                            )}
                            {scoreTemplates.length > 0 && (
                                <div className="space-y-2">
                                    <div className="text-xs uppercase tracking-wide text-muted">
                                        Zapisane szablony
                                    </div>
                                    <div className="flex flex-col gap-2">
                                        {scoreTemplates.map((template) => {
                                            const isActive = template.id === editingTemplateId;
                                            const rulesCount = template.rules.length;
                                            const ruleLabel =
                                                rulesCount === 1
                                                    ? "1 reguła"
                                                    : rulesCount >= 2 && rulesCount <= 4
                                                    ? `${rulesCount} reguły`
                                                    : `${rulesCount} reguł`;
                                            const universeLabel = template.universe.trim()
                                                ? template.universe.trim()
                                                : "Auto (fallback)";
                                            return (
                                                <div
                                                    key={template.id}
                                                    className={`flex flex-wrap items-center justify-between gap-2 rounded-xl border px-3 py-2 bg-surface ${
                                                        isActive ? "border-primary" : "border-soft"
                                                    }`}
                                                >
                                                    <div>
                                                        <div className="font-medium text-primary">
                                                            {template.title}
                                                        </div>
                                                        <div className="text-xs text-subtle">
                                                            {ruleLabel} • Limit {template.limit} • Sortowanie: {" "}
                                                            {template.sort === "asc" ? "rosnąco" : "malejąco"} • Universe: {" "}
                                                            {universeLabel}
                                                        </div>
                                                    </div>
                                                    <div className="flex flex-wrap gap-2">
                                                        <button
                                                            type="button"
                                                            onClick={() => handleApplyScoreTemplate(template)}
                                                            className="px-3 py-1.5 rounded-lg bg-accent text-primary text-xs font-medium transition hover:bg-[#27AE60]"
                                                        >
                                                            Załaduj
                                                        </button>
                                                        <button
                                                            type="button"
                                                            onClick={() => handleDeleteScoreTemplate(template.id)}
                                                            className="px-3 py-1.5 rounded-lg border border-soft text-xs text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                                        >
                                                            Usuń
                                                        </button>
                                                    </div>
                                                </div>
                                            );
                                        })}
                                    </div>
                                </div>
                            )}
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
                )}

                    {view === "portfolio" && (
                    <Section
                        id="portfolio"
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
                                    powyżej, zapisany szablon lub dowolną istniejącą nazwę rankingu z backendu.
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
                                            <div className="space-y-2">
                                                <label className="flex flex-col gap-2">
                                                    <span className="text-xs uppercase tracking-wide text-muted">
                                                        Szablon z konfiguratora
                                                    </span>
                                                    <div className="flex flex-wrap gap-2">
                                                        <select
                                                            value={pfSelectedTemplateId ?? ""}
                                                            onChange={(e) =>
                                                                setPfSelectedTemplateId(
                                                                    e.target.value ? e.target.value : null
                                                                )
                                                            }
                                                            className={`${inputBaseClasses} min-w-[12rem]`}
                                                            disabled={scoreTemplates.length === 0}
                                                        >
                                                            <option value="">
                                                                Brak – użyj bieżącej konfiguracji
                                                            </option>
                                                            {scoreTemplates.map((template) => (
                                                                <option key={template.id} value={template.id}>
                                                                    {template.title}
                                                                </option>
                                                            ))}
                                                        </select>
                                                        {pfSelectedTemplateId && (
                                                            <button
                                                                type="button"
                                                                onClick={() => setPfSelectedTemplateId(null)}
                                                                className="px-3 py-1.5 rounded-lg border border-soft text-xs text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                                            >
                                                                Wyczyść
                                                            </button>
                                                        )}
                                                        {pfSelectedTemplateId && (
                                                            <button
                                                                type="button"
                                                                onClick={() => {
                                                                    const template = scoreTemplates.find(
                                                                        (item) => item.id === pfSelectedTemplateId
                                                                    );
                                                                    if (template) {
                                                                        handleApplyScoreTemplate(template);
                                                                    }
                                                                }}
                                                                className="px-3 py-1.5 rounded-lg bg-accent text-primary text-xs font-medium transition hover:bg-[#27AE60]"
                                                            >
                                                                Otwórz w konfiguratorze
                                                            </button>
                                                        )}
                                                    </div>
                                                </label>
                                                <div className="text-xs text-subtle">
                                                    {scoreTemplates.length
                                                        ? "Wybierz zapisany zestaw reguł, aby szybko zasymulować portfel."
                                                        : "Zapisz konfigurację score powyżej, by móc użyć jej tutaj."}
                                                </div>
                                            </div>
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
                                        {pfPortfolioVisibleRows.length > 0 && (
                                            <div className="mt-6 space-y-4">
                                                <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                                                    <div className="flex-1">
                                                        <Stats data={pfPortfolioVisibleRows} />
                                                    </div>
                                                    <div className="flex flex-wrap items-center gap-2 md:justify-end">
                                                        {PERIOD_OPTIONS.map(({ label, value }) => (
                                                            <Chip
                                                                key={`pf-${value}`}
                                                                active={pfPeriod === value}
                                                                onClick={() => setPfPeriod(value)}
                                                            >
                                                                {label}
                                                            </Chip>
                                                        ))}
                                                    </div>
                                                </div>
                                                <div className="rounded-lg border border-dashed border-soft/70 bg-white/60 p-3">
                                                    <div className="flex flex-wrap items-center gap-3">
                                                        <div className="text-xs font-semibold uppercase tracking-wide text-muted">
                                                            Porównania benchmarków
                                                        </div>
                                                        <TickerAutosuggest
                                                            onPick={handleAddPfComparison}
                                                            placeholder={
                                                                pfComparisonLimitReached
                                                                    ? "Osiągnięto limit porównań"
                                                                    : "Dodaj benchmark (np. WIG20.WA)"
                                                            }
                                                            inputClassName="w-60"
                                                            disabled={pfComparisonLimitReached}
                                                        />
                                                        {pfComparisonLimitReached && (
                                                            <span className="text-[11px] text-subtle">
                                                                Maksymalnie {MAX_COMPARISONS} serii.
                                                            </span>
                                                        )}
                                                    </div>
                                                    <div className="mt-3 space-y-2">
                                                        {pfBenchmarkSeries && (
                                                            <div className="inline-flex items-center gap-2 rounded-full border border-soft bg-white/80 px-3 py-1 text-xs font-medium text-neutral shadow-sm">
                                                                <span
                                                                    className="h-2.5 w-2.5 rounded-full"
                                                                    style={{ backgroundColor: pfBenchmarkSeries.color }}
                                                                />
                                                                {pfBenchmarkSeries.label}
                                                                <span className="text-[11px] text-subtle">(z symulacji)</span>
                                                            </div>
                                                        )}
                                                        {pfComparisonSymbols.length ? (
                                                            <div className="flex flex-wrap gap-2">
                                                                {pfComparisonSymbols.map((sym) => {
                                                                    const color = pfComparisonColorMap[sym] ?? "#475569";
                                                                    return (
                                                                        <span
                                                                            key={sym}
                                                                            className="inline-flex items-center gap-2 rounded-full border border-soft bg-white/80 px-3 py-1 text-xs font-medium text-neutral shadow-sm"
                                                                        >
                                                                            <span
                                                                                className="h-2.5 w-2.5 rounded-full"
                                                                                style={{ backgroundColor: color }}
                                                                            />
                                                                            {sym}
                                                                            <button
                                                                                type="button"
                                                                                onClick={() => handleRemovePfComparison(sym)}
                                                                                className="text-subtle transition hover:text-negative focus-visible:text-negative"
                                                                                aria-label={`Usuń ${sym} z porównań`}
                                                                            >
                                                                                ×
                                                                            </button>
                                                                        </span>
                                                                    );
                                                                })}
                                                            </div>
                                                        ) : !pfBenchmarkSeries ? (
                                                            <p className="text-xs text-subtle">
                                                                Dodaj indeks lub ETF, aby zestawić portfel z rynkowymi benchmarkami.
                                                            </p>
                                                        ) : null}
                                                        {pfComparisonErrorEntries.map(([sym, message]) => (
                                                            <div key={sym} className="text-xs text-negative">
                                                                {sym}: {message}
                                                            </div>
                                                        ))}
                                                    </div>
                                                </div>
                                                <PriceChart
                                                    rows={pfPortfolioRowsWithSma}
                                                    showArea
                                                    showSMA={false}
                                                    brushDataRows={
                                                        pfPeriod === "max" ? pfBrushRows : undefined
                                                    }
                                                    brushRange={
                                                        pfPeriod === "max" ? pfBrushRange : null
                                                    }
                                                    onBrushChange={
                                                        pfPeriod === "max"
                                                            ? handlePfBrushSelectionChange
                                                            : undefined
                                                    }
                                                    primarySymbol="Portfel"
                                                    comparisonSeries={pfComparisonSeriesForChart}
                                                />
                                            </div>
                                        )}
                                        {pfRes.rebalances && pfRes.rebalances.length > 0 && (
                                            <div className="mt-6 space-y-4">
                                                <button
                                                    type="button"
                                                    onClick={() =>
                                                        setPfTimelineOpen((prev) => !prev)
                                                    }
                                                    className="inline-flex items-center gap-2 rounded-full border border-soft bg-white px-4 py-2 text-sm font-semibold text-primary transition hover:border-[var(--color-primary)] hover:text-[var(--color-primary)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-primary)]/40"
                                                    aria-expanded={pfTimelineOpen}
                                                >
                                                    {pfTimelineOpen
                                                        ? "Ukryj historię transakcji"
                                                        : "Historia transakcji"}
                                                </button>
                                                {pfTimelineOpen && (
                                                    <div className="space-y-6">
                                                        <RebalanceTimeline
                                                            events={pfRes.rebalances}
                                                            equity={pfRes.equity}
                                                        />
                                                    </div>
                                                )}
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
                                                {pfBenchmarkSeries
                                                    ? `Benchmark: ${pfBenchmarkSeries.label}.`
                                                    : "Bez benchmarku."}
                                                {pfComparisonSymbols.length > 0
                                                    ? ` Dodatkowe porównania: ${pfComparisonSymbols.join(", ")}.`
                                                    : ""}
                                            </div>
                                        </div>
                                    </>
                                )}
                            </div>
                        </div>
                    </Card>
                </Section>
                    )}

                <footer className="pt-6 text-center text-sm text-subtle">
                    © {new Date().getFullYear()} Analityka Rynków • MVP
                </footer>
                </div>
            </main>
        </div>
    </div>
    );
}
