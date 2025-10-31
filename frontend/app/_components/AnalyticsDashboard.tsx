"use client";

import React, { useMemo, useState, useEffect, useId, useCallback, useRef } from "react";
import type { SVGAttributes } from "react";
import { createPortal } from "react-dom";
import Image from "next/image";
import Link from "next/link";
import Script from "next/script";
import { useTheme, type ThemeMode } from "@/components/theme-provider";
import { formatPct } from "@/lib/format";
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
import type { Props as BrushProps } from "recharts/types/cartesian/Brush";

type BrushTravellerProps = {
    x: number;
    y: number;
    width: number;
    height: number;
    stroke?: SVGAttributes<SVGElement>["stroke"];
};

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
const ADMIN_API = "/api/admin";
const LOCAL_ADMIN_API = "http://localhost:8000/api/admin";
const LOCAL_CLICKHOUSE_STORAGE_KEY = "gpw-local-clickhouse-config";
const CUSTOM_INDICES_STORAGE_KEY = "gpw-custom-indices";
const MAX_UNIVERSE_FALLBACK_SYMBOLS = 500;

const NETWORK_ERROR_PATTERNS = [
    "failed to fetch",
    "fetch failed",
    "network request failed",
];

const extractErrorMessage = (error: unknown): string | null => {
    if (error instanceof Error && error.message) {
        return error.message;
    }
    if (error && typeof error === "object" && "message" in error) {
        const message = (error as { message?: unknown }).message;
        if (typeof message === "string" && message.length > 0) {
            return message;
        }
    }
    return null;
};

const isNetworkError = (error: unknown): boolean => {
    const message = extractErrorMessage(error);
    if (!message) {
        return false;
    }
    const normalized = message.toLowerCase();
    return NETWORK_ERROR_PATTERNS.some((pattern) =>
        normalized.includes(pattern)
    );
};

const resolveErrorMessage = (error: unknown, fallback: string): string => {
    if (isNetworkError(error)) {
        return fallback;
    }
    const message = extractErrorMessage(error);
    return message ?? fallback;
};

const removeUndefined = (obj: Record<string, unknown>) =>
    Object.fromEntries(
        Object.entries(obj).filter(([, value]) => value !== undefined && value !== null)
    );

const parseOptionalNumber = (value: unknown): number | undefined => {
    if (typeof value === "number") {
        return Number.isFinite(value) ? value : undefined;
    }
    if (typeof value === "string") {
        const trimmed = value.trim();
        if (!trimmed) return undefined;
        const normalized = trimmed.replace(",", ".");
        const parsed = Number(normalized);
        return Number.isFinite(parsed) ? parsed : undefined;
    }
    return undefined;
};

const stringifyOptionalNumber = (
    value: number | null | undefined
): string | undefined => {
    if (value === null || value === undefined) {
        return undefined;
    }
    return Number.isFinite(value) ? `${value}` : undefined;
};

const findScoreMetric = (value: string): ScoreMetricOption | undefined =>
    SCORE_METRIC_OPTIONS.find((option) => option.value === value);

type ScoreComponentRequest = {
    metric: ScoreMetricOption["backendMetric"];
    lookback_days: number;
    weight: number;
    direction: "asc" | "desc";
    label?: string;
    min_value?: number;
    max_value?: number;
    scoring?: { type: "linear_clamped"; worst: number; best: number };
    normalize?: "none" | "percentile";
};

const PERCENT_BASED_SCORE_METRICS = new Set<ScoreComponentRequest["metric"]>([
    "total_return",
    "volatility",
    "max_drawdown",
    "distance_from_high",
    "distance_from_low",
]);

const CHART_BRUSH_CLASS = "metric-chart-brush";
const CHART_BRUSH_STROKE = "#4663F0";
const CHART_BRUSH_BACKGROUND_FILL = "#E6EBFF";
const CHART_BRUSH_TRAVELLER_WIDTH = 18;

const ChartBrushTraveller: React.FC<BrushTravellerProps> = ({
    x,
    y,
    width,
    height,
    stroke,
}) => {
    const handleStroke = stroke ?? CHART_BRUSH_STROKE;
    const radius = 8;
    const padding = Math.min(12, Math.max(4, height * 0.2));
    const lineStart = padding;
    const lineEnd = Math.max(lineStart + 6, height - padding);
    const centerX = width / 2;

    return (
        <g className="metric-chart-brush__handle" transform={`translate(${x}, ${y})`}>
            <rect
                width={width}
                height={height}
                rx={radius}
                ry={radius}
                fill="#ffffff"
                stroke={handleStroke}
                strokeWidth={2}
            />
            <line
                x1={centerX - 3}
                y1={lineStart}
                x2={centerX - 3}
                y2={lineEnd}
                stroke={handleStroke}
                strokeWidth={1.5}
                strokeLinecap="round"
            />
            <line
                x1={centerX + 3}
                y1={lineStart}
                x2={centerX + 3}
                y2={lineEnd}
                stroke={handleStroke}
                strokeWidth={1.5}
                strokeLinecap="round"
            />
        </g>
    );
};

const CHART_BRUSH_COMMON_PROPS = {
    className: CHART_BRUSH_CLASS,
    travellerWidth: CHART_BRUSH_TRAVELLER_WIDTH,
    traveller: (props: BrushTravellerProps) => <ChartBrushTraveller {...props} />,
    stroke: CHART_BRUSH_STROKE,
    fill: CHART_BRUSH_BACKGROUND_FILL,
} satisfies Partial<BrushProps>;

type PortfolioSimulationStage = "preparing" | "ranking" | "building" | "finalizing";

type PortfolioSimulationProgress = {
    percent: number;
    stage: PortfolioSimulationStage;
    message?: string | null;
};

const PF_PROGRESS_STAGE_LABELS: Record<PortfolioSimulationStage, string> = {
    preparing: "Przygotowywanie symulacji",
    ranking: "Budowanie rankingu",
    building: "Tworzenie portfela",
    finalizing: "Finalizacja wyników",
};

type LocalClickhouseConfigState = {
    source: "env" | "override";
    mode: "url" | "manual";
    url: string | null;
    host: string | null;
    port: number | null;
    secure: boolean;
    verify: boolean | null;
    database: string | null;
    username: string | null;
    has_password: boolean;
    ca: string | null;
};

type LocalClickhousePersistedConfig = {
    mode: "url" | "manual";
    url?: string;
    host?: string;
    port?: string;
    database?: string;
    username?: string;
    password?: string;
    secure?: boolean;
    verify?: boolean;
    ca?: string;
};

type LocalClickhouseEnsureResult = { ok: true } | { ok: false; error: string };

type GpwBenchmarkConstituent = {
    symbol: string;
    symbol_base?: string | null;
    raw_symbol?: string | null;
    company_name?: string | null;
    weight?: number | null;
};

type GpwBenchmarkPortfolio = {
    index_code: string;
    index_name?: string | null;
    effective_date: string;
    constituents: GpwBenchmarkConstituent[];
};

type GpwBenchmarkPortfoliosResponse = {
    portfolios: GpwBenchmarkPortfolio[];
};

type BenchmarkUniverseConstituent = {
    symbol: string;
    baseSymbol: string;
    rawSymbol: string | null;
    companyName: string | null;
    weight?: number | null;
    weightPct?: number | null;
};

type BenchmarkUniverseOption = {
    code: string;
    name: string;
    effectiveDate: string;
    symbols: string[];
    constituents: BenchmarkUniverseConstituent[];
    isCustom?: boolean;
};

const normalizeBenchmarkConstituents = (
    entries: BenchmarkUniverseConstituent[],
): BenchmarkUniverseConstituent[] => {
    if (!entries.length) {
        return entries;
    }

    const weightValues = entries.map((entry) => {
        if (typeof entry.weight === "number" && Number.isFinite(entry.weight)) {
            if (entry.weight > 0) {
                return entry.weight;
            }
            if (entry.weight === 0) {
                return 0;
            }
        }
        return null;
    });

    const totalWeight = weightValues.reduce<number>(
        (sum, value) => sum + (value ?? 0),
        0,
    );
    if (totalWeight > 0) {
        return entries.map((entry, idx) => {
            const weightValue = weightValues[idx];
            if (weightValue == null) {
                return {
                    ...entry,
                    weight: entry.weight ?? null,
                    weightPct: null,
                };
            }
            return {
                ...entry,
                weight: weightValue,
                weightPct: (weightValue / totalWeight) * 100,
            };
        });
    }

    const pctValues = entries.map((entry) => {
        if (typeof entry.weightPct === "number" && Number.isFinite(entry.weightPct)) {
            if (entry.weightPct > 0) {
                return entry.weightPct;
            }
            if (entry.weightPct === 0) {
                return 0;
            }
        }
        return null;
    });

    const totalPct = pctValues.reduce<number>(
        (sum, value) => sum + (value ?? 0),
        0,
    );
    if (totalPct > 0) {
        return entries.map((entry, idx) => {
            const pctValue = pctValues[idx];
            if (pctValue == null) {
                return {
                    ...entry,
                    weight: entry.weight ?? null,
                    weightPct: null,
                };
            }
            return {
                ...entry,
                weight: entry.weight ?? pctValue,
                weightPct: (pctValue / totalPct) * 100,
            };
        });
    }

    return entries.map((entry) => ({
        ...entry,
        weight: entry.weight ?? null,
        weightPct: entry.weightPct ?? null,
    }));
};

type CustomIndexConstituent = {
    symbol: string;
    weightPct: number;
};

type CustomIndexDefinition = {
    id: string;
    code: string;
    name?: string | null;
    symbols: string[];
    constituents: CustomIndexConstituent[];
    startDate: string;
    baseValue: number;
    createdAt: string;
    updatedAt: string;
};

type CustomIndexDraftRow = {
    id: string;
    symbol: string;
    weightPct: number | "";
};

type CustomIndexDraft = {
    code: string;
    name: string;
    constituents: CustomIndexDraftRow[];
    startDate: string;
    baseValue: string;
};

type GpwBenchmarkHistoryPoint = {
    date: string;
    value?: number | null;
    change_pct?: number | null;
};

type GpwBenchmarkHistorySeries = {
    index_code: string;
    index_name?: string | null;
    points: GpwBenchmarkHistoryPoint[];
};

type GpwBenchmarkHistoryResponse = {
    items: GpwBenchmarkHistorySeries[];
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

        const lookbackDays = resolveLookbackDays(option, rule.lookbackDays);
        const label = computeMetricLabel(option, lookbackDays, rule.label);

        if (option.backendMetric === "price_change") {
            const worstInput = parseOptionalNumber(rule.min);
            const bestInput = parseOptionalNumber(rule.max);
            let worst = clampNumber(
                typeof worstInput === "number" && Number.isFinite(worstInput)
                    ? worstInput
                    : 0,
                -1000,
                1000
            );
            let best = clampNumber(
                typeof bestInput === "number" && Number.isFinite(bestInput)
                    ? bestInput
                    : 100,
                -1000,
                1000
            );
            if (best <= worst) {
                if (worst >= 1000) {
                    worst = 999.999;
                    best = 1000;
                } else {
                    best = Math.min(1000, worst + 0.0001);
                }
            }

            acc.push({
                metric: option.backendMetric,
                lookback_days: lookbackDays,
                weight: Number(weightNumeric),
                direction,
                label,
                scoring: { type: "linear_clamped", worst, best },
                normalize: rule.transform === "percentile" ? "percentile" : "none",
            });
            return acc;
        }

        const rawMinValue = parseOptionalNumber(rule.min);
        const rawMaxValue = parseOptionalNumber(rule.max);
        const hasScale =
            typeof rawMinValue === "number" &&
            typeof rawMaxValue === "number" &&
            Number.isFinite(rawMinValue) &&
            Number.isFinite(rawMaxValue) &&
            rawMaxValue > rawMinValue;

        let minValue = rawMinValue;
        let maxValue = rawMaxValue;

        if (
            hasScale &&
            option &&
            PERCENT_BASED_SCORE_METRICS.has(option.backendMetric)
        ) {
            minValue = rawMinValue! / 100;
            maxValue = rawMaxValue! / 100;
        }

        acc.push({
            metric: option.backendMetric,
            lookback_days: lookbackDays,
            weight: Number(weightNumeric),
            direction,
            label,
            ...(hasScale
                ? {
                      min_value: minValue,
                      max_value: maxValue,
                  }
                : {}),
            ...(rule.transform === "percentile"
                ? { normalize: "percentile" as const }
                : {}),
        });
        return acc;
    }, []);

const toScorePreviewRulePayload = (
    component: ScoreComponentRequest
): ScorePreviewRulePayload => {
    const payload: ScorePreviewRulePayload = {
        metric: `${component.metric}_${component.lookback_days}`,
        weight: component.weight,
        direction: component.direction,
        normalize: component.normalize ?? "none",
    };

    if (component.label) {
        payload.label = component.label;
    }
    if (Number.isFinite(component.lookback_days)) {
        payload.lookbackDays = component.lookback_days;
    }
    if (typeof component.min_value === "number") {
        payload.min_value = component.min_value;
    }
    if (typeof component.max_value === "number") {
        payload.max_value = component.max_value;
    }
    if (component.scoring) {
        payload.scoring = component.scoring;
    }

    return payload;
};

const extractMetricValueFromRow = (
    row: ScorePreviewRow,
    component: ScoreComponentRequest,
    rule: ScoreBuilderRule,
    option?: ScoreMetricOption
): number | undefined => {
    const metricsRecord = row.metrics;
    if (!metricsRecord) {
        return undefined;
    }

    const entries = Object.entries(metricsRecord)
        .filter(([, value]) => typeof value === "number" && Number.isFinite(value))
        .map(([key, value]) => ({
            key,
            normalized: key.replace(/[^a-z0-9]+/gi, "").toLowerCase(),
            value: value as number,
        }));

    if (!entries.length) {
        return undefined;
    }

    const baseMetricKey = `${component.metric}_${component.lookback_days}`;
    const camelMetricKey = baseMetricKey.replace(/_([a-z0-9])/gi, (_, char: string) => char.toUpperCase());
    const canonicalMetricKey =
        component.metric === "price_change" ? camelMetricKey : baseMetricKey;

    const aliasCandidates = [
        canonicalMetricKey,
        baseMetricKey,
        camelMetricKey,
        component.metric,
        component.label,
        option?.value,
        option?.label,
        option?.backendMetric,
        rule.metric,
        rule.label ?? undefined,
    ].filter((key): key is string => typeof key === "string" && key.trim().length > 0);

    const aliases = Array.from(new Set(aliasCandidates.map((key) => key.trim())));

    for (const alias of aliases) {
        const normalizedAlias = alias.replace(/[^a-z0-9]+/gi, "").toLowerCase();
        const match = entries.find((entry) => entry.normalized === normalizedAlias);
        if (match) {
            return match.value;
        }
    }

    return undefined;
};

const createRuleId = () =>
    `rule-${Math.random().toString(36).slice(2, 8)}-${Date.now().toString(36)}`;

const createTemplateId = () =>
    `tpl-${Math.random().toString(36).slice(2, 8)}-${Date.now().toString(36)}`;

type ScoreMetricLookbackConfig = {
    min: number;
    max: number;
    step?: number;
    default: number;
    presets?: { label: string; value: number }[];
    formatLabel?: (lookbackDays: number) => string;
};

type ScoreMetricOption = {
    value: string;
    label: string;
    backendMetric:
        | "total_return"
        | "volatility"
        | "max_drawdown"
        | "sharpe"
        | "price_change"
        | "rsi"
        | "distance_from_high"
        | "distance_from_low"
        | "sma"
        | "ema"
        | "macd"
        | "macd_signal"
        | "macd_histogram"
        | "stochastic"
        | "stochastic_k"
        | "stochastic_d"
        | "obv"
        | "mfi"
        | "roc"
        | "bollinger_upper"
        | "bollinger_lower"
        | "bollinger_bandwidth"
        | "bollinger_percent_b";
    lookback: number;
    defaultDirection: "asc" | "desc";
    description?: string;
    customLookback?: ScoreMetricLookbackConfig;
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
    {
        value: "distance_from_high_252",
        label: "Odległość od 52-tyg. maksimum",
        backendMetric: "distance_from_high",
        lookback: 252,
        defaultDirection: "asc",
        description:
            "Procentowe odchylenie od najwyższej ceny zamknięcia w ostatnim roku (niżej = bliżej szczytu).",
    },
    {
        value: "distance_from_low_252",
        label: "Odległość od 52-tyg. minimum",
        backendMetric: "distance_from_low",
        lookback: 252,
        defaultDirection: "desc",
        description:
            "Procentowy dystans od najniższej ceny zamknięcia w ostatnim roku (wyżej = dalej od dna).",
    },
    {
        value: "total_return_custom",
        label: "Zmiana ceny (dowolny okres)",
        backendMetric: "price_change",
        lookback: 252,
        defaultDirection: "desc",
        description: "Wybierz liczbę dni wstecz, aby policzyć zmianę ceny.",
        customLookback: {
            min: 5,
            max: 3650,
            step: 1,
            default: 252,
            presets: [
                { label: "1M", value: 21 },
                { label: "3M", value: 63 },
                { label: "6M", value: 126 },
                { label: "1R", value: 252 },
                { label: "3L", value: 756 },
                { label: "5L", value: 1260 },
            ],
            formatLabel: (lookbackDays) =>
                `Zmiana ceny (${lookbackDays} dni)`,
        },
    },
    {
        value: "rsi_custom",
        label: "RSI (dowolny okres)",
        backendMetric: "rsi",
        lookback: 14,
        defaultDirection: "asc",
        description:
            "Wskaźnik siły względnej (RSI) z możliwością wyboru horyzontu w dniach.",
        customLookback: {
            min: 5,
            max: 365,
            step: 1,
            default: 14,
            presets: [
                { label: "7 dni", value: 7 },
                { label: "14 dni", value: 14 },
                { label: "21 dni", value: 21 },
                { label: "28 dni", value: 28 },
                { label: "63 dni", value: 63 },
            ],
            formatLabel: (lookbackDays) => `RSI (${lookbackDays} dni)`,
        },
    },
    {
        value: "sma_custom",
        label: "SMA (średnia krocząca)",
        backendMetric: "sma",
        lookback: 50,
        defaultDirection: "desc",
        description:
            "Prosta średnia krocząca liczona na podstawie kursów zamknięcia.",
        customLookback: {
            min: 5,
            max: 365,
            step: 1,
            default: 50,
            presets: [
                { label: "20 dni", value: 20 },
                { label: "50 dni", value: 50 },
                { label: "100 dni", value: 100 },
                { label: "200 dni", value: 200 },
            ],
            formatLabel: (lookbackDays) => `SMA (${lookbackDays} dni)`,
        },
    },
    {
        value: "ema_custom",
        label: "EMA (średnia wykładnicza)",
        backendMetric: "ema",
        lookback: 21,
        defaultDirection: "desc",
        description:
            "Wykładnicza średnia krocząca z silniejszą wagą ostatnich notowań.",
        customLookback: {
            min: 5,
            max: 365,
            step: 1,
            default: 21,
            presets: [
                { label: "12 dni", value: 12 },
                { label: "21 dni", value: 21 },
                { label: "50 dni", value: 50 },
                { label: "100 dni", value: 100 },
            ],
            formatLabel: (lookbackDays) => `EMA (${lookbackDays} dni)`,
        },
    },
    {
        value: "macd_default",
        label: "MACD (12/26/9)",
        backendMetric: "macd",
        lookback: 26,
        defaultDirection: "desc",
        description: "Linia MACD z klasycznymi parametrami 12/26/9.",
    },
    {
        value: "macd_signal",
        label: "MACD – linia sygnału",
        backendMetric: "macd_signal",
        lookback: 26,
        defaultDirection: "desc",
        description: "9-okresowa średnia linii MACD używana do generowania sygnałów.",
    },
    {
        value: "macd_histogram",
        label: "MACD – histogram",
        backendMetric: "macd_histogram",
        lookback: 26,
        defaultDirection: "desc",
        description: "Różnica między linią MACD a linią sygnału (impet trendu).",
    },
    {
        value: "stochastic_custom",
        label: "Stochastic %K",
        backendMetric: "stochastic",
        lookback: 14,
        defaultDirection: "asc",
        description:
            "Oscylator stochastyczny (%K) wskazujący położenie ceny w zakresie z wybranego okresu.",
        customLookback: {
            min: 5,
            max: 365,
            step: 1,
            default: 14,
            presets: [
                { label: "9 dni", value: 9 },
                { label: "14 dni", value: 14 },
                { label: "21 dni", value: 21 },
                { label: "63 dni", value: 63 },
            ],
            formatLabel: (lookbackDays) => `Stochastic %K (${lookbackDays} dni)`,
        },
    },
    {
        value: "stochastic_d_custom",
        label: "Stochastic %D",
        backendMetric: "stochastic_d",
        lookback: 14,
        defaultDirection: "asc",
        description:
            "Wygładzona linia %D oscylatora stochastycznego (średnia z wartości %K).",
        customLookback: {
            min: 5,
            max: 365,
            step: 1,
            default: 14,
            presets: [
                { label: "9 dni", value: 9 },
                { label: "14 dni", value: 14 },
                { label: "21 dni", value: 21 },
                { label: "63 dni", value: 63 },
            ],
            formatLabel: (lookbackDays) => `Stochastic %D (${lookbackDays} dni)`,
        },
    },
    {
        value: "obv_default",
        label: "On-Balance Volume (OBV)",
        backendMetric: "obv",
        lookback: 63,
        defaultDirection: "desc",
        description:
            "Skumulowany wolumen ważony kierunkiem zmiany ceny (akumulacja vs dystrybucja).",
    },
    {
        value: "mfi_custom",
        label: "Money Flow Index (MFI)",
        backendMetric: "mfi",
        lookback: 14,
        defaultDirection: "asc",
        description:
            "Oscylator MFI łączący cenę i wolumen w celu identyfikacji napływów kapitału.",
        customLookback: {
            min: 5,
            max: 365,
            step: 1,
            default: 14,
            presets: [
                { label: "7 dni", value: 7 },
                { label: "14 dni", value: 14 },
                { label: "21 dni", value: 21 },
                { label: "63 dni", value: 63 },
            ],
            formatLabel: (lookbackDays) => `MFI (${lookbackDays} dni)`,
        },
    },
    {
        value: "roc_custom",
        label: "Momentum ROC",
        backendMetric: "roc",
        lookback: 63,
        defaultDirection: "desc",
        description: "Rate of Change – procentowa zmiana ceny w wybranym horyzoncie.",
        customLookback: {
            min: 5,
            max: 365,
            step: 1,
            default: 63,
            presets: [
                { label: "21 dni", value: 21 },
                { label: "63 dni", value: 63 },
                { label: "126 dni", value: 126 },
                { label: "252 dni", value: 252 },
            ],
            formatLabel: (lookbackDays) => `ROC (${lookbackDays} dni)`,
        },
    },
    {
        value: "bollinger_bandwidth_custom",
        label: "Szerokość wstęg Bollingera",
        backendMetric: "bollinger_bandwidth",
        lookback: 20,
        defaultDirection: "asc",
        description:
            "Odległość między górną a dolną wstęgą Bollingera względem średniej (niżej = węższe wstęgi).",
        customLookback: {
            min: 5,
            max: 365,
            step: 1,
            default: 20,
            presets: [
                { label: "10 dni", value: 10 },
                { label: "20 dni", value: 20 },
                { label: "50 dni", value: 50 },
            ],
            formatLabel: (lookbackDays) => `Szerokość Bollinger (${lookbackDays} dni)`,
        },
    },
    {
        value: "bollinger_percent_b_custom",
        label: "Pozycja względem wstęg Bollingera (%B)",
        backendMetric: "bollinger_percent_b",
        lookback: 20,
        defaultDirection: "desc",
        description:
            "Położenie ceny względem wstęg Bollingera (0 = dolna, 1 = górna).",
        customLookback: {
            min: 5,
            max: 365,
            step: 1,
            default: 20,
            presets: [
                { label: "10 dni", value: 10 },
                { label: "20 dni", value: 20 },
                { label: "50 dni", value: 50 },
            ],
            formatLabel: (lookbackDays) => `%B Bollinger (${lookbackDays} dni)`,
        },
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

const DEFAULT_METRIC_PREVIEW_SYMBOL = "CDR.WA";

const SCORE_TEMPLATE_STORAGE_KEY = "gpw_score_templates_v1";
const AUTH_USER_STORAGE_KEY = "gpw_auth_user_v1";
const AUTH_ADMIN_STORAGE_KEY = "gpw_auth_admin_v1";

const clampNumber = (value: number, min: number, max: number): number => {
    if (Number.isNaN(value)) return min;
    if (value < min) return min;
    if (value > max) return max;
    return value;
};

const resolveLookbackDays = (
    option: ScoreMetricOption | undefined,
    lookbackDays?: number | null
): number => {
    const custom = option?.customLookback;
    const fallback = custom?.default ?? option?.lookback ?? 252;
    const candidate = typeof lookbackDays === "number" ? lookbackDays : Number(lookbackDays);
    const numeric = Number.isFinite(candidate) ? candidate : fallback;
    if (custom) {
        const { min, max, step = 1 } = custom;
        const clamped = clampNumber(Math.round(numeric), min, max);
        const snapped = Math.round(clamped / step) * step;
        return clampNumber(snapped, min, max);
    }
    return Math.round(numeric);
};

const computeMetricLabel = (
    option: ScoreMetricOption | undefined,
    lookbackDays: number,
    fallbackLabel?: string | null
): string | undefined => {
    const trimmedFallback = fallbackLabel?.trim();
    if (trimmedFallback) {
        return trimmedFallback;
    }
    if (option?.customLookback?.formatLabel) {
        return option.customLookback.formatLabel(lookbackDays);
    }
    return option?.label;
};

const SHAREHOLDER_KEYWORDS = [
    "akcjonariat",
    "akcjonariusz",
    "akcjonariusze",
    "akcjon",
    "shareholder",
    "shareholders",
    "shareholderstructure",
    "ownership",
    "owner",
];

const SHAREHOLDER_NAME_KEYWORDS = [
    "name",
    "akcjon",
    "shareholder",
    "holder",
    "entity",
    "podmiot",
];

const SHAREHOLDER_STAKE_KEYWORDS = [
    "udz",
    "udzial",
    "udział",
    "stake",
    "share",
    "percent",
    "procent",
    "percentage",
    "pakiet",
];

const COMPANY_SIZE_KEYWORDS = [
    "wielkosc",
    "wielkość",
    "companysize",
    "size",
    "capitalisation",
    "capitalization",
    "classification",
];

const RAW_FACT_CANDIDATES: { label: string; keywords: string[] }[] = [
    { label: "Segment", keywords: ["segment"] },
    { label: "Rynek", keywords: ["market", "rynek"] },
    { label: "Free float", keywords: ["freefloat", "free float"] },
    { label: "Kapitał zakładowy", keywords: ["kapital", "capital", "sharecapital"] },
    { label: "Liczba akcji", keywords: ["liczbaakcji", "numberofshares", "sharesnumber", "sharescount"] },
];

const normalizeKey = (key: string): string =>
    key
        .normalize("NFD")
        .replace(/[^\p{L}\p{N}]+/gu, " ")
        .replace(/[\u0300-\u036f]/g, "")
        .trim()
        .toLowerCase();

const prettifyKey = (rawKey: string): string => {
    const cleaned = rawKey
        .replace(/[_\s]+/g, " ")
        .replace(/[\u0300-\u036f]/g, "")
        .trim();
    if (!cleaned) {
        return "";
    }
    return cleaned
        .split(" ")
        .filter(Boolean)
        .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
        .join(" ");
};

const deduplicateStrings = (values: string[], limit = Infinity): string[] => {
    const seen = new Set<string>();
    const output: string[] = [];
    for (const value of values) {
        const cleaned = value.replace(/\s+/g, " ").trim();
        if (!cleaned) continue;
        const normalized = cleaned.toLowerCase();
        if (seen.has(normalized)) continue;
        seen.add(normalized);
        output.push(cleaned);
        if (output.length >= limit) {
            break;
        }
    }
    return output;
};

const collectValuesByKeyKeywords = (
    value: JsonValue,
    keywords: string[],
    limit?: number
): JsonValue[] => {
    if (!value || typeof value !== "object") {
        return [];
    }

    const normalizedKeywords = keywords.map((keyword) => normalizeKey(keyword));
    const results: JsonValue[] = [];
    const stack: Array<{ key: string | null; value: JsonValue }> = [{ key: null, value }];

    while (stack.length) {
        const current = stack.pop();
        if (!current) continue;
        const currentValue = current.value;
        if (Array.isArray(currentValue)) {
            for (const item of currentValue) {
                stack.push({ key: current.key, value: item });
            }
            continue;
        }
        if (currentValue && typeof currentValue === "object") {
            for (const [childKey, childValue] of Object.entries(currentValue)) {
                const normalizedKey = normalizeKey(childKey);
                if (normalizedKeywords.some((keyword) => normalizedKey.includes(keyword))) {
                    results.push(childValue as JsonValue);
                    if (limit && results.length >= limit) {
                        return results;
                    }
                }
                stack.push({ key: childKey, value: childValue as JsonValue });
            }
        }
    }

    return results;
};

const splitShareholdingString = (value: string): string[] => {
    const withoutHtml = value
        .replace(/<br\s*\/?/gi, "\n")
        .replace(/<[^>]+>/g, " ");
    return withoutHtml
        .split(/[\n\r;•●▪·\u2022\u2023\u25CF\u25A0]+/)
        .map((part) =>
            part
                .replace(/^[\s•·\-–—\u2022\u2023\u25CF\u25A0]+/, "")
                .replace(/\s+/g, " ")
                .trim()
        )
        .filter(Boolean);
};

const flattenShareholdingValue = (value: JsonValue): string[] => {
    if (value === null || value === undefined) {
        return [];
    }
    if (typeof value === "string") {
        return splitShareholdingString(value);
    }
    if (typeof value === "number") {
        return [String(value)];
    }
    if (typeof value === "boolean") {
        return [value ? "Tak" : "Nie"];
    }
    if (Array.isArray(value)) {
        return value.flatMap((item) => flattenShareholdingValue(item));
    }

    const objectValue = value as { [key: string]: JsonValue };
    const nameParts: string[] = [];
    const stakeParts: string[] = [];
    const otherParts: string[] = [];

    for (const [rawKey, child] of Object.entries(objectValue)) {
        const key = normalizeKey(rawKey);
        const childValues = flattenShareholdingValue(child);
        if (!childValues.length) {
            continue;
        }
        if (SHAREHOLDER_NAME_KEYWORDS.some((keyword) => key.includes(keyword))) {
            nameParts.push(...childValues);
            continue;
        }
        if (SHAREHOLDER_STAKE_KEYWORDS.some((keyword) => key.includes(keyword))) {
            stakeParts.push(...childValues);
            continue;
        }
        otherParts.push(`${prettifyKey(rawKey)}: ${childValues.join(", ")}`.trim());
    }

    const combined: string[] = [];
    if (nameParts.length || stakeParts.length) {
        const main = [nameParts.join(" "), stakeParts.join(" ")]
            .map((part) => part.replace(/\s+/g, " ").trim())
            .filter(Boolean)
            .join(" – ");
        if (main) {
            combined.push(main);
        }
    }
    combined.push(...otherParts.filter(Boolean));

    if (!combined.length) {
        const fallback = Object.values(objectValue)
            .flatMap((child) => flattenShareholdingValue(child))
            .map((part) => part.replace(/\s+/g, " ").trim())
            .filter(Boolean);
        if (fallback.length) {
            combined.push(fallback.join(", "));
        }
    }

    return combined;
};

const flattenGenericValue = (value: JsonValue): string[] => {
    if (value === null || value === undefined) {
        return [];
    }
    if (typeof value === "string") {
        const cleaned = value
            .replace(/<br\s*\/?/gi, " ")
            .replace(/<[^>]+>/g, " ")
            .replace(/\s+/g, " ")
            .trim();
        return cleaned ? [cleaned] : [];
    }
    if (typeof value === "number") {
        return [String(value)];
    }
    if (typeof value === "boolean") {
        return [value ? "Tak" : "Nie"];
    }
    if (Array.isArray(value)) {
        return deduplicateStrings(value.flatMap((item) => flattenGenericValue(item)));
    }

    const objectValue = value as { [key: string]: JsonValue };
    const entries: string[] = [];
    for (const [rawKey, child] of Object.entries(objectValue)) {
        const childValues = flattenGenericValue(child);
        if (!childValues.length) {
            continue;
        }
        const label = prettifyKey(rawKey);
        if (!label) {
            entries.push(...childValues);
        } else if (childValues.length === 1) {
            entries.push(`${label}: ${childValues[0]}`);
        } else {
            entries.push(`${label}: ${childValues.join(", ")}`);
        }
    }
    return entries;
};

const extractRawInsights = (payload: JsonValue): CompanyRawInsights => {
    const shareholdingValues = collectValuesByKeyKeywords(payload, SHAREHOLDER_KEYWORDS);
    const shareholding = deduplicateStrings(
        shareholdingValues.flatMap((entry) => flattenShareholdingValue(entry)),
        20
    );

    const companySizeMatch = collectValuesByKeyKeywords(payload, COMPANY_SIZE_KEYWORDS, 1)[0];
    const companySizeCandidates = companySizeMatch ? flattenGenericValue(companySizeMatch) : [];
    const companySize = companySizeCandidates.length ? companySizeCandidates[0] : null;

    const facts: { label: string; value: string }[] = [];
    for (const candidate of RAW_FACT_CANDIDATES) {
        const matches = collectValuesByKeyKeywords(payload, candidate.keywords, 1);
        if (!matches.length) {
            continue;
        }
        const formatted = flattenGenericValue(matches[0]).find(Boolean);
        if (!formatted) {
            continue;
        }
        facts.push({ label: candidate.label, value: formatted });
    }

    const dedupedFacts: { label: string; value: string }[] = [];
    const seenFacts = new Set<string>();
    for (const fact of facts) {
        const key = `${fact.label}|${fact.value}`.toLowerCase();
        if (seenFacts.has(key)) {
            continue;
        }
        seenFacts.add(key);
        dedupedFacts.push(fact);
    }

    return {
        shareholding,
        companySize,
        facts: dedupedFacts,
    };
};

const resolveUniverseWithFallback = (
    universe: ScorePreviewRequest["universe"],
    fallback?: string[] | null,
    preferFallback = false
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
    if (preferFallback && fallback && fallback.length) {
        return [...fallback];
    }
    return undefined;
};

const buildUniverseFiltersPayload = (
    value: string | string[] | null | undefined,
    customIndexMap?: Record<string, string[]>
): { include?: string | string[]; indices?: string[] } | undefined => {
    if (value == null) {
        return undefined;
    }

    const rawEntries = Array.isArray(value) ? value : [value];
    const tokens: string[] = [];

    for (const entry of rawEntries) {
        if (typeof entry !== "string") continue;
        entry
            .split(/[\s,;]+/)
            .map((token) => token.trim())
            .filter(Boolean)
            .forEach((token) => tokens.push(token));
    }

    if (!tokens.length) {
        return undefined;
    }

    const includeSet = new Set<string>();
    const indexSet = new Set<string>();

    tokens.forEach((token) => {
        const lowered = token.toLowerCase();
        if (lowered.startsWith("index:")) {
            const rawCodes = token.slice(token.indexOf(":") + 1).trim();
            if (!rawCodes) {
                return;
            }
            const parts = rawCodes
                .split(/[+&]/)
                .map((part) => part.trim().toUpperCase())
                .filter(Boolean);
            parts.forEach((code) => {
                const customSymbols = customIndexMap?.[code];
                if (customSymbols && customSymbols.length > 0) {
                    customSymbols.forEach((symbol) => {
                        const normalized = symbol.trim().toUpperCase();
                        if (normalized) {
                            includeSet.add(normalized);
                        }
                    });
                } else {
                    indexSet.add(code);
                }
            });
        } else {
            const cleaned = token.trim();
            if (cleaned) {
                includeSet.add(cleaned);
            }
        }
    });

    const payload: { include?: string | string[]; indices?: string[] } = {};

    const includeTokens = Array.from(includeSet);
    const indexTokens = Array.from(indexSet);

    if (includeTokens.length === 1) {
        payload.include = includeTokens[0];
    } else if (includeTokens.length > 1) {
        payload.include = includeTokens;
    }

    if (indexTokens.length) {
        payload.indices = indexTokens;
    }

    return Object.keys(payload).length ? payload : undefined;
};

const splitUniverseTokens = (value: string | null | undefined): string[] => {
    if (!value) {
        return [];
    }
    return value
        .split(/[\s,;]+/)
        .map((token) => token.trim())
        .filter(Boolean);
};

const toggleUniverseTokenValue = (
    value: string | null | undefined,
    token: string
): { next: string; isActive: boolean; changed: boolean } => {
    const normalizedToken = token.trim().toLowerCase();
    const currentTokens = splitUniverseTokens(value);
    const filtered = currentTokens.filter(
        (item) => item.toLowerCase() !== normalizedToken
    );

    if (filtered.length !== currentTokens.length) {
        return {
            next: filtered.length ? filtered.join(", ") : "",
            isActive: false,
            changed: true,
        };
    }

    filtered.push(token);
    return {
        next: filtered.join(", "),
        isActive: true,
        changed: true,
    };
};

const removeUniverseTokenValue = (
    value: string | null | undefined,
    token: string
): string => {
    const normalizedToken = token.trim().toLowerCase();
    const filtered = splitUniverseTokens(value).filter(
        (item) => item.toLowerCase() !== normalizedToken
    );
    return filtered.length ? filtered.join(", ") : "";
};

const universeIncludesToken = (
    value: string | null | undefined,
    token: string
): boolean => {
    const normalizedToken = token.trim().toLowerCase();
    return splitUniverseTokens(value).some(
        (item) => item.toLowerCase() === normalizedToken
    );
};

const extractIndexCodesFromUniverse = (
    value: string | string[] | null | undefined
): string[] => {
    if (value == null) {
        return [];
    }
    const entries = Array.isArray(value) ? value : [value];
    const collected: string[] = [];
    entries.forEach((entry) => {
        if (typeof entry !== "string") {
            return;
        }
        entry
            .split(/[\s,;]+/)
            .map((token) => token.trim())
            .filter(Boolean)
            .forEach((token) => {
                const lowered = token.toLowerCase();
                if (!lowered.startsWith("index:")) {
                    return;
                }
                const tail = token.slice(token.indexOf(":") + 1).trim();
                if (!tail) {
                    return;
                }
                tail
                    .split(/[+&]/)
                    .map((part) => part.trim().toUpperCase())
                    .filter(Boolean)
                    .forEach((code) => {
                        collected.push(code);
                    });
            });
    });
    return Array.from(new Set(collected));
};

const createCustomIndexId = (): string => {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
        return crypto.randomUUID();
    }
    return `custom-${Math.random().toString(36).slice(2, 10)}`;
};

const createCustomIndexRowId = (): string =>
    `row-${Math.random().toString(36).slice(2, 10)}-${Date.now().toString(36)}`;

const createEmptyCustomIndexRow = (): CustomIndexDraftRow => ({
    id: createCustomIndexRowId(),
    symbol: "",
    weightPct: "",
});

const toTemplateRule = (rule: ScoreBuilderRule): ScoreTemplateRule => {
    const option = findScoreMetric(rule.metric);
    const lookbackDays = resolveLookbackDays(option, rule.lookbackDays);
    const label =
        computeMetricLabel(option, lookbackDays, rule.label) ??
        rule.label ??
        option?.label ??
        rule.metric;
    const minValue = parseOptionalNumber(rule.min);
    const maxValue = parseOptionalNumber(rule.max);
    return {
        metric: rule.metric,
        weight: Number(rule.weight) || 0,
        direction: rule.direction === "asc" ? "asc" : "desc",
        label: label ?? null,
        transform: rule.transform ?? "raw",
        lookbackDays,
        min: minValue,
        max: maxValue,
    };
};

const fromTemplateRules = (rules: ScoreTemplateRule[]): ScoreBuilderRule[] =>
    rules.map((rule) => {
        const option = findScoreMetric(rule.metric);
        const lookbackDays = resolveLookbackDays(option, rule.lookbackDays);
        const label =
            computeMetricLabel(option, lookbackDays, rule.label) ??
            rule.label ??
            option?.label ??
            rule.metric;
        return {
            id: createRuleId(),
            metric: rule.metric,
            weight: Number(rule.weight) || 0,
            direction: rule.direction === "asc" ? "asc" : "desc",
            label,
            transform: rule.transform ?? "raw",
            lookbackDays,
            min: stringifyOptionalNumber(rule.min),
            max: stringifyOptionalNumber(rule.max),
        };
    });

const getDefaultScoreRules = (): ScoreBuilderRule[] => {
    const picks: { option: ScoreMetricOption; weight: number }[] = [
        { option: SCORE_METRIC_OPTIONS[2], weight: 40 },
        { option: SCORE_METRIC_OPTIONS[1], weight: 25 },
        { option: SCORE_METRIC_OPTIONS[3], weight: 20 },
        { option: SCORE_METRIC_OPTIONS[4], weight: 15 },
    ].filter((item) => item.option);

    return picks.map(({ option, weight }) => {
        const lookbackDays = resolveLookbackDays(option, option.lookback);
        const label =
            computeMetricLabel(option, lookbackDays) ?? option.label ?? option.value;
        return {
            id: createRuleId(),
            metric: option.value,
            label,
            weight,
            direction: option.defaultDirection,
            transform: "raw",
            lookbackDays,
        };
    });
};

const GOOGLE_CLIENT_ID =
    process.env.NEXT_PUBLIC_GOOGLE_CLIENT_ID ??
    process.env.GOOGLE_CLIENT_ID_FALLBACK ??
    "";
const GOOGLE_REDIRECT_PATH = "/api/auth/google/redirect";

type GoogleCredentialResponse = {
    credential?: string;
};

type GoogleIdConfiguration = {
    client_id: string;
    callback: (response: GoogleCredentialResponse) => void;
    ux_mode?: "popup" | "redirect";
    auto_select?: boolean;
    cancel_on_tap_outside?: boolean;
    login_uri?: string;
};

const shouldUseGoogleRedirect = (): boolean => {
    if (typeof window === "undefined") {
        return false;
    }
    const userAgent = `${window.navigator?.userAgent ?? ""} ${
        (window.navigator as Navigator & { vendor?: string }).vendor ?? ""
    }`;
    const isMobileDevice = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(
        userAgent
    );
    const isSmallViewport = window.innerWidth <= 768;
    return isMobileDevice || isSmallViewport;
};
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
    isAdmin: boolean;
};

type AdminEntry = {
    email: string;
    createdAt: string;
    addedBy: string | null;
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
type SymbolKind = "stock" | "index";

type SymbolRow = {
    symbol: string;
    name?: string | null;
    raw?: string | null;
    kind: SymbolKind;
    display?: string | null;
    ticker?: string | null;
    code?: string | null;
    isin?: string | null;
    company_name?: string | null;
    full_name?: string | null;
    short_name?: string | null;
};

type ComparisonMeta = {
    kind: SymbolKind;
    name?: string | null;
};

type WatchSnapshot = {
    latestPrice: number | null;
    change: number | null;
    changePct: number | null;
    kind: SymbolKind;
};

type WatchlistGroup = "owned" | "wishlist" | "index";

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

type BenchmarkChangePeriod = "1D" | "5D" | "1M" | "6M" | "YTD" | "1R" | "5L";

const BENCHMARK_CHANGE_PERIOD_OPTIONS: { value: BenchmarkChangePeriod; label: string }[] = [
    { value: "1D", label: "1 dzień (1D)" },
    { value: "5D", label: "5 dni (5D)" },
    { value: "1M", label: "1 miesiąc (1M)" },
    { value: "6M", label: "6 miesięcy (6M)" },
    { value: "YTD", label: "Od początku roku (YTD)" },
    { value: "1R", label: "1 rok (1R)" },
    { value: "5L", label: "5 lat (5L)" },
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
    "#4663F0",
    "#F59E0B",
    "#8B5CF6",
    "#EC4899",
    "#38BDF8",
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
    lookbackDays?: number | null;
};

type ScorePreviewRulePayload = {
    metric: string;
    weight: number;
    direction: "asc" | "desc";
    label?: string;
    lookbackDays?: number | null;
    min_value?: number;
    max_value?: number;
    scoring?: { type: "linear_clamped"; worst: number; best: number };
    normalize?: "none" | "percentile";
};

type ScorePreviewRequest = {
    name?: string;
    description?: string;
    rules: ScorePreviewRulePayload[];
    limit?: number;
    universe?: string | string[] | null;
    sort?: "asc" | "desc" | null;
    as_of?: string | null;
};

type ScoreTemplateRule = {
    metric: string;
    weight: number;
    direction: "asc" | "desc";
    label?: string | null;
    transform?: "raw" | "zscore" | "percentile" | "";
    lookbackDays?: number | null;
    min?: number | null;
    max?: number | null;
    scoring?: { type: "linear_clamped"; worst: number; best: number } | null;
    normalize?: "none" | "percentile" | null;
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

type ScorePreviewMissingRow = {
    symbol: string;
    raw?: string;
    reason: string;
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
    missing: ScorePreviewMissingRow[];
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
    fees?: number;
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

type CompanyFundamentalsResponse = Record<string, number | null>;

type CompanyProfileResponse = {
    symbol: string;
    raw_symbol: string;
    symbol_gpw?: string | null;
    symbol_gpw_benchmark?: string | null;
    symbol_stooq?: string | null;
    symbol_yahoo?: string | null;
    symbol_google?: string | null;
    name?: string | null;
    short_name?: string | null;
    isin?: string | null;
    sector?: string | null;
    industry?: string | null;
    country?: string | null;
    headquarters?: string | null;
    website?: string | null;
    description?: string | null;
    logo_url?: string | null;
    employees?: number | null;
    founded_year?: number | null;
    listing_date?: string | null;
    fundamentals: CompanyFundamentalsResponse;
    extra: Record<string, unknown>;
    raw: Record<string, unknown>;
};

type BenchmarkSymbolOption = {
    symbol: string;
    symbol_base?: string | null;
    indices: string[];
    company_name?: string | null;
};

type BenchmarkSymbolListResponse = {
    items: BenchmarkSymbolOption[];
};

type UniverseCandidateItem = {
    symbol: string;
    name?: string | null;
    isin?: string | null;
    sector?: string | null;
    industry?: string | null;
};

type UniverseCandidateListResponse = {
    total: number;
    items: UniverseCandidateItem[];
};

type JsonValue =
    | string
    | number
    | boolean
    | null
    | JsonValue[]
    | { [key: string]: JsonValue };

type CompanyRawInsights = {
    shareholding: string[];
    companySize: string | null;
    facts: { label: string; value: string }[];
};

type CompanySyncResultPayload = {
    fetched: number;
    synced: number;
    failed: number;
    errors: string[];
    started_at: string;
    finished_at: string;
};

type CompanySyncStatusPayload = {
    job_id: string | null;
    status: "idle" | "running" | "completed" | "failed";
    stage: "idle" | "fetching" | "harvesting" | "inserting" | "finished" | "failed";
    total: number | null;
    processed: number;
    synced: number;
    failed: number;
    started_at: string | null;
    finished_at: string | null;
    current_symbol: string | null;
    message: string | null;
    errors: string[];
    result?: CompanySyncResultPayload | null;
};

type CompanySyncScheduleStatusPayload = {
    mode: "idle" | "once" | "recurring";
    next_run_at: string | null;
    recurring_interval_minutes: number | null;
    recurring_start_at: string | null;
    last_run_started_at: string | null;
    last_run_finished_at: string | null;
    last_run_status: "idle" | "running" | "success" | "failed";
};

type HttpRequestLogEntry = {
    url: string;
    params: Record<string, unknown>;
    started_at: string;
    finished_at?: string | null;
    status_code?: number | null;
    error?: string | null;
    source?: string | null;
};

type OhlcSyncResultPayload = {
    symbols: number;
    inserted: number;
    skipped: number;
    errors: string[];
    started_at: string;
    finished_at: string;
    truncated: boolean;
    request_log: HttpRequestLogEntry[];
    requested_as_admin: boolean;
    sync_type: "historical_prices";
};

type OhlcImportResponsePayload = {
    inserted: number;
    skipped: number;
    errors: string[];
};

type OhlcSyncProgressPayload = {
    status: "idle" | "running" | "success" | "error";
    total_symbols: number;
    processed_symbols: number;
    inserted_rows: number;
    skipped_symbols: number;
    current_symbol: string | null;
    started_at: string | null;
    finished_at: string | null;
    message: string | null;
    errors: string[];
    requested_as_admin: boolean;
    result?: OhlcSyncResultPayload | null;
};

type OhlcSyncScheduleOptionsPayload = {
    symbols?: string[] | null;
    start?: string | null;
    truncate?: boolean;
    run_as_admin?: boolean;
};

type OhlcSyncScheduleStatusPayload = {
    mode: "idle" | "once" | "recurring";
    next_run_at: string | null;
    recurring_interval_minutes: number | null;
    recurring_start_at: string | null;
    last_run_started_at: string | null;
    last_run_finished_at: string | null;
    last_run_status: "idle" | "running" | "success" | "failed";
    options?: OhlcSyncScheduleOptionsPayload | null;
};

const COMPANY_STAGE_LABELS: Record<CompanySyncStatusPayload["stage"], string> = {
    idle: "Oczekiwanie",
    fetching: "Pobieranie listy spółek",
    harvesting: "Pobieranie profili",
    inserting: "Zapisywanie do bazy",
    finished: "Zakończono",
    failed: "Błąd",
};

const COMPANY_STATUS_LABELS: Record<CompanySyncStatusPayload["status"], string> = {
    idle: "Brak aktywnej synchronizacji",
    running: "Synchronizacja w toku",
    completed: "Synchronizacja zakończona",
    failed: "Synchronizacja zakończona błędem",
};

const OHLC_PROGRESS_STATUS_LABELS: Record<OhlcSyncProgressPayload["status"], string> = {
    idle: "Brak aktywnej synchronizacji",
    running: "Trwa synchronizacja",
    success: "Zakończono pomyślnie",
    error: "Błąd synchronizacji",
};

const SCHEDULE_MODE_LABELS: Record<CompanySyncScheduleStatusPayload["mode"], string> = {
    idle: "Brak aktywnego harmonogramu",
    once: "Jednorazowy",
    recurring: "Cykliczny",
};

const OHLC_SCHEDULE_MODE_LABELS: Record<OhlcSyncScheduleStatusPayload["mode"], string> = {
    idle: "Brak aktywnego harmonogramu",
    once: "Jednorazowy",
    recurring: "Cykliczny",
};

const SCHEDULE_STATUS_LABELS: Record<CompanySyncScheduleStatusPayload["last_run_status"], string> = {
    idle: "Brak uruchomień",
    running: "W trakcie",
    success: "Zakończono pomyślnie",
    failed: "Zakończono błędem",
};

const OHLC_SCHEDULE_STATUS_LABELS: Record<OhlcSyncScheduleStatusPayload["last_run_status"], string> = {
    idle: "Brak uruchomień",
    running: "W trakcie",
    success: "Zakończono pomyślnie",
    failed: "Zakończono błędem",
};

const FUNDAMENTAL_LABELS: Record<string, string> = {
    market_cap: "Kapitalizacja rynkowa",
    shares_outstanding: "Liczba akcji",
    book_value: "Wartość księgowa",
    revenue_ttm: "Przychody (TTM)",
    net_income_ttm: "Zysk netto (TTM)",
    ebitda_ttm: "EBITDA (TTM)",
    eps: "Zysk na akcję (EPS)",
    pe_ratio: "P/E",
    pb_ratio: "P/BV",
    dividend_yield: "Stopa dywidendy",
    roe: "ROE",
    roa: "ROA",
    gross_margin: "Marża brutto",
    operating_margin: "Marża operacyjna",
    profit_margin: "Marża netto",
};

const FUNDAMENTAL_ORDER = [
    "market_cap",
    "shares_outstanding",
    "book_value",
    "revenue_ttm",
    "net_income_ttm",
    "ebitda_ttm",
    "eps",
    "pe_ratio",
    "pb_ratio",
    "dividend_yield",
    "roe",
    "roa",
    "gross_margin",
    "operating_margin",
    "profit_margin",
];

const HIGHLIGHT_FUNDAMENTALS = [
    { key: "pe_ratio", label: "P/E" },
    { key: "pb_ratio", label: "P/BV" },
] as const;

const FUNDAMENTAL_PERCENT_KEYS = new Set<string>([
    "dividend_yield",
    "roe",
    "roa",
    "gross_margin",
    "operating_margin",
    "profit_margin",
]);

const FUNDAMENTAL_CURRENCY_KEYS = new Set<string>([
    "market_cap",
    "shares_outstanding",
    "book_value",
    "revenue_ttm",
    "net_income_ttm",
    "ebitda_ttm",
]);

const COMPANY_POLL_INTERVAL = 4000;
const COMPANY_FETCH_LIMIT = 200;

const parseApiError = async (response: Response): Promise<string> => {
    const text = await response.text();
    if (!text) {
        return response.statusText || "Wystąpił nieznany błąd";
    }
    try {
        const parsed = JSON.parse(text);
        if (parsed && typeof parsed === "object") {
            const record = parsed as Record<string, unknown>;
            const messageKeys: Array<"detail" | "error" | "message"> = [
                "detail",
                "error",
                "message",
            ];
            for (const key of messageKeys) {
                const value = record[key];
                if (typeof value === "string" && value.trim()) {
                    return value;
                }
            }
        }
    } catch {
        // ignorujemy – odpowiedź nie musi być JSON-em
    }
    return text;
};

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

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
async function searchSymbols(
    q: string,
    kinds: SymbolKind[] = ["stock", "index"]
): Promise<SymbolRow[]> {
    const query = q.trim();
    if (!query) {
        return [];
    }

    const wantStocks = kinds.includes("stock");
    const wantIndices = kinds.includes("index");

    const tasks: Array<Promise<SymbolRow[]>> = [];

    if (wantStocks) {
        type SymbolsApiRow = {
            symbol?: string;
            raw?: string | null;
            ticker?: string | null;
            code?: string | null;
            isin?: string | null;
            name?: string | null;
            company_name?: string | null;
            full_name?: string | null;
            short_name?: string | null;
            display?: string | null;
        };

        tasks.push(
            fetch(`${API}/symbols?q=${encodeURIComponent(query)}`)
                .then((response) => {
                    if (!response.ok) {
                        throw new Error(`API /symbols ${response.status}`);
                    }
                    return response.json() as Promise<SymbolsApiRow[]>;
                })
                .then((rows): SymbolRow[] =>
                    rows
                        .map<SymbolRow | null>((row) => {
                            const symbol = typeof row.symbol === "string" ? row.symbol.trim() : "";
                            if (!symbol) return null;
                            const normalized = symbol.toUpperCase();
                            const raw = typeof row.raw === "string" ? row.raw.trim() : null;
                            const resolvedName =
                                typeof row.name === "string" && row.name.trim()
                                    ? row.name.trim()
                                    : typeof row.company_name === "string" && row.company_name.trim()
                                      ? row.company_name.trim()
                                      : typeof row.full_name === "string" && row.full_name.trim()
                                        ? row.full_name.trim()
                                        : raw ?? null;
                            const normalizeOptional = (value: unknown): string | null => {
                                if (typeof value !== "string") return null;
                                const trimmed = value.trim();
                                return trimmed ? trimmed : null;
                            };
                            return {
                                symbol: normalized,
                                name: resolvedName,
                                raw: raw ?? normalized,
                                kind: "stock" as const,
                                display: normalizeOptional(row.display),
                                ticker: normalizeOptional(row.ticker),
                                code: normalizeOptional(row.code),
                                isin: normalizeOptional(row.isin),
                                company_name: normalizeOptional(row.company_name),
                                full_name: normalizeOptional(row.full_name),
                                short_name: normalizeOptional(row.short_name),
                            } satisfies SymbolRow;
                        })
                        .filter((row): row is SymbolRow => row !== null)
                )
                .catch((): SymbolRow[] => [])
        );
    }

    if (wantIndices) {
        tasks.push(
            fetch(
                `${API}/indices/list?q=${encodeURIComponent(query)}&limit=${encodeURIComponent("50")}`
            )
                .then((response) => {
                    if (!response.ok) {
                        throw new Error(`API /indices/list ${response.status}`);
                    }
                    return response.json() as Promise<
                        { items?: Array<{ code?: string; name?: string | null }> }
                    >;
                })
                .then((payload): SymbolRow[] =>
                    (payload.items ?? [])
                        .map<SymbolRow | null>((item) => {
                            const code = typeof item.code === "string" ? item.code.trim().toUpperCase() : "";
                            if (!code) return null;
                            const name = typeof item.name === "string" ? item.name.trim() : null;
                            return {
                                symbol: code,
                                name: name ?? null,
                                raw: code,
                                kind: "index" as const,
                            } satisfies SymbolRow;
                        })
                        .filter((row): row is SymbolRow => row !== null)
                )
                .catch((): SymbolRow[] => [])
        );
    }

    if (!tasks.length) {
        return [];
    }

    const settled = await Promise.allSettled(tasks);
    const combined: SymbolRow[] = [];
    settled.forEach((result) => {
        if (result.status === "fulfilled") {
            combined.push(...result.value);
        }
    });

    const unique = new Map<string, SymbolRow>();
    combined.forEach((row) => {
        if (!unique.has(row.symbol)) {
            unique.set(row.symbol, row);
        }
    });

    return Array.from(unique.values());
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

const normalizeIndexSymbol = (value: string): string => {
    const cleaned = value.trim().toUpperCase();
    return cleaned.endsWith(".WA") ? cleaned.slice(0, -3) : cleaned;
};

async function fetchIndexQuotes(symbol: string, start = "2015-01-01"): Promise<Row[]> {
    const normalized = normalizeIndexSymbol(symbol);
    if (!normalized) {
        return [];
    }
    const params = new URLSearchParams();
    params.set("codes", normalized);
    if (start) {
        params.set("start", start);
    }
    const response = await fetch(`${API}/indices/history?${params.toString()}`);
    if (!response.ok) {
        throw new Error(`API /indices/history ${response.status}`);
    }
    const payload = (await response.json()) as GpwBenchmarkHistoryResponse;
    const series = payload.items.find((item) => item.index_code === normalized) ?? payload.items[0];
    if (!series) {
        return [];
    }
    return series.points
        .filter((point) => point.date && point.value != null)
        .map((point) => {
            const value = Number(point.value ?? 0);
            return {
                date: point.date,
                open: value,
                high: value,
                low: value,
                close: value,
                volume: 0,
            } satisfies Row;
        });
}

const toUTCDate = (value: string | null | undefined): Date | null => {
    if (!value || typeof value !== "string") {
        return null;
    }
    const normalized = `${value}T00:00:00Z`;
    const parsed = new Date(normalized);
    if (Number.isNaN(parsed.getTime())) {
        return null;
    }
    return parsed;
};

const shiftUTCDate = (
    value: string,
    shift: { days?: number; months?: number; years?: number }
): string | null => {
    const base = toUTCDate(value);
    if (!base) {
        return null;
    }
    const next = new Date(
        Date.UTC(base.getUTCFullYear(), base.getUTCMonth(), base.getUTCDate())
    );
    if (shift.years) {
        next.setUTCFullYear(next.getUTCFullYear() - shift.years);
    }
    if (shift.months) {
        next.setUTCMonth(next.getUTCMonth() - shift.months);
    }
    if (shift.days) {
        next.setUTCDate(next.getUTCDate() - shift.days);
    }
    return next.toISOString().slice(0, 10);
};

const findValueOnOrAfter = (
    points: GpwBenchmarkHistoryPoint[],
    targetDate: string,
    lastDate?: string
): number | null => {
    if (!points.length) {
        return null;
    }
    let fallback: number | null = null;
    for (const point of points) {
        if (!point.date || point.value == null) {
            continue;
        }
        if (lastDate && point.date > lastDate) {
            break;
        }
        if (fallback == null) {
            fallback = point.value;
        }
        if (point.date >= targetDate) {
            return point.value;
        }
    }
    return fallback;
};

const computeBenchmarkBaselineValue = (
    points: GpwBenchmarkHistoryPoint[],
    period: BenchmarkChangePeriod
): number | null => {
    if (!points.length) {
        return null;
    }
    const ordered = [...points].sort((a, b) => a.date.localeCompare(b.date));
    const lastPoint = ordered[ordered.length - 1];
    if (!lastPoint || lastPoint.value == null) {
        return null;
    }
    if (period === "1D") {
        for (let i = ordered.length - 2; i >= 0; i -= 1) {
            const candidate = ordered[i];
            if (candidate && candidate.value != null) {
                return candidate.value;
            }
        }
        return null;
    }
    const lastDate = lastPoint.date;
    if (!lastDate) {
        return null;
    }
    let target: string | null = null;
    switch (period) {
        case "5D":
            target = shiftUTCDate(lastDate, { days: 5 });
            break;
        case "1M":
            target = shiftUTCDate(lastDate, { months: 1 });
            break;
        case "6M":
            target = shiftUTCDate(lastDate, { months: 6 });
            break;
        case "YTD": {
            const date = toUTCDate(lastDate);
            if (date) {
                target = `${date.getUTCFullYear()}-01-01`;
            }
            break;
        }
        case "1R":
            target = shiftUTCDate(lastDate, { years: 1 });
            break;
        case "5L":
            target = shiftUTCDate(lastDate, { years: 5 });
            break;
        default:
            target = shiftUTCDate(lastDate, { days: 1 });
            break;
    }
    if (!target) {
        return null;
    }
    return findValueOnOrAfter(ordered, target, lastDate);
};

async function computeCustomIndexSeries(
    index: CustomIndexDefinition
): Promise<GpwBenchmarkHistorySeries | null> {
    let entries = (index.constituents ?? [])
        .map((entry) => {
            const symbol = typeof entry.symbol === "string" ? entry.symbol.trim().toUpperCase() : "";
            const weightValue = Number(entry.weightPct);
            const weightPct = Number.isFinite(weightValue) && weightValue > 0 ? weightValue : 0;
            return { symbol, weightPct };
        })
        .filter((entry) => entry.symbol);

    if (!entries.length && Array.isArray(index.symbols)) {
        entries = index.symbols
            .map((symbol) => (typeof symbol === "string" ? symbol.trim().toUpperCase() : ""))
            .filter(Boolean)
            .map((symbol) => ({ symbol, weightPct: 0 }));
    }

    if (!entries.length) {
        return null;
    }

    const dedup = new Map<string, number>();
    for (const entry of entries) {
        const current = dedup.get(entry.symbol) ?? 0;
        dedup.set(entry.symbol, current + entry.weightPct);
    }

    const sanitizedEntries = Array.from(dedup.entries())
        .slice(0, MAX_UNIVERSE_FALLBACK_SYMBOLS)
        .map(([symbol, weightPct]) => ({ symbol, weightPct: weightPct > 0 ? weightPct : 0 }));

    if (!sanitizedEntries.length) {
        return null;
    }

    const startCandidate =
        typeof index.startDate === "string" && index.startDate.trim().length
            ? index.startDate.trim()
            : new Date().toISOString().slice(0, 10);
    const baseValueNumeric =
        typeof index.baseValue === "number" && Number.isFinite(index.baseValue) && index.baseValue > 0
            ? index.baseValue
            : 100;

    const fetchTasks = sanitizedEntries.map(async (entry) => {
        try {
            const rows = await fetchQuotes(entry.symbol, startCandidate);
            return { ...entry, rows };
        } catch {
            return null;
        }
    });

    const resolved = await Promise.all(fetchTasks);
    const prepared = resolved
        .map((item) => {
            if (!item || !Array.isArray(item.rows)) {
                return null;
            }
            const sorted = item.rows
                .filter((row): row is Row => Boolean(row && row.date && Number.isFinite(row.close)))
                .map((row) => ({ date: row.date, close: Number(row.close) }))
                .sort((a, b) => a.date.localeCompare(b.date));
            if (!sorted.length) {
                return null;
            }
            return { symbol: item.symbol, weightPct: item.weightPct, rows: sorted };
        })
        .filter(
            (item): item is { symbol: string; weightPct: number; rows: { date: string; close: number }[] } =>
                Boolean(item)
        );

    if (!prepared.length) {
        return null;
    }

    const startDates = prepared
        .map((series) => series.rows.find((row) => row.date >= startCandidate)?.date ?? series.rows[0]?.date ?? null)
        .filter((date): date is string => Boolean(date));
    if (!startDates.length) {
        return null;
    }
    const effectiveStart = startDates.reduce((max, value) => (value > max ? value : max), startDates[0]);

    const normalizedSeries = prepared
        .map((series) => {
            const rowsAfterStart = series.rows.filter((row) => row.date >= effectiveStart);
            if (!rowsAfterStart.length) {
                return null;
            }
            const basePrice = rowsAfterStart[0]?.close ?? null;
            if (basePrice == null || basePrice <= 0) {
                return null;
            }
            return {
                symbol: series.symbol,
                weightPct: series.weightPct,
                basePrice,
                rows: rowsAfterStart,
            };
        })
        .filter(
            (
                series
            ): series is {
                symbol: string;
                weightPct: number;
                basePrice: number;
                rows: { date: string; close: number }[];
            } => Boolean(series)
        );

    if (!normalizedSeries.length) {
        return null;
    }

    const dateSet = new Set<string>();
    normalizedSeries.forEach((series) => {
        series.rows.forEach((row) => {
            if (row.date >= effectiveStart) {
                dateSet.add(row.date);
            }
        });
    });
    const timeline = Array.from(dateSet).sort();
    if (!timeline.length) {
        return null;
    }

    const positiveWeightSum = normalizedSeries.reduce((sum, series) => {
        const value = Number(series.weightPct);
        return sum + (Number.isFinite(value) && value > 0 ? value : 0);
    }, 0);
    const normalizedWeights = normalizedSeries.map((series) => {
        if (positiveWeightSum > 0) {
            return Math.max(series.weightPct, 0) / positiveWeightSum;
        }
        return normalizedSeries.length ? 1 / normalizedSeries.length : 0;
    });

    const priceMaps = normalizedSeries.map((series) => {
        const map = new Map<string, number>();
        series.rows.forEach((row) => {
            if (row.close > 0) {
                map.set(row.date, row.close);
            }
        });
        return map;
    });
    const lastPrices = normalizedSeries.map((series) => series.basePrice);
    const points: GpwBenchmarkHistoryPoint[] = [];
    let prevValue: number | null = null;

    for (const date of timeline) {
        let missing = false;
        normalizedSeries.forEach((series, idx) => {
            const map = priceMaps[idx];
            const value = map.get(date);
            if (value != null && value > 0) {
                lastPrices[idx] = value;
            }
            if (!(lastPrices[idx] > 0)) {
                missing = true;
            }
        });
        if (missing) {
            continue;
        }
        let factor = 0;
        normalizedSeries.forEach((series, idx) => {
            factor += normalizedWeights[idx] * (lastPrices[idx] / series.basePrice);
        });
        const value = baseValueNumeric * factor;
        const changePct = prevValue && prevValue !== 0 ? (value - prevValue) / prevValue : null;
        points.push({ date, value, change_pct: changePct });
        prevValue = value;
    }

    if (!points.length) {
        return null;
    }

    return {
        index_code: index.code.trim().toUpperCase(),
        index_name: index.name?.trim() || index.code.trim().toUpperCase(),
        points,
    };
}

type InstrumentSeriesResult = { rows: Row[]; kind: SymbolKind };

async function fetchInstrumentSeries(
    symbol: string,
    kindHint: SymbolKind | undefined,
    start: string
): Promise<InstrumentSeriesResult> {
    const normalized = symbol.trim().toUpperCase();
    if (!normalized) {
        return { rows: [], kind: kindHint ?? "stock" };
    }

    if (kindHint === "index") {
        const rows = await fetchIndexQuotes(normalized, start);
        return { rows, kind: "index" };
    }

    if (kindHint === "stock") {
        const rows = await fetchQuotes(normalized, start);
        return { rows, kind: "stock" };
    }

    try {
        const rows = await fetchQuotes(normalized, start);
        return { rows, kind: "stock" };
    } catch (error) {
        const indexRows = await fetchIndexQuotes(normalized, start);
        if (!indexRows.length) {
            throw error;
        }
        return { rows: indexRows, kind: "index" };
    }
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
    fallbackUniverse?: string[] | null,
    customIndexMap?: Record<string, string[]>
): Promise<PortfolioResp> {
    const {
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

    const topN = Math.max(limitCandidate ?? components.length, 1);

    const resolvedUniverse = resolveUniverseWithFallback(universe, fallbackUniverse);

    const autoPayload = removeUndefined({
        top_n: topN,
        weighting: weighting === "score" ? "score" : "equal",
        direction: direction === "asc" ? "asc" : "desc",
        min_score: typeof minScore === "number" ? minScore : undefined,
        max_score: typeof maxScore === "number" ? maxScore : undefined,
        components: components.map((component) => ({
            metric: component.metric,
            lookback_days: component.lookback_days,
            weight: component.weight,
            direction: component.direction,
            min_value: component.min_value,
            max_value: component.max_value,
            ...(component.scoring ? { scoring: component.scoring } : {}),
            normalize: component.normalize ?? "none",
        })),
        filters: buildUniverseFiltersPayload(resolvedUniverse, customIndexMap),
    });

    const payload = removeUndefined({
        start,
        end,
        rebalance,
        initial_capital: initialCapital,
        fee_pct: feePct,
        threshold_pct: thresholdPct,
        benchmark: benchmark?.trim() ? benchmark.trim() : undefined,
        auto: autoPayload,
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
            // ignore – fallback to generic error below
        }
        throw new Error(message || `API /backtest/portfolio ${response.status}`);
    }

    const json = await response.json();
    return normalizePortfolioResponse(json);
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
        fees: pickNumber(statsSources, ["fees", "fees_paid", "transaction_fees", "costs"]),
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

    const missing: ScorePreviewMissingRow[] = [];
    const missingSource = getProp(raw, "missing");
    if (Array.isArray(missingSource)) {
        missingSource.forEach((item) => {
            if (!item || typeof item !== "object") return;
            const rawSymbolCandidate = getProp(item, "raw") ?? getProp(item, "symbol");
            const symbolCandidate = getProp(item, "symbol") ?? getProp(item, "name");
            const reasonCandidate = getProp(item, "reason") ?? getProp(item, "message");
            const rawSymbol = rawSymbolCandidate ? String(rawSymbolCandidate) : undefined;
            const symbol = symbolCandidate
                ? String(symbolCandidate)
                : rawSymbol ?? undefined;
            const reason = reasonCandidate
                ? String(reasonCandidate)
                : "Brak informacji o przyczynie.";
            if (!symbol && !rawSymbol) return;
            missing.push({
                symbol: symbol ?? (rawSymbol ?? ""),
                raw: rawSymbol,
                reason,
            });
        });
    }

    return { rows, missing, meta };
};

async function previewScoreRanking(
    payload: ScorePreviewRequest,
    options?: { signal?: AbortSignal }
): Promise<ScorePreviewResult> {
    if (!payload.rules.length) {
        throw new Error("Dodaj co najmniej jedną metrykę scoringową.");
    }

    const preparedRules = payload.rules.map((rule) =>
        removeUndefined({
            metric: rule.metric,
            weight: rule.weight,
            direction: rule.direction,
            label: rule.label,
            min_value: rule.min_value,
            max_value: rule.max_value,
            scoring: rule.scoring,
            normalize: rule.normalize ?? "none",
        })
    );

    const prepared = removeUndefined({
        name: payload.name,
        description: payload.description,
        rules: preparedRules,
        limit: payload.limit,
        universe: payload.universe ?? undefined,
        sort: payload.sort ?? undefined,
        as_of: payload.as_of ?? undefined,
    });

    const response = await fetch("/api/score/preview", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(prepared),
        signal: options?.signal,
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
    <div className="rounded-3xl border border-soft bg-surface shadow-[0_18px_40px_rgba(15,23,42,0.08)]">
        {(title || right) && (
            <div className="flex flex-col gap-3 border-b border-soft/60 px-5 py-4 md:flex-row md:items-center md:justify-between md:px-8">
                {title && <div className="text-lg font-semibold text-neutral md:flex-1">{title}</div>}
                {right && (
                    <div className="flex w-full flex-wrap gap-2 sm:w-auto sm:justify-end">{right}</div>
                )}
            </div>
        )}
        <div className="px-5 py-5 md:px-8 md:py-8">{children}</div>
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
        <div className="flex flex-col gap-6 md:flex-row md:items-end md:justify-between">
            <div className="space-y-3">
                {kicker && (
                    <span className="text-xs font-semibold uppercase tracking-[0.28em] text-primary">
                        {kicker}
                    </span>
                )}
                <div className="space-y-2">
                    <h2 className="text-3xl font-semibold text-neutral md:text-[2.5rem]">
                        {title}
                    </h2>
                    {description && (
                        <p className="max-w-3xl text-base text-subtle">{description}</p>
                    )}
                </div>
            </div>
            {actions && <div className="flex flex-wrap gap-3">{actions}</div>}
        </div>
        <div className="mt-10">{children}</div>
    </section>
);

type CompanySyncPanelProps = {
    symbol: string | null;
    setSymbol: (
        value: string | null | ((prev: string | null) => string | null)
    ) => void;
};

const CompanySyncPanel = ({ symbol, setSymbol }: CompanySyncPanelProps) => {
    const [status, setStatus] = useState<CompanySyncStatusPayload | null>(null);
    const [statusError, setStatusError] = useState<string | null>(null);
    const [companies, setCompanies] = useState<CompanyProfileResponse[]>([]);
    const [companiesError, setCompaniesError] = useState<string | null>(null);
    const [companiesLoading, setCompaniesLoading] = useState(false);
    const [selectedCompany, setSelectedCompany] = useState<CompanyProfileResponse | null>(null);
    const [detailsError, setDetailsError] = useState<string | null>(null);
    const [detailsLoading, setDetailsLoading] = useState(false);
    const [isStarting, setIsStarting] = useState(false);
    const [isStartingLocal, setIsStartingLocal] = useState(false);
    const [search, setSearch] = useState("");
    const [activeQuery, setActiveQuery] = useState<string | undefined>(undefined);
    const [schedule, setSchedule] = useState<CompanySyncScheduleStatusPayload | null>(null);
    const [scheduleError, setScheduleError] = useState<string | null>(null);
    const [scheduleSuccess, setScheduleSuccess] = useState<string | null>(null);
    const [isScheduling, setIsScheduling] = useState(false);
    const [onceDateInput, setOnceDateInput] = useState("");
    const [recurringIntervalInput, setRecurringIntervalInput] = useState("1440");
    const [recurringStartInput, setRecurringStartInput] = useState("");
    const [admins, setAdmins] = useState<AdminEntry[]>([]);
    const [adminsError, setAdminsError] = useState<string | null>(null);
    const [adminsSuccess, setAdminsSuccess] = useState<string | null>(null);
    const [isAddingAdmin, setIsAddingAdmin] = useState(false);
    const [newAdminEmail, setNewAdminEmail] = useState("");
    const [universeFiltersInput, setUniverseFiltersInput] = useState("index:WIG40");
    const [universeIncludeMetadata, setUniverseIncludeMetadata] = useState(true);
    const [universeCandidates, setUniverseCandidates] = useState<UniverseCandidateItem[]>([]);
    const [universeTotal, setUniverseTotal] = useState<number | null>(null);
    const [universeLoading, setUniverseLoading] = useState(false);
    const [universeError, setUniverseError] = useState<string | null>(null);
    const [universeFetched, setUniverseFetched] = useState(false);
    const [ohlcSymbolsInput, setOhlcSymbolsInput] = useState("");
    const [ohlcStartInput, setOhlcStartInput] = useState("");
    const [ohlcTruncate, setOhlcTruncate] = useState(false);
    const [ohlcRunAsAdmin, setOhlcRunAsAdmin] = useState(true);
    const [ohlcIsSyncing, setOhlcIsSyncing] = useState(false);
    const [ohlcError, setOhlcError] = useState<string | null>(null);
    const [ohlcResult, setOhlcResult] = useState<OhlcSyncResultPayload | null>(null);
    const [ohlcRequestLog, setOhlcRequestLog] = useState<HttpRequestLogEntry[]>([]);
    const [ohlcShowRequestLog, setOhlcShowRequestLog] = useState(false);
    const [ohlcProgress, setOhlcProgress] = useState<OhlcSyncProgressPayload | null>(null);
    const [ohlcSchedule, setOhlcSchedule] = useState<OhlcSyncScheduleStatusPayload | null>(null);
    const [ohlcScheduleError, setOhlcScheduleError] = useState<string | null>(null);
    const [ohlcScheduleSuccess, setOhlcScheduleSuccess] = useState<string | null>(null);
    const [ohlcIsScheduling, setOhlcIsScheduling] = useState(false);
    const [ohlcScheduleMode, setOhlcScheduleMode] = useState<"once" | "recurring">("once");
    const [ohlcOnceDateInput, setOhlcOnceDateInput] = useState("");
    const [ohlcRecurringIntervalInput, setOhlcRecurringIntervalInput] = useState("1440");
    const [ohlcRecurringStartInput, setOhlcRecurringStartInput] = useState("");
    const [ohlcScheduleSymbolsInput, setOhlcScheduleSymbolsInput] = useState("");
    const [ohlcScheduleStartInput, setOhlcScheduleStartInput] = useState("");
    const [ohlcScheduleTruncate, setOhlcScheduleTruncate] = useState(false);
    const [ohlcScheduleRunAsAdmin, setOhlcScheduleRunAsAdmin] = useState(true);
    const [ohlcImporting, setOhlcImporting] = useState(false);
    const [ohlcImportError, setOhlcImportError] = useState<string | null>(null);
    const [ohlcImportSuccess, setOhlcImportSuccess] = useState<string | null>(null);
    const ohlcImportInputRef = useRef<HTMLInputElement | null>(null);
    const [localClickhouseMode, setLocalClickhouseMode] = useState<"url" | "manual">(
        "url"
    );
    const [localClickhouseUrl, setLocalClickhouseUrl] = useState("");
    const [localClickhouseHost, setLocalClickhouseHost] = useState("");
    const [localClickhousePort, setLocalClickhousePort] = useState("");
    const [localClickhouseDatabase, setLocalClickhouseDatabase] = useState("");
    const [localClickhouseUsername, setLocalClickhouseUsername] = useState("");
    const [localClickhousePassword, setLocalClickhousePassword] = useState("");
    const [localClickhouseSecure, setLocalClickhouseSecure] = useState(true);
    const [localClickhouseVerify, setLocalClickhouseVerify] = useState(true);
    const [localClickhouseCa, setLocalClickhouseCa] = useState("");
    const [localClickhouseDirty, setLocalClickhouseDirty] = useState(false);
    const [localClickhouseSaving, setLocalClickhouseSaving] = useState(false);
    const [localClickhouseError, setLocalClickhouseError] = useState<string | null>(
        null
    );
    const [localClickhouseSuccess, setLocalClickhouseSuccess] = useState<
        string | null
    >(null);
    const [localClickhouseAppliedAt, setLocalClickhouseAppliedAt] = useState<
        number | null
    >(null);
    const [localClickhouseStatus, setLocalClickhouseStatus] =
        useState<LocalClickhouseConfigState | null>(null);
    const [localClickhouseBackendReachable, setLocalClickhouseBackendReachable] =
        useState(false);
    const [benchmarkSymbols, setBenchmarkSymbols] = useState<BenchmarkSymbolOption[]>([]);
    const [benchmarkSymbolsLoading, setBenchmarkSymbolsLoading] = useState(false);
    const [benchmarkSymbolsError, setBenchmarkSymbolsError] = useState<string | null>(
        null
    );
    const [benchmarkInputs, setBenchmarkInputs] = useState<Record<string, string>>({});
    const [benchmarkOriginals, setBenchmarkOriginals] =
        useState<Record<string, string>>({});
    const [benchmarkStatuses, setBenchmarkStatuses] = useState<
        Record<string, { status: "idle" | "saving" | "success" | "error"; message: string | null }>
    >({});
    const benchmarkOriginalsRef = useRef<Record<string, string>>({});

    const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);
    const selectedSymbolRef = useRef<string | null>(
        symbol ?? DEFAULT_WATCHLIST[0] ?? null
    );


    useEffect(() => {
        selectedSymbolRef.current = symbol ?? null;
    }, [symbol]);

    useEffect(() => {
        try {
            const raw = localStorage.getItem(LOCAL_CLICKHOUSE_STORAGE_KEY);
            if (!raw) {
                return;
            }
            const parsed = JSON.parse(raw) as LocalClickhousePersistedConfig;
            if (parsed.mode === "manual" || parsed.mode === "url") {
                setLocalClickhouseMode(parsed.mode);
            }
            setLocalClickhouseUrl(parsed.url ?? "");
            setLocalClickhouseHost(parsed.host ?? "");
            setLocalClickhousePort(parsed.port ?? "");
            setLocalClickhouseDatabase(parsed.database ?? "");
            setLocalClickhouseUsername(parsed.username ?? "");
            setLocalClickhousePassword(parsed.password ?? "");
            setLocalClickhouseSecure(
                parsed.secure !== undefined ? Boolean(parsed.secure) : true
            );
            setLocalClickhouseVerify(
                parsed.verify !== undefined ? Boolean(parsed.verify) : true
            );
            setLocalClickhouseCa(parsed.ca ?? "");
            const hasConfig =
                (parsed.mode === "url" && (parsed.url?.trim().length ?? 0) > 0) ||
                (parsed.mode === "manual" && (parsed.host?.trim().length ?? 0) > 0);
            setLocalClickhouseDirty(hasConfig);
            setLocalClickhouseAppliedAt(null);
        } catch {
            // Ignorujemy błędy parsowania – użytkownik może nadpisać konfigurację ręcznie.
        }
    }, []);

    useEffect(() => {
        let cancelled = false;
        (async () => {
            try {
                const response = await fetch(`${LOCAL_ADMIN_API}/config/clickhouse`, {
                    cache: "no-store",
                });
                if (!response.ok) {
                    return;
                }
                const data = (await response.json()) as LocalClickhouseConfigState;
                if (cancelled) {
                    return;
                }
                setLocalClickhouseStatus(data);
                setLocalClickhouseBackendReachable(true);
            } catch {
                if (cancelled) {
                    return;
                }
                setLocalClickhouseBackendReachable(false);
            }
        })();
        return () => {
            cancelled = true;
        };
    }, []);

    const numberFormatter = useMemo(
        () => new Intl.NumberFormat("pl-PL", { maximumFractionDigits: 2 }),
        []
    );
    const integerFormatter = useMemo(
        () => new Intl.NumberFormat("pl-PL", { maximumFractionDigits: 0 }),
        []
    );
    const percentFormatter = useMemo(
        () => new Intl.NumberFormat("pl-PL", { style: "percent", maximumFractionDigits: 2 }),
        []
    );

    const formatDateTime = (value?: string | null) => {
        if (!value) return "—";
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) return value;
        return parsed.toLocaleString("pl-PL", { hour12: false });
    };

    const applyLocalClickhouseConfig = useCallback(async (): Promise<LocalClickhouseEnsureResult> => {
        const trimmedUrl = localClickhouseUrl.trim();
        const trimmedHost = localClickhouseHost.trim();
        const trimmedPort = localClickhousePort.trim();
        const trimmedDatabase = localClickhouseDatabase.trim();
        const trimmedUsername = localClickhouseUsername.trim();
        const trimmedPassword = localClickhousePassword;
        const trimmedCa = localClickhouseCa.trim();

        const requiresConfig =
            (localClickhouseMode === "url" && trimmedUrl.length > 0) ||
            (localClickhouseMode === "manual" && trimmedHost.length > 0);

        if (!requiresConfig) {
            const error =
                "Podaj konfigurację ClickHouse lub przywróć ustawienia domyślne.";
            setLocalClickhouseError(error);
            setLocalClickhouseSuccess(null);
            return { ok: false, error };
        }

        if (trimmedPort.length > 0) {
            const portNumber = Number(trimmedPort);
            if (
                !Number.isInteger(portNumber) ||
                portNumber <= 0 ||
                portNumber > 65535
            ) {
                const error = "Port ClickHouse musi być liczbą z zakresu 1-65535.";
                setLocalClickhouseError(error);
                setLocalClickhouseSuccess(null);
                return { ok: false, error };
            }
        }

        const payload: Record<string, unknown> = {
            mode: localClickhouseMode,
            secure: localClickhouseSecure,
            verify: localClickhouseVerify,
        };
        if (localClickhouseMode === "url") {
            payload.url = trimmedUrl;
        } else {
            payload.host = trimmedHost;
        }
        if (trimmedPort.length > 0) {
            payload.port = Number(trimmedPort);
        }
        if (trimmedDatabase.length > 0) {
            payload.database = trimmedDatabase;
        }
        if (trimmedUsername.length > 0) {
            payload.username = trimmedUsername;
        }
        if (trimmedPassword.length > 0) {
            payload.password = trimmedPassword;
        }
        if (trimmedCa.length > 0) {
            payload.ca = trimmedCa;
        }

        setLocalClickhouseSaving(true);
        setLocalClickhouseError(null);
        setLocalClickhouseSuccess(null);
        try {
            const response = await fetch(`${LOCAL_ADMIN_API}/config/clickhouse`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });
            if (!response.ok) {
                throw new Error(await parseApiError(response));
            }
            const data = (await response.json()) as LocalClickhouseConfigState;
            setLocalClickhouseStatus(data);
            setLocalClickhouseBackendReachable(true);
            setLocalClickhouseSuccess(
                "Zapisano konfigurację połączenia z ClickHouse."
            );
            setLocalClickhouseAppliedAt(Date.now());
            setLocalClickhouseDirty(false);
            try {
                const toPersist: LocalClickhousePersistedConfig = {
                    mode: localClickhouseMode,
                    url:
                        localClickhouseMode === "url" && trimmedUrl.length > 0
                            ? trimmedUrl
                            : undefined,
                    host:
                        localClickhouseMode === "manual" && trimmedHost.length > 0
                            ? trimmedHost
                            : undefined,
                    port: trimmedPort.length > 0 ? trimmedPort : undefined,
                    database:
                        trimmedDatabase.length > 0 ? trimmedDatabase : undefined,
                    username:
                        trimmedUsername.length > 0 ? trimmedUsername : undefined,
                    password:
                        trimmedPassword.length > 0 ? trimmedPassword : undefined,
                    secure: localClickhouseSecure,
                    verify: localClickhouseVerify,
                    ca: trimmedCa.length > 0 ? trimmedCa : undefined,
                };
                localStorage.setItem(
                    LOCAL_CLICKHOUSE_STORAGE_KEY,
                    JSON.stringify(toPersist)
                );
            } catch {
                // Ignorujemy błąd zapisu w localStorage.
            }
            return { ok: true };
        } catch (error) {
            const message = resolveErrorMessage(
                error,
                "Nie udało się zapisać konfiguracji ClickHouse"
            );
            setLocalClickhouseError(message);
            setLocalClickhouseSuccess(null);
            if (isNetworkError(error)) {
                setLocalClickhouseBackendReachable(false);
            }
            return { ok: false, error: message };
        } finally {
            setLocalClickhouseSaving(false);
        }
    }, [
        localClickhouseMode,
        localClickhouseUrl,
        localClickhouseHost,
        localClickhousePort,
        localClickhouseDatabase,
        localClickhouseUsername,
        localClickhousePassword,
        localClickhouseSecure,
        localClickhouseVerify,
        localClickhouseCa,
    ]);

    const ensureLocalClickhouseConfig = useCallback(async (): Promise<LocalClickhouseEnsureResult> => {
        const trimmedUrl = localClickhouseUrl.trim();
        const trimmedHost = localClickhouseHost.trim();
        const requiresConfig =
            (localClickhouseMode === "url" && trimmedUrl.length > 0) ||
            (localClickhouseMode === "manual" && trimmedHost.length > 0);
        if (!requiresConfig) {
            return { ok: true };
        }
        if (localClickhouseDirty || !localClickhouseAppliedAt) {
            return applyLocalClickhouseConfig();
        }
        return { ok: true };
    }, [
        applyLocalClickhouseConfig,
        localClickhouseMode,
        localClickhouseUrl,
        localClickhouseHost,
        localClickhouseDirty,
        localClickhouseAppliedAt,
    ]);

    const handleResetLocalClickhouse = useCallback(async () => {
        setLocalClickhouseSaving(true);
        setLocalClickhouseError(null);
        setLocalClickhouseSuccess(null);
        try {
            const response = await fetch(`${LOCAL_ADMIN_API}/config/clickhouse`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ reset: true }),
            });
            if (!response.ok) {
                throw new Error(await parseApiError(response));
            }
            const data = (await response.json()) as LocalClickhouseConfigState;
            setLocalClickhouseStatus(data);
            setLocalClickhouseBackendReachable(true);
            setLocalClickhouseSuccess(
                "Przywrócono domyślną konfigurację ClickHouse."
            );
            setLocalClickhouseMode("url");
            setLocalClickhouseUrl("");
            setLocalClickhouseHost("");
            setLocalClickhousePort("");
            setLocalClickhouseDatabase("");
            setLocalClickhouseUsername("");
            setLocalClickhousePassword("");
            setLocalClickhouseSecure(true);
            setLocalClickhouseVerify(true);
            setLocalClickhouseCa("");
            setLocalClickhouseDirty(false);
            setLocalClickhouseAppliedAt(Date.now());
            try {
                localStorage.removeItem(LOCAL_CLICKHOUSE_STORAGE_KEY);
            } catch {
                // Ignorujemy błąd usunięcia.
            }
            return true;
        } catch (error) {
            const message = resolveErrorMessage(
                error,
                "Nie udało się przywrócić domyślnej konfiguracji ClickHouse"
            );
            setLocalClickhouseError(message);
            if (isNetworkError(error)) {
                setLocalClickhouseBackendReachable(false);
            }
            return false;
        } finally {
            setLocalClickhouseSaving(false);
        }
    }, []);

    const refreshLocalClickhouseStatus = useCallback(async () => {
        try {
            const response = await fetch(`${LOCAL_ADMIN_API}/config/clickhouse`, {
                cache: "no-store",
            });
            if (!response.ok) {
                throw new Error(await parseApiError(response));
            }
            const data = (await response.json()) as LocalClickhouseConfigState;
            setLocalClickhouseStatus(data);
            setLocalClickhouseBackendReachable(true);
            setLocalClickhouseSuccess(
                "Odświeżono konfigurację ClickHouse z backendu lokalnego."
            );
            setLocalClickhouseError(null);
        } catch (error) {
            const message = resolveErrorMessage(
                error,
                "Nie udało się pobrać konfiguracji ClickHouse z backendu lokalnego"
            );
            setLocalClickhouseError(message);
            setLocalClickhouseSuccess(null);
            if (isNetworkError(error)) {
                setLocalClickhouseBackendReachable(false);
            }
        }
    }, []);

    const formatDate = (value?: string | null) => {
        if (!value) return "—";
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) return value;
        return parsed.toLocaleDateString("pl-PL");
    };

    const formatDuration = (start?: string | null, end?: string | null) => {
        if (!start || !end) return "—";
        const started = new Date(start);
        const finished = new Date(end);
        if (Number.isNaN(started.getTime()) || Number.isNaN(finished.getTime())) {
            return "—";
        }
        const diffMs = finished.getTime() - started.getTime();
        if (diffMs < 0) return "—";
        if (diffMs < 1000) {
            return `${diffMs} ms`;
        }
        const totalSeconds = diffMs / 1000;
        if (totalSeconds < 60) {
            return `${totalSeconds.toFixed(1)} s`;
        }
        const minutes = Math.floor(totalSeconds / 60);
        const seconds = Math.round(totalSeconds % 60);
        if (minutes >= 60) {
            const hours = Math.floor(minutes / 60);
            const remainingMinutes = minutes % 60;
            const parts: string[] = [`${hours} h`];
            if (remainingMinutes) {
                parts.push(`${remainingMinutes} min`);
            }
            if (seconds) {
                parts.push(`${seconds} s`);
            }
            return parts.join(" ");
        }
        const parts: string[] = [`${minutes} min`];
        if (seconds) {
            parts.push(`${seconds} s`);
        }
        return parts.join(" ");
    };

    const formatFundamentalValue = (key: string, value: number | null) => {
        if (value === null || value === undefined || Number.isNaN(value)) {
            return "—";
        }
        if (FUNDAMENTAL_PERCENT_KEYS.has(key)) {
            return percentFormatter.format(value);
        }
        if (FUNDAMENTAL_CURRENCY_KEYS.has(key)) {
            return integerFormatter.format(value);
        }
        return numberFormatter.format(value);
    };

    const toLocalDateTimeInputValue = useCallback((value: string | null | undefined) => {
        if (!value) return "";
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) return "";
        const pad = (num: number) => num.toString().padStart(2, "0");
        const date = `${parsed.getFullYear()}-${pad(parsed.getMonth() + 1)}-${pad(parsed.getDate())}`;
        const time = `${pad(parsed.getHours())}:${pad(parsed.getMinutes())}`;
        return `${date}T${time}`;
    }, []);

    const toIsoDateTime = useCallback((value: string, errorMessage: string) => {
        const trimmed = value.trim();
        if (!trimmed) {
            throw new Error(errorMessage);
        }
        const parsed = new Date(trimmed);
        if (Number.isNaN(parsed.getTime())) {
            throw new Error(errorMessage);
        }
        return parsed.toISOString();
    }, []);

    const toOptionalIsoDateTime = useCallback(
        (value: string, errorMessage: string) => {
            const trimmed = value.trim();
            if (!trimmed) {
                return null;
            }
            const parsed = new Date(trimmed);
            if (Number.isNaN(parsed.getTime())) {
                throw new Error(errorMessage);
            }
            return parsed.toISOString();
        },
        []
    );

    const toOptionalIsoDate = useCallback(
        (value: string, errorMessage: string) => {
            const trimmed = value.trim();
            if (!trimmed) {
                return null;
            }
            const parsed = new Date(trimmed);
            if (Number.isNaN(parsed.getTime())) {
                throw new Error(errorMessage);
            }
            return parsed.toISOString().slice(0, 10);
        },
        []
    );

    const formatIntervalLabel = useCallback((minutes: number | null) => {
        if (!minutes || !Number.isFinite(minutes)) {
            return "—";
        }
        if (minutes % 1440 === 0) {
            const days = Math.round(minutes / 1440);
            return `co ${days} ${days === 1 ? "dzień" : days < 5 ? "dni" : "dni"}`;
        }
        if (minutes % 60 === 0) {
            const hours = Math.round(minutes / 60);
            return `co ${hours} ${hours === 1 ? "godzinę" : hours < 5 ? "godziny" : "godzin"}`;
        }
        return `co ${minutes} minut`;
    }, []);

    const ohlcHasErrors = Boolean(ohlcResult?.errors?.length);
    const ohlcProgressPercent = useMemo(() => {
        if (!ohlcProgress) {
            return 0;
        }
        if (ohlcProgress.total_symbols > 0) {
            const ratio = ohlcProgress.processed_symbols / ohlcProgress.total_symbols;
            const bounded = Math.max(0, Math.min(1, Number.isFinite(ratio) ? ratio : 0));
            return Math.round(bounded * 100);
        }
        return ohlcProgress.status === "success" ? 100 : 0;
    }, [ohlcProgress]);
    const ohlcProgressStatusLabel = useMemo(
        () => (ohlcProgress ? OHLC_PROGRESS_STATUS_LABELS[ohlcProgress.status] : null),
        [ohlcProgress]
    );

    const stopPolling = useCallback(() => {
        if (pollingRef.current) {
            clearInterval(pollingRef.current);
            pollingRef.current = null;
        }
    }, []);

    const fetchStatus = useCallback(async () => {
        try {
            const response = await fetch(`${ADMIN_API}/companies/sync/status`, {
                cache: "no-store",
            });
            if (!response.ok) {
                throw new Error(await parseApiError(response));
            }
            const payload = (await response.json()) as CompanySyncStatusPayload;
            setStatus(payload);
            setStatusError(null);
        } catch (error) {
            if (error instanceof Error && error.message) {
                setStatusError(error.message);
            } else {
                setStatusError("Nie udało się pobrać statusu synchronizacji");
            }
        }
    }, []);

    const fetchSchedule = useCallback(async () => {
        setScheduleError(null);
        try {
            const response = await fetch(`${ADMIN_API}/companies/sync/schedule`, {
                cache: "no-store",
            });
            if (!response.ok) {
                throw new Error(await parseApiError(response));
            }
            const payload = (await response.json()) as CompanySyncScheduleStatusPayload;
            setSchedule(payload);
        } catch (error) {
            if (error instanceof Error && error.message) {
                setScheduleError(error.message);
            } else {
                setScheduleError("Nie udało się pobrać harmonogramu synchronizacji");
            }
        }
    }, []);

    const fetchOhlcSchedule = useCallback(async () => {
        setOhlcScheduleError(null);
        try {
            const response = await fetch(`${ADMIN_API}/ohlc/sync/schedule`, {
                cache: "no-store",
            });
            if (!response.ok) {
                throw new Error(await parseApiError(response));
            }
            const payload = (await response.json()) as OhlcSyncScheduleStatusPayload;
            setOhlcSchedule(payload);
        } catch (error) {
            if (error instanceof Error && error.message) {
                setOhlcScheduleError(error.message);
            } else {
                setOhlcScheduleError("Nie udało się pobrać harmonogramu notowań");
            }
        }
    }, []);

    const fetchAdmins = useCallback(async () => {
        setAdminsError(null);
        try {
            const response = await fetch(`/api/admins`, { cache: "no-store" });
            if (!response.ok) {
                throw new Error(await parseApiError(response));
            }
            const payload = (await response.json()) as { admins: AdminEntry[] };
            setAdmins(payload.admins);
        } catch (error) {
            if (error instanceof Error && error.message) {
                setAdminsError(error.message);
            } else {
                setAdminsError("Nie udało się pobrać listy administratorów");
            }
        }
    }, []);

    const fetchOhlcProgress = useCallback(async () => {
        try {
            const response = await fetch(`/api/admin/ohlc/progress`, { cache: "no-store" });
            if (!response.ok) {
                throw new Error(await parseApiError(response));
            }
            const payload = (await response.json()) as OhlcSyncProgressPayload;
            setOhlcProgress(payload);
            if (payload?.result) {
                setOhlcResult(payload.result);
                setOhlcRequestLog(payload.result.request_log ?? []);
                setOhlcShowRequestLog(false);
            }
            return payload;
        } catch (error) {
            if (error instanceof Error && error.message) {
                setOhlcProgress((prev) =>
                    prev
                        ? {
                              ...prev,
                              message: error.message,
                          }
                        : prev
                );
            }
            return null;
        }
    }, []);

    const fetchCompanyDetails = useCallback(async (symbol: string) => {
        const response = await fetch(`${API}/companies/${encodeURIComponent(symbol)}`, {
            cache: "no-store",
        });
        if (!response.ok) {
            throw new Error(await parseApiError(response));
        }
        return (await response.json()) as CompanyProfileResponse;
    }, []);

    const fetchCompanies = useCallback(
        async (query?: string) => {
            setCompaniesLoading(true);
            try {
                const params = new URLSearchParams({
                    limit: String(COMPANY_FETCH_LIMIT),
                });
                if (query && query.trim()) {
                    params.set("q", query.trim());
                }
                const response = await fetch(`${API}/companies?${params.toString()}`, {
                    cache: "no-store",
                });
                if (!response.ok) {
                    throw new Error(await parseApiError(response));
                }
                const data = (await response.json()) as CompanyProfileResponse[];
                const sortedData = [...data].sort((a, b) => {
                    const symbolA = (a?.symbol ?? a?.raw_symbol ?? "").trim().toUpperCase();
                    const symbolB = (b?.symbol ?? b?.raw_symbol ?? "").trim().toUpperCase();
                    return symbolA.localeCompare(symbolB);
                });
                setCompanies(sortedData);
                setCompaniesError(null);
                const current = selectedSymbolRef.current;
                let nextSymbol: string | null = null;
                if (
                    current &&
                    sortedData.some(
                        (item) =>
                            item.symbol === current || item.raw_symbol === current
                    )
                ) {
                    nextSymbol = current;
                } else if (sortedData.length > 0) {
                    nextSymbol = sortedData[0].symbol ?? sortedData[0].raw_symbol ?? null;
                }

                const normalizedNextSymbol =
                    nextSymbol && nextSymbol.trim().length > 0
                        ? nextSymbol.trim().toUpperCase()
                        : null;

                if (normalizedNextSymbol !== current) {
                    setSymbol(normalizedNextSymbol);
                    selectedSymbolRef.current = normalizedNextSymbol;
                } else {
                    selectedSymbolRef.current = current;
                }

                const lookupSymbol = selectedSymbolRef.current;
                if (lookupSymbol) {
                    const local = sortedData.find(
                        (item) =>
                            item.symbol === lookupSymbol ||
                            item.raw_symbol === lookupSymbol
                    );
                    setSelectedCompany(local ?? null);
                } else {
                    setSelectedCompany(null);
                }
            } catch (error) {
                setCompaniesError(
                    resolveErrorMessage(error, "Nie udało się pobrać listy spółek")
                );
            } finally {
                setCompaniesLoading(false);
            }
        },
        [setSymbol]
    );

    const fetchBenchmarkSymbols = useCallback(async () => {
        setBenchmarkSymbolsLoading(true);
        setBenchmarkSymbolsError(null);
        try {
            const response = await fetch(`${ADMIN_API}/indices/benchmark/symbols`, {
                cache: "no-store",
            });
            if (!response.ok) {
                throw new Error(await parseApiError(response));
            }
            const payload = (await response.json()) as BenchmarkSymbolListResponse;
            setBenchmarkSymbols(payload.items ?? []);
        } catch (error) {
            setBenchmarkSymbolsError(
                resolveErrorMessage(error, "Nie udało się pobrać symboli GPW Benchmark")
            );
        } finally {
            setBenchmarkSymbolsLoading(false);
        }
    }, []);

    const runCompanySync = useCallback(
        async (
            baseUrl: string,
            setLoading: React.Dispatch<React.SetStateAction<boolean>>,
            fallbackMessage: string
        ) => {
            setLoading(true);
            setStatusError(null);
            try {
                const response = await fetch(`${baseUrl}/companies/sync/background`, {
                    method: "POST",
                });
                if (!response.ok) {
                    throw new Error(await parseApiError(response));
                }
                const payload = (await response.json()) as CompanySyncStatusPayload;
                setStatus(payload);
            } catch (error) {
                setStatusError(resolveErrorMessage(error, fallbackMessage));
            } finally {
                setLoading(false);
            }
        },
        [setStatus, setStatusError]
    );

    const startSync = useCallback(async () => {
        await runCompanySync(
            ADMIN_API,
            setIsStarting,
            "Nie udało się uruchomić synchronizacji"
        );
    }, [runCompanySync]);

    const startLocalSync = useCallback(async () => {
        const result = await ensureLocalClickhouseConfig();
        if (!result.ok) {
            setStatusError(result.error);
            return;
        }
        await runCompanySync(
            LOCAL_ADMIN_API,
            setIsStartingLocal,
            "Nie udało się uruchomić lokalnej synchronizacji. Upewnij się, że backend działa na http://localhost:8000."
        );
    }, [ensureLocalClickhouseConfig, runCompanySync]);

    const handleSearchSubmit = useCallback(
        (event: React.FormEvent<HTMLFormElement>) => {
            event.preventDefault();
            const normalized = search.trim();
            const query = normalized.length ? normalized : undefined;
            setActiveQuery(query);
            fetchCompanies(query);
        },
        [search, fetchCompanies]
    );

    const handleResetSearch = useCallback(() => {
        setSearch("");
        setActiveQuery(undefined);
        fetchCompanies();
    }, [fetchCompanies]);

    const fetchUniverseCandidates = useCallback(async () => {
        const tokens = universeFiltersInput
            .split(/[\s,;]+/)
            .map((token) => token.trim())
            .filter((token) => token.length > 0);

        if (!tokens.length) {
            setUniverseError("Podaj przynajmniej jeden filtr uniwersum.");
            setUniverseCandidates([]);
            setUniverseTotal(null);
            setUniverseFetched(false);
            return;
        }

        setUniverseLoading(true);
        setUniverseError(null);

        try {
            const params = new URLSearchParams();
            for (const token of tokens) {
                params.append("universe", token);
            }
            params.append("with_company_info", universeIncludeMetadata ? "true" : "false");
            params.append("include_index_history", "true");

            const response = await fetch(
                `${ADMIN_API}/universe/candidates?${params.toString()}`,
                { cache: "no-store" }
            );
            if (!response.ok) {
                throw new Error(await parseApiError(response));
            }

            const data = (await response.json()) as UniverseCandidateListResponse;
            setUniverseCandidates(data.items);
            setUniverseTotal(data.total);
            setUniverseFetched(true);
        } catch (error) {
            setUniverseError(
                resolveErrorMessage(
                    error,
                    "Nie udało się pobrać listy symboli uniwersum"
                )
            );
            setUniverseCandidates([]);
            setUniverseTotal(null);
            setUniverseFetched(false);
        } finally {
            setUniverseLoading(false);
        }
    }, [
        universeFiltersInput,
        universeIncludeMetadata,
    ]);

    const handleUniverseSubmit = useCallback(
        (event: React.FormEvent<HTMLFormElement>) => {
            event.preventDefault();
            void fetchUniverseCandidates();
        },
        [fetchUniverseCandidates]
    );

    const handleSelectCompany = useCallback(
        (candidate: string) => {
            const normalized = candidate.trim().toUpperCase();
            if (!normalized) {
                setSymbol(null);
                selectedSymbolRef.current = null;
                setSelectedCompany(null);
                return;
            }

            setSymbol(normalized);
            selectedSymbolRef.current = normalized;

            const local = companies.find(
                (item) =>
                    item.symbol === normalized ||
                    item.raw_symbol === normalized ||
                    item.symbol === candidate ||
                    item.raw_symbol === candidate
            );

            setSelectedCompany(local ?? null);
        },
        [companies, setSymbol]
    );

    const handleBenchmarkInputChange = useCallback(
        (key: string, value: string) => {
            setBenchmarkInputs((prev) => ({ ...prev, [key]: value }));
        },
        []
    );

    const handleBenchmarkSave = useCallback(
        async (company: CompanyProfileResponse) => {
            const baseSymbol = company.raw_symbol ?? company.symbol ?? "";
            const key = baseSymbol.trim().toUpperCase();
            if (!key) {
                return;
            }
            const currentValue = (benchmarkInputs[key] ?? "").trim();
            const payload = {
                symbol: baseSymbol,
                benchmark_symbol: currentValue.length > 0 ? currentValue : null,
            };
            setBenchmarkStatuses((prev) => ({
                ...prev,
                [key]: { status: "saving", message: null },
            }));
            try {
                const response = await fetch(`${ADMIN_API}/companies/benchmark-symbol`, {
                    method: "POST",
                    cache: "no-store",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                });
                let responseBody: unknown = null;
                try {
                    responseBody = await response.json();
                } catch {
                    responseBody = null;
                }
                if (!response.ok) {
                    const message =
                        responseBody &&
                        typeof responseBody === "object" &&
                        "detail" in responseBody &&
                        typeof (responseBody as { detail?: unknown }).detail === "string"
                            ? ((responseBody as { detail: string }).detail ||
                              "Nie udało się zapisać symbolu")
                            : responseBody &&
                              typeof responseBody === "object" &&
                              "error" in responseBody &&
                              typeof (responseBody as { error?: unknown }).error === "string"
                            ? ((responseBody as { error: string }).error ||
                              "Nie udało się zapisać symbolu")
                            : "Nie udało się zapisać symbolu";
                    throw new Error(message);
                }
                const updated = (responseBody ?? {}) as CompanyProfileResponse;
                const savedValue = updated.symbol_gpw_benchmark ?? "";
                setBenchmarkInputs((prev) => ({ ...prev, [key]: savedValue }));
                setBenchmarkOriginals((prev) => {
                    const next = { ...prev, [key]: savedValue };
                    benchmarkOriginalsRef.current = next;
                    return next;
                });
                setBenchmarkStatuses((prev) => ({
                    ...prev,
                    [key]: { status: "success", message: "Zapisano" },
                }));
                setCompanies((prev) =>
                    prev.map((item) => {
                        const itemKey = (item.raw_symbol ?? item.symbol ?? "").trim().toUpperCase();
                        if (itemKey === key) {
                            return { ...item, symbol_gpw_benchmark: savedValue };
                        }
                        return item;
                    })
                );
                setSelectedCompany((prev) => {
                    if (!prev) {
                        return prev;
                    }
                    const prevKey = (prev.raw_symbol ?? prev.symbol ?? "").trim().toUpperCase();
                    if (prevKey === key) {
                        return { ...prev, symbol_gpw_benchmark: savedValue };
                    }
                    return prev;
                });
            } catch (error) {
                const message = resolveErrorMessage(error, "Nie udało się zapisać symbolu");
                setBenchmarkStatuses((prev) => ({
                    ...prev,
                    [key]: { status: "error", message },
                }));
            }
        },
        [benchmarkInputs]
    );

    const runOhlcSync = useCallback(
        async (baseUrl: string, fallbackMessage: string) => {
            const defaultMessage =
                fallbackMessage || "Nie udało się uruchomić synchronizacji notowań";
            setOhlcIsSyncing(true);
            setOhlcError(null);
            setOhlcResult(null);
            setOhlcRequestLog([]);
            let pollingCancelled = false;
            try {
                const parsedSymbols = ohlcSymbolsInput
                    .split(/[\s,;]+/)
                    .map((symbol) => symbol.trim())
                    .filter((symbol) => symbol.length > 0)
                    .map((symbol) => symbol.toUpperCase());
                const uniqueSymbols = Array.from(new Set(parsedSymbols));
                const payload: Record<string, unknown> = {
                    run_as_admin: ohlcRunAsAdmin,
                    truncate: ohlcTruncate,
                };
                const startedAtIso = new Date().toISOString();
                setOhlcProgress({
                    status: "running",
                    total_symbols: uniqueSymbols.length,
                    processed_symbols: 0,
                    inserted_rows: 0,
                    skipped_symbols: 0,
                    current_symbol: null,
                    started_at: startedAtIso,
                    finished_at: null,
                    message: "Inicjowanie synchronizacji...",
                    errors: [],
                    requested_as_admin: ohlcRunAsAdmin,
                });
                const pollProgress = async () => {
                    while (!pollingCancelled) {
                        const snapshot = await fetchOhlcProgress();
                        if (
                            snapshot &&
                            (snapshot.status === "success" || snapshot.status === "error")
                        ) {
                            break;
                        }
                        if (pollingCancelled) {
                            break;
                        }
                        await delay(1500);
                    }
                };
                void pollProgress();
                if (uniqueSymbols.length > 0) {
                    payload.symbols = uniqueSymbols;
                }
                const trimmedStart = ohlcStartInput.trim();
                if (trimmedStart) {
                    payload.start = trimmedStart;
                }
                const response = await fetch(`${baseUrl}/ohlc/sync/background`, {
                    method: "POST",
                    cache: "no-store",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                });
                let data: unknown = null;
                try {
                    data = await response.json();
                } catch {
                    data = null;
                }
                if (!response.ok) {
                    const message =
                        data &&
                        typeof data === "object" &&
                        "error" in data &&
                        data.error &&
                        typeof (data as { error: unknown }).error === "string"
                            ? ((data as { error: string }).error || defaultMessage)
                            : defaultMessage;
                    throw new Error(message);
                }
            } catch (error) {
                const message = resolveErrorMessage(error, defaultMessage);
                setOhlcError(message);
                setOhlcProgress((prev) =>
                    prev
                        ? {
                              ...prev,
                              status: "error",
                              message,
                              finished_at: new Date().toISOString(),
                          }
                        : prev
                );
            } finally {
                pollingCancelled = true;
                await fetchOhlcProgress();
                setOhlcIsSyncing(false);
            }
        },
        [
            ohlcSymbolsInput,
            ohlcRunAsAdmin,
            ohlcTruncate,
            ohlcStartInput,
            fetchOhlcProgress,
        ]
    );

    const handleOhlcSync = useCallback(
        (event: React.FormEvent<HTMLFormElement>) => {
            event.preventDefault();
            void runOhlcSync(
                ADMIN_API,
                "Nie udało się uruchomić synchronizacji notowań"
            );
        },
        [runOhlcSync]
    );

    const handleOhlcSyncLocal = useCallback(async () => {
        const result = await ensureLocalClickhouseConfig();
        if (!result.ok) {
            setOhlcError(result.error);
            return;
        }
        await runOhlcSync(
            LOCAL_ADMIN_API,
            "Nie udało się uruchomić lokalnej synchronizacji. Upewnij się, że backend działa na http://localhost:8000."
        );
    }, [ensureLocalClickhouseConfig, runOhlcSync]);

    const runOhlcImport = useCallback(
        async (
            baseUrl: string,
            file: File,
            fallbackMessage: string
        ): Promise<OhlcImportResponsePayload> => {
            const defaultMessage =
                fallbackMessage || "Nie udało się zaimportować pliku z notowaniami";
            const formData = new FormData();
            formData.append("file", file);
            const response = await fetch(`${baseUrl}/ohlc/import`, {
                method: "POST",
                body: formData,
            });
            let data: unknown = null;
            try {
                data = await response.json();
            } catch {
                data = null;
            }
            if (!response.ok) {
                const message =
                    data &&
                    typeof data === "object" &&
                    data !== null &&
                    "error" in data &&
                    typeof (data as { error?: unknown }).error === "string"
                        ? ((data as { error?: string }).error || defaultMessage)
                        : defaultMessage;
                throw new Error(message);
            }
            if (!data || typeof data !== "object") {
                throw new Error(defaultMessage);
            }
            const payload = data as Partial<OhlcImportResponsePayload>;
            return {
                inserted:
                    typeof payload.inserted === "number" && Number.isFinite(payload.inserted)
                        ? payload.inserted
                        : 0,
                skipped:
                    typeof payload.skipped === "number" && Number.isFinite(payload.skipped)
                        ? payload.skipped
                        : 0,
                errors: Array.isArray(payload.errors)
                    ? payload.errors.filter(
                          (value): value is string => typeof value === "string"
                      )
                    : [],
            };
        },
        []
    );

    const formatOhlcImportSummary = useCallback(
        (result: OhlcImportResponsePayload): string => {
            const parts = [
                `Zapisano ${integerFormatter.format(result.inserted)} rekordów.`,
            ];
            if (result.skipped > 0) {
                parts.push(
                    `Pominięto ${integerFormatter.format(result.skipped)} rekordów.`
                );
            }
            if (result.errors.length > 0) {
                const preview = result.errors.slice(0, 3).join(" • ");
                const suffix = result.errors.length > 3 ? " …" : "";
                parts.push(`Błędy: ${preview}${suffix}`);
            }
            return parts.join(" ");
        },
        [integerFormatter]
    );

    const handleOhlcImport = useCallback(
        async (event: React.FormEvent<HTMLFormElement>) => {
            event.preventDefault();
            setOhlcImportError(null);
            setOhlcImportSuccess(null);
            const input = ohlcImportInputRef.current;
            const file = input?.files?.[0];
            if (!file) {
                setOhlcImportError("Wybierz plik z danymi notowań (CSV lub ZIP).");
                return;
            }
            setOhlcImporting(true);
            try {
                const result = await runOhlcImport(
                    ADMIN_API,
                    file,
                    "Nie udało się zaimportować pliku z notowaniami"
                );
                setOhlcImportSuccess(formatOhlcImportSummary(result));
                if (input) {
                    input.value = "";
                }
            } catch (error) {
                setOhlcImportError(
                    resolveErrorMessage(
                        error,
                        "Nie udało się zaimportować pliku z notowaniami"
                    )
                );
            } finally {
                setOhlcImporting(false);
            }
        },
        [formatOhlcImportSummary, runOhlcImport]
    );

    const handleOhlcImportLocal = useCallback(async () => {
        const input = ohlcImportInputRef.current;
        const file = input?.files?.[0];
        setOhlcImportError(null);
        setOhlcImportSuccess(null);
        if (!file) {
            setOhlcImportError("Wybierz plik z danymi notowań (CSV lub ZIP).");
            return;
        }
        setOhlcImporting(true);
        const ensured = await ensureLocalClickhouseConfig();
        if (!ensured.ok) {
            setOhlcImporting(false);
            setOhlcImportError(ensured.error);
            return;
        }
        try {
            const result = await runOhlcImport(
                LOCAL_ADMIN_API,
                file,
                "Nie udało się zaimportować pliku na lokalny backend. Upewnij się, że działa na http://localhost:8000."
            );
            setOhlcImportSuccess(formatOhlcImportSummary(result));
            if (input) {
                input.value = "";
            }
        } catch (error) {
            setOhlcImportError(
                resolveErrorMessage(
                    error,
                    "Nie udało się zaimportować pliku na lokalny backend. Upewnij się, że działa na http://localhost:8000."
                )
            );
        } finally {
            setOhlcImporting(false);
        }
    }, [
        ensureLocalClickhouseConfig,
        formatOhlcImportSummary,
        runOhlcImport,
    ]);

    const handleOhlcImportReset = useCallback(() => {
        const input = ohlcImportInputRef.current;
        if (input) {
            input.value = "";
        }
        setOhlcImportError(null);
        setOhlcImportSuccess(null);
    }, []);

    const handleScheduleOnce = useCallback(
        async (event: React.FormEvent<HTMLFormElement>) => {
            event.preventDefault();
            setIsScheduling(true);
            setScheduleError(null);
            setScheduleSuccess(null);
            try {
                const iso = toIsoDateTime(
                    onceDateInput,
                    "Wybierz poprawną datę i godzinę synchronizacji."
                );
                const response = await fetch(`${ADMIN_API}/companies/sync/schedule`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ mode: "once", scheduled_for: iso }),
                });
                if (!response.ok) {
                    throw new Error(await parseApiError(response));
                }
                const payload = (await response.json()) as CompanySyncScheduleStatusPayload;
                setSchedule(payload);
                setScheduleSuccess("Zaplanowano synchronizację jednorazową.");
            } catch (error) {
                setScheduleError(
                    error instanceof Error
                        ? error.message
                        : "Nie udało się zaplanować synchronizacji."
                );
            } finally {
                setIsScheduling(false);
            }
        },
        [onceDateInput, toIsoDateTime]
    );

    const handleScheduleRecurring = useCallback(
        async (event: React.FormEvent<HTMLFormElement>) => {
            event.preventDefault();
            setIsScheduling(true);
            setScheduleError(null);
            setScheduleSuccess(null);
            try {
                const interval = Number(recurringIntervalInput);
                if (!Number.isFinite(interval) || interval <= 0) {
                    throw new Error("Interwał musi być dodatnią liczbą minut.");
                }
                const startIso = toOptionalIsoDateTime(
                    recurringStartInput,
                    "Podaj poprawną datę rozpoczęcia harmonogramu."
                );
                const payload: Record<string, unknown> = {
                    mode: "recurring",
                    interval_minutes: Math.round(interval),
                };
                if (startIso) {
                    payload.start_at = startIso;
                }
                const response = await fetch(`${ADMIN_API}/companies/sync/schedule`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                });
                if (!response.ok) {
                    throw new Error(await parseApiError(response));
                }
                const result = (await response.json()) as CompanySyncScheduleStatusPayload;
                setSchedule(result);
                setScheduleSuccess("Zaktualizowano harmonogram cykliczny.");
            } catch (error) {
                setScheduleError(
                    error instanceof Error
                        ? error.message
                        : "Nie udało się zapisać harmonogramu."
                );
            } finally {
                setIsScheduling(false);
            }
        },
        [recurringIntervalInput, recurringStartInput, toOptionalIsoDateTime]
    );

    const handleCancelSchedule = useCallback(async () => {
        setIsScheduling(true);
        setScheduleError(null);
        setScheduleSuccess(null);
        try {
            const response = await fetch(`${ADMIN_API}/companies/sync/schedule`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ mode: "cancel" }),
            });
            if (!response.ok) {
                throw new Error(await parseApiError(response));
            }
            const payload = (await response.json()) as CompanySyncScheduleStatusPayload;
            setSchedule(payload);
            setScheduleSuccess("Usunięto harmonogram synchronizacji.");
        } catch (error) {
            setScheduleError(
                error instanceof Error
                    ? error.message
                    : "Nie udało się usunąć harmonogramu."
            );
        } finally {
            setIsScheduling(false);
        }
    }, []);

    const handleOhlcScheduleSubmit = useCallback(
        async (event: React.FormEvent<HTMLFormElement>) => {
            event.preventDefault();
            setOhlcIsScheduling(true);
            setOhlcScheduleError(null);
            setOhlcScheduleSuccess(null);
            try {
                const optionsPayload: Record<string, unknown> = {
                    run_as_admin: ohlcScheduleRunAsAdmin,
                    truncate: ohlcScheduleTruncate,
                };
                const symbols = ohlcScheduleSymbolsInput
                    .split(/[\s,;]+/)
                    .map((symbol) => symbol.trim())
                    .filter(Boolean);
                if (symbols.length > 0) {
                    optionsPayload.symbols = symbols;
                }
                const startIso = toOptionalIsoDate(
                    ohlcScheduleStartInput,
                    "Podaj poprawną datę początkową (YYYY-MM-DD)."
                );
                if (startIso) {
                    optionsPayload.start = startIso;
                }

                const payload: Record<string, unknown> = {
                    mode: ohlcScheduleMode,
                    options: optionsPayload,
                };

                if (ohlcScheduleMode === "once") {
                    payload.scheduled_for = toIsoDateTime(
                        ohlcOnceDateInput,
                        "Podaj datę i godzinę uruchomienia synchronizacji."
                    );
                } else {
                    const intervalNumeric = Number(ohlcRecurringIntervalInput);
                    if (!Number.isFinite(intervalNumeric) || intervalNumeric <= 0) {
                        throw new Error("Interwał musi być dodatnią liczbą minut.");
                    }
                    payload.interval_minutes = Math.round(intervalNumeric);
                    const startAtIso = toOptionalIsoDateTime(
                        ohlcRecurringStartInput,
                        "Podaj poprawną datę rozpoczęcia harmonogramu."
                    );
                    if (startAtIso) {
                        payload.start_at = startAtIso;
                    }
                }

                const response = await fetch(`${ADMIN_API}/ohlc/sync/schedule`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                });
                if (!response.ok) {
                    throw new Error(await parseApiError(response));
                }
                const result = (await response.json()) as OhlcSyncScheduleStatusPayload;
                setOhlcSchedule(result);
                setOhlcScheduleSuccess(
                    ohlcScheduleMode === "once"
                        ? "Zapisano harmonogram jednorazowy."
                        : "Zapisano harmonogram cykliczny."
                );
            } catch (error) {
                setOhlcScheduleError(
                    error instanceof Error
                        ? error.message
                        : "Nie udało się zapisać harmonogramu notowań."
                );
            } finally {
                setOhlcIsScheduling(false);
            }
        },
        [
            ohlcScheduleMode,
            ohlcOnceDateInput,
            ohlcRecurringIntervalInput,
            ohlcRecurringStartInput,
            ohlcScheduleRunAsAdmin,
            ohlcScheduleTruncate,
            ohlcScheduleSymbolsInput,
            ohlcScheduleStartInput,
            toIsoDateTime,
            toOptionalIsoDateTime,
            toOptionalIsoDate,
        ]
    );

    const handleOhlcScheduleCancel = useCallback(async () => {
        setOhlcIsScheduling(true);
        setOhlcScheduleError(null);
        setOhlcScheduleSuccess(null);
        try {
            const response = await fetch(`${ADMIN_API}/ohlc/sync/schedule`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ mode: "cancel" }),
            });
            if (!response.ok) {
                throw new Error(await parseApiError(response));
            }
            const payload = (await response.json()) as OhlcSyncScheduleStatusPayload;
            setOhlcSchedule(payload);
            setOhlcScheduleSuccess("Usunięto harmonogram notowań.");
        } catch (error) {
            setOhlcScheduleError(
                error instanceof Error
                    ? error.message
                    : "Nie udało się usunąć harmonogramu notowań."
            );
        } finally {
            setOhlcIsScheduling(false);
        }
    }, []);

    const handleAddAdmin = useCallback(
        async (event: React.FormEvent<HTMLFormElement>) => {
            event.preventDefault();
            setIsAddingAdmin(true);
            setAdminsError(null);
            setAdminsSuccess(null);
            try {
                const trimmed = newAdminEmail.trim();
                if (!trimmed) {
                    throw new Error("Podaj adres e-mail administratora.");
                }
                const response = await fetch(`/api/admins`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ email: trimmed }),
                });
                if (!response.ok) {
                    throw new Error(await parseApiError(response));
                }
                const payload = (await response.json()) as { admins: AdminEntry[] };
                setAdmins(payload.admins);
                setAdminsSuccess("Dodano administratora.");
                setNewAdminEmail("");
            } catch (error) {
                setAdminsError(
                    error instanceof Error
                        ? error.message
                        : "Nie udało się dodać administratora."
                );
            } finally {
                setIsAddingAdmin(false);
            }
        },
        [newAdminEmail]
    );

    const refreshSchedule = useCallback(() => {
        setScheduleSuccess(null);
        void fetchSchedule();
    }, [fetchSchedule]);

    const refreshOhlcSchedule = useCallback(() => {
        setOhlcScheduleSuccess(null);
        void fetchOhlcSchedule();
    }, [fetchOhlcSchedule]);

    const refreshAdmins = useCallback(() => {
        setAdminsSuccess(null);
        void fetchAdmins();
    }, [fetchAdmins]);


    useEffect(() => {
        void fetchOhlcProgress();
    }, [fetchOhlcProgress]);

    useEffect(() => {
        fetchStatus();
        fetchCompanies();
        fetchSchedule();
        fetchAdmins();
        fetchOhlcSchedule();
        fetchBenchmarkSymbols();
        return () => stopPolling();
    }, [
        fetchStatus,
        fetchCompanies,
        fetchSchedule,
        fetchAdmins,
        fetchOhlcSchedule,
        fetchBenchmarkSymbols,
        stopPolling,
    ]);

    useEffect(() => {
        const nextOriginals: Record<string, string> = {};
        const seenKeys: string[] = [];
        companies.forEach((company) => {
            const rawKey = company.raw_symbol ?? company.symbol ?? "";
            const key = rawKey.trim().toUpperCase();
            if (!key) {
                return;
            }
            seenKeys.push(key);
            nextOriginals[key] = company.symbol_gpw_benchmark ?? "";
        });
        const previousOriginals = benchmarkOriginalsRef.current;

        setBenchmarkInputs((prevInputs) => {
            const nextInputs: Record<string, string> = {};
            seenKeys.forEach((key) => {
                const savedValue = nextOriginals[key] ?? "";
                const previousInput = prevInputs[key];
                const previousOriginal = previousOriginals[key];
                if (
                    previousInput !== undefined &&
                    previousOriginal !== undefined &&
                    previousInput.trim() !== previousOriginal.trim()
                ) {
                    nextInputs[key] = previousInput;
                } else {
                    nextInputs[key] = savedValue;
                }
            });
            return nextInputs;
        });

        setBenchmarkStatuses((prevStatuses) => {
            const nextStatuses: typeof prevStatuses = {};
            seenKeys.forEach((key) => {
                nextStatuses[key] = prevStatuses[key] ?? {
                    status: "idle",
                    message: null,
                };
            });
            return nextStatuses;
        });

        setBenchmarkOriginals((prev) => {
            const prevKeys = Object.keys(prev);
            if (
                prevKeys.length === seenKeys.length &&
                prevKeys.every((key) => prev[key] === nextOriginals[key])
            ) {
                return prev;
            }
            return nextOriginals;
        });

        benchmarkOriginalsRef.current = nextOriginals;
    }, [companies]);

    useEffect(() => {
        if (status?.status === "running") {
            if (!pollingRef.current) {
                pollingRef.current = setInterval(() => {
                    fetchStatus();
                }, COMPANY_POLL_INTERVAL);
            }
        } else {
            stopPolling();
            if (status?.status === "completed") {
                fetchCompanies(activeQuery);
                fetchSchedule();
                fetchOhlcSchedule();
            }
        }
    }, [
        status?.status,
        fetchStatus,
        stopPolling,
        fetchCompanies,
        activeQuery,
        fetchSchedule,
        fetchOhlcSchedule,
    ]);

    useEffect(() => () => stopPolling(), [stopPolling]);

    useEffect(() => {
        if (!schedule) {
            setOnceDateInput("");
            setRecurringStartInput("");
            return;
        }
        if (schedule.mode === "once") {
            setOnceDateInput(toLocalDateTimeInputValue(schedule.next_run_at));
        }
        if (schedule.mode === "recurring") {
            if (schedule.recurring_interval_minutes) {
                setRecurringIntervalInput(String(schedule.recurring_interval_minutes));
            }
            const nextInput = schedule.next_run_at || schedule.recurring_start_at;
            setRecurringStartInput(toLocalDateTimeInputValue(nextInput));
        }
        if (schedule.mode === "idle") {
            setOnceDateInput("");
            setRecurringStartInput("");
        }
    }, [schedule, toLocalDateTimeInputValue]);

    useEffect(() => {
        if (!ohlcSchedule) {
            setOhlcScheduleMode("once");
            setOhlcOnceDateInput("");
            setOhlcRecurringIntervalInput("1440");
            setOhlcRecurringStartInput("");
            setOhlcScheduleSymbolsInput("");
            setOhlcScheduleStartInput("");
            setOhlcScheduleTruncate(false);
            setOhlcScheduleRunAsAdmin(true);
            return;
        }
        setOhlcScheduleMode(
            ohlcSchedule.mode === "recurring"
                ? "recurring"
                : ohlcSchedule.mode === "once"
                ? "once"
                : "once"
        );
        if (ohlcSchedule.mode === "once") {
            setOhlcOnceDateInput(toLocalDateTimeInputValue(ohlcSchedule.next_run_at));
        }
        if (ohlcSchedule.mode === "recurring") {
            if (ohlcSchedule.recurring_interval_minutes) {
                setOhlcRecurringIntervalInput(String(ohlcSchedule.recurring_interval_minutes));
            }
            const nextInput = ohlcSchedule.next_run_at || ohlcSchedule.recurring_start_at;
            setOhlcRecurringStartInput(toLocalDateTimeInputValue(nextInput));
        }
        if (ohlcSchedule.mode === "idle") {
            setOhlcOnceDateInput("");
            setOhlcRecurringStartInput("");
            setOhlcRecurringIntervalInput("1440");
        }
        const options = ohlcSchedule.options ?? null;
        if (options?.symbols && options.symbols.length > 0) {
            setOhlcScheduleSymbolsInput(options.symbols.join(", "));
        } else {
            setOhlcScheduleSymbolsInput("");
        }
        if (options?.start) {
            setOhlcScheduleStartInput(options.start);
        } else {
            setOhlcScheduleStartInput("");
        }
        setOhlcScheduleTruncate(Boolean(options?.truncate));
        setOhlcScheduleRunAsAdmin(options?.run_as_admin ?? true);
    }, [ohlcSchedule, toLocalDateTimeInputValue]);

    useEffect(() => {
        if (!symbol) {
            setSelectedCompany(null);
            setDetailsLoading(false);
            return;
        }
        let cancelled = false;
        setDetailsLoading(true);
        setDetailsError(null);
        fetchCompanyDetails(symbol)
            .then((data) => {
                if (!cancelled) {
                    setSelectedCompany(data);
                }
            })
            .catch((error) => {
                if (!cancelled) {
                    setDetailsError(
                        error instanceof Error && error.message
                            ? error.message
                            : "Nie udało się pobrać szczegółów spółki"
                    );
                }
            })
            .finally(() => {
                if (!cancelled) {
                    setDetailsLoading(false);
                }
            });
        return () => {
            cancelled = true;
        };
    }, [symbol, fetchCompanyDetails, status?.status]);

    const fundamentalEntries = useMemo(() => {
        if (!selectedCompany) return [] as [string, number | null][];
        const entries = Object.entries(selectedCompany.fundamentals || {});
        return entries
            .filter(([key]) => Boolean(key))
            .sort((a, b) => {
                const idxA = FUNDAMENTAL_ORDER.indexOf(a[0]);
                const idxB = FUNDAMENTAL_ORDER.indexOf(b[0]);
                if (idxA === -1 && idxB === -1) {
                    return a[0].localeCompare(b[0]);
                }
                if (idxA === -1) return 1;
                if (idxB === -1) return -1;
                return idxA - idxB;
            });
    }, [selectedCompany]);

    const total = status?.total ?? status?.result?.fetched ?? 0;
    const processed = status?.processed ?? status?.result?.fetched ?? 0;
    const synced = status?.synced ?? status?.result?.synced ?? 0;
    const failed = status?.failed ?? status?.result?.failed ?? 0;
    const progressPercent = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : status?.status === "completed" ? 100 : 0;
    const statusLabel = status ? COMPANY_STATUS_LABELS[status.status] : "Brak danych";
    const stageLabel = status ? COMPANY_STAGE_LABELS[status.stage] : "";
    const visibleErrors = (status?.errors ?? []).filter((message) => Boolean(message));
    const descriptionSnippet = selectedCompany?.description
        ? selectedCompany.description.length > 600
            ? `${selectedCompany.description.slice(0, 600)}…`
            : selectedCompany.description
        : null;
    const marketCapValue = selectedCompany?.fundamentals?.market_cap ?? null;
    const companyEmployees =
        selectedCompany?.employees !== undefined && selectedCompany?.employees !== null
            ? integerFormatter.format(selectedCompany.employees)
            : "—";
    const symbolMappings = useMemo(
        () =>
            selectedCompany
                ? [
                      {
                          label: "GPW",
                          value: selectedCompany.symbol_gpw ?? selectedCompany.raw_symbol,
                      },
                      {
                          label: "GPW Benchmark",
                          value: selectedCompany.symbol_gpw_benchmark ?? null,
                      },
                      { label: "Stooq", value: selectedCompany.symbol_stooq ?? null },
                      { label: "Yahoo Finance", value: selectedCompany.symbol_yahoo ?? null },
                      { label: "Google Finance", value: selectedCompany.symbol_google ?? null },
                  ]
                : [],
        [selectedCompany]
    );
    const rawInsights = useMemo<CompanyRawInsights>(() => {
        if (!selectedCompany) {
            return { shareholding: [], companySize: null, facts: [] };
        }

        const extra = (selectedCompany.extra ?? {}) as Record<string, unknown>;
        const shareholdingExtraRaw = extra["stooq_shareholding"];
        const shareholdingFromExtra = Array.isArray(shareholdingExtraRaw)
            ? Array.from(
                  new Set(
                      shareholdingExtraRaw
                          .map((entry) =>
                              typeof entry === "string"
                                  ? entry.replace(/\s+/g, " ").trim()
                                  : ""
                          )
                          .filter((entry) => Boolean(entry))
                  )
              ).slice(0, 20)
            : [];

        const companySizeExtraRaw = extra["stooq_company_size"];
        const companySizeFromExtra =
            typeof companySizeExtraRaw === "string"
                ? companySizeExtraRaw.replace(/\s+/g, " ").trim() || null
                : null;

        const factsExtraRaw = extra["stooq_facts"];
        const factsFromExtra = Array.isArray(factsExtraRaw)
            ? factsExtraRaw
                  .map((fact) => {
                      if (!fact || typeof fact !== "object") {
                          return null;
                      }
                      const labelRaw = (fact as { label?: unknown }).label;
                      const valueRaw = (fact as { value?: unknown }).value;
                      if (typeof labelRaw !== "string" || typeof valueRaw !== "string") {
                          return null;
                      }
                      const label = labelRaw.replace(/\s+/g, " ").trim();
                      const value = valueRaw.replace(/\s+/g, " ").trim();
                      if (!label || !value) {
                          return null;
                      }
                      return { label, value };
                  })
                  .filter((fact): fact is { label: string; value: string } => Boolean(fact))
            : [];

        if (
            shareholdingFromExtra.length > 0 ||
            companySizeFromExtra ||
            factsFromExtra.length > 0
        ) {
            return {
                shareholding: shareholdingFromExtra,
                companySize: companySizeFromExtra,
                facts: factsFromExtra,
            };
        }

        const rawPayloadCandidate =
            typeof extra["raw_payload"] === "string"
                ? (extra["raw_payload"] as string)
                : typeof selectedCompany.raw?.raw_payload === "string"
                ? (selectedCompany.raw?.raw_payload as string)
                : null;

        if (!rawPayloadCandidate || !rawPayloadCandidate.trim()) {
            return { shareholding: [], companySize: null, facts: [] };
        }

        try {
            const parsed = JSON.parse(rawPayloadCandidate) as JsonValue;
            if (!parsed || typeof parsed !== "object") {
                return { shareholding: [], companySize: null, facts: [] };
            }
            return extractRawInsights(parsed);
        } catch {
            return { shareholding: [], companySize: null, facts: [] };
        }
    }, [selectedCompany]);
    const indexMembership = useMemo(() => {
        if (!selectedCompany) {
            return [] as string[];
        }

        const extra = (selectedCompany.extra ?? {}) as Record<string, unknown>;
        const stooqIndicesCandidate = extra["stooq_indices"];
        if (Array.isArray(stooqIndicesCandidate)) {
            const normalized = stooqIndicesCandidate
                .map((entry) =>
                    typeof entry === "string" ? entry.replace(/\s+/g, " ").trim() : ""
                )
                .filter((entry) => Boolean(entry));
            if (normalized.length > 0) {
                return Array.from(new Set(normalized));
            }
        }

        const candidateKeys = [
            "index",
            "indexes",
            "indices",
            "index_membership",
            "indexMembership",
            "indeks",
        ];

        const collected: string[] = [];
        const addFromSource = (source?: Record<string, unknown>) => {
            if (!source) return;
            for (const key of candidateKeys) {
                const rawValue = source[key];
                if (!rawValue) continue;
                if (typeof rawValue === "string") {
                    collected.push(rawValue);
                    continue;
                }
                if (Array.isArray(rawValue)) {
                    for (const item of rawValue) {
                        if (typeof item === "string") {
                            collected.push(item);
                        }
                    }
                }
            }
        };

        addFromSource(extra);
        addFromSource(selectedCompany.raw);

        const normalized = new Set<string>();
        for (const rawEntry of collected) {
            const sanitized = rawEntry.replace(/<[^>]+>/g, " ");
            const parts = sanitized
                .split(/[\n;,]+/)
                .map((part) =>
                    part
                        .replace(/^[\s•·\-–—\u2022\u2023\u2043\u2219]+/, "")
                        .replace(/\s+/g, " ")
                        .trim()
                )
                .filter((part) => part.length > 0);
            for (const part of parts) {
                normalized.add(part);
            }
        }

        return Array.from(normalized);
    }, [selectedCompany]);
    const scheduleModeLabel = schedule
        ? SCHEDULE_MODE_LABELS[schedule.mode]
        : SCHEDULE_MODE_LABELS.idle;
    const scheduleStatusLabel = schedule
        ? SCHEDULE_STATUS_LABELS[schedule.last_run_status]
        : SCHEDULE_STATUS_LABELS.idle;
    const scheduleNextRunLabel = schedule?.next_run_at
        ? formatDateTime(schedule.next_run_at)
        : "—";
    const scheduleLastStartLabel = schedule?.last_run_started_at
        ? formatDateTime(schedule.last_run_started_at)
        : "—";
    const scheduleLastFinishLabel = schedule?.last_run_finished_at
        ? formatDateTime(schedule.last_run_finished_at)
        : "—";
    const scheduleIntervalLabel = formatIntervalLabel(
        schedule?.recurring_interval_minutes ?? null
    );
    const hasActiveSchedule = schedule?.mode === "once" || schedule?.mode === "recurring";
    const ohlcScheduleModeLabel = ohlcSchedule
        ? OHLC_SCHEDULE_MODE_LABELS[ohlcSchedule.mode]
        : OHLC_SCHEDULE_MODE_LABELS.idle;
    const ohlcScheduleStatusLabel = ohlcSchedule
        ? OHLC_SCHEDULE_STATUS_LABELS[ohlcSchedule.last_run_status]
        : OHLC_SCHEDULE_STATUS_LABELS.idle;
    const ohlcScheduleNextRunLabel = ohlcSchedule?.next_run_at
        ? formatDateTime(ohlcSchedule.next_run_at)
        : "—";
    const ohlcScheduleLastStartLabel = ohlcSchedule?.last_run_started_at
        ? formatDateTime(ohlcSchedule.last_run_started_at)
        : "—";
    const ohlcScheduleLastFinishLabel = ohlcSchedule?.last_run_finished_at
        ? formatDateTime(ohlcSchedule.last_run_finished_at)
        : "—";
    const ohlcScheduleIntervalLabel = formatIntervalLabel(
        ohlcSchedule?.recurring_interval_minutes ?? null
    );
    const ohlcHasActiveSchedule =
        ohlcSchedule?.mode === "once" || ohlcSchedule?.mode === "recurring";
    const localClickhouseStatusDescription = useMemo(() => {
        if (!localClickhouseStatus) {
            return null;
        }
        const parts: string[] = [];
        if (localClickhouseStatus.mode === "url") {
            if (localClickhouseStatus.url) {
                parts.push(localClickhouseStatus.url);
            }
        } else if (localClickhouseStatus.host) {
            const portLabel = localClickhouseStatus.port
                ? `:${localClickhouseStatus.port}`
                : "";
            parts.push(`${localClickhouseStatus.host}${portLabel}`);
        }
        if (localClickhouseStatus.database) {
            parts.push(`DB: ${localClickhouseStatus.database}`);
        }
        if (localClickhouseStatus.username) {
            parts.push(
                localClickhouseStatus.has_password
                    ? `Użytkownik: ${localClickhouseStatus.username} (hasło ustawione)`
                    : `Użytkownik: ${localClickhouseStatus.username}`
            );
        } else if (localClickhouseStatus.has_password) {
            parts.push("Hasło ustawione");
        }
        if (!parts.length && localClickhouseStatus.mode === "manual") {
            parts.push("Konfiguracja ręczna bez szczegółów");
        }
        return parts.join(" • ");
    }, [localClickhouseStatus]);

    return (
        <div className="space-y-16">
            <Section
                id="universe-candidates"
                kicker="Rankingi"
                title="Sprawdź kandydatów uniwersum"
                description="Pobierz listę tickerów, które backend bierze pod uwagę dla przekazanych filtrów wszechświata."
            >
                <div className="grid gap-6 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,1.8fr)]">
                    <Card title="Konfiguracja zapytania">
                        <form onSubmit={handleUniverseSubmit} className="space-y-4">
                            <p className="text-sm text-muted">
                                Wpisz filtry dokładnie w takim formacie, w jakim przesyła je frontend
                                (np. <code className="rounded bg-soft-surface px-1">index:WIG40</code> lub
                                <code className="rounded bg-soft-surface px-1">isin:PLLOTOS00025</code>).
                            </p>
                            <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                <span>Filtry uniwersum</span>
                                <textarea
                                    value={universeFiltersInput}
                                    onChange={(event) => setUniverseFiltersInput(event.target.value)}
                                    placeholder="np. index:WIG40"
                                    className="min-h-[120px] rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                />
                                <span className="text-xs font-normal text-subtle">
                                    Oddziel filtry spacją, przecinkiem lub nową linią. Prefiks
                                    <code className="mx-1 rounded bg-soft-surface px-1">index:</code>
                                    wczytuje skład indeksu z tabel GPW Benchmark.
                                </span>
                            </label>
                            <div className="flex flex-wrap gap-4 text-sm font-medium text-primary">
                                <label className="flex items-center gap-2">
                                    <input
                                        type="checkbox"
                                        checked={universeIncludeMetadata}
                                        onChange={(event) => setUniverseIncludeMetadata(event.target.checked)}
                                    />
                                    <span>Dołącz nazwę, ISIN, sektor i branżę spółek</span>
                                </label>
                            </div>
                            {universeError && (
                                <div className="rounded-xl border border-rose-200/60 bg-rose-50/70 px-3 py-2 text-xs text-rose-600">
                                    {universeError}
                                </div>
                            )}
                            <div className="flex flex-wrap items-center gap-3">
                                <button
                                    type="submit"
                                    disabled={universeLoading}
                                    className="rounded-full bg-primary px-4 py-2 text-sm font-semibold text-white shadow hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-60"
                                >
                                    {universeLoading ? "Pobieranie..." : "Pobierz listę"}
                                </button>
                                <button
                                    type="button"
                                    onClick={() => setUniverseFiltersInput("index:WIG40")}
                                    className="rounded-full border border-soft px-4 py-2 text-sm font-semibold text-primary transition hover:border-primary/40 hover:text-primary"
                                >
                                    Ustaw WIG40
                                </button>
                                <button
                                    type="button"
                                    onClick={() => setUniverseFiltersInput("")}
                                    className="rounded-full border border-soft px-4 py-2 text-sm font-semibold text-primary transition hover:border-primary/40 hover:text-primary"
                                >
                                    Wyczyść
                                </button>
                            </div>
                        </form>
                    </Card>
                    <Card
                        title="Wynik"
                        right={
                            typeof universeTotal === "number" ? (
                                <span className="text-sm text-subtle">
                                    Liczba symboli:{" "}
                                    <span className="font-semibold text-primary">
                                        {integerFormatter.format(universeTotal)}
                                    </span>
                                </span>
                            ) : null
                        }
                    >
                        <div className="space-y-4">
                            {universeLoading && (
                                <div className="rounded-xl border border-soft bg-white/60 px-3 py-2 text-sm text-muted">
                                    Pobieranie listy symboli...
                                </div>
                            )}
                            {!universeLoading && universeFetched && universeCandidates.length === 0 && !universeError && (
                                <div className="rounded-xl border border-soft bg-white/60 px-3 py-2 text-sm text-muted">
                                    Brak symboli spełniających zadane filtry.
                                </div>
                            )}
                            {universeCandidates.length > 0 && (
                                <div className="overflow-auto rounded-xl border border-soft">
                                    <table className="min-w-full divide-y divide-soft text-sm">
                                        <thead className="bg-soft-surface text-xs uppercase tracking-wide text-subtle">
                                            <tr>
                                                <th className="px-3 py-2 text-left font-semibold">#</th>
                                                <th className="px-3 py-2 text-left font-semibold">Symbol</th>
                                                <th className="px-3 py-2 text-left font-semibold">Nazwa</th>
                                                <th className="px-3 py-2 text-left font-semibold">ISIN</th>
                                                <th className="px-3 py-2 text-left font-semibold">Sektor</th>
                                                <th className="px-3 py-2 text-left font-semibold">Branża</th>
                                            </tr>
                                        </thead>
                                        <tbody className="divide-y divide-soft bg-white/70">
                                            {universeCandidates.map((item, index) => (
                                                <tr key={`${item.symbol}-${index}`}>
                                                    <td className="px-3 py-2 text-xs text-subtle">
                                                        {index + 1}
                                                    </td>
                                                    <td className="px-3 py-2 font-semibold text-primary">{item.symbol}</td>
                                                    <td className="px-3 py-2 text-sm text-muted">
                                                        {item.name && item.name.trim() ? item.name : "—"}
                                                    </td>
                                                    <td className="px-3 py-2 text-sm text-muted">
                                                        {item.isin && item.isin.trim() ? item.isin : "—"}
                                                    </td>
                                                    <td className="px-3 py-2 text-sm text-muted">
                                                        {item.sector && item.sector.trim() ? item.sector : "—"}
                                                    </td>
                                                    <td className="px-3 py-2 text-sm text-muted">
                                                        {item.industry && item.industry.trim() ? item.industry : "—"}
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            )}
                        </div>
                    </Card>
                </div>
            </Section>
            <Section
                id="prices-sync"
                kicker="Stooq"
                title="Synchronizacja notowań historycznych"
                description="Pobierz dzienne dane OHLC ze Stooq i zapisz je w bazie ClickHouse."
            >
                <div className="grid gap-6 lg:grid-cols-[minmax(0,1.1fr)_minmax(0,1.9fr)]">
                    <div className="space-y-6">
                        <Card title="Uruchom synchronizację notowań">
                            <form onSubmit={handleOhlcSync} className="space-y-4">
                                <p className="text-sm text-muted">
                                    Skonfiguruj zakres danych i uruchom pobieranie notowań ze Stooq.
                                </p>
                                <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                    <span>Lista symboli (opcjonalnie)</span>
                                    <textarea
                                        value={ohlcSymbolsInput}
                                        onChange={(event) => setOhlcSymbolsInput(event.target.value)}
                                        placeholder="np. CDR.WA, PKO.WA, PKN.WA"
                                        className="min-h-[120px] rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                        disabled={ohlcIsSyncing}
                                    />
                                    <span className="text-xs font-normal text-subtle">
                                        Pozostaw puste, aby zsynchronizować wszystkie dostępne symbole z bazy.
                                    </span>
                                </label>
                                <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                    <span>Data początkowa (opcjonalnie)</span>
                                    <input
                                        type="date"
                                        value={ohlcStartInput}
                                        onChange={(event) => setOhlcStartInput(event.target.value)}
                                        className="rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                        disabled={ohlcIsSyncing}
                                    />
                                    <span className="text-xs font-normal text-subtle">
                                        Zostaw puste, aby pobrać pełną historię dostępnych notowań.
                                    </span>
                                </label>
                                <div className="space-y-3 text-sm text-primary">
                                    <label className="flex items-center gap-3">
                                        <input
                                            type="checkbox"
                                            className="h-4 w-4 rounded border-soft text-primary focus:ring-primary/40"
                                            checked={ohlcTruncate}
                                            onChange={(event) => {
                                                const checked = event.target.checked;
                                                setOhlcTruncate(checked);
                                                if (checked && !ohlcRunAsAdmin) {
                                                    setOhlcRunAsAdmin(true);
                                                }
                                            }}
                                            disabled={ohlcIsSyncing}
                                        />
                                        <span>Wyczyść tabelę przed synchronizacją</span>
                                    </label>
                                    <p className="text-xs text-subtle">
                                        Czyszczenie wymaga uruchomienia w trybie administratora.
                                    </p>
                                    <label className="flex items-center gap-3">
                                        <input
                                            type="checkbox"
                                            className="h-4 w-4 rounded border-soft text-primary focus:ring-primary/40"
                                            checked={ohlcRunAsAdmin}
                                            onChange={(event) => setOhlcRunAsAdmin(event.target.checked)}
                                            disabled={ohlcIsSyncing}
                                        />
                                        <span>Uruchom w trybie administratora</span>
                                    </label>
                                </div>
                                {ohlcError && <div className="text-sm text-negative">{ohlcError}</div>}
                                {ohlcProgress && ohlcProgress.status !== "idle" && (
                                    <div className="space-y-3 rounded-xl border border-soft bg-white/70 p-4 text-xs text-subtle">
                                        <div className="flex items-center justify-between gap-3 text-[13px] font-semibold text-primary">
                                            <span>{ohlcProgressStatusLabel}</span>
                                            <span className="text-[11px] text-muted">{ohlcProgressPercent}%</span>
                                        </div>
                                        <div className="h-2 rounded-full bg-soft">
                                            <div
                                                className={`h-2 rounded-full transition-all ${
                                                    ohlcProgress.status === "error"
                                                        ? "bg-negative"
                                                        : ohlcProgress.status === "success"
                                                        ? "bg-emerald-500"
                                                        : "bg-primary"
                                                }`}
                                                style={{ width: `${ohlcProgressPercent}%` }}
                                            />
                                        </div>
                                        <div className="grid gap-2 text-xs sm:grid-cols-2">
                                            <div>
                                                Symbole:
                                                <span className="ml-1 font-semibold text-primary">
                                                    {integerFormatter.format(ohlcProgress.processed_symbols)} /
                                                    {" "}
                                                    {integerFormatter.format(
                                                        ohlcProgress.total_symbols ||
                                                            Math.max(ohlcProgress.processed_symbols, 0)
                                                    )}
                                                </span>
                                            </div>
                                            <div>
                                                Zapisane wiersze:
                                                <span className="ml-1 font-semibold text-primary">
                                                    {integerFormatter.format(ohlcProgress.inserted_rows)}
                                                </span>
                                            </div>
                                            <div>
                                                Pominięte symbole:
                                                <span className="ml-1 font-semibold text-primary">
                                                    {integerFormatter.format(ohlcProgress.skipped_symbols)}
                                                </span>
                                            </div>
                                            <div>
                                                Aktualny symbol:
                                                <span className="ml-1 font-semibold text-primary">
                                                    {ohlcProgress.current_symbol ?? "—"}
                                                </span>
                                            </div>
                                        </div>
                                        {ohlcProgress.message && (
                                            <div
                                                className={`text-xs ${
                                                    ohlcProgress.status === "error"
                                                        ? "text-negative"
                                                        : "text-primary"
                                                }`}
                                            >
                                                {ohlcProgress.message}
                                            </div>
                                        )}
                                        {ohlcProgress.status === "error" && ohlcProgress.errors.length > 0 && (
                                            <ul className="list-disc space-y-1 pl-4 text-[11px] text-negative">
                                                {ohlcProgress.errors.map((error, index) => (
                                                    <li key={`${error}-${index}`}>{error}</li>
                                                ))}
                                            </ul>
                                        )}
                                    </div>
                                )}
                                <div className="flex flex-wrap gap-3">
                                    <button
                                        type="submit"
                                        disabled={ohlcIsSyncing}
                                        className="rounded-full bg-primary px-4 py-2 text-sm font-semibold text-white shadow hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-60"
                                    >
                                        {ohlcIsSyncing ? "Synchronizowanie..." : "Synchronizuj notowania"}
                                    </button>
                                    <button
                                        type="button"
                                        onClick={() => {
                                            void handleOhlcSyncLocal();
                                        }}
                                        disabled={ohlcIsSyncing}
                                        title="Wymaga uruchomionego backendu pod adresem http://localhost:8000"
                                        className="rounded-full bg-emerald-600 px-4 py-2 text-sm font-semibold text-white shadow hover:bg-emerald-500 disabled:cursor-not-allowed disabled:opacity-60"
                                    >
                                        {ohlcIsSyncing ? "Synchronizowanie..." : "Uruchom lokalnie"}
                                    </button>
                                    <button
                                        type="button"
                                        onClick={() => {
                                            setOhlcSymbolsInput("");
                                            setOhlcStartInput("");
                                            setOhlcTruncate(false);
                                            setOhlcError(null);
                                        }}
                                        disabled={ohlcIsSyncing}
                                        className="rounded-full border border-soft px-4 py-2 text-sm font-semibold text-primary transition hover:border-primary/40 hover:text-primary disabled:cursor-not-allowed disabled:opacity-60"
                                    >
                                        Wyczyść formularz
                                    </button>
                                </div>
                                <p className="text-xs text-subtle">
                                    Aby zsynchronizować dane lokalnie, uruchom backend na adresie
                                    <code className="mx-1 rounded bg-soft px-1 py-0.5 text-[10px]">http://localhost:8000</code>
                                    i użyj przycisku „Uruchom lokalnie”.
                                </p>
                            </form>
                        </Card>
                        <Card title="Agent Windows – aplikacja desktopowa">
                            <div className="space-y-4 text-sm text-muted">
                                <p>
                                    Aplikacja Tkinter działa poza przeglądarką. Uruchom ją poleceniem
                                    <code className="mx-1 rounded bg-soft px-1 py-0.5 text-[10px]">
                                        python backend\windows_agent\app.py
                                    </code>
                                    , a następnie przypnij skrót do pulpitu lub paska zadań.
                                </p>
                                <ol className="list-decimal space-y-2 pl-5">
                                    <li>
                                        W zakładce „Pobieranie danych” wybierz spółki, zakres dat i rodzaje
                                        danych (notowania, profile, wiadomości). Wyniki zapisywane są w
                                        katalogu Dokumenty\GPW Analytics.
                                    </li>
                                    <li>
                                        W zakładce „Połączenie z bazą danych” wpisz parametry ClickHouse.
                                        Hasło trafia do Menedżera poświadczeń systemu Windows dzięki
                                        bibliotece <code>keyring</code>.
                                    </li>
                                    <li>
                                        Po pobraniu danych użyj przycisku „Eksportuj ostatnie dane”, aby
                                        wysłać je do swojej instancji ClickHouse (lokalnej lub w chmurze).
                                    </li>
                                </ol>
                                <p>
                                    Panel pozostaje miejscem do podglądu synchronizacji, a agent desktopowy
                                    przejmuje pracę offline – bez kolejki zadań i dodatkowych endpointów.
                                </p>
                            </div>
                        </Card>
                        <Card title="Import notowań z pliku">
                            <form onSubmit={handleOhlcImport} className="space-y-4">
                                <p className="text-sm text-muted">
                                    Wgraj plik CSV lub archiwum ZIP z plikami <code>.MST</code> i zapisz
                                    dane w ClickHouse Cloud lub działającym lokalnie backendzie.
                                </p>
                                <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                    <span>Plik z notowaniami (CSV lub ZIP)</span>
                                    <input
                                        type="file"
                                        accept=".csv,.zip,text/csv,application/zip"
                                        ref={ohlcImportInputRef}
                                        onChange={() => {
                                            setOhlcImportError(null);
                                            setOhlcImportSuccess(null);
                                        }}
                                        className="rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                        disabled={ohlcImporting}
                                    />
                                    <span className="text-xs font-normal text-subtle">
                                        CSV: kolumny symbol, date, open, high, low, close, volume
                                        (opcjonalnie). ZIP: spakowane pliki <code>.MST</code> z
                                        notowaniami dziennymi.
                                    </span>
                                </label>
                                {ohlcImportError && (
                                    <div className="rounded-xl border border-rose-200/60 bg-rose-50/70 px-3 py-2 text-sm text-rose-600">
                                        {ohlcImportError}
                                    </div>
                                )}
                                {ohlcImportSuccess && (
                                    <div className="rounded-xl border border-emerald-200/60 bg-emerald-50/70 px-3 py-2 text-sm text-emerald-700">
                                        {ohlcImportSuccess}
                                    </div>
                                )}
                                <div className="flex flex-wrap gap-3">
                                    <button
                                        type="submit"
                                        disabled={ohlcImporting}
                                        className="rounded-full bg-primary px-4 py-2 text-sm font-semibold text-white shadow hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-60"
                                    >
                                        {ohlcImporting ? "Wgrywanie..." : "Wyślij do ClickHouse"}
                                    </button>
                                    <button
                                        type="button"
                                        onClick={() => {
                                            void handleOhlcImportLocal();
                                        }}
                                        disabled={ohlcImporting}
                                        title="Wymaga uruchomionego backendu pod adresem http://localhost:8000"
                                        className="rounded-full bg-emerald-600 px-4 py-2 text-sm font-semibold text-white shadow hover:bg-emerald-500 disabled:cursor-not-allowed disabled:opacity-60"
                                    >
                                        {ohlcImporting ? "Wgrywanie..." : "Wyślij na backend lokalny"}
                                    </button>
                                    <button
                                        type="button"
                                        onClick={handleOhlcImportReset}
                                        disabled={ohlcImporting}
                                        className="rounded-full border border-soft px-4 py-2 text-sm font-semibold text-primary transition hover:border-primary/40 hover:text-primary disabled:cursor-not-allowed disabled:opacity-60"
                                    >
                                        Wyczyść formularz
                                    </button>
                                </div>
                                <p className="text-xs text-subtle">
                                    Plik możesz przygotować lokalnie poleceniem
                                    <code className="mx-1 rounded bg-soft px-1 py-0.5 text-[10px]">
                                        python -m api.offline_export --output ohlc.csv
                                    </code>
                                    .
                                </p>
                            </form>
                        </Card>
                        <Card
                            title="Konfiguracja lokalnego ClickHouse"
                            right={
                                <button
                                    type="button"
                                    onClick={() => {
                                        void refreshLocalClickhouseStatus();
                                    }}
                                    disabled={localClickhouseSaving}
                                    className="rounded-full border border-soft px-4 py-2 text-sm font-semibold text-primary transition hover:border-primary/40 hover:text-primary disabled:cursor-not-allowed disabled:opacity-60"
                                >
                                    Sprawdź backend
                                </button>
                            }
                        >
                            <form
                                className="space-y-4"
                                onSubmit={(event) => {
                                    event.preventDefault();
                                    void applyLocalClickhouseConfig();
                                }}
                            >
                                <p className="text-sm text-muted">
                                    Wprowadź dane połączenia z ClickHouse Cloud, aby przycisk
                                    „Uruchom lokalnie” mógł od razu zapisać je w backendzie.
                                    Dane są przechowywane tylko w tej przeglądarce.
                                </p>
                                <div className="flex flex-wrap gap-4 text-sm font-medium text-primary">
                                    <label className="flex items-center gap-2">
                                        <input
                                            type="radio"
                                            value="url"
                                            checked={localClickhouseMode === "url"}
                                            onChange={() => {
                                                setLocalClickhouseMode("url");
                                                setLocalClickhouseDirty(true);
                                            }}
                                            disabled={localClickhouseSaving}
                                        />
                                        <span>Adres URL</span>
                                    </label>
                                    <label className="flex items-center gap-2">
                                        <input
                                            type="radio"
                                            value="manual"
                                            checked={localClickhouseMode === "manual"}
                                            onChange={() => {
                                                setLocalClickhouseMode("manual");
                                                setLocalClickhouseDirty(true);
                                            }}
                                            disabled={localClickhouseSaving}
                                        />
                                        <span>Ręczna konfiguracja</span>
                                    </label>
                                </div>
                                {localClickhouseMode === "url" ? (
                                    <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                        <span>Adres ClickHouse</span>
                                        <input
                                            type="text"
                                            autoComplete="off"
                                            value={localClickhouseUrl}
                                            onChange={(event) => {
                                                setLocalClickhouseUrl(event.target.value);
                                                setLocalClickhouseDirty(true);
                                            }}
                                            placeholder="np. https://abc123.eu-west-1.aws.clickhouse.cloud:8443/default"
                                            className="rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                            disabled={localClickhouseSaving}
                                        />
                                        <span className="text-xs font-normal text-subtle">
                                            Możesz podać login i hasło w URL lub w polach poniżej.
                                        </span>
                                    </label>
                                ) : (
                                    <div className="grid gap-3 sm:grid-cols-[minmax(0,1fr)_auto]">
                                        <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                            <span>Host ClickHouse</span>
                                            <input
                                                type="text"
                                                autoComplete="off"
                                                value={localClickhouseHost}
                                                onChange={(event) => {
                                                    setLocalClickhouseHost(event.target.value);
                                                    setLocalClickhouseDirty(true);
                                                }}
                                                placeholder="np. abc123.eu-west-1.aws.clickhouse.cloud"
                                                className="rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                                disabled={localClickhouseSaving}
                                            />
                                        </label>
                                        <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                            <span>Port</span>
                                            <input
                                                type="number"
                                                min={1}
                                                max={65535}
                                                value={localClickhousePort}
                                                onChange={(event) => {
                                                    setLocalClickhousePort(event.target.value);
                                                    setLocalClickhouseDirty(true);
                                                }}
                                                placeholder="np. 8443"
                                                className="w-32 rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                                disabled={localClickhouseSaving}
                                            />
                                        </label>
                                    </div>
                                )}
                                <div className="grid gap-3 sm:grid-cols-2">
                                    <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                        <span>Baza danych</span>
                                        <input
                                            type="text"
                                            autoComplete="off"
                                            value={localClickhouseDatabase}
                                            onChange={(event) => {
                                                setLocalClickhouseDatabase(event.target.value);
                                                setLocalClickhouseDirty(true);
                                            }}
                                            placeholder="np. default"
                                            className="rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                            disabled={localClickhouseSaving}
                                        />
                                    </label>
                                    <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                        <span>Użytkownik</span>
                                        <input
                                            type="text"
                                            autoComplete="off"
                                            value={localClickhouseUsername}
                                            onChange={(event) => {
                                                setLocalClickhouseUsername(event.target.value);
                                                setLocalClickhouseDirty(true);
                                            }}
                                            placeholder="np. default"
                                            className="rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                            disabled={localClickhouseSaving}
                                        />
                                    </label>
                                    <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                        <span>Hasło</span>
                                        <input
                                            type="password"
                                            autoComplete="new-password"
                                            value={localClickhousePassword}
                                            onChange={(event) => {
                                                setLocalClickhousePassword(event.target.value);
                                                setLocalClickhouseDirty(true);
                                            }}
                                            placeholder="Wpisz hasło użytkownika"
                                            className="rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                            disabled={localClickhouseSaving}
                                        />
                                    </label>
                                    <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                        <span>Certyfikat CA (opcjonalnie)</span>
                                        <input
                                            type="text"
                                            autoComplete="off"
                                            value={localClickhouseCa}
                                            onChange={(event) => {
                                                setLocalClickhouseCa(event.target.value);
                                                setLocalClickhouseDirty(true);
                                            }}
                                            placeholder="Ścieżka do certyfikatu"
                                            className="rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                            disabled={localClickhouseSaving}
                                        />
                                    </label>
                                </div>
                                <div className="flex flex-wrap gap-4 text-sm text-primary">
                                    <label className="flex items-center gap-2">
                                        <input
                                            type="checkbox"
                                            checked={localClickhouseSecure}
                                            onChange={(event) => {
                                                setLocalClickhouseSecure(event.target.checked);
                                                setLocalClickhouseDirty(true);
                                            }}
                                            disabled={localClickhouseMode === "url" || localClickhouseSaving}
                                            className="h-4 w-4 rounded border-soft text-primary focus:ring-primary/40"
                                        />
                                        <span>Wymuś HTTPS/TLS</span>
                                    </label>
                                    <label className="flex items-center gap-2">
                                        <input
                                            type="checkbox"
                                            checked={localClickhouseVerify}
                                            onChange={(event) => {
                                                setLocalClickhouseVerify(event.target.checked);
                                                setLocalClickhouseDirty(true);
                                            }}
                                            disabled={localClickhouseSaving}
                                            className="h-4 w-4 rounded border-soft text-primary focus:ring-primary/40"
                                        />
                                        <span>Weryfikuj certyfikat TLS</span>
                                    </label>
                                </div>
                                {localClickhouseStatus && (
                                    <div className="space-y-1 rounded-xl border border-soft bg-white/60 p-3 text-xs text-subtle">
                                        <div className="font-semibold text-primary">
                                            {localClickhouseStatus.source === "override"
                                                ? "Backend korzysta z konfiguracji zapisanej w panelu."
                                                : "Backend korzysta z ustawień środowiskowych."}
                                        </div>
                                        {localClickhouseStatusDescription && (
                                            <div>{localClickhouseStatusDescription}</div>
                                        )}
                                        {!localClickhouseBackendReachable && (
                                            <div className="text-negative">
                                                Brak połączenia z lokalnym backendem. Uruchom skrypt
                                                <code className="mx-1 rounded bg-soft px-1 py-0.5 text-[10px]">./scripts/local-sync.sh</code>
                                                i spróbuj ponownie.
                                            </div>
                                        )}
                                    </div>
                                )}
                                {localClickhouseSuccess && (
                                    <div className="rounded-xl border border-emerald-200/60 bg-emerald-50/70 px-3 py-2 text-xs text-emerald-700">
                                        {localClickhouseSuccess}
                                    </div>
                                )}
                                {localClickhouseError && (
                                    <div className="rounded-xl border border-rose-200/60 bg-rose-50/70 px-3 py-2 text-xs text-rose-600">
                                        {localClickhouseError}
                                    </div>
                                )}
                                <div className="flex flex-wrap gap-3">
                                    <button
                                        type="submit"
                                        className="rounded-full bg-emerald-600 px-4 py-2 text-sm font-semibold text-white shadow hover:bg-emerald-500 disabled:cursor-not-allowed disabled:opacity-60"
                                        disabled={localClickhouseSaving}
                                    >
                                        {localClickhouseSaving ? "Zapisywanie..." : "Zapisz i zastosuj"}
                                    </button>
                                    <button
                                        type="button"
                                        onClick={() => {
                                            void handleResetLocalClickhouse();
                                        }}
                                        className="rounded-full border border-soft px-4 py-2 text-sm font-semibold text-primary transition hover:border-primary/40 hover:text-primary disabled:cursor-not-allowed disabled:opacity-60"
                                        disabled={localClickhouseSaving}
                                    >
                                        Przywróć domyślne
                                    </button>
                                </div>
                                <p className="text-xs text-subtle">
                                    Hasło nie opuszcza tej przeglądarki. W razie potrzeby usuń je
                                    poprzez przywrócenie ustawień domyślnych lub wyczyszczenie pamięci
                                    lokalnej.
                                </p>
                            </form>
                        </Card>
                        <Card
                            title="Harmonogram synchronizacji notowań"
                            right={
                                <button
                                    type="button"
                                    onClick={refreshOhlcSchedule}
                                    className="rounded-full border border-soft px-4 py-2 text-sm font-semibold text-primary transition hover:border-primary/40 hover:text-primary"
                                >
                                    Odśwież harmonogram
                                </button>
                            }
                        >
                            <div className="space-y-4">
                                <div className="grid gap-3 text-xs text-subtle sm:grid-cols-2">
                                    <div className="rounded-xl border border-soft bg-white/60 p-3">
                                        <div className="text-[10px] uppercase tracking-wide text-muted">Tryb</div>
                                        <div className="text-sm font-semibold text-primary">
                                            {ohlcScheduleModeLabel}
                                        </div>
                                    </div>
                                    <div className="rounded-xl border border-soft bg-white/60 p-3">
                                        <div className="text-[10px] uppercase tracking-wide text-muted">Status</div>
                                        <div className="text-sm font-semibold text-primary">
                                            {ohlcScheduleStatusLabel}
                                        </div>
                                    </div>
                                    <div className="rounded-xl border border-soft bg-white/60 p-3">
                                        <div className="text-[10px] uppercase tracking-wide text-muted">Najbliższe uruchomienie</div>
                                        <div className="text-sm font-semibold text-primary">
                                            {ohlcScheduleNextRunLabel}
                                        </div>
                                    </div>
                                    <div className="rounded-xl border border-soft bg-white/60 p-3">
                                        <div className="text-[10px] uppercase tracking-wide text-muted">Interwał</div>
                                        <div className="text-sm font-semibold text-primary">
                                            {ohlcScheduleIntervalLabel}
                                        </div>
                                    </div>
                                    <div className="rounded-xl border border-soft bg-white/60 p-3">
                                        <div className="text-[10px] uppercase tracking-wide text-muted">Ostatni start</div>
                                        <div className="text-sm font-semibold text-primary">
                                            {ohlcScheduleLastStartLabel}
                                        </div>
                                    </div>
                                    <div className="rounded-xl border border-soft bg-white/60 p-3">
                                        <div className="text-[10px] uppercase tracking-wide text-muted">Ostatnie zakończenie</div>
                                        <div className="text-sm font-semibold text-primary">
                                            {ohlcScheduleLastFinishLabel}
                                        </div>
                                    </div>
                                </div>
                                {ohlcScheduleSuccess && (
                                    <div className="rounded-xl border border-emerald-200/60 bg-emerald-50/70 px-3 py-2 text-xs text-emerald-700">
                                        {ohlcScheduleSuccess}
                                    </div>
                                )}
                                {ohlcScheduleError && (
                                    <div className="rounded-xl border border-rose-200/60 bg-rose-50/70 px-3 py-2 text-xs text-rose-600">
                                        {ohlcScheduleError}
                                    </div>
                                )}
                                <form onSubmit={handleOhlcScheduleSubmit} className="space-y-3">
                                    <div className="flex flex-wrap gap-4 text-sm font-medium text-primary">
                                        <label className="flex items-center gap-2">
                                            <input
                                                type="radio"
                                                value="once"
                                                checked={ohlcScheduleMode === "once"}
                                                onChange={() => setOhlcScheduleMode("once")}
                                                disabled={ohlcIsScheduling}
                                            />
                                            <span>Jednorazowy</span>
                                        </label>
                                        <label className="flex items-center gap-2">
                                            <input
                                                type="radio"
                                                value="recurring"
                                                checked={ohlcScheduleMode === "recurring"}
                                                onChange={() => setOhlcScheduleMode("recurring")}
                                                disabled={ohlcIsScheduling}
                                            />
                                            <span>Cykliczny</span>
                                        </label>
                                    </div>
                                    {ohlcScheduleMode === "once" ? (
                                        <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                            <span>Data i godzina uruchomienia</span>
                                            <input
                                                type="datetime-local"
                                                value={ohlcOnceDateInput}
                                                onChange={(event) => setOhlcOnceDateInput(event.target.value)}
                                                className="rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                                disabled={ohlcIsScheduling}
                                                required
                                            />
                                        </label>
                                    ) : (
                                        <div className="grid gap-3 sm:grid-cols-2">
                                            <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                                <span>Interwał (minuty)</span>
                                                <input
                                                    type="number"
                                                    min={5}
                                                    value={ohlcRecurringIntervalInput}
                                                    onChange={(event) =>
                                                        setOhlcRecurringIntervalInput(event.target.value)
                                                    }
                                                    className="rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                                    disabled={ohlcIsScheduling}
                                                />
                                                <span className="text-xs font-normal text-subtle">
                                                    Minimalnie 5 minut.
                                                </span>
                                            </label>
                                            <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                                <span>Start harmonogramu (opcjonalnie)</span>
                                                <input
                                                    type="datetime-local"
                                                    value={ohlcRecurringStartInput}
                                                    onChange={(event) =>
                                                        setOhlcRecurringStartInput(event.target.value)
                                                    }
                                                    className="rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                                    disabled={ohlcIsScheduling}
                                                />
                                                <span className="text-xs font-normal text-subtle">
                                                    Domyślnie harmonogram startuje natychmiast.
                                                </span>
                                            </label>
                                        </div>
                                    )}
                                    <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                        <span>Lista symboli (opcjonalnie)</span>
                                        <textarea
                                            value={ohlcScheduleSymbolsInput}
                                            onChange={(event) => setOhlcScheduleSymbolsInput(event.target.value)}
                                            className="min-h-[100px] rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                            placeholder="np. CDR.WA, PKN.WA"
                                            disabled={ohlcIsScheduling}
                                        />
                                        <span className="text-xs font-normal text-subtle">
                                            Pozostaw puste, aby zsynchronizować wszystkie dostępne symbole.
                                        </span>
                                    </label>
                                    <label className="flex flex-col gap-2 text-sm font-medium text-primary">
                                        <span>Data początkowa (opcjonalnie)</span>
                                        <input
                                            type="date"
                                            value={ohlcScheduleStartInput}
                                            onChange={(event) => setOhlcScheduleStartInput(event.target.value)}
                                            className="rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                            disabled={ohlcIsScheduling}
                                        />
                                    </label>
                                    <div className="space-y-3 text-sm text-primary">
                                        <label className="flex items-center gap-3">
                                            <input
                                                type="checkbox"
                                                className="h-4 w-4 rounded border-soft text-primary focus:ring-primary/40"
                                                checked={ohlcScheduleTruncate}
                                                onChange={(event) => {
                                                    const checked = event.target.checked;
                                                    setOhlcScheduleTruncate(checked);
                                                    if (checked && !ohlcScheduleRunAsAdmin) {
                                                        setOhlcScheduleRunAsAdmin(true);
                                                    }
                                                }}
                                                disabled={ohlcIsScheduling}
                                            />
                                            <span>Wyczyść tabelę przed synchronizacją</span>
                                        </label>
                                        <p className="text-xs text-subtle">
                                            Czyszczenie wymaga trybu administratora.
                                        </p>
                                        <label className="flex items-center gap-3">
                                            <input
                                                type="checkbox"
                                                className="h-4 w-4 rounded border-soft text-primary focus:ring-primary/40"
                                                checked={ohlcScheduleRunAsAdmin}
                                                onChange={(event) => setOhlcScheduleRunAsAdmin(event.target.checked)}
                                                disabled={ohlcIsScheduling}
                                            />
                                            <span>Uruchom w trybie administratora</span>
                                        </label>
                                    </div>
                                    <div className="flex flex-wrap gap-3">
                                        <button
                                            type="submit"
                                            disabled={ohlcIsScheduling}
                                            className="rounded-full bg-primary px-4 py-2 text-sm font-semibold text-white shadow hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-60"
                                        >
                                            Zapisz harmonogram
                                        </button>
                                        <button
                                            type="button"
                                            onClick={handleOhlcScheduleCancel}
                                            disabled={ohlcIsScheduling || !ohlcHasActiveSchedule}
                                            className="rounded-full border border-soft px-4 py-2 text-sm font-semibold text-muted transition hover:border-primary/40 hover:text-primary disabled:cursor-not-allowed disabled:opacity-60"
                                        >
                                            Usuń harmonogram
                                        </button>
                                    </div>
                                </form>
                            </div>
                        </Card>
                        <Card title="Ostatnia synchronizacja notowań">
                            {ohlcResult ? (
                                <div className="space-y-4">
                                    <div className="grid grid-cols-2 gap-3 text-xs text-subtle sm:grid-cols-4">
                                        <div>
                                            <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                                Symbole
                                            </span>
                                            <span className="text-sm font-semibold text-primary">
                                                {integerFormatter.format(ohlcResult.symbols)}
                                            </span>
                                        </div>
                                        <div>
                                            <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                                Zapisane wiersze
                                            </span>
                                            <span className="text-sm font-semibold text-primary">
                                                {integerFormatter.format(ohlcResult.inserted)}
                                            </span>
                                        </div>
                                        <div>
                                            <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                                Pominięte symbole
                                            </span>
                                            <span className="text-sm font-semibold text-primary">
                                                {integerFormatter.format(ohlcResult.skipped)}
                                            </span>
                                        </div>
                                        <div>
                                            <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                                Czyszczenie tabeli
                                            </span>
                                            <span className="text-sm font-semibold text-primary">
                                                {ohlcResult.truncated ? "Tak" : "Nie"}
                                            </span>
                                        </div>
                                    </div>
                                    <div className="grid gap-2 text-xs text-subtle sm:grid-cols-2">
                                        <div>Start: {formatDateTime(ohlcResult.started_at)}</div>
                                        <div>Koniec: {formatDateTime(ohlcResult.finished_at)}</div>
                                        <div>Czas trwania: {formatDuration(ohlcResult.started_at, ohlcResult.finished_at)}</div>
                                        <div>
                                            Tryb: {ohlcResult.requested_as_admin ? "Administrator" : "Standardowy"}
                                        </div>
                                    </div>
                                    {ohlcHasErrors ? (
                                        <div className="rounded-xl border border-amber-200 bg-amber-50/60 p-4 text-xs text-amber-600">
                                            <div className="mb-2 font-semibold">Komunikaty błędów</div>
                                            <ul className="list-disc space-y-1 pl-4">
                                                {ohlcResult.errors.map((error, index) => (
                                                    <li key={`${error}-${index}`}>{error}</li>
                                                ))}
                                            </ul>
                                        </div>
                                    ) : (
                                        <div className="rounded-xl border border-soft bg-white/60 p-3 text-xs text-subtle">
                                            Brak błędów zgłoszonych podczas synchronizacji.
                                        </div>
                                    )}
                                    <div className="space-y-2">
                                        <button
                                            type="button"
                                            onClick={() => setOhlcShowRequestLog((prev) => !prev)}
                                            className="text-xs font-semibold text-primary transition hover:text-primary/80"
                                        >
                                            {ohlcShowRequestLog ? "Ukryj logi zapytań" : "Pokaż logi zapytań HTTP"}
                                        </button>
                                        {ohlcShowRequestLog && (
                                            <div className="space-y-2">
                                                {ohlcRequestLog.length ? (
                                                    ohlcRequestLog.map((entry, index) => (
                                                        <div
                                                            key={`${entry.url}-${entry.started_at}-${index}`}
                                                            className="space-y-2 rounded-xl border border-soft bg-white/70 p-3 text-xs text-subtle"
                                                        >
                                                            <div className="break-all text-sm font-medium text-primary">
                                                                {entry.url}
                                                            </div>
                                                            {entry.source && (
                                                                <div>
                                                                    Źródło:
                                                                    <span className="ml-1 font-semibold text-primary">
                                                                        {entry.source}
                                                                    </span>
                                                                </div>
                                                            )}
                                                            <div>Status: {entry.status_code ?? "—"}</div>
                                                            <div>Start: {formatDateTime(entry.started_at)}</div>
                                                            <div>
                                                                Koniec:
                                                                {entry.finished_at
                                                                    ? ` ${formatDateTime(entry.finished_at)}`
                                                                    : " —"}
                                                            </div>
                                                            <div>
                                                                Czas: {formatDuration(entry.started_at, entry.finished_at ?? null)}
                                                            </div>
                                                            {entry.params && Object.keys(entry.params).length > 0 && (
                                                                <pre className="whitespace-pre-wrap break-all rounded-lg bg-soft/60 px-3 py-2 text-[11px] text-muted">
                                                                    {JSON.stringify(entry.params, null, 2)}
                                                                </pre>
                                                            )}
                                                            {entry.error && (
                                                                <div className="text-[11px] text-negative">Błąd: {entry.error}</div>
                                                            )}
                                                        </div>
                                                    ))
                                                ) : (
                                                    <div className="rounded-xl border border-soft bg-white/60 p-3 text-xs text-subtle">
                                                        Brak logów zapytań.
                                                    </div>
                                                )}
                                            </div>
                                        )}
                                    </div>
                                </div>
                            ) : (
                                <p className="text-sm text-subtle">
                                    Uruchom synchronizację, aby zobaczyć podsumowanie notowań.
                                </p>
                            )}
                        </Card>
                    </div>
                    <div className="space-y-6">
                        <Card title="Wskazówki">
                            <div className="space-y-3 text-sm text-muted">
                                <p>
                                    Synchronizacja pobiera pliki CSV ze Stooq i zapisuje je do tabeli{" "}
                                    <code className="mx-1 rounded bg-soft px-1 py-0.5 text-xs">ohlc</code>{" "}
                                    w ClickHouse.
                                </p>
                                <p>
                                    Jeżeli pozostawisz pole symboli puste, aplikacja spróbuje zsynchronizować wszystkie symbole dostępne w bazie spółek oraz w istniejącej tabeli notowań.
                                </p>
                                <p>
                                    Tryb administratora umożliwia czyszczenie tabeli przed ponownym załadowaniem danych. Używaj go ostrożnie, aby nie utracić historycznych notowań.
                                </p>
                            </div>
                        </Card>
                    </div>
                </div>
            </Section>
            <Section
            id="companies-sync"
            kicker="GPW"
            title="Synchronizacja danych o spółkach"
            description="Uruchom pobieranie profili spółek z GPW w tle, obserwuj postęp synchronizacji i przeglądaj szczegółowe dane fundamentalne."
        >
            <div className="grid gap-6 lg:grid-cols-[minmax(0,1.1fr)_minmax(0,1.9fr)]">
                <div className="space-y-6">
                    <Card
                        title="Status synchronizacji"
                        right={
                            <>
                                <button
                                    type="button"
                                    onClick={startSync}
                                    disabled={isStarting || status?.status === "running"}
                                    className="rounded-full bg-primary px-4 py-2 text-sm font-semibold text-white shadow hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-60"
                                >
                                    {isStarting ? "Uruchamianie..." : "Uruchom synchronizację"}
                                </button>
                                    <button
                                        type="button"
                                        onClick={() => {
                                            void startLocalSync();
                                        }}
                                    disabled={isStartingLocal}
                                    title="Wymaga uruchomionego backendu pod adresem http://localhost:8000"
                                    className="rounded-full bg-emerald-600 px-4 py-2 text-sm font-semibold text-white shadow hover:bg-emerald-500 disabled:cursor-not-allowed disabled:opacity-60"
                                >
                                    {isStartingLocal ? "Uruchamianie..." : "Uruchom lokalnie"}
                                </button>
                                <button
                                    type="button"
                                    onClick={fetchStatus}
                                    className="rounded-full border border-soft px-4 py-2 text-sm font-semibold text-primary transition hover:border-primary/40 hover:text-primary"
                                >
                                    Odśwież status
                                </button>
                            </>
                        }
                    >
                        <div className="space-y-4">
                            <div className="space-y-1">
                                <p className="text-sm text-muted">{statusLabel}</p>
                                {stageLabel && (
                                    <p className="text-xs text-subtle">
                                        Etap: {stageLabel}
                                        {status?.current_symbol ? ` • ${status.current_symbol}` : ""}
                                    </p>
                                )}
                                {status?.message && (
                                    <p className="text-xs text-subtle">{status.message}</p>
                                )}
                            </div>
                            <div>
                                <div className="mb-2 flex items-center justify-between text-xs text-subtle">
                                    <span>Postęp</span>
                                    <span>{progressPercent}%</span>
                                </div>
                                <div className="h-2 w-full overflow-hidden rounded-full bg-soft">
                                    <div
                                        className={`h-full transition-all duration-500 ${
                                            status?.status === "failed" ? "bg-rose-500" : "bg-primary"
                                        }`}
                                        style={{ width: `${progressPercent}%` }}
                                    />
                                </div>
                                <p className="mt-4 text-xs text-subtle">
                                    Aby zsynchronizować dane lokalnie, uruchom backend na adresie
                                    <code className="mx-1 rounded bg-soft px-1 py-0.5 text-[10px]">http://localhost:8000</code>
                                    i użyj przycisku „Uruchom lokalnie”.
                                </p>
                                <div className="mt-3 grid grid-cols-2 gap-3 text-xs text-subtle sm:grid-cols-4">
                                    <div>
                                        <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                            Przetworzono
                                        </span>
                                        <span className="text-sm font-semibold text-primary">
                                            {integerFormatter.format(processed)}
                                        </span>
                                    </div>
                                    <div>
                                        <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                            Zapisano
                                        </span>
                                        <span className="text-sm font-semibold text-primary">
                                            {integerFormatter.format(synced)}
                                        </span>
                                    </div>
                                    <div>
                                        <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                            Do pobrania
                                        </span>
                                        <span className="text-sm font-semibold text-primary">
                                            {total > 0
                                                ? integerFormatter.format(Math.max(total - processed, 0))
                                                : "—"}
                                        </span>
                                    </div>
                                    <div>
                                        <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                            Błędy
                                        </span>
                                        <span className="text-sm font-semibold text-primary">
                                            {integerFormatter.format(failed)}
                                        </span>
                                    </div>
                                </div>
                                <div className="mt-3 grid grid-cols-2 gap-3 text-xs text-subtle sm:grid-cols-4">
                                    <div>
                                        <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                            Start
                                        </span>
                                        <span className="text-sm font-semibold text-primary">
                                            {formatDateTime(status?.started_at ?? status?.result?.started_at)}
                                        </span>
                                    </div>
                                    <div>
                                        <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                            Koniec
                                        </span>
                                        <span className="text-sm font-semibold text-primary">
                                            {formatDateTime(status?.finished_at ?? status?.result?.finished_at)}
                                        </span>
                                    </div>
                                    <div>
                                        <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                            Pobrano
                                        </span>
                                        <span className="text-sm font-semibold text-primary">
                                            {integerFormatter.format(status?.result?.fetched ?? total)}
                                        </span>
                                    </div>
                                    <div>
                                        <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                            Zapisano
                                        </span>
                                        <span className="text-sm font-semibold text-primary">
                                            {integerFormatter.format(status?.result?.synced ?? synced)}
                                        </span>
                                    </div>
                                </div>
                            </div>
                            {(statusError || visibleErrors.length > 0) && (
                                <div className="space-y-2">
                                    {statusError && (
                                        <p className="text-xs text-amber-500">{statusError}</p>
                                    )}
                                    {visibleErrors.length > 0 && (
                                        <ul className="space-y-1 rounded-xl border border-rose-200/60 bg-rose-50/70 px-3 py-2 text-xs text-rose-600">
                                            {visibleErrors.map((errorMessage, index) => (
                                                <li key={`${errorMessage}-${index}`}>• {errorMessage}</li>
                                            ))}
                                        </ul>
                                    )}
                                </div>
                            )}
                        </div>
                    </Card>
                    <Card
                        title="Harmonogram synchronizacji"
                        right={
                            <button
                                type="button"
                                onClick={refreshSchedule}
                                className="rounded-full border border-soft px-4 py-2 text-sm font-semibold text-primary transition hover:border-primary/40 hover:text-primary"
                            >
                                Odśwież harmonogram
                            </button>
                        }
                    >
                        <div className="space-y-4">
                            <div className="grid gap-3 text-xs text-subtle sm:grid-cols-2">
                                <div>
                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                        Tryb harmonogramu
                                    </span>
                                    <span className="text-sm font-semibold text-primary">
                                        {scheduleModeLabel}
                                    </span>
                                </div>
                                <div>
                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                        Status ostatniego uruchomienia
                                    </span>
                                    <span className="text-sm font-semibold text-primary">
                                        {scheduleStatusLabel}
                                    </span>
                                </div>
                                <div>
                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                        Najbliższa synchronizacja
                                    </span>
                                    <span className="text-sm font-semibold text-primary">
                                        {scheduleNextRunLabel}
                                    </span>
                                </div>
                                <div>
                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                        Interwał
                                    </span>
                                    <span className="text-sm font-semibold text-primary">
                                        {scheduleIntervalLabel}
                                    </span>
                                </div>
                                <div>
                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                        Ostatnie rozpoczęcie
                                    </span>
                                    <span className="text-sm font-semibold text-primary">
                                        {scheduleLastStartLabel}
                                    </span>
                                </div>
                                <div>
                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                        Ostatnie zakończenie
                                    </span>
                                    <span className="text-sm font-semibold text-primary">
                                        {scheduleLastFinishLabel}
                                    </span>
                                </div>
                            </div>
                            {scheduleSuccess && (
                                <div className="rounded-xl border border-emerald-200/60 bg-emerald-50/70 px-3 py-2 text-xs text-emerald-700">
                                    {scheduleSuccess}
                                </div>
                            )}
                            {scheduleError && (
                                <div className="rounded-xl border border-rose-200/60 bg-rose-50/70 px-3 py-2 text-xs text-rose-600">
                                    {scheduleError}
                                </div>
                            )}
                            <div className="grid gap-4 lg:grid-cols-2">
                                <form onSubmit={handleScheduleOnce} className="space-y-3">
                                    <div className="text-xs font-semibold uppercase tracking-wide text-subtle">
                                        Jednorazowa synchronizacja
                                    </div>
                                    <input
                                        type="datetime-local"
                                        value={onceDateInput}
                                        onChange={(event) => setOnceDateInput(event.target.value)}
                                        className="w-full rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                        required
                                    />
                                    <button
                                        type="submit"
                                        disabled={isScheduling}
                                        className="w-full rounded-full bg-primary px-4 py-2 text-sm font-semibold text-white shadow hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-60"
                                    >
                                        Zaplanuj jednorazowo
                                    </button>
                                </form>
                                <form onSubmit={handleScheduleRecurring} className="space-y-3">
                                    <div className="text-xs font-semibold uppercase tracking-wide text-subtle">
                                        Harmonogram cykliczny
                                    </div>
                                    <div className="flex items-center gap-2">
                                        <input
                                            type="number"
                                            min={5}
                                            value={recurringIntervalInput}
                                            onChange={(event) => setRecurringIntervalInput(event.target.value)}
                                            className="w-32 rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                            required
                                        />
                                        <span className="text-xs text-subtle">minut</span>
                                    </div>
                                    <div className="space-y-1">
                                        <label className="block text-[10px] uppercase tracking-wide text-subtle">
                                            Start harmonogramu
                                        </label>
                                        <input
                                            type="datetime-local"
                                            value={recurringStartInput}
                                            onChange={(event) => setRecurringStartInput(event.target.value)}
                                            className="w-full rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                            placeholder="Od razu"
                                        />
                                    </div>
                                    <button
                                        type="submit"
                                        disabled={isScheduling}
                                        className="w-full rounded-full bg-primary px-4 py-2 text-sm font-semibold text-white shadow hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-60"
                                    >
                                        Zapisz harmonogram
                                    </button>
                                </form>
                            </div>
                            <div className="flex flex-wrap gap-2">
                                <button
                                    type="button"
                                    onClick={handleCancelSchedule}
                                    disabled={isScheduling || !hasActiveSchedule}
                                    className="rounded-full border border-soft px-4 py-2 text-sm font-semibold text-muted transition hover:border-primary/40 hover:text-primary disabled:cursor-not-allowed disabled:opacity-60"
                                >
                                    Usuń harmonogram
                                </button>
                            </div>
                        </div>
                    </Card>
                    <Card
                        title="Administratorzy panelu"
                        right={
                            <button
                                type="button"
                                onClick={refreshAdmins}
                                className="rounded-full border border-soft px-4 py-2 text-sm font-semibold text-primary transition hover:border-primary/40 hover:text-primary"
                            >
                                Odśwież listę
                            </button>
                        }
                    >
                        <div className="space-y-4">
                            {adminsSuccess && (
                                <div className="rounded-xl border border-emerald-200/60 bg-emerald-50/70 px-3 py-2 text-xs text-emerald-700">
                                    {adminsSuccess}
                                </div>
                            )}
                            {adminsError && (
                                <div className="rounded-xl border border-rose-200/60 bg-rose-50/70 px-3 py-2 text-xs text-rose-600">
                                    {adminsError}
                                </div>
                            )}
                            <div className="space-y-2">
                                {admins.length === 0 ? (
                                    <p className="text-xs text-subtle">
                                        Brak zdefiniowanych administratorów.
                                    </p>
                                ) : (
                                    admins.map((admin) => (
                                        <div
                                            key={admin.email}
                                            className="rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm"
                                        >
                                            <div className="flex items-center justify-between gap-3">
                                                <span className="font-semibold break-all">{admin.email}</span>
                                                <span className="text-[11px] uppercase tracking-wide text-subtle">
                                                    {formatDateTime(admin.createdAt)}
                                                </span>
                                            </div>
                                            <p className="text-[11px] text-subtle">
                                                Dodano przez {admin.addedBy ?? "system"}
                                            </p>
                                        </div>
                                    ))
                                )}
                            </div>
                            <form onSubmit={handleAddAdmin} className="space-y-3">
                                <div className="text-xs font-semibold uppercase tracking-wide text-subtle">
                                    Dodaj administratora
                                </div>
                                <input
                                    type="email"
                                    value={newAdminEmail}
                                    onChange={(event) => setNewAdminEmail(event.target.value)}
                                    placeholder="adres@example.com"
                                    className="w-full rounded-xl border border-soft bg-white/70 px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40"
                                    required
                                />
                                <button
                                    type="submit"
                                    disabled={isAddingAdmin}
                                    className="w-full rounded-full bg-primary px-4 py-2 text-sm font-semibold text-white shadow hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-60"
                                >
                                    Dodaj administratora
                                </button>
                            </form>
                        </div>
                    </Card>
                    <Card
                        title="Lista spółek"
                        right={
                            <form
                                onSubmit={handleSearchSubmit}
                                className="flex w-full flex-col gap-2 sm:flex-row"
                            >
                                <input
                                    type="text"
                                    value={search}
                                    onChange={(event) => setSearch(event.target.value)}
                                    placeholder="Symbol, nazwa lub ISIN"
                                    className="flex-1 rounded-full border border-soft bg-transparent px-3 py-2 text-sm text-primary placeholder:text-subtle focus:outline-none focus:ring-2 focus:ring-primary/40"
                                />
                                <div className="flex gap-2">
                                    <button
                                        type="submit"
                                        className="rounded-full bg-primary px-4 py-2 text-sm font-semibold text-white shadow hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-60"
                                        disabled={companiesLoading}
                                    >
                                        Szukaj
                                    </button>
                                    <button
                                        type="button"
                                        onClick={handleResetSearch}
                                        className="rounded-full border border-soft px-4 py-2 text-sm font-semibold text-muted transition hover:border-primary/40 hover:text-primary"
                                    >
                                        Wyczyść
                                    </button>
                                </div>
                            </form>
                        }
                    >
                        <div className="space-y-3">
                            {companiesLoading && (
                                <p className="text-xs text-subtle">Ładowanie listy spółek…</p>
                            )}
                            {companiesError && (
                                <p className="text-xs text-amber-500">{companiesError}</p>
                            )}
                            {!companiesLoading && !companiesError && companies.length === 0 && (
                                <p className="text-xs text-subtle">
                                    Brak spółek do wyświetlenia. Uruchom synchronizację, aby pobrać dane.
                                </p>
                            )}
                            <div className="max-h-80 space-y-1 overflow-y-auto pr-1">
                                {companies.map((company) => {
                                    const isActive =
                                        company.symbol === symbol ||
                                        company.raw_symbol === symbol;
                                    const marketCap = company.fundamentals?.market_cap ?? null;
                                    return (
                                        <button
                                            type="button"
                                            key={`${company.symbol}-${company.raw_symbol}`}
                                            onClick={() => handleSelectCompany(company.symbol)}
                                            className={`w-full rounded-xl border px-3 py-3 text-left transition ${
                                                isActive
                                                    ? "border-primary/60 bg-primary/10 shadow-inner"
                                                    : "border-transparent bg-soft/40 hover:border-primary/40 hover:bg-soft/80"
                                            }`}
                                        >
                                            <div className="flex items-start justify-between gap-3">
                                                <div className="space-y-1">
                                                    <div className="text-sm font-semibold text-primary">
                                                        {company.name ?? company.short_name ?? company.raw_symbol ?? company.symbol}
                                                    </div>
                                                    <div className="text-xs text-subtle">
                                                        {company.raw_symbol ?? company.symbol}
                                                        {company.sector ? ` • ${company.sector}` : ""}
                                                        {company.industry ? ` • ${company.industry}` : ""}
                                                    </div>
                                                </div>
                                                <div className="text-right text-xs text-subtle">
                                                    {marketCap !== null
                                                        ? `${integerFormatter.format(marketCap)}`
                                                        : "—"}
                                                </div>
                                            </div>
                                        </button>
                                    );
                                })}
                            </div>
                        </div>
                    </Card>
                    <Card title="Mapowanie symboli GPW Benchmark">
                        <div className="space-y-3 text-sm text-subtle">
                            <p>
                                Powiąż spółki z symbolami używanymi w indeksach GPW Benchmark. Wpisz
                                oznaczenie tak, jak występuje w indeksie (np. <span className="font-semibold">CDR.WA</span>) i zapisz zmiany.
                            </p>
                            {benchmarkSymbolsLoading && (
                                <p className="text-xs">Ładowanie listy symboli benchmarku…</p>
                            )}
                            {benchmarkSymbolsError && (
                                <p className="text-xs text-amber-500">{benchmarkSymbolsError}</p>
                            )}
                            <datalist id="benchmark-symbols-list">
                                {benchmarkSymbols.map((option) => {
                                    const labelParts: string[] = [];
                                    if (option.company_name) {
                                        labelParts.push(option.company_name);
                                    }
                                    if (option.indices.length > 0) {
                                        labelParts.push(option.indices.join(", "));
                                    }
                                    return (
                                        <option
                                            key={option.symbol}
                                            value={option.symbol}
                                            label={labelParts.length ? labelParts.join(" • ") : undefined}
                                        />
                                    );
                                })}
                            </datalist>
                            {companies.length > 0 ? (
                                <div className="max-h-96 space-y-2 overflow-y-auto pr-1">
                                    {companies.map((company) => {
                                        const baseSymbol = company.raw_symbol ?? company.symbol ?? "";
                                        const key = baseSymbol.trim().toUpperCase();
                                        if (!key) {
                                            return null;
                                        }
                                        const inputValue = benchmarkInputs[key] ?? "";
                                        const savedValue = benchmarkOriginals[key] ?? "";
                                        const status = benchmarkStatuses[key] ?? {
                                            status: "idle" as const,
                                            message: null,
                                        };
                                        const isDirty = inputValue.trim() !== savedValue.trim();
                                        const statusClasses =
                                            status.status === "error"
                                                ? "text-rose-600"
                                                : status.status === "success"
                                                    ? "text-emerald-600"
                                                    : "text-subtle";
                                        return (
                                            <div
                                                key={`${key}-benchmark-mapping`}
                                                className="rounded-xl border border-soft bg-white/70 p-3 shadow-sm"
                                            >
                                                <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                                                    <div className="space-y-1">
                                                        <div className="text-sm font-semibold text-primary">
                                                            {company.name ?? company.short_name ?? baseSymbol}
                                                        </div>
                                                        <div className="text-xs text-subtle">
                                                            {baseSymbol}
                                                            {company.sector ? ` • ${company.sector}` : ""}
                                                        </div>
                                                    </div>
                                                    <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
                                                        <input
                                                            type="text"
                                                            list="benchmark-symbols-list"
                                                            value={inputValue}
                                                            onChange={(event) =>
                                                                handleBenchmarkInputChange(
                                                                    key,
                                                                    event.target.value
                                                                )
                                                            }
                                                            placeholder="np. CDR.WA"
                                                            className="w-full rounded-xl border border-soft bg-surface px-3 py-2 text-sm text-primary shadow-sm focus:border-primary focus:outline-none focus:ring-1 focus:ring-primary/40 sm:w-40"
                                                        />
                                                        <button
                                                            type="button"
                                                            onClick={() => handleBenchmarkSave(company)}
                                                            disabled={status.status === "saving" || (!isDirty && status.status !== "error")}
                                                            className="rounded-full bg-primary px-4 py-2 text-xs font-semibold text-white shadow transition hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-60"
                                                        >
                                                            {status.status === "saving" ? "Zapisywanie…" : "Zapisz"}
                                                        </button>
                                                    </div>
                                                </div>
                                                {status.status !== "idle" && status.message && (
                                                    <p className={`mt-2 text-xs ${statusClasses}`}>
                                                        {status.message}
                                                    </p>
                                                )}
                                            </div>
                                        );
                                    })}
                                </div>
                            ) : (
                                <p className="text-xs">
                                    Brak spółek do zmapowania. Uruchom synchronizację, aby pobrać dane.
                                </p>
                            )}
                        </div>
                    </Card>
                </div>
                <Card
                    title={
                        selectedCompany
                            ? `Szczegóły ${
                                  selectedCompany.name ??
                                  selectedCompany.short_name ??
                                  selectedCompany.raw_symbol ??
                                  selectedCompany.symbol
                              }`
                            : "Wybierz spółkę z listy"
                    }
                >
                    {detailsLoading && (
                        <p className="text-xs text-subtle">Ładowanie szczegółów…</p>
                    )}
                    {detailsError && (
                        <p className="text-xs text-amber-500">{detailsError}</p>
                    )}
                    {selectedCompany ? (
                        <div className="space-y-6">
                            <div className="space-y-2">
                                <div className="space-y-1">
                                    <p className="text-xs uppercase tracking-[0.3em] text-subtle">
                                        {selectedCompany.raw_symbol}
                                    </p>
                                    <h3 className="text-xl font-semibold text-primary">
                                        {selectedCompany.name ??
                                            selectedCompany.short_name ??
                                            selectedCompany.raw_symbol ??
                                            selectedCompany.symbol}
                                    </h3>
                                    <p className="text-sm text-muted">
                                        {selectedCompany.sector}
                                        {selectedCompany.industry ? ` • ${selectedCompany.industry}` : ""}
                                        {selectedCompany.country ? ` • ${selectedCompany.country}` : ""}
                                    </p>
                                </div>
                                {descriptionSnippet && (
                                    <p className="text-sm leading-relaxed text-subtle">
                                        {descriptionSnippet}
                                    </p>
                                )}
                            </div>
                            <div className="grid gap-3 text-sm text-subtle sm:grid-cols-2">
                                <div>
                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                        Symbol
                                    </span>
                                    <span className="text-base font-semibold text-primary">
                                        {selectedCompany.raw_symbol ?? selectedCompany.symbol}
                                    </span>
                                </div>
                                <div>
                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                        ISIN
                                    </span>
                                    <span className="text-base font-semibold text-primary">
                                        {selectedCompany.isin ?? "—"}
                                    </span>
                                </div>
                                <div>
                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                        Notowanie od
                                    </span>
                                    <span className="text-base font-semibold text-primary">
                                        {formatDate(selectedCompany.listing_date)}
                                    </span>
                                </div>
                                <div>
                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                        Liczba pracowników
                                    </span>
                                    <span className="text-base font-semibold text-primary">
                                        {companyEmployees}
                                    </span>
                                </div>
                                <div>
                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                        Siedziba
                                    </span>
                                    <span className="text-base font-semibold text-primary">
                                        {selectedCompany.headquarters ?? "—"}
                                    </span>
                                </div>
                                <div>
                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                        Strona WWW
                                    </span>
                                    {selectedCompany.website ? (
                                        <a
                                            href={selectedCompany.website}
                                            target="_blank"
                                            rel="noreferrer"
                                            className="text-base font-semibold text-primary underline decoration-dotted underline-offset-4"
                                        >
                                            {selectedCompany.website}
                                        </a>
                                    ) : (
                                        <span className="text-base font-semibold text-primary">—</span>
                                    )}
                                </div>
                                <div>
                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                        Kapitalizacja
                                    </span>
                                    <span className="text-base font-semibold text-primary">
                                        {marketCapValue !== null
                                            ? integerFormatter.format(marketCapValue)
                                            : "—"}
                                    </span>
                                </div>
                            </div>
                            {symbolMappings.length > 0 && (
                                <div className="rounded-lg border border-soft p-4">
                                    <p className="text-[10px] uppercase tracking-wide text-subtle">
                                        Symbole na platformach
                                    </p>
                                    <div className="mt-2 grid gap-3 text-sm text-subtle sm:grid-cols-2">
                                        {symbolMappings.map(({ label, value }) => (
                                            <div key={label}>
                                                <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                                    {label}
                                                </span>
                                                <span className="text-base font-semibold text-primary">
                                                    {value ?? "—"}
                                                </span>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}
                            <div className="grid gap-3 sm:grid-cols-2">
                                {HIGHLIGHT_FUNDAMENTALS.map(({ key, label }) => {
                                    const rawValue = selectedCompany.fundamentals?.[key] ?? null;
                                    return (
                                        <div
                                            key={key}
                                            className="rounded-xl border border-soft bg-soft-surface px-4 py-3"
                                        >
                                            <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                                {label}
                                            </span>
                                            <span className="text-lg font-semibold text-primary">
                                                {formatFundamentalValue(key, rawValue)}
                                            </span>
                                        </div>
                                    );
                                })}
                            </div>
                            <div className="space-y-2">
                                <h4 className="text-sm font-semibold text-primary">
                                    Udział w indeksach
                                </h4>
                                {indexMembership.length > 0 ? (
                                    <div className="flex flex-wrap gap-2">
                                        {indexMembership.map((indexName) => (
                                            <span
                                                key={indexName}
                                                className="rounded-full border border-soft bg-soft-surface px-3 py-1 text-xs font-medium text-primary"
                                            >
                                                {indexName}
                                            </span>
                                        ))}
                                    </div>
                                ) : (
                                    <p className="text-xs text-subtle">
                                        Brak informacji o udziałach w indeksach.
                                    </p>
                                )}
                            </div>
                            {(rawInsights.companySize || rawInsights.shareholding.length > 0 ||
                                rawInsights.facts.length > 0) && (
                                <div className="space-y-3">
                                    <h4 className="text-sm font-semibold text-primary">
                                        Dodatkowe dane z GPW
                                    </h4>
                                    {rawInsights.companySize && (
                                        <div className="rounded-xl border border-soft bg-soft-surface px-4 py-3">
                                            <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                                Wielkość spółki
                                            </span>
                                            <span className="text-sm font-semibold text-primary">
                                                {rawInsights.companySize}
                                            </span>
                                        </div>
                                    )}
                                    {rawInsights.shareholding.length > 0 && (
                                        <div className="rounded-xl border border-soft bg-soft-surface px-4 py-3">
                                            <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                                Akcjonariat
                                            </span>
                                            <ul className="mt-2 space-y-1 text-xs text-subtle">
                                                {rawInsights.shareholding.map((entry, index) => (
                                                    <li
                                                        key={`${entry}-${index}`}
                                                        className="flex items-start gap-2"
                                                    >
                                                        <span className="mt-1 block h-1.5 w-1.5 flex-none rounded-full bg-primary/70" />
                                                        <span className="leading-snug text-primary/80">
                                                            {entry}
                                                        </span>
                                                    </li>
                                                ))}
                                            </ul>
                                        </div>
                                    )}
                                    {rawInsights.facts.length > 0 && (
                                        <div className="grid gap-3 text-xs text-subtle sm:grid-cols-2">
                                            {rawInsights.facts.map((fact) => (
                                                <div
                                                    key={`${fact.label}-${fact.value}`}
                                                    className="rounded-xl border border-soft bg-soft-surface px-4 py-3"
                                                >
                                                    <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                                        {fact.label}
                                                    </span>
                                                    <span className="text-sm font-medium text-primary">
                                                        {fact.value}
                                                    </span>
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            )}
                            {fundamentalEntries.length > 0 && (
                                <div className="space-y-3">
                                    <h4 className="text-sm font-semibold text-primary">
                                        Wskaźniki fundamentalne
                                    </h4>
                                    <div className="grid gap-3 text-xs text-subtle sm:grid-cols-2">
                                        {fundamentalEntries.map(([key, value]) => (
                                            <div key={key}>
                                                <span className="block text-[10px] uppercase tracking-wide text-subtle">
                                                    {FUNDAMENTAL_LABELS[key] ?? key}
                                                </span>
                                                <span className="text-sm font-semibold text-primary">
                                                    {formatFundamentalValue(key, value)}
                                                </span>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}
                        </div>
                    ) : (
                        <p className="text-sm text-subtle">
                            Wybierz spółkę z listy, aby zobaczyć szczegółowe dane.
                        </p>
                    )}
                </Card>
            </div>
            </Section>
        </div>
    );
};

export type DashboardView = "analysis" | "score" | "portfolio" | "sync";

export type AnalyticsDashboardProps = {
    view: DashboardView;
};
type NavItem = {
    href: string;
    label: string;
    key?: DashboardView;
    icon?: React.ComponentType<{ className?: string }>;
    description?: string;
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

const IconSync = ({ className }: { className?: string }) => (
    <svg
        viewBox="0 0 24 24"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        className={className}
    >
        <path
            d="M20 4V9H15"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
        <path
            d="M4 20V15H9"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
        <path
            d="M7 7C8.343 5.657 10.209 4.857 12.142 4.857C14.075 4.857 15.941 5.657 17.284 7"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
        />
        <path
            d="M17 17C15.657 18.343 13.791 19.143 11.858 19.143C9.925 19.143 8.059 18.343 6.716 17"
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
                <Link
                    key={item.href}
                    href={item.href}
                    className="px-3 py-1 rounded-full border border-white/20 bg-white/10 text-white/80 hover:text-white hover:border-white/40 transition"
                >
                    {item.label}
                </Link>
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

const collapsedFloatingTooltipClass =
    "pointer-events-none fixed z-[9999] -translate-y-1/2 whitespace-nowrap rounded-lg bg-primary-soft px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.28em] text-neutral shadow-brand-floating ring-1 ring-[rgba(16,163,127,0.25)]";

const SidebarNav = ({
    items,
    activeKey,
    collapsed,
    onNavigate,
    scrollContainerRef,
}: {
    items: NavItem[];
    activeKey?: DashboardView;
    collapsed?: boolean;
    onNavigate?: () => void;
    scrollContainerRef?: React.RefObject<HTMLElement | null>;
}) => {
    const [floatingLabel, setFloatingLabel] = useState<{
        label: string;
        top: number;
        left: number;
    } | null>(null);
    const hoveredElementRef = useRef<HTMLElement | null>(null);

    const hideFloatingLabel = useCallback(() => {
        setFloatingLabel(null);
        hoveredElementRef.current = null;
    }, []);

    const updateFloatingLabelPosition = useCallback(() => {
        const element = hoveredElementRef.current;
        if (!element) {
            return;
        }
        const rect = element.getBoundingClientRect();
        setFloatingLabel((prev) => {
            if (!prev) {
                return null;
            }
            return {
                ...prev,
                top: rect.top + rect.height / 2,
                left: rect.right + 12,
            };
        });
    }, []);

    const showFloatingLabel = useCallback((element: HTMLElement, label: string) => {
        hoveredElementRef.current = element;
        const rect = element.getBoundingClientRect();
        setFloatingLabel({
            label,
            top: rect.top + rect.height / 2,
            left: rect.right + 12,
        });
    }, []);

    useEffect(() => {
        if (!collapsed) {
            hideFloatingLabel();
            return;
        }
        updateFloatingLabelPosition();
    }, [collapsed, hideFloatingLabel, updateFloatingLabelPosition]);

    useEffect(() => {
        if (!collapsed || !floatingLabel) {
            return;
        }

        const handleScrollOrResize = () => {
            updateFloatingLabelPosition();
        };

        const scrollContainer = scrollContainerRef?.current;
        scrollContainer?.addEventListener("scroll", handleScrollOrResize, { passive: true });
        window.addEventListener("resize", handleScrollOrResize);
        window.addEventListener("scroll", handleScrollOrResize, true);

        return () => {
            scrollContainer?.removeEventListener("scroll", handleScrollOrResize);
            window.removeEventListener("resize", handleScrollOrResize);
            window.removeEventListener("scroll", handleScrollOrResize, true);
        };
    }, [collapsed, floatingLabel, scrollContainerRef, updateFloatingLabelPosition]);

    if (!items.length) return null;

    return (
        <>
            <nav className={`space-y-1.5 ${collapsed ? "text-[11px]" : "text-sm"}`}>
                {items.map((item) => {
                    const active = item.key && item.key === activeKey;
                    const Icon = item.icon;
                    const handleMouseEnter = collapsed
                        ? (event: React.MouseEvent<HTMLAnchorElement>) =>
                              showFloatingLabel(event.currentTarget, item.label)
                        : undefined;
                    const handleFocus = collapsed
                        ? (event: React.FocusEvent<HTMLAnchorElement>) =>
                              showFloatingLabel(event.currentTarget, item.label)
                        : undefined;
                    const handleMouseLeave = collapsed ? hideFloatingLabel : undefined;
                    const handleBlur = collapsed ? hideFloatingLabel : undefined;
                    return (
                        <Link
                            key={item.href}
                            href={item.href}
                            aria-label={collapsed ? item.label : undefined}
                            className={`group relative flex items-center rounded-xl border border-transparent px-3 py-2 transition ${
                                collapsed ? "overflow-visible" : "overflow-hidden"
                            } ${
                                collapsed ? "justify-center" : "gap-3"
                            } ${
                                active
                                    ? "bg-primary-soft text-primary shadow-[0_0_0_1px_rgba(16,163,127,0.2)]"
                                    : "text-muted hover:border-soft hover:text-neutral hover:bg-soft-surface"
                            }`}
                            title={item.label}
                            onClick={() => onNavigate?.()}
                            aria-current={active ? "page" : undefined}
                            onMouseEnter={handleMouseEnter}
                            onFocus={handleFocus}
                            onMouseLeave={handleMouseLeave}
                            onBlur={handleBlur}
                        >
                            {active && (
                                <span
                                    aria-hidden
                                    className={`absolute left-2 top-1/2 h-7 w-1 -translate-y-1/2 rounded-full bg-accent ${
                                        collapsed ? "left-1 h-6" : ""
                                    }`}
                                />
                            )}
                            {Icon ? (
                                <span
                                    aria-hidden
                                    className={`relative z-10 inline-flex items-center justify-center rounded-lg transition ${
                                        collapsed
                                            ? "h-12 w-12 bg-soft-surface"
                                            : "h-10 w-10 bg-soft-surface group-hover:bg-primary-soft"
                                    } ${active ? "text-primary" : "text-muted group-hover:text-neutral"}`}
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
                                        active
                                            ? "text-primary"
                                            : "text-subtle group-hover:text-neutral"
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
                        </Link>
                    );
                })}
            </nav>
            {collapsed && floatingLabel
                ? createPortal(
                      <span
                          className={`${collapsedFloatingTooltipClass} opacity-100`}
                          style={{
                              top: `${floatingLabel.top}px`,
                              left: `${floatingLabel.left}px`,
                          }}
                      >
                          {floatingLabel.label}
                      </span>,
                      document.body
                  )
                : null}
        </>
    );
};

const SidebarContent = ({
    collapsed,
    navItems,
    activeKey,
    isAuthenticated,
    authUser,
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
    isAuthenticated: boolean;
    authUser: AuthUser | null;
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
    const [accountMenuOpen, setAccountMenuOpen] = useState(false);
    const accountMenuRef = useRef<HTMLDivElement | null>(null);
    const navScrollRef = useRef<HTMLDivElement | null>(null);
    const toggleHoveredElementRef = useRef<HTMLElement | null>(null);
    const [toggleFloatingLabel, setToggleFloatingLabel] = useState<{
        label: string;
        top: number;
        left: number;
    } | null>(null);
    const { theme, setTheme, isReady: isThemeReady } = useTheme();

    const hideToggleFloatingLabel = useCallback(() => {
        setToggleFloatingLabel(null);
        toggleHoveredElementRef.current = null;
    }, []);

    const updateToggleFloatingLabelPosition = useCallback(() => {
        const element = toggleHoveredElementRef.current;
        if (!element) {
            return;
        }
        const rect = element.getBoundingClientRect();
        setToggleFloatingLabel((prev) => {
            if (!prev) {
                return null;
            }
            return {
                ...prev,
                top: rect.top + rect.height / 2,
                left: rect.right + 12,
            };
        });
    }, []);

    const showToggleFloatingLabel = useCallback(
        (element: HTMLElement) => {
            toggleHoveredElementRef.current = element;
            const rect = element.getBoundingClientRect();
            setToggleFloatingLabel({
                label: collapseToggleLabel,
                top: rect.top + rect.height / 2,
                left: rect.right + 12,
            });
        },
        [collapseToggleLabel]
    );

    useEffect(() => {
        if (!toggleFloatingLabel) {
            return;
        }

        const element = toggleHoveredElementRef.current;
        if (!element || !document.body.contains(element)) {
            hideToggleFloatingLabel();
            return;
        }

        updateToggleFloatingLabelPosition();
    }, [collapsed, toggleFloatingLabel, hideToggleFloatingLabel, updateToggleFloatingLabelPosition]);

    useEffect(() => {
        if (!toggleFloatingLabel) {
            return;
        }

        const handleScrollOrResize = () => {
            updateToggleFloatingLabelPosition();
        };

        window.addEventListener("resize", handleScrollOrResize);
        window.addEventListener("scroll", handleScrollOrResize, true);

        return () => {
            window.removeEventListener("resize", handleScrollOrResize);
            window.removeEventListener("scroll", handleScrollOrResize, true);
        };
    }, [toggleFloatingLabel, updateToggleFloatingLabelPosition]);

    useEffect(() => {
        if (!toggleFloatingLabel) {
            return;
        }

        setToggleFloatingLabel((prev) => {
            if (!prev || prev.label === collapseToggleLabel) {
                return prev;
            }
            return {
                ...prev,
                label: collapseToggleLabel,
            };
        });
    }, [collapseToggleLabel, toggleFloatingLabel]);

    useEffect(() => {
        if (!accountMenuOpen) {
            return;
        }

        const handleClickOutside = (event: MouseEvent) => {
            if (accountMenuRef.current && !accountMenuRef.current.contains(event.target as Node)) {
                setAccountMenuOpen(false);
            }
        };

        const handleKeyDown = (event: KeyboardEvent) => {
            if (event.key === "Escape") {
                setAccountMenuOpen(false);
            }
        };

        document.addEventListener("mousedown", handleClickOutside);
        document.addEventListener("keydown", handleKeyDown);

        return () => {
            document.removeEventListener("mousedown", handleClickOutside);
            document.removeEventListener("keydown", handleKeyDown);
        };
    }, [accountMenuOpen]);

    useEffect(() => {
        if (!isAuthenticated && accountMenuOpen) {
            setAccountMenuOpen(false);
        }
    }, [isAuthenticated, accountMenuOpen]);

    const accountMenuPositionClass = collapsed
        ? "left-0 bottom-full mb-3"
        : "right-0 bottom-full mb-3";
    const accountMenuBaseClass =
        "absolute z-20 flex flex-col gap-2 rounded-2xl border border-soft bg-primary-soft p-3 shadow-brand-elevated";
    const accountMenuClassName = collapsed
        ? `${accountMenuBaseClass} ${accountMenuPositionClass} min-w-[260px]`
        : `${accountMenuBaseClass} ${accountMenuPositionClass} w-full`;
    const themeOptions: { value: ThemeMode; label: string }[] = [
        { value: "light", label: "Tryb jasny" },
        { value: "dark", label: "Tryb ciemny" },
    ];
    const renderBrandBadge = () => (
        <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-accent-gradient text-sm font-semibold text-white">
            GA
        </div>
    );
    const renderExpandedHeader = () => (
        <div className="flex w-full items-center gap-3">
            <div className="group relative">{renderBrandBadge()}</div>
            <div className="leading-tight">
                <p className="text-[11px] font-semibold uppercase tracking-[0.28em] text-muted">
                    GPW Analytics
                </p>
                <p className="text-base font-semibold text-neutral">Panel demo</p>
            </div>
        </div>
    );
    const renderExpandedToggle = () => (
        <button
            type="button"
            onClick={onToggleCollapse}
            className="group relative flex h-10 w-10 items-center justify-center rounded-xl text-muted transition focus:outline-none focus-visible:ring-2 focus-visible:ring-[rgba(16,163,127,0.35)] focus-visible:ring-offset-2 focus-visible:ring-offset-primary-strong hover:shadow-[0_0_0_1px_rgba(15,23,42,0.12)] active:shadow-[0_0_0_1px_rgba(15,23,42,0.12)]"
            aria-label={collapseToggleLabel}
            aria-expanded={!collapsed}
            onMouseEnter={(event) => showToggleFloatingLabel(event.currentTarget)}
            onFocus={(event) => showToggleFloatingLabel(event.currentTarget)}
            onMouseLeave={hideToggleFloatingLabel}
            onBlur={hideToggleFloatingLabel}
        >
            <SidebarToggleGlyph className="h-[1.625rem] w-[1.625rem] text-neutral" />
        </button>
    );
    const headerAlignment = collapsed
        ? "justify-center"
        : onToggleCollapse
        ? "justify-between"
        : "justify-start";
    return (
        <div className="flex h-full flex-col bg-primary-strong text-neutral">
            {toggleFloatingLabel
                ? createPortal(
                      <span
                          className={`${collapsedFloatingTooltipClass} opacity-100`}
                          style={{
                              top: `${toggleFloatingLabel.top}px`,
                              left: `${toggleFloatingLabel.left}px`,
                          }}
                      >
                          {toggleFloatingLabel.label}
                      </span>,
                      document.body
                  )
                : null}
            <div className={`${sectionPadding} ${headerSpacing} pt-6`}>
                <div className={`flex items-center ${headerAlignment} gap-3`}>
                    {collapsed ? (
                        onToggleCollapse ? (
                            <button
                                type="button"
                                onClick={onToggleCollapse}
                                className="group relative flex h-12 w-12 items-center justify-center rounded-xl bg-accent-gradient text-sm font-semibold text-white transition focus:outline-none focus-visible:ring-2 focus-visible:ring-white/40 focus-visible:ring-offset-2 focus-visible:ring-offset-primary-strong"
                                aria-label={collapseToggleLabel}
                                aria-expanded={!collapsed}
                                onMouseEnter={(event) => showToggleFloatingLabel(event.currentTarget)}
                                onFocus={(event) => showToggleFloatingLabel(event.currentTarget)}
                                onMouseLeave={hideToggleFloatingLabel}
                                onBlur={hideToggleFloatingLabel}
                            >
                                <span className="pointer-events-none select-none transition-opacity duration-150 group-hover:opacity-0 group-focus-visible:opacity-0">
                                    GA
                                </span>
                                <span className="pointer-events-none absolute inset-0 flex items-center justify-center opacity-0 transition-opacity duration-150 group-hover:opacity-100 group-focus-visible:opacity-100">
                                    <SidebarToggleGlyph className="h-[1.625rem] w-[1.625rem] text-white" />
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
            </div>
            <div
                ref={navScrollRef}
                className={`flex-1 overflow-y-auto overflow-x-visible pb-6 ${sectionPadding} ${navSpacing}`}
            >
                <div className="space-y-3">
                    {!collapsed && (
                        <p className="text-[11px] font-semibold uppercase tracking-[0.32em] text-muted">
                            Nawigacja
                        </p>
                    )}
                    <SidebarNav
                        items={navItems}
                        activeKey={activeKey}
                        collapsed={collapsed}
                        onNavigate={onNavigate}
                        scrollContainerRef={navScrollRef}
                    />
                </div>
            </div>
            <div
                className={`border-t border-soft ${sectionPadding} ${
                    collapsed ? "py-5" : "py-6"
                } text-sm`}
            >
                {isAuthenticated ? (
                    <div ref={accountMenuRef} className="relative">
                        <button
                            type="button"
                            onClick={() => setAccountMenuOpen((prev) => !prev)}
                            className={`group w-full rounded-2xl border border-soft bg-soft-surface text-left transition hover:border-primary hover:bg-primary-soft focus:outline-none focus-visible:ring-0 focus-visible:ring-offset-0 disabled:cursor-not-allowed disabled:opacity-60 ${
                                collapsed ? "p-2" : "px-4 py-3"
                            }`}
                            aria-haspopup="menu"
                            aria-expanded={accountMenuOpen}
                            disabled={authLoading}
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
                                        className="h-10 w-10 rounded-full border border-soft object-cover"
                                        unoptimized
                                    />
                                ) : (
                                    <div className="flex h-10 w-10 items-center justify-center rounded-full border border-soft bg-soft-surface text-sm font-semibold">
                                        {(authUser?.name ?? authUser?.email ?? "U").charAt(0).toUpperCase()}
                                    </div>
                                )}
                                {!collapsed && (
                                    <div className="flex-1">
                                        <p className="font-semibold text-neutral">
                                            {authUser?.name ?? authUser?.email ?? "Użytkownik Google"}
                                        </p>
                                        {authUser?.email ? <span className="sr-only">{authUser.email}</span> : null}
                                    </div>
                                )}
                                <span className="sr-only">Otwórz panel konta</span>
                            </div>
                        </button>
                        {accountMenuOpen && (
                            <div className={accountMenuClassName} role="menu">
                                {authUser?.email ? (
                                    <div className="rounded-xl bg-soft-surface px-3 py-2">
                                        <p className="text-[11px] font-semibold uppercase tracking-[0.28em] text-muted">
                                            Zalogowano jako
                                        </p>
                                        <p className="mt-1 break-all text-sm font-semibold text-neutral">{authUser.email}</p>
                                    </div>
                                ) : null}
                                <div className="mt-2 space-y-3">
                                    <div className="space-y-1">
                                        <button
                                            type="button"
                                            className="w-full rounded-xl px-3 py-2 text-left text-sm font-semibold text-neutral transition hover:bg-primary-soft focus:outline-none focus-visible:ring-2 focus-visible:ring-[rgba(16,163,127,0.35)] focus-visible:ring-offset-2 focus-visible:ring-offset-primary-soft"
                                            onClick={() => setAccountMenuOpen(false)}
                                        >
                                            Rozszerz plan
                                        </button>
                                    </div>
                                    <div className="rounded-xl border border-soft bg-soft-surface p-3">
                                        <p className="text-[11px] font-semibold uppercase tracking-[0.28em] text-muted">
                                            Wygląd
                                        </p>
                                        <div className="mt-2 grid grid-cols-2 gap-2">
                                            {themeOptions.map(({ value, label }) => {
                                                const isActive = theme === value;
                                                const buttonClasses = `w-full rounded-xl border px-3 py-2 text-left text-sm font-semibold transition focus:outline-none focus-visible:ring-2 focus-visible:ring-[rgba(16,163,127,0.35)] focus-visible:ring-offset-2 focus-visible:ring-offset-primary-soft ${
                                                    isActive
                                                        ? "border-primary bg-primary-soft text-primary"
                                                        : "border-soft text-muted hover:border-primary hover:bg-primary-soft hover:text-neutral"
                                                }`;
                                                return (
                                                    <button
                                                        key={value}
                                                        type="button"
                                                        className={buttonClasses}
                                                        onClick={() => setTheme(value)}
                                                        aria-pressed={isActive}
                                                        disabled={!isThemeReady}
                                                    >
                                                        {label}
                                                    </button>
                                                );
                                            })}
                                        </div>
                                    </div>
                                    <button
                                        type="button"
                                        className="w-full rounded-xl px-3 py-2 text-left text-sm font-semibold text-neutral transition hover:bg-primary-soft focus:outline-none focus-visible:ring-2 focus-visible:ring-rose-200/80 focus-visible:ring-offset-2 focus-visible:ring-offset-primary-soft disabled:cursor-not-allowed disabled:opacity-60"
                                        onClick={() => {
                                            setAccountMenuOpen(false);
                                            handleLogout();
                                        }}
                                        disabled={authLoading}
                                    >
                                        Wyloguj się
                                    </button>
                                </div>
                            </div>
                        )}
                    </div>
                ) : (
                    <div
                        className={`space-y-3 ${
                            collapsed ? "text-center text-[11px]" : "text-sm"
                        } text-muted`}
                    >
                        <button
                            className={`w-full rounded-2xl border border-soft bg-soft-surface font-semibold text-neutral transition hover:border-primary hover:bg-primary-soft disabled:cursor-not-allowed disabled:opacity-60 ${
                                collapsed ? "py-2" : "px-4 py-3"
                            }`}
                            onClick={() => openAuthDialog("login")}
                            disabled={authLoading}
                        >
                            {collapsed ? "Zaloguj" : "Zaloguj się"}
                        </button>
                        <button
                            className={`w-full rounded-2xl bg-surface font-semibold text-neutral shadow transition hover:bg-soft-surface disabled:cursor-not-allowed disabled:opacity-60 ${
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
                                Ustaw zmienną NEXT_PUBLIC_GOOGLE_CLIENT_ID (lub GOOGLE_CLIENT_ID), aby włączyć logowanie.
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
    title,
}: {
    active?: boolean;
    onClick?: () => void;
    children: React.ReactNode;
    className?: string;
    title?: string;
}) => (
    <button
        onClick={onClick}
        title={title}
        className={`inline-flex items-center justify-center rounded-full border px-3 py-1.5 text-sm font-medium transition ${
            active
                ? "border-primary bg-primary-glow text-primary shadow-primary-glow"
                : "border-soft bg-surface text-muted hover:border-primary hover:text-primary hover:shadow-primary-glow"
        } ${className ?? ""}`}
    >
        {children}
    </button>
);

const inputBaseClasses =
    "rounded-xl border border-soft bg-surface px-3 py-2 text-neutral transition-colors focus:outline-none focus:border-[var(--color-primary)] focus:ring-2 focus:ring-[rgba(16,163,127,0.25)] disabled:cursor-not-allowed disabled:opacity-60";

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
        { key: "fees", label: "Koszt transakcji", format: (v) => formatNumber(v, 2) },
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
                                        className="group w-full rounded-2xl border border-soft bg-surface p-5 text-left shadow-sm transition hover:-translate-y-0.5 hover:border-[var(--color-primary)] hover:shadow-lg focus:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-primary)]/40"
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
                    <div className="relative z-50 max-h-[90vh] w-full max-w-5xl overflow-hidden rounded-3xl bg-surface p-6 shadow-2xl">
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

type MetricTone = "positive" | "negative" | "neutral" | "primary";

const METRIC_TONE_PILL_STYLES: Record<MetricTone, string> = {
    positive: "bg-[rgba(46,204,113,0.15)] text-accent",
    negative: "bg-[rgba(231,76,60,0.18)] text-negative",
    neutral: "bg-soft-surface text-neutral",
    primary: "bg-[rgba(10,35,66,0.12)] text-primary",
};

const prettifyMetricLabel = (raw: string) =>
    raw
        .replace(/[_-]+/g, " ")
        .trim()
        .replace(/\s+/g, " ")
        .replace(/\b\w/g, (char) => char.toUpperCase());

const normalizeMetricKey = (value: string) =>
    value.replace(/[^a-z0-9]+/gi, "").toLowerCase();

const toCamel = (value: string) =>
    value.replace(/_([a-z0-9])/gi, (_, char: string) => char.toUpperCase());

const toSnake = (value: string) =>
    value
        .replace(/([A-Z])/g, "_$1")
        .replace(/__+/g, "_")
        .toLowerCase();

const buildNormalizedMap = (metrics: Record<string, number> | undefined) => {
    const map = new Map<string, number>();
    if (!metrics) return map;
    Object.entries(metrics).forEach(([key, value]) => {
        if (typeof value !== "number" || Number.isNaN(value)) return;
        map.set(normalizeMetricKey(key), value);
    });
    return map;
};

const findValue = (map: Map<string, number>, aliases: string[]) => {
    for (const alias of aliases) {
        const normalized = normalizeMetricKey(alias);
        if (!normalized) continue;
        if (map.has(normalized)) {
            return map.get(normalized);
        }
    }
    return undefined;
};

const resolvePriceChangePercent = (
    map: Map<string, number>,
    key: string
): number | undefined => {
    const camel = toCamel(key);
    const snake = toSnake(key);
    const normalizedKey = normalizeMetricKey(key);
    const direct = findValue(map, [key, camel, snake, normalizedKey]);
    if (typeof direct === "number") {
        return direct;
    }
    const diff = findValue(map, [
        `${key}Diff`,
        `${key}_diff`,
        `${camel}Diff`,
        `${snake}_diff`,
        `${normalizedKey}diff`,
        `${normalizedKey}value`,
        "pricechangediff",
        "pricechangevalue",
    ]);
    const priceNow = findValue(map, [
        "priceNow",
        "price_now",
        "lastPrice",
        "last_price",
        "close",
        "closePrice",
        "price",
    ]);
    if (typeof diff === "number" && typeof priceNow === "number") {
        const priceThen = priceNow - diff;
        if (Math.abs(priceThen) > 1e-9) {
            return (priceNow / priceThen - 1) * 100;
        }
    }
    return undefined;
};

const formatMetricNumber = (value: number) => {
    const magnitude = Math.abs(value);
    if (magnitude >= 1000) {
        return value.toLocaleString("pl-PL", {
            maximumFractionDigits: 0,
        });
    }
    if (magnitude >= 100) {
        return value.toLocaleString("pl-PL", {
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
        });
    }
    if (magnitude >= 10) {
        return value.toLocaleString("pl-PL", {
            minimumFractionDigits: 1,
            maximumFractionDigits: 1,
        });
    }
    return value.toLocaleString("pl-PL", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });
};

const shouldHighlightPositiveMetric = (key: string) => {
    const normalized = key.replace(/[_-]+/g, " ").toLowerCase();
    return /return|growth|momentum|profit|yield|performance|alpha|cagr|roi|gain|trend|stopa/.test(
        normalized
    );
};

function ScoreRankingTable({ rows }: { rows: ScorePreviewRow[] }) {
    if (!rows.length) return null;

    const metricKeys = Array.from(
        new Set(rows.flatMap((row) => Object.keys(row.metrics ?? {})))
    ).slice(0, 4);

    const buildMetricDisplay = (
        row: ScorePreviewRow,
        key: string,
        metricMap: Map<string, number>
    ) => {
        let tone: MetricTone = "neutral";
        let text = "—";
        let indicator: "up" | "down" | undefined;

        if (/price[_-]?change/i.test(key)) {
            const pct = resolvePriceChangePercent(metricMap, key);
            if (typeof pct === "number") {
                const digits = Math.abs(pct) >= 100 ? 0 : 2;
                tone = pct > 0 ? "positive" : pct < 0 ? "negative" : "neutral";
                text = formatPct(pct, digits);
                indicator = pct > 0 ? "up" : pct < 0 ? "down" : undefined;
            }
        } else {
            const normalizedKey = normalizeMetricKey(key);
            const fromMap = metricMap.get(normalizedKey);
            const direct = row.metrics?.[key];
            const numeric =
                typeof direct === "number" && Number.isFinite(direct)
                    ? direct
                    : typeof fromMap === "number"
                        ? fromMap
                        : undefined;
            if (typeof numeric === "number") {
                tone =
                    numeric < 0
                        ? "negative"
                        : shouldHighlightPositiveMetric(key)
                            ? "positive"
                            : "neutral";
                text = formatMetricNumber(numeric);
            }
        }

        return { tone, text, indicator };
    };

    const renderPill = (value: React.ReactNode, tone: MetricTone = "neutral") => {
        if (value === null || value === undefined) {
            return <span className="text-subtle">—</span>;
        }

        if (typeof value === "string") {
            const trimmed = value.trim();
            if (!trimmed || trimmed === "—") {
                return <span className="text-subtle">—</span>;
            }
        }

        return (
            <span
                className={`inline-flex items-center gap-1 rounded-lg px-2 py-1 text-sm font-medium ${METRIC_TONE_PILL_STYLES[tone]}`}
            >
                {value}
            </span>
        );
    };

    return (
        <div className="overflow-x-auto rounded-xl border border-soft bg-surface">
            <table className="min-w-full text-sm">
                <thead className="bg-soft-surface text-left text-xs font-semibold uppercase tracking-wide text-muted">
                    <tr className="border-b border-soft">
                        <th className="px-4 py-3 text-center">#</th>
                        <th className="px-4 py-3">Spółka</th>
                        <th className="px-4 py-3">Score</th>
                        <th className="px-4 py-3">Waga</th>
                        {metricKeys.map((key) => (
                            <th key={key} className="px-4 py-3">
                                {prettifyMetricLabel(key)}
                            </th>
                        ))}
                    </tr>
                </thead>
                <tbody className="divide-y divide-soft">
                    {rows.map((row, idx) => {
                            const metricMap = buildNormalizedMap(row.metrics);
                            const baseRank = row.rank ?? idx + 1;
                            const scoreValue =
                                typeof row.score === "number"
                                    ? row.score.toFixed(2)
                                    : "—";
                            const weightValue =
                                typeof row.weight === "number"
                                    ? formatPercent(row.weight)
                                    : "—";
                            return (
                                <tr
                                    key={`${row.symbol}-${idx}`}
                                    className="bg-surface"
                                >
                                    <td className="px-4 py-3 text-center text-sm font-semibold text-primary">
                                        {baseRank}
                                    </td>
                                    <td className="px-4 py-3">
                                        <div className="text-sm font-semibold text-primary">
                                            {row.symbol}
                                        </div>
                                        {row.name && (
                                            <div className="text-xs text-subtle">{row.name}</div>
                                        )}
                                    </td>
                                    <td className="px-4 py-3">
                                        {renderPill(scoreValue, "primary")}
                                    </td>
                                    <td className="px-4 py-3">
                                        {renderPill(weightValue)}
                                    </td>
                                    {metricKeys.map((key) => {
                                        const { tone, text, indicator } = buildMetricDisplay(
                                            row,
                                            key,
                                            metricMap
                                        );
                                        const hasValue = text !== "—";
                                        const content = hasValue ? (
                                            <>
                                                {indicator === "up" && (
                                                    <span className="text-[10px] leading-none">▲</span>
                                                )}
                                                {indicator === "down" && (
                                                    <span className="text-[10px] leading-none">▼</span>
                                                )}
                                                <span>{text}</span>
                                            </>
                                        ) : (
                                            "—"
                                        );

                                        return (
                                            <td key={key} className="px-4 py-3">
                                                {renderPill(content, tone)}
                                            </td>
                                        );
                                    })}
                                </tr>
                            );
                        })}
                </tbody>
            </table>
        </div>
    );
}

function ScoreMissingTable({ items }: { items: ScorePreviewMissingRow[] }) {
    if (!items.length) return null;

    return (
        <div className="mt-6">
            <div className="text-sm font-medium text-subtle mb-2">
                Spółki bez obliczonego score
            </div>
            <div className="overflow-x-auto">
                <table className="min-w-full text-sm">
                    <thead className="text-left text-subtle">
                        <tr className="border-b border-soft">
                            <th className="py-2 pr-4 font-medium">Spółka</th>
                            <th className="py-2 pr-4 font-medium">Powód</th>
                        </tr>
                    </thead>
                    <tbody>
                        {items.map((item) => {
                            const key = item.raw ?? item.symbol;
                            return (
                                <tr key={key} className="border-b border-soft last:border-b-0">
                                    <td className="py-2 pr-4 font-medium">
                                        {item.symbol || item.raw || "—"}
                                    </td>
                                    <td className="py-2 pr-4 text-subtle">{item.reason}</td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

function Watchlist({
    items,
    current,
    onPick,
    onRemove,
    displayNames,
    snapshots,
    group,
}: {
    items: string[];
    current: string | null;
    onPick: (s: string) => void;
    onRemove: (s: string) => void;
    displayNames?: Record<string, string>;
    snapshots?: Record<string, WatchSnapshot>;
    group?: WatchlistGroup;
}) {
    const badgeLabel =
        group === "wishlist" ? "Na radarze" : group === "index" ? "Benchmark" : "Inwestycja";
    const badgeClasses =
        group === "wishlist"
            ? "border-amber-200 bg-amber-50 text-amber-600"
            : group === "index"
            ? "border-indigo-200 bg-indigo-50 text-indigo-600"
            : "border-emerald-200 bg-emerald-50 text-emerald-600";

    const formatValue = (value: number | null | undefined): string | null => {
        if (typeof value !== "number" || Number.isNaN(value) || !Number.isFinite(value)) {
            return null;
        }
        return value.toLocaleString("pl-PL", {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
        });
    };

    if (!items.length) {
        return (
            <div className="rounded-2xl border border-dashed border-soft bg-white/60 p-6 text-sm text-subtle">
                Dodaj spółkę powyżej, aby zbudować własną listę obserwacyjną.
            </div>
        );
    }

    return (
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
            {items.map((s) => {
                const label = displayNames?.[s] ?? s;
                const snapshot = snapshots?.[s];
                const change = snapshot?.change ?? null;
                const changePct = snapshot?.changePct ?? null;
                const formattedPrice = formatValue(snapshot?.latestPrice ?? null);
                const formattedChange =
                    typeof change === "number" && Number.isFinite(change)
                        ? formatValue(Math.abs(change))
                        : null;
                const formattedChangePct =
                    typeof changePct === "number" && Number.isFinite(changePct)
                        ? formatValue(Math.abs(changePct))
                        : null;
                const direction =
                    typeof change === "number" && Number.isFinite(change)
                        ? change > 0
                            ? "up"
                            : change < 0
                            ? "down"
                            : "flat"
                        : null;
                const changeClass =
                    direction === "up"
                        ? "text-emerald-600"
                        : direction === "down"
                        ? "text-rose-600"
                        : "text-subtle";
                const changeLabel = formattedChange
                    ? `${direction === "down" ? "−" : direction === "up" ? "+" : ""}${formattedChange} zł${
                          formattedChangePct ? ` (${formattedChangePct}%)` : ""
                      }`
                    : "Brak zmian";
                const priceLabel = formattedPrice ? `${formattedPrice} zł` : "Brak danych";
                return (
                    <button
                        key={s}
                        type="button"
                        onClick={() => onPick(s)}
                        className={`group relative flex w-full flex-col rounded-2xl border bg-white/95 p-4 text-left shadow-[0_18px_42px_rgba(15,23,42,0.12)] transition-all duration-200 focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-soft focus-visible:ring-offset-2 focus-visible:ring-offset-primary-soft ${
                            s === current
                                ? "-translate-y-0.5 border-primary shadow-primary-glow"
                                : "border-soft hover:-translate-y-1 hover:border-primary hover:shadow-primary-glow"
                        }`}
                        aria-pressed={s === current}
                    >
                        <div className="flex items-start justify-between gap-3">
                            <div>
                                <p className="text-xs font-semibold uppercase tracking-[0.32em] text-subtle">
                                    Symbol
                                </p>
                                <div className="mt-2 text-lg font-semibold text-neutral">
                                    {label}
                                    {label !== s ? (
                                        <span className="ml-2 text-sm font-medium text-subtle">{s}</span>
                                    ) : null}
                                </div>
                            </div>
                            <span
                                className={`inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-semibold tracking-wide ${badgeClasses}`}
                            >
                                {badgeLabel}
                            </span>
                        </div>
                        <div className="mt-6 flex items-end justify-between gap-4">
                            <div>
                                <p className="text-xs font-semibold uppercase tracking-wide text-subtle">
                                    Kurs
                                </p>
                                <p className="mt-1 text-2xl font-semibold text-neutral">{priceLabel}</p>
                            </div>
                            <div className={`text-right text-sm font-semibold ${changeClass}`}>
                                {changeLabel}
                            </div>
                        </div>
                        <div className="mt-6 flex items-center justify-between text-xs text-subtle">
                            <span>{snapshot?.kind === "index" ? "Indeks GPW" : "Akcje GPW"}</span>
                            <button
                                type="button"
                                onClick={(event) => {
                                    event.stopPropagation();
                                    onRemove(s);
                                }}
                                className="inline-flex items-center gap-1 rounded-full border border-transparent px-2 py-1 font-semibold text-subtle transition hover:border-rose-200 hover:bg-rose-50 hover:text-rose-600 focus:outline-none focus-visible:ring-2 focus-visible:ring-rose-400/40 focus-visible:ring-offset-2 focus-visible:ring-offset-white"
                                aria-label={`Usuń ${label} z listy`}
                            >
                                Usuń
                            </button>
                        </div>
                    </button>
                );
            })}
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
const tickerDisplayPattern = /^[0-9A-Z][0-9A-Z._-]{0,15}$/;

const normalizeUpper = (value: string | null | undefined): string =>
    typeof value === "string" ? value.trim().toUpperCase() : "";

const getPreferredDisplayForRow = (row: SymbolRow): string => {
    const symbolUpper = normalizeUpper(row.symbol);
    const rawUpper = normalizeUpper(row.raw);
    const candidates = [row.display, row.short_name, row.name, row.company_name, row.full_name];
    for (const candidate of candidates) {
        if (typeof candidate !== "string") continue;
        const trimmed = candidate.trim();
        if (!trimmed) continue;
        const normalizedCandidate = trimmed.toUpperCase();
        if (normalizedCandidate === symbolUpper || normalizedCandidate === rawUpper) {
            continue;
        }
        if (!tickerDisplayPattern.test(normalizedCandidate)) {
            continue;
        }
        return trimmed;
    }
    return row.symbol;
};

const extractDisplayName = (symbol: string, meta?: SymbolRow): string | null => {
    if (!meta) return null;
    const preferred = getPreferredDisplayForRow(meta);
    if (!preferred) return null;
    const normalizedPreferred = normalizeUpper(preferred);
    const normalizedSymbol = normalizeUpper(symbol);
    if (normalizedPreferred === normalizedSymbol) {
        return null;
    }
    const normalizedTicker = normalizeUpper(meta.ticker);
    if (normalizedTicker && normalizedPreferred === normalizedTicker) {
        return null;
    }
    return preferred;
};

function TickerAutosuggest({
    onPick,
    placeholder = "Dodaj symbol (np. CDR.WA)",
    inputClassName = "",
    disabled = false,
    allowedKinds,
    autoFocus = false,
    allowFreeEntry = false,
}: {
    onPick: (symbol: string, meta?: SymbolRow) => void;
    placeholder?: string;
    inputClassName?: string;
    disabled?: boolean;
    allowedKinds?: SymbolKind[];
    autoFocus?: boolean;
    allowFreeEntry?: boolean;
}) {
    const [q, setQ] = useState("");
    const [list, setList] = useState<SymbolRow[]>([]);
    const [open, setOpen] = useState(false);
    const [idx, setIdx] = useState(-1);
    const [loading, setLoading] = useState(false);

    const allowedKindsSignature = (allowedKinds ?? [])
        .filter((kind): kind is SymbolKind => kind === "stock" || kind === "index")
        .join("|");
    const allowedKindsList = useMemo<SymbolKind[]>(() => {
        if (!allowedKindsSignature) {
            return ["stock"];
        }
        const seen = new Set<SymbolKind>();
        const result: SymbolKind[] = [];
        for (const raw of allowedKindsSignature.split("|")) {
            const kind = raw === "index" ? "index" : "stock";
            if (!seen.has(kind)) {
                seen.add(kind);
                result.push(kind);
            }
        }
        return result.length ? result : ["stock"];
    }, [allowedKindsSignature]);

    const containerRef = useRef<HTMLDivElement | null>(null);

    useEffect(() => {
        if (disabled) {
            setList([]);
            setOpen(false);
            setIdx(-1);
            return;
        }

        const query = q.trim();
        if (!query) {
            setList([]);
            setOpen(false);
            setIdx(-1);
            return;
        }

        let cancelled = false;
        setLoading(true);
        const handle = setTimeout(async () => {
            try {
                const rows = await searchSymbols(query, allowedKindsList);
                if (cancelled) return;
                const allowedSet = new Set(allowedKindsList);
                const filtered = rows.filter((row) => allowedSet.has(row.kind));
                setList(filtered);
                setOpen(true);
                setIdx(filtered.length ? 0 : -1);
            } catch {
                if (!cancelled) {
                    setList([]);
                    setIdx(-1);
                }
            } finally {
                if (!cancelled) {
                    setLoading(false);
                }
            }
        }, 200);

        return () => {
            cancelled = true;
            clearTimeout(handle);
        };
    }, [allowedKindsList, disabled, q]);

    useEffect(() => {
        if (!disabled) return;
        setQ("");
    }, [disabled]);

    useEffect(() => {
        if (!open) return;
        const onClickOutside = (event: MouseEvent) => {
            if (!containerRef.current) return;
            if (containerRef.current.contains(event.target as Node)) return;
            setOpen(false);
        };
        document.addEventListener("mousedown", onClickOutside);
        return () => {
            document.removeEventListener("mousedown", onClickOutside);
        };
    }, [open]);

    const choose = useCallback(
        (row: SymbolRow) => {
            onPick(row.symbol, row);
            setQ("");
            setList([]);
            setOpen(false);
            setIdx(-1);
        },
        [onPick]
    );

    const onKeyDown = useCallback(
        (e: React.KeyboardEvent<HTMLInputElement>) => {
            if (e.key === "Enter") {
                if (idx >= 0 && idx < list.length) {
                    e.preventDefault();
                    choose(list[idx]);
                    return;
                }
                if (allowFreeEntry) {
                    const normalized = e.currentTarget.value.trim().toUpperCase();
                    if (normalized && tickerDisplayPattern.test(normalized)) {
                        e.preventDefault();
                        onPick(normalized);
                        setQ("");
                        setList([]);
                        setOpen(false);
                        setIdx(-1);
                    }
                }
                return;
            }
            if (!open || !list.length) return;
            if (e.key === "ArrowDown") {
                e.preventDefault();
                setIdx((i) => Math.min(i + 1, list.length - 1));
            } else if (e.key === "ArrowUp") {
                e.preventDefault();
                setIdx((i) => Math.max(i - 1, 0));
            } else if (e.key === "Escape") {
                setOpen(false);
            }
        },
        [allowFreeEntry, choose, idx, list, onPick, open]
    );

    useEffect(() => {
        if (!open) return;
        if (idx >= list.length) {
            setIdx(list.length ? list.length - 1 : -1);
        }
    }, [idx, list.length, open]);

    return (
        <div className="relative" ref={containerRef}>
            <input
                value={q}
                onChange={(e) => setQ(e.target.value)}
                onFocus={() => !disabled && list.length && setOpen(true)}
                onKeyDown={onKeyDown}
                onBlur={() => setIdx((current) => (current >= list.length ? list.length - 1 : current))}
                placeholder={placeholder}
                disabled={disabled}
                aria-disabled={disabled}
                autoFocus={autoFocus}
                className={[
                    inputBaseClasses,
                    inputClassName || "w-56",
                ]
                    .filter(Boolean)
                    .join(" ")}
            />
            {open && (
                <div className="absolute z-20 mt-1 w-full max-h-72 overflow-auto rounded-xl border border-soft bg-surface shadow-lg">
                    {loading && (
                        <div className="px-3 py-2 text-sm text-subtle">Szukam…</div>
                    )}
                    {!loading && list.length === 0 && (
                        <div className="px-3 py-2 text-sm text-subtle">Brak wyników</div>
                    )}
                    {!loading &&
                        list.map((row, i) => {
                            const isActive = i === idx;
                            const displaySymbol = getPreferredDisplayForRow(row);
                            const displayUpper = normalizeUpper(displaySymbol);
                            const symbolUpper = normalizeUpper(row.symbol);
                            const kindLabel = row.kind === "index" ? "Indeks" : "Spółka";
                            const metadataEntries: Array<{ label: string; value: string }> = [];

                            if (row.kind === "stock") {
                                const rawEntries: Array<[string, string | null | undefined]> = [
                                    ["Symbol", row.ticker ?? row.raw ?? null],
                                    ["Kod", row.code],
                                    ["ISIN", row.isin],
                                    ["Nazwa", row.name],
                                    ["Nazwa spółki", row.company_name],
                                    ["Pełna nazwa", row.full_name],
                                    ["Skrót", row.short_name],
                                ];
                                const seen = new Set<string>();
                                for (const [label, value] of rawEntries) {
                                    if (!value) continue;
                                    const trimmed = value.trim();
                                    if (!trimmed) continue;
                                    const dedupeKey = trimmed.toUpperCase();
                                    if (seen.has(dedupeKey)) continue;
                                    seen.add(dedupeKey);
                                    metadataEntries.push({ label, value: trimmed });
                                }
                            }

                            const secondaryDetails: string[] = [];
                            if (displayUpper !== symbolUpper) {
                                secondaryDetails.push(row.symbol);
                            }
                            const addDetail = (value?: string | null) => {
                                if (typeof value !== "string") return;
                                const trimmed = value.trim();
                                if (!trimmed) return;
                                const normalized = normalizeUpper(trimmed);
                                if (normalized === displayUpper || normalized === symbolUpper) return;
                                if (
                                    secondaryDetails.some(
                                        (detail) => normalizeUpper(detail) === normalized
                                    )
                                ) {
                                    return;
                                }
                                secondaryDetails.push(trimmed);
                            };
                            addDetail(row.name);
                            addDetail(row.raw);

                            return (
                                <button
                                    key={`${row.kind}-${row.symbol}`}
                                    type="button"
                                    onMouseDown={(event) => {
                                        event.preventDefault();
                                        choose(row);
                                    }}
                                    className={[
                                        "w-full px-3 py-2 text-left text-sm transition",
                                        isActive ? "bg-[#E3ECF5]" : "hover:bg-[#EEF3F7]",
                                    ]
                                        .filter(Boolean)
                                        .join(" ")}
                                >
                                    <div className="flex flex-col gap-2">
                                        <div className="flex items-start justify-between gap-3">
                                            <div>
                                                <div className="font-semibold text-primary">
                                                    {displaySymbol}
                                                </div>
                                                {secondaryDetails.map((detail) => (
                                                    <div
                                                        key={`${row.symbol}-${detail}`}
                                                        className="text-xs text-muted"
                                                    >
                                                        {detail}
                                                    </div>
                                                ))}
                                            </div>
                                            <span className="rounded-full border border-soft px-2 py-0.5 text-[11px] uppercase tracking-wide text-subtle">
                                                {kindLabel}
                                            </span>
                                        </div>
                                        {metadataEntries.length > 0 && (
                                            <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-[11px] text-subtle sm:grid-cols-3">
                                                {metadataEntries.map(({ label, value }) => (
                                                    <React.Fragment key={`${row.symbol}-${label}`}>
                                                        <dt className="uppercase tracking-wide text-[10px]">{label}</dt>
                                                        <dd className="text-right font-medium text-primary">{value}</dd>
                                                    </React.Fragment>
                                                ))}
                                            </dl>
                                        )}
                                    </div>
                                </button>
                            );
                        })}
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
                                {...CHART_BRUSH_COMMON_PROPS}
                                dataKey="date"
                                height={30}
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

type MetricRulePreviewProps = {
    rule: ScoreBuilderRule;
    metricOption?: ScoreMetricOption;
    lookbackValue: number;
    asOf?: string | null;
    onLookbackChange?: (value: number) => void;
};

const computePreviewStartISO = (period: number | "max"): string => {
    if (period === "max") {
        return "1990-01-01";
    }
    const startDate = new Date(Date.now() - period * DAY_MS);
    return startDate.toISOString().slice(0, 10);
};

type ChartWindowRange = { startIndex: number; endIndex: number };

function InfoHint({ text }: { text: string }) {
    const tooltipId = useId();
    return (
        <span className="group relative inline-flex">
            <span
                tabIndex={0}
                aria-describedby={tooltipId}
                aria-label={text}
                className="inline-flex h-4 w-4 cursor-help items-center justify-center rounded-full border border-soft text-[10px] font-semibold text-muted transition group-hover:border-primary group-hover:text-primary group-focus-visible:border-primary group-focus-visible:text-primary"
            >
                i
            </span>
            <span
                role="tooltip"
                id={tooltipId}
                className="pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-64 -translate-x-1/2 rounded-lg bg-neutral px-3 py-2 text-[11px] font-medium text-white opacity-0 shadow-lg transition group-hover:block group-hover:translate-y-1 group-hover:opacity-100 group-focus-visible:block group-focus-visible:translate-y-1 group-focus-visible:opacity-100"
            >
                {text}
            </span>
        </span>
    );
}

const safeParseDate = (value: string | null | undefined): Date | null => {
    if (typeof value !== "string" || !value.trim()) {
        return null;
    }
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date;
};

const computeWindowStartIndex = (
    rows: Row[],
    endIndex: number,
    lookbackDays: number
): number => {
    if (!rows.length) {
        return 0;
    }
    const total = rows.length;
    const safeEnd = Math.max(0, Math.min(endIndex, total - 1));
    if (!Number.isFinite(lookbackDays) || lookbackDays <= 0) {
        return safeEnd;
    }
    const endDate = safeParseDate(rows[safeEnd]?.date);
    if (!endDate) {
        return Math.max(0, safeEnd - lookbackDays + 1);
    }
    const threshold = endDate.getTime() - lookbackDays * DAY_MS;
    let start = safeEnd;
    for (let i = safeEnd; i >= 0; i -= 1) {
        const currentDate = safeParseDate(rows[i]?.date);
        if (!currentDate) {
            start = i;
            continue;
        }
        start = i;
        if (currentDate.getTime() <= threshold) {
            start = Math.min(safeEnd, i + 1);
            break;
        }
    }
    return start;
};

const findEndIndexForDate = (
    rows: Row[],
    isoDate: string | null | undefined
): number | null => {
    const target = safeParseDate(isoDate);
    if (!target) {
        return null;
    }
    for (let i = rows.length - 1; i >= 0; i -= 1) {
        const rowDate = safeParseDate(rows[i]?.date);
        if (!rowDate) {
            continue;
        }
        if (rowDate.getTime() <= target.getTime()) {
            return i;
        }
    }
    return null;
};

const computeWindowDurationDays = (
    rows: Row[],
    range: ChartWindowRange | null
): number | null => {
    if (!range) {
        return null;
    }
    const startRow = rows[range.startIndex];
    const endRow = rows[range.endIndex];
    if (!startRow || !endRow) {
        return null;
    }
    const startDate = safeParseDate(startRow.date);
    const endDate = safeParseDate(endRow.date);
    if (!startDate || !endDate) {
        return null;
    }
    const diff = endDate.getTime() - startDate.getTime();
    return diff < 0 ? 0 : Math.round(diff / DAY_MS);
};

function MetricRulePreview({
    rule,
    metricOption,
    lookbackValue,
    asOf,
    onLookbackChange,
}: MetricRulePreviewProps) {
    const chartGradientId = useId();
    const [selectedSymbol, setSelectedSymbol] = useState<string>(() => DEFAULT_METRIC_PREVIEW_SYMBOL);
    const [selectedSymbolMeta, setSelectedSymbolMeta] = useState<SymbolRow | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [missingReason, setMissingReason] = useState<string | null>(null);
    const [preview, setPreview] = useState<{
        rawValue: number | null;
        score: number | null;
        metricKey: string;
    } | null>(null);
    const [previewOpen, setPreviewOpen] = useState(false);
    const [chartRows, setChartRows] = useState<Row[]>([]);
    const [chartLoading, setChartLoading] = useState(false);
    const [chartError, setChartError] = useState<string | null>(null);
    const [windowRange, setWindowRange] = useState<ChartWindowRange | null>(null);
    const [manualWindowRange, setManualWindowRange] = useState<ChartWindowRange | null>(null);
    const chartRowsRef = useRef<Row[]>([]);
    const lastFetchParamsRef = useRef<{ symbol: string; startISO: string } | null>(null);

    const sanitizedRule = useMemo(() => {
        const weightNumeric = Number(rule.weight);
        const normalizedWeight =
            Number.isFinite(weightNumeric) && weightNumeric > 0 ? weightNumeric : 1;
        return {
            ...rule,
            weight: normalizedWeight,
            lookbackDays: lookbackValue,
        };
    }, [rule, lookbackValue]);

    const component = useMemo(() => {
        const [single] = buildScoreComponents([sanitizedRule]);
        return single ?? null;
    }, [sanitizedRule]);

    const percentBased = useMemo(
        () =>
            metricOption
                ? metricOption.backendMetric === "price_change" ||
                  PERCENT_BASED_SCORE_METRICS.has(metricOption.backendMetric)
                : false,
        [metricOption]
    );

    const handleSymbolPick = useCallback(
        (symbol: string, meta?: SymbolRow) => {
            const normalized = symbol.trim().toUpperCase();
            if (!normalized) return;
            setSelectedSymbol(normalized);
            setSelectedSymbolMeta(meta ?? null);
        },
        []
    );

    const effectiveAsOf = useMemo(() => {
        if (windowRange && chartRows[windowRange.endIndex]) {
            return chartRows[windowRange.endIndex].date ?? null;
        }
        const normalized = typeof asOf === "string" ? asOf.trim() : "";
        return normalized || null;
    }, [windowRange, chartRows, asOf]);

    useEffect(() => {
        if (!component) {
            setPreview(null);
            return;
        }
        const normalizedSymbol = selectedSymbol.trim().toUpperCase();
        if (!normalizedSymbol) {
            setPreview(null);
            return;
        }
        const payload = toScorePreviewRulePayload(component);
        let cancelled = false;
        const controller = new AbortController();
        const timer = window.setTimeout(() => {
            setLoading(true);
            setError(null);
            setMissingReason(null);
            previewScoreRanking(
                {
                    rules: [payload],
                    limit: 1,
                    universe: normalizedSymbol,
                    sort: component.direction === "asc" ? "asc" : "desc",
                    as_of: effectiveAsOf ?? undefined,
                },
                { signal: controller.signal }
            )
                .then((result) => {
                    if (cancelled) return;
                    const missingEntry = result.missing.find(
                        (item) => item.symbol?.toUpperCase() === normalizedSymbol
                    );
                    if (missingEntry) {
                        setPreview(null);
                        setMissingReason(
                            missingEntry.reason ?? "Brak danych dla wskazanej spółki."
                        );
                        return;
                    }
                    const row =
                        result.rows.find(
                            (entry) => entry.symbol?.toUpperCase() === normalizedSymbol
                        ) ?? result.rows[0];
                    if (!row) {
                        setPreview(null);
                        setMissingReason("Brak wyników dla wskazanej spółki.");
                        return;
                    }
                    const metricValue = extractMetricValueFromRow(
                        row,
                        component,
                        rule,
                        metricOption
                    );
                    const scoreValue = typeof row.score === "number" ? row.score : null;
                    setPreview({
                        rawValue: typeof metricValue === "number" ? metricValue : null,
                        score: scoreValue,
                        metricKey: payload.metric,
                    });
                })
                .catch((err: unknown) => {
                    if (cancelled) return;
                    if (err instanceof DOMException && err.name === "AbortError") {
                        return;
                    }
                    setError(resolveErrorMessage(err, "Nie udało się obliczyć metryki."));
                    setPreview(null);
                })
                .finally(() => {
                    if (!cancelled) {
                        setLoading(false);
                    }
                });
        }, 200);
        return () => {
            cancelled = true;
            window.clearTimeout(timer);
            controller.abort();
        };
    }, [component, selectedSymbol, effectiveAsOf, metricOption, rule]);

    useEffect(() => {
        chartRowsRef.current = chartRows;
    }, [chartRows]);

    const previousSymbolRef = useRef<string | null>(null);

    useEffect(() => {
        const normalizedSymbol = selectedSymbol.trim().toUpperCase();
        if (!normalizedSymbol) {
            setChartRows([]);
            setChartError(null);
            setWindowRange(null);
            setManualWindowRange(null);
            chartRowsRef.current = [];
            lastFetchParamsRef.current = null;
            previousSymbolRef.current = null;
            setChartLoading(false);
            return;
        }

        const symbolChanged = previousSymbolRef.current !== normalizedSymbol;
        previousSymbolRef.current = normalizedSymbol;

        if (symbolChanged) {
            setChartRows([]);
            setWindowRange(null);
            setManualWindowRange(null);
            chartRowsRef.current = [];
        }

        const fallbackDays =
            Number.isFinite(lookbackValue) && lookbackValue > 0
                ? Math.max(Math.floor(lookbackValue * 6), 365)
                : 365;
        const startISO = computePreviewStartISO(fallbackDays);
        const desiredStartDate = safeParseDate(startISO);
        const existingRows = chartRowsRef.current;
        const earliestExistingDate = safeParseDate(existingRows[0]?.date);
        const lastParams = lastFetchParamsRef.current;
        const needsMoreHistory =
            !earliestExistingDate ||
            !desiredStartDate ||
            earliestExistingDate.getTime() > desiredStartDate.getTime();
        if (!symbolChanged && lastParams && !needsMoreHistory && lastParams.startISO <= startISO) {
            return;
        }

        let cancelled = false;
        setChartLoading(true);
        setChartError(null);
        fetchQuotes(normalizedSymbol, startISO)
            .then((rows) => {
                if (cancelled) return;
                setChartRows(rows);
                chartRowsRef.current = rows;
                lastFetchParamsRef.current = { symbol: normalizedSymbol, startISO };
            })
            .catch((err: unknown) => {
                if (cancelled) return;
                setChartRows([]);
                setWindowRange(null);
                setManualWindowRange(null);
                setChartError(resolveErrorMessage(err, "Nie udało się pobrać danych cenowych."));
                lastFetchParamsRef.current = null;
            })
            .finally(() => {
                if (!cancelled) {
                    setChartLoading(false);
                }
            });
        return () => {
            cancelled = true;
        };
    }, [selectedSymbol, lookbackValue]);

    useEffect(() => {
        if (!chartRows.length) {
            setWindowRange(null);
            setManualWindowRange(null);
            return;
        }
        const total = chartRows.length;
        const manualRange = (() => {
            if (!manualWindowRange) return null;
            const rawStart = Math.max(0, Math.min(manualWindowRange.startIndex, total - 1));
            const rawEnd = Math.max(0, Math.min(manualWindowRange.endIndex, total - 1));
            return {
                startIndex: Math.min(rawStart, rawEnd),
                endIndex: Math.max(rawStart, rawEnd),
            };
        })();
        const targetEnd = manualRange
            ? manualRange.endIndex
            : findEndIndexForDate(chartRows, asOf) ?? total - 1;
        const safeEnd = Math.max(0, Math.min(targetEnd, total - 1));
        const startIndex = manualRange
            ? manualRange.startIndex
            : computeWindowStartIndex(chartRows, safeEnd, lookbackValue);
        const normalizedRange = {
            startIndex: Math.min(startIndex, safeEnd),
            endIndex: Math.max(startIndex, safeEnd),
        };
        setWindowRange((current) => {
            if (
                current &&
                current.startIndex === normalizedRange.startIndex &&
                current.endIndex === normalizedRange.endIndex
            ) {
                return current;
            }
            return normalizedRange;
        });
        if (manualRange) {
            setManualWindowRange((current) => {
                if (!current) return null;
                if (
                    current.startIndex === normalizedRange.startIndex &&
                    current.endIndex === normalizedRange.endIndex
                ) {
                    return current;
                }
                return normalizedRange;
            });
        }
    }, [chartRows, lookbackValue, asOf, manualWindowRange]);

    useEffect(() => {
        setManualWindowRange(null);
    }, [selectedSymbol, asOf]);

    useEffect(() => {
        if (!manualWindowRange) return;
        const manualDuration = computeWindowDurationDays(chartRows, manualWindowRange);
        if (
            manualDuration == null ||
            !Number.isFinite(lookbackValue) ||
            Math.abs(manualDuration - lookbackValue) > 1
        ) {
            setManualWindowRange(null);
        }
    }, [lookbackValue, chartRows, manualWindowRange]);

    useEffect(() => {
        setMissingReason(null);
    }, [selectedSymbol, lookbackValue]);

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

    const brushDateFormatter = useMemo(
        () =>
            new Intl.DateTimeFormat("pl-PL", {
                year: "numeric",
                month: "short",
            }),
        []
    );

    const axisTickFormatter = useCallback(
        (value: string | number) => {
            if (typeof value !== "string" && typeof value !== "number") {
                return "";
            }
            const date = new Date(value);
            if (Number.isNaN(date.getTime())) {
                return "";
            }
            return axisDateFormatter.format(date);
        },
        [axisDateFormatter]
    );

    const brushTickFormatter = useCallback(
        (value: string | number) => {
            if (typeof value !== "string" && typeof value !== "number") {
                return "";
            }
            const date = new Date(value);
            if (Number.isNaN(date.getTime())) {
                return "";
            }
            return brushDateFormatter.format(date);
        },
        [brushDateFormatter]
    );

    const windowRows = useMemo(() => {
        if (!windowRange) return [];
        const total = chartRows.length;
        if (!total) return [];
        const start = Math.max(0, Math.min(windowRange.startIndex, total - 1));
        const end = Math.max(start, Math.min(windowRange.endIndex, total - 1));
        return chartRows.slice(start, end + 1);
    }, [chartRows, windowRange]);

    const chartData = useMemo<PriceChartPoint[]>(() => {
        if (!windowRows.length) return [];
        const base = windowRows[0]?.close ?? 0;
        return windowRows.map((row) => {
            const change = row.close - base;
            const changePct = base !== 0 ? (change / base) * 100 : 0;
            return { ...row, change, changePct, sma: null };
        });
    }, [windowRows]);

    const brushData = useMemo<PriceChartPoint[]>(() => {
        if (!chartRows.length) return [];
        const base = chartRows[0]?.close ?? 0;
        return chartRows.map((row) => {
            const change = row.close - base;
            const changePct = base !== 0 ? (change / base) * 100 : 0;
            return { ...row, change, changePct, sma: null };
        });
    }, [chartRows]);

    const chartStats = useMemo(() => {
        if (!chartData.length) return null;
        const first = chartData[0];
        const last = chartData[chartData.length - 1];
        if (!first || !last) return null;
        const change = last.close - first.close;
        const changePct = first.close !== 0 ? (change / first.close) * 100 : 0;
        return {
            change,
            changePct,
            latestPrice: last.close,
        };
    }, [chartData]);

    const chartChangeClass = useMemo(() => {
        if (!chartStats) return "text-subtle";
        if (Math.abs(chartStats.change) < 1e-10) {
            return "text-subtle";
        }
        return chartStats.change > 0 ? "text-accent" : "text-negative";
    }, [chartStats]);

    const chartChangeValue = useMemo(() => {
        if (!chartStats) return "—";
        if (!Number.isFinite(chartStats.change)) return "—";
        const absolute = priceFormatter.format(Math.abs(chartStats.change));
        if (Math.abs(chartStats.change) < 1e-10) {
            return priceFormatter.format(0);
        }
        const sign = chartStats.change > 0 ? "+" : "-";
        return `${sign}${absolute}`;
    }, [chartStats, priceFormatter]);

    const chartChangePct = useMemo(() => {
        if (!chartStats) return "—";
        if (!Number.isFinite(chartStats.changePct)) return "—";
        const value = chartStats.changePct;
        const absolute = percentFormatter.format(Math.abs(value));
        if (Math.abs(value) < 1e-10) {
            return `${percentFormatter.format(0)}%`;
        }
        const sign = value > 0 ? "+" : "-";
        return `${sign}${absolute}%`;
    }, [chartStats, percentFormatter]);

    const chartLatestPrice = useMemo(() => {
        if (!chartData.length) return "—";
        const last = chartData[chartData.length - 1];
        if (!last) return "—";
        return priceFormatter.format(last.close);
    }, [chartData, priceFormatter]);

    const chartPrimaryColor = chartStats && chartStats.change < 0 ? "#EA4335" : "#1DB954";
    const chartStrokeColor = chartStats && chartStats.change < 0 ? "#C5221F" : "#0B8F47";

    const metricRange = useMemo(() => {
        if (!component) return null;
        if (component.scoring?.type === "linear_clamped") {
            const { worst, best } = component.scoring;
            const min = Math.min(worst, best);
            const max = Math.max(worst, best);
            return { min, max } as const;
        }
        if (
            typeof component.min_value === "number" &&
            typeof component.max_value === "number"
        ) {
            const min = Math.min(component.min_value, component.max_value);
            const max = Math.max(component.min_value, component.max_value);
            return { min, max } as const;
        }
        return null;
    }, [component]);

    const pointerPosition = useMemo(() => {
        if (!metricRange || preview?.rawValue == null) return null;
        const lower = Math.min(metricRange.min, metricRange.max);
        const upper = Math.max(metricRange.min, metricRange.max);
        if (!Number.isFinite(lower) || !Number.isFinite(upper) || upper === lower) {
            return null;
        }
        const clamped = Math.min(Math.max(preview.rawValue, lower), upper);
        return ((clamped - lower) / (upper - lower)) * 100;
    }, [metricRange, preview]);

    const normalizedPercent = useMemo(() => {
        if (preview?.score == null || Number.isNaN(preview.score)) {
            return null;
        }
        const clamped = Math.max(0, Math.min(1, preview.score));
        return clamped * 100;
    }, [preview]);

    const formatRawValue = useCallback(
        (value: number) => {
            if (!Number.isFinite(value)) {
                return "—";
            }
            if (percentBased) {
                return `${value.toFixed(2)}%`;
            }
            const fractionDigits = Math.abs(value) >= 100 ? 1 : 2;
            return value.toLocaleString("pl-PL", {
                minimumFractionDigits: fractionDigits,
                maximumFractionDigits: fractionDigits,
            });
        },
        [percentBased]
    );

    const windowDurationDays = useMemo(
        () => computeWindowDurationDays(chartRows, windowRange),
        [chartRows, windowRange]
    );

    const windowStartDate =
        windowRange && chartRows[windowRange.startIndex]
            ? chartRows[windowRange.startIndex].date ?? null
            : null;
    const windowEndDate =
        windowRange && chartRows[windowRange.endIndex]
            ? chartRows[windowRange.endIndex].date ?? null
            : null;

    const handleBrushSelectionChange = useCallback(
        (range: BrushStartEndIndex) => {
            if (!chartRows.length) {
                return;
            }
            const total = chartRows.length;
            const rawStart =
                typeof range.startIndex === "number" ? range.startIndex : range.endIndex ?? 0;
            const rawEnd =
                typeof range.endIndex === "number" ? range.endIndex : range.startIndex ?? 0;
            const clampedStart = Math.max(0, Math.min(rawStart, total - 1));
            const clampedEnd = Math.max(0, Math.min(rawEnd, total - 1));
            const nextStart = Math.min(clampedStart, clampedEnd);
            const nextEnd = Math.max(clampedStart, clampedEnd);
            const nextRange: ChartWindowRange = { startIndex: nextStart, endIndex: nextEnd };
            setWindowRange((current) => {
                if (current && current.startIndex === nextStart && current.endIndex === nextEnd) {
                    return current;
                }
                return nextRange;
            });
            setManualWindowRange((current) => {
                if (current && current.startIndex === nextStart && current.endIndex === nextEnd) {
                    return current;
                }
                return nextRange;
            });
            setMissingReason(null);
            if (onLookbackChange) {
                const duration = computeWindowDurationDays(chartRows, nextRange);
                if (duration != null && Number.isFinite(duration) && duration > 0) {
                    onLookbackChange(duration);
                } else if (nextEnd > nextStart) {
                    onLookbackChange(nextEnd - nextStart);
                }
            }
        },
        [chartRows, onLookbackChange]
    );

    const selectedDisplayLabel = useMemo(() => {
        const base = selectedSymbol || DEFAULT_METRIC_PREVIEW_SYMBOL;
        const display = extractDisplayName(base, selectedSymbolMeta ?? undefined);
        return display ? `${base} — ${display}` : base;
    }, [selectedSymbol, selectedSymbolMeta]);

    const lookbackTarget =
        Number.isFinite(lookbackValue) && lookbackValue > 0 ? Math.floor(lookbackValue) : null;

    const sliderAvailable = brushData.length > 1 && Boolean(windowRange);

    return (
        <div className="space-y-3 rounded-2xl border border-soft bg-surface p-4">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                <div>
                    <div className="text-sm font-medium text-primary">Podgląd metryki</div>
                    <div className="text-xs text-subtle">
                        Aktualna wartość dla wybranej spółki – aktualizuje się wraz ze zmianą
                        parametrów.
                    </div>
                </div>
                <div className="flex items-center gap-2">
                    {loading ? <span className="text-[11px] text-muted">Ładowanie…</span> : null}
                    <button
                        type="button"
                        onClick={() => setPreviewOpen((prev) => !prev)}
                        className="rounded-xl border border-soft px-3 py-2 text-xs font-medium text-primary transition hover:border-[var(--color-tech)] hover:text-[var(--color-tech)]"
                    >
                        {previewOpen ? "Zamknij podgląd metryki" : "Pokaż podgląd metryki"}
                    </button>
                </div>
            </div>
            {previewOpen ? (
                <div className="space-y-4 pt-1">
                    <div className="grid gap-4 md:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
                        <div className="flex flex-col gap-2">
                            <span className="text-xs uppercase tracking-wide text-muted">Spółka</span>
                            <TickerAutosuggest
                                onPick={handleSymbolPick}
                                allowedKinds={["stock"]}
                                allowFreeEntry
                                placeholder="Dodaj symbol (np. CDR.WA)"
                                inputClassName="w-full"
                            />
                            <span className="text-[11px] text-subtle">
                                Wpisz pierwszy znak, aby zobaczyć podpowiedzi z bazy GPW.
                            </span>
                            <div className="text-sm text-neutral">
                                Wybrana spółka: {" "}
                                <span className="font-semibold text-primary">{selectedDisplayLabel}</span>
                            </div>
                        </div>
                        {metricOption ? (
                            <div className="rounded-xl bg-soft-surface px-3 py-2 text-xs text-subtle">
                                <div className="font-semibold text-neutral">{metricOption.label}</div>
                                <div>
                                    Zakres: {lookbackValue} dni • Kierunek: {" "}
                                    {rule.direction === "asc" ? "mniej = lepiej" : "więcej = lepiej"}
                                </div>
                                {metricOption.description ? (
                                    <div className="mt-1">{metricOption.description}</div>
                                ) : null}
                            </div>
                        ) : (
                            <div className="rounded-xl bg-soft-surface px-3 py-2 text-xs text-subtle">
                                Wybierz metrykę powyżej, aby zobaczyć podgląd wartości.
                            </div>
                        )}
                    </div>
                    {error ? (
                        <div className="rounded-xl border border-negative bg-negative/5 px-3 py-2 text-xs text-negative">
                            {error}
                        </div>
                    ) : null}
                    {missingReason ? (
                        <div className="rounded-xl border border-soft px-3 py-2 text-xs text-subtle">
                            {missingReason}
                        </div>
                    ) : null}
                    <div className="space-y-3">
                        <div className="space-y-4 rounded-2xl bg-soft-surface p-4">
                            <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,260px)]">
                                <div className="space-y-2">
                                    <div className="text-xs uppercase tracking-wide text-muted">
                                        Zmiana ceny w wybranym zakresie
                                    </div>
                                    <div className="text-2xl font-semibold text-neutral">
                                        {selectedDisplayLabel}
                                    </div>
                                    <div
                                        className={`flex flex-wrap items-baseline gap-2 text-sm ${chartChangeClass}`}
                                    >
                                        <span className="text-lg font-semibold">{chartChangePct}</span>
                                        <span>{chartChangeValue}</span>
                                        <span className="text-xs text-subtle">
                                            Ostatnia cena: {" "}
                                            <span className="font-semibold text-neutral">
                                                {chartLatestPrice}
                                            </span>
                                        </span>
                                    </div>
                                    <div className="text-xs text-subtle">
                                        Okres metryki: {" "}
                                        {windowDurationDays != null ? `${windowDurationDays} dni` : "—"}
                                        {lookbackTarget != null
                                            ? ` • docelowo ${lookbackTarget} dni`
                                            : null}
                                    </div>
                                    {windowStartDate && windowEndDate ? (
                                        <div className="text-xs text-subtle">
                                            Zakres dat: {windowStartDate} → {windowEndDate}
                                        </div>
                                    ) : null}
                                </div>
                                <div className="space-y-3">
                                    <div className="flex items-center justify-between text-xs uppercase tracking-wide text-muted">
                                        <span>Score</span>
                                        <span className="text-sm font-semibold text-primary">
                                            {preview?.score != null ? preview.score.toFixed(3) : "—"}
                                        </span>
                                    </div>
                                    <div className="relative h-2 overflow-hidden rounded-full bg-soft">
                                        <div
                                            className="absolute inset-y-0 left-0 rounded-full bg-primary transition-all"
                                            style={{ width: `${normalizedPercent ?? 0}%` }}
                                        />
                                    </div>
                                    <div className="flex justify-between text-[10px] text-subtle">
                                        <span>0</span>
                                        <span>1</span>
                                    </div>
                                    <div className="space-y-1">
                                        <div className="text-xs uppercase tracking-wide text-muted">
                                            Wartość metryki
                                        </div>
                                        {metricRange && preview?.rawValue != null ? (
                                            <>
                                                <div className="relative h-2 overflow-hidden rounded-full bg-soft">
                                                    <div
                                                        className="absolute inset-0"
                                                        style={{
                                                            background:
                                                                (component?.direction ?? "desc") === "asc"
                                                                    ? "linear-gradient(to right, #22C55E, #F97316, #EF4444)"
                                                                    : "linear-gradient(to right, #EF4444, #F97316, #22C55E)",
                                                        }}
                                                    />
                                                    {pointerPosition != null ? (
                                                        <div
                                                            className="absolute -top-1 bottom-[-1px] w-[2px] bg-primary"
                                                            style={{ left: `${pointerPosition}%` }}
                                                        />
                                                    ) : null}
                                                </div>
                                                <div className="flex justify-between text-[10px] text-subtle">
                                                    <span>{formatRawValue(metricRange.min)}</span>
                                                    <span>{formatRawValue(metricRange.max)}</span>
                                                </div>
                                                <div className="text-xs text-muted">
                                                    Aktualna wartość: {" "}
                                                    <span className="font-semibold text-primary">
                                                        {formatRawValue(preview.rawValue)}
                                                    </span>
                                                </div>
                                            </>
                                        ) : (
                                            <div className="rounded-xl border border-dashed border-soft px-3 py-2 text-xs text-subtle">
                                                Brak zdefiniowanej skali – pokazujemy jedynie wynik znormalizowany.
                                            </div>
                                        )}
                                    </div>
                                </div>
                            </div>
                            <div className="space-y-6">
                                <div className="relative h-64">
                                    {chartData.length ? (
                                        <ResponsiveContainer width="100%" height="100%">
                                            <AreaChart
                                                data={chartData}
                                                margin={{ top: 10, right: 16, left: 0, bottom: 0 }}
                                            >
                                                <defs>
                                                    <linearGradient id={chartGradientId} x1="0" y1="0" x2="0" y2="1">
                                                        <stop offset="0%" stopColor={chartPrimaryColor} stopOpacity={0.35} />
                                                        <stop offset="95%" stopColor={chartPrimaryColor} stopOpacity={0} />
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
                                                    tickFormatter={(value) => priceFormatter.format(value)}
                                                    domain={["auto", "auto"]}
                                                />
                                                <Tooltip<number, string>
                                                    cursor={{ stroke: chartStrokeColor, strokeOpacity: 0.2, strokeWidth: 1 }}
                                                    content={(tooltipProps) => (
                                                        <ChartTooltipContent
                                                            {...tooltipProps}
                                                            priceFormatter={priceFormatter}
                                                            percentFormatter={percentFormatter}
                                                            dateFormatter={tooltipDateFormatter}
                                                            showSMA={false}
                                                        />
                                                    )}
                                                    wrapperStyle={{ outline: "none" }}
                                                    position={{ y: 24 }}
                                                />
                                                <Area
                                                    type="monotone"
                                                    dataKey="close"
                                                    stroke={chartStrokeColor}
                                                    strokeWidth={2}
                                                    fill={`url(#${chartGradientId})`}
                                                    fillOpacity={1}
                                                    isAnimationActive={false}
                                                />
                                            </AreaChart>
                                        </ResponsiveContainer>
                                    ) : !chartLoading ? (
                                        <div className="flex h-full items-center justify-center text-xs text-subtle">
                                            Brak danych cenowych dla wybranego zakresu.
                                        </div>
                                    ) : null}
                                    {chartLoading ? (
                                        <div className="pointer-events-none absolute right-3 top-3 z-10 text-[11px] text-muted">
                                            Ładowanie wykresu…
                                        </div>
                                    ) : null}
                                </div>
                                <div className="space-y-2">
                                    <div className="relative h-32 rounded-2xl border border-soft bg-surface px-3 py-4">
                                        {sliderAvailable ? (
                                            <ResponsiveContainer width="100%" height="100%">
                                                <AreaChart
                                                    data={brushData}
                                                    margin={{ top: 8, right: 16, left: 0, bottom: 16 }}
                                                >
                                                    <XAxis
                                                        dataKey="date"
                                                        tickFormatter={brushTickFormatter}
                                                        tick={{ fontSize: 10, fill: "#64748B" }}
                                                        tickMargin={12}
                                                        axisLine={false}
                                                        tickLine={false}
                                                        minTickGap={32}
                                                    />
                                                    <YAxis hide domain={["auto", "auto"]} />
                                                    <Area
                                                        type="monotone"
                                                        dataKey="close"
                                                        stroke={chartStrokeColor}
                                                        fill={chartPrimaryColor}
                                                        fillOpacity={0.15}
                                                        isAnimationActive={false}
                                                        dot={false}
                                                    />
                                                    <Brush
                                                        {...CHART_BRUSH_COMMON_PROPS}
                                                        dataKey="date"
                                                        height={48}
                                                        startIndex={windowRange?.startIndex}
                                                        endIndex={windowRange?.endIndex}
                                                        onChange={handleBrushSelectionChange}
                                                        onDragEnd={handleBrushSelectionChange}
                                                    />
                                                </AreaChart>
                                            </ResponsiveContainer>
                                        ) : !chartLoading ? (
                                            <div className="flex h-full items-center justify-center text-xs text-subtle">
                                                Brak danych historycznych.
                                            </div>
                                        ) : null}
                                        {chartLoading ? (
                                            <div className="pointer-events-none absolute bottom-3 right-3 z-10 text-[11px] text-muted">
                                                Ładowanie wykresu…
                                            </div>
                                        ) : null}
                                    </div>
                                    {sliderAvailable ? (
                                        <div className="flex justify-between text-xs text-muted">
                                            <span>{windowStartDate ?? "—"}</span>
                                            <span>{windowEndDate ?? "—"}</span>
                                        </div>
                                    ) : null}
                                </div>
                            </div>
                            {chartError ? (
                                <div className="rounded-xl border border-negative bg-negative/5 px-3 py-2 text-xs text-negative">
                                    {chartError}
                                </div>
                            ) : null}
                        </div>
                        {!preview && !loading && !error ? (
                            <div className="rounded-2xl border border-dashed border-soft p-4 text-xs text-subtle">
                                {metricOption
                                    ? "Wprowadź symbol spółki, aby zobaczyć wynik metryki."
                                    : "Wybierz metrykę, aby zobaczyć podgląd."}
                            </div>
                        ) : null}
                    </div>
                </div>
            ) : null}
        </div>
    );
}
/** =========================
 *  Główny komponent dashboardu
 *  ========================= */
export function AnalyticsDashboard({ view }: AnalyticsDashboardProps) {
    const defaultScoreDraft = useMemo(() => getDefaultScoreDraft(), []);
    const defaultPortfolioDraft = useMemo(() => getDefaultPortfolioDraft(), []);

    const [authUser, setAuthUserState] = useState<AuthUser | null>(() => {
        if (typeof window === "undefined") {
            return null;
        }
        try {
            const stored = window.sessionStorage.getItem(AUTH_USER_STORAGE_KEY);
            if (!stored) {
                return null;
            }
            const parsed = JSON.parse(stored);
            return parsed && typeof parsed === "object" ? (parsed as AuthUser) : null;
        } catch {
            return null;
        }
    });
    const setAuthUser = useCallback((user: AuthUser | null) => {
        setAuthUserState(user);
        if (typeof window === "undefined") {
            return;
        }
        try {
            if (user) {
                window.sessionStorage.setItem(
                    AUTH_USER_STORAGE_KEY,
                    JSON.stringify(user)
                );
            } else {
                window.sessionStorage.removeItem(AUTH_USER_STORAGE_KEY);
            }
        } catch {
            // Ignoruj błędy zapisu w sessionStorage
        }
    }, []);
    const [isAdmin, setIsAdmin] = useState<boolean>(() => {
        if (typeof window === "undefined") {
            return false;
        }
        try {
            return window.sessionStorage.getItem(AUTH_ADMIN_STORAGE_KEY) === "1";
        } catch {
            return false;
        }
    });
    const setAdminFlag = useCallback(
        (flag: boolean) => {
            setIsAdmin(flag);
            if (typeof window === "undefined") {
                return;
            }
            try {
                if (flag) {
                    window.sessionStorage.setItem(AUTH_ADMIN_STORAGE_KEY, "1");
                } else {
                    window.sessionStorage.removeItem(AUTH_ADMIN_STORAGE_KEY);
                }
            } catch {
                // ignoruj błędy zapisu flagi
            }
        },
        [setIsAdmin]
    );
    const [authLoading, setAuthLoading] = useState(true);
    const [authError, setAuthError] = useState<string | null>(null);
    const [profileError, setProfileError] = useState<string | null>(null);
    const [profileHydrated, setProfileHydrated] = useState(false);
    const [googleLoaded, setGoogleLoaded] = useState(false);
    const googleInitializedRef = useRef(false);
    const googleInitModeRef = useRef<"popup" | "redirect" | null>(null);
    const lastSavedPreferencesRef = useRef<string | null>(null);
    const [authDialogOpen, setAuthDialogOpen] = useState(false);
    const [authDialogMode, setAuthDialogMode] = useState<"login" | "signup">("login");
    const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
    const [sidebarMobileOpen, setSidebarMobileOpen] = useState(false);

    const [symbolDisplayNames, setSymbolDisplayNames] = useState<Record<string, string>>({});
    const [watch, setWatch] = useState<string[]>(() => [...DEFAULT_WATCHLIST]);
    const [symbol, setSymbolState] = useState<string | null>(DEFAULT_WATCHLIST[0] ?? null);
    const activeSymbolLabel = symbol ? symbolDisplayNames[symbol] ?? symbol : null;
    const [period, setPeriod] = useState<ChartPeriod>(365);
    const [area, setArea] = useState(true);
    const [smaOn, setSmaOn] = useState(true);
    const [watchSnapshots, setWatchSnapshots] = useState<Record<string, WatchSnapshot>>({});
    const watchMetaRef = useRef<Record<string, SymbolKind>>({});
    const [watchlistGroup, setWatchlistGroup] = useState<WatchlistGroup>("owned");
    const [watchSort, setWatchSort] = useState<"custom" | "price" | "name">("custom");

    useEffect(() => {
        if (!watch.length) {
            setWatchSnapshots({});
            watchMetaRef.current = {};
            return;
        }
        let cancelled = false;
        const effectivePeriod = period === "max" ? 365 : period;
        const startISO = computeStartISOForPeriod(effectivePeriod);
        (async () => {
            const results = await Promise.all(
                watch.map(async (sym) => {
                    const normalized = sym.trim().toUpperCase();
                    if (!normalized) {
                        return { symbol: sym, snapshot: null as WatchSnapshot | null };
                    }
                    try {
                        const { rows: seriesRows, kind } = await fetchInstrumentSeries(
                            normalized,
                            watchMetaRef.current[normalized],
                            startISO
                        );
                        if (!seriesRows.length) {
                            return {
                                symbol: normalized,
                                snapshot: {
                                    latestPrice: null,
                                    change: null,
                                    changePct: null,
                                    kind,
                                } as WatchSnapshot,
                            };
                        }
                        const first = seriesRows[0];
                        const last = seriesRows[seriesRows.length - 1];
                        const change = last.close - first.close;
                        const changePct =
                            first.close !== 0 ? (change / first.close) * 100 : null;
                        return {
                            symbol: normalized,
                            snapshot: {
                                latestPrice: last.close ?? null,
                                change,
                                changePct,
                                kind,
                            } as WatchSnapshot,
                        };
                    } catch {
                        return { symbol: normalized, snapshot: null };
                    }
                })
            );
            if (cancelled) return;
            const latest = new Map(results.map((entry) => [entry.symbol, entry.snapshot]));
            setWatchSnapshots((prev) => {
                const next: Record<string, WatchSnapshot> = {};
                watch.forEach((sym) => {
                    const normalized = sym.trim().toUpperCase();
                    if (!normalized) return;
                    const snapshot = latest.get(normalized);
                    if (snapshot) {
                        next[normalized] = snapshot;
                    } else if (prev[normalized]) {
                        next[normalized] = prev[normalized];
                    }
                });
                return next;
            });
            const nextMeta: Record<string, SymbolKind> = { ...watchMetaRef.current };
            results.forEach((entry) => {
                if (entry.snapshot) {
                    nextMeta[entry.symbol] = entry.snapshot.kind;
                }
            });
            Object.keys(nextMeta).forEach((key) => {
                if (!watch.includes(key)) {
                    delete nextMeta[key];
                }
            });
            watchMetaRef.current = nextMeta;
        })();
        return () => {
            cancelled = true;
        };
    }, [watch, period]);

    const visibleWatchItems = useMemo(() => {
        if (!watch.length) {
            return [];
        }
        if (watchSort === "custom") {
            return [...watch];
        }
        const ordered = [...watch];
        if (watchSort === "price") {
            const toNumber = (value: number | null | undefined) =>
                typeof value === "number" && Number.isFinite(value)
                    ? value
                    : Number.NEGATIVE_INFINITY;
            ordered.sort((a, b) => {
                const bPrice = toNumber(watchSnapshots[b]?.latestPrice);
                const aPrice = toNumber(watchSnapshots[a]?.latestPrice);
                return bPrice - aPrice;
            });
        } else if (watchSort === "name") {
            ordered.sort((a, b) => {
                const aLabel = (symbolDisplayNames[a] ?? a).toUpperCase();
                const bLabel = (symbolDisplayNames[b] ?? b).toUpperCase();
                return aLabel.localeCompare(bLabel, "pl-PL");
            });
        }
        return ordered;
    }, [watch, watchSort, watchSnapshots, symbolDisplayNames]);

    const watchlistSegments = useMemo(
        () => [
            { value: "owned" as WatchlistGroup, label: "Posiadanie", count: watch.length },
            { value: "wishlist" as WatchlistGroup, label: "Do kupienia", count: 0 },
            {
                value: "index" as WatchlistGroup,
                label: "WIG",
                count: watch.filter((sym) => sym.toUpperCase().includes("WIG")).length,
            },
        ],
        [watch]
    );

    const activeWatchSegment = useMemo(
        () => watchlistSegments.find((segment) => segment.value === watchlistGroup),
        [watchlistSegments, watchlistGroup]
    );

    const watchSortLabel = useMemo(() => {
        if (watchSort === "price") {
            return "ceny";
        }
        if (watchSort === "name") {
            return "nazwy";
        }
        return "kolejności własnej";
    }, [watchSort]);

    const handleCycleWatchSort = useCallback(() => {
        setWatchSort((prev) => (prev === "custom" ? "price" : prev === "price" ? "name" : "custom"));
    }, []);

    const [rows, setRows] = useState<Row[]>([]);
    const [allRows, setAllRows] = useState<Row[]>([]);
    const [brushRange, setBrushRange] = useState<BrushStartEndIndex | null>(null);
    const [comparisonSymbols, setComparisonSymbols] = useState<string[]>([]);
    const [comparisonAllRows, setComparisonAllRows] = useState<Record<string, Row[]>>({});
    const [comparisonErrors, setComparisonErrors] = useState<Record<string, string>>({});
    const [comparisonMeta, setComparisonMeta] = useState<Record<string, ComparisonMeta>>({});
    const [loading, setLoading] = useState(false);
    const [err, setErr] = useState("");

    const setSymbol = (
        value: string | null | ((prev: string | null) => string | null)
    ) => {
        setSymbolState((prev) => {
            const resolved =
                typeof value === "function"
                    ? (value as (prev: string | null) => string | null)(prev)
                    : value;
            const normalized =
                resolved && resolved.trim().length > 0
                    ? resolved.trim().toUpperCase()
                    : null;
            return normalized === prev ? prev : normalized;
        });
    };

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
    const [benchmarkPortfolios, setBenchmarkPortfolios] = useState<GpwBenchmarkPortfolio[]>([]);
    const [customIndices, setCustomIndices] = useState<CustomIndexDefinition[]>(() => {
        if (typeof window === "undefined") {
            return [];
        }
        try {
            const stored = window.localStorage.getItem(CUSTOM_INDICES_STORAGE_KEY);
            if (!stored) {
                return [];
            }
            const parsed: unknown = JSON.parse(stored);
            if (!Array.isArray(parsed)) {
                return [];
            }
            const normalized: CustomIndexDefinition[] = [];
            parsed.forEach((item) => {
                if (!item || typeof item !== "object") {
                    return;
                }
                const codeRaw = (item as { code?: unknown }).code;
                if (typeof codeRaw !== "string") {
                    return;
                }
                const code = codeRaw.trim().toUpperCase().replace(/[^A-Z0-9_-]/g, "");
                if (!code) {
                    return;
                }
                const nameRaw = (item as { name?: unknown }).name;
                const nameValue =
                    typeof nameRaw === "string" ? nameRaw.trim() : undefined;

                const constituentsRaw = (item as { constituents?: unknown }).constituents;
                let parsedConstituents: CustomIndexConstituent[] = [];

                if (Array.isArray(constituentsRaw)) {
                    const dedup = new Map<string, number>();
                    constituentsRaw.forEach((entry) => {
                        if (!entry || typeof entry !== "object") {
                            return;
                        }
                        const symbolRaw = (entry as { symbol?: unknown }).symbol;
                        if (typeof symbolRaw !== "string") {
                            return;
                        }
                        const symbol = symbolRaw.trim().toUpperCase();
                        if (!symbol) {
                            return;
                        }
                        const weightRaw = (entry as { weightPct?: unknown }).weightPct;
                        const numericWeight =
                            typeof weightRaw === "number"
                                ? weightRaw
                                : typeof weightRaw === "string"
                                ? Number(weightRaw)
                                : 0;
                        if (Number.isFinite(numericWeight) && numericWeight > 0) {
                            dedup.set(symbol, (dedup.get(symbol) ?? 0) + numericWeight);
                        } else if (!dedup.has(symbol)) {
                            dedup.set(symbol, 0);
                        }
                    });
                    const entries = Array.from(dedup.entries()).slice(
                        0,
                        MAX_UNIVERSE_FALLBACK_SYMBOLS
                    );
                    if (entries.length) {
                        const weightSum = entries.reduce(
                            (sum, [, weight]) => sum + Math.max(weight, 0),
                            0
                        );
                        const scale = weightSum > 0 ? 100 / weightSum : 0;
                        parsedConstituents = entries.map(([symbol, weight]) => ({
                            symbol,
                            weightPct:
                                weightSum > 0
                                    ? Math.max(weight, 0) * scale
                                    : entries.length
                                    ? 100 / entries.length
                                    : 0,
                        }));
                    }
                }

                const symbolsRaw = (item as { symbols?: unknown }).symbols;
                if (!parsedConstituents.length && Array.isArray(symbolsRaw)) {
                    const uniqueSymbols = Array.from(
                        new Set(
                            symbolsRaw
                                .map((sym) =>
                                    typeof sym === "string" ? sym.trim().toUpperCase() : ""
                                )
                                .filter(Boolean)
                        )
                    ).slice(0, MAX_UNIVERSE_FALLBACK_SYMBOLS);
                    if (uniqueSymbols.length) {
                        const equalWeight = 100 / uniqueSymbols.length;
                        parsedConstituents = uniqueSymbols.map((symbol) => ({
                            symbol,
                            weightPct: equalWeight,
                        }));
                    }
                }

                if (!parsedConstituents.length) {
                    return;
                }

                const startDateRaw = (item as { startDate?: unknown }).startDate;
                let startDate = new Date().toISOString().slice(0, 10);
                if (typeof startDateRaw === "string" && startDateRaw.trim().length) {
                    const parsedStart = toUTCDate(startDateRaw.trim());
                    if (parsedStart) {
                        startDate = parsedStart.toISOString().slice(0, 10);
                    }
                }
                const baseValueRaw = (item as { baseValue?: unknown }).baseValue;
                const baseValue =
                    typeof baseValueRaw === "number" &&
                    Number.isFinite(baseValueRaw) &&
                    baseValueRaw > 0
                        ? baseValueRaw
                        : 100;

                const createdAtRaw = (item as { createdAt?: unknown }).createdAt;
                const updatedAtRaw = (item as { updatedAt?: unknown }).updatedAt;
                const createdAt =
                    typeof createdAtRaw === "string" && createdAtRaw.trim().length
                        ? createdAtRaw
                        : new Date().toISOString();
                const updatedAt =
                    typeof updatedAtRaw === "string" && updatedAtRaw.trim().length
                        ? updatedAtRaw
                        : createdAt;
                const idRaw = (item as { id?: unknown }).id;
                const id =
                    typeof idRaw === "string" && idRaw.trim().length
                        ? idRaw
                        : createCustomIndexId();

                const symbols = parsedConstituents
                    .map((entry) => entry.symbol)
                    .slice(0, MAX_UNIVERSE_FALLBACK_SYMBOLS);

                normalized.push({
                    id,
                    code,
                    name: nameValue && nameValue.length ? nameValue : undefined,
                    symbols,
                    constituents: parsedConstituents,
                    startDate,
                    baseValue,
                    createdAt,
                    updatedAt,
                });
            });
            return normalized;
        } catch {
            return [];
        }
    });
    const [customIndexFormOpen, setCustomIndexFormOpen] = useState(false);
    const [customIndexDraft, setCustomIndexDraft] = useState<CustomIndexDraft>(() => ({
        code: "",
        name: "",
        constituents: [createEmptyCustomIndexRow()],
        startDate: new Date().toISOString().slice(0, 10),
        baseValue: "100",
    }));
    const [customIndexError, setCustomIndexError] = useState<string | null>(null);
    const [benchmarkHistory, setBenchmarkHistory] = useState<
        Record<string, GpwBenchmarkHistorySeries>
    >({});
    const [customBenchmarkHistory, setCustomBenchmarkHistory] = useState<
        Record<string, GpwBenchmarkHistorySeries>
    >({});
    const [benchmarkChangePeriod, setBenchmarkChangePeriod] =
        useState<BenchmarkChangePeriod>("1D");
    const [benchmarkChangeMenuOpen, setBenchmarkChangeMenuOpen] = useState(false);
    const benchmarkChangeMenuRef = useRef<HTMLDivElement | null>(null);
    const [selectedBenchmarkCode, setSelectedBenchmarkCode] = useState<string | null>(null);
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

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }
        try {
            window.localStorage.setItem(
                CUSTOM_INDICES_STORAGE_KEY,
                JSON.stringify(customIndices)
            );
        } catch {
            // Ignorujemy błędy zapisu w localStorage.
        }
    }, [customIndices]);

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



    const benchmarkValueFormatter = useMemo(
        () => new Intl.NumberFormat("pl-PL", { maximumFractionDigits: 2 }),
        []
    );
    const benchmarkPercentFormatter = useMemo(
        () =>
            new Intl.NumberFormat("pl-PL", {
                style: "percent",
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
            }),
        []
    );
    const benchmarkWeightFormatter = useMemo(
        () =>
            new Intl.NumberFormat("pl-PL", {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
            }),
        []
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

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }
        const url = new URL(window.location.href);
        const authErrorParam = url.searchParams.get("auth_error");
        const authStatusParam = url.searchParams.get("auth");
        let shouldReplace = false;

        if (authErrorParam) {
            setAuthError(authErrorParam);
            setProfileError(authErrorParam);
            setAuthDialogMode("login");
            setAuthDialogOpen(true);
            setAuthLoading(false);
            url.searchParams.delete("auth_error");
            shouldReplace = true;
        }

        if (authStatusParam === "google_success") {
            setAuthDialogOpen(false);
            url.searchParams.delete("auth");
            shouldReplace = true;
        }

        if (shouldReplace) {
            const nextUrl = `${url.pathname}${url.search}${url.hash}`;
            window.history.replaceState({}, "", nextUrl);
        }
    }, [setAuthDialogMode, setAuthDialogOpen, setAuthError, setProfileError, setAuthLoading]);

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
        try {
            const response = await fetch("/api/account/profile", { cache: "no-store" });
            if (!response.ok) {
                if (response.status === 401) {
                    setAuthUser(null);
                    setAdminFlag(false);
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
            setAdminFlag(Boolean(data.isAdmin));
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
        }
    }, [hydrateFromPreferences, resetToDefaults, setAdminFlag, setAuthUser]);

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
        [fetchProfile, setAuthUser]
    );

    const initializeGoogle = useCallback(() => {
        if (!googleLoaded || !GOOGLE_CLIENT_ID) {
            return false;
        }
        if (typeof window === "undefined") {
            return false;
        }
        const googleApi = window.google?.accounts?.id;
        if (!googleApi) {
            return false;
        }
        const shouldRedirect = shouldUseGoogleRedirect();
        const desiredMode: "popup" | "redirect" = shouldRedirect ? "redirect" : "popup";
        if (googleInitializedRef.current && googleInitModeRef.current === desiredMode) {
            return true;
        }
        if (googleInitModeRef.current && googleInitModeRef.current !== desiredMode) {
            googleApi.disableAutoSelect?.();
        }
        const loginUri = shouldRedirect
            ? `${window.location.origin}${GOOGLE_REDIRECT_PATH}`
            : undefined;
        const config: GoogleIdConfiguration = {
            client_id: GOOGLE_CLIENT_ID,
            callback: (response) => {
                void handleGoogleCredential(response?.credential);
            },
            ux_mode: desiredMode,
            auto_select: false,
        };
        if (!shouldRedirect) {
            config.cancel_on_tap_outside = true;
        } else if (loginUri) {
            config.login_uri = loginUri;
        }
        googleApi.initialize(config);
        googleInitializedRef.current = true;
        googleInitModeRef.current = desiredMode;
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
        setAdminFlag(false);
        resetToDefaults();
        setAuthError(null);
        setProfileError(null);
        window.google?.accounts?.id?.disableAutoSelect?.();
        if (typeof window !== "undefined") {
            window.location.replace("/");
        }
    }, [resetToDefaults, setAdminFlag, setAuthUser]);

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

    useEffect(() => {
        let active = true;
        const loadBenchmarkPortfolios = async () => {
            try {
                const response = await fetch("/api/indices/portfolios");
                if (!response.ok) {
                    return;
                }
                const data = (await response.json()) as GpwBenchmarkPortfoliosResponse;
                if (!active || !data || !Array.isArray(data.portfolios)) {
                    return;
                }
                const cleaned = data.portfolios.filter(
                    (item) => Array.isArray(item.constituents) && item.constituents.length > 0
                );
                setBenchmarkPortfolios(cleaned);
            } catch {
                // ignorujemy chwilowe błędy – widget indeksów jest opcjonalny
            }
        };
        void loadBenchmarkPortfolios();
        return () => {
            active = false;
        };
    }, []);

    useEffect(() => {
        if (!benchmarkPortfolios.length) {
            return;
        }
        let active = true;
        const loadBenchmarkHistory = async () => {
            try {
                const response = await fetch("/api/indices/history");
                if (!response.ok) {
                    return;
                }
                const data = (await response.json()) as GpwBenchmarkHistoryResponse;
                if (!active || !data || !Array.isArray(data.items)) {
                    return;
                }
                const map: Record<string, GpwBenchmarkHistorySeries> = {};
                data.items.forEach((series) => {
                    map[series.index_code] = series;
                });
                setBenchmarkHistory(map);
            } catch {
                // ignorujemy chwilowe błędy sieciowe
            }
        };
        void loadBenchmarkHistory();
        return () => {
            active = false;
        };
    }, [benchmarkPortfolios]);

    useEffect(() => {
        if (!customIndices.length) {
            setCustomBenchmarkHistory({});
            return;
        }
        let cancelled = false;
        const loadCustomHistory = async () => {
            const results = await Promise.all(
                customIndices.map(async (index) => {
                    try {
                        return await computeCustomIndexSeries(index);
                    } catch {
                        return null;
                    }
                })
            );
            if (cancelled) {
                return;
            }
            const map: Record<string, GpwBenchmarkHistorySeries> = {};
            results.forEach((series) => {
                if (series && Array.isArray(series.points) && series.points.length > 0) {
                    map[series.index_code] = {
                        index_code: series.index_code,
                        index_name: series.index_name,
                        points: [...series.points].sort((a, b) => a.date.localeCompare(b.date)),
                    };
                }
            });
            setCustomBenchmarkHistory(map);
        };
        void loadCustomHistory();
        return () => {
            cancelled = true;
        };
    }, [customIndices]);

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
    const [pfComparisonMeta, setPfComparisonMeta] = useState<Record<string, ComparisonMeta>>({});
    const [pfPeriod, setPfPeriod] = useState<ChartPeriod>("max");
    const pfTotal = pfRows.reduce((a, b) => a + (Number(b.weight) || 0), 0);
    const pfRangeInvalid = pfStart > pfEnd;
    const [pfLoading, setPfLoading] = useState(false);
    const [pfErr, setPfErr] = useState("");
    const [pfProgress, setPfProgress] = useState<PortfolioSimulationProgress | null>(null);
    const pfProgressTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
    const pfProgressCleanupRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const pfProgressMessagesRef = useRef<string[]>([]);
    const pfProgressMessageIndexRef = useRef(0);
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

    const benchmarkUniverseOptions = useMemo<BenchmarkUniverseOption[]>(() => {
        const options: BenchmarkUniverseOption[] = [];

        const normalizeSymbol = (value: unknown): string =>
            typeof value === "string" ? value.trim().toUpperCase() : "";
        const normalizeName = (value: unknown): string =>
            typeof value === "string" ? value.replace(/\s+/g, " ").trim() : "";
        const parseWeight = (value: unknown): number | null => {
            if (typeof value === "number" && Number.isFinite(value)) {
                return value;
            }
            if (typeof value === "string" && value.trim()) {
                const cleaned = value.replace(/,/g, ".").replace(/\s+/g, "").trim();
                if (!cleaned) {
                    return null;
                }
                const normalized = Number(cleaned);
                if (Number.isFinite(normalized)) {
                    return normalized;
                }
            }
            return null;
        };

        benchmarkPortfolios.forEach((portfolio) => {
            const symbolSet = new Set<string>();
            const constituentMap = new Map<string, BenchmarkUniverseConstituent>();

            portfolio.constituents.forEach((entry) => {
                const displaySymbol = normalizeSymbol(entry.symbol);
                const rawSymbol = normalizeSymbol(entry.raw_symbol);
                const baseSymbol = normalizeSymbol(
                    entry.symbol_base ?? entry.raw_symbol ?? entry.symbol
                );
                const key = baseSymbol || rawSymbol || displaySymbol;
                const parsedWeight = parseWeight((entry as { weight?: unknown }).weight);
                const weightValue =
                    parsedWeight != null
                        ? parsedWeight > 0
                            ? parsedWeight
                            : parsedWeight === 0
                                ? 0
                                : null
                        : null;

                if (displaySymbol) {
                    symbolSet.add(displaySymbol);
                }
                if (baseSymbol) {
                    symbolSet.add(baseSymbol);
                }
                if (rawSymbol) {
                    symbolSet.add(rawSymbol);
                }

                if (!key) {
                    return;
                }

                const existing = constituentMap.get(key);
                const companyName = normalizeName(entry.company_name);

                if (existing) {
                    if (!existing.symbol && displaySymbol) {
                        existing.symbol = displaySymbol;
                    }
                    if (!existing.rawSymbol && rawSymbol) {
                        existing.rawSymbol = rawSymbol;
                    }
                    if (!existing.companyName && companyName) {
                        existing.companyName = companyName;
                    }
                    if (weightValue != null) {
                        const nextWeight = (existing.weight ?? 0) + weightValue;
                        existing.weight = Number.isFinite(nextWeight) ? nextWeight : existing.weight ?? null;
                        if (existing.weightPct == null || existing.weightPct === 0) {
                            existing.weightPct = weightValue;
                        }
                    }
                    return;
                }

                constituentMap.set(key, {
                    symbol: displaySymbol || rawSymbol || key,
                    baseSymbol: key,
                    rawSymbol: rawSymbol || null,
                    companyName: companyName || null,
                    weight: weightValue,
                    weightPct: weightValue,
                });
            });

            const symbols = Array.from(symbolSet).filter(Boolean);
            const constituents = normalizeBenchmarkConstituents(
                Array.from(constituentMap.values())
            ).sort((a, b) => a.baseSymbol.localeCompare(b.baseSymbol));
            const code = (portfolio.index_code ?? "").trim().toUpperCase();
            const effectiveDate = portfolio.effective_date ?? "";

            options.push({
                code,
                name: portfolio.index_name?.trim() || code,
                effectiveDate,
                symbols,
                constituents,
            });
        });

        customIndices.forEach((index) => {
            const uniqueSymbols = Array.from(
                new Set(
                    index.constituents
                        .map((item) => item.symbol.trim().toUpperCase())
                        .filter(Boolean)
                )
            ).slice(0, MAX_UNIVERSE_FALLBACK_SYMBOLS);
            const constituents: BenchmarkUniverseConstituent[] = uniqueSymbols.map((symbol) => {
                const baseSymbol = symbol.includes(".") ? symbol.split(".", 1)[0] : symbol;
                const matching = index.constituents.find(
                    (entry) => entry.symbol.trim().toUpperCase() === symbol
                );
                const weightPct =
                    matching && Number.isFinite(matching.weightPct)
                        ? Number(matching.weightPct)
                        : null;
                const sanitizedWeight =
                    weightPct != null
                        ? weightPct > 0
                            ? weightPct
                            : weightPct === 0
                                ? 0
                                : null
                        : null;
                return {
                    symbol,
                    baseSymbol,
                    rawSymbol: symbol,
                    companyName: null,
                    weight: sanitizedWeight,
                    weightPct: sanitizedWeight,
                };
            });
            const dateCandidates = [index.updatedAt, index.createdAt].filter(
                (value): value is string => typeof value === "string" && value.trim().length > 0
            );
            let effectiveDate = "";
            if (index.startDate && typeof index.startDate === "string") {
                const parsedStart = toUTCDate(index.startDate);
                if (parsedStart) {
                    effectiveDate = parsedStart.toISOString().slice(0, 10);
                }
            }
            for (const candidate of dateCandidates) {
                const parsed = new Date(candidate);
                if (!Number.isNaN(parsed.getTime())) {
                    effectiveDate = parsed.toISOString().slice(0, 10);
                    break;
                }
            }
            if (!effectiveDate) {
                effectiveDate = new Date(index.updatedAt || index.createdAt || Date.now())
                    .toISOString()
                    .slice(0, 10);
            }
            const code = index.code.trim().toUpperCase();
            const name = index.name?.trim() || `${code} (własny)`;

            options.push({
                code,
                name,
                effectiveDate,
                symbols: uniqueSymbols,
                constituents: normalizeBenchmarkConstituents(constituents),
                isCustom: true,
            });
        });

        return options.sort((a, b) => a.code.localeCompare(b.code));
    }, [benchmarkPortfolios, customIndices]);

    const benchmarkUniverseOptionMap = useMemo(() => {
        const map: Record<string, BenchmarkUniverseOption> = {};
        benchmarkUniverseOptions.forEach((option) => {
            map[option.code.toUpperCase()] = option;
        });
        return map;
    }, [benchmarkUniverseOptions]);

    const customIndexMap = useMemo(() => {
        const map: Record<string, string[]> = {};
        customIndices.forEach((index) => {
            const code = index.code.trim().toUpperCase();
            if (!code) {
                return;
            }
            const sourceSymbols =
                index.constituents && index.constituents.length
                    ? index.constituents.map((entry) => entry.symbol)
                    : index.symbols;
            const symbols = Array.from(
                new Set(
                    (sourceSymbols ?? [])
                        .map((symbol) => symbol.trim().toUpperCase())
                        .filter(Boolean)
                )
            );
            if (symbols.length) {
                map[code] = symbols.slice(0, MAX_UNIVERSE_FALLBACK_SYMBOLS);
            }
        });
        return map;
    }, [customIndices]);

    useEffect(() => {
        if (!benchmarkUniverseOptions.length) {
            setSelectedBenchmarkCode((prev) => (prev === null ? prev : null));
            return;
        }
        setSelectedBenchmarkCode((prev) => {
            if (prev && benchmarkUniverseOptions.some((option) => option.code === prev)) {
                return prev;
            }
            return benchmarkUniverseOptions[0].code;
        });
    }, [benchmarkUniverseOptions]);

    useEffect(() => {
        if (!benchmarkChangeMenuOpen) {
            return;
        }
        const handleClick = (event: MouseEvent) => {
            if (!benchmarkChangeMenuRef.current) {
                return;
            }
            if (benchmarkChangeMenuRef.current.contains(event.target as Node)) {
                return;
            }
            setBenchmarkChangeMenuOpen(false);
        };
        document.addEventListener("mousedown", handleClick);
        return () => {
            document.removeEventListener("mousedown", handleClick);
        };
    }, [benchmarkChangeMenuOpen]);

    const toggleBenchmarkDetails = useCallback(
        (code: string) => {
            setSelectedBenchmarkCode((prev) => (prev === code ? null : code));
        },
        [setSelectedBenchmarkCode]
    );

    const handleBenchmarkUniverseSelect = useCallback(
        (option: BenchmarkUniverseOption, target: "score" | "pf" | "both" = "score") => {
            const universeToken = `index:${option.code}`;
            let scoreResult: { next: string; isActive: boolean; changed: boolean } | null = null;
            let pfResult: { next: string; isActive: boolean; changed: boolean } | null = null;

            if (target === "score" || target === "both") {
                scoreResult = toggleUniverseTokenValue(scoreUniverse, universeToken);
                if (scoreResult.changed) {
                    setScoreUniverse(scoreResult.next);
                }
            }
            if (target === "pf" || target === "both") {
                pfResult = toggleUniverseTokenValue(pfScoreUniverse, universeToken);
                if (pfResult.changed) {
                    setPfScoreUniverse(pfResult.next);
                }
            }

            setSelectedBenchmarkCode(option.code);

            if ((scoreResult?.isActive || pfResult?.isActive) && option.symbols.length) {
                setScoreUniverseFallback((prev) => {
                    const merged = new Set<string>(
                        prev.map((symbol) => symbol.trim().toUpperCase()).filter(Boolean)
                    );
                    option.symbols.forEach((symbol) => {
                        const normalized = symbol.trim().toUpperCase();
                        if (normalized) {
                            merged.add(normalized);
                        }
                    });
                    return Array.from(merged).slice(0, MAX_UNIVERSE_FALLBACK_SYMBOLS);
                });
            }
        },
        [
            pfScoreUniverse,
            scoreUniverse,
            setPfScoreUniverse,
            setScoreUniverse,
            setScoreUniverseFallback,
        ]
    );

    const benchmarkOverview = useMemo(
        () =>
            benchmarkUniverseOptions.map((option) => {
                const historySeries =
                    benchmarkHistory[option.code] ?? customBenchmarkHistory[option.code];
                const points = historySeries?.points ?? [];
                const sanitizedPoints = points.reduce<GpwBenchmarkHistoryPoint[]>((acc, point) => {
                    if (!point) {
                        return acc;
                    }
                    const dateValue = typeof point.date === "string" ? point.date.trim() : "";
                    if (!dateValue) {
                        return acc;
                    }
                    const rawValue = point.value;
                    let numericValue: number | null = null;
                    if (typeof rawValue === "number" && Number.isFinite(rawValue)) {
                        numericValue = rawValue;
                    } else if (rawValue != null) {
                        const stringValue = String(rawValue).trim();
                        if (stringValue) {
                            const parsed = Number(stringValue.replace(/,/g, "."));
                            if (Number.isFinite(parsed)) {
                                numericValue = parsed;
                            }
                        }
                    }
                    if (numericValue == null) {
                        return acc;
                    }
                    let changePct: number | null = null;
                    if (typeof point.change_pct === "number" && Number.isFinite(point.change_pct)) {
                        changePct = point.change_pct;
                    }
                    acc.push({
                        date: dateValue,
                        value: numericValue,
                        change_pct: changePct,
                    });
                    return acc;
                }, []);
                const ordered = [...sanitizedPoints].sort((a, b) => a.date.localeCompare(b.date));
                const lastPoint = ordered[ordered.length - 1];
                const baselineValue = computeBenchmarkBaselineValue(
                    ordered,
                    benchmarkChangePeriod
                );
                const lastValue = lastPoint?.value ?? null;
                let changePct =
                    lastValue != null && baselineValue != null && baselineValue !== 0
                        ? (lastValue - baselineValue) / baselineValue
                        : null;
                if (
                    changePct == null &&
                    benchmarkChangePeriod === "1D" &&
                    lastPoint?.change_pct != null
                ) {
                    changePct = lastPoint.change_pct;
                }
                return {
                    code: option.code,
                    name: option.name,
                    effectiveDate: option.effectiveDate,
                    symbolsCount: option.constituents.length,
                    latestValue: lastValue,
                    changePct,
                    lastDate: lastPoint?.date ?? null,
                    isCustom: option.isCustom === true,
                };
            }),
        [
            benchmarkChangePeriod,
            benchmarkHistory,
            benchmarkUniverseOptions,
            customBenchmarkHistory,
        ]
    );

    const stopPfProgressInterval = useCallback(() => {
        if (pfProgressTimerRef.current) {
            clearInterval(pfProgressTimerRef.current);
            pfProgressTimerRef.current = null;
        }
    }, []);

    const clearPfProgressCleanup = useCallback(() => {
        if (pfProgressCleanupRef.current) {
            clearTimeout(pfProgressCleanupRef.current);
            pfProgressCleanupRef.current = null;
        }
    }, []);

    const resetPfProgressTimers = useCallback(() => {
        stopPfProgressInterval();
        clearPfProgressCleanup();
        pfProgressMessagesRef.current = [];
        pfProgressMessageIndexRef.current = 0;
    }, [clearPfProgressCleanup, stopPfProgressInterval]);

    useEffect(
        () => () => {
            resetPfProgressTimers();
        },
        [resetPfProgressTimers]
    );

    useEffect(() => {
        if (pfMode !== "score") {
            resetPfProgressTimers();
            setPfProgress(null);
        }
    }, [pfMode, resetPfProgressTimers]);

    const pfProgressState = pfMode === "score" ? pfProgress : null;
    const pfProgressPercent = pfProgressState
        ? Math.max(0, Math.min(100, Math.round(pfProgressState.percent)))
        : 0;
    const pfProgressStageLabel = pfProgressState
        ? PF_PROGRESS_STAGE_LABELS[pfProgressState.stage]
        : null;
    const pfProgressMessage = pfProgressState?.message?.trim()
        ? pfProgressState.message.trim()
        : null;

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
    }, [isAuthenticated, preferencesJson, profileHydrated, setAuthUser]);

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
            color: "#4663F0",
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
        setComparisonMeta((prev) => {
            if (!(normalized in prev)) return prev;
            const next = { ...prev };
            delete next[normalized];
            return next;
        });
        setComparisonAllRows((prev) => {
            if (!(normalized in prev)) return prev;
            const next = { ...prev };
            delete next[normalized];
            return next;
        });
        setComparisonErrors((prev) => {
            if (!(normalized in prev)) return prev;
            const next = { ...prev };
            delete next[normalized];
            return next;
        });
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
                    fetchInstrumentSeries(sym, comparisonMeta[sym]?.kind, startISO).then(
                        (result) => ({ symbol: sym, result })
                    )
                )
            );
            if (!live) return;

            const nextAll: Record<string, Row[]> = {};
            const nextErrors: Record<string, string> = {};
            const nextMeta: Record<string, ComparisonMeta> = {};

            results.forEach((result, idx) => {
                const sym = comparisonSymbols[idx];
                if (!sym) return;
                if (result.status === "fulfilled") {
                    nextAll[sym] = result.value.result.rows;
                    nextMeta[sym] = {
                        ...(comparisonMeta[sym] ?? { kind: result.value.result.kind }),
                        kind: result.value.result.kind,
                    };
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
            if (Object.keys(nextMeta).length) {
                setComparisonMeta((prev) => {
                    const merged = { ...prev };
                    for (const [sym, meta] of Object.entries(nextMeta)) {
                        const existing = merged[sym];
                        merged[sym] = {
                            ...(existing ?? meta),
                            kind: meta.kind,
                            name: existing?.name ?? meta.name ?? existing?.name ?? null,
                        };
                    }
                    return merged;
                });
            }
        })();

        return () => {
            live = false;
        };
    }, [comparisonSymbols, comparisonMeta, period, symbol]);

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
            comparisonSymbols.map((sym, idx) => {
                const meta = comparisonMeta[sym];
                const hasName = meta?.name && meta.name.trim().length > 0;
                const baseLabel = hasName ? `${sym} – ${meta?.name}` : sym;
                const label =
                    meta?.kind === "index" && !hasName ? `${sym} (indeks)` : baseLabel;
                return {
                    symbol: sym,
                    label,
                    color: COMPARISON_COLORS[idx % COMPARISON_COLORS.length],
                    rows: visibleComparisonRows[sym] ?? [],
                };
            }),
        [comparisonMeta, comparisonSymbols, visibleComparisonRows]
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
        (candidate: string, meta?: SymbolRow) => {
            const normalized = candidate.trim().toUpperCase();
            if (!normalized || !symbol) return;
            if (normalized === symbol.toUpperCase()) return;
            let added = false;
            setComparisonSymbols((prev) => {
                if (prev.includes(normalized) || prev.length >= MAX_COMPARISONS) {
                    return prev;
                }
                added = true;
                return [...prev, normalized];
            });
            if (added) {
                setComparisonMeta((prev) => ({
                    ...prev,
                    [normalized]: {
                        kind: meta?.kind ?? "stock",
                        name: meta?.name ?? meta?.raw ?? prev[normalized]?.name ?? null,
                    },
                }));
            }
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
        setComparisonMeta((prev) => {
            if (!(sym in prev)) return prev;
            const next = { ...prev };
            delete next[sym];
            return next;
        });
    }, []);

    const handleAddPfComparison = useCallback(
        (candidate: string, meta?: SymbolRow) => {
            const normalized = candidate.trim().toUpperCase();
            if (!normalized) return;
            if (pfLastBenchmark && normalized === pfLastBenchmark.toUpperCase()) return;
            let added = false;
            setPfComparisonSymbols((prev) => {
                if (prev.includes(normalized) || prev.length >= MAX_COMPARISONS) {
                    return prev;
                }
                added = true;
                return [...prev, normalized];
            });
            if (added) {
                setPfComparisonMeta((prev) => ({
                    ...prev,
                    [normalized]: {
                        kind: meta?.kind ?? "stock",
                        name: meta?.name ?? meta?.raw ?? prev[normalized]?.name ?? null,
                    },
                }));
            }
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
        setPfComparisonMeta((prev) => {
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
    const navItems: NavItem[] = [
        {
            href: view === "analysis" ? "#analysis" : "/",
            label: "Analiza techniczna",
            key: "analysis",
            icon: IconSparkline,
            description: "Zaawansowane wykresy, wskaźniki i zestawienia notowań GPW.",
        },
        {
            href: view === "score" ? "#score" : "/ranking-score",
            label: "Ranking score",
            key: "score",
            icon: IconTrophy,
            description: "Buduj rankingi momentum i ryzyka dopasowane do Twojej strategii.",
        },
        {
            href: view === "portfolio" ? "#portfolio" : "/symulator-portfela",
            label: "Symulacja portfela",
            key: "portfolio",
            icon: IconPie,
            description: "Testuj portfele z rebalancingiem, kosztami i porównaniem do benchmarków.",
        },
        ...(isAdmin
            ? [
                  {
                      href:
                          view === "sync"
                              ? "#prices-sync"
                              : "/synchronizacja-danych",
                      label: "Synchronizacja danych",
                      key: "sync",
                      icon: IconSync,
                      description:
                          "Zarządzaj synchronizacją profili spółek oraz notowań historycznych.",
                  } satisfies NavItem,
              ]
            : []),
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
                    fetchInstrumentSeries(sym, pfComparisonMeta[sym]?.kind, startISO).then(
                        (result) => ({ symbol: sym, result })
                    )
                )
            );

            if (!live) return;

            const nextAll: Record<string, Row[]> = {};
            const nextErrors: Record<string, string> = {};
            const nextMeta: Record<string, ComparisonMeta> = {};

            results.forEach((result, idx) => {
                const sym = pfComparisonSymbols[idx];
                if (!sym) return;
                if (result.status === "fulfilled") {
                    nextAll[sym] = result.value.result.rows;
                    nextMeta[sym] = {
                        ...(pfComparisonMeta[sym] ?? { kind: result.value.result.kind }),
                        kind: result.value.result.kind,
                    };
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
            if (Object.keys(nextMeta).length) {
                setPfComparisonMeta((prev) => {
                    const merged = { ...prev };
                    for (const [sym, meta] of Object.entries(nextMeta)) {
                        const existing = merged[sym];
                        merged[sym] = {
                            ...(existing ?? meta),
                            kind: meta.kind,
                            name: existing?.name ?? meta.name ?? existing?.name ?? null,
                        };
                    }
                    return merged;
                });
            }
        })();

        return () => {
            live = false;
        };
    }, [pfComparisonMeta, pfComparisonSymbols, pfRes, pfStart]);

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
            const meta = pfComparisonMeta[sym];
            const hasName = meta?.name && meta.name.trim().length > 0;
            const baseLabel = hasName ? `${sym} – ${meta?.name}` : sym;
            const label =
                meta?.kind === "index" && !hasName ? `${sym} (indeks)` : baseLabel;
            series.push({
                symbol: sym,
                label,
                color: COMPARISON_COLORS[(idx + offset) % COMPARISON_COLORS.length],
                rows: pfComparisonVisibleRows[sym] ?? [],
            });
        });
        return series;
    }, [pfBenchmarkSeries, pfComparisonMeta, pfComparisonSymbols, pfComparisonVisibleRows]);

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

    const expandUniverseValueWithCustomIndices = useCallback(
        (value: string | string[] | null | undefined): string | string[] | null | undefined => {
            if (value == null) {
                return value ?? null;
            }
            const entries = Array.isArray(value) ? value : [value];
            const expanded: string[] = [];
            const seen = new Set<string>();

            const pushToken = (token: string) => {
                const trimmed = token.trim();
                if (!trimmed) {
                    return;
                }
                const normalized = trimmed.toLowerCase();
                if (seen.has(normalized)) {
                    return;
                }
                seen.add(normalized);
                expanded.push(trimmed);
            };

            entries.forEach((entry) => {
                if (typeof entry !== "string") {
                    pushToken(String(entry));
                    return;
                }
                const trimmedEntry = entry.trim();
                if (!trimmedEntry) {
                    return;
                }
                const lowered = trimmedEntry.toLowerCase();
                if (lowered.startsWith("index:")) {
                    const afterColon = trimmedEntry.slice(trimmedEntry.indexOf(":") + 1).trim();
                    if (!afterColon) {
                        return;
                    }
                    const parts = afterColon
                        .split(/[+&]/)
                        .map((part) => part.trim().toUpperCase())
                        .filter(Boolean);
                    let expandedAny = false;
                    parts.forEach((code) => {
                        const customSymbols = customIndexMap[code];
                        if (customSymbols && customSymbols.length) {
                            customSymbols.forEach((symbol) => pushToken(symbol));
                            expandedAny = true;
                        } else {
                            pushToken(`index:${code}`);
                        }
                    });
                    if (expandedAny) {
                        return;
                    }
                    return;
                }
                pushToken(trimmedEntry);
            });

            if (!expanded.length) {
                return null;
            }
            if (!Array.isArray(value) && expanded.length === 1) {
                return expanded[0];
            }
            return expanded;
        },
        [customIndexMap]
    );

    const computeUniverseFallback = useCallback(
        (value: string | string[] | null | undefined): string[] | undefined => {
            const baseSet = new Set<string>(
                scoreUniverseFallback.map((symbol) => symbol.trim().toUpperCase()).filter(Boolean)
            );
            const codes = extractIndexCodesFromUniverse(value);
            codes.forEach((code) => {
                const option = benchmarkUniverseOptionMap[code];
                if (option) {
                    option.symbols.forEach((symbol) => {
                        const normalized = symbol.trim().toUpperCase();
                        if (normalized) {
                            baseSet.add(normalized);
                        }
                    });
                }
                const customSymbols = customIndexMap[code];
                if (customSymbols && customSymbols.length) {
                    customSymbols.forEach((symbol) => {
                        const normalized = symbol.trim().toUpperCase();
                        if (normalized) {
                            baseSet.add(normalized);
                        }
                    });
                }
            });

            if (!baseSet.size) {
                return scoreUniverseFallback.length
                    ? [...scoreUniverseFallback]
                    : undefined;
            }

            return Array.from(baseSet).slice(0, MAX_UNIVERSE_FALLBACK_SYMBOLS);
        },
        [benchmarkUniverseOptionMap, customIndexMap, scoreUniverseFallback]
    );

    const addScoreRule = () => {
        const defaultOption = SCORE_METRIC_OPTIONS[0];
        const lookbackDays = resolveLookbackDays(defaultOption, defaultOption?.lookback);
        const label =
            computeMetricLabel(defaultOption, lookbackDays) ??
            defaultOption?.label ??
            defaultOption?.value ??
            "";
        setScoreRules((prev) => [
            ...prev,
            {
                id: createRuleId(),
                metric: defaultOption?.value ?? "",
                weight: 10,
                direction: defaultOption?.defaultDirection ?? "desc",
                transform: "raw",
                label,
                lookbackDays,
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

    const addCustomIndexRow = useCallback(() => {
        setCustomIndexDraft((prev) => {
            if (prev.constituents.length >= MAX_UNIVERSE_FALLBACK_SYMBOLS) {
                return prev;
            }
            return {
                ...prev,
                constituents: [...prev.constituents, createEmptyCustomIndexRow()],
            };
        });
    }, []);

    const removeCustomIndexRow = useCallback((rowId: string) => {
        setCustomIndexDraft((prev) => {
            const nextRows = prev.constituents.filter((row) => row.id !== rowId);
            return {
                ...prev,
                constituents: nextRows.length ? nextRows : [createEmptyCustomIndexRow()],
            };
        });
    }, []);

    const updateCustomIndexRowSymbol = useCallback((rowId: string, symbol: string) => {
        const normalized = symbol.trim().toUpperCase();
        setCustomIndexDraft((prev) => ({
            ...prev,
            constituents: prev.constituents.map((row) =>
                row.id === rowId ? { ...row, symbol: normalized } : row
            ),
        }));
    }, []);

    const updateCustomIndexRowWeight = useCallback((rowId: string, value: string) => {
        setCustomIndexDraft((prev) => ({
            ...prev,
            constituents: prev.constituents.map((row) => {
                if (row.id !== rowId) {
                    return row;
                }
                if (value === "") {
                    return { ...row, weightPct: "" };
                }
                const numericValue = Number(value);
                if (!Number.isFinite(numericValue)) {
                    return row;
                }
                return { ...row, weightPct: numericValue };
            }),
        }));
    }, []);

    const handleSaveCustomIndex = useCallback(() => {
        const normalizedCode = customIndexDraft.code
            .trim()
            .toUpperCase()
            .replace(/[^A-Z0-9_-]/g, "");
        if (!normalizedCode) {
            setCustomIndexError("Podaj kod indeksu (litery, cyfry, myślnik lub podkreślenie).");
            return;
        }

        const entries: { symbol: string; weight: number | null }[] = [];
        const seen = new Set<string>();
        customIndexDraft.constituents.forEach((row) => {
            const symbol = row.symbol.trim().toUpperCase();
            if (!symbol || seen.has(symbol)) {
                return;
            }
            seen.add(symbol);
            const rawWeight =
                typeof row.weightPct === "number"
                    ? row.weightPct
                    : row.weightPct === ""
                    ? null
                    : Number(row.weightPct);
            const weight =
                typeof rawWeight === "number" && Number.isFinite(rawWeight) && rawWeight > 0
                    ? rawWeight
                    : null;
            entries.push({ symbol, weight });
        });

        if (!entries.length) {
            setCustomIndexError("Dodaj co najmniej jedną spółkę.");
            return;
        }

        const limitedEntries = entries.slice(0, MAX_UNIVERSE_FALLBACK_SYMBOLS);
        const positiveSum = limitedEntries.reduce(
            (sum, entry) => sum + (entry.weight ?? 0),
            0
        );
        const missingCount = limitedEntries.filter((entry) => entry.weight == null).length;

        let computedWeights: number[] = [];
        if (positiveSum <= 0) {
            const equalWeight = 100 / limitedEntries.length;
            computedWeights = limitedEntries.map(() => Number(equalWeight.toFixed(4)));
        } else {
            const remainder = Math.max(0, 100 - positiveSum);
            const fillValue = missingCount > 0 ? remainder / missingCount : 0;
            const provisional = limitedEntries.map((entry) => {
                if (entry.weight != null && entry.weight > 0) {
                    return entry.weight;
                }
                return fillValue > 0 ? fillValue : 0;
            });
            const sumWeights = provisional.reduce((sum, value) => sum + value, 0);
            if (sumWeights <= 0) {
                const equalWeight = 100 / limitedEntries.length;
                computedWeights = limitedEntries.map(() => Number(equalWeight.toFixed(4)));
            } else {
                const scale = 100 / sumWeights;
                computedWeights = provisional.map((value) => Number((value * scale).toFixed(4)));
            }
        }

        const normalizedConstituents = limitedEntries.map((entry, idx) => ({
            symbol: entry.symbol,
            weightPct: computedWeights[idx],
        }));

        const codeExists =
            benchmarkPortfolios.some(
                (portfolio) =>
                    (portfolio.index_code ?? "").trim().toUpperCase() === normalizedCode
            ) || customIndices.some((index) => index.code === normalizedCode);
        if (codeExists) {
            setCustomIndexError("Indeks o takim kodzie już istnieje.");
            return;
        }

        const nameValue = customIndexDraft.name.trim();
        const parsedStart = toUTCDate(customIndexDraft.startDate);
        if (!parsedStart) {
            setCustomIndexError("Podaj poprawną datę startu indeksu.");
            return;
        }
        const startDate = parsedStart.toISOString().slice(0, 10);

        const baseValueInput = customIndexDraft.baseValue.trim();
        if (!baseValueInput) {
            setCustomIndexError("Podaj wartość początkową indeksu.");
            return;
        }
        const baseValueNumeric = Number(baseValueInput);
        if (!Number.isFinite(baseValueNumeric) || baseValueNumeric <= 0) {
            setCustomIndexError("Wartość początkowa musi być dodatnią liczbą.");
            return;
        }

        const timestamp = new Date().toISOString();
        const symbols = normalizedConstituents.map((entry) => entry.symbol);

        setCustomIndices((prev) => [
            ...prev,
            {
                id: createCustomIndexId(),
                code: normalizedCode,
                name: nameValue ? nameValue : undefined,
                symbols,
                constituents: normalizedConstituents,
                startDate,
                baseValue: baseValueNumeric,
                createdAt: timestamp,
                updatedAt: timestamp,
            },
        ]);
        setCustomIndexDraft({
            code: "",
            name: "",
            constituents: [createEmptyCustomIndexRow()],
            startDate: new Date().toISOString().slice(0, 10),
            baseValue: "100",
        });
        setCustomIndexError(null);
        setCustomIndexFormOpen(false);
    }, [
        benchmarkPortfolios,
        customIndexDraft,
        customIndices,
    ]);

    const canAddCustomIndexRow =
        customIndexDraft.constituents.length < MAX_UNIVERSE_FALLBACK_SYMBOLS;

    const handleDeleteCustomIndex = useCallback(
        (id: string) => {
            const removed = customIndices.find((index) => index.id === id) ?? null;
            setCustomIndices((prev) => prev.filter((index) => index.id !== id));
            if (removed) {
                const token = `index:${removed.code.trim().toUpperCase()}`;
                setScoreUniverse((prev) => removeUniverseTokenValue(prev, token));
                setPfScoreUniverse((prev) => removeUniverseTokenValue(prev, token));
                setSelectedBenchmarkCode((prev) => {
                    if (!prev) {
                        return prev;
                    }
                    return prev === removed?.code.trim().toUpperCase() ? null : prev;
                });
            }
        },
        [customIndices, setPfScoreUniverse, setScoreUniverse, setSelectedBenchmarkCode]
    );

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
                lookbackDays: component.lookback_days,
                min_value: component.min_value,
                max_value: component.max_value,
                ...(component.scoring ? { scoring: component.scoring } : {}),
                normalize: component.normalize ?? "none",
            }));

            const limitValue = !scoreLimitInvalid && Number.isFinite(scoreLimit)
                ? Math.floor(Number(scoreLimit))
                : undefined;

            const parsedUniverse = parseUniverseValue(scoreUniverse);
            const expandedUniverse = expandUniverseValueWithCustomIndices(parsedUniverse);
            const fallbackUniverse = computeUniverseFallback(expandedUniverse);
            const resolvedUniverse = resolveUniverseWithFallback(
                expandedUniverse ?? undefined,
                fallbackUniverse
            );

            const payload: ScorePreviewRequest = {
                name: scoreNameInput.trim() || undefined,
                description: scoreDescription.trim() || undefined,
                rules: rulePayload,
                limit: limitValue,
                universe: resolvedUniverse,
                sort: scoreSort,
                as_of: scoreAsOf?.trim() ? scoreAsOf : undefined,
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
                resetPfProgressTimers();
                setPfProgress(null);
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
                const expandedUniverseValue =
                    expandUniverseValueWithCustomIndices(universeValue);
                const fallbackForPf = computeUniverseFallback(expandedUniverseValue);

                const selectedTemplate = pfSelectedTemplateId
                    ? scoreTemplates.find((tpl) => tpl.id === pfSelectedTemplateId)
                    : null;
                const componentsForScore = selectedTemplate
                    ? buildScoreComponents(fromTemplateRules(selectedTemplate.rules))
                    : scoreComponents;
                if (!componentsForScore.length) {
                    throw new Error("Skonfiguruj ranking score, aby uruchomić symulację.");
                }

                resetPfProgressTimers();
                const componentLabels = Array.from(
                    new Set(
                        componentsForScore
                            .map((component) => (component.label ?? "").trim())
                            .filter((label) => Boolean(label))
                    )
                );
                pfProgressMessagesRef.current = componentLabels;
                pfProgressMessageIndexRef.current = 0;
                const initialMessage = componentLabels.length
                    ? `Analiza metryki: ${componentLabels[0]}`
                    : "Budowanie rankingu spółek…";
                setPfProgress({
                    percent: 5,
                    stage: "ranking",
                    message: initialMessage,
                });
                pfProgressTimerRef.current = setInterval(() => {
                    setPfProgress((prev) => {
                        if (!prev) {
                            return prev;
                        }
                        const messages = pfProgressMessagesRef.current;
                        let nextMessage = prev.message ?? undefined;
                        if (messages.length > 1) {
                            pfProgressMessageIndexRef.current =
                                (pfProgressMessageIndexRef.current + 1) % messages.length;
                            nextMessage = `Analiza metryki: ${messages[pfProgressMessageIndexRef.current]}`;
                        }
                        const increment = prev.percent >= 90 ? 0 : Math.max(2, Math.random() * 6);
                        const nextPercent = Math.min(90, prev.percent + increment);
                        let nextStage: PortfolioSimulationStage = prev.stage;
                        if (nextPercent >= 65 && nextStage !== "finalizing") {
                            nextStage = "building";
                        }
                        return {
                            ...prev,
                            percent: nextPercent,
                            stage: nextStage,
                            message: nextMessage,
                        };
                    });
                }, 900);

                const res = await backtestPortfolioByScore(
                    {
                        score: pfScoreName.trim(),
                        limit: pfScoreLimitInvalid ? undefined : Math.floor(pfScoreLimit),
                        weighting: pfScoreWeighting,
                        direction: pfScoreDirection,
                        universe: expandedUniverseValue ?? undefined,
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
                    fallbackForPf,
                    customIndexMap
                );
                setPfProgress((prev) =>
                    prev
                        ? {
                              ...prev,
                              percent: 100,
                              stage: "finalizing",
                              message: "Finalizowanie wyników symulacji…",
                          }
                        : {
                              percent: 100,
                              stage: "finalizing",
                              message: "Finalizowanie wyników symulacji…",
                          }
                );
                setPfRes(res);
            }

            setPfLastBenchmark(pfBenchmark);
        } catch (e: unknown) {
            const message = e instanceof Error ? e.message : String(e);
            setPfErr(message);
            if (pfMode === "score") {
                resetPfProgressTimers();
                setPfProgress(null);
            }
        } finally {
            if (pfMode === "score") {
                resetPfProgressTimers();
                pfProgressCleanupRef.current = setTimeout(() => {
                    setPfProgress(null);
                }, 1200);
            } else {
                resetPfProgressTimers();
                setPfProgress(null);
            }
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
                className={`fixed inset-y-0 left-0 z-50 w-80 transform border-r border-soft bg-primary-strong text-neutral shadow-brand-elevated transition-transform duration-300 ease-in-out lg:hidden ${
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
                        isAuthenticated={isAuthenticated}
                        authUser={authUser}
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
                        className="absolute right-4 top-4 inline-flex h-9 w-9 items-center justify-center rounded-full border border-soft text-lg text-muted transition hover:border-primary hover:text-neutral"
                        aria-label="Zamknij menu"
                    >
                        ×
                    </button>
                </div>
            </div>
            <aside
                className={`hidden lg:flex ${sidebarCollapsed ? "lg:w-20" : "lg:w-[280px]"} flex-col border-r border-soft bg-primary-strong text-neutral lg:sticky lg:top-0 lg:h-screen lg:flex-shrink-0`}
            >
                <SidebarContent
                    collapsed={sidebarCollapsed}
                    navItems={navItems}
                    activeKey={view}
                    isAuthenticated={isAuthenticated}
                    authUser={authUser}
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
                <header className="sticky top-0 z-30 border-b border-soft bg-white/90 text-muted shadow-[0_12px_30px_rgba(15,23,42,0.08)] backdrop-blur lg:hidden">
                    <div className="mx-auto w-full max-w-6xl px-4 py-2 md:px-8 md:py-3.5">
                        <div className="mb-1.5 flex items-center justify-between lg:mb-0">
                            <div className="flex items-center gap-2">
                                <button
                                    type="button"
                                    onClick={() => setSidebarMobileOpen(true)}
                                    className="inline-flex h-10 w-10 items-center justify-center rounded-full border border-soft/80 text-muted transition hover:border-primary hover:text-neutral focus:outline-none focus-visible:ring-2 focus-visible:ring-primary/30 lg:hidden"
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
                                <span className="text-sm font-semibold text-muted lg:hidden">GPW Analytics</span>
                            </div>
                        </div>
                        <div className="mt-6 hidden space-y-4">
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
                                            <span className="sr-only">{authUser.email}</span>
                                        ) : null}
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
                                            className="flex-1 rounded-lg bg-surface px-4 py-2 text-sm font-semibold text-primary shadow-lg shadow-black/10 transition hover:bg-soft-surface disabled:cursor-not-allowed disabled:opacity-60"
                                            onClick={() => openAuthDialog("signup")}
                                            disabled={authLoading}
                                        >
                                            Załóż konto
                                        </button>
                                    </div>
                                    <div className="h-3" aria-hidden="true" />
                                    {!GOOGLE_CLIENT_ID && (
                                        <p className="text-[11px] text-amber-200">
                                            Ustaw zmienną NEXT_PUBLIC_GOOGLE_CLIENT_ID (lub GOOGLE_CLIENT_ID), aby włączyć logowanie.
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
                        <div className="mt-6 hidden">
                            <SectionNav items={navItems} />
                        </div>
                    </div>
                </header>

                {!isAuthenticated && authDialogOpen && (
                <div
                    className="fixed inset-0 z-50 flex items-center justify-center bg-transparent px-4 py-6"
                    role="dialog"
                    aria-modal="true"
                    aria-labelledby="auth-dialog-title"
                    onClick={closeAuthDialog}
                >
                    <div
                        className="w-full max-w-lg rounded-3xl bg-surface text-neutral shadow-2xl"
                        onClick={(event) => event.stopPropagation()}
                    >
                        <div className="flex items-start justify-between border-b border-soft px-6 py-5">
                            <div className="space-y-1">
                                <p className="text-xs font-semibold uppercase tracking-[0.35em] text-subtle">
                                    {authDialogSectionLabel}
                                </p>
                                <h2 id="auth-dialog-title" className="text-xl font-semibold text-neutral">
                                    {authDialogHeading}
                                </h2>
                            </div>
                            <button
                                type="button"
                                onClick={closeAuthDialog}
                                className="flex h-9 w-9 items-center justify-center rounded-full border border-soft text-lg text-subtle transition hover:border-soft hover:text-muted"
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
                                    className="flex w-full items-center justify-center gap-3 rounded-xl border border-soft bg-soft-surface px-4 py-3 text-sm font-semibold text-subtle"
                                >
                                    Kontynuuj przez Facebook
                                </button>
                                <button
                                    type="button"
                                    disabled
                                    className="flex w-full items-center justify-center gap-3 rounded-xl border border-soft bg-soft-surface px-4 py-3 text-sm font-semibold text-subtle"
                                >
                                    Kontynuuj przez Apple
                                </button>
                                <button
                                    type="button"
                                    onClick={triggerGoogleAuth}
                                    className="flex w-full items-center justify-center gap-3 rounded-xl border border-soft bg-surface px-4 py-3 text-sm font-semibold text-primary shadow hover:bg-soft-surface disabled:cursor-not-allowed disabled:opacity-60"
                                    disabled={authLoading || !GOOGLE_CLIENT_ID}
                                >
                                    {authLoading ? "Ładowanie logowania..." : "Kontynuuj przez konto Google"}
                                </button>
                            </div>
                            <div className="flex items-center gap-3 text-subtle">
                                <span className="h-px flex-1 bg-soft-surface" />
                                <span className="text-xs uppercase tracking-[0.3em]">lub</span>
                                <span className="h-px flex-1 bg-soft-surface" />
                            </div>
                            <div className="flex items-center gap-2 rounded-full bg-soft-surface p-1 text-sm font-semibold text-subtle">
                                <button
                                    type="button"
                                    onClick={() => setAuthDialogMode("login")}
                                    className={`flex-1 rounded-full px-4 py-2 transition ${
                                        authDialogMode === "login"
                                            ? "bg-surface text-neutral shadow"
                                            : "hover:text-muted"
                                    }`}
                                >
                                    Zaloguj się
                                </button>
                                <button
                                    type="button"
                                    onClick={() => setAuthDialogMode("signup")}
                                    className={`flex-1 rounded-full px-4 py-2 transition ${
                                        authDialogMode === "signup"
                                            ? "bg-surface text-neutral shadow"
                                            : "hover:text-muted"
                                    }`}
                                >
                                    Załóż konto
                                </button>
                            </div>
                            <div className="space-y-4">
                                <label className="flex flex-col gap-2 text-sm font-medium text-muted">
                                    <span>E-mail</span>
                                    <input
                                        type="email"
                                        placeholder="adres@email.com"
                                        className="rounded-xl border border-soft bg-soft-surface px-3 py-2 text-subtle shadow-inner"
                                        disabled
                                    />
                                </label>
                                <label className="flex flex-col gap-2 text-sm font-medium text-muted">
                                    <span>Hasło</span>
                                    <input
                                        type="password"
                                        placeholder="••••••••"
                                        className="rounded-xl border border-soft bg-soft-surface px-3 py-2 text-subtle shadow-inner"
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
                                <p className="text-xs text-subtle">
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

            <main className="flex-1">
                <div className="mx-auto w-full max-w-6xl px-4 py-8 md:px-8 md:py-12 space-y-16">
                    {view === "sync" && (
                        isAdmin ? (
                            <CompanySyncPanel symbol={symbol} setSymbol={setSymbol} />
                        ) : (
                            <Section
                                id="companies-sync"
                                kicker="GPW"
                                title="Synchronizacja danych o spółkach"
                                description="Panel synchronizacji jest dostępny wyłącznie dla administratorów."
                            >
                                <div className="rounded-2xl border border-dashed border-soft bg-white/70 p-6 text-sm text-muted">
                                    {isAuthenticated ? (
                                        <p>
                                            Twoje konto nie ma uprawnień administratora. Skontaktuj się z osobą
                                            zarządzającą uprawnieniami, aby uzyskać dostęp do harmonogramu
                                            synchronizacji.
                                        </p>
                                    ) : (
                                        <p>
                                            Zaloguj się na konto administratora, aby uruchamiać synchronizację danych o
                                            spółkach GPW.
                                        </p>
                                    )}
                                </div>
                            </Section>
                        )
                    )}
                    {view === "analysis" && (
                        <Section
                            id="analysis"
                        title="Analiza techniczna i kontekst"
                        description="Dodawaj symbole z GPW do listy obserwacyjnej i analizuj wykres wraz z kluczowymi statystykami, wskaźnikami momentum oraz podglądem fundamentów."
                        actions={
                            <TickerAutosuggest
                                onPick={(sym, meta) => {
                                    const normalized = sym.trim().toUpperCase();
                                    if (!normalized) return;
                                    setSymbolDisplayNames((prev) => {
                                        const displayName = extractDisplayName(normalized, meta);
                                        if (!displayName) {
                                            return prev;
                                        }
                                        if (prev[normalized] === displayName) {
                                            return prev;
                                        }
                                        return { ...prev, [normalized]: displayName };
                                    });
                                    setWatch((w) => (w.includes(normalized) ? w : [normalized, ...w]));
                                    setSymbol(normalized);
                                }}
                            />
                        }
                    >
                        <div className="space-y-10">
                            <Card>
                                <div className="space-y-6">
                                    <div className="flex flex-col gap-6 lg:flex-row lg:items-center lg:justify-between">
                                        <div className="space-y-3">
                                            <span className="inline-flex items-center rounded-full bg-primary-soft-glow px-3 py-1 text-xs font-semibold uppercase tracking-[0.28em] text-primary">
                                                Listy obserwacyjne
                                            </span>
                                            <div className="space-y-2">
                                                <h3 className="text-2xl font-semibold text-neutral">
                                                    Twoje spółki i indeksy
                                                </h3>
                                                <p className="max-w-xl text-sm text-subtle">
                                                    Masz {watch.length} instrument{watch.length === 1 ? "" : watch.length % 10 >= 2 && watch.length % 10 <= 4 && (watch.length % 100 < 10 || watch.length % 100 >= 20) ? "y" : "ów"} na bieżącej liście. Kliknij na pozycję, aby przełączyć moduły analizy poniżej.
                                                </p>
                                            </div>
                                        </div>
                                        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-end">
                                            <div className="inline-flex items-center gap-1 rounded-full border border-soft bg-soft-surface p-1 shadow-inner">
                                                {watchlistSegments.map((segment) => (
                                                    <button
                                                        key={segment.value}
                                                        type="button"
                                                        onClick={() => setWatchlistGroup(segment.value)}
                                                        className={`flex items-center gap-1 rounded-full px-4 py-2 text-xs font-semibold transition ${
                                                            watchlistGroup === segment.value
                                                                ? "bg-surface text-neutral shadow"
                                                                : "text-subtle hover:text-muted"
                                                        }`}
                                                    >
                                                        {segment.label}
                                                        <span className="text-[11px] font-medium text-subtle">
                                                            {segment.count}
                                                        </span>
                                                    </button>
                                                ))}
                                            </div>
                                            <button
                                                type="button"
                                                className="inline-flex items-center gap-2 rounded-full bg-primary px-4 py-2 text-sm font-semibold text-white shadow-[0_15px_30px_rgba(16,163,127,0.35)] transition hover:bg-[color:color-mix(in_srgb,var(--color-primary)_92%,var(--color-primary-strong)_8%)] focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-strong focus-visible:ring-offset-2 focus-visible:ring-offset-primary-soft"
                                            >
                                                <span className="text-lg leading-none">+</span>
                                                Nowa lista obserwacyjna
                                            </button>
                                        </div>
                                    </div>
                                    <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
                                        <p className="text-sm text-subtle">
                                            {activeWatchSegment ? `Wyświetlasz listę: ${activeWatchSegment.label}.` : "Wybierz listę, aby zmienić kontekst obserwacji."}
                                        </p>
                                        <div className="flex flex-wrap items-center gap-2">
                                            <button
                                                type="button"
                                                onClick={handleCycleWatchSort}
                                                className="inline-flex items-center gap-2 rounded-full border border-soft bg-surface px-4 py-2 text-sm font-semibold text-muted transition hover:border-primary hover:text-primary focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-soft focus-visible:ring-offset-2 focus-visible:ring-offset-primary-soft"
                                            >
                                                Sortuj według {watchSortLabel}
                                            </button>
                                            <button
                                                type="button"
                                                className="inline-flex items-center gap-2 rounded-full border border-soft bg-surface px-4 py-2 text-sm font-semibold text-muted transition hover:border-primary hover:text-primary focus:outline-none focus-visible:ring-2 focus-visible:ring-primary-soft focus-visible:ring-offset-2 focus-visible:ring-offset-primary-soft"
                                            >
                                                Wyświetl jako portfel
                                            </button>
                                        </div>
                                    </div>
                                    <Watchlist
                                        items={visibleWatchItems}
                                        current={symbol}
                                        onPick={(sym) => setSymbol(sym)}
                                        onRemove={removeFromWatch}
                                        displayNames={symbolDisplayNames}
                                        snapshots={watchSnapshots}
                                        group={watchlistGroup}
                                    />
                                </div>
                            </Card>

                            <div className="grid md:grid-cols-3 gap-6">
                                <div className="md:col-span-2 space-y-6">
                                    <Card
                                        title={symbol ? `${activeSymbolLabel ?? symbol} – wykres cenowy` : "Wykres cenowy"}
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
                                                                        : "Dodaj spółkę lub indeks"
                                                                }
                                                                inputClassName="w-56"
                                                                disabled={comparisonLimitReached}
                                                                allowedKinds={["stock", "index"]}
                                                            />
                                                            {comparisonLimitReached && (
                                                                <span className="text-[11px] text-subtle">
                                                                    Maksymalnie {MAX_COMPARISONS} instrumentów.
                                                                </span>
                                                            )}
                                                        </div>
                                                        <div className="mt-3 space-y-2">
                                                            {comparisonSymbols.length ? (
                                                                <div className="flex flex-wrap gap-2">
                                                                    {comparisonSymbols.map((sym) => {
                                                                        const color = comparisonColorMap[sym] ?? "#475569";
                                                                        const meta = comparisonMeta[sym];
                                                                        const hasName = meta?.name && meta.name.trim().length > 0;
                                                                        const label = hasName
                                                                            ? `${sym} – ${meta?.name}`
                                                                            : meta?.kind === "index"
                                                                            ? `${sym} (indeks)`
                                                                            : sym;
                                                                        return (
                                                                            <span
                                                                                key={sym}
                                                                                className="inline-flex items-center gap-2 rounded-full border border-soft bg-white/80 px-3 py-1 text-xs font-medium text-neutral shadow-sm"
                                                                                title={label}
                                                                            >
                                                                                <span
                                                                                    className="h-2.5 w-2.5 rounded-full"
                                                                                    style={{ backgroundColor: color }}
                                                                                />
                                                                                <span className="whitespace-nowrap">
                                                                                    {label}
                                                                                </span>
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
                                    <span className="flex items-center gap-2 text-muted text-xs uppercase tracking-wide">
                                        Universe / filtr
                                        <button
                                            type="button"
                                            tabIndex={0}
                                            aria-label="Informacja o polu Universe"
                                            className="group relative inline-flex h-4 w-4 items-center justify-center rounded-full border border-soft bg-soft-surface text-[0.65rem] font-semibold text-muted outline-none transition hover:text-primary focus-visible:text-primary"
                                        >
                                            i
                                            <span className="pointer-events-none absolute bottom-full left-1/2 z-10 hidden w-64 -translate-x-1/2 -translate-y-2 rounded bg-soft-surface p-2 text-[0.65rem] text-left font-normal normal-case leading-relaxed text-subtle shadow-lg group-hover:block group-focus-visible:block">
                                                Pozostaw puste, aby uwzględnić wszystkie spółki GPW dostępne w bazie danych.
                                                Możesz też łączyć kilka indeksów, np.{" "}
                                                <code className="mx-1 rounded bg-soft-surface px-1">index:MWIG40</code>{" "}
                                                <span className="text-muted">+</span>{" "}
                                                <code className="mx-1 rounded bg-soft-surface px-1">index:SWIG80</code>,{" "}
                                                oraz własne konfiguracje.
                                            </span>
                                        </button>
                                    </span>
                                    <input
                                        type="text"
                                        value={scoreUniverse}
                                        onChange={(e) => setScoreUniverse(e.target.value)}
                                        className={inputBaseClasses}
                                        placeholder="np. index:MWIG40, index:SWIG80"
                                    />
                                    {benchmarkUniverseOptions.length > 0 && (
                                        <div className="flex flex-wrap items-center gap-2 text-xs text-subtle">
                                            <span>Wybierz indeks:</span>
                                            {benchmarkUniverseOptions.map((option) => {
                                                const token = `index:${option.code}`.toLowerCase();
                                                const isActive = universeIncludesToken(
                                                    scoreUniverse,
                                                    token
                                                );
                                                const baseClasses =
                                                    "rounded-full border px-3 py-1 transition text-xs";
                                                const activeClasses =
                                                    "border-[var(--color-primary)] text-primary bg-primary/10";
                                                const inactiveClasses =
                                                    "border-soft text-muted hover:border-[var(--color-primary)] hover:text-primary";
                                                return (
                                                    <button
                                                        key={option.code}
                                                        type="button"
                                                        onClick={() =>
                                                            handleBenchmarkUniverseSelect(option, "score")
                                                        }
                                                        className={`${baseClasses} ${
                                                            isActive ? activeClasses : inactiveClasses
                                                        }`}
                                                        title={`Skład indeksu na ${option.effectiveDate}`}
                                                    >
                                                        <span className="font-semibold">{option.code}</span>
                                                        {option.isCustom && (
                                                            <span className="ml-2 rounded-full bg-primary/10 px-2 py-[1px] text-[10px] uppercase tracking-wide text-primary">
                                                                Własny
                                                            </span>
                                                        )}
                                                        {option.name && option.name !== option.code && (
                                                            <span className="ml-1 text-[10px]">
                                                                {option.name}
                                                            </span>
                                                        )}
                                                        <span className="ml-1 text-[10px] text-subtle">
                                                            {option.effectiveDate}
                                                        </span>
                                                    </button>
                                                );
                                            })}
                                        </div>
                                    )}
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
                                    {scoreRules.map((rule, idx) => {
                                        const metricOption = SCORE_METRIC_OPTIONS.find(
                                            (option) => option.value === rule.metric
                                        );
                                        const lookbackConfig = metricOption?.customLookback;
                                        const lookbackValue = resolveLookbackDays(
                                            metricOption,
                                            rule.lookbackDays
                                        );
                                        const lookbackPresets = lookbackConfig?.presets ?? [];
                                        const presetValue = lookbackPresets.some(
                                            (preset) => preset.value === lookbackValue
                                        )
                                            ? String(lookbackValue)
                                            : "custom";
                                        const updateLookback = (nextLookback: number | null | undefined) => {
                                            setScoreRules((prev) =>
                                                prev.map((r) => {
                                                    if (r.id !== rule.id) return r;
                                                    const optionForRule = SCORE_METRIC_OPTIONS.find(
                                                        (option) => option.value === r.metric
                                                    );
                                                    const normalized = resolveLookbackDays(
                                                        optionForRule,
                                                        nextLookback
                                                    );
                                                    const nextLabel =
                                                        computeMetricLabel(
                                                            optionForRule,
                                                            normalized
                                                        ) ?? r.metric;
                                                    return {
                                                        ...r,
                                                        lookbackDays: normalized,
                                                        label: nextLabel,
                                                    };
                                                })
                                            );
                                        };
                                        const displayLabel =
                                            computeMetricLabel(metricOption, lookbackValue) ??
                                            rule.label ??
                                            metricOption?.label ??
                                            rule.metric;
                                        const isCustomTotalReturn =
                                            metricOption?.value === "total_return_custom";
                                        const rawWorstValue = parseOptionalNumber(rule.min);
                                        const rawBestValue = parseOptionalNumber(rule.max);
                                        let previewWorst = clampNumber(
                                            typeof rawWorstValue === "number" &&
                                                Number.isFinite(rawWorstValue)
                                                ? rawWorstValue
                                                : 0,
                                            -1000,
                                            1000
                                        );
                                        let previewBest = clampNumber(
                                            typeof rawBestValue === "number" &&
                                                Number.isFinite(rawBestValue)
                                                ? rawBestValue
                                                : 100,
                                            -1000,
                                            1000
                                        );
                                        if (previewBest <= previewWorst) {
                                            if (previewWorst >= 1000) {
                                                previewWorst = 999.999;
                                                previewBest = 1000;
                                            } else {
                                                previewBest = Math.min(1000, previewWorst + 0.0001);
                                            }
                                        }
                                        const baseMetricKey = metricOption?.backendMetric
                                            ? `${metricOption.backendMetric}_${lookbackValue}`
                                            : rule.metric.replace(/[\s-]+/g, "_");
                                        const camelMetricKey = baseMetricKey.replace(
                                            /_([a-z0-9])/gi,
                                            (_, char: string) => char.toUpperCase()
                                        );
                                        const canonicalMetricKey =
                                            metricOption?.backendMetric === "price_change"
                                                ? camelMetricKey
                                                : baseMetricKey;
                                        const metricScale = isCustomTotalReturn
                                            ? {
                                                  worst: previewWorst,
                                                  best: previewBest,
                                                  direction:
                                                      rule.direction === "desc" ? "up" : "down",
                                                  metricKey: canonicalMetricKey,
                                              }
                                            : null;
                                        const clampScaleInput = (value: string): string => {
                                            const parsed = parseOptionalNumber(value);
                                            if (typeof parsed !== "number") return value;
                                            const clamped = clampNumber(parsed, -1000, 1000);
                                            return `${clamped}`;
                                        };

                                        return (
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
                                                                onChange={(e) => {
                                                                    const { value } = e.target;
                                                                    setScoreRules((prev) =>
                                                                        prev.map((r) => {
                                                                            if (r.id !== rule.id) return r;
                                                                            const selectedOption = SCORE_METRIC_OPTIONS.find(
                                                                                (option) => option.value === value
                                                                            );
                                                                            const lookbackDays = resolveLookbackDays(
                                                                                selectedOption,
                                                                                selectedOption?.lookback
                                                                            );
                                                                            const label =
                                                                                computeMetricLabel(
                                                                                    selectedOption,
                                                                                    lookbackDays
                                                                                ) ?? value;
                                                                            const isPriceChangeSelected =
                                                                                selectedOption?.value ===
                                                                                "total_return_custom";
                                                                            const wasPriceChange =
                                                                                metricOption?.value ===
                                                                                "total_return_custom";
                                                                            const next: ScoreBuilderRule = {
                                                                                ...r,
                                                                                metric: value,
                                                                                label,
                                                                                direction:
                                                                                    selectedOption?.defaultDirection ??
                                                                                    r.direction,
                                                                                lookbackDays,
                                                                            };
                                                                            if (isPriceChangeSelected) {
                                                                                next.min =
                                                                                    typeof r.min === "string" &&
                                                                                    r.min.trim().length > 0
                                                                                        ? clampScaleInput(r.min)
                                                                                        : "0";
                                                                                next.max =
                                                                                    typeof r.max === "string" &&
                                                                                    r.max.trim().length > 0
                                                                                        ? clampScaleInput(r.max)
                                                                                        : "100";
                                                                                if (next.transform !== "percentile") {
                                                                                    next.transform = "raw";
                                                                                }
                                                                            } else if (wasPriceChange &&
                                                                                !isPriceChangeSelected
                                                                            ) {
                                                                                if (
                                                                                    next.transform !== "percentile" &&
                                                                                    next.transform !== "zscore"
                                                                                ) {
                                                                                    next.transform = "raw";
                                                                                }
                                                                            }
                                                                            return next;
                                                                        })
                                                                    );
                                                                }}
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
                                                    {lookbackConfig && (
                                                        <div className="grid gap-3 md:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
                                                            <label className="flex flex-col gap-2">
                                                                <span className="text-xs uppercase tracking-wide text-muted">
                                                                    Okres (dni)
                                                                </span>
                                                                <div className="flex flex-col gap-2 sm:flex-row">
                                                                    {lookbackPresets.length > 0 && (
                                                                        <select
                                                                            value={presetValue}
                                                                            onChange={(e) => {
                                                                                const value = e.target.value;
                                                                                if (value === "custom") {
                                                                                    updateLookback(
                                                                                        rule.lookbackDays ?? lookbackValue
                                                                                    );
                                                                                    return;
                                                                                }
                                                                                updateLookback(Number(value));
                                                                            }}
                                                                            className={inputBaseClasses}
                                                                        >
                                                                            {lookbackPresets.map((preset) => (
                                                                                <option key={preset.value} value={preset.value}>
                                                                                    {preset.label}
                                                                                </option>
                                                                            ))}
                                                                            <option value="custom">Inny zakres</option>
                                                                        </select>
                                                                    )}
                                                                    <input
                                                                        type="number"
                                                                        min={lookbackConfig.min}
                                                                        max={lookbackConfig.max}
                                                                        step={lookbackConfig.step ?? 1}
                                                                        value={lookbackValue}
                                                                        onChange={(e) => {
                                                                            const numeric = Number(e.target.value);
                                                                            updateLookback(numeric);
                                                                        }}
                                                                        className={inputBaseClasses}
                                                                    />
                                                                </div>
                                                                <span className="text-[11px] text-subtle">
                                                                    Zakres {lookbackConfig.min}–{lookbackConfig.max} dni.
                                                                </span>
                                                            </label>
                                                            <div className="text-xs text-subtle">
                                                                Aktualna etykieta: {" "}
                                                                <b>
                                                                    {displayLabel}
                                                                </b>
                                                            </div>
                                                        </div>
                                                    )}
                                                    {metricOption?.description && (
                                                        <div className="text-xs text-subtle">
                                                            {metricOption.description}
                                                        </div>
                                                    )}
                                                    <div className="grid gap-3 md:grid-cols-[minmax(0,220px)_minmax(0,1fr)] md:items-start">
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
                                                        {isCustomTotalReturn && metricScale ? (
                                                            <div className="space-y-2 rounded-2xl border border-soft bg-surface p-4">
                                                                <div className="flex items-center gap-2 text-sm font-medium text-primary">
                                                                    Skala punktacji
                                                                    <InfoHint text="Skala liniowa, obcięta do [0,1]. Wartości r poniżej „Najgorszego wyniku” dostają 0, powyżej „Najlepszego” otrzymują 1." />
                                                                </div>
                                                                <div className="grid gap-3 sm:grid-cols-2">
                                                                    <label className="flex flex-col gap-2">
                                                                        <span className="text-xs uppercase tracking-wide text-muted">
                                                                            Najgorszy wynik
                                                                        </span>
                                                                        <div className="relative">
                                                                            <input
                                                                                type="number"
                                                                                min={-1000}
                                                                                max={1000}
                                                                                step={0.1}
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
                                                                                onBlur={(e) => {
                                                                                    const sanitized = clampScaleInput(e.target.value);
                                                                                    if (sanitized !== e.target.value) {
                                                                                        setScoreRules((prev) =>
                                                                                            prev.map((r) =>
                                                                                                r.id === rule.id ? { ...r, min: sanitized } : r
                                                                                            )
                                                                                        );
                                                                                    }
                                                                                }}
                                                                                className={`${inputBaseClasses} pr-10`}
                                                                            />
                                                                            <span className="pointer-events-none absolute inset-y-0 right-3 flex items-center text-sm text-muted">
                                                                                %
                                                                            </span>
                                                                        </div>
                                                                    </label>
                                                                    <label className="flex flex-col gap-2">
                                                                        <span className="text-xs uppercase tracking-wide text-muted">
                                                                            Najlepszy wynik
                                                                        </span>
                                                                        <div className="relative">
                                                                            <input
                                                                                type="number"
                                                                                min={-1000}
                                                                                max={1000}
                                                                                step={0.1}
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
                                                                                onBlur={(e) => {
                                                                                    const sanitized = clampScaleInput(e.target.value);
                                                                                    if (sanitized !== e.target.value) {
                                                                                        setScoreRules((prev) =>
                                                                                            prev.map((r) =>
                                                                                                r.id === rule.id ? { ...r, max: sanitized } : r
                                                                                            )
                                                                                        );
                                                                                    }
                                                                                }}
                                                                                className={`${inputBaseClasses} pr-10`}
                                                                            />
                                                                            <span className="pointer-events-none absolute inset-y-0 right-3 flex items-center text-sm text-muted">
                                                                                %
                                                                            </span>
                                                                        </div>
                                                                    </label>
                                                                </div>
                                                            </div>
                                                        ) : (
                                                            <div className="grid gap-3 sm:grid-cols-2">
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
                                                        )}
                                                    </div>
                                                    <div className="grid gap-3 md:grid-cols-2">
                                                        {isCustomTotalReturn ? (
                                                            <label className="flex items-center gap-3 rounded-xl border border-soft bg-soft-surface px-3 py-2 text-sm text-primary">
                                                                <input
                                                                    type="checkbox"
                                                                    checked={rule.transform === "percentile"}
                                                                    onChange={(e) =>
                                                                        setScoreRules((prev) =>
                                                                            prev.map((r) =>
                                                                                r.id === rule.id
                                                                                    ? {
                                                                                          ...r,
                                                                                          transform: e.target.checked
                                                                                              ? "percentile"
                                                                                              : "raw",
                                                                                      }
                                                                                    : r
                                                                            )
                                                                        )
                                                                    }
                                                                    className="h-4 w-4 rounded border-soft text-primary focus:ring-primary"
                                                                />
                                                                <span className="font-medium">Zastosuj normalizację percentylową po skali</span>
                                                            </label>
                                                        ) : (
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
                                                        )}
                                                    <div className="text-xs text-subtle">
                                                        {isCustomTotalReturn
                                                            ? "Percentyle liczone po score."
                                                            : "Metryki korzystają z danych cenowych (zwroty, zmienność, Sharpe). Wagi są skalowane automatycznie."}
                                                    </div>
                                                </div>
                                                {metricOption ? (
                                                    <MetricRulePreview
                                                        rule={rule}
                                                        metricOption={metricOption}
                                                        lookbackValue={lookbackValue}
                                                        asOf={scoreAsOf}
                                                        onLookbackChange={updateLookback}
                                                    />
                                                ) : null}
                                            </div>
                                        {idx === scoreRules.length - 1 && (
                                            <div className="mt-3 text-xs text-subtle">
                                                Zmieniaj wagi i parametry, aby zobaczyć wpływ na ranking.
                                            </div>
                                            )}
                                            </div>
                                        );
                                    })}
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
                                    <ScoreMissingTable items={scoreResults.missing} />
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
                    {benchmarkUniverseOptions.length > 0 && (
                        <Card title="Indeksy GPW Benchmark">
                            <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
                                <p className="text-sm text-subtle">
                                    Aktualne portfele i wyniki indeksów dostępnych jako wszechświat
                                    dla rankingu.
                                </p>
                                <div className="relative" ref={benchmarkChangeMenuRef}>
                                    <button
                                        type="button"
                                        onClick={() => setBenchmarkChangeMenuOpen((prev) => !prev)}
                                        className={`rounded-lg border px-3 py-1.5 text-xs font-medium transition ${
                                            benchmarkChangeMenuOpen
                                                ? "border-[var(--color-primary)] text-primary"
                                                : "border-soft text-muted hover:border-[var(--color-primary)] hover:text-primary"
                                        }`}
                                    >
                                        Okres zmiany: {
                                            BENCHMARK_CHANGE_PERIOD_OPTIONS.find(
                                                (option) => option.value === benchmarkChangePeriod
                                            )?.label ?? benchmarkChangePeriod
                                        }
                                    </button>
                                    {benchmarkChangeMenuOpen && (
                                        <div className="absolute right-0 z-20 mt-2 w-56 rounded-xl border border-soft bg-surface shadow-lg">
                                            {BENCHMARK_CHANGE_PERIOD_OPTIONS.map((option) => (
                                                <button
                                                    key={option.value}
                                                    type="button"
                                                    onClick={() => {
                                                        setBenchmarkChangePeriod(option.value);
                                                        setBenchmarkChangeMenuOpen(false);
                                                    }}
                                                    className={`block w-full px-3 py-2 text-left text-xs transition ${
                                                        option.value === benchmarkChangePeriod
                                                            ? "bg-primary/10 text-primary"
                                                            : "text-muted hover:bg-soft-surface hover:text-primary"
                                                    }`}
                                                >
                                                    {option.label}
                                                </button>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            </div>
                            <div className="mb-6 space-y-3 rounded-xl border border-soft bg-soft-surface px-4 py-4">
                                <div className="flex flex-wrap items-center justify-between gap-2">
                                    <div>
                                        <div className="text-sm font-medium text-primary">Własne indeksy</div>
                                        <div className="text-xs text-subtle">
                                            Definiuj własne listy spółek – zapisujemy je lokalnie w przeglądarce.
                                        </div>
                                    </div>
                                    <button
                                        type="button"
                                        onClick={() => {
                                            setCustomIndexFormOpen((prev) => {
                                                const next = !prev;
                                                if (next) {
                                                    setCustomIndexDraft({
                                                        code: "",
                                                        name: "",
                                                        constituents: [createEmptyCustomIndexRow()],
                                                        startDate: new Date().toISOString().slice(0, 10),
                                                        baseValue: "100",
                                                    });
                                                }
                                                return next;
                                            });
                                            setCustomIndexError(null);
                                        }}
                                        className="rounded-lg border border-soft px-3 py-1.5 text-xs font-medium text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                    >
                                        {customIndexFormOpen ? "Zamknij" : "Nowy indeks"}
                                    </button>
                                </div>
                                {customIndexFormOpen && (
                                    <div className="space-y-3 rounded-xl border border-dashed border-soft bg-surface px-4 py-4">
                                        <div className="grid gap-3 md:grid-cols-2">
                                            <label className="flex flex-col gap-2">
                                                <span className="text-xs uppercase tracking-wide text-muted">
                                                    Kod indeksu
                                                </span>
                                                <input
                                                    type="text"
                                                    value={customIndexDraft.code}
                                                    onChange={(e) =>
                                                        setCustomIndexDraft((prev) => ({
                                                            ...prev,
                                                            code: e.target.value,
                                                        }))
                                                    }
                                                    className={inputBaseClasses}
                                                    placeholder="np. QUALITY20"
                                                />
                                            </label>
                                            <label className="flex flex-col gap-2">
                                                <span className="text-xs uppercase tracking-wide text-muted">
                                                    Nazwa (opcjonalnie)
                                                </span>
                                                <input
                                                    type="text"
                                                    value={customIndexDraft.name}
                                                    onChange={(e) =>
                                                        setCustomIndexDraft((prev) => ({
                                                            ...prev,
                                                            name: e.target.value,
                                                        }))
                                                    }
                                                    className={inputBaseClasses}
                                                    placeholder="np. Ranking jakościowy"
                                                />
                                            </label>
                                        </div>
                                        <div className="space-y-3">
                                            <div className="flex flex-wrap items-center justify-between gap-2">
                                                <span className="text-xs uppercase tracking-wide text-muted">
                                                    Skład indeksu
                                                </span>
                                                <button
                                                    type="button"
                                                    onClick={addCustomIndexRow}
                                                    disabled={!canAddCustomIndexRow}
                                                    className="rounded-lg border border-dashed border-soft px-3 py-1.5 text-xs text-muted transition hover:border-[var(--color-primary)] hover:text-primary disabled:cursor-not-allowed disabled:opacity-50"
                                                >
                                                    Dodaj spółkę
                                                </button>
                                            </div>
                                            <div className="space-y-2">
                                                {customIndexDraft.constituents.map((row) => (
                                                    <div
                                                        key={row.id}
                                                        className="flex flex-wrap items-center gap-3 rounded-xl border border-soft bg-soft-surface px-3 py-3"
                                                    >
                                                        <div className="flex flex-col gap-1">
                                                            <span className="text-[11px] uppercase tracking-wide text-muted">
                                                                Symbol
                                                            </span>
                                                            <TickerAutosuggest
                                                                onPick={(symbol) => updateCustomIndexRowSymbol(row.id, symbol)}
                                                                placeholder={row.symbol || "Symbol (np. CDR.WA)"}
                                                                inputClassName="w-48 md:w-56"
                                                                allowFreeEntry
                                                            />
                                                            {row.symbol && (
                                                                <span className="text-[11px] text-subtle">
                                                                    Wybrano: {row.symbol}
                                                                </span>
                                                            )}
                                                        </div>
                                                        <div className="flex flex-col gap-1">
                                                            <span className="text-[11px] uppercase tracking-wide text-muted">
                                                                Udział %
                                                            </span>
                                                            <div className="flex items-center gap-2">
                                                                <input
                                                                    type="number"
                                                                    min={0}
                                                                    step={0.01}
                                                                    value={row.weightPct === "" ? "" : row.weightPct}
                                                                    onChange={(e) => updateCustomIndexRowWeight(row.id, e.target.value)}
                                                                    className={`${inputBaseClasses} w-24`}
                                                                    placeholder="np. 10"
                                                                />
                                                                <span className="text-sm text-subtle">%</span>
                                                            </div>
                                                        </div>
                                                        {customIndexDraft.constituents.length > 1 && (
                                                            <button
                                                                type="button"
                                                                onClick={() => removeCustomIndexRow(row.id)}
                                                                className="rounded-lg border border-soft px-2 py-1 text-xs text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                                            >
                                                                Usuń
                                                            </button>
                                                        )}
                                                    </div>
                                                ))}
                                            </div>
                                            <div className="text-xs text-subtle">
                                                Niepodane udziały rozłożymy równomiernie. Limit {MAX_UNIVERSE_FALLBACK_SYMBOLS} spółek.
                                            </div>
                                        </div>
                                        <div className="grid gap-3 md:grid-cols-2">
                                            <label className="flex flex-col gap-2">
                                                <span className="text-xs uppercase tracking-wide text-muted">
                                                    Data startu
                                                </span>
                                                <input
                                                    type="date"
                                                    value={customIndexDraft.startDate}
                                                    onChange={(e) =>
                                                        setCustomIndexDraft((prev) => ({
                                                            ...prev,
                                                            startDate: e.target.value,
                                                        }))
                                                    }
                                                    className={inputBaseClasses}
                                                />
                                            </label>
                                            <label className="flex flex-col gap-2">
                                                <span className="text-xs uppercase tracking-wide text-muted">
                                                    Wartość początkowa
                                                </span>
                                                <input
                                                    type="number"
                                                    min={1}
                                                    step={0.01}
                                                    value={customIndexDraft.baseValue}
                                                    onChange={(e) =>
                                                        setCustomIndexDraft((prev) => ({
                                                            ...prev,
                                                            baseValue: e.target.value,
                                                        }))
                                                    }
                                                    className={inputBaseClasses}
                                                    placeholder="np. 100"
                                                />
                                            </label>
                                        </div>
                                        {customIndexError && (
                                            <div className="text-xs text-negative">{customIndexError}</div>
                                        )}
                                        <div className="flex flex-wrap gap-2">
                                            <button
                                                type="button"
                                                onClick={handleSaveCustomIndex}
                                                className="rounded-lg bg-primary px-3 py-2 text-xs font-medium text-white transition hover:bg-[var(--color-primary-dark)]"
                                            >
                                                Zapisz indeks
                                            </button>
                                            <button
                                                type="button"
                                                onClick={() => {
                                                    setCustomIndexFormOpen(false);
                                                    setCustomIndexError(null);
                                                    setCustomIndexDraft({
                                                        code: "",
                                                        name: "",
                                                        constituents: [createEmptyCustomIndexRow()],
                                                        startDate: new Date().toISOString().slice(0, 10),
                                                        baseValue: "100",
                                                    });
                                                }}
                                                className="rounded-lg border border-soft px-3 py-2 text-xs text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                            >
                                                Anuluj
                                            </button>
                                        </div>
                                    </div>
                                )}
                                {customIndices.length > 0 ? (
                                    <div className="space-y-2">
                                        {customIndices.map((index) => {
                                            const normalizedCode = index.code.trim().toUpperCase();
                                            const option = benchmarkUniverseOptionMap[normalizedCode];
                                            const fallbackOption: BenchmarkUniverseOption =
                                                option ?? {
                                                    code: normalizedCode,
                                                    name:
                                                        index.name?.trim() || `${normalizedCode} (własny)`,
                                                    effectiveDate:
                                                        index.startDate && index.startDate.trim().length
                                                            ? index.startDate
                                                            : new Date(
                                                                  index.updatedAt || index.createdAt || Date.now()
                                                              )
                                                                  .toISOString()
                                                                  .slice(0, 10),
                                                    symbols: index.constituents.map((item) => item.symbol),
                                                    constituents: index.constituents.map((item) => {
                                                        const symbol = item.symbol;
                                                        const baseSymbol = symbol.includes(".")
                                                            ? symbol.split(".", 1)[0]
                                                            : symbol;
                                                        return {
                                                            symbol,
                                                            baseSymbol,
                                                            rawSymbol: symbol,
                                                            companyName: null,
                                                            weightPct: item.weightPct,
                                                        };
                                                    }),
                                                    isCustom: true,
                                                };
                                            const previewSymbols = index.constituents
                                                .slice(0, 6)
                                                .map((item) => item.symbol)
                                                .join(", ");
                                            return (
                                                <div
                                                    key={index.id}
                                                    className="flex flex-wrap items-start justify-between gap-3 rounded-xl border border-soft bg-surface px-3 py-3 shadow-sm"
                                                >
                                                    <div className="space-y-1">
                                                        <div className="text-sm font-semibold text-primary">
                                                            {normalizedCode}
                                                            <span className="ml-2 rounded-full bg-primary/10 px-2 py-[1px] text-[10px] uppercase tracking-wide text-primary">
                                                                Własny
                                                            </span>
                                                        </div>
                                                        {index.name && index.name.trim() && (
                                                            <div className="text-xs text-subtle">
                                                                {index.name.trim()}
                                                            </div>
                                                        )}
                                                        <div className="text-xs text-muted">
                                                            {index.constituents.length} spółek • {previewSymbols}
                                                            {index.constituents.length > 6 ? "…" : ""}
                                                        </div>
                                                        <div className="text-xs text-subtle">
                                                            Start: {index.startDate}
                                                            {" • "}
                                                            Wartość początkowa: {benchmarkValueFormatter.format(index.baseValue)}
                                                        </div>
                                                    </div>
                                                    <div className="flex flex-wrap gap-2">
                                                        <button
                                                            type="button"
                                                            onClick={() => handleBenchmarkUniverseSelect(fallbackOption, "score")}
                                                            className="rounded-lg border border-soft px-3 py-1.5 text-xs text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                                        >
                                                            Dodaj do score
                                                        </button>
                                                        <button
                                                            type="button"
                                                            onClick={() => handleBenchmarkUniverseSelect(fallbackOption, "pf")}
                                                            className="rounded-lg border border-soft px-3 py-1.5 text-xs text-muted transition hover:border-[var(--color-primary)] hover:text-primary"
                                                        >
                                                            Dodaj do symulatora
                                                        </button>
                                                        <button
                                                            type="button"
                                                            onClick={() => handleDeleteCustomIndex(index.id)}
                                                            className="rounded-lg border border-soft px-3 py-1.5 text-xs text-negative transition hover:border-negative hover:text-negative"
                                                        >
                                                            Usuń
                                                        </button>
                                                    </div>
                                                </div>
                                            );
                                        })}
                                    </div>
                                ) : (
                                    <div className="text-xs text-subtle">
                                        Nie dodano jeszcze żadnego własnego indeksu.
                                    </div>
                                )}
                            </div>
                            {benchmarkOverview.length > 0 ? (
                                <>
                                    <div className="overflow-x-auto">
                                        <table className="min-w-full divide-y divide-soft text-sm">
                                            <thead className="bg-soft-surface text-xs uppercase tracking-wide text-muted">
                                                <tr>
                                                    <th className="px-3 py-2 text-left">Indeks</th>
                                                    <th className="px-3 py-2 text-left">Nazwa</th>
                                                    <th className="px-3 py-2 text-right">Spółki</th>
                                                    <th className="px-3 py-2 text-left">Skład z dnia</th>
                                                    <th className="px-3 py-2 text-right">Ostatnia wartość</th>
                                                    <th className="px-3 py-2 text-right">Zmiana ({benchmarkChangePeriod})</th>
                                                    <th className="px-3 py-2 text-left">Notowanie z dnia</th>
                                                </tr>
                                            </thead>
                                            <tbody className="divide-y divide-soft">
                                                {benchmarkOverview.map((item) => {
                                                    const optionDetails =
                                                        benchmarkUniverseOptionMap[
                                                            item.code.toUpperCase()
                                                        ];
                                                    const isExpanded =
                                                        selectedBenchmarkCode === item.code;

                                                    return (
                                                        <React.Fragment key={item.code}>
                                                            <tr
                                                                role="button"
                                                                tabIndex={0}
                                                                onClick={() =>
                                                                    toggleBenchmarkDetails(
                                                                        item.code
                                                                    )
                                                                }
                                                                onKeyDown={(event) => {
                                                                    if (
                                                                        event.key ===
                                                                            "Enter" ||
                                                                        event.key === " " ||
                                                                        event.key === "Space"
                                                                    ) {
                                                                        event.preventDefault();
                                                                        toggleBenchmarkDetails(
                                                                            item.code
                                                                        );
                                                                    }
                                                                }}
                                                                className={`cursor-pointer transition ${
                                                                    isExpanded
                                                                        ? "bg-primary/5"
                                                                        : "hover:bg-soft-surface"
                                                                }`}
                                                                title={`Skład indeksu na ${item.effectiveDate}`}
                                                            >
                                                                <td className="px-3 py-2 font-medium text-primary">
                                                                    <div className="flex items-center gap-2">
                                                                        <span
                                                                            aria-hidden
                                                                            className="text-sm text-muted"
                                                                        >
                                                                            {isExpanded
                                                                                ? "▾"
                                                                                : "▸"}
                                                                        </span>
                                                                        <span>{item.code}</span>
                                                                        {item.isCustom && (
                                                                            <span className="rounded-full bg-primary/10 px-2 py-[2px] text-[10px] uppercase tracking-wide text-primary">
                                                                                Własny
                                                                            </span>
                                                                        )}
                                                                    </div>
                                                                </td>
                                                                <td className="px-3 py-2 text-subtle">
                                                                    {item.name || "—"}
                                                                </td>
                                                                <td className="px-3 py-2 text-right">
                                                                    {item.symbolsCount}
                                                                </td>
                                                                <td className="px-3 py-2 text-subtle">
                                                                    {item.effectiveDate}
                                                                </td>
                                                                <td className="px-3 py-2 text-right">
                                                                    {item.latestValue != null
                                                                        ? benchmarkValueFormatter.format(
                                                                              item.latestValue
                                                                          )
                                                                        : "—"}
                                                                </td>
                                                                <td
                                                                    className={`px-3 py-2 text-right ${
                                                                        item.changePct != null
                                                                            ? item.changePct >= 0
                                                                                ? "text-positive"
                                                                                : "text-negative"
                                                                            : "text-subtle"
                                                                    }`}
                                                                >
                                                                    {item.changePct != null
                                                                        ? benchmarkPercentFormatter.format(
                                                                              item.changePct
                                                                          )
                                                                        : "—"}
                                                                </td>
                                                                <td className="px-3 py-2 text-subtle">
                                                                    {item.lastDate ?? "—"}
                                                                </td>
                                                            </tr>
                                                            {isExpanded && (
                                                                <tr className="bg-soft-surface">
                                                                    <td
                                                                        className="px-3 py-3"
                                                                        colSpan={7}
                                                                    >
                                                                        <div className="space-y-3">
                                                                            <div className="flex flex-wrap items-center gap-3">
                                                                                <span className="text-sm font-medium text-neutral">
                                                                                    Skład indeksu
                                                                                    {optionDetails?.effectiveDate
                                                                                        ? ` (${optionDetails.effectiveDate})`
                                                                                        : ""}
                                                                                </span>
                                                                            </div>
                                                                            {optionDetails ? (
                                                                                optionDetails.constituents.length > 0 ? (
                                                                                    <div className="overflow-x-auto">
                                                                                        <table className="min-w-full divide-y divide-soft text-sm">
                                                                                            <thead className="bg-soft-surface text-xs uppercase tracking-wide text-muted">
                                                                                                <tr>
                                                                                                    <th className="px-3 py-2 text-left">
                                                                                                        Symbol
                                                                                                    </th>
                                                                                                    <th className="px-3 py-2 text-right">
                                                                                                        Udział
                                                                                                    </th>
                                                                                                </tr>
                                                                                            </thead>
                                                                                            <tbody className="divide-y divide-soft">
                                                                                                {optionDetails.constituents.map(
                                                                                                    (constituent) => (
                                                                                                        <tr
                                                                                                            key={`${item.code}-${constituent.baseSymbol ?? constituent.symbol}`}
                                                                                                        >
                                                                                                            <td className="px-3 py-2 font-medium text-primary">
                                                                                                                {constituent.symbol}
                                                                                                                {constituent.symbol !==
                                                                                                                    constituent.baseSymbol && (
                                                                                                                    <span className="ml-2 text-xs text-subtle">
                                                                                                                        ({constituent.baseSymbol})
                                                                                                                    </span>
                                                                                                                )}
                                                                                                            </td>
                                                                                                            <td className="px-3 py-2 text-right text-subtle">
                                                                                                                {typeof constituent.weightPct === "number"
                                                                                                                    ? `${benchmarkWeightFormatter.format(
                                                                                                                          constituent.weightPct
                                                                                                                      )}%`
                                                                                                                    : "—"}
                                                                                                            </td>
                                                                                                        </tr>
                                                                                                    )
                                                                                                )}
                                                                                            </tbody>
                                                                                        </table>
                                                                                    </div>
                                                                                ) : (
                                                                                    <div className="text-xs text-subtle">
                                                                                        Brak danych o składzie tego indeksu.
                                                                                    </div>
                                                                                )
                                                                            ) : (
                                                                                <div className="text-xs text-subtle">
                                                                                    Nie udało się odnaleźć szczegółów tego indeksu.
                                                                                </div>
                                                                            )}
                                                                        </div>
                                                                    </td>
                                                                </tr>
                                                            )}
                                                        </React.Fragment>
                                                    );
                                                })}
                                            </tbody>
                                        </table>
                                    </div>
                                </>
                            ) : (
                                <div className="text-xs text-subtle">
                                    Ładujemy dane indeksów GPW Benchmark…
                                </div>
                            )}
                        </Card>
                    )}
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
                                                        placeholder="np. index:MWIG40, index:SWIG80"
                                                    />
                                                    {benchmarkUniverseOptions.length > 0 && (
                                                        <div className="flex flex-wrap items-center gap-2 text-xs text-subtle">
                                                            <span>Wybierz indeks:</span>
                                                            {benchmarkUniverseOptions.map((option) => {
                                                                const token = `index:${option.code}`.toLowerCase();
                                                                const isActive = universeIncludesToken(
                                                                    pfScoreUniverse,
                                                                    token
                                                                );
                                                                const baseClasses =
                                                                    "rounded-full border px-3 py-1 transition text-xs";
                                                                const activeClasses =
                                                                    "border-[var(--color-primary)] text-primary bg-primary/10";
                                                                const inactiveClasses =
                                                                    "border-soft text-muted hover:border-[var(--color-primary)] hover:text-primary";
                                                                return (
                                                                    <button
                                                                        key={`pf-${option.code}`}
                                                                        type="button"
                                                                        onClick={() =>
                                                                            handleBenchmarkUniverseSelect(
                                                                                option,
                                                                                "pf"
                                                                            )
                                                                        }
                                                                        className={`${baseClasses} ${
                                                                            isActive
                                                                                ? activeClasses
                                                                                : inactiveClasses
                                                                        }`}
                                                                        title={`Skład indeksu na ${option.effectiveDate}`}
                                                                    >
                                                                        <span className="font-semibold">
                                                                            {option.code}
                                                                        </span>
                                                                        {option.isCustom && (
                                                                            <span className="ml-2 rounded-full bg-primary/10 px-2 py-[1px] text-[10px] uppercase tracking-wide text-primary">
                                                                                Własny
                                                                            </span>
                                                                        )}
                                                                        {option.name && option.name !== option.code && (
                                                                            <span className="ml-1 text-[10px]">
                                                                                {option.name}
                                                                            </span>
                                                                        )}
                                                                        <span className="ml-1 text-[10px] text-subtle">
                                                                            {option.effectiveDate}
                                                                        </span>
                                                                    </button>
                                                                );
                                                            })}
                                                        </div>
                                                    )}
                                                    <span className="text-xs text-subtle">
                                                        Możesz łączyć kilka indeksów oraz własne konfiguracje,
                                                        np. <code className="rounded bg-soft-surface px-1">index:MWIG40</code>{" "}
                                                        <span className="text-muted">+</span>{" "}
                                                        <code className="rounded bg-soft-surface px-1">index:SWIG80</code>.
                                                    </span>
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
                                                allowedKinds={["stock", "index"]}
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
                                    {pfProgressState && (
                                        <div
                                            className="w-full rounded-2xl border border-dashed border-soft/80 bg-white/80 p-3 text-xs text-muted"
                                            aria-live="polite"
                                        >
                                            <div className="flex items-center justify-between font-medium text-primary/80">
                                                <span>{pfProgressStageLabel ?? "Postęp symulacji"}</span>
                                                <span className="font-semibold text-primary">{pfProgressPercent}%</span>
                                            </div>
                                            <div className="mt-2 h-2 w-full overflow-hidden rounded-full bg-soft">
                                                <div
                                                    className="h-full rounded-full bg-primary transition-all duration-500"
                                                    style={{ width: `${pfProgressPercent}%` }}
                                                />
                                            </div>
                                            {pfProgressMessage && (
                                                <div className="mt-2 text-[11px] text-subtle">
                                                    {pfProgressMessage}
                                                </div>
                                            )}
                                        </div>
                                    )}
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
                                                                    : "Dodaj benchmark (spółka lub indeks)"
                                                            }
                                                            inputClassName="w-60"
                                                            disabled={pfComparisonLimitReached}
                                                            allowedKinds={["stock", "index"]}
                                                        />
                                                        {pfComparisonLimitReached && (
                                                            <span className="text-[11px] text-subtle">
                                                                Maksymalnie {MAX_COMPARISONS} instrumentów.
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
                                                                    const meta = pfComparisonMeta[sym];
                                                                    const hasName = meta?.name && meta.name.trim().length > 0;
                                                                    const label = hasName
                                                                        ? `${sym} – ${meta?.name}`
                                                                        : meta?.kind === "index"
                                                                        ? `${sym} (indeks)`
                                                                        : sym;
                                                                    return (
                                                                        <span
                                                                            key={sym}
                                                                            className="inline-flex items-center gap-2 rounded-full border border-soft bg-white/80 px-3 py-1 text-xs font-medium text-neutral shadow-sm"
                                                                            title={label}
                                                                        >
                                                                            <span
                                                                                className="h-2.5 w-2.5 rounded-full"
                                                                                style={{ backgroundColor: color }}
                                                                            />
                                                                            <span className="whitespace-nowrap">
                                                                                {label}
                                                                            </span>
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
                                                    className="inline-flex items-center gap-2 rounded-full border border-soft bg-surface px-4 py-2 text-sm font-semibold text-primary transition hover:border-[var(--color-primary)] hover:text-[var(--color-primary)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-primary)]/40"
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
