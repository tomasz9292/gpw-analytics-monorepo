"use client";

import { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption, SeriesOption } from "echarts";

import { useTheme, type ThemeMode } from "@/components/theme-provider";

export type ChartPoint = {
    date: string;
    close: number;
    open?: number;
    high?: number;
    low?: number;
    volume?: number;
    sma?: number | null;
};

export type ComparisonSeries = {
    name: string;
    color?: string;
    data: ChartPoint[];
};

export type BrushRange = { startIndex: number; endIndex: number };

type MainChartEChartsProps = {
    data: ChartPoint[];
    comparisonData?: ComparisonSeries[];
    type?: "area" | "candlestick";
    theme?: ThemeMode;
    interval?: string | number;
    brushData?: ChartPoint[];
    brushRange?: BrushRange | null;
    onBrushChange?: (range: BrushRange) => void;
    height?: number | string;
    className?: string;
    showSMA?: boolean;
    primaryLabel?: string;
    showLegend?: boolean;
};

const formatDateLabel = (value: string, withDay = false) => {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    const formatter = new Intl.DateTimeFormat("pl-PL", {
        day: withDay ? "numeric" : undefined,
        month: "short",
        year: "numeric",
    });
    return formatter.format(date);
};

export function MainChartECharts({
    data,
    comparisonData,
    type = "area",
    theme,
    interval,
    brushData,
    brushRange,
    onBrushChange,
    height = 360,
    className,
    showSMA = false,
    primaryLabel = "Kurs",
    showLegend = true,
}: MainChartEChartsProps) {
    const themeContext = useTheme();
    const resolvedTheme = theme ?? themeContext.theme ?? "light";
    const isDark = resolvedTheme === "dark";

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

    const axisData = useMemo(() => (brushData ?? data).map((point) => point.date), [brushData, data]);
    const axisIndexMap = useMemo(() => {
        const map = new Map<string, number>();
        axisData.forEach((value, idx) => map.set(value, idx));
        return map;
    }, [axisData]);

    const hasComparison = comparisonData?.some((series) => series.data.length) ?? false;
    const usePercentScale = type !== "candlestick" && hasComparison;
    const comparisonOnSecondaryAxis = type === "candlestick" && hasComparison;

    const firstClose = data[0]?.close ?? 0;
    const lastClose = data[data.length - 1]?.close ?? firstClose;
    const isGrowing = lastClose >= firstClose;
    const primaryStroke = isGrowing ? "#34D399" : "#F87171";
    const primaryAlt = isGrowing ? "#059669" : "#DC2626";
    const areaFill = isGrowing
        ? "rgba(52,211,153,0.18)"
        : "rgba(248,113,113,0.18)";
    const gridColor = isDark ? "rgba(255,255,255,0.08)" : "#E2E8F0";
    const textColor = isDark ? "#E5E7EB" : "#334155";
    const mutedText = isDark ? "#94A3B8" : "#475569";

    const percentLabel = (value: number) => `${percentFormatter.format(value)}%`;

    const primaryBase = firstClose || 0;

    const sliderStartValue =
        brushRange && axisData[brushRange.startIndex] !== undefined
            ? axisData[brushRange.startIndex]
            : axisData[0];
    const sliderEndValue =
        brushRange && axisData[brushRange.endIndex] !== undefined
            ? axisData[brushRange.endIndex]
            : axisData[axisData.length - 1];

    const series = useMemo(() => {
        const next: SeriesOption[] = [];
        const alignToAxis = <T,>(
            points: ChartPoint[],
            mapper: (point: ChartPoint) => T | null,
            makeEmpty?: () => T
        ): T[] => {
            const aligned: T[] = new Array(axisData.length);
            if (makeEmpty) {
                for (let i = 0; i < axisData.length; i += 1) {
                    aligned[i] = makeEmpty();
                }
            }
            points.forEach((point) => {
                const mapped = mapper(point);
                if (mapped == null) return;
                const idx = axisIndexMap.get(point.date);
                if (idx == null) return;
                aligned[idx] = mapped;
            });
            return aligned;
        };

        if (type === "candlestick") {
            const candlestickData = alignToAxis(data, (point) => {
                const open = point.open ?? point.close;
                const close = point.close;
                const high = point.high ?? Math.max(open, close);
                const low = point.low ?? Math.min(open, close);
                const change = close - primaryBase;
                const changePct = primaryBase !== 0 ? (change / primaryBase) * 100 : 0;
                return {
                    name: point.date,
                    value: [open, close, low, high],
                    close,
                    open,
                    high,
                    low,
                    change,
                    changePct,
                };
            }, () => ({ value: [NaN, NaN, NaN, NaN] }));

            next.push({
                type: "candlestick",
                name: primaryLabel,
                data: candlestickData,
                itemStyle: {
                    color: primaryStroke,
                    color0: primaryAlt,
                    borderColor: primaryStroke,
                    borderColor0: primaryAlt,
                },
                emphasis: { focus: "series" },
            });

            if (showSMA) {
                const smaData = alignToAxis(data, (point) =>
                    point.sma == null ? null : Number(point.sma.toFixed(4)),
                    () => NaN
                );
                next.push({
                    type: "line",
                    name: "SMA 20",
                    data: smaData,
                    smooth: true,
                    showSymbol: false,
                    yAxisIndex: 0,
                    lineStyle: { color: "#0EA5E9", width: 1.5, type: "dashed" },
                    tooltip: {
                        valueFormatter: (value) => priceFormatter.format(Number(value ?? 0)),
                    },
                });
            }
        } else {
            const lineData = alignToAxis(data, (point) => {
                const change = point.close - primaryBase;
                const changePct = primaryBase !== 0 ? (change / primaryBase) * 100 : 0;
                return {
                    name: point.date,
                    value: usePercentScale ? changePct : point.close,
                    close: point.close,
                    change,
                    changePct,
                    volume: point.volume ?? null,
                    sma: point.sma ?? null,
                };
            });

            next.push({
                type: "line",
                name: primaryLabel,
                data: lineData,
                smooth: true,
                showSymbol: false,
                lineStyle: { width: 2, color: primaryStroke },
                areaStyle:
                    type === "area"
                        ? {
                              color: areaFill,
                          }
                        : undefined,
                emphasis: { focus: "series" },
            });

            if (showSMA) {
                const smaData = alignToAxis(data, (point) => {
                    if (point.sma == null) return null;
                    const diff = point.sma - primaryBase;
                    return usePercentScale
                        ? primaryBase !== 0
                            ? (diff / primaryBase) * 100
                            : 0
                        : point.sma;
                });
                next.push({
                    type: "line",
                    name: "SMA 20",
                    data: smaData,
                    smooth: true,
                    showSymbol: false,
                    lineStyle: { color: "#0EA5E9", width: 1.5, type: "dashed" },
                    emphasis: { focus: "series" },
                });
            }
        }

        (comparisonData ?? [])
            .filter((series) => series.data.length)
            .forEach((series) => {
                const base = series.data[0]?.close ?? 0;
                const values = alignToAxis(series.data, (point) => {
                    const change = point.close - base;
                    const changePct = base !== 0 ? (change / base) * 100 : 0;
                    return {
                        name: point.date,
                        value:
                            usePercentScale || comparisonOnSecondaryAxis
                                ? changePct
                                : point.close,
                        close: point.close,
                        change,
                        changePct,
                    };
                });
                next.push({
                    type: "line",
                    name: series.name,
                    data: values,
                    smooth: true,
                    showSymbol: false,
                    yAxisIndex: comparisonOnSecondaryAxis ? 1 : 0,
                    lineStyle: { width: 2, color: series.color },
                    emphasis: { focus: "series" },
                });
            });

        return next;
    }, [
        comparisonData,
        comparisonOnSecondaryAxis,
        data,
        primaryAlt,
        primaryBase,
        primaryLabel,
        primaryStroke,
        priceFormatter,
        showSMA,
        type,
        usePercentScale,
        areaFill,
        axisData,
        axisIndexMap,
    ]);

    const yAxes: NonNullable<EChartsOption["yAxis"]> = [
        {
            type: "value",
            scale: true,
            axisLabel: {
                color: textColor,
                formatter: (value: number) =>
                    usePercentScale ? percentLabel(value) : priceFormatter.format(value),
            },
            splitLine: { lineStyle: { color: gridColor } },
            axisLine: { show: false },
            axisTick: { show: false },
        },
    ];

    if (comparisonOnSecondaryAxis) {
        yAxes.push({
            type: "value",
            position: "right",
            scale: true,
            axisLabel: { color: mutedText, formatter: (value: number) => percentLabel(value) },
            splitLine: { show: false },
            axisLine: { show: false },
            axisTick: { show: false },
        });
    }

    const hasBrushRange = Boolean(brushRange && axisData.length);

    const option: EChartsOption = {
        backgroundColor: "transparent",
        animation: false,
        textStyle: { color: textColor, fontFamily: "Inter, system-ui, -apple-system, sans-serif" },
        legend:
            showLegend !== false
                ? {
                      top: 6,
                      right: 8,
                      textStyle: { color: textColor, fontSize: 11 },
                  }
                : undefined,
        grid: {
            left: 52,
            right: comparisonOnSecondaryAxis ? 64 : 28,
            top: showLegend === false ? 16 : 32,
            bottom: 72,
        },
        xAxis: {
            type: "category",
            data: axisData,
            boundaryGap: type === "candlestick",
            axisLine: { lineStyle: { color: gridColor } },
            axisTick: { show: false },
            axisLabel: {
                color: mutedText,
                formatter: (value: string) => formatDateLabel(value),
                hideOverlap: true,
            },
        },
        yAxis: yAxes,
        dataZoom: [
            {
                type: "inside",
                xAxisIndex: 0,
                zoomOnMouseWheel: true,
                moveOnMouseMove: true,
                moveOnMouseWheel: true,
                start: hasBrushRange ? undefined : 0,
                end: hasBrushRange ? undefined : 100,
                startValue: sliderStartValue,
                endValue: sliderEndValue,
            },
            {
                type: "slider",
                xAxisIndex: 0,
                height: 36,
                bottom: 16,
                showDetail: false,
                handleSize: 24,
                borderColor: "transparent",
                backgroundColor: isDark ? "rgba(255,255,255,0.05)" : "rgba(148,163,184,0.14)",
                handleStyle: { color: primaryStroke },
                dataBackground: {
                    lineStyle: { color: primaryStroke, opacity: 0.8 },
                    areaStyle: { color: areaFill },
                },
                start: hasBrushRange ? undefined : 0,
                end: hasBrushRange ? undefined : 100,
                startValue: sliderStartValue,
                endValue: sliderEndValue,
            },
        ],
        axisPointer: {
            link: [{ xAxisIndex: "all" }],
        },
        tooltip: {
            trigger: "axis",
            backgroundColor: isDark ? "rgba(17,24,39,0.9)" : "rgba(255,255,255,0.95)",
            borderColor: isDark ? "#1E293B" : "#E2E8F0",
            textStyle: { color: textColor },
            axisPointer: { type: "cross", snap: true },
            formatter: (params: unknown) => {
                const items = Array.isArray(params) ? params : [params];
                const [first] = items;
                const dateLabel =
                    (first as { axisValueLabel?: string; name?: string } | undefined)?.axisValueLabel ??
                    (first as { name?: string } | undefined)?.name ??
                    "";

                const lines = items.map((item) => {
                    const color =
                        typeof item.color === "string"
                            ? item.color
                            : primaryStroke;
                    const raw = item.data as
                        | { value?: number | number[]; close?: number; change?: number; changePct?: number }
                        | number[];

                    const close =
                        typeof raw === "object" && raw !== null && !Array.isArray(raw)
                            ? raw.close ?? (Array.isArray(raw.value) ? raw.value[1] : raw.value)
                            : Array.isArray(raw) && raw.length >= 2
                            ? raw[1]
                            : null;

                    const change =
                        typeof raw === "object" && raw !== null && !Array.isArray(raw)
                            ? raw.change
                            : null;
                    const changePct =
                        typeof raw === "object" && raw !== null && !Array.isArray(raw)
                            ? raw.changePct
                            : null;

                    const valueText = close != null ? priceFormatter.format(Number(close)) : "—";
                    const changeText =
                        change != null && Number.isFinite(change)
                            ? `${change > 0 ? "+" : change < 0 ? "-" : ""}${priceFormatter.format(
                                  Math.abs(change)
                              )}`
                            : null;
                    const changePctText =
                        changePct != null && Number.isFinite(changePct)
                            ? `${changePct > 0 ? "+" : changePct < 0 ? "-" : ""}${percentLabel(
                                  Math.abs(changePct)
                              )}`
                            : null;

                    return `<div style="display:flex;align-items:center;justify-content:space-between;gap:12px;margin-top:6px;">
                        <span style="display:flex;align-items:center;gap:6px;font-weight:600;color:${color};">
                            <span style="width:8px;height:8px;border-radius:9999px;background:${color};display:inline-block;"></span>
                            ${item.seriesName}
                        </span>
                        <span style="text-align:right;color:${textColor};font-weight:600;">${valueText}${
                        changeText ? `<span style="margin-left:8px;color:${color};font-weight:600;">${changeText}${changePctText ? ` (${changePctText})` : ""}</span>` : ""
                    }</span>
                    </div>`;
                });

                const header = `<div style="font-weight:700;margin-bottom:4px;color:${textColor};">${dateLabel}</div>`;
                const intervalLabel =
                    interval != null
                        ? `<div style="color:${mutedText};font-size:11px;">Zakres: ${interval}</div>`
                        : "";
                return `<div>${header}${intervalLabel}${lines.join("")}</div>`;
            },
        },
        series,
    };

    type DataZoomPayload = {
        start?: number;
        end?: number;
        startValue?: number | string;
        endValue?: number | string;
    };
    type DataZoomEvent = DataZoomPayload & { batch?: DataZoomPayload[] };

    const handleDataZoom = (event: DataZoomEvent) => {
        if (!onBrushChange) return;
        const payload: DataZoomPayload = event.batch?.[0] ?? event;
        const startValue =
            payload.startValue ?? payload.start ?? (axisData.length ? axisData[0] : undefined);
        const endValue =
            payload.endValue ??
            payload.end ??
            (axisData.length ? axisData[axisData.length - 1] : undefined);

        if (startValue === undefined || endValue === undefined || !axisData.length) {
            return;
        }

        const toIndex = (value: number | string | undefined): number => {
            if (typeof value === "string") {
                const mapped = axisIndexMap.get(value);
                if (mapped != null) return mapped;
            }
            if (typeof value === "number" && value <= 100) {
                const ratio = Math.max(0, Math.min(value, 100)) / 100;
                return Math.round(ratio * Math.max(axisData.length - 1, 0));
            }
            if (typeof value === "number") {
                return Math.max(0, Math.min(Math.round(value), axisData.length - 1));
            }
            return 0;
        };

        const startIndex = toIndex(startValue);
        const endIndex = toIndex(endValue);

        onBrushChange({
            startIndex: Math.min(startIndex, endIndex),
            endIndex: Math.max(startIndex, endIndex),
        });
    };

    const resolvedHeight =
        typeof height === "number" ? `${height}px` : typeof height === "string" ? height : "360px";

    return (
        <ReactECharts
            option={option}
            style={{ width: "100%", height: resolvedHeight }}
            className={className}
            notMerge
            lazyUpdate
            onEvents={{ datazoom: handleDataZoom }}
            key={`${type}-${axisData[0] ?? "none"}-${axisData[axisData.length - 1] ?? "none"}-${
                data.length
            }-${comparisonData?.length ?? 0}`}
            // notMerge ensures dataZoom is recalculated per dataset/interval switch
        />
    );
}
