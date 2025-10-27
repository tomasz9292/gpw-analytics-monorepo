"use client";

import { useEffect, useMemo, useState } from "react";

import { normalize } from "@/lib/normalize";

export type MetricScale = {
    worst: number;
    best: number;
    direction: "up" | "down";
    metricKey: string;
};

export type Stock = {
    id: string;
    ticker: string;
    name: string;
    metrics: Record<string, number | undefined>;
};

type Props = {
    scale: MetricScale;
    stocks: Stock[];
};

const WIDTH = 320;
const HEIGHT = 80;
const PADDING_X = 16;
const PADDING_Y = 12;

const clamp = (value: number, min: number, max: number) =>
    Math.min(max, Math.max(min, value));

const formatRaw = (value: number) =>
    value.toLocaleString("pl-PL", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });

export default function MiniScoreChart({ scale, stocks }: Props) {
    const [selectedId, setSelectedId] = useState<string>("");

    const availableStocks = useMemo(() => {
        const withMetric = stocks
            .map((stock) => ({
                ...stock,
                metricValue: stock.metrics?.[scale.metricKey],
            }))
            .filter((stock) => typeof stock.metricValue === "number");

        return withMetric.sort((a, b) => a.ticker.localeCompare(b.ticker));
    }, [scale.metricKey, stocks]);

    useEffect(() => {
        if (!availableStocks.length) {
            setSelectedId("");
            return;
        }
        setSelectedId((prev) => {
            if (prev && availableStocks.some((stock) => stock.id === prev)) {
                return prev;
            }
            return availableStocks[0]?.id ?? "";
        });
    }, [availableStocks]);

    const selected = useMemo(
        () => availableStocks.find((stock) => stock.id === selectedId) ?? null,
        [availableStocks, selectedId]
    );

    const rangeMin = Math.min(scale.worst, scale.best);
    const rangeMax = Math.max(scale.worst, scale.best);
    const span = Math.max(rangeMax - rangeMin, 1e-9);

    const contentWidth = WIDTH - PADDING_X * 2;
    const contentHeight = HEIGHT - PADDING_Y * 2;

    const xForValue = (value: number) =>
        PADDING_X + ((value - rangeMin) / span) * contentWidth;
    const yForScore = (score: number) =>
        PADDING_Y + (1 - score) * contentHeight;

    const startScore = normalize(rangeMin, scale);
    const endScore = normalize(rangeMax, scale);
    const linePath = `M ${xForValue(rangeMin)} ${yForScore(startScore)} L ${xForValue(
        rangeMax
    )} ${yForScore(endScore)}`;

    const marker = (() => {
        if (!selected || typeof selected.metricValue !== "number") {
            return null;
        }
        const rawValue = selected.metricValue;
        const clampedValue = clamp(rawValue, rangeMin, rangeMax);
        const score = normalize(rawValue, scale);
        return {
            rawValue,
            clampedValue,
            score,
            x: xForValue(clampedValue),
            y: yForScore(score),
        };
    })();

    return (
        <div className="space-y-3">
            <label className="flex flex-col gap-2">
                <span className="text-xs uppercase tracking-wide text-muted">
                    Spółka
                </span>
                <select
                    value={selectedId}
                    onChange={(event) => setSelectedId(event.target.value)}
                    className="w-full rounded-xl border border-soft bg-surface px-3 py-2 text-sm text-neutral focus:outline-none focus:border-[var(--color-tech)] focus:ring-2 focus:ring-[rgba(52,152,219,0.15)] disabled:cursor-not-allowed disabled:opacity-60"
                    disabled={!availableStocks.length}
                >
                    {!availableStocks.length ? (
                        <option value="">Brak danych</option>
                    ) : null}
                    {availableStocks.map((stock) => (
                        <option key={stock.id} value={stock.id}>
                            {stock.ticker} — {stock.name}
                        </option>
                    ))}
                </select>
            </label>

            <div className="relative">
                <svg
                    viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
                    role="img"
                    aria-label="Skala punktacji"
                    className="h-20 w-full"
                >
                    <rect
                        x={PADDING_X}
                        y={PADDING_Y}
                        width={contentWidth}
                        height={contentHeight}
                        fill="transparent"
                        stroke="rgba(148, 163, 184, 0.35)"
                        strokeDasharray="4 4"
                    />
                    <path
                        d={linePath}
                        fill="none"
                        stroke="var(--color-primary)"
                        strokeWidth={2}
                        strokeLinecap="round"
                    />
                    {marker ? (
                        <circle
                            cx={marker.x}
                            cy={marker.y}
                            r={5}
                            fill="white"
                            stroke="var(--color-primary)"
                            strokeWidth={2}
                        />
                    ) : null}
                </svg>
                {marker ? (
                    <div
                        className="absolute -translate-x-1/2 rounded-lg border border-soft bg-surface px-2 py-1 text-xs shadow"
                        style={{
                            left: `${(marker.x / WIDTH) * 100}%`,
                            top: `${(marker.y / HEIGHT) * 100}%`,
                        }}
                    >
                        <div>Raw: {formatRaw(marker.rawValue)}</div>
                        <div>Score: {marker.score.toFixed(2)}</div>
                    </div>
                ) : (
                    <div className="absolute inset-0 flex items-center justify-center text-xs text-subtle">
                        Wybierz spółkę, aby zobaczyć pozycję na skali.
                    </div>
                )}
            </div>

            <div className="text-xs text-subtle">
                Zakres: {formatRaw(scale.worst)} – {formatRaw(scale.best)}, Kierunek: {" "}
                {scale.direction === "up" ? "Więcej = lepiej" : "Mniej = lepiej"}
            </div>
        </div>
    );
}
