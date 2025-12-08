"use client";

import type { ReactNode } from "react";

export type ChartIntervalValue = 90 | 180 | 365 | 1825 | "max";
export type ChartTypeValue = "area" | "candlestick";

type ChartControlsProps = {
    interval: ChartIntervalValue;
    onIntervalChange: (next: ChartIntervalValue) => void;
    type: ChartTypeValue;
    onTypeChange: (next: ChartTypeValue) => void;
    className?: string;
    children?: ReactNode;
    showTypeSwitcher?: boolean;
};

const INTERVAL_OPTIONS: { label: string; value: ChartIntervalValue }[] = [
    { label: "3M", value: 90 },
    { label: "6M", value: 180 },
    { label: "1R", value: 365 },
    { label: "5L", value: 1825 },
    { label: "MAX", value: "max" },
];

const baseChipClass =
    "inline-flex items-center justify-center rounded-full border px-3 py-1.5 text-xs font-semibold transition";

const inactiveChipClass =
    "border-soft bg-surface text-muted hover:border-primary hover:text-primary hover:shadow-primary-glow";

const activeChipClass = "border-primary bg-primary-glow text-primary shadow-primary-glow";

export function ChartControls({
    interval,
    onIntervalChange,
    type,
    onTypeChange,
    className,
    children,
    showTypeSwitcher = true,
}: ChartControlsProps) {
    return (
        <div className={`flex flex-wrap items-center gap-2 ${className ?? ""}`}>
            <div className="inline-flex flex-wrap gap-2 rounded-full bg-soft-surface px-1 py-1">
                {INTERVAL_OPTIONS.map((option) => {
                    const isActive = interval === option.value;
                    return (
                        <button
                            key={option.value}
                            type="button"
                            onClick={() => onIntervalChange(option.value)}
                            className={`${baseChipClass} ${isActive ? activeChipClass : inactiveChipClass}`}
                        >
                            {option.label}
                        </button>
                    );
                })}
            </div>
            {showTypeSwitcher ? (
                <div className="inline-flex items-center gap-1 rounded-full bg-soft-surface p-1 shadow-inner">
                    {(["area", "candlestick"] as const).map((variant) => {
                        const isActive = type === variant;
                        return (
                            <button
                                key={variant}
                                type="button"
                                onClick={() => onTypeChange(variant)}
                                className={`${baseChipClass} ${
                                    isActive ? activeChipClass : inactiveChipClass
                                } px-4`}
                            >
                                {variant === "area" ? "Area" : "Swiece"}
                            </button>
                        );
                    })}
                </div>
            ) : null}
            {children ? <div className="inline-flex items-center gap-2">{children}</div> : null}
        </div>
    );
}
