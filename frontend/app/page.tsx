"use client";

import React, { useMemo, useState, useEffect } from "react";
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
} from "recharts";

/** =========================
 *  API base (proxy w next.config.mjs)
 *  ========================= */
const API = "/api";

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

type Rebalance = "none" | "monthly" | "quarterly" | "yearly";

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
    start: string,
    rebalance: Rebalance
): Promise<PortfolioResp> {
    const payload = {
        start_date: start,
        rebalance,
        positions: symbols.map((symbol, idx) => ({
            symbol,
            weight: weightsPct[idx],
        })),
    };

    const url = `/api/backtest/portfolio`;
    const primary = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
    });

    if (primary.ok) {
        const json = await primary.json();
        return normalizePortfolioResponse(json);
    }

    const qs = new URLSearchParams({
        symbols: symbols.join(","),
        weights: weightsPct.join(","),
        start,
        rebalance,
    });

    const fallback = await fetch(`${url}?${qs.toString()}`);
    if (!fallback.ok) {
        throw new Error(`API /backtest/portfolio ${primary.status} / ${fallback.status}`);
    }
    const fallbackJson = await fallback.json();
    return normalizePortfolioResponse(fallbackJson);
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
    };
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
    <div className="bg-white rounded-2xl shadow-sm border border-gray-100">
        {(title || right) && (
            <div className="px-4 md:px-6 py-3 border-b border-gray-100 flex items-center justify-between">
                <div className="font-semibold text-gray-900">{title}</div>
                <div>{right}</div>
            </div>
        )}
        <div className="p-4 md:p-6">{children}</div>
    </div>
);

const Chip = ({
    active,
    onClick,
    children,
}: {
    active?: boolean;
    onClick?: () => void;
    children: React.ReactNode;
}) => (
    <button
        onClick={onClick}
        className={`rounded-full px-3 py-1 text-sm border ${active
                ? "bg-black text-white border-black"
                : "bg-white text-gray-700 border-gray-300 hover:border-gray-400"
            }`}
    >
        {children}
    </button>
);

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
                    <div className="text-gray-500">{item.label}</div>
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
            <div className="text-sm text-gray-600 font-medium">Podsumowanie pozycji</div>
            <div className="overflow-x-auto">
                <table className="min-w-full text-sm">
                    <thead className="text-left text-gray-500">
                        <tr className="border-b border-gray-200">
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
                            <tr key={`${row.symbol}-${idx}`} className="border-b border-gray-100">
                                <td className="py-2 pr-4 font-medium text-gray-900">{row.symbol}</td>
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
            <div className="text-sm text-gray-600 font-medium">Harmonogram rebalansingu</div>
            <div className="space-y-3">
                {events.map((event, idx) => (
                    <div key={`${event.date}-${idx}`} className="rounded-xl border border-gray-200 bg-gray-50/70 p-4">
                        <div className="flex flex-wrap items-center justify-between gap-2">
                            <div className="text-sm font-semibold text-gray-900">{event.date}</div>
                            <div className="text-xs text-gray-500">
                                {event.reason || "Planowy rebalansing"}
                                {typeof event.turnover === "number"
                                    ? ` • obrót ${formatPercent(event.turnover, 1)}`
                                    : ""}
                            </div>
                        </div>
                        {event.trades && event.trades.length > 0 && (
                            <div className="mt-3 overflow-x-auto">
                                <table className="min-w-full text-xs md:text-sm">
                                    <thead className="text-left text-gray-500">
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
                                            <tr key={`${trade.symbol}-${tradeIdx}`} className="border-t border-gray-100">
                                                <td className="py-1 pr-3 font-medium text-gray-900">{trade.symbol}</td>
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
            <div className="text-sm text-gray-500">
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
                            "text-xl leading-none text-gray-400 hover:text-rose-600 focus-visible:text-rose-600",
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
                <div className="text-gray-500">Kurs</div>
                <div className="text-xl font-semibold">{close.toFixed(2)}</div>
            </div>
            <div>
                <div className="text-gray-500">Zmiana (okres)</div>
                <div
                    className={`text-xl font-semibold ${ch >= 0 ? "text-emerald-600" : "text-rose-600"
                        }`}
                >
                    {ch.toFixed(2)} ({chPct.toFixed(1)}%)
                </div>
            </div>
            <div>
                <div className="text-gray-500">Max</div>
                <div className="text-xl font-semibold">{max.toFixed(2)}</div>
            </div>
            <div>
                <div className="text-gray-500">Min</div>
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
                    "px-3 py-2 rounded-xl border bg-white",
                    inputClassName || "w-56",
                ]
                    .filter(Boolean)
                    .join(" ")}
            />
            {open && (
                <div className="absolute z-20 mt-1 w-full rounded-xl border bg-white shadow-lg max-h-72 overflow-auto">
                    {loading && (
                        <div className="px-3 py-2 text-sm text-gray-500">Szukam…</div>
                    )}
                    {!loading && list.length === 0 && (
                        <div className="px-3 py-2 text-sm text-gray-500">Brak wyników</div>
                    )}
                    {!loading &&
                        list.map((row, i) => (
                            <button
                                key={row.symbol}
                                onMouseDown={(e) => {
                                    e.preventDefault();
                                    choose(row.symbol);
                                }}
                                className={`w-full text-left px-3 py-2 text-sm hover:bg-gray-50 ${i === idx ? "bg-gray-100" : ""
                                    }`}
                            >
                                <div className="flex items-center justify-between">
                                    <span className="font-medium">{row.symbol}</span>
                                    <span className="text-gray-500">{row.name}</span>
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
function PriceChart({
    rows,
    showArea,
    showSMA,
}: {
    rows: RowSMA[];
    showArea: boolean;
    showSMA: boolean;
}) {
    return (
        <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
                {showArea ? (
                    <AreaChart data={rows}>
                        <defs>
                            <linearGradient id="g" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.25} />
                                <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                            </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                        <XAxis dataKey="date" tick={{ fontSize: 12 }} tickMargin={8} />
                        <YAxis tick={{ fontSize: 12 }} width={60} />
                        <Tooltip />
                        <Area
                            type="monotone"
                            dataKey="close"
                            stroke="#2563eb"
                            fill="url(#g)"
                            fillOpacity={1}
                        />
                        {showSMA && (
                            <Line
                                type="monotone"
                                dataKey="sma"
                                stroke="#0ea5e9"
                                dot={false}
                                strokeDasharray="4 4"
                            />
                        )}
                    </AreaChart>
                ) : (
                    <LineChart data={rows}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                        <XAxis dataKey="date" tick={{ fontSize: 12 }} tickMargin={8} />
                        <YAxis tick={{ fontSize: 12 }} width={60} />
                        <Tooltip />
                        <Line type="monotone" dataKey="close" stroke="#2563eb" dot={false} />
                        {showSMA && (
                            <Line
                                type="monotone"
                                dataKey="sma"
                                stroke="#0ea5e9"
                                dot={false}
                                strokeDasharray="4 4"
                            />
                        )}
                    </LineChart>
                )}
            </ResponsiveContainer>
        </div>
    );
}

function RsiChart({ rows }: { rows: RowRSI[] }) {
    return (
        <div className="h-40">
            <ResponsiveContainer width="100%" height="100%">
                <LineChart data={rows}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                    <XAxis dataKey="date" tick={{ fontSize: 12 }} tickMargin={8} />
                    <YAxis domain={[0, 100]} tick={{ fontSize: 12 }} width={40} />
                    <Tooltip />
                    <Line type="monotone" dataKey="rsi" stroke="#111827" dot={false} />
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
    const [period, setPeriod] = useState<90 | 180 | 365>(365);
    const [area, setArea] = useState(true);
    const [smaOn, setSmaOn] = useState(true);

    const [rows, setRows] = useState<Row[]>([]);
    const [loading, setLoading] = useState(false);
    const [err, setErr] = useState("");

    // Portfel
    const [pfRows, setPfRows] = useState<{ symbol: string; weight: number }[]>([
        { symbol: "CDR.WA", weight: 40 },
        { symbol: "ORLEN.WA", weight: 30 },
        { symbol: "PKO.WA", weight: 30 },
    ]);
    const [pfStart, setPfStart] = useState("2015-01-01");
    const [pfFreq, setPfFreq] = useState<Rebalance>("monthly");
    const [pfRes, setPfRes] = useState<PortfolioResp | null>(null);
    const pfTotal = pfRows.reduce((a, b) => a + (Number(b.weight) || 0), 0);
    const [pfLoading, setPfLoading] = useState(false);
    const [pfErr, setPfErr] = useState("");

    // Quotes loader
    useEffect(() => {
        let live = true;
        if (!symbol) {
            setRows([]);
            setErr("");
            setLoading(false);
            return;
        }

        (async () => {
            try {
                setLoading(true);
                setErr("");
                const startISO =
                    period === 90
                        ? new Date(Date.now() - 90 * 24 * 3600 * 1000)
                            .toISOString()
                            .slice(0, 10)
                        : period === 180
                            ? new Date(Date.now() - 180 * 24 * 3600 * 1000)
                                .toISOString()
                                .slice(0, 10)
                            : "2015-01-01";
                const data = await fetchQuotes(symbol, startISO);
                if (live) setRows(data);
            } catch (e: unknown) {
                const message = e instanceof Error ? e.message : String(e);
                if (live) {
                    setErr(message);
                    setRows([]);
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

    const symbolLabel = symbol ?? "—";

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
        <div className="min-h-screen bg-gray-50 text-gray-900">
            <header className="max-w-6xl mx-auto px-4 md:px-8 py-6 flex items-center justify-between">
                <h1 className="text-2xl md:text-3xl font-bold">Analityka Rynków</h1>
                <div className="flex gap-2">
                    <button className="px-4 py-2 rounded-xl border">Zaloguj</button>
                    <button className="px-4 py-2 rounded-xl bg-black text-white">
                        Utwórz konto
                    </button>
                </div>
            </header>

            <main className="max-w-6xl mx-auto px-4 md:px-8 space-y-6 pb-10">
                {/* Watchlist */}
                <Card
                    title="Twoja lista obserwacyjna"
                    right={
                        <TickerAutosuggest
                            onPick={(sym) => {
                                setWatch((w) => (w.includes(sym) ? w : [sym, ...w]));
                                setSymbol(sym);
                            }}
                        />
                    }
                >
                    <Watchlist
                        items={watch}
                        current={symbol}
                        onPick={(sym) => setSymbol(sym)}
                        onRemove={removeFromWatch}
                    />
                </Card>

                <div className="grid md:grid-cols-3 gap-6">
                    {/* Lewa kolumna */}
                    <div className="md:col-span-2 space-y-6">
                        {/* Wykres cenowy */}
                        <Card
                            title={symbol ? `${symbol} – wykres cenowy` : "Wykres cenowy"}
                            right={
                                <div className="flex gap-2">
                                    <Chip active={period === 90} onClick={() => setPeriod(90)}>
                                        3M
                                    </Chip>
                                    <Chip active={period === 180} onClick={() => setPeriod(180)}>
                                        6M
                                    </Chip>
                                    <Chip active={period === 365} onClick={() => setPeriod(365)}>
                                        1R
                                    </Chip>
                                    <Chip active={area} onClick={() => setArea(!area)}>
                                        Area
                                    </Chip>
                                    <Chip active={smaOn} onClick={() => setSmaOn(!smaOn)}>
                                        SMA 20
                                    </Chip>
                                </div>
                            }
                        >
                            {!symbol ? (
                                <div className="p-6 text-sm text-gray-500">
                                    Dodaj spółkę do listy obserwacyjnej, aby zobaczyć wykres.
                                </div>
                            ) : loading ? (
                                <div className="p-6 text-sm text-gray-500">
                                    Ładowanie danych z API…
                                </div>
                            ) : rows.length ? (
                                <>
                                    <Stats data={rows} />
                                    <div className="h-2" />
                                    <PriceChart rows={withSma} showArea={area} showSMA={smaOn} />
                                </>
                            ) : (
                                <div className="p-6 text-sm text-gray-500">
                                    Brak danych do wyświetlenia
                                </div>
                            )}
                            {err && symbol && (
                                <div className="mt-3 text-sm text-rose-600">Błąd: {err}</div>
                            )}
                        </Card>

                        {/* RSI */}
                        <Card title="RSI (14)">
                            {!symbol ? (
                                <div className="p-6 text-sm text-gray-500">
                                    Dodaj spółkę, aby zobaczyć wskaźnik RSI.
                                </div>
                            ) : (
                                <RsiChart rows={withRsi} />
                            )}
                        </Card>

                        {/* Portfel */}
                        <Card title="Portfel – symulacja & rebalansing">
                            <div className="space-y-6">
                                <div className="grid gap-6 md:grid-cols-[minmax(0,2fr)_minmax(0,1fr)]">
                                    {/* Skład portfela */}
                                    <div className="space-y-3">
                                        <div className="text-sm text-gray-600">Skład portfela</div>
                                        {pfRows.map((r, i) => (
                                            <div
                                                key={i}
                                                className="flex flex-wrap items-center gap-3 rounded-xl border border-gray-200 bg-gray-50/60 px-3 py-3"
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
                                                        className="w-24 md:w-20 px-3 py-2 rounded-xl border"
                                                    />
                                                    <span className="text-sm text-gray-500">%</span>
                                                    <button
                                                        onClick={() =>
                                                            setPfRows((rows) =>
                                                                rows.filter((_, idx) => idx !== i)
                                                            )
                                                        }
                                                        className="px-2 py-1 text-sm rounded-lg border"
                                                        title="Usuń"
                                                    >
                                                        ✕
                                                    </button>
                                                </div>
                                            </div>
                                        ))}
                                        <button
                                            onClick={() =>
                                                setPfRows((rows) => [...rows, { symbol: "", weight: 0 }])
                                            }
                                            className="w-full sm:w-auto px-3 py-2 rounded-xl border"
                                        >
                                            Dodaj pozycję
                                        </button>
                                        <div
                                            className={`text-sm ${
                                                pfTotal === 100 ? "text-emerald-600" : "text-rose-600"
                                            }`}
                                        >
                                            Suma wag: <b>{pfTotal}%</b>{" "}
                                            {pfTotal === 100 ? "(OK)" : "(docelowo 100%)"}
                                        </div>
                                    </div>

                                    {/* Ustawienia symulacji */}
                                    <div className="space-y-4">
                                        <div className="grid gap-3 text-sm sm:grid-cols-2">
                                            <label className="flex flex-col gap-1">
                                                <span className="text-gray-600">Data startu</span>
                                                <input
                                                    type="date"
                                                    value={pfStart}
                                                    onChange={(e) => setPfStart(e.target.value)}
                                                    className="px-3 py-2 rounded-xl border"
                                                />
                                            </label>
                                            <label className="flex flex-col gap-1">
                                                <span className="text-gray-600">Rebalansing</span>
                                                <select
                                                    value={pfFreq}
                                                    onChange={(e) =>
                                                        setPfFreq(e.target.value as Rebalance)
                                                    }
                                                    className="px-3 py-2 rounded-xl border"
                                                >
                                                    <option value="none">Brak</option>
                                                    <option value="monthly">Miesięczny</option>
                                                    <option value="quarterly">Kwartalny</option>
                                                    <option value="yearly">Roczny</option>
                                                </select>
                                            </label>
                                        </div>
                                        <button
                                            disabled={
                                                pfTotal !== 100 || pfRows.some((r2) => !r2.symbol) || pfLoading
                                            }
                                            onClick={async () => {
                                                try {
                                                    setPfErr("");
                                                    setPfLoading(true);
                                                    setPfRes(null);
                                                    const symbols = pfRows.map((r2) => r2.symbol);
                                                    const weights = pfRows.map((r2) => Number(r2.weight));
                                                    const res = await backtestPortfolio(
                                                        symbols,
                                                        weights,
                                                        pfStart,
                                                        pfFreq
                                                    );
                                                    setPfRes(res);
                                                } catch (e: unknown) {
                                                    const message =
                                                        e instanceof Error ? e.message : String(e);
                                                    setPfErr(message);
                                                } finally {
                                                    setPfLoading(false);
                                                }
                                            }}
                                            className="w-full md:w-auto px-4 py-2 rounded-xl bg-black text-white disabled:opacity-50"
                                        >
                                            {pfLoading ? "Liczenie…" : "Symuluj portfel"}
                                        </button>
                                        {pfErr && (
                                            <div className="text-sm text-rose-600">Błąd: {pfErr}</div>
                                        )}
                                    </div>
                                </div>

                                {/* Wynik + wykres */}
                                <div>
                                    {!pfRes ? (
                                        <div className="text-sm text-gray-600">
                                            Skonfiguruj portfel (symbole + wagi), wybierz datę startu i
                                            rebalansing, potem uruchom symulację.
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
                                                    <LineChart data={pfRes.equity}>
                                                        <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                                                        <XAxis
                                                            dataKey="date"
                                                            tick={{ fontSize: 12 }}
                                                            tickMargin={8}
                                                        />
                                                        <YAxis tick={{ fontSize: 12 }} width={60} />
                                                        <Tooltip />
                                                        <Line
                                                            type="monotone"
                                                            dataKey="value"
                                                            stroke="#111827"
                                                            dot={false}
                                                        />
                                                    </LineChart>
                                                </ResponsiveContainer>
                                            </div>
                                            {pfRes.rebalances && pfRes.rebalances.length > 0 && (
                                                <div className="mt-6">
                                                    <RebalanceTimeline events={pfRes.rebalances} />
                                                </div>
                                            )}
                                            <div className="text-xs text-gray-500 mt-2">
                                                Symulacja startuje z wartości 1.0, rebalansing: {pfFreq}.
                                                Wagi są normalizowane do 100%.
                                            </div>
                                        </>
                                    )}
                                </div>
                            </div>
                        </Card>
                    </div>

                    {/* Prawa kolumna */}
                    <div className="space-y-6">
                        <Card title={`Fundamenty – ${symbolLabel}`}>
                            <div className="text-sm text-gray-500">
                                {symbol
                                    ? "Dane przykładowe — podłączymy realne API fundamentów w kolejnym kroku."
                                    : "Dodaj spółkę, aby zobaczyć sekcję fundamentów."}
                            </div>
                            {symbol && (
                                <div className="mt-4 grid grid-cols-2 gap-y-2 text-sm">
                                    <div className="text-gray-500">Kapitalizacja</div>
                                    <div>$—</div>
                                    <div className="text-gray-500">P/E (TTM)</div>
                                    <div>—</div>
                                    <div className="text-gray-500">Przychody</div>
                                    <div>—</div>
                                    <div className="text-gray-500">Marża netto</div>
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
                            <p className="text-xs text-gray-500 mt-3">
                                Podmienimy na realny backend skanera.
                            </p>
                        </Card>
                    </div>
                </div>

                <footer className="pt-6 text-center text-sm text-gray-500">
                    © {new Date().getFullYear()} Analityka Rynków • MVP
                </footer>
            </main>
        </div>
    );
}
