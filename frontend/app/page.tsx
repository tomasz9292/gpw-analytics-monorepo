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
};
type PortfolioResp = { equity: PortfolioPoint[]; stats: PortfolioStats };

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
    const qs = new URLSearchParams({
        symbols: symbols.join(","),
        weights: weightsPct.join(","), // w %
        start,
        rebalance,
    });
    const r = await fetch(`/api/backtest/portfolio?${qs.toString()}`);
    if (!r.ok) throw new Error(`API /backtest/portfolio ${r.status}`);
    return r.json();
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
}: {
    onPick: (symbol: string) => void;
    placeholder?: string;
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
                className="px-3 py-2 rounded-xl border bg-white w-56"
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
                            <div className="grid md:grid-cols-3 gap-6">
                                {/* Konfiguracja portfela */}
                                <div className="space-y-3">
                                    <div className="text-sm text-gray-600">Skład portfela</div>
                                    {pfRows.map((r, i) => (
                                        <div key={i} className="flex items-center gap-2">
                                            <TickerAutosuggest
                                                onPick={(sym) => {
                                                    setPfRows((rows) =>
                                                        rows.map((x, idx) =>
                                                            idx === i ? { ...x, symbol: sym } : x
                                                        )
                                                    );
                                                }}
                                                placeholder={r.symbol || "Symbol"}
                                            />
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
                                                                ? { ...x, weight: Number(e.target.value) }
                                                                : x
                                                        )
                                                    )
                                                }
                                                className="w-20 px-3 py-2 rounded-xl border"
                                            />
                                            <span className="text-sm text-gray-500">%</span>
                                            <button
                                                onClick={() =>
                                                    setPfRows((rows) => rows.filter((_, idx) => idx !== i))
                                                }
                                                className="px-2 py-1 text-sm rounded-lg border"
                                                title="Usuń"
                                            >
                                                ✕
                                            </button>
                                        </div>
                                    ))}
                                    <button
                                        onClick={() =>
                                            setPfRows((rows) => [...rows, { symbol: "", weight: 0 }])
                                        }
                                        className="px-3 py-2 rounded-xl border"
                                    >
                                        Dodaj pozycję
                                    </button>

                                    <div
                                        className={`text-sm mt-1 ${pfTotal === 100 ? "text-emerald-600" : "text-rose-600"
                                            }`}
                                    >
                                        Suma wag: <b>{pfTotal}%</b>{" "}
                                        {pfTotal === 100 ? "(OK)" : "(docelowo 100%)"}
                                    </div>

                                    <div className="grid grid-cols-2 gap-3 mt-3 text-sm">
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
                                        className="mt-2 px-4 py-2 rounded-xl bg-black text-white disabled:opacity-50"
                                    >
                                        {pfLoading ? "Liczenie…" : "Symuluj portfel"}
                                    </button>
                                    {pfErr && (
                                        <div className="text-sm text-rose-600 mt-2">Błąd: {pfErr}</div>
                                    )}
                                </div>

                                {/* Wynik + wykres */}
                                <div className="md:col-span-2">
                                    {!pfRes ? (
                                        <div className="text-sm text-gray-600 mb-2">
                                            Skonfiguruj portfel (symbole + wagi), wybierz datę startu
                                            i rebalansing, potem uruchom symulację.
                                        </div>
                                    ) : (
                                        <>
                                            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm mb-4">
                                                <div>
                                                    <div className="text-gray-500">CAGR</div>
                                                    <div className="text-lg font-semibold">
                                                        {(pfRes.stats.cagr * 100).toFixed(2)}%
                                                    </div>
                                                </div>
                                                <div>
                                                    <div className="text-gray-500">Max DD</div>
                                                    <div className="text-lg font-semibold">
                                                        {(pfRes.stats.max_drawdown * 100).toFixed(1)}%
                                                    </div>
                                                </div>
                                                <div>
                                                    <div className="text-gray-500">Vol</div>
                                                    <div className="text-lg font-semibold">
                                                        {(pfRes.stats.volatility * 100).toFixed(1)}%
                                                    </div>
                                                </div>
                                                <div>
                                                    <div className="text-gray-500">Sharpe</div>
                                                    <div className="text-lg font-semibold">
                                                        {pfRes.stats.sharpe.toFixed(2)}
                                                    </div>
                                                </div>
                                            </div>
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
