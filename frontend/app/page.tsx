"use client";

import React, { useMemo, useState, useEffect, useRef } from "react";
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
    Brush,
} from "recharts";

/** ====== KONFIG ====== **/
const API = process.env.NEXT_PUBLIC_API_BASE || "/api"; // prod: pełny URL, dev: proxy

/** ====== TYPY ====== **/
type SymbolRow = { symbol: string; name: string };
type Row = { date: string; open: number; high: number; low: number; close: number; volume: number };

type PortfolioPoint = { date: string; value: number };
type PortfolioResp = {
    equity: PortfolioPoint[];
    stats: {
        cagr: number;
        max_drawdown: number;
        volatility: number;
        sharpe: number;
        last_value: number;
    };
};

/** ====== API HELPERS ====== **/
async function searchSymbols(q: string): Promise<SymbolRow[]> {
    const r = await fetch(`${API}/symbols?q=${encodeURIComponent(q)}`);
    if (!r.ok) throw new Error(`API /symbols ${r.status}`);
    return r.json();
}
async function fetchQuotes(symbol: string, start = "2015-01-01"): Promise<Row[]> {
    const r = await fetch(
        `${API}/quotes?symbol=${encodeURIComponent(symbol)}&start=${encodeURIComponent(start)}`
    );
    if (!r.ok) throw new Error(`API /quotes ${r.status}`);
    return r.json();
}
async function backtestPortfolio(
    symbols: string[],
    weightsPct: number[],
    start: string,
    rebalance: "none" | "monthly" | "quarterly" | "yearly"
): Promise<PortfolioResp> {
    const qs = new URLSearchParams({
        symbols: symbols.join(","),
        weights: weightsPct.join(","),
        start,
        rebalance,
    });
    const r = await fetch(`${API}/backtest/portfolio?${qs.toString()}`);
    if (!r.ok) throw new Error(`API /backtest/portfolio ${r.status}`);
    return r.json();
}

/** ====== WSKAŹNIKI ====== **/
function sma(rows: Row[], w = 20) {
    const out = rows.map((d, i) => {
        if (i < w - 1) return { ...d, sma: null as number | null };
        const slice = rows.slice(i - w + 1, i + 1);
        const avg = slice.reduce((a, b) => a + b.close, 0) / w;
        return { ...d, sma: Number(avg.toFixed(2)) };
    });
    return out as (Row & { sma: number | null })[];
}
function rsi(rows: Row[], p = 14) {
    let g = 0,
        l = 0;
    const out = rows.map((d, i) => {
        if (i === 0) return { ...d, rsi: null as number | null };
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
    return out as (Row & { rsi: number | null })[];
}

/** ====== UI POMOCE ====== **/
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
            <div className="px-4 md:px-6 py-3 border-b border-gray-100 flex flex-wrap items-center justify-between gap-3">
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

/** ====== AUTOSUGGEST ====== **/
function TickerAutosuggest({
    value,
    onPick,
    placeholder = "Dodaj ticker (np. CDR.WA)",
    className = "",
}: {
    value?: string;
    onPick: (symbol: string) => void;
    placeholder?: string;
    className?: string;
}) {
    const [q, setQ] = useState(value || "");
    const [list, setList] = useState<SymbolRow[]>([]);
    const [open, setOpen] = useState(false);
    const [idx, setIdx] = useState(-1);
    const [loading, setLoading] = useState(false);

    useEffect(() => setQ(value || ""), [value]);

    // debounce search
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
            } catch {
                setList([]);
            } finally {
                setLoading(false);
            }
        }, 220);
        return () => clearTimeout(h);
    }, [q]);

    function choose(s: string) {
        onPick(s);
        setQ(s);
        setOpen(false);
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
        <div className={`relative ${className}`}>
            <input
                value={q}
                onChange={(e) => setQ(e.target.value)}
                onFocus={() => list.length && setOpen(true)}
                onKeyDown={onKeyDown}
                placeholder={placeholder}
                className="px-3 py-2 rounded-xl border bg-white w-full"
            />
            {open && (
                <div className="absolute z-20 mt-1 w-full rounded-xl border bg-white shadow-lg max-h-72 overflow-auto">
                    {loading && <div className="px-3 py-2 text-sm text-gray-500">Szukam…</div>}
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

/** ====== WYKRESY ====== **/
function PriceChart({
    rows,
    showArea,
    showSMA,
}: {
    rows: (Row & { sma?: number | null })[];
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
                        <Area type="monotone" dataKey="close" stroke="#2563eb" fill="url(#g)" fillOpacity={1} />
                        {showSMA && (
                            <Line
                                type="monotone"
                                dataKey="sma"
                                stroke="#0ea5e9"
                                dot={false}
                                strokeDasharray="4 4"
                            />
                        )}
                        <Brush dataKey="date" height={18} stroke="#9ca3af" />
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
                        <Brush dataKey="date" height={18} stroke="#9ca3af" />
                    </LineChart>
                )}
            </ResponsiveContainer>
        </div>
    );
}

function RsiChart({ rows }: { rows: (Row & { rsi: number | null })[] }) {
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

/** ====== STRONA ====== **/
export default function Page() {
    /** Watchlista + wykres pojedynczej spółki */
    const [watch, setWatch] = useState(["CDR.WA", "ORLEN.WA", "PKO.WA", "PZU.WA", "KGH.WA"]);
    const [symbol, setSymbol] = useState(watch[0]);
    const [period, setPeriod] = useState<90 | 180 | 365>(365);
    const [area, setArea] = useState(true);
    const [smaOn, setSmaOn] = useState(true);
    const [rows, setRows] = useState<Row[]>([]);
    const [loading, setLoading] = useState(false);
    const [err, setErr] = useState("");

    useEffect(() => {
        let live = true;
        (async () => {
            try {
                setLoading(true);
                setErr("");
                const startISO =
                    period === 90
                        ? new Date(Date.now() - 90 * 86400000).toISOString().slice(0, 10)
                        : period === 180
                            ? new Date(Date.now() - 180 * 86400000).toISOString().slice(0, 10)
                            : "2015-01-01";
                const data = await fetchQuotes(symbol, startISO);
                if (live) setRows(data);
            } catch (e: any) {
                if (live) {
                    setErr(e?.message || String(e));
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

    const withSma = useMemo(() => (smaOn ? sma(rows, 20) : rows), [rows, smaOn]);
    const withRsi = useMemo(() => rsi(rows, 14), [rows]);

    /** PORTFEL + PORÓWNANIE */
    const [pfRows, setPfRows] = useState<{ symbol: string; weight: number }[]>([
        { symbol: "CDR.WA", weight: 40 },
        { symbol: "ORLEN.WA", weight: 30 },
        { symbol: "PKO.WA", weight: 30 },
    ]);
    const [pfStart, setPfStart] = useState("2015-01-01");
    const [pfFreq, setPfFreq] = useState<"none" | "monthly" | "quarterly" | "yearly">("quarterly");
    const [pfInit, setPfInit] = useState<number>(10000);
    const [pfRes, setPfRes] = useState<PortfolioResp | null>(null);
    const [pfLoading, setPfLoading] = useState(false);
    const [pfErr, setPfErr] = useState("");
    const pfTotal = pfRows.reduce((a, b) => a + (Number(b.weight) || 0), 0);

    // porównanie z pojedynczą spółką
    const [cmpSym, setCmpSym] = useState<string>("PKO.WA");
    const [cmpData, setCmpData] = useState<{ date: string; value: number }[] | null>(null);

    // po udanej symulacji dociągamy serię porównawczą
    useEffect(() => {
        (async () => {
            if (!pfRes || !cmpSym) {
                setCmpData(null);
                return;
            }
            try {
                const q = await fetchQuotes(cmpSym, pfStart);
                if (!q.length) {
                    setCmpData(null);
                    return;
                }
                // normalizacja: start = 1.0
                const first = q[0].close;
                const norm = q.map((d) => ({ date: d.date, value: d.close / first }));
                // dopasowanie po datach do serii portfela
                const eqDates = new Set(pfRes.equity.map((e) => e.date));
                const aligned = norm.filter((n) => eqDates.has(n.date));
                setCmpData(aligned);
            } catch {
                setCmpData(null);
            }
        })();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [pfRes?.equity?.length, cmpSym, pfStart]);

    // equity przeskalowana do wartości początkowej
    const pfEquityScaled = useMemo(() => {
        if (!pfRes) return [];
        return pfRes.equity.map((e) => ({ date: e.date, value: Number((e.value * (pfInit || 0)).toFixed(2)) }));
    }, [pfRes, pfInit]);

    // porównanie także przeskalowane do tej samej bazy
    const cmpScaled = useMemo(() => {
        if (!cmpData || !pfRes) return null;
        return cmpData.map((c) => ({ date: c.date, value: Number((c.value * (pfInit || 0)).toFixed(2)) }));
    }, [cmpData, pfRes, pfInit]);

    // własny tooltip: % zmiany od początku dla portfela i porównania
    const firstVal = pfEquityScaled.length ? pfEquityScaled[0].value : null;
    const firstCmp = cmpScaled && cmpScaled.length ? cmpScaled[0].value : null;
    const TooltipContent = ({ active, label, payload }: any) => {
        if (!active || !payload?.length || firstVal == null) return null;
        const pVal = payload.find((p: any) => p.dataKey === "value")?.value as number | undefined;
        const cVal = payload.find((p: any) => p.dataKey === "cmp")?.value as number | undefined;
        const pCh = pVal != null ? ((pVal - firstVal) / firstVal) * 100 : null;
        const cCh = cVal != null && firstCmp != null ? ((cVal - firstCmp) / firstCmp) * 100 : null;
        return (
            <div className="rounded-lg border bg-white px-3 py-2 text-xs shadow">
                <div className="font-medium text-gray-900">{label}</div>
                {pVal != null && (
                    <div className="mt-1">
                        Portfel: <b>{pVal.toLocaleString()} PLN</b>{" "}
                        <span className={pCh != null && pCh >= 0 ? "text-emerald-600" : "text-rose-600"}>
                            ({pCh != null ? pCh.toFixed(2) : "—"}%)
                        </span>
                    </div>
                )}
                {cVal != null && (
                    <div>
                        {cmpSym}: <b>{cVal.toLocaleString()} PLN</b>{" "}
                        <span className={cCh != null && cCh >= 0 ? "text-emerald-600" : "text-rose-600"}>
                            ({cCh != null ? cCh.toFixed(2) : "—"}%)
                        </span>
                    </div>
                )}
            </div>
        );
    };

    return (
        <div className="min-h-screen bg-gray-50 text-gray-900">
            <header className="max-w-6xl mx-auto px-4 md:px-8 py-6 flex items-center justify-between">
                <h1 className="text-2xl md:text-3xl font-bold">Analityka Rynków</h1>
                <div className="flex gap-2">
                    <a
                        className="px-4 py-2 rounded-xl border"
                        href="https://vercel.com"
                        target="_blank"
                        rel="noreferrer"
                    >
                        Wersja demo
                    </a>
                    <button className="px-4 py-2 rounded-xl bg-black text-white">Utwórz konto</button>
                </div>
            </header>

            <main className="max-w-6xl mx-auto px-4 md:px-8 space-y-6 pb-10">
                {/* Watchlista + wykres spółki */}
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
                    <div className="flex flex-wrap gap-2">
                        {watch.map((s) => (
                            <Chip key={s} active={s === symbol} onClick={() => setSymbol(s)}>
                                {s}
                            </Chip>
                        ))}
                    </div>
                </Card>

                <div className="grid md:grid-cols-3 gap-6">
                    <div className="md:col-span-2 space-y-6">
                        <Card
                            title={`${symbol} – wykres cenowy`}
                            right={
                                <div className="flex flex-wrap gap-2">
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
                            {loading ? (
                                <div className="p-6 text-sm text-gray-500">Ładowanie danych z API…</div>
                            ) : rows.length ? (
                                <PriceChart rows={withSma as any} showArea={area} showSMA={smaOn} />
                            ) : (
                                <div className="p-6 text-sm text-gray-500">Brak danych do wyświetlenia</div>
                            )}
                            {err && <div className="mt-3 text-sm text-rose-600">Błąd: {err}</div>}
                        </Card>

                        {/* Fundamenty + Skaner poniżej pierwszego wykresu */}
                        <div className="grid md:grid-cols-2 gap-6">
                            <Card title={`Fundamenty – ${symbol}`}>
                                <div className="text-sm text-gray-500">
                                    Dane przykładowe — podłączymy realne API fundamentów w kolejnym kroku.
                                </div>
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
                            </Card>

                            <Card title="Skaner (demo)">
                                <ul className="text-sm list-disc pl-5 space-y-1">
                                    <li>Wysoki wolumen vs 20-sesyjna średnia</li>
                                    <li>RSI &lt; 30 (wyprzedanie)</li>
                                    <li>Przebicie SMA50 od dołu</li>
                                    <li>Nowe 52-tygodniowe maksimum</li>
                                </ul>
                                <p className="text-xs text-gray-500 mt-3">Podmienimy na realny backend skanera.</p>
                            </Card>
                        </div>

                        {/* RSI */}
                        <Card title="RSI (14)">
                            <RsiChart rows={withRsi as any} />
                        </Card>

                        {/* PORTFEL */}
                        <Card title="Portfel – symulacja & rebalansing">
                            {/* Kontrolki w kolumnie, wykres POD kontrolkami (jak prosiłeś) */}
                            <div className="grid md:grid-cols-2 gap-6">
                                <div className="space-y-4">
                                    {/* Skład portfela */}
                                    <div>
                                        <div className="text-sm text-gray-600 mb-2">Skład portfela</div>
                                        <div className="space-y-2">
                                            {pfRows.map((r, i) => (
                                                <div key={i} className="grid grid-cols-[1fr,88px,32px,32px] gap-2">
                                                    <TickerAutosuggest
                                                        value={r.symbol}
                                                        onPick={(sym) =>
                                                            setPfRows((rows) =>
                                                                rows.map((x, idx) => (idx === i ? { ...x, symbol: sym } : x))
                                                            )
                                                        }
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
                                                                    idx === i ? { ...x, weight: Number(e.target.value) } : x
                                                                )
                                                            )
                                                        }
                                                        className="px-3 py-2 rounded-xl border"
                                                    />
                                                    <div className="flex items-center justify-center text-sm text-gray-500">
                                                        %
                                                    </div>
                                                    <button
                                                        onClick={() => setPfRows((rows) => rows.filter((_, idx) => idx !== i))}
                                                        className="px-2 py-1 text-sm rounded-lg border"
                                                        title="Usuń"
                                                    >
                                                        ✕
                                                    </button>
                                                </div>
                                            ))}
                                            <button
                                                onClick={() => setPfRows((rows) => [...rows, { symbol: "", weight: 0 }])}
                                                className="px-3 py-2 rounded-xl border"
                                            >
                                                Dodaj pozycję
                                            </button>
                                            <div
                                                className={`text-sm ${pfTotal === 100 ? "text-emerald-600" : "text-rose-600"
                                                    }`}
                                            >
                                                Suma wag: <b>{pfTotal}%</b> {pfTotal === 100 ? "(OK)" : "(docelowo 100%)"}
                                            </div>
                                        </div>
                                    </div>

                                    {/* Wartość początkowa / data / rebalans / porównanie */}
                                    <div className="grid sm:grid-cols-2 gap-3">
                                        <label className="flex flex-col gap-1 text-sm">
                                            <span className="text-gray-600">Wartość początkowa</span>
                                            <div className="flex">
                                                <input
                                                    type="number"
                                                    min={0}
                                                    step={100}
                                                    value={pfInit}
                                                    onChange={(e) => setPfInit(Number(e.target.value))}
                                                    className="px-3 py-2 rounded-l-xl border w-full"
                                                />
                                                <span className="px-3 py-2 rounded-r-xl border border-l-0 bg-gray-50 text-gray-600">
                                                    PLN
                                                </span>
                                            </div>
                                        </label>
                                        <label className="flex flex-col gap-1 text-sm">
                                            <span className="text-gray-600">Data startu</span>
                                            <input
                                                type="date"
                                                value={pfStart}
                                                onChange={(e) => setPfStart(e.target.value)}
                                                className="px-3 py-2 rounded-xl border"
                                            />
                                        </label>
                                        <label className="flex flex-col gap-1 text-sm">
                                            <span className="text-gray-600">Rebalansing</span>
                                            <select
                                                value={pfFreq}
                                                onChange={(e) => setPfFreq(e.target.value as any)}
                                                className="px-3 py-2 rounded-xl border"
                                            >
                                                <option value="none">Brak</option>
                                                <option value="monthly">Miesięczny</option>
                                                <option value="quarterly">Kwartalny</option>
                                                <option value="yearly">Roczny</option>
                                            </select>
                                        </label>
                                        <label className="flex flex-col gap-1 text-sm">
                                            <span className="text-gray-600">Porównaj ze spółką</span>
                                            <TickerAutosuggest
                                                value={cmpSym}
                                                onPick={setCmpSym}
                                                placeholder="np. PKO.WA"
                                            />
                                        </label>
                                    </div>

                                    {/* Przycisk */}
                                    <div className="pt-1">
                                        <button
                                            disabled={pfTotal !== 100 || pfRows.some((r) => !r.symbol) || pfLoading}
                                            onClick={async () => {
                                                try {
                                                    setPfErr("");
                                                    setPfLoading(true);
                                                    setPfRes(null);
                                                    const symbols = pfRows.map((r) => r.symbol);
                                                    const weights = pfRows.map((r) => Number(r.weight));
                                                    const res = await backtestPortfolio(symbols, weights, pfStart, pfFreq);
                                                    setPfRes(res);
                                                } catch (e: any) {
                                                    setPfErr(e?.message || String(e));
                                                } finally {
                                                    setPfLoading(false);
                                                }
                                            }}
                                            className="mt-2 px-4 py-2 rounded-xl bg-black text-white disabled:opacity-50"
                                        >
                                            {pfLoading ? "Liczenie…" : "Symuluj portfel"}
                                        </button>
                                        {pfErr && <div className="text-sm text-rose-600 mt-2">Błąd: {pfErr}</div>}
                                    </div>
                                </div>

                                {/* Wykres i metryki POD kontrolkami (zawsze w tej kolumnie) */}
                                <div className="space-y-4">
                                    {!pfRes ? (
                                        <div className="text-sm text-gray-600">
                                            Skonfiguruj portfel, wybierz start i rebalansing — uruchom symulację.
                                        </div>
                                    ) : (
                                        <>
                                            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
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

                                            <div className="h-80">
                                                <ResponsiveContainer width="100%" height="100%">
                                                    <LineChart data={mergeSeriesForChart(pfEquityScaled, cmpScaled)}>
                                                        <CartesianGrid strokeDasharray="3 3" stroke="#eee" />
                                                        <XAxis dataKey="date" tick={{ fontSize: 12 }} tickMargin={8} />
                                                        <YAxis tick={{ fontSize: 12 }} width={70} />
                                                        <Tooltip content={<TooltipContent />} />
                                                        <Line type="monotone" dataKey="value" stroke="#111827" dot={false} name="Portfel" />
                                                        {cmpScaled && (
                                                            <Line
                                                                type="monotone"
                                                                dataKey="cmp"
                                                                stroke="#2563eb"
                                                                dot={false}
                                                                name={cmpSym}
                                                                strokeDasharray="4 4"
                                                            />
                                                        )}
                                                        <Brush dataKey="date" height={20} stroke="#9ca3af" />
                                                    </LineChart>
                                                </ResponsiveContainer>
                                            </div>

                                            <div className="text-xs text-gray-500">
                                                Symulacja w backendzie startuje z wartości 1.0 i jest przeskalowana do{" "}
                                                {pfInit.toLocaleString()} PLN po stronie frontu. Rebalansing: {pfFreq}. Wagi
                                                są normalizowane do 100%.
                                            </div>
                                        </>
                                    )}
                                </div>
                            </div>
                        </Card>
                    </div>

                    {/* Prawa kolumna może zostać pusta albo na przyszłe moduły */}
                    <div className="space-y-6">
                        {/* Miejsce na kolejne funkcje (alerty, zapis portfeli itp.) */}
                    </div>
                </div>

                <footer className="pt-6 text-center text-sm text-gray-500">
                    © {new Date().getFullYear()} Analityka Rynków • MVP
                </footer>
            </main>
        </div>
    );
}

/** ====== POMOC: scalenie serii (portfel + porównanie) po dacie do jednego datasetu dla wykresu */
function mergeSeriesForChart(
    pf: { date: string; value: number }[],
    cmp: { date: string; value: number }[] | null
) {
    if (!cmp) return pf;
    const map = new Map<string, { date: string; value: number; cmp?: number }>();
    pf.forEach((p) => map.set(p.date, { ...p }));
    cmp.forEach((c) => {
        const prev = map.get(c.date);
        if (prev) map.set(c.date, { ...prev, cmp: c.value });
    });
    return Array.from(map.values());
}
