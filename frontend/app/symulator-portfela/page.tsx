"use client";

import { useMemo, useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type SimulationMode = "manual" | "auto";
type RebalanceMode = "none" | "monthly" | "quarterly" | "yearly";
type WeightingMode = "equal" | "score";
type ScoreDirection = "asc" | "desc";

type ScoreMetric = "total_return" | "volatility" | "max_drawdown" | "sharpe";

type ScoreComponentInput = {
  id: string;
  metric: ScoreMetric;
  lookbackDays: number;
  weight: number;
  direction: ScoreDirection;
};

type PortfolioSimulationRequest = {
  start: string;
  end?: string | null;
  rebalance: RebalanceMode;
  initial_capital: number;
  fee_pct: number;
  threshold_pct: number;
  benchmark?: string | null;
  manual?: {
    symbols: string[];
    weights?: number[];
  } | null;
  auto?: {
    top_n: number;
    weighting: WeightingMode;
    direction: ScoreDirection;
    min_score?: number | null;
    max_score?: number | null;
    components: {
      metric: ScoreMetric;
      lookback_days: number;
      weight: number;
      direction: ScoreDirection;
    }[];
    filters?: {
      include?: string[];
      exclude?: string[];
      prefixes?: string[];
    } | null;
  } | null;
};

type PortfolioEquitySnapshot = {
  date: string;
  value: number;
  return_pct: number;
};

type PortfolioSimulationResponse = {
  summary: {
    start: string;
    end: string;
    initial_value: number;
    final_value: number;
    total_return_pct: number;
    cagr_pct: number;
    volatility_pct: number;
    max_drawdown_pct: number;
    sharpe: number;
    turnover_pct: number | null;
    trades_count: number | null;
    fees_paid: number | null;
  };
  equity_curve: PortfolioEquitySnapshot[];
  allocations: {
    symbol: string;
    target_weight: number;
    realized_weight?: number | null;
    return_pct?: number | null;
    contribution_pct?: number | null;
    value?: number | null;
  }[];
  rebalances: {
    date: string;
    reason?: string | null;
    turnover?: number | null;
    trades?: {
      symbol: string;
      action?: string | null;
      weight_change?: number | null;
      value_change?: number | null;
      target_weight?: number | null;
      shares_change?: number | null;
      price?: number | null;
      shares_after?: number | null;
      note?: string | null;
    }[] | null;
  }[];
};

type MetricDescriptor = {
  value: ScoreMetric;
  label: string;
  description: string;
  defaultLookback: number;
  defaultWeight: number;
  defaultDirection: ScoreDirection;
};

const SCORE_METRICS: MetricDescriptor[] = [
  {
    value: "total_return",
    label: "Skumulowana stopa zwrotu",
    description: "Porównuje cenę zamknięcia z wartością sprzed okresu lookback.",
    defaultLookback: 252,
    defaultWeight: 40,
    defaultDirection: "desc",
  },
  {
    value: "volatility",
    label: "Zmienność",
    description: "Roczna odchylenie standardowe dziennych stóp zwrotu.",
    defaultLookback: 126,
    defaultWeight: 20,
    defaultDirection: "asc",
  },
  {
    value: "max_drawdown",
    label: "Maksymalne obsunięcie",
    description: "Największy spadek od szczytu w zadanym oknie.",
    defaultLookback: 252,
    defaultWeight: 20,
    defaultDirection: "asc",
  },
  {
    value: "sharpe",
    label: "Sharpe",
    description: "Relacja stopy zwrotu do ryzyka (przy stopie wolnej od ryzyka 0).",
    defaultLookback: 252,
    defaultWeight: 20,
    defaultDirection: "desc",
  },
];

const createComponentId = () => `comp-${Math.random().toString(36).slice(2, 10)}`;

const DEFAULT_AUTO_COMPONENTS: ScoreComponentInput[] = [
  {
    id: createComponentId(),
    metric: "total_return",
    lookbackDays: 252,
    weight: 40,
    direction: "desc",
  },
  {
    id: createComponentId(),
    metric: "volatility",
    lookbackDays: 126,
    weight: 20,
    direction: "asc",
  },
  {
    id: createComponentId(),
    metric: "max_drawdown",
    lookbackDays: 252,
    weight: 20,
    direction: "asc",
  },
];

const REBALANCE_OPTIONS: { value: RebalanceMode; label: string }[] = [
  { value: "monthly", label: "Miesięcznie" },
  { value: "quarterly", label: "Kwartalnie" },
  { value: "yearly", label: "Rocznie" },
  { value: "none", label: "Brak" },
];

const formatPercent = (value: number | null | undefined, fractionDigits = 2) => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return `${(value * 100).toFixed(fractionDigits)}%`;
};

const formatNumber = (value: number | null | undefined, fractionDigits = 2) => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return value.toLocaleString("pl-PL", {
    minimumFractionDigits: 0,
    maximumFractionDigits: fractionDigits,
  });
};

const parseSymbols = (value: string): string[] =>
  value
    .split(/[\s,;\n]+/)
    .map((token) => token.trim().toUpperCase())
    .filter((token) => token.length > 0);

const clampNumber = (value: number, min: number, max: number) =>
  Math.min(Math.max(value, min), max);

export default function PortfolioSimulatorPage() {
  const [mode, setMode] = useState<SimulationMode>("manual");
  const [start, setStart] = useState("2018-01-01");
  const [end, setEnd] = useState<string | null>(null);
  const [rebalance, setRebalance] = useState<RebalanceMode>("monthly");
  const [initialCapital, setInitialCapital] = useState(10000);
  const [feePct, setFeePct] = useState(0.0);
  const [thresholdPct, setThresholdPct] = useState(0.0);
  const [manualSymbols, setManualSymbols] = useState("CDR.WA, PKN.WA, PZU.WA");
  const [manualWeights, setManualWeights] = useState("");
  const [autoTopN, setAutoTopN] = useState(5);
  const [autoWeighting, setAutoWeighting] = useState<WeightingMode>("equal");
  const [autoDirection, setAutoDirection] = useState<ScoreDirection>("desc");
  const [autoUniverse, setAutoUniverse] = useState("");
  const [autoComponents, setAutoComponents] = useState<ScoreComponentInput[]>(
    DEFAULT_AUTO_COMPONENTS,
  );
  const [minScore, setMinScore] = useState<string>("");
  const [maxScore, setMaxScore] = useState<string>("");

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<PortfolioSimulationResponse | null>(null);

  const manualSymbolsList = useMemo(() => parseSymbols(manualSymbols), [manualSymbols]);

  const addAutoComponent = () => {
    const preset = SCORE_METRICS[autoComponents.length % SCORE_METRICS.length];
    setAutoComponents((prev) => [
      ...prev,
      {
        id: createComponentId(),
        metric: preset.value,
        lookbackDays: preset.defaultLookback,
        weight: preset.defaultWeight,
        direction: preset.defaultDirection,
      },
    ]);
  };

  const updateComponent = (id: string, patch: Partial<ScoreComponentInput>) => {
    setAutoComponents((prev) =>
      prev.map((component) =>
        component.id === id ? { ...component, ...patch } : component,
      ),
    );
  };

  const removeComponent = (id: string) => {
    setAutoComponents((prev) => prev.filter((component) => component.id !== id));
  };

  const handleSubmit = async () => {
    setIsLoading(true);
    setError(null);

    const payload: PortfolioSimulationRequest = {
      start,
      rebalance,
      initial_capital: Number(initialCapital) || 0,
      fee_pct: Number(feePct) / 100,
      threshold_pct: Number(thresholdPct) / 100,
    };

    if (end && end.trim().length > 0) {
      payload.end = end;
    }

    if (mode === "manual") {
      const weightsList = parseSymbols(manualWeights).map((item) => Number(item));
      payload.manual = {
        symbols: manualSymbolsList,
        weights: weightsList.length > 0 ? weightsList : undefined,
      };
      payload.auto = null;
    } else {
      payload.manual = null;
      payload.auto = {
        top_n: clampNumber(Number(autoTopN) || 1, 1, 5000),
        weighting: autoWeighting,
        direction: autoDirection,
        components: autoComponents.map((component) => ({
          metric: component.metric,
          lookback_days: clampNumber(Number(component.lookbackDays) || 1, 1, 3650),
          weight: clampNumber(Number(component.weight) || 1, 1, 10),
          direction: component.direction,
        })),
      };

      const minScoreNumber = Number(minScore);
      if (!Number.isNaN(minScoreNumber) && minScore.trim().length > 0) {
        payload.auto.min_score = minScoreNumber;
      }
      const maxScoreNumber = Number(maxScore);
      if (!Number.isNaN(maxScoreNumber) && maxScore.trim().length > 0) {
        payload.auto.max_score = maxScoreNumber;
      }

      const universeList = parseSymbols(autoUniverse);
      if (universeList.length > 0) {
        payload.auto.filters = { include: universeList };
      }
    }

    try {
      const response = await fetch("/api/portfolio/simulate", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || "Nie udało się uruchomić symulacji");
      }
      const data: PortfolioSimulationResponse = await response.json();
      setResult(data);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Nieznany błąd";
      setError(message);
      setResult(null);
    } finally {
      setIsLoading(false);
    }
  };

  const chartData = useMemo(() => {
    if (!result) {
      return [];
    }
    return result.equity_curve.map((point) => ({
      date: point.date,
      value: point.value,
      returnPercent: point.return_pct * 100,
    }));
  }, [result]);

  const summaryCards = useMemo(() => {
    if (!result) {
      return [];
    }
    const { summary } = result;
    return [
      {
        label: "Zwrot całkowity",
        value: formatPercent(summary.total_return_pct),
      },
      {
        label: "CAGR",
        value: formatPercent(summary.cagr_pct),
      },
      {
        label: "Volatility",
        value: formatPercent(summary.volatility_pct),
      },
      {
        label: "Max drawdown",
        value: formatPercent(summary.max_drawdown_pct),
      },
      {
        label: "Sharpe",
        value: summary.sharpe.toFixed(2),
      },
      {
        label: "Obrót",
        value: formatPercent(summary.turnover_pct),
      },
    ];
  }, [result]);

  return (
    <div className="bg-page min-h-[calc(100vh-48px)] pb-16">
      <div className="mx-auto flex w-full max-w-[1200px] flex-col gap-8 px-6 pt-12">
        <header className="flex flex-col gap-2">
          <p className="text-sm uppercase tracking-[0.2em] text-subtle">
            Symulacje portfela GPW
          </p>
          <h1 className="text-3xl font-semibold text-primary">
            Zbuduj i przetestuj własny portfel inwestycyjny
          </h1>
          <p className="max-w-3xl text-base text-muted">
            Nowa odsłona symulatora łączy prosty konfigurator z dynamiczną
            analizą wyników. Przełączaj się między trybem ręcznym i
            automatycznym, aby oceniać strategie oparte na rankingach.
          </p>
        </header>

        <div className="grid gap-8 lg:grid-cols-[360px,1fr]">
          <section className="bg-surface shadow-brand-floating rounded-2xl border border-soft p-6">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold text-neutral">Konfiguracja</h2>
              <div className="flex items-center gap-2 text-sm">
                <button
                  type="button"
                  className={`rounded-full px-3 py-1 text-sm font-medium transition ${mode === "manual" ? "bg-primary text-white" : "bg-soft-surface text-muted"}`}
                  onClick={() => setMode("manual")}
                >
                  Tryb ręczny
                </button>
                <button
                  type="button"
                  className={`rounded-full px-3 py-1 text-sm font-medium transition ${mode === "auto" ? "bg-primary text-white" : "bg-soft-surface text-muted"}`}
                  onClick={() => setMode("auto")}
                >
                  Tryb auto
                </button>
              </div>
            </div>

            <div className="mt-6 flex flex-col gap-6 text-sm">
              <div className="grid gap-3">
                <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                  Data startowa
                </label>
                <input
                  type="date"
                  className="rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                  value={start}
                  onChange={(event) => setStart(event.target.value)}
                />
              </div>

              <div className="grid gap-3">
                <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                  Data końcowa (opcjonalnie)
                </label>
                <input
                  type="date"
                  className="rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                  value={end ?? ""}
                  onChange={(event) => setEnd(event.target.value || null)}
                />
              </div>

              <div className="grid gap-3">
                <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                  Rebalancing
                </label>
                <select
                  className="rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                  value={rebalance}
                  onChange={(event) => setRebalance(event.target.value as RebalanceMode)}
                >
                  {REBALANCE_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </div>

              <div className="grid gap-3">
                <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                  Kapitał początkowy (PLN)
                </label>
                <input
                  type="number"
                  min={0}
                  className="rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                  value={initialCapital}
                  onChange={(event) => setInitialCapital(Number(event.target.value))}
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="grid gap-3">
                  <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                    Próg rebalansu (%)
                  </label>
                  <input
                    type="number"
                    min={0}
                    step={0.1}
                    className="rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                    value={thresholdPct}
                    onChange={(event) => setThresholdPct(Number(event.target.value))}
                  />
                </div>
                <div className="grid gap-3">
                  <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                    Koszt transakcyjny (%)
                  </label>
                  <input
                    type="number"
                    min={0}
                    step={0.01}
                    className="rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                    value={feePct}
                    onChange={(event) => setFeePct(Number(event.target.value))}
                  />
                </div>
              </div>

              {mode === "manual" ? (
                <div className="flex flex-col gap-6">
                  <div className="grid gap-3">
                    <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                      Lista spółek (oddziel przecinkami)
                    </label>
                    <textarea
                      className="h-24 rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                      value={manualSymbols}
                      onChange={(event) => setManualSymbols(event.target.value)}
                    />
                    <p className="text-xs text-subtle">
                      Wpisz ticker w formacie GPW, np. CDR.WA. Zostaną automatycznie
                      zamienione na wielkie litery.
                    </p>
                  </div>
                  <div className="grid gap-3">
                    <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                      Wagi (opcjonalnie)
                    </label>
                    <textarea
                      className="h-16 rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                      value={manualWeights}
                      onChange={(event) => setManualWeights(event.target.value)}
                    />
                    <p className="text-xs text-subtle">
                      Liczby odpowiadają kolejności spółek. Brak wag oznacza równy
                      podział.
                    </p>
                  </div>
                </div>
              ) : (
                <div className="flex flex-col gap-6">
                  <div className="grid gap-3">
                    <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                      Ranking – liczba spółek
                    </label>
                    <input
                      type="number"
                      min={1}
                      className="rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                      value={autoTopN}
                      onChange={(event) => setAutoTopN(Number(event.target.value))}
                    />
                  </div>

                  <div className="grid gap-3">
                    <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                      Wybierz sposób ważenia
                    </label>
                    <select
                      className="rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                      value={autoWeighting}
                      onChange={(event) =>
                        setAutoWeighting(event.target.value as WeightingMode)
                      }
                    >
                      <option value="equal">Równe wagi</option>
                      <option value="score">Proporcjonalnie do score</option>
                    </select>
                  </div>

                  <div className="grid gap-3">
                    <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                      Kierunek sortowania
                    </label>
                    <select
                      className="rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                      value={autoDirection}
                      onChange={(event) =>
                        setAutoDirection(event.target.value as ScoreDirection)
                      }
                    >
                      <option value="desc">Najwyższy score na górze</option>
                      <option value="asc">Najniższy score na górze</option>
                    </select>
                  </div>

                  <div className="grid gap-3">
                    <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                      Filtr wszechświata (tickery)
                    </label>
                    <textarea
                      className="h-20 rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                      value={autoUniverse}
                      onChange={(event) => setAutoUniverse(event.target.value)}
                    />
                    <p className="text-xs text-subtle">
                      Podaj tickery oddzielone przecinkiem, aby ograniczyć ranking do
                      własnej listy obserwacyjnej.
                    </p>
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <div className="grid gap-3">
                      <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                        Min score
                      </label>
                      <input
                        type="number"
                        className="rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                        value={minScore}
                        onChange={(event) => setMinScore(event.target.value)}
                      />
                    </div>
                    <div className="grid gap-3">
                      <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                        Max score
                      </label>
                      <input
                        type="number"
                        className="rounded-lg border border-soft bg-soft-surface px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                        value={maxScore}
                        onChange={(event) => setMaxScore(event.target.value)}
                      />
                    </div>
                  </div>

                  <div className="flex flex-col gap-4">
                    <div className="flex items-center justify-between">
                      <span className="text-xs font-semibold uppercase tracking-wide text-muted">
                        Komponenty score
                      </span>
                      <button
                        type="button"
                        className="rounded-full bg-primary px-3 py-1 text-xs font-medium text-white shadow-brand-floating"
                        onClick={addAutoComponent}
                      >
                        Dodaj metrykę
                      </button>
                    </div>

                    <div className="flex flex-col gap-4">
                      {autoComponents.map((component) => (
                        <div
                          key={component.id}
                          className="rounded-xl border border-soft bg-soft-surface p-4"
                        >
                          <div className="flex items-start justify-between gap-3">
                            <div className="flex-1 space-y-3">
                              <div className="grid gap-2">
                                <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                                  Metryka
                                </label>
                                <select
                                  className="rounded-lg border border-soft bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                                  value={component.metric}
                                  onChange={(event) =>
                                    updateComponent(component.id, {
                                      metric: event.target.value as ScoreMetric,
                                    })
                                  }
                                >
                                  {SCORE_METRICS.map((metric) => (
                                    <option key={metric.value} value={metric.value}>
                                      {metric.label}
                                    </option>
                                  ))}
                                </select>
                              </div>

                              <div className="grid grid-cols-3 gap-3">
                                <div className="grid gap-2">
                                  <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                                    Lookback (dni)
                                  </label>
                                  <input
                                    type="number"
                                    min={1}
                                    className="rounded-lg border border-soft bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                                    value={component.lookbackDays}
                                    onChange={(event) =>
                                      updateComponent(component.id, {
                                        lookbackDays: Number(event.target.value),
                                      })
                                    }
                                  />
                                </div>
                                <div className="grid gap-2">
                                  <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                                    Waga
                                  </label>
                                  <input
                                    type="number"
                                    min={1}
                                    max={10}
                                    className="rounded-lg border border-soft bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                                    value={component.weight}
                                    onChange={(event) =>
                                      updateComponent(component.id, {
                                        weight: Number(event.target.value),
                                      })
                                    }
                                  />
                                </div>
                                <div className="grid gap-2">
                                  <label className="text-xs font-semibold uppercase tracking-wide text-muted">
                                    Kierunek
                                  </label>
                                  <select
                                    className="rounded-lg border border-soft bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
                                    value={component.direction}
                                    onChange={(event) =>
                                      updateComponent(component.id, {
                                        direction: event.target.value as ScoreDirection,
                                      })
                                    }
                                  >
                                    <option value="desc">Więcej = lepiej</option>
                                    <option value="asc">Mniej = lepiej</option>
                                  </select>
                                </div>
                              </div>

                              <p className="text-xs text-subtle">
                                {
                                  SCORE_METRICS.find((metric) => metric.value === component.metric)?.description
                                }
                              </p>
                            </div>
                            {autoComponents.length > 1 ? (
                              <button
                                type="button"
                                className="rounded-full bg-negative px-3 py-1 text-xs font-semibold text-white"
                                onClick={() => removeComponent(component.id)}
                              >
                                Usuń
                              </button>
                            ) : null}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              )}

              <button
                type="button"
                onClick={handleSubmit}
                className="mt-2 flex items-center justify-center rounded-xl bg-accent px-4 py-3 text-sm font-semibold text-white shadow-brand-elevated transition hover:bg-accent-strong disabled:cursor-not-allowed disabled:bg-subtle"
                disabled={isLoading || (mode === "manual" && manualSymbolsList.length === 0)}
              >
                {isLoading ? "Symulacja w toku..." : "Uruchom symulację"}
              </button>
              {error ? <p className="text-xs text-negative">{error}</p> : null}
            </div>
          </section>

          <section className="flex min-h-[520px] flex-col gap-6">
            {!result ? (
              <div className="flex flex-1 items-center justify-center rounded-3xl border border-dashed border-soft bg-soft-surface p-12 text-center text-muted">
                Skonfiguruj parametry i uruchom symulację, aby zobaczyć wyniki.
              </div>
            ) : (
              <>
                <div className="grid gap-4 rounded-3xl border border-soft bg-surface p-6 shadow-brand-floating">
                  <div className="flex flex-wrap items-center justify-between gap-4">
                    <div>
                      <h2 className="text-lg font-semibold text-neutral">
                        Wyniki portfela
                      </h2>
                      <p className="text-xs text-subtle">
                        {result.summary.start} → {result.summary.end}
                      </p>
                    </div>
                    <div className="text-right">
                      <p className="text-sm font-semibold text-primary">
                        Wartość końcowa
                      </p>
                      <p className="text-2xl font-semibold text-neutral">
                        {formatNumber(result.summary.final_value, 2)} PLN
                      </p>
                    </div>
                  </div>

                  <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                    {summaryCards.map((card) => (
                      <div
                        key={card.label}
                        className="rounded-2xl border border-soft bg-soft-surface p-4"
                      >
                        <p className="text-xs uppercase tracking-wide text-muted">
                          {card.label}
                        </p>
                        <p className="mt-2 text-xl font-semibold text-neutral">
                          {card.value}
                        </p>
                      </div>
                    ))}
                  </div>

                  <div className="h-72 w-full rounded-2xl border border-soft bg-white p-4">
                    <ResponsiveContainer>
                      <AreaChart data={chartData} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
                        <defs>
                          <linearGradient id="equityGradient" x1="0" x2="0" y1="0" y2="1">
                            <stop offset="5%" stopColor="#2ECC71" stopOpacity={0.8} />
                            <stop offset="95%" stopColor="#2ECC71" stopOpacity={0.05} />
                          </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="#EEF2F6" />
                        <XAxis dataKey="date" tick={{ fontSize: 11 }} minTickGap={24} />
                        <YAxis
                          tickFormatter={(value) => `${value.toFixed(0)}`}
                          width={70}
                          tick={{ fontSize: 11 }}
                        />
                        <Tooltip
                          formatter={(value: number, name) => {
                            if (name === "returnPercent") {
                              return [`${value.toFixed(2)}%`, "Zwrot od startu"];
                            }
                            return [value.toFixed(2), "Wartość (PLN)"];
                          }}
                          labelFormatter={(label) => `Data: ${label}`}
                          contentStyle={{ fontSize: 12 }}
                        />
                        <Area
                          type="monotone"
                          dataKey="value"
                          stroke="#1E8449"
                          strokeWidth={2}
                          fill="url(#equityGradient)"
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                <div className="grid gap-6 rounded-3xl border border-soft bg-surface p-6 shadow-brand-floating">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <h3 className="text-lg font-semibold text-neutral">
                      Ostatni portfel
                    </h3>
                    <p className="text-xs text-subtle">
                      {result.allocations.length} składników
                    </p>
                  </div>
                  <div className="overflow-hidden rounded-2xl border border-soft">
                    <table className="min-w-full divide-y divide-soft text-sm">
                      <thead className="bg-soft-surface text-xs uppercase tracking-wide text-muted">
                        <tr>
                          <th className="px-4 py-3 text-left">Spółka</th>
                          <th className="px-4 py-3 text-right">Waga docelowa</th>
                          <th className="px-4 py-3 text-right">Waga zrealizowana</th>
                          <th className="px-4 py-3 text-right">Zwrot</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-soft bg-white">
                        {result.allocations.map((allocation) => (
                          <tr key={allocation.symbol}>
                            <td className="px-4 py-3 font-medium text-neutral">
                              {allocation.symbol}
                            </td>
                            <td className="px-4 py-3 text-right text-muted">
                              {formatPercent(allocation.target_weight)}
                            </td>
                            <td className="px-4 py-3 text-right text-muted">
                              {formatPercent(allocation.realized_weight)}
                            </td>
                            <td className="px-4 py-3 text-right text-neutral">
                              {formatPercent(allocation.return_pct)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                {result.rebalances.length > 0 ? (
                  <div className="grid gap-4 rounded-3xl border border-soft bg-surface p-6 shadow-brand-floating">
                    <h3 className="text-lg font-semibold text-neutral">Harmonogram rebalansów</h3>
                    <div className="flex flex-col gap-4">
                      {result.rebalances.map((event) => (
                        <div
                          key={event.date}
                          className="rounded-2xl border border-soft bg-soft-surface p-4"
                        >
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <div>
                              <p className="text-sm font-semibold text-neutral">{event.date}</p>
                              {event.reason ? (
                                <p className="text-xs text-muted">{event.reason}</p>
                              ) : null}
                            </div>
                            <div className="text-xs text-subtle">
                              Obrót: {formatPercent(event.turnover)}
                            </div>
                          </div>
                          {event.trades && event.trades.length > 0 ? (
                            <div className="mt-3 overflow-hidden rounded-xl border border-soft bg-white">
                              <table className="min-w-full text-xs">
                                <thead className="bg-soft-surface uppercase tracking-wide text-muted">
                                  <tr>
                                    <th className="px-3 py-2 text-left">Ticker</th>
                                    <th className="px-3 py-2 text-left">Akcja</th>
                                    <th className="px-3 py-2 text-right">Zmiana wagi</th>
                                    <th className="px-3 py-2 text-right">Cena</th>
                                  </tr>
                                </thead>
                                <tbody className="divide-y divide-soft">
                                  {event.trades.map((trade, index) => (
                                    <tr key={`${trade.symbol}-${index}`}>
                                      <td className="px-3 py-2 font-medium text-neutral">
                                        {trade.symbol}
                                      </td>
                                      <td className="px-3 py-2 text-muted">{trade.action ?? ""}</td>
                                      <td className="px-3 py-2 text-right text-muted">
                                        {formatPercent(trade.weight_change)}
                                      </td>
                                      <td className="px-3 py-2 text-right text-muted">
                                        {trade.price ? `${trade.price.toFixed(2)} PLN` : "-"}
                                      </td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          ) : null}
                        </div>
                      ))}
                    </div>
                  </div>
                ) : null}
              </>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}
