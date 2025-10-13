import fs from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";

const resolveConfiguredDir = (): string | null => {
    const configured = process.env.APP_DATA_DIR ?? process.env.DATA_DIR;
    if (!configured) return null;
    const trimmed = configured.trim();
    if (!trimmed) return null;
    if (path.isAbsolute(trimmed)) {
        return trimmed;
    }
    return path.join(process.cwd(), trimmed);
};

const DEFAULT_DATA_DIRS = [path.join(process.cwd(), "data"), path.join(tmpdir(), "gpw-analytics")];
const configuredDir = resolveConfiguredDir();
const DATA_DIRS = configuredDir ? [configuredDir] : DEFAULT_DATA_DIRS;
const DATA_FILES = DATA_DIRS.map((dir) => path.join(dir, "users.json"));

export type StoredScoreBuilderRule = {
    id: string;
    metric: string;
    label?: string | null;
    weight: number;
    direction: "asc" | "desc";
    min?: string;
    max?: string;
    transform?: "raw" | "zscore" | "percentile" | "";
};

export type StoredScoreTemplateRule = {
    metric: string;
    weight: number;
    direction: "asc" | "desc";
    label?: string | null;
    transform?: "raw" | "zscore" | "percentile" | "";
};

export type StoredScoreTemplate = {
    id: string;
    title: string;
    name?: string | null;
    description?: string | null;
    rules: StoredScoreTemplateRule[];
    limit: number;
    sort: "asc" | "desc";
    universe: string;
    minMcap: string;
    minTurnover: string;
    createdAt: string;
};

export type StoredScoreDraft = {
    name: string;
    description: string;
    limit: number;
    sort: "asc" | "desc";
    universe: string;
    minMcap: string;
    minTurnover: string;
    asOf: string;
    rules: StoredScoreBuilderRule[];
};

export type StoredPortfolioRow = {
    symbol: string;
    weight: number;
};

export type StoredPortfolioDraft = {
    mode: "manual" | "score";
    rows: StoredPortfolioRow[];
    start: string;
    end: string;
    initial: number;
    fee: number;
    threshold: number;
    benchmark: string | null;
    frequency: "none" | "monthly" | "quarterly" | "yearly";
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

export type StoredPreferences = {
    watchlist: string[];
    scoreTemplates: StoredScoreTemplate[];
    scoreDraft: StoredScoreDraft;
    portfolioDraft: StoredPortfolioDraft;
};

export type StoredUser = {
    id: string;
    email: string | null;
    name: string | null;
    picture: string | null;
    provider: "google";
    createdAt: string;
    updatedAt: string;
    preferences: StoredPreferences;
};

export type PublicUserProfile = {
    user: {
        id: string;
        email: string | null;
        name: string | null;
        picture: string | null;
        provider: "google";
        createdAt: string;
        updatedAt: string;
    };
    preferences: StoredPreferences;
};

const DEFAULT_SCORE_RULE: StoredScoreBuilderRule = {
    id: "return-weighted",
    metric: "total_return",
    weight: 50,
    direction: "desc",
    label: "Zwrot 12M",
    transform: "percentile",
};

const DEFAULT_SCORE_DRAFT: StoredScoreDraft = {
    name: "custom_quality",
    description: "Ranking jakościowy – przykład",
    limit: 10,
    sort: "desc",
    universe: "",
    minMcap: "",
    minTurnover: "",
    asOf: new Date().toISOString().slice(0, 10),
    rules: [DEFAULT_SCORE_RULE],
};

const DEFAULT_PORTFOLIO_DRAFT: StoredPortfolioDraft = {
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
};

const DEFAULT_PREFERENCES: StoredPreferences = {
    watchlist: ["CDR.WA", "PKN.WA", "PKOBP"],
    scoreTemplates: [],
    scoreDraft: DEFAULT_SCORE_DRAFT,
    portfolioDraft: DEFAULT_PORTFOLIO_DRAFT,
};

const readStore = async (): Promise<Record<string, StoredUser>> => {
    for (const file of DATA_FILES) {
        try {
            const raw = await fs.readFile(file, "utf8");
            const parsed = JSON.parse(raw) as Record<string, StoredUser>;
            if (!parsed || typeof parsed !== "object") {
                return {};
            }
            return parsed;
        } catch (err: unknown) {
            const code = (err as NodeJS.ErrnoException)?.code;
            if (code === "ENOENT" || code === "EACCES" || code === "EROFS") {
                continue;
            }
            throw err;
        }
    }
    return {};
};

const writeStore = async (data: Record<string, StoredUser>) => {
    let lastError: unknown;
    for (const [index, dir] of DATA_DIRS.entries()) {
        const file = DATA_FILES[index];
        try {
            await fs.mkdir(dir, { recursive: true });
            await fs.writeFile(file, JSON.stringify(data, null, 2), "utf8");
            return;
        } catch (err: unknown) {
            const code = (err as NodeJS.ErrnoException)?.code;
            if (code === "EACCES" || code === "EROFS" || code === "ENOENT") {
                lastError = err;
                continue;
            }
            throw err;
        }
    }
    if (lastError) {
        const error = new Error(
            "Nie można zapisać profilu użytkownika. Skonfiguruj APP_DATA_DIR lub sprawdź uprawnienia do katalogu danych."
        );
        (error as Error & { cause?: unknown }).cause = lastError;
        throw error;
    }
};

const normalizeString = (value: unknown): string => {
    return typeof value === "string" ? value.trim() : "";
};

const normalizeOptionalString = (value: unknown): string | null => {
    if (typeof value !== "string") return null;
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
};

const normalizeNumber = (value: unknown): number => {
    const numeric = typeof value === "number" ? value : Number(value);
    return Number.isFinite(numeric) ? numeric : 0;
};

const normalizeRule = (rule: unknown): StoredScoreBuilderRule | null => {
    if (!rule || typeof rule !== "object") return null;
    const candidate = rule as Partial<StoredScoreBuilderRule> & { id?: unknown };
    const metric = normalizeString(candidate.metric);
    if (!metric) return null;
    const id = normalizeString(candidate.id);
    const direction = candidate.direction === "asc" ? "asc" : "desc";
    const weight = normalizeNumber(candidate.weight);
    return {
        id: id || `${metric}-${direction}`,
        metric,
        direction,
        weight,
        label: normalizeOptionalString(candidate.label),
        min: normalizeOptionalString(candidate.min) ?? undefined,
        max: normalizeOptionalString(candidate.max) ?? undefined,
        transform:
            candidate.transform === "zscore"
                ? "zscore"
                : candidate.transform === "percentile"
                ? "percentile"
                : "raw",
    };
};

const normalizeTemplateRule = (rule: unknown): StoredScoreTemplateRule | null => {
    if (!rule || typeof rule !== "object") return null;
    const candidate = rule as Partial<StoredScoreTemplateRule>;
    const metric = normalizeString(candidate.metric);
    if (!metric) return null;
    const direction = candidate.direction === "asc" ? "asc" : "desc";
    const weight = normalizeNumber(candidate.weight) || 1;
    return {
        metric,
        direction,
        weight,
        label: normalizeOptionalString(candidate.label),
        transform:
            candidate.transform === "zscore"
                ? "zscore"
                : candidate.transform === "percentile"
                ? "percentile"
                : "raw",
    };
};

const normalizeTemplate = (tpl: unknown): StoredScoreTemplate | null => {
    if (!tpl || typeof tpl !== "object") return null;
    const candidate = tpl as Partial<StoredScoreTemplate> & { createdAt?: unknown };
    const id = normalizeString(candidate.id);
    const title = normalizeString(candidate.title) || id;
    if (!id || !title) return null;
    const rules = Array.isArray(candidate.rules)
        ? candidate.rules.map(normalizeTemplateRule).filter((rule): rule is StoredScoreTemplateRule => Boolean(rule))
        : [];
    if (!rules.length) return null;
    const limitRaw = normalizeNumber(candidate.limit);
    const limit = limitRaw > 0 ? Math.floor(limitRaw) : 10;
    const createdAtCandidate = normalizeString(candidate.createdAt);
    const createdAt = createdAtCandidate || new Date().toISOString();
    return {
        id,
        title,
        name: normalizeOptionalString(candidate.name),
        description: normalizeOptionalString(candidate.description),
        rules,
        limit,
        sort: candidate.sort === "asc" ? "asc" : "desc",
        universe: normalizeString(candidate.universe),
        minMcap: normalizeString(candidate.minMcap),
        minTurnover: normalizeString(candidate.minTurnover),
        createdAt,
    };
};

const normalizeScoreDraft = (draft: unknown): StoredScoreDraft => {
    if (!draft || typeof draft !== "object") return DEFAULT_SCORE_DRAFT;
    const candidate = draft as Partial<StoredScoreDraft> & { rules?: unknown };
    const rules = Array.isArray(candidate.rules)
        ? candidate.rules.map(normalizeRule).filter((rule): rule is StoredScoreBuilderRule => Boolean(rule))
        : DEFAULT_SCORE_DRAFT.rules;
    return {
        name: normalizeString(candidate.name) || DEFAULT_SCORE_DRAFT.name,
        description: normalizeString(candidate.description) || DEFAULT_SCORE_DRAFT.description,
        limit: (() => {
            const limit = normalizeNumber(candidate.limit);
            return limit > 0 ? Math.floor(limit) : DEFAULT_SCORE_DRAFT.limit;
        })(),
        sort: candidate.sort === "asc" ? "asc" : "desc",
        universe: normalizeString(candidate.universe),
        minMcap: normalizeString(candidate.minMcap),
        minTurnover: normalizeString(candidate.minTurnover),
        asOf: (() => {
            const raw = normalizeString(candidate.asOf);
            return raw || new Date().toISOString().slice(0, 10);
        })(),
        rules,
    };
};

const normalizePortfolioRow = (row: unknown): StoredPortfolioRow | null => {
    if (!row || typeof row !== "object") return null;
    const candidate = row as Partial<StoredPortfolioRow>;
    const symbol = normalizeString(candidate.symbol).toUpperCase();
    if (!symbol) return null;
    const weight = normalizeNumber(candidate.weight);
    return {
        symbol,
        weight,
    };
};

const normalizePortfolioDraft = (draft: unknown): StoredPortfolioDraft => {
    if (!draft || typeof draft !== "object") return DEFAULT_PORTFOLIO_DRAFT;
    const candidate = draft as Partial<StoredPortfolioDraft> & { rows?: unknown; score?: unknown; comparisons?: unknown };
    const rows = Array.isArray(candidate.rows)
        ? candidate.rows
              .map(normalizePortfolioRow)
              .filter((row): row is StoredPortfolioRow => Boolean(row))
        : DEFAULT_PORTFOLIO_DRAFT.rows;
    const comparisons = Array.isArray(candidate.comparisons)
        ? Array.from(
              new Set(
                  candidate.comparisons
                      .map((item) => normalizeString(item).toUpperCase())
                      .filter((item) => Boolean(item))
              )
          )
        : DEFAULT_PORTFOLIO_DRAFT.comparisons;
    const scoreSection = candidate.score && typeof candidate.score === "object" ? candidate.score : {};
    return {
        mode: candidate.mode === "score" ? "score" : "manual",
        rows,
        start: normalizeString(candidate.start) || DEFAULT_PORTFOLIO_DRAFT.start,
        end: normalizeString(candidate.end) || new Date().toISOString().slice(0, 10),
        initial: (() => {
            const numeric = normalizeNumber(candidate.initial);
            return numeric > 0 ? numeric : DEFAULT_PORTFOLIO_DRAFT.initial;
        })(),
        fee: (() => {
            const numeric = normalizeNumber(candidate.fee);
            return numeric >= 0 ? numeric : DEFAULT_PORTFOLIO_DRAFT.fee;
        })(),
        threshold: (() => {
            const numeric = normalizeNumber(candidate.threshold);
            return numeric >= 0 ? numeric : DEFAULT_PORTFOLIO_DRAFT.threshold;
        })(),
        benchmark: (() => {
            const normalized = normalizeOptionalString(candidate.benchmark);
            return normalized ? normalized.toUpperCase() : null;
        })(),
        frequency:
            candidate.frequency === "none"
                ? "none"
                : candidate.frequency === "quarterly"
                ? "quarterly"
                : candidate.frequency === "yearly"
                ? "yearly"
                : "monthly",
        score: {
            name: normalizeString((scoreSection as StoredPortfolioDraft["score"])?.name) || DEFAULT_PORTFOLIO_DRAFT.score.name,
            limit: (() => {
                const numeric = normalizeNumber((scoreSection as StoredPortfolioDraft["score"])?.limit);
                return numeric > 0 ? Math.floor(numeric) : DEFAULT_PORTFOLIO_DRAFT.score.limit;
            })(),
            weighting:
                normalizeString((scoreSection as StoredPortfolioDraft["score"])?.weighting) || DEFAULT_PORTFOLIO_DRAFT.score.weighting,
            direction:
                (scoreSection as StoredPortfolioDraft["score"])?.direction === "asc"
                    ? "asc"
                    : "desc",
            universe: normalizeString((scoreSection as StoredPortfolioDraft["score"])?.universe),
            min: normalizeString((scoreSection as StoredPortfolioDraft["score"])?.min),
            max: normalizeString((scoreSection as StoredPortfolioDraft["score"])?.max),
        },
        comparisons,
    };
};

const normalizeWatchlist = (value: unknown): string[] => {
    if (!Array.isArray(value)) return DEFAULT_PREFERENCES.watchlist;
    return Array.from(
        new Set(
            value
                .map((item) => normalizeString(item).toUpperCase())
                .filter((item) => Boolean(item))
        )
    );
};

const normalizePreferences = (prefs: unknown): StoredPreferences => {
    if (!prefs || typeof prefs !== "object") {
        return DEFAULT_PREFERENCES;
    }
    const candidate = prefs as Partial<StoredPreferences> & {
        watchlist?: unknown;
        scoreTemplates?: unknown;
        scoreDraft?: unknown;
        portfolioDraft?: unknown;
    };
    const watchlist = normalizeWatchlist(candidate.watchlist);
    const templates = Array.isArray(candidate.scoreTemplates)
        ? candidate.scoreTemplates
              .map(normalizeTemplate)
              .filter((tpl): tpl is StoredScoreTemplate => Boolean(tpl))
        : DEFAULT_PREFERENCES.scoreTemplates;
    return {
        watchlist,
        scoreTemplates: templates,
        scoreDraft: normalizeScoreDraft(candidate.scoreDraft),
        portfolioDraft: normalizePortfolioDraft(candidate.portfolioDraft),
    };
};

export const getOrCreateUserProfile = async ({
    id,
    email,
    name,
    picture,
}: {
    id: string;
    email: string | null;
    name: string | null;
    picture: string | null;
}): Promise<PublicUserProfile> => {
    const store = await readStore();
    const now = new Date().toISOString();
    const existing = store[id];
    if (existing) {
        const normalizedPreferences = normalizePreferences(existing.preferences);
        store[id] = {
            ...existing,
            email: email ?? existing.email,
            name: name ?? existing.name,
            picture: picture ?? existing.picture,
            updatedAt: now,
            preferences: normalizedPreferences,
        };
        await writeStore(store);
        return {
            user: {
                id,
                email: store[id].email,
                name: store[id].name,
                picture: store[id].picture,
                provider: "google",
                createdAt: store[id].createdAt,
                updatedAt: store[id].updatedAt,
            },
            preferences: normalizedPreferences,
        };
    }

    const preferences = DEFAULT_PREFERENCES;
    const newUser: StoredUser = {
        id,
        email,
        name,
        picture,
        provider: "google",
        createdAt: now,
        updatedAt: now,
        preferences,
    };
    store[id] = newUser;
    await writeStore(store);
    return {
        user: {
            id,
            email,
            name,
            picture,
            provider: "google",
            createdAt: now,
            updatedAt: now,
        },
        preferences,
    };
};

export const updateUserProfile = async (
    id: string,
    {
        email,
        name,
        picture,
    }: { email: string | null; name: string | null; picture: string | null },
    payload: unknown
): Promise<PublicUserProfile> => {
    const store = await readStore();
    const now = new Date().toISOString();
    const preferences = normalizePreferences(payload);
    const existing = store[id];
    if (existing) {
        store[id] = {
            ...existing,
            email: email ?? existing.email,
            name: name ?? existing.name,
            picture: picture ?? existing.picture,
            updatedAt: now,
            preferences,
        };
    } else {
        store[id] = {
            id,
            email,
            name,
            picture,
            provider: "google",
            createdAt: now,
            updatedAt: now,
            preferences,
        };
    }
    await writeStore(store);
    return {
        user: {
            id,
            email: store[id].email,
            name: store[id].name,
            picture: store[id].picture,
            provider: "google",
            createdAt: store[id].createdAt,
            updatedAt: store[id].updatedAt,
        },
        preferences,
    };
};
