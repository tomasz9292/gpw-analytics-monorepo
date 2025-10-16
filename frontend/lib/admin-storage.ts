import fs from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";

export type AdminEntry = {
    email: string;
    createdAt: string;
    addedBy: string | null;
};

const DEFAULT_ADMIN_EMAILS = ["tomasz.wasik92@gmail.com"];

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
const DATA_FILES = DATA_DIRS.map((dir) => path.join(dir, "admins.json"));

const normalizeEmail = (value: string | null | undefined): string | null => {
    if (!value) return null;
    const trimmed = value.trim().toLowerCase();
    return trimmed && trimmed.includes("@") ? trimmed : null;
};

const ensureDir = async (dir: string) => {
    try {
        await fs.mkdir(dir, { recursive: true });
    } catch (error: unknown) {
        const code = (error as NodeJS.ErrnoException)?.code;
        if (code === "EEXIST" || code === "EISDIR") {
            return;
        }
        throw error;
    }
};

const writeAdminStore = async (store: Record<string, AdminEntry>) => {
    let lastError: unknown;
    const serialized = JSON.stringify(store, null, 2);
    for (const [index, dir] of DATA_DIRS.entries()) {
        const file = DATA_FILES[index];
        try {
            await ensureDir(dir);
            await fs.writeFile(file, serialized, "utf8");
            return;
        } catch (error: unknown) {
            const code = (error as NodeJS.ErrnoException)?.code;
            if (code === "EROFS" || code === "EACCES" || code === "ENOENT") {
                lastError = error;
                continue;
            }
            throw error;
        }
    }
    if (lastError) {
        throw lastError;
    }
};

const parseStorePayload = (payload: unknown): Record<string, AdminEntry> => {
    if (!payload || typeof payload !== "object") {
        return {};
    }
    const source = payload as Record<string, unknown>;
    const result: Record<string, AdminEntry> = {};
    for (const [key, value] of Object.entries(source)) {
        const email = normalizeEmail(key);
        if (!email) continue;
        const entry = value as Partial<AdminEntry> | null | undefined;
        const createdAt =
            entry && typeof entry.createdAt === "string" && entry.createdAt
                ? entry.createdAt
                : new Date().toISOString();
        const addedBy =
            entry && typeof entry.addedBy === "string" && entry.addedBy.trim()
                ? entry.addedBy.trim().toLowerCase()
                : null;
        result[email] = {
            email,
            createdAt,
            addedBy,
        };
    }
    return result;
};

const readAdminStore = async (): Promise<Record<string, AdminEntry>> => {
    for (const file of DATA_FILES) {
        try {
            const raw = await fs.readFile(file, "utf8");
            const parsed = JSON.parse(raw) as unknown;
            const store = parseStorePayload(parsed);
            const withDefaults = ensureDefaultAdmins(store);
            if (withDefaults.changed) {
                await writeAdminStore(withDefaults.store);
                return withDefaults.store;
            }
            return store;
        } catch (error: unknown) {
            const code = (error as NodeJS.ErrnoException)?.code;
            if (code === "ENOENT" || code === "EACCES" || code === "EROFS") {
                continue;
            }
            throw error;
        }
    }
    const initial = ensureDefaultAdmins({}).store;
    await writeAdminStore(initial);
    return initial;
};

const ensureDefaultAdmins = (store: Record<string, AdminEntry>) => {
    let changed = false;
    for (const email of DEFAULT_ADMIN_EMAILS) {
        const normalized = normalizeEmail(email);
        if (!normalized) continue;
        if (!store[normalized]) {
            store[normalized] = {
                email: normalized,
                createdAt: new Date().toISOString(),
                addedBy: null,
            };
            changed = true;
        }
    }
    return { store, changed };
};

export class AdminAlreadyExistsError extends Error {}

export const getAdminList = async (): Promise<AdminEntry[]> => {
    const store = await readAdminStore();
    return Object.values(store).sort((a, b) => a.email.localeCompare(b.email));
};

export const isAdminEmail = async (email: string | null | undefined): Promise<boolean> => {
    const normalized = normalizeEmail(email);
    if (!normalized) {
        return false;
    }
    const store = await readAdminStore();
    return Boolean(store[normalized]);
};

export const addAdmin = async (
    email: string,
    addedBy: string | null = null
): Promise<AdminEntry[]> => {
    const normalized = normalizeEmail(email);
    if (!normalized) {
        throw new Error("Podaj poprawny adres e-mail administratora.");
    }
    const store = await readAdminStore();
    if (store[normalized]) {
        throw new AdminAlreadyExistsError("Podany adres jest już administratorem.");
    }
    store[normalized] = {
        email: normalized,
        createdAt: new Date().toISOString(),
        addedBy: addedBy ? normalizeEmail(addedBy) : null,
    };
    await writeAdminStore(store);
    return Object.values(store).sort((a, b) => a.email.localeCompare(b.email));
};
