"use client";

import {
    createContext,
    useCallback,
    useContext,
    useEffect,
    useMemo,
    useState,
    type ReactNode,
} from "react";

export type ThemeMode = "light" | "dark";

type ThemeContextValue = {
    theme: ThemeMode;
    setTheme: (next: ThemeMode) => void;
    toggleTheme: () => void;
    isReady: boolean;
};

const ThemeContext = createContext<ThemeContextValue | undefined>(undefined);

const THEME_STORAGE_KEY = "gpw-theme-preference";

const applyThemeToDocument = (theme: ThemeMode) => {
    const root = document.documentElement;
    root.dataset.theme = theme;
    root.style.colorScheme = theme;
};

export function ThemeProvider({ children }: { children: ReactNode }) {
    const [theme, setThemeState] = useState<ThemeMode>("light");
    const [isReady, setIsReady] = useState(false);

    useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }
        const storedTheme = window.localStorage.getItem(THEME_STORAGE_KEY);
        const systemPrefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
        const nextTheme: ThemeMode = storedTheme === "light" || storedTheme === "dark"
            ? storedTheme
            : systemPrefersDark
            ? "dark"
            : "light";

        setThemeState(nextTheme);
        applyThemeToDocument(nextTheme);
        setIsReady(true);
    }, []);

    useEffect(() => {
        if (!isReady || typeof window === "undefined") {
            return;
        }
        window.localStorage.setItem(THEME_STORAGE_KEY, theme);
        applyThemeToDocument(theme);
    }, [theme, isReady]);

    const updateTheme = useCallback((next: ThemeMode) => {
        setThemeState(next);
    }, []);

    const toggleTheme = useCallback(() => {
        setThemeState((prev) => (prev === "light" ? "dark" : "light"));
    }, []);

    const value = useMemo<ThemeContextValue>(
        () => ({
            theme,
            setTheme: updateTheme,
            toggleTheme,
            isReady,
        }),
        [theme, updateTheme, toggleTheme, isReady]
    );

    return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme() {
    const context = useContext(ThemeContext);
    if (!context) {
        throw new Error("useTheme must be used within a ThemeProvider");
    }
    return context;
}
