"use client";

import { useEffect, useState } from "react";

import type { ComponentType } from "react";
import type { AnalyticsDashboardProps } from "./AnalyticsDashboard";

export function AnalyticsDashboard(props: AnalyticsDashboardProps) {
    const [Component, setComponent] = useState<ComponentType<AnalyticsDashboardProps> | null>(null);

    useEffect(() => {
        let mounted = true;

        import("./AnalyticsDashboard")
            .then((mod) => {
                if (mounted) {
                    setComponent(() => mod.AnalyticsDashboard);
                }
            })
            .catch(() => {
                if (mounted) {
                    setComponent(() => null);
                }
            });

        return () => {
            mounted = false;
        };
    }, []);

    if (!Component) {
        return (
            <div className="flex min-h-screen items-center justify-center bg-slate-950">
                <div className="space-y-3 text-center">
                    <div className="mx-auto h-12 w-12 animate-spin rounded-full border-2 border-slate-800 border-t-slate-400" />
                    <p className="text-sm font-semibold text-slate-300">Ładowanie pulpitu...</p>
                </div>
            </div>
        );
    }

    return <Component {...props} />;
}

export type { AnalyticsDashboardProps };
