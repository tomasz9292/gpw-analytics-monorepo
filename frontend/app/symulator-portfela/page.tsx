"use client";

import dynamic from "next/dynamic";

import { AnalyticsDashboardLoading } from "../_components/AnalyticsDashboardLoading";
import type { AnalyticsDashboardProps } from "../_components/AnalyticsDashboard";

const AnalyticsDashboard = dynamic<AnalyticsDashboardProps>(
    () =>
        import("../_components/AnalyticsDashboard").then(
            (mod) => mod.AnalyticsDashboard
        ),
    {
        ssr: false,
        loading: () => <AnalyticsDashboardLoading />,
    }
);

export default function PortfolioSimulatorPage() {
    return <AnalyticsDashboard view="portfolio" />;
}
