export function AnalyticsDashboardLoading() {
    return (
        <div className="flex min-h-screen items-center justify-center bg-primary-strong text-white">
            <div className="space-y-3 text-center">
                <div className="mx-auto h-12 w-12 animate-spin rounded-full border-2 border-white/10 border-t-[var(--color-primary)]" />
                <p className="text-sm font-semibold text-subtle">Ładowanie pulpitu...</p>
            </div>
        </div>
    );
}
