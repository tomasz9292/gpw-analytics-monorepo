export function AnalyticsDashboardLoading() {
    return (
        <div className="flex min-h-screen items-center justify-center bg-slate-950">
            <div className="space-y-3 text-center">
                <div className="mx-auto h-12 w-12 animate-spin rounded-full border-2 border-slate-800 border-t-slate-400" />
                <p className="text-sm font-semibold text-slate-300">Ładowanie pulpitu...</p>
            </div>
        </div>
    );
}
