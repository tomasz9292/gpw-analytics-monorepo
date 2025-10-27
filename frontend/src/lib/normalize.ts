import type { MetricScale } from "@/components/MiniScoreChart";

export function normalize(value: number, scale: MetricScale): number {
    const key = scale.metricKey.replace(/[^a-z0-9]+/gi, "");
    const isPriceChange = /pricechange/i.test(key);

    const adjusted =
        scale.direction === "up" && isPriceChange ? Math.max(0, value) : value;

    const lo = Math.min(scale.worst, scale.best);
    const hi = Math.max(scale.worst, scale.best);
    const clamped = Math.min(hi, Math.max(lo, adjusted));
    let r = (clamped - lo) / Math.max(hi - lo, 1e-9);
    if (scale.direction === "down") r = 1 - r;
    return Math.min(1, Math.max(0, r));
}
