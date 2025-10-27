export const formatPct = (v: number, digits = 2) =>
    `${v >= 0 ? "+" : ""}${v.toFixed(digits)}%`;
