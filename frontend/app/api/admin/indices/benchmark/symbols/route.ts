import { NextRequest, NextResponse } from "next/server";

import { ensureAdmin } from "@/lib/admin-auth";

const resolveBackendUrl = (path: string) => {
    const base =
        process.env.NEXT_PUBLIC_API_BASE ||
        "https://gpw-analytics-starter-backend-1.onrender.com";
    const normalizedBase = base.endsWith("/") ? base : `${base}/`;
    if (!path) {
        return normalizedBase.replace(/\/$/, "");
    }
    const normalizedPath = path.startsWith("/") ? path.slice(1) : path;
    return `${normalizedBase}${normalizedPath}`;
};

export async function GET(req: NextRequest) {
    const auth = await ensureAdmin(req);
    if ("response" in auth) {
        return auth.response;
    }

    const url = new URL(resolveBackendUrl("/indices/benchmark/symbols"));
    req.nextUrl.searchParams.forEach((value, key) => {
        url.searchParams.set(key, value);
    });

    const backendResponse = await fetch(url, {
        cache: "no-store",
        headers: {
            accept: req.headers.get("accept") ?? "application/json",
        },
    });

    try {
        const payload = await backendResponse.json();
        if (!backendResponse.ok) {
            const errorPayload =
                payload && typeof payload === "object"
                    ? payload
                    : { error: "Nie udało się pobrać symboli GPW Benchmark" };
            return NextResponse.json(errorPayload, { status: backendResponse.status });
        }
        return NextResponse.json(payload, { status: backendResponse.status });
    } catch {
        const text = await backendResponse.text().catch(() => "");
        return NextResponse.json(
            {
                error:
                    text?.trim() ||
                    `Nie udało się pobrać symboli GPW Benchmark (status ${backendResponse.status})`,
            },
            { status: backendResponse.status }
        );
    }
}
