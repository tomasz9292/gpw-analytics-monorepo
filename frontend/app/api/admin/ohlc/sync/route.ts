import { NextRequest, NextResponse } from "next/server";

import { ensureAdmin } from "@/lib/admin-auth";

const resolveBackendUrl = (path: string) => {
    const base =
        process.env.NEXT_PUBLIC_API_BASE ||
        "https://gpw-analytics-starter-backend-1.onrender.com";
    return path.startsWith("http")
        ? path
        : `${base}${path.startsWith("/") ? path : `/${path}`}`;
};

export async function POST(req: NextRequest) {
    const auth = await ensureAdmin(req);
    if ("response" in auth) {
        return auth.response;
    }

    let payload: unknown;
    try {
        payload = await req.json();
    } catch {
        payload = {};
    }

    const body =
        payload && typeof payload === "object" && !Array.isArray(payload)
            ? (payload as Record<string, unknown>)
            : {};

    const backendResponse = await fetch(resolveBackendUrl("/ohlc/sync"), {
        method: "POST",
        cache: "no-store",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
    });

    try {
        const data = await backendResponse.json();
        if (!backendResponse.ok) {
            const errorPayload =
                data && typeof data === "object"
                    ? data
                    : { error: "Nie udało się uruchomić synchronizacji notowań" };
            return NextResponse.json(errorPayload, {
                status: backendResponse.status,
            });
        }
        return NextResponse.json(data);
    } catch {
        const text = await backendResponse.text().catch(() => "");
        return NextResponse.json(
            {
                error:
                    text?.trim() ||
                    `Nie udało się uruchomić synchronizacji notowań (status ${backendResponse.status})`,
            },
            { status: backendResponse.status }
        );
    }
}
