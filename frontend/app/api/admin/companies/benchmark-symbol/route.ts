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

export async function POST(req: NextRequest) {
    const auth = await ensureAdmin(req);
    if ("response" in auth) {
        return auth.response;
    }

    let payload: unknown = null;
    try {
        payload = await req.json();
    } catch {
        payload = null;
    }

    const backendResponse = await fetch(resolveBackendUrl("/companies/benchmark-symbol"), {
        method: "POST",
        cache: "no-store",
        headers: {
            "content-type": "application/json",
            accept: req.headers.get("accept") ?? "application/json",
        },
        body: JSON.stringify(payload ?? {}),
    });

    try {
        const responseBody = await backendResponse.json();
        if (!backendResponse.ok) {
            const errorPayload =
                responseBody && typeof responseBody === "object"
                    ? responseBody
                    : { error: "Nie udało się zapisać symbolu GPW Benchmark" };
            return NextResponse.json(errorPayload, { status: backendResponse.status });
        }
        return NextResponse.json(responseBody, { status: backendResponse.status });
    } catch {
        const text = await backendResponse.text().catch(() => "");
        return NextResponse.json(
            {
                error:
                    text?.trim() ||
                    `Nie udało się zapisać symbolu GPW Benchmark (status ${backendResponse.status})`,
            },
            { status: backendResponse.status }
        );
    }
}
